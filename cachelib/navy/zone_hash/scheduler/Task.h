// forked from https://github.com/CarterLi/liburing4cpp
#pragma once

#include <execinfo.h>

#include <cassert>
#include <coroutine>
#include <cstdint>
#include <exception>
#include <type_traits>
#include <utility>
#include <variant>

#include "cachelib/external/folly/folly/experimental/symbolizer/Symbolizer.h"
#include "cachelib/navy/common/Buffer.h"

namespace facebook {
namespace cachelib {
namespace navy {
template <typename T, bool nothrow>
struct Task;

// only for internal usage
template <typename T, bool nothrow>
struct TaskPromiseBase {
  Task<T, nothrow> get_return_object();

  auto initial_suspend() { return std::suspend_always(); }

  auto final_suspend() noexcept {
    struct Awaiter : std::suspend_always {
      TaskPromiseBase* me_;

      Awaiter(TaskPromiseBase* me) : me_(me){};
      std::coroutine_handle<> await_suspend(
          [[maybe_unused]] std::coroutine_handle<> caller) const noexcept {
        if (__builtin_expect(me_->result_.index() == 3, false)) {
          // FIXME: destroy current coroutine; otherwise memory leaks.
          if (me_->waiter_) {
            me_->waiter_.destroy();
          }
          std::coroutine_handle<TaskPromiseBase>::from_promise(*me_).destroy();
        } else if (me_->waiter_) {
          return me_->waiter_;
        }
        return std::noop_coroutine();
      }
    };
    return Awaiter(this);
  }

  void unhandled_exception() {
    if constexpr (!nothrow) {
      std::exception_ptr eptr = std::current_exception();
      try {
        if (eptr) {
          std::rethrow_exception(eptr);
        }
      } catch (const std::exception& e) {
        XLOG(INFO) << "Caught exception: " << e.what() << std::endl;
        folly::symbolizer::SafeStackTracePrinter printer;
        printer.printStackTrace(true);

        if (__builtin_expect(result_.index() == 3, false))
          return;
        result_.template emplace<2>(eptr);
      }

    } else {
      __builtin_unreachable();
    }
  }

 protected:
  friend struct Task<T, nothrow>;

  TaskPromiseBase() = default;

  std::coroutine_handle<> waiter_;
  std::variant<std::monostate,
               std::conditional_t<std::is_void_v<T>, std::monostate, T>,
               std::conditional_t<!nothrow, std::exception_ptr, std::monostate>,
               std::monostate // indicates that the promise is detached
               >
      result_;
};

// only for internal usage
template <typename T, bool nothrow>
struct TaskPromise final : TaskPromiseBase<T, nothrow> {
  using TaskPromiseBase<T, nothrow>::result_;

  template <typename U>
  void return_value(U&& u) {
    if (__builtin_expect(result_.index() == 3, false))
      return;
    result_.template emplace<1>(static_cast<U&&>(u));
  }

  void return_value(int u) {
    if (__builtin_expect(result_.index() == 3, false))
      return;
    result_.template emplace<1>(u);
  }
};

template <bool nothrow>
struct TaskPromise<void, nothrow> final : TaskPromiseBase<void, nothrow> {
  using TaskPromiseBase<void, nothrow>::result_;

  void return_void() {
    if (__builtin_expect(result_.index() == 3, false))
      return;
    result_.template emplace<1>(std::monostate{});
  }
};

/**
 * An awaitable object that returned by an async function
 * @tparam T value type holded by this Task
 * @tparam nothrow if true, the coroutine assigned by this Task won't throw
 * exceptions ( slightly better performance )
 * @warning do NOT discard this object when returned by some function, or UB
 * WILL happen
 */
template <typename T = void, bool nothrow = false>
struct Task final {
  using promise_type = TaskPromise<T, nothrow>;
  using handle_t = std::coroutine_handle<promise_type>;

  Task(const Task&) = delete;
  Task& operator=(const Task&) = delete;

  bool await_ready() { return !coro_ || coro_.done(); }

  template <typename T_, bool nothrow_>
  auto await_suspend(
      std::coroutine_handle<TaskPromise<T_, nothrow_>> caller) noexcept {
    coro_.promise().waiter_ = caller;
    return coro_;
  }

  T await_resume() const { return get_result(); }

  /** Get the result hold by this Task */
  T get_result() const {
    auto& result_ = coro_.promise().result_;
    assert(result_.index() != 0);
    if constexpr (!nothrow) {
      if (auto* pep = std::get_if<2>(&result_)) {
        std::rethrow_exception(*pep);
      }
    }
    if constexpr (std::is_same_v<T, Buffer>) {
      return Buffer(std::move(*std::get_if<1>(&result_)));
    } else if constexpr (std::is_same_v<T,
                                        std::tuple<Buffer, uint64_t, Buffer>>) {
      auto& tuple = *std::get_if<1>(&result_);
      return {std::move(std::get<0>(tuple)), std::get<1>(tuple),
              std::move(std::get<2>(tuple))};
    } else if constexpr (!std::is_void_v<T>) {
      return *std::get_if<1>(&result_);
    }
  }

  /** Get is the coroutine done */
  bool done() const { return coro_.done(); }

  void resume() const { coro_.resume(); }

  void detach() {
    assert(!detached_);
    coro_.promise().result_.template emplace<3>(std::monostate{});
    detached_ = true;
    coro_.resume(); // since initial_suspend -> suspend_always{}, need a way to
                    // resume coroutine
  }

  /** Only for placeholder */
  Task() : coro_(nullptr){};

  Task(Task&& other) noexcept { coro_ = std::exchange(other.coro_, nullptr); }

  Task& operator=(Task&& other) noexcept {
    if (coro_)
      coro_.destroy();
    coro_ = std::exchange(other.coro_, nullptr);
    return *this;
  }

  /** Destroy (when done) or detach (when not done) the Task object */
  ~Task() {
    if (!detached_) {
      if (!coro_) {
        return;
      }

      if (coro_.done()) {
        coro_.destroy();
      } else {
        XLOG(ERR, "Destroy undone task without calling detach()!");
        folly::symbolizer::SafeStackTracePrinter printer;
        printer.printStackTrace(true);
      }
    }
  }

 private:
  friend struct TaskPromiseBase<T, nothrow>;
  Task(promise_type* p) : coro_(handle_t::from_promise(*p)) {}
  handle_t coro_;
  bool detached_{false};
};

template <typename T, bool nothrow>
Task<T, nothrow> TaskPromiseBase<T, nothrow>::get_return_object() {
  return Task<T, nothrow>(static_cast<TaskPromise<T, nothrow>*>(this));
}

} // namespace navy
} // namespace cachelib
} // namespace facebook