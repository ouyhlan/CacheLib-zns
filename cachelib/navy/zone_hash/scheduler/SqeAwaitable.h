// forked from https://github.com/CarterLi/liburing4cpp
#pragma once

#include <liburing.h>

#include <cassert>
#include <climits>
#include <coroutine>
#include <cstdint>
#include <type_traits>

namespace facebook {
namespace cachelib {
namespace navy {

struct Resolver {
  virtual void resolve(uint64_t nvme_result) noexcept = 0;
};

struct ResumeResolver final : Resolver {
  friend struct SqeAwaitable;
  friend struct EventSqeAwaitable;

  void resolve(uint64_t nvme_result) noexcept override {
    this->nvme_result = nvme_result;
    handle.resume();
  }

 private:
  std::coroutine_handle<> handle;
  uint64_t nvme_result = 0;
};
static_assert(std::is_trivially_destructible_v<ResumeResolver>);

// struct DeferredResolver final : Resolver {
//   void resolve(uint64_t result) noexcept override { this->result = result; }

// #ifndef NDEBUG
//   ~DeferredResolver() {
//     assert(!!result && "DeferredResolver is destructed before it's
//     resolved");
//   }
// #endif

//   std::optional<int> result;
// };

// struct CallbackResolver final : Resolver {
//   CallbackResolver(std::function<void(int result)>&& cb) : cb(std::move(cb))
//   {}

//   void resolve(uint64_t result) noexcept override {
//     this->cb(result);
//     delete this;
//   }

//  private:
//   std::function<void(int result)> cb;
// };

struct SqeAwaitable {
  // TODO: use cancel_token to implement cancellation
  SqeAwaitable(io_uring_sqe* sqe) noexcept : sqe(sqe) {}

  // User MUST keep resolver alive before the operation is finished
  // void set_deferred(DeferredResolver& resolver) {
  //     io_uring_sqe_set_data(sqe, &resolver);
  // }

  // void set_callback(std::function<void (int result)> cb) {
  //     io_uring_sqe_set_data(sqe, new CallbackResolver(std::move(cb)));
  // }

  auto operator co_await() {
    struct AwaitSqe {
      ResumeResolver resolver{};
      io_uring_sqe* sqe;

      AwaitSqe(io_uring_sqe* sqe) : sqe(sqe) {}

      constexpr bool await_ready() const noexcept { return false; }

      void await_suspend(std::coroutine_handle<> handle) noexcept {
        resolver.handle = handle;
        io_uring_sqe_set_data(sqe, &resolver);
      }

      uint64_t await_resume() const { return resolver.nvme_result; }
    };

    return AwaitSqe(sqe);
  }

 private:
  io_uring_sqe* sqe;
};

// struct EventSqeAwaitable {
//   EventSqeAwaitable(io_uring_sqe* sqe) noexcept : sqe(sqe) {}

//   auto operator co_await() {
//     struct AwaitSqe {
//       ResumeResolver resolver{};
//       io_uring_sqe* sqe;

//       AwaitSqe(io_uring_sqe* sqe) : sqe(sqe) {}

//       constexpr bool await_ready() const noexcept { return false; }

//       void await_suspend(std::coroutine_handle<> handle) noexcept {
//         resolver.handle = handle;
//         resolver.inline_work = true;
//         io_uring_sqe_set_data(sqe, &resolver);
//       }

//       constexpr bool await_resume() const noexcept { return true; }
//     };

//     return AwaitSqe(sqe);
//   }

//  private:
//   io_uring_sqe* sqe;
// };

} // namespace navy
} // namespace cachelib
} // namespace facebook