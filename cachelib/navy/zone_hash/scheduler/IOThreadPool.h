#pragma once

#include <folly/Format.h>
#include <folly/ThreadLocal.h>
#include <folly/logging/xlog.h>
#include <folly/system/ThreadName.h>

#include <coroutine>
#include <cstdint>
#include <vector>

#include "cachelib/common/AtomicCounter.h"
#undef BLOCK_SIZE
#include <concurrentqueue/moodycamel/concurrentqueue.h>

#include "cachelib/navy/zone_hash/scheduler/IOExecutor.h"

namespace facebook {
namespace cachelib {
namespace navy {

struct ScheduleResolver {
  void resolve(IOExecutor& executor) {
    io_executor = &executor;
    handle.resume();
  }

  std::coroutine_handle<> handle;
  IOExecutor* io_executor = nullptr;
};

class IOThreadPool {
  struct ScheduleAwaiter {
    ScheduleResolver resolver{};
    IOThreadPool& thread_pool_;
    bool need_new_schedule_;

    ScheduleAwaiter(IOThreadPool& thread_pool, bool need_new_schedule)
        : thread_pool_(thread_pool), need_new_schedule_(need_new_schedule) {}

    constexpr bool await_ready() const noexcept { return false; }

    bool await_suspend(std::coroutine_handle<> handle) noexcept {
      IOExecutor* tl_executor = thread_pool_.getIOExecutor();
      // check if current thread is io thread to avoid extra cost
      if (need_new_schedule_ || tl_executor == nullptr ||
          !tl_executor->isFreeSlot()) {
        resolver.handle = handle;
        thread_pool_.enqueue(resolver);
        return true;
      }

      resolver.io_executor = tl_executor;
      return false;
    }

    constexpr IOExecutor& await_resume() const noexcept {
      return *resolver.io_executor;
    }
  };

 public:
  explicit IOThreadPool(std::string name,
                        io_uring& initialer_uring,
                        int fd,
                        uint32_t nsid,
                        uint32_t queue_depth,
                        uint32_t lba_size,
                        uint32_t num_threads);

  IOThreadPool(const IOThreadPool&) = delete;
  IOThreadPool& operator=(const IOThreadPool&) = delete;
  ~IOThreadPool() { finish(); }

  ScheduleAwaiter schedule(bool need_new_schedule) {
    return ScheduleAwaiter(*this, need_new_schedule);
  }

  void finish();

  virtual void enqueue(ScheduleResolver& resolver) = 0;

  IOExecutor* getIOExecutor() {
    if (!tl_executor_) {
      return tl_executor_.get();
    } else {
      return nullptr;
    }
  }

 protected:
  folly::ThreadLocalPtr<IOExecutor> tl_executor_;
  AtomicCounter next_executor_id_{0};
  std::vector<io_uring> rings_;
  std::vector<IOExecutor*> executors_;
  std::vector<std::thread> io_threads_;

  uint64_t fetchNextExecutorId() {
    return next_executor_id_.fetch_add(1) % executors_.size();
  }

  IOExecutor& ioExecutor(uint32_t id) {
    return *executors_[id % executors_.size()];
  }
};

class OrderedIOThreadPool : public IOThreadPool {
 public:
  explicit OrderedIOThreadPool(std::string name,
                               io_uring& initialer_uring,
                               int fd,
                               uint32_t nsid,
                               uint32_t queue_depth,
                               uint32_t lba_size,
                               uint32_t num_threads);

  void enqueue(ScheduleResolver& resolver) override;

  uint32_t dequeue(uint32_t index,
                   std::vector<ScheduleResolver*>::iterator it,
                   uint32_t max_size);

 private:
  std::vector<moodycamel::ConcurrentQueue<ScheduleResolver*>> task_queues_;
};

class PreemptiveIOThreadPool : public IOThreadPool {
 public:
  explicit PreemptiveIOThreadPool(std::string name,
                                  io_uring& initialer_uring,
                                  int fd,
                                  uint32_t nsid,
                                  uint32_t queue_depth,
                                  uint32_t lba_size,
                                  uint32_t num_threads);

  void enqueue(ScheduleResolver& resolver) override;

  uint32_t dequeue(std::vector<ScheduleResolver*>::iterator it,
                   uint32_t max_size);

 private:
  moodycamel::ConcurrentQueue<ScheduleResolver*> tasks_;

  uint32_t remainTaskApprox() const { return tasks_.size_approx(); }
};

} // namespace navy
} // namespace cachelib
} // namespace facebook