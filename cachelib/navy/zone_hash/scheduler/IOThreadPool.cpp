#include "cachelib/navy/zone_hash/scheduler/IOThreadPool.h"

#include "cachelib/navy/zone_hash/scheduler/IOExecutor.h"

namespace facebook {
namespace cachelib {
namespace navy {

IOThreadPool::IOThreadPool([[maybe_unused]] std::string name,
                           io_uring& initialer_uring,
                           [[maybe_unused]] int fd,
                           [[maybe_unused]] uint32_t nsid,
                           uint32_t queue_depth,
                           [[maybe_unused]] uint32_t lba_size,
                           uint32_t num_threads)
    : rings_(num_threads), executors_(num_threads) {
  for (uint32_t i = 0; i < num_threads; i++) {
    io_uring_params p = {};
    p.flags = IORING_SETUP_SQE128 | IORING_SETUP_CQE32 | IORING_SETUP_SQPOLL;
    p.sq_thread_idle = 2000;
    p.wq_fd = initialer_uring.ring_fd;
    p.flags |= IORING_SETUP_ATTACH_WQ;

    int err = io_uring_queue_init_params(queue_depth, &(rings_[i]), &p);
    if (err) {
      XLOG(ERR,
           folly::sformat("io_uring_queue_init_params failed, error code: {}",
                          err));
      throw std::runtime_error(folly::sformat(
          "io_uring_queue_init_params failed, error code: {}", err));
    }
    if (!(p.features & IORING_FEAT_SQPOLL_NONFIXED)) {
      XLOG(ERR, "No SQPOLL sharing, skipping");
      throw std::invalid_argument("No SQPOLL sharing, skipping");
    }
  }
}

void IOThreadPool::finish() {
  for (auto& executor : executors_) {
    executor->finish();
  }

  for (auto& thread : io_threads_) {
    thread.join();
  }
}

OrderedIOThreadPool::OrderedIOThreadPool(std::string name,
                                         io_uring& initialer_uring,
                                         int fd,
                                         uint32_t nsid,
                                         uint32_t queue_depth,
                                         uint32_t lba_size,
                                         uint32_t num_threads)
    : IOThreadPool(
          name, initialer_uring, fd, nsid, queue_depth, lba_size, num_threads),
      task_queues_(num_threads) {
  for (uint32_t i = 0; i < num_threads; i++) {
    executors_[i] = new IOExecutor(
        rings_[i], fd, nsid, lba_size, queue_depth,
        [this, i](std::vector<ScheduleResolver*>::iterator it,
                  uint32_t max_size) { return dequeue(i, it, max_size); });

    io_threads_.emplace_back([this, executor = executors_[i],
                              threadName = folly::sformat("{}_{}", name, i)] {
      tl_executor_.reset(executor); // give the ownership to threadLocalPtr
      folly::setThreadName(threadName);
      executor->run();
    });
  }
}

void OrderedIOThreadPool::enqueue(ScheduleResolver& resolver) {
  uint64_t next_executor_id = fetchNextExecutorId();
  task_queues_[next_executor_id].enqueue(&resolver);
  ioExecutor(next_executor_id).wake();
}

uint32_t OrderedIOThreadPool::dequeue(
    uint32_t index,
    std::vector<ScheduleResolver*>::iterator it,
    uint32_t max_size) {
  return task_queues_[index].try_dequeue_bulk(it, max_size);
}

PreemptiveIOThreadPool::PreemptiveIOThreadPool(std::string name,
                                               io_uring& initialer_uring,
                                               int fd,
                                               uint32_t nsid,
                                               uint32_t queue_depth,
                                               uint32_t lba_size,
                                               uint32_t num_threads)
    : IOThreadPool(
          name, initialer_uring, fd, nsid, queue_depth, lba_size, num_threads) {
  for (uint32_t i = 0; i < num_threads; i++) {
    executors_[i] = new IOExecutor(
        rings_[i], fd, nsid, lba_size, queue_depth,
        [this](std::vector<ScheduleResolver*>::iterator it, uint32_t max_size) {
          return dequeue(it, max_size);
        });

    io_threads_.emplace_back([this, executor = executors_[i],
                              threadName = folly::sformat("{}_{}", name, i)] {
      tl_executor_.reset(executor); // give the ownership to threadLocalPtr
      folly::setThreadName(threadName);
      executor->run();
    });
  }
}

void PreemptiveIOThreadPool::enqueue(ScheduleResolver& resolver) {
  tasks_.enqueue(&resolver);

  uint32_t executor_id = fetchNextExecutorId();
  for (uint32_t i = 0; i < 32 && remainTaskApprox() > 0; i++) {
    ioExecutor(executor_id + i).wake();
  }
}

uint32_t PreemptiveIOThreadPool::dequeue(
    std::vector<ScheduleResolver*>::iterator it, uint32_t max_size) {
  return tasks_.try_dequeue_bulk(it, max_size);
}

} // namespace navy
} // namespace cachelib
} // namespace facebook