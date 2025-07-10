#pragma once

#include <fcntl.h>
#include <libnvme.h>
#include <liburing.h>
#include <sys/eventfd.h>

#include <coro/coro.hpp>
#include <cstdint>
#include <cstdio>
#include <cstring>

#include "cachelib/navy/zone_hash/scheduler/SqeAwaitable.h"

namespace facebook {
namespace cachelib {
namespace navy {

struct ScheduleResolver;
using DequeueFn =
    std::function<uint32_t(std::vector<ScheduleResolver*>::iterator, uint32_t)>;

class IOExecutor {
 public:
  IOExecutor(io_uring& ring,
             int fd,
             uint32_t nsid,
             uint32_t lba_size,
             uint32_t entries,
             DequeueFn dequeue_fn);

  ~IOExecutor() { io_uring_queue_exit(&ring_); }

  IOExecutor(const IOExecutor&) = delete;
  IOExecutor& operator=(const IOExecutor&) = delete;

  bool isFreeSlot() const {
    return wait_cqe_count_ < (max_entries_ - (event_called_ ? 1 : 0));
  }

  void wake();

  SqeAwaitable read(uint64_t slba, void* buf, uint32_t buf_len);

  SqeAwaitable append(uint64_t zslba, void* buf, uint32_t buf_len);

  SqeAwaitable reset(uint64_t zslba);

  SqeAwaitable changeZoneIntoSLC(uint64_t zslba);

  SqeAwaitable changeZoneIntoQLC(uint64_t zslba);

  void finish();

  void run();

 private:
  io_uring& ring_;
  const uint32_t max_entries_;
  const uint32_t lba_size_;
  const DequeueFn dequeue_fn_;

  // nvme device related
  std::array<int, 2> fds_; // [dev_, event_fd_]
  int event_fd_;
  uint32_t nsid_;
  uint32_t cqe_count_;
  uint32_t wait_cqe_count_;
  uint64_t dummy;

  bool end_;
  bool event_called_;

  io_uring_sqe* fetchSqe();

  SqeAwaitable iouringPassthruEnqueue(nvme_uring_cmd& cmd);

  SqeAwaitable awaitWork(io_uring_sqe* sqe) {
    wait_cqe_count_++;
    return SqeAwaitable(sqe);
  }

  // EventSqeAwaitable eventAwaitWork(io_uring_sqe* sqe) {
  //   wait_cqe_count_++;
  //   return EventSqeAwaitable(sqe);
  // }

  void produce();
};

} // namespace navy
} // namespace cachelib
} // namespace facebook