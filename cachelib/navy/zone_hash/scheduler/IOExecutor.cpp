#include "cachelib/navy/zone_hash/scheduler/IOExecutor.h"

#include <folly/Format.h>
#include <folly/logging/xlog.h>
#include <liburing.h>
#include <nvme/types.h>

#include <cerrno>
#include <cstdint>
#include <stdexcept>
#include <vector>

#include "cachelib/navy/zone_hash/scheduler/IOThreadPool.h"

namespace facebook {
namespace cachelib {
namespace navy {

IOExecutor::IOExecutor(io_uring& ring,
                       int fd,
                       uint32_t nsid,
                       uint32_t lba_size,
                       uint32_t entries,
                       DequeueFn dequeue_fn)
    : ring_(ring),
      max_entries_(entries),
      lba_size_(lba_size),
      dequeue_fn_(std::move(dequeue_fn)),
      nsid_(nsid),
      cqe_count_(0),
      wait_cqe_count_(0),
      end_(false),
      event_called_(true) {
  event_fd_ = eventfd(0, O_NONBLOCK | O_CLOEXEC);
  if (event_fd_ == -1) {
    XLOG(ERR, "Create eventfd failed!");
    throw std::runtime_error("Create eventfd failed!");
  }

  fds_ = {fd, event_fd_};
  int err = io_uring_register_files(&ring_, fds_.data(), 2);
  if (err) {
    XLOG(ERR, folly::sformat("Error registering buffers: {}", err));
    throw std::runtime_error(
        folly::sformat("Error registering buffers: {}", err));
  }
}

void IOExecutor::wake() { eventfd_write(event_fd_, 1); }

SqeAwaitable IOExecutor::read(uint64_t slba, void* buf, uint32_t buf_len) {
  uint32_t nlb = (buf_len / lba_size_) - 1;

  // TODO: fix buffers
  nvme_uring_cmd cmd{};
  cmd.opcode = nvme_cmd_read;
  cmd.nsid = nsid_;
  cmd.addr = (__u64)(uintptr_t)buf;
  cmd.data_len = buf_len;
  cmd.cdw10 = slba & 0xFFFFFFFF;
  cmd.cdw11 = slba >> 32;
  cmd.cdw12 = nlb;
  return iouringPassthruEnqueue(cmd);
}

SqeAwaitable IOExecutor::append(uint64_t zslba, void* buf, uint32_t buf_len) {
  uint32_t nlb = (buf_len / lba_size_) - 1;

  nvme_uring_cmd cmd{};
  cmd.opcode = nvme_zns_cmd_append;
  cmd.nsid = nsid_;
  cmd.addr = (__u64)(uintptr_t)buf;
  cmd.data_len = buf_len;
  cmd.cdw10 = zslba & 0xFFFFFFFF;
  cmd.cdw11 = zslba >> 32;
  cmd.cdw12 = nlb;
  return iouringPassthruEnqueue(cmd);
}

SqeAwaitable IOExecutor::reset(uint64_t zslba) {
  nvme_uring_cmd cmd{};
  cmd.opcode = nvme_zns_cmd_mgmt_send;
  cmd.nsid = nsid_;
  cmd.cdw10 = zslba & 0xFFFFFFFF;
  cmd.cdw11 = zslba >> 32;
  cmd.cdw13 = 0x04;
  return iouringPassthruEnqueue(cmd);
}

SqeAwaitable IOExecutor::changeZoneIntoSLC(uint64_t zslba) {
  nvme_uring_cmd cmd{};
  cmd.opcode = nvme_zns_cmd_mgmt_send;
  cmd.nsid = nsid_;
  cmd.cdw10 = zslba & 0xFFFFFFFF;
  cmd.cdw11 = zslba >> 32;
  cmd.cdw13 = 0x12;
  return iouringPassthruEnqueue(cmd);
}

SqeAwaitable IOExecutor::changeZoneIntoQLC(uint64_t zslba) {
  nvme_uring_cmd cmd{};
  cmd.opcode = nvme_zns_cmd_mgmt_send;
  cmd.nsid = nsid_;
  cmd.cdw10 = zslba & 0xFFFFFFFF;
  cmd.cdw11 = zslba >> 32;
  cmd.cdw13 = 0x13;
  return iouringPassthruEnqueue(cmd);
}

io_uring_sqe* IOExecutor::fetchSqe() {
  io_uring_sqe* sqe = io_uring_get_sqe(&ring_);
  if (__builtin_expect(!!sqe, true)) {
    return sqe;
  }

  io_uring_cq_advance(&ring_, cqe_count_);
  cqe_count_ = 0;
  io_uring_submit(&ring_);
  sqe = io_uring_get_sqe(&ring_);
  assert(sqe != nullptr);
  return sqe;
}

SqeAwaitable IOExecutor::iouringPassthruEnqueue(nvme_uring_cmd& cmd) {
  io_uring_sqe* sqe = fetchSqe();
  XDCHECK(sqe != nullptr);

  sqe->opcode = IORING_OP_URING_CMD;
  sqe->flags |= IOSQE_FIXED_FILE;
  sqe->fd = 0;
  sqe->cmd_op = NVME_URING_CMD_IO;
  memcpy(sqe->cmd, &cmd, sizeof(nvme_uring_cmd));
  return awaitWork(sqe);
}

void IOExecutor::produce() {
  if (event_called_) {
    io_uring_sqe* sqe = fetchSqe();
    io_uring_prep_read(sqe, 1, &dummy, sizeof(dummy), 0);
    io_uring_sqe_set_data(sqe, nullptr);
    sqe->flags |= IOSQE_FIXED_FILE;

    wait_cqe_count_++;
    event_called_ = false;
  }

  XDCHECK_GE(max_entries_, wait_cqe_count_);
  uint32_t max_num_tasks = max_entries_ - wait_cqe_count_;
  if (max_num_tasks > 0) {
    std::vector<ScheduleResolver*> enqueue_tasks(max_num_tasks);
    uint32_t num_tasks = dequeue_fn_(enqueue_tasks.begin(), max_num_tasks);

    for (uint32_t i = 0; i < num_tasks; i++) {
      enqueue_tasks[i]->resolve(*this);
    }
  }
}

void IOExecutor::finish() { end_ = true; }

void IOExecutor::run() {
  while (!end_ || (wait_cqe_count_ - (uint32_t)(!event_called_)) > 0) {
    produce();
    io_uring_submit_and_wait(&ring_, 1);

    unsigned head;
    io_uring_cqe* cqe;

    io_uring_for_each_cqe(&ring_, head, cqe) {
      if (cqe->res == -EAGAIN) {
        continue;
      } else if (cqe->res < 0) [[unlikely]] {
        XLOG(ERR,
             folly::sformat("ZNS async operation failed, with error code: {}, "
                            "NVME result: {}",
                            cqe->res, cqe->big_cqe[0]));
        throw std::runtime_error(folly::sformat(
            "ZNS async operation failed, with error code: {}", cqe->res));
      }

      ++cqe_count_;
      auto coro = static_cast<Resolver*>(io_uring_cqe_get_data(cqe));
      if (coro != nullptr) [[likely]] {
        coro->resolve(cqe->big_cqe[0]);
      } else {
        event_called_ = true;
      }

      --wait_cqe_count_;
    }

    if (cqe_count_ > 0) {
      io_uring_cq_advance(&ring_, cqe_count_);
      cqe_count_ = 0;
    }
  }

  XDCHECK(wait_cqe_count_ == 0);
}

} // namespace navy
} // namespace cachelib
} // namespace facebook