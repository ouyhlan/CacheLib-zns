#pragma once

#include <bits/types/struct_iovec.h>
#include <folly/Format.h>
#include <folly/ThreadLocal.h>
#include <folly/logging/xlog.h>
#include <liburing.h>
#include <liburing/io_uring.h>
#include <nvme/ioctl.h>

#include <cerrno>
#include <cstdint>
#include <stdexcept>
#include <vector>
namespace facebook {
namespace cachelib {
namespace navy {

class PollIOContext {
 public:
  PollIOContext(io_uring& initialer_uring,
                int fd,
                uint32_t nsid,
                uint32_t lba_size,
                uint32_t page_size,
                uint32_t queue_depth)
      : fds_{fd},
        nsid_(nsid),
        lba_size_(lba_size),
        page_size_(page_size),
        queue_depth_(queue_depth),
        initialer_uring_(&initialer_uring) {
    int err = io_uring_register_files(initialer_uring_, fds_.data(), 1);
    if (err) {
      throw std::runtime_error(
          folly::sformat("Error registering buffers: {}", err));
    }

    tl_ring_.reset(initialer_uring_);
  }

  void read(uint64_t slba, void* buf, uint32_t buf_len) {
    io_uring* curr_ring = currRing();
    iovec* iov = ioBuffer(curr_ring);
    XDCHECK(buf_len == iov->iov_len);

    uint32_t nlb = (buf_len / lba_size_) - 1;
    nvme_uring_cmd cmd{};
    cmd.opcode = nvme_cmd_read;
    cmd.nsid = nsid_;
    cmd.addr = (__u64)(uintptr_t)iov->iov_base;
    cmd.data_len = iov->iov_len;
    cmd.cdw10 = slba & 0xFFFFFFFF;
    cmd.cdw11 = slba >> 32;
    cmd.cdw12 = nlb;

    io_uring_sqe* sqe = fetchSqe(curr_ring);

    // set fixed buffer
    sqe->buf_index = 0;
    sqe->uring_cmd_flags |= IORING_URING_CMD_FIXED;

    prepPassthruSQE(sqe, cmd);
    io_uring_submit(curr_ring);
    poll(curr_ring, 1);

    std::memcpy(buf, iov->iov_base, iov->iov_len);
  }

  uint64_t append(uint64_t slba, const void* buf, uint32_t buf_len) {
    uint64_t res;
    uint32_t nlb = (buf_len / lba_size_) - 1;

    nvme_uring_cmd cmd{};
    cmd.opcode = nvme_zns_cmd_append;
    cmd.nsid = nsid_;
    cmd.addr = (__u64)(uintptr_t)buf;
    cmd.data_len = buf_len;
    cmd.cdw10 = slba & 0xFFFFFFFF;
    cmd.cdw11 = slba >> 32;
    cmd.cdw12 = nlb;

    io_uring* curr_ring = currRing();
    io_uring_sqe* sqe = fetchSqe(curr_ring);

    prepPassthruSQE(sqe, cmd);
    io_uring_sqe_set_data(sqe, &res);
    io_uring_submit(curr_ring);
    poll(curr_ring, 1);
    return res;
  }

  void write(uint64_t slba, const void* buf, uint32_t buf_len) {
    uint32_t nlb = (buf_len / lba_size_) - 1;

    nvme_uring_cmd cmd{};
    cmd.opcode = nvme_cmd_write;
    cmd.nsid = nsid_;
    cmd.addr = (__u64)(uintptr_t)buf;
    cmd.data_len = buf_len;
    cmd.cdw10 = slba & 0xFFFFFFFF;
    cmd.cdw11 = slba >> 32;
    cmd.cdw12 = nlb;

    io_uring* curr_ring = currRing();
    io_uring_sqe* sqe = fetchSqe(curr_ring);
    prepPassthruSQE(sqe, cmd);
    io_uring_submit(curr_ring);
    poll(curr_ring, 1);
  }

  void changeZonesIntoSLC(std::vector<uint64_t>& slba_arr) {
    io_uring* curr_ring = currRing();

    for (uint32_t curr = 0; curr < slba_arr.size();) {
      uint32_t submit_count = 0;
      for (uint32_t i = 0; i < queue_depth_ && curr < slba_arr.size(); i++) {
        io_uring_sqe* sqe = fetchSqe(curr_ring);
        prepChangeIntoSLCSQE(sqe, slba_arr[curr]);
        submit_count++;
        curr++;
      }

      io_uring_submit(curr_ring);
      poll(curr_ring, submit_count);
    }
  }

  void changeZonesIntoQLC(std::vector<uint64_t>& slba_arr) {
    io_uring* curr_ring = currRing();

    for (uint32_t curr = 0; curr < slba_arr.size();) {
      uint32_t submit_count = 0;
      for (uint32_t i = 0; i < queue_depth_ && curr < slba_arr.size(); i++) {
        io_uring_sqe* sqe = fetchSqe(curr_ring);
        prepChangeIntoQLCSQE(sqe, slba_arr[curr]);
        submit_count++;
        curr++;
      }

      io_uring_submit(curr_ring);
      poll(curr_ring, submit_count);
    }
  }

  void changeZoneIntoSLC(uint64_t slba) {
    io_uring* curr_ring = currRing();
    io_uring_sqe* sqe = fetchSqe(curr_ring);
    prepChangeIntoSLCSQE(sqe, slba);
    io_uring_submit(curr_ring);
    poll(curr_ring, 1);
  }

  void changeZoneIntoQLC(uint64_t slba) {
    io_uring* curr_ring = currRing();
    io_uring_sqe* sqe = fetchSqe(curr_ring);
    prepChangeIntoQLCSQE(sqe, slba);
    io_uring_submit(curr_ring);
    poll(curr_ring, 1);
  }

 private:
  std::array<int, 1> fds_; // [dev_]
  uint32_t nsid_;
  const uint32_t lba_size_;
  const uint32_t page_size_;
  const uint32_t queue_depth_;

  folly::ThreadLocalPtr<io_uring> tl_ring_;
  folly::ThreadLocalPtr<iovec> io_buffer_;

  io_uring* initialer_uring_;

  io_uring* currRing() {
    if (!tl_ring_) {
      io_uring* curr_ring = new io_uring;
      io_uring_params p = {};
      p.flags = IORING_SETUP_SQE128 | IORING_SETUP_CQE32 | IORING_SETUP_SQPOLL;
      p.sq_thread_idle = 2000;

      if (initialer_uring_ != nullptr) {
        p.wq_fd = initialer_uring_->ring_fd;
        p.flags |= IORING_SETUP_ATTACH_WQ;
      }

      int err = io_uring_queue_init_params(queue_depth_, curr_ring, &p);
      if (err) {
        throw std::runtime_error(folly::sformat(
            "io_uring_queue_init_params failed, error code: {}", err));
      }
      if (!(p.features & IORING_FEAT_SQPOLL_NONFIXED)) {
        throw std::invalid_argument("No SQPOLL sharing, skipping");
      }

      err = io_uring_register_files(curr_ring, fds_.data(), 1);
      if (err) {
        throw std::runtime_error(
            folly::sformat("Error registering buffers: {}", err));
      }

      tl_ring_.reset(curr_ring);
    }
    return tl_ring_.get();
  }

  iovec* ioBuffer(io_uring* curr_ring) {
    if (!io_buffer_) {
      iovec* iov = new iovec;
      iov->iov_base = malloc(page_size_);
      iov->iov_len = page_size_;

      int ret = io_uring_register_buffers(curr_ring, iov, 1);
      if (ret) {
        throw std::runtime_error(
            folly::sformat("Error registering buffers, error code: {}", ret));
      }
      io_buffer_.reset(iov);
    }

    return io_buffer_.get();
  }

  io_uring_sqe* fetchSqe(io_uring* ring) {
    io_uring_sqe* sqe = io_uring_get_sqe(ring);
    if (sqe == nullptr) [[unlikely]] {
      throw std::runtime_error("failed to fetch sqe!");
    }
    return sqe;
  }

  void prepPassthruSQE(io_uring_sqe* sqe, nvme_uring_cmd& cmd) {
    XDCHECK(sqe != nullptr);

    sqe->opcode = IORING_OP_URING_CMD;
    sqe->flags |= IOSQE_FIXED_FILE;
    sqe->fd = 0;
    sqe->cmd_op = NVME_URING_CMD_IO;
    memcpy(sqe->cmd, &cmd, sizeof(nvme_uring_cmd));
  }

  void prepChangeIntoSLCSQE(io_uring_sqe* sqe, uint64_t zslba) {
    nvme_uring_cmd cmd{};
    cmd.opcode = nvme_zns_cmd_mgmt_send;
    cmd.nsid = nsid_;
    cmd.cdw10 = zslba & 0xFFFFFFFF;
    cmd.cdw11 = zslba >> 32;
    cmd.cdw13 = 0x12;

    prepPassthruSQE(sqe, cmd);
  }

  void prepChangeIntoQLCSQE(io_uring_sqe* sqe, uint64_t zslba) {
    nvme_uring_cmd cmd{};
    cmd.opcode = nvme_zns_cmd_mgmt_send;
    cmd.nsid = nsid_;
    cmd.cdw10 = zslba & 0xFFFFFFFF;
    cmd.cdw11 = zslba >> 32;
    cmd.cdw13 = 0x13;

    prepPassthruSQE(sqe, cmd);
  }

  void poll(io_uring* ring, int nr_ios) {
    unsigned head;
    io_uring_cqe* cqe;

    while (nr_ios > 0) {
      io_uring_wait_cqe_nr(ring, &cqe, nr_ios);

      uint32_t cqe_count = 0;
      io_uring_for_each_cqe(ring, head, cqe) {
        if (cqe->res == -EAGAIN) {
          continue;
        } else if (cqe->res < 0) [[unlikely]] {
          XLOG(ERR,
               folly::sformat("ZNS async operation failed, with error code: {}",
                              cqe->res));
          throw std::runtime_error(folly::sformat(
              "ZNS async operation failed, with error code: {}", cqe->res));
        }

        auto* res_ptr = reinterpret_cast<uint64_t*>(io_uring_cqe_get_data(cqe));
        if (res_ptr != nullptr) [[unlikely]] {
          *res_ptr = cqe->big_cqe[0];
        }

        nr_ios--;
        cqe_count++;
      }

      io_uring_cq_advance(ring, cqe_count);
    }
  }
};

} // namespace navy
} // namespace cachelib
} // namespace facebook