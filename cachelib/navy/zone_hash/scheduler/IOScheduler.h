#pragma once

#include <folly/Format.h>
#include <folly/logging/xlog.h>
#include <folly/system/ThreadName.h>

#include <cstdint>
#include <memory>

#include "cachelib/navy/zone_hash/scheduler/IOThreadPool.h"

namespace facebook {
namespace cachelib {
namespace navy {

enum class AsyncJobType { Flush, Backend };

class IOScheduler {
 public:
  explicit IOScheduler(io_uring& initialer_uring,
                       int fd,
                       uint nsid,
                       uint32_t queue_depth,
                       uint32_t lba_size,
                       uint32_t flush_num_threads,
                       uint32_t backend_num_threads) {
    flush_ = std::make_unique<OrderedIOThreadPool>("flush",
                                                   initialer_uring,
                                                   fd,
                                                   nsid,
                                                   queue_depth,
                                                   lba_size,
                                                   flush_num_threads);

    garbageCollection_ =
        std::make_unique<PreemptiveIOThreadPool>("gc_worker",
                                                 initialer_uring,
                                                 fd,
                                                 nsid,
                                                 queue_depth,
                                                 lba_size,
                                                 backend_num_threads);
  }

  IOScheduler(const IOScheduler&) = delete;
  IOScheduler& operator=(const IOScheduler&) = delete;

  ~IOScheduler() { finish(); }

  auto schedule(AsyncJobType type, bool need_new_schedule) {
    switch (type) {
    case AsyncJobType::Flush:
      return flush_->schedule(need_new_schedule);
      break;
    case AsyncJobType::Backend:
      return garbageCollection_->schedule(need_new_schedule);
      break;
    default:
      XLOGF(ERR,
            "IOScheduler: unrecognized job type: {}",
            static_cast<uint32_t>(type));
      XDCHECK(false);
    }
  }

  void finish() {
    flush_->finish();
    garbageCollection_->finish();
  }

 private:
  std::unique_ptr<IOThreadPool> flush_;
  std::unique_ptr<IOThreadPool> garbageCollection_;
};

} // namespace navy
} // namespace cachelib
} // namespace facebook