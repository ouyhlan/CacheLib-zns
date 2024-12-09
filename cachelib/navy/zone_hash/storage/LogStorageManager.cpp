#include "cachelib/navy/zone_hash/storage/LogStorageManager.h"

#include <folly/Format.h>
#include <folly/SharedMutex.h>
#include <folly/logging/xlog.h>

#include <atomic>
#include <cassert>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/scheduler/JobScheduler.h"
#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

LogStorageManager::LogStorageManager(ZoneManager& zns_mgr,
                                     ZoneNandType zone_type,
                                     uint32_t num_zones,
                                     uint32_t num_clean_zones,
                                     uint64_t segment_size_byte,
                                     uint64_t page_size_byte,
                                     uint32_t num_threads,
                                     LogCleanSegmentFn clean_fn)
    : zns_mgr_(zns_mgr),
      zone_type_(zone_type),
      num_clean_zones_(num_clean_zones),
      segment_size_byte_(segment_size_byte),
      page_size_byte_(page_size_byte),
      reclaim_scheduled_(0),
      log_clean_segment_fn_(clean_fn),
      threads_pool_(num_threads, "log_pool") {
  auto acquire_zone_arr = zns_mgr_.allocate(num_zones, zone_type_);
  if (acquire_zone_arr.size() <= num_clean_zones) {
    throw std::invalid_argument(
        folly::sformat("Invalid num of zones because we need at "
                       "least {} zones for the set cache",
                       num_clean_zones + 1));
  }

  XLOG(INFO,
       folly::sformat(
           "LogStorageManager created: num zones: {}, num clean zones: {}",
           acquire_zone_arr.size(),
           num_clean_zones_));

  for (uint32_t i = 1; i < acquire_zone_arr.size(); i++) {
    free_zone_id_arr_.push_back(acquire_zone_arr[i]);
  }

  active_zone_ = zns_mgr_.getZone(acquire_zone_arr[0]);
}

void LogStorageManager::flushSegment(
    uint32_t logical_segment_id,
    Buffer&& buffer,
    std::function<void(FlashSegmentOffsetT)> callback) {
  auto flush_job = [this,
                    logical_segment_id,
                    buf = std::move(buffer),
                    cb = std::move(callback)]() mutable {
    Zone* curr_zone = active_zone_.load(std::memory_order_relaxed);
    while (curr_zone == nullptr || !curr_zone->haveFreeSpace()) {
      bool res = retrieveFreeZone();
      if (!res) {
        return JobExitCode::Reschedule;
      }

      curr_zone = active_zone_.load(std::memory_order_relaxed);
    }

    auto [status, byte_address] =
        curr_zone->appendSegment(logical_segment_id, buf);
    if (!status) {
      return JobExitCode::Reschedule;
    }

    cb(getFlashSegmentOffset(byte_address));
    return JobExitCode::Done;
  };

  threads_pool_.enqueue(
      std::move(flush_job), "Flush Segment", JobQueue::QueuePos::Front);
}

// lock must hold by the caller
Buffer LogStorageManager::readSegment(
    FlashSegmentOffsetT flash_segment_offset) {
  FlashByteAddressT flash_byte_address =
      getFlashByteAddressFromSegmentOffset(flash_segment_offset);

  return zns_mgr_.readSegment(flash_byte_address);
}

// lock must hold by the caller
Buffer LogStorageManager::readPage(FlashPageOffsetT flash_page_offset) {
  FlashByteAddressT flash_byte_address =
      getFlashByteAddressFromPageOffset(flash_page_offset);

  return zns_mgr_.readPage(flash_byte_address);
}

void LogStorageManager::garbageCollection() {
  uint32_t reclaim_zone_id;
  {
    std::unique_lock<std::mutex> clean_lock(clean_mutex_);
    XDCHECK(allocate_zone_id_arr_.size() > 0);
    reclaim_zone_id = allocate_zone_id_arr_.front();
    allocate_zone_id_arr_.pop_front();
  }

  auto logical_segment_arr = zns_mgr_.segmentArr(reclaim_zone_id);

  auto num_segment_to_clean =
      std::make_shared<AtomicCounter>(logical_segment_arr.size());

  auto reset_fn = [this, reclaim_zone_id]() {
    zns_mgr_.reset(reclaim_zone_id);

    {
      std::unique_lock<std::mutex> clean_lock(clean_mutex_);
      free_zone_id_arr_.push_back(reclaim_zone_id);

      XDCHECK(reclaim_scheduled_ > 0);
      reclaim_scheduled_--;
    }
  };

  for (auto logical_segment_id : logical_segment_arr) {
    threads_pool_.enqueue(
        [=] {
          log_clean_segment_fn_(logical_segment_id);

          if (num_segment_to_clean->sub_fetch(1) == 0) {
            reset_fn();
          }

          return JobExitCode::Done;
        },
        "clean segment",
        JobQueue::QueuePos::Back);
  }
}

bool LogStorageManager::retrieveFreeZone() {
  bool res = false;
  Zone* prev_active_zone = active_zone_.load(std::memory_order_relaxed);
  std::unique_lock<std::mutex> clean_lock(clean_mutex_);

  // wait for free zone
  if (free_zone_id_arr_.size() > 0) {
    uint32_t free_zone_id = free_zone_id_arr_.front();
    Zone* free_zone = zns_mgr_.getZone(free_zone_id);
    XDCHECK(free_zone->zoneType() == zone_type_);
    
    if (active_zone_.compare_exchange_weak(
            prev_active_zone, free_zone, std::memory_order_relaxed)) {
      if (prev_active_zone != nullptr) {
        allocate_zone_id_arr_.push_back(prev_active_zone->zoneId());
      }
      free_zone_id_arr_.pop_front();
    }

    res = true;
  }

  // Schedule Garbage Collection
  uint32_t new_sched = 0;
  auto planned_clean_zones = free_zone_id_arr_.size() + reclaim_scheduled_;
  if (planned_clean_zones < num_clean_zones_) {
    new_sched = num_clean_zones_ - planned_clean_zones;
    reclaim_scheduled_ += new_sched;
  }

  for (uint32_t i = 0; i < new_sched; i++) {
    threads_pool_.enqueue(
        [this] {
          garbageCollection();
          return JobExitCode::Done;
        },
        "garbage collection",
        JobQueue::QueuePos::Back);
  }

  return res;
}

} // namespace navy
} // namespace cachelib
} // namespace facebook