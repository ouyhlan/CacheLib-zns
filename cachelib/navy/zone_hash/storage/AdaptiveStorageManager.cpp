#include "cachelib/navy/zone_hash/storage/AdaptiveStorageManager.h"

#include <folly/Format.h>
#include <folly/logging/xlog.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/scheduler/JobScheduler.h"
#include "cachelib/navy/zone_hash/utils/LogSegment.h"
#include "cachelib/navy/zone_hash/utils/Types.h"
#include "cachelib/navy/zone_hash/utils/ZoneBucket.h"

namespace facebook {
namespace cachelib {
namespace navy {

AdaptiveStorageManager::AdaptiveStorageManager(
    ZoneManager& zns_mgr,
    ZoneNandType zone_type,
    uint32_t initial_num_zones,
    uint32_t num_free_zones,
    uint64_t segment_size_byte,
    uint64_t page_size_byte,
    CalculateInvalidRateFn calculate_fn,
    AdaptiveReclaimFn reclaim_fn,
    uint32_t num_threads)
    : zns_mgr_(zns_mgr),
      zone_type_(zone_type),
      num_free_zones_(num_free_zones),
      zone_size_byte_(zns_mgr.zoneSize()),
      segment_size_byte_(segment_size_byte),
      page_size_byte_(page_size_byte),
      calculate_invalid_rate_fn_(calculate_fn),
      reclaim_fn_(reclaim_fn),
      reclaim_scheduled_(0),
      threads_pool_(num_threads, "ada_pool") {
  auto acquire_zone_arr = zns_mgr_.allocate(initial_num_zones, zone_type_);
  for (auto zone_id : acquire_zone_arr) {
    free_zone_id_arr_.push_back(zone_id);
  }

  XLOG(INFO,
       folly::sformat("AdaptiveStorageManager created: num zones: {}",
                      acquire_zone_arr.size()));

  zns_mgr.registerAdaptiveStorageManager(*this);
}

FlashSegmentOffsetT AdaptiveStorageManager::segmentAppend(
    uint32_t zone_id, uint32_t logical_segment_id, Buffer&& buffer) {
  auto curr_zone = zns_mgr_.getZone(zone_id);

  auto [status, byte_address] =
      curr_zone->appendSegment(logical_segment_id, buffer);
  XDCHECK(status); // garbage collection will not full up the zone

  return getFlashSegmentOffset(byte_address);
}

void AdaptiveStorageManager::flushSegment(
    uint32_t logical_segment_id,
    Buffer&& buffer,
    std::function<void(FlashSegmentOffsetT)> callback) {
  auto flush_job = [this, logical_segment_id, buf = std::move(buffer),
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

  threads_pool_.enqueue(std::move(flush_job), "Flush Segment",
                        JobQueue::QueuePos::Front);
}

Buffer AdaptiveStorageManager::readSegment(
    FlashSegmentOffsetT flash_segment_offset) {
  FlashByteAddressT flash_byte_address =
      getFlashByteAddressFromSegmentOffset(flash_segment_offset);

  return zns_mgr_.readSegment(flash_byte_address);
}

Buffer AdaptiveStorageManager::readPage(FlashPageOffsetT flash_page_offset) {
  FlashByteAddressT flash_byte_address =
      getFlashByteAddressFromPageOffset(flash_page_offset);

  return zns_mgr_.readPage(flash_byte_address);
}

void AdaptiveStorageManager::checkIfNeedGarbageCollection() {
  uint32_t new_sched = 0;
  auto planned_free_zones = free_zone_id_arr_.size() + reclaim_scheduled_;
  if (planned_free_zones < num_free_zones_) {
    new_sched = num_free_zones_ - reclaim_scheduled_;
    reclaim_scheduled_ += new_sched;
  }

  for (uint32_t i = 0; i < new_sched; i++) {
    threads_pool_.enqueue(
        [this] {
          garbageCollection();
          return JobExitCode::Done;
        },
        "reclaim.evict",
        JobQueue::QueuePos::Back);
  }
}

void AdaptiveStorageManager::garbageCollection() {
  auto [status, loan_zone_id] = zns_mgr_.getFreeZoneFromSet();
  zns_mgr_.changeZoneType(loan_zone_id, zone_type_);

  if (status != Status::Ok) {
    throw std::runtime_error("Cannot loan zone from set!");
  }

  // retrieve reclaim zone
  uint32_t reclaim_zone_id;
  {
    std::unique_lock<std::mutex> clean_lock(clean_mutex_);
    XDCHECK(allocate_zone_id_arr_.size() > 0);

    double max_invalid_rate = 0.0;
    for (auto zone_id : allocate_zone_id_arr_) {
      double curr_invalid_rate =
          calculate_invalid_rate_fn_(zns_mgr_.segmentArr(zone_id));
      XDCHECK(curr_invalid_rate >= 0);
      if (curr_invalid_rate >= max_invalid_rate) {
        reclaim_zone_id = zone_id;
      }
    }

    allocate_zone_id_arr_.erase(reclaim_zone_id);
  }

  auto logical_segment_arr = zns_mgr_.segmentArr(reclaim_zone_id);

  // reset_fn
  uint32_t free_zone_id = loan_zone_id;
  auto num_segment_to_clean =
      std::make_shared<AtomicCounter>(logical_segment_arr.size());
  auto reset_fn = [this, reclaim_zone_id, free_zone_id]() {
    zns_mgr_.reset(reclaim_zone_id);

    {
      std::unique_lock<std::mutex> clean_lock(clean_mutex_);
      free_zone_id_arr_.push_back(free_zone_id);

      XDCHECK(reclaim_scheduled_ > 0);
      reclaim_scheduled_--;
      ;
    }

    zns_mgr_.returnZoneToSet(reclaim_zone_id);
  };

  for (auto logical_segment_id : logical_segment_arr) {
    threads_pool_.enqueue(
        [=, buffer = Buffer(), iter = LogSegment::Iterator()]() mutable {
          Status status =
              reclaim_fn_(free_zone_id, logical_segment_id, buffer, iter);
          if (status == Status::Retry) {
            return JobExitCode::Reschedule;
          } else if (status == Status::DeviceError) {
            XLOG(INFO, "Read segment failed!");
          }

          if (num_segment_to_clean->sub_fetch(1) == 0) {
            reset_fn();
          }

          return JobExitCode::Done;
        },
        "clean segment", JobQueue::QueuePos::Back);
  }
}

bool AdaptiveStorageManager::retrieveFreeZone() {
  bool res = false;
  Zone* prev_active_zone = active_zone_.load(std::memory_order_relaxed);
  std::unique_lock<std::mutex> clean_lock(clean_mutex_);

  // wait for free zone
  if (free_zone_id_arr_.size() > 0) {
    uint32_t free_zone_id = free_zone_id_arr_.front();
    Zone* free_zone = zns_mgr_.getZone(free_zone_id);
    XDCHECK(free_zone->zoneType() == zone_type_);

    if (active_zone_.compare_exchange_weak(prev_active_zone, free_zone,
                                           std::memory_order_relaxed)) {
      if (prev_active_zone != nullptr) {
        allocate_zone_id_arr_.insert(prev_active_zone->zoneId());
      }
      
      free_zone_id_arr_.pop_front();
    }

    res = true;
  }

  checkIfNeedGarbageCollection();
  return res;
}

} // namespace navy
} // namespace cachelib
} // namespace facebook