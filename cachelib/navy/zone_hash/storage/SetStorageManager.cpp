#include "cachelib/navy/zone_hash/storage/SetStorageManager.h"

#include <folly/Format.h>
#include <folly/SharedMutex.h>
#include <folly/logging/xlog.h>

#include <cstdint>
#include <memory>
#include <mutex>

#include "cachelib/common/BloomFilter.h"
#include "cachelib/navy/scheduler/ThreadPoolJobQueue.h"
#include "cachelib/navy/zone_hash/utils/CuckooHashMap.h"
#include "cachelib/navy/zone_hash/utils/Types.h"
#include "cachelib/navy/zone_hash/utils/ZoneBucket.h"

namespace facebook {
namespace cachelib {
namespace navy {

SetStorageManager::SetStorageManager(ZoneManager& zns_mgr,
                                     ZoneNandType zone_type,
                                     uint32_t initial_num_zones,
                                     uint32_t num_clean_zones,
                                     uint64_t page_size_byte,
                                     uint32_t num_threads,
                                     SetCleanPageFn clean_fn)
    : zns_mgr_(zns_mgr),
      zone_type_(zone_type),
      num_clean_zones_(num_clean_zones),
      zone_size_byte_(zns_mgr.zoneSize()),
      page_size_byte_(page_size_byte),
      reclaim_scheduled_(0),
      threads_pool_(num_threads, "set_pool"),
      set_clean_page_fn_(clean_fn) {
  auto acquire_zone_arr = zns_mgr_.allocate(initial_num_zones, zone_type_);
  if (acquire_zone_arr.size() <= num_clean_zones) {
    throw std::invalid_argument(
        folly::sformat("Invalid num of zones because we need at "
                       "least {} zones for the set cache",
                       num_clean_zones + 1));
  }

  XLOG(INFO,
       folly::sformat(
           "SetStorageManager created: need num zones: {}, allocated num zones: {}, num clean zones: {}",
           initial_num_zones,
           acquire_zone_arr.size(),
           num_clean_zones_));

  for (uint32_t i = 1; i < acquire_zone_arr.size(); i++) {
    free_zone_id_arr_.push_back(acquire_zone_arr[i]);
  }

  active_zone_ = zns_mgr_.getZone(acquire_zone_arr[0]);
  zns_mgr.registerSetStorageManager(*this);
}

void SetStorageManager::pageAppend(
    SetIdT set_id,
    Buffer&& buffer,
    std::function<void(FlashPageOffsetT)> callback) {
  auto flush_job = [this,
                    set_id,
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

    auto [status, byte_address] = curr_zone->appendPage(set_id, buf);
    if (!status) {
      return JobExitCode::Reschedule;
    }

    cb(getFlashPageOffsetFromByteAddress(byte_address));
    return JobExitCode::Done;
  };

  threads_pool_.enqueue(
      std::move(flush_job), "Flush Page", JobQueue::QueuePos::Back);
}

Buffer SetStorageManager::readPage(FlashPageOffsetT flash_page_offset) {
  FlashByteAddressT flash_byte_address =
      getFlashByteAddressFromPageOffset(flash_page_offset);

  return zns_mgr_.readPage(flash_byte_address);
}

bool SetStorageManager::comparePageAge(FlashPageOffsetT a, FlashPageOffsetT b) {
  uint32_t a_zone_id = getZoneIdFromPageOffset(a);
  uint32_t b_zone_id = getZoneIdFromPageOffset(b);

  if (a_zone_id == b_zone_id) {
    return a >= b;
  }

  uint32_t a_timestamp = zns_mgr_.timestamp(a_zone_id);
  uint32_t b_timestamp = zns_mgr_.timestamp(b_zone_id);
  XDCHECK(a_timestamp != 0 && b_timestamp != 0);

  // larger timestamp means younger age
  return a_timestamp >= b_timestamp;
}

uint32_t SetStorageManager::getFreeZoneId() {
  // caller must hold append_mutex
  std::unique_lock<std::mutex> clean_lock(clean_mutex_);

  while (free_zone_id_arr_.size() == 0) {
    clean_lock.unlock();
    garbageCollection();
    clean_lock.lock();
  }

  auto free_zone_id = free_zone_id_arr_.front();
  free_zone_id_arr_.pop_front();

  checkIfNeedGarbageCollection();
  return free_zone_id;
}

bool SetStorageManager::retrieveFreeZone() {
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

  checkIfNeedGarbageCollection();
  return res;
}

// must hold clean_mutex
void SetStorageManager::checkIfNeedGarbageCollection() {
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
}

void SetStorageManager::garbageCollection() {
  uint32_t reclaim_zone_id;
  {
    std::unique_lock<std::mutex> clean_lock(clean_mutex_);

    XDCHECK(allocate_zone_id_arr_.size() > 0);
    reclaim_zone_id = allocate_zone_id_arr_.front();
    allocate_zone_id_arr_.pop_front();
  }

  FlashPageOffsetT page_addr = getZoneStartPageOffset(reclaim_zone_id);
  auto page_arr = zns_mgr_.pageArr(reclaim_zone_id);
  for (auto& [set_id, page_addr] : page_arr) {
    set_clean_page_fn_(set_id, page_addr);
  }

  zns_mgr_.reset(reclaim_zone_id);

  {
    std::unique_lock<std::mutex> clean_lock(clean_mutex_);
    free_zone_id_arr_.push_back(reclaim_zone_id);

    XDCHECK(reclaim_scheduled_ > 0);
    reclaim_scheduled_--;
  }
}

} // namespace navy
} // namespace cachelib
} // namespace facebook