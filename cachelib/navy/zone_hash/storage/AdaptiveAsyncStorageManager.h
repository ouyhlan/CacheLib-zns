#pragma once

#include <folly/Format.h>

#include <cstdint>
#include <functional>
#include <mutex>

#include "cachelib/navy/zone_hash/scheduler/IOExecutor.h"
#include "cachelib/navy/zone_hash/scheduler/IOScheduler.h"
#include "cachelib/navy/zone_hash/storage/ZonedAsyncStorageManager.h"
#include "coro/semaphore.hpp"

namespace facebook {
namespace cachelib {
namespace navy {

using AdaptiveReclaimFn = std::function<void(uint32_t, uint32_t, coro::latch&)>;
using AdaptiveRemoveFn = std::function<void(uint32_t, coro::latch&)>;

class AdaptiveAsyncStorageManager : public ZonedAsyncStorageManager {
 public:
  AdaptiveAsyncStorageManager(ZoneManager& zns_mgr,
                              JobScheduler& scheduler,
                              ZoneNandType zone_type,
                              uint32_t initial_num_zones,
                              uint32_t num_clean_zones,
                              uint64_t segment_size_byte,
                              uint64_t page_size_byte,
                              CalculateInvalidRateFn calculate_fn,
                              AdaptiveReclaimFn reclaim_fn,
                              AdaptiveRemoveFn remove_fn)
      : ZonedAsyncStorageManager(
            zns_mgr, scheduler, zone_type, num_clean_zones, page_size_byte),
        segment_size_byte_(segment_size_byte),
        calculate_invalid_rate_fn_(calculate_fn),
        reclaim_fn_(reclaim_fn),
        remove_fn_(remove_fn),
        allocate_semaphore_(max_zones_num, 0),
        num_external_zones_(0) {
    auto acquire_zone_arr = zns_mgr_.allocate(initial_num_zones, zone_type_);

    XLOG(INFO,
         folly::sformat("AdaptiveStorageManager created: num zones: {}",
                        acquire_zone_arr.size()));

    for (uint32_t i = 0; i < acquire_zone_arr.size(); i++) {
      free_zone_rb_.produce(acquire_zone_arr[i]);
    }

    zns_mgr.registerAdaptiveAsyncStorageManager(*this);
  }

  Task<> flushSegment(uint32_t logical_segment_id,
                      Buffer buffer,
                      UpdateSegmentIndexFn update_fn) {
    Zone* curr_zone = co_await allocate(segment_size_byte_);

    auto& io_executor =
        co_await zns_mgr_.asyncSchedule(AsyncJobType::Flush, true);
    FlashSegmentOffsetT flash_segment_offset =
        co_await curr_zone->asyncAppendSegment(io_executor, logical_segment_id,
                                               std::move(buffer));

    update_fn(logical_segment_id, flash_segment_offset);

    if (curr_zone->appendedBytes(segment_size_byte_) == curr_zone->capacity()) {
      zoneFinishAction(curr_zone->zoneId());
    }
  }

  Task<> flushSegment(uint32_t zone_id,
                      uint32_t logical_segment_id,
                      Buffer buffer,
                      UpdateSegmentIndexFn update_fn) {
    Zone* curr_zone = zns_mgr_.getZone(zone_id);
    curr_zone->allocate(segment_size_byte_);

    auto& io_executor =
        co_await zns_mgr_.asyncSchedule(AsyncJobType::Flush, true);
    FlashSegmentOffsetT flash_segment_offset =
        co_await curr_zone->asyncAppendSegment(io_executor, logical_segment_id,
                                               std::move(buffer));

    update_fn(logical_segment_id, flash_segment_offset);

    if (curr_zone->appendedBytes(segment_size_byte_) == curr_zone->capacity()) {
      zoneFinishAction(curr_zone->zoneId());
    }
  }

  Task<Buffer> readSegment(IOExecutor& io_executor,
                           FlashSegmentOffsetT flash_segment_offset) {
    FlashByteAddressT flash_byte_address =
        getFlashByteAddressFromSegmentOffset(flash_segment_offset);

    return zns_mgr_.asyncRead(io_executor, flash_byte_address,
                              segment_size_byte_);
  }

  Task<uint32_t> getCleanZoneId() {
    co_await allocate_semaphore_.acquire();
    uint32_t remove_zone_id;
    {
      auto clean_lock = std::unique_lock<std::mutex>(clean_mutex_);
      XDCHECK(allocate_zone_id_arr_.size() > 0);

      double max_invalid_rate = 0.0;
      for (auto zone_id : allocate_zone_id_arr_) {
        double curr_invalid_rate =
            calculate_invalid_rate_fn_(zns_mgr_.segmentArr(zone_id));
        // XLOG(INFO, folly::sformat("{}: {}", zone_id, curr_invalid_rate));
        XDCHECK(curr_invalid_rate >= 0);
        if (curr_invalid_rate >= max_invalid_rate) {
          max_invalid_rate = curr_invalid_rate;
          remove_zone_id = zone_id;
        }
      }

      // XLOG(INFO, folly::sformat("pick {}", remove_zone_id));
      std::erase(allocate_zone_id_arr_, remove_zone_id);
    }
    XLOGF(INFO, "Adaptive: garbage collection {}", remove_zone_id);
    auto logical_segment_arr = zns_mgr_.segmentArr(remove_zone_id);

    coro::latch l(logical_segment_arr.size());
    for (auto logical_segment_id : logical_segment_arr) {
      remove_fn_(logical_segment_id, l);
    }

    co_await l;
    co_await zns_mgr_.asyncReset(remove_zone_id);

    co_return remove_zone_id;
  }

  void addExternalZoneNum(uint32_t new_external_zones) {
    if (new_external_zones == 0) {
      return;
    }

    // add free zone immediately
    getFreeZoneFromExternal().detach();

    {
      auto clean_lock = std::unique_lock<std::mutex>(clean_mutex_);
      num_external_zones_ += new_external_zones - 1;
    }
  }

 private:
  const uint64_t segment_size_byte_;
  const CalculateInvalidRateFn calculate_invalid_rate_fn_;
  const AdaptiveReclaimFn reclaim_fn_;
  const AdaptiveRemoveFn remove_fn_;

  coro::semaphore allocate_semaphore_;
  std::deque<uint32_t> allocate_zone_id_arr_;

  uint32_t num_external_zones_;

  void checkIfNeedGarbageCollection() override {
    uint32_t external_sched = 0;
    uint32_t new_gc_sched = 0;
    {
      auto clean_lock = std::unique_lock<std::mutex>(clean_mutex_);

      // minus one since we will allocate immediately
      uint32_t planned_free_zones = free_zone_rb_.size() + reclaim_scheduled_;
      if (planned_free_zones < num_clean_zones_) {
        uint32_t need_schedule = num_clean_zones_ - planned_free_zones;
        external_sched = std::min(need_schedule, num_external_zones_);
        new_gc_sched = need_schedule - external_sched;
        reclaim_scheduled_ += new_gc_sched + external_sched;
      }
    }

    for (uint32_t i = 0; i < external_sched; i++) {
      getFreeZoneFromExternal().detach();
    }

    for (uint32_t i = 0; i < new_gc_sched; i++) {
      scheduleGarbageCollection().detach(); // run gc coroutine
    }
  }

  Task<> garbageCollection() override {
    auto loan_zone_id = co_await zns_mgr_.getFreeZoneFromSet();
    co_await zns_mgr_.asyncChangeZoneType(loan_zone_id, zone_type_);

    co_await allocate_semaphore_.acquire();
    uint32_t reclaim_zone_id;
    {
      auto clean_lock = std::unique_lock<std::mutex>(clean_mutex_);
      XDCHECK(allocate_zone_id_arr_.size() > 0);

      double max_invalid_rate = 0.0;
      for (auto zone_id : allocate_zone_id_arr_) {
        double curr_invalid_rate =
            calculate_invalid_rate_fn_(zns_mgr_.segmentArr(zone_id));
        // XLOG(INFO, folly::sformat("{}: {}", zone_id, curr_invalid_rate));
        XDCHECK(curr_invalid_rate >= 0);
        if (curr_invalid_rate >= max_invalid_rate) {
          max_invalid_rate = curr_invalid_rate;
          reclaim_zone_id = zone_id;
        }
      }

      // XLOG(INFO, folly::sformat("pick {}", reclaim_zone_id));
      std::erase(allocate_zone_id_arr_, reclaim_zone_id);
    }
    XLOGF(INFO, "Adaptive: garbage collection {} copy to {}", reclaim_zone_id, loan_zone_id);

    auto logical_segment_arr = zns_mgr_.segmentArr(reclaim_zone_id);

    coro::latch l(logical_segment_arr.size());
    for (auto logical_segment_id : logical_segment_arr) {
      reclaim_fn_(loan_zone_id, logical_segment_id, l);
    }

    co_await l;
    co_await zns_mgr_.asyncReset(reclaim_zone_id);

    {
      auto clean_lock = std::unique_lock<std::mutex>(clean_mutex_);
      reclaim_scheduled_--;
    }

    auto curr_active_zone = active_zone_.load(std::memory_order_relaxed);
    if (curr_active_zone->zoneId() == reclaim_zone_id) {
      active_zone_.store(nullptr, std::memory_order_relaxed);
    }

    free_zone_rb_.produce(loan_zone_id);    
    zns_mgr_.returnZoneToSet(reclaim_zone_id);
  }

  Task<> getFreeZoneFromExternal() {
    auto free_zone_id = co_await zns_mgr_.getFreeZoneFromSet();
    co_await zns_mgr_.asyncChangeZoneType(free_zone_id, zone_type_);
    free_zone_rb_.produce(free_zone_id);
  }

  void zoneFinishAction(uint32_t zone_id) override {
    {
      auto clean_lock = std::unique_lock<std::mutex>(clean_mutex_);
      allocate_zone_id_arr_.push_back(zone_id);
    }
    allocate_semaphore_.release();
  }

  FlashSegmentOffsetT getFlashSegmentOffsetFromByteAddress(
      FlashByteAddressT flash_byte_address) {
    XDCHECK(flash_byte_address % segment_size_byte_ == 0);
    return flash_byte_address / segment_size_byte_;
  }

  FlashByteAddressT getFlashByteAddressFromSegmentOffset(
      FlashSegmentOffsetT flash_segment_offset) {
    return static_cast<FlashByteAddressT>(flash_segment_offset) *
           segment_size_byte_;
  }
};

} // namespace navy
} // namespace cachelib
} // namespace facebook