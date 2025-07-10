#include "cachelib/navy/zone_hash/storage/SetAsyncStorageManager.h"

#include <cstdint>
#include <mutex>

#include "cachelib/navy/zone_hash/scheduler/IOScheduler.h"

namespace facebook {
namespace cachelib {
namespace navy {

SetAsyncStorageManager::SetAsyncStorageManager(ZoneManager& zns_mgr,
                                               JobScheduler& scheduler,
                                               ZoneNandType zone_type,
                                               uint32_t initial_num_zones,
                                               uint32_t num_clean_zones,
                                               uint64_t page_size_byte,
                                               SetCleanPageFn clean_fn)
    : ZonedAsyncStorageManager(
          zns_mgr, scheduler, zone_type, num_clean_zones, page_size_byte),
      set_clean_page_fn_(clean_fn),
      num_external_zones_(0) {
  auto acquire_zone_arr = zns_mgr_.allocate(initial_num_zones, zone_type_);
  if (acquire_zone_arr.size() <= num_clean_zones) {
    throw std::invalid_argument(
        folly::sformat("Invalid num of zones because we need at "
                       "least {} zones for the set cache",
                       num_clean_zones + 1));
  }

  XLOG(INFO,
       folly::sformat("SetStorageManager created: need num zones: {}, "
                      "allocated num zones: {}, num clean zones: {}",
                      initial_num_zones,
                      acquire_zone_arr.size(),
                      num_clean_zones_));

  for (uint32_t i = 1; i < acquire_zone_arr.size(); i++) {
    free_zone_rb_.produce(acquire_zone_arr[i]);
  }

  active_zone_ = zns_mgr_.getZone(acquire_zone_arr[0]);
  zns_mgr.registerSetAsyncStorageManager(*this);
}

Task<> SetAsyncStorageManager::asyncPageAppend(SetIdT set_id,
                                               Buffer buffer,
                                               UpdatePageIndexFn update_fn) {
  Zone* append_zone = co_await allocate(page_size_byte_);

  auto& io_executor =
      co_await zns_mgr_.asyncSchedule(AsyncJobType::Flush, true);
  auto flash_page_offset = co_await append_zone->asyncAppendPage(
      io_executor, set_id, std::move(buffer));

  update_fn(
      set_id, flash_page_offset, zns_mgr_.timestamp(append_zone->zoneId()));

  if (append_zone->appendedBytes(page_size_byte_) == append_zone->capacity()) {
    zoneFinishAction(append_zone->zoneId());
  }
}

bool SetAsyncStorageManager::comparePageAge(FlashPageOffsetT a,
                                            FlashPageOffsetT b) {
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

Task<> SetAsyncStorageManager::addFreeZone(uint32_t zone_id) {
  co_await zns_mgr_.asyncChangeZoneType(zone_id, zone_type_);
  free_zone_rb_.produce(zone_id);
}

Task<uint32_t> SetAsyncStorageManager::getFreeZoneId() {
  uint32_t new_sched = 0;
  {
    auto clean_lock = std::unique_lock<std::mutex>(clean_mutex_);

    uint32_t planned_free_zones = free_zone_rb_.size() + reclaim_scheduled_;
    if (planned_free_zones < (num_clean_zones_)) {
      new_sched = num_clean_zones_ - planned_free_zones;
      reclaim_scheduled_ += new_sched;
    }
  }

  for (uint32_t i = 0; i < new_sched; i++) {
    scheduleGarbageCollection().detach(); // run gc coroutine
  }

  uint32_t free_zone_id = co_await free_zone_rb_.consume();
  co_return free_zone_id;
}

void SetAsyncStorageManager::checkIfNeedGarbageCollection() {
  uint32_t external_sched = 0;
  uint32_t new_gc_sched = 0;
  {
    auto clean_lock = std::unique_lock<std::mutex>(clean_mutex_);

    // minus one since we will allocate immediately
    uint32_t planned_free_zones = free_zone_rb_.size() + reclaim_scheduled_;
    if (planned_free_zones < num_clean_zones_) {
      uint32_t need_schedule = num_clean_zones_ - planned_free_zones;
      external_sched = std::min(need_schedule, num_external_zones_);

      if (planned_free_zones == 0) {
        new_gc_sched = 1;
      }
      new_gc_sched = std::max(new_gc_sched, need_schedule - external_sched);
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

Task<> SetAsyncStorageManager::garbageCollection() {
  uint32_t reclaim_zone_id = co_await allocate_zone_rb_.consume();
  XLOGF(INFO, "SET: garbage collection with {}", reclaim_zone_id);

  auto page_arr = zns_mgr_.pageArr(reclaim_zone_id);
  for (auto& [set_id, page_addr] : page_arr) {
    set_clean_page_fn_(set_id, page_addr);
  }

  co_await zns_mgr_.asyncReset(reclaim_zone_id);
  {
    auto clean_lock = std::unique_lock<std::mutex>(clean_mutex_);
    reclaim_scheduled_--;
  }
  free_zone_rb_.produce(reclaim_zone_id);
}

Task<> SetAsyncStorageManager::getFreeZoneFromExternal() {
  auto free_zone_id = co_await zns_mgr_.getFreeZoneFromAdaptive();
  co_await zns_mgr_.asyncChangeZoneType(free_zone_id, zone_type_);
  free_zone_rb_.produce(free_zone_id);
}

} // namespace navy
} // namespace cachelib
} // namespace facebook