#include "cachelib/navy/zone_hash/storage/ZonedAsyncStorageManager.h"

#include <atomic>
#include <cstdint>
#include <mutex>

#include "cachelib/navy/scheduler/JobScheduler.h"

namespace facebook {
namespace cachelib {
namespace navy {

ZonedAsyncStorageManager::ZonedAsyncStorageManager(ZoneManager& zns_mgr,
                                                   JobScheduler& scheduler,
                                                   ZoneNandType zone_type,
                                                   uint32_t num_clean_zones,
                                                   uint64_t page_size_byte)
    : zns_mgr_(zns_mgr),
      zone_type_(zone_type),
      zone_capacity_size_(zns_mgr.capacity(zone_type_)),
      num_clean_zones_(num_clean_zones),
      page_size_byte_(page_size_byte),
      zone_size_byte_(zns_mgr.zoneSize()),
      free_zone_exists_(true),
      active_zone_(nullptr),
      reclaim_scheduled_(0),
      scheduler_(scheduler) {}

Buffer ZonedAsyncStorageManager::makeIOBuffer(size_t size_byte) const {
  return zns_mgr_.makeIOBuffer(size_byte);
}

Buffer ZonedAsyncStorageManager::readPage(FlashPageOffsetT flash_page_offset) {
  FlashByteAddressT flash_byte_address =
      getFlashByteAddressFromPageOffset(flash_page_offset);

  return zns_mgr_.readPage(flash_byte_address);
}

// Task<Buffer> ZonedAsyncStorageManager::asyncReadPage(
//     FlashPageOffsetT flash_page_offset) {
//   FlashByteAddressT flash_byte_address =
//       getFlashByteAddressFromPageOffset(flash_page_offset);

//   auto& io_executor = co_await zns_mgr_.asyncSchedule(AsyncJobType::Read);
//   auto buffer = co_await zns_mgr_.asyncRead(
//       io_executor, flash_byte_address, page_size_byte_);
//   co_return buffer;
// }

Task<Buffer> ZonedAsyncStorageManager::asyncReadPage(
    IOExecutor& io_executor, FlashPageOffsetT flash_page_offset) {
  FlashByteAddressT flash_byte_address =
      getFlashByteAddressFromPageOffset(flash_page_offset);

  return zns_mgr_.asyncRead(io_executor, flash_byte_address, page_size_byte_);
}

Task<Zone*> ZonedAsyncStorageManager::allocate(uint32_t size) {
  Zone* curr_zone = nullptr;
  while (true) {
    co_await free_zone_exists_;

    curr_zone = active_zone_.load(std::memory_order_relaxed);
    if ((curr_zone != nullptr) &&
        (curr_zone->allocate(size) + size <= zone_capacity_size_)) {
      break;
    }

    if (retrieve_mutex_.try_lock()) {
      curr_zone = active_zone_.load(std::memory_order_relaxed);
      if ((curr_zone == nullptr) ||
          (curr_zone->allocate(size) + size > zone_capacity_size_)) {
        free_zone_exists_.reset();
        retrieveFreeZone().detach();
      } else {
        retrieve_mutex_.unlock();
      }
    }
  }

  co_return curr_zone;
}

Task<> ZonedAsyncStorageManager::retrieveFreeZone() {
  uint32_t free_zone_id = co_await free_zone_rb_.consume();
  Zone* free_zone = zns_mgr_.getZone(free_zone_id);
  XDCHECK(free_zone->zoneType() == zone_type_);

  active_zone_.store(free_zone, std::memory_order_relaxed);
  retrieve_mutex_.unlock();
  free_zone_exists_.set(scheduler_, JobType::Write);

  checkIfNeedGarbageCollection();
}

void ZonedAsyncStorageManager::checkIfNeedGarbageCollection() {
  uint32_t new_sched = 0;
  {
    auto clean_lock = std::unique_lock<std::mutex>(clean_mutex_);

    // minus one since we will allocate immediately
    uint32_t planned_free_zones = free_zone_rb_.size() + reclaim_scheduled_;
    if (planned_free_zones < num_clean_zones_) {
      new_sched = num_clean_zones_ - planned_free_zones;
      reclaim_scheduled_ += new_sched;
    }
  }

  for (uint32_t i = 0; i < new_sched; i++) {
    scheduleGarbageCollection().detach(); // run gc coroutine
  }
}

Task<> ZonedAsyncStorageManager::scheduleGarbageCollection() {
  co_await zns_mgr_.asyncSchedule(AsyncJobType::Backend, true);
  co_await garbageCollection();
}

void ZonedAsyncStorageManager::zoneFinishAction(uint32_t zone_id) {
  allocate_zone_rb_.produce(zone_id);
}

} // namespace navy
} // namespace cachelib
} // namespace facebook