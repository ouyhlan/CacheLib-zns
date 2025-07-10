#include "cachelib/navy/zone_hash/storage/LogAsyncStorageManager.h"

#include "cachelib/navy/zone_hash/scheduler/IOScheduler.h"
#include "coro/latch.hpp"

namespace facebook {
namespace cachelib {
namespace navy {

LogAsyncStorageManager::LogAsyncStorageManager(ZoneManager& zns_mgr,
                                               JobScheduler& scheduler,
                                               ZoneNandType zone_type,
                                               uint32_t num_zones,
                                               uint32_t num_clean_zones,
                                               uint64_t segment_size_byte,
                                               uint64_t page_size_byte,
                                               AsyncCleanSegmentFn clean_fn)
    : ZonedAsyncStorageManager(
          zns_mgr, scheduler, zone_type, num_clean_zones, page_size_byte),
      segment_size_byte_(segment_size_byte),
      clean_segment_fn_(clean_fn) {
  auto acquire_zone_arr = zns_mgr_.allocate(num_zones, zone_type_);
  if (acquire_zone_arr.size() <= num_clean_zones) {
    throw std::invalid_argument(
        folly::sformat("Invalid num of zones because we need at "
                       "least {} zones for the set cache",
                       num_clean_zones + 1));
  }

  XLOG(INFO,
       folly::sformat("LogAsyncStorageManager created: num zones: {}, num "
                      "clean zones: {}",
                      acquire_zone_arr.size(),
                      num_clean_zones_));

  for (uint32_t i = 1; i < acquire_zone_arr.size(); i++) {
    free_zone_rb_.produce(acquire_zone_arr[i]);
  }

  active_zone_ = zns_mgr_.getZone(acquire_zone_arr[0]);
}

Task<> LogAsyncStorageManager::flushSegment(uint32_t logical_segment_id,
                                            Buffer buffer,
                                            UpdateSegmentIndexFn update_fn) {
  Zone* append_zone = co_await allocate(segment_size_byte_);

  auto& io_executor =
      co_await zns_mgr_.asyncSchedule(AsyncJobType::Flush, true);
  FlashSegmentOffsetT flash_segment_offset =
      co_await append_zone->asyncAppendSegment(io_executor, logical_segment_id,
                                               std::move(buffer));

  update_fn(logical_segment_id, flash_segment_offset);

  if (append_zone->appendedBytes(segment_size_byte_) ==
      append_zone->capacity()) {
    zoneFinishAction(append_zone->zoneId());
  }
}

Task<Buffer> LogAsyncStorageManager::readSegment(
    IOExecutor& io_executor, FlashSegmentOffsetT flash_segment_offset) {
  FlashByteAddressT flash_byte_address =
      getFlashByteAddressFromSegmentOffset(flash_segment_offset);

  return zns_mgr_.asyncRead(io_executor, flash_byte_address,
                            segment_size_byte_);
}

Task<> LogAsyncStorageManager::garbageCollection() {
  uint32_t reclaim_zone_id = co_await allocate_zone_rb_.consume();

  auto logical_segment_arr = zns_mgr_.segmentArr(reclaim_zone_id);

  coro::latch l(logical_segment_arr.size());

  for (auto logical_segment_id : logical_segment_arr) {
    clean_segment_fn_(logical_segment_id, l);
  }

  co_await l;
  co_await zns_mgr_.asyncReset(reclaim_zone_id);
  {
    auto clean_lock = std::unique_lock<std::mutex>(clean_mutex_);
    reclaim_scheduled_--;
  }
  free_zone_rb_.produce(reclaim_zone_id);
}

} // namespace navy
} // namespace cachelib
} // namespace facebook