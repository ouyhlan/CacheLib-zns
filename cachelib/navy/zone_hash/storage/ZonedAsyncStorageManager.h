#pragma once

#include <folly/Format.h>
#include <folly/logging/xlog.h>

#include <atomic>
#include <coro/coro.hpp>
#include <cstdint>

#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/scheduler/JobScheduler.h"
#include "cachelib/navy/zone_hash/scheduler/Deque.h"
#include "cachelib/navy/zone_hash/scheduler/Event.h"
#include "cachelib/navy/zone_hash/scheduler/IOExecutor.h"
#include "cachelib/navy/zone_hash/storage/Zone.h"
#include "cachelib/navy/zone_hash/storage/ZoneManager.h"
#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

class ZonedAsyncStorageManager {
 public:
  // FIXME: if have more zones, please change this value
  constexpr static uint32_t max_zones_num = 1000;

  ZonedAsyncStorageManager(ZoneManager& zns_mgr,
                           JobScheduler &scheduler,
                           ZoneNandType zone_type,
                           uint32_t num_clean_zones,
                           uint64_t page_size_byte);

  Buffer makeIOBuffer(size_t size_byte) const;

  auto asyncSchedule(AsyncJobType job_type) {
    return zns_mgr_.asyncSchedule(job_type, true);
  }

  Buffer readPage(FlashPageOffsetT flash_page_offset);

  // Task<Buffer> asyncReadPage(FlashPageOffsetT flash_page_offset);

  Task<Buffer> asyncReadPage(IOExecutor& io_executor,
                             FlashPageOffsetT flash_page_offset);

 protected:
  ZoneManager& zns_mgr_;
  ZoneNandType zone_type_;
  uint64_t zone_capacity_size_;
  uint32_t num_clean_zones_;
  const uint64_t page_size_byte_;
  const uint64_t zone_size_byte_;

  Deque<uint32_t> free_zone_rb_;
  Deque<uint32_t> allocate_zone_rb_;

  std::mutex retrieve_mutex_;
  Event free_zone_exists_;
  std::atomic<Zone*> active_zone_;

  std::mutex clean_mutex_;
  uint32_t reclaim_scheduled_;

  JobScheduler &scheduler_;

  Task<Zone*> allocate(uint32_t size);

  Task<> retrieveFreeZone();

  virtual void checkIfNeedGarbageCollection();

  Task<> scheduleGarbageCollection();

  virtual Task<> garbageCollection() = 0;

  virtual void zoneFinishAction(uint32_t zone_id);

  FlashByteAddressT getFlashByteAddressFromPageOffset(
      FlashPageOffsetT flash_page_offset) {
    return static_cast<FlashByteAddressT>(flash_page_offset) * page_size_byte_;
  }
};

} // namespace navy
} // namespace cachelib
} // namespace facebook