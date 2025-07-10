#pragma once

#include <folly/SharedMutex.h>
#include <folly/logging/xlog.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <vector>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/BloomFilter.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/scheduler/ThreadPoolJobScheduler.h"
#include "cachelib/navy/zone_hash/storage/ZNSDevice.h"
#include "cachelib/navy/zone_hash/storage/Zone.h"
#include "cachelib/navy/zone_hash/storage/ZoneManager.h"
#include "cachelib/navy/zone_hash/utils/CuckooHashMap.h"
#include "cachelib/navy/zone_hash/utils/Types.h"
#include "cachelib/navy/zone_hash/utils/ZoneBucket.h"

namespace facebook {
namespace cachelib {
namespace navy {

using SetCleanPageFn = std::function<void(uint32_t, FlashPageOffsetT)>;

class SetStorageManager {
 public:
  SetStorageManager(ZoneManager& zns_mgr,
                    ZoneNandType zone_type,
                    uint32_t initial_num_zones,
                    uint32_t num_clean_zones,
                    uint64_t page_size_byte,
                    uint32_t num_threads,
                    SetCleanPageFn clean_fn);

  Buffer makeIOBuffer(size_t size_byte) const {
    return zns_mgr_.makeIOBuffer(size_byte);
  }

  void pageAppend(SetIdT set_id,
                  Buffer&& buffer,
                  std::function<void(FlashPageOffsetT)> callback);

  Buffer readPage(FlashPageOffsetT flash_page_offset);

  // true if a younger than b
  bool comparePageAge(FlashPageOffsetT a, FlashPageOffsetT b);

  void addFreeZone(uint32_t zone_id) {
    zns_mgr_.changeZoneType(zone_id, zone_type_);

    std::unique_lock<std::mutex> clean_lock(clean_mutex_);
    free_zone_id_arr_.push_back(zone_id);
  }

  uint32_t getFreeZoneId();

 private:
  ZoneManager& zns_mgr_;
  ZoneNandType zone_type_;
  const uint32_t num_clean_zones_;
  const uint64_t zone_size_byte_;
  const uint64_t page_size_byte_;

  std::mutex append_mutex_;
  std::atomic<Zone*> active_zone_;

  std::mutex clean_mutex_;
  uint32_t reclaim_scheduled_;
  std::list<uint32_t> allocate_zone_id_arr_;
  std::list<uint32_t> free_zone_id_arr_;

  ThreadPoolExecutor threads_pool_;

  const SetCleanPageFn set_clean_page_fn_;

  bool retrieveFreeZone();

  void checkIfNeedGarbageCollection();

  void garbageCollection();

  uint32_t getZoneIdFromPageOffset(FlashPageOffsetT flash_page_offset) {
    return flash_page_offset / (zone_size_byte_ / page_size_byte_);
  }

  FlashPageOffsetT getZoneStartPageOffset(uint32_t zone_id) {
    return zone_id * (zone_size_byte_ / page_size_byte_);
  }

  FlashByteAddressT getFlashByteAddressFromPageOffset(
      FlashPageOffsetT flash_page_offset) {
    return static_cast<FlashByteAddressT>(flash_page_offset) * page_size_byte_;
  }

  FlashPageOffsetT getFlashPageOffsetFromByteAddress(
      FlashByteAddressT flash_byte_address) {
    XDCHECK(flash_byte_address % page_size_byte_ == 0);
    return flash_byte_address / page_size_byte_;
  }
};

} // namespace navy
} // namespace cachelib
} // namespace facebook