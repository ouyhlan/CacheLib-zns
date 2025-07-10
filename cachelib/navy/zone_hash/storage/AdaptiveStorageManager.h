#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Device.h"
#include "cachelib/navy/scheduler/ThreadPoolJobScheduler.h"
#include "cachelib/navy/zone_hash/storage/ZNSDevice.h"
#include "cachelib/navy/zone_hash/storage/Zone.h"
#include "cachelib/navy/zone_hash/storage/ZoneManager.h"
#include "cachelib/navy/zone_hash/utils/LogSegment.h"
#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

using AdaptiveReclaimFn =
    std::function<Status(uint32_t, uint32_t, Buffer&, LogSegment::Iterator&)>;

class AdaptiveStorageManager {
 public:
  AdaptiveStorageManager(ZoneManager& zns_mgr,
                         ZoneNandType zone_type,
                         uint32_t initial_num_zones,
                         uint32_t num_free_zones,
                         uint64_t segment_size_byte,
                         uint64_t page_size_byte,
                         CalculateInvalidRateFn calculate_fn,
                         AdaptiveReclaimFn reclaim_fn,
                         uint32_t num_threads);

  Buffer makeIOBuffer(size_t size_byte) const {
    return zns_mgr_.makeIOBuffer(size_byte);
  }

  FlashSegmentOffsetT segmentAppend(uint32_t zone_id,
                                    uint32_t logical_segment_id,
                                    Buffer&& buffer);

  void flushSegment(uint32_t logical_segment_id,
                    Buffer&& buffer,
                    std::function<void(FlashSegmentOffsetT)> callback);

  Buffer readSegment(FlashSegmentOffsetT flash_segment_offset);
  Buffer readPage(FlashPageOffsetT flash_page_offset);

 private:
  ZoneManager& zns_mgr_;
  ZoneNandType zone_type_;
  const uint32_t num_free_zones_;
  const uint64_t zone_size_byte_;
  const uint64_t segment_size_byte_;
  const uint64_t page_size_byte_;
  const CalculateInvalidRateFn calculate_invalid_rate_fn_;
  const AdaptiveReclaimFn reclaim_fn_;

  std::atomic<Zone*> active_zone_;

  std::mutex clean_mutex_;
  uint32_t reclaim_scheduled_;
  std::unordered_set<uint32_t> allocate_zone_id_arr_;
  std::list<uint32_t> free_zone_id_arr_;

  ThreadPoolExecutor threads_pool_;

  void checkIfNeedGarbageCollection();

  void garbageCollection();

  bool retrieveFreeZone();

  FlashByteAddressT getZoneStartByteAddress(uint32_t zone_id) {
    return zone_id * zone_size_byte_;
  }

  FlashByteAddressT getFlashByteAddressFromSegmentOffset(
      FlashSegmentOffsetT flash_segment_offset) {
    return static_cast<FlashByteAddressT>(flash_segment_offset) *
           segment_size_byte_;
  }

  FlashByteAddressT getFlashByteAddressFromPageOffset(
      FlashPageOffsetT flash_page_offset) {
    return static_cast<FlashByteAddressT>(flash_page_offset) * page_size_byte_;
  }

  FlashSegmentOffsetT getFlashSegmentOffset(
      FlashByteAddressT flash_byte_address) {
    XDCHECK(flash_byte_address % segment_size_byte_ == 0);
    return flash_byte_address / segment_size_byte_;
  }
};

} // namespace navy
} // namespace cachelib
} // namespace facebook