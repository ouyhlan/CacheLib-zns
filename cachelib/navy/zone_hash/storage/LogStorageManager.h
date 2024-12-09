#pragma once

#include <folly/SharedMutex.h>

#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/scheduler/ThreadPoolJobScheduler.h"
#include "cachelib/navy/zone_hash/storage/ZNSDevice.h"
#include "cachelib/navy/zone_hash/storage/Zone.h"
#include "cachelib/navy/zone_hash/storage/ZoneManager.h"
#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

// This is designed like following:
// 1 spare zone with circular zone append
class LogStorageManager {
 public:
  // [start_zone, end_zone]
  LogStorageManager(ZoneManager& zns_mgr,
                    ZoneNandType zone_type,
                    uint32_t num_zones,
                    uint32_t num_clean_zones,
                    uint64_t segment_size_byte,
                    uint64_t page_size_byte,
                    uint32_t num_threads,
                    LogCleanSegmentFn clean_fn);

  Buffer makeIOBuffer(size_t size_byte) const {
    return zns_mgr_.makeIOBuffer(size_byte);
  }

  void flushSegment(uint32_t logical_segment_id,
                       Buffer&& buffer,
                       std::function<void(FlashSegmentOffsetT)> callback);

  Buffer readSegment(FlashSegmentOffsetT flash_segment_offset);

  Buffer readPage(FlashPageOffsetT flash_page_offset);

 private:
  ZoneManager& zns_mgr_;
  ZoneNandType zone_type_;
  const uint32_t num_clean_zones_;
  const uint64_t segment_size_byte_;
  const uint64_t page_size_byte_;

  std::atomic<Zone*> active_zone_;

  std::mutex clean_mutex_;
  uint32_t reclaim_scheduled_;
  std::deque<uint32_t> allocate_zone_id_arr_;
  std::deque<uint32_t> free_zone_id_arr_;

  const LogCleanSegmentFn log_clean_segment_fn_;

  ThreadPoolExecutor threads_pool_;

  void garbageCollection();

  bool retrieveFreeZone();

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

  void addChecksumForSegment(MutableBufferView mutable_view);
};

} // namespace navy
} // namespace cachelib
} // namespace facebook