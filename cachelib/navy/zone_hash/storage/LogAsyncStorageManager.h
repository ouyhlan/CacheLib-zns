#pragma once
#include <cstdint>
#include <functional>

#include "cachelib/navy/zone_hash/scheduler/IOExecutor.h"
#include "cachelib/navy/zone_hash/storage/ZonedAsyncStorageManager.h"
#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

using AsyncCleanSegmentFn = std::function<void(uint32_t, coro::latch&)>;

class LogAsyncStorageManager : public ZonedAsyncStorageManager {
 public:
  LogAsyncStorageManager(ZoneManager& zns_mgr,
                         JobScheduler& scheduler,
                         ZoneNandType zone_type,
                         uint32_t num_zones,
                         uint32_t num_clean_zones,
                         uint64_t segment_size_byte,
                         uint64_t page_size_byte,
                         AsyncCleanSegmentFn clean_fn);

  Task<> flushSegment(uint32_t logical_segment_id,
                      Buffer buffer,
                      UpdateSegmentIndexFn update_fn);

  Task<Buffer> readSegment(IOExecutor& io_executor,
                           FlashSegmentOffsetT flash_segment_offset);

 private:
  const uint64_t segment_size_byte_;
  const AsyncCleanSegmentFn clean_segment_fn_;

  Task<> garbageCollection() override;

  FlashByteAddressT getFlashByteAddressFromSegmentOffset(
      FlashSegmentOffsetT flash_segment_offset) {
    return static_cast<FlashByteAddressT>(flash_segment_offset) *
           segment_size_byte_;
  }
};

} // namespace navy
} // namespace cachelib
} // namespace facebook