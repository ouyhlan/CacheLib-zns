#pragma once

#include <folly/Format.h>
#include <folly/logging/xlog.h>

#include <cstdint>
#include <mutex>
#include <stdexcept>
#include <utility>
#include <vector>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/zone_hash/storage/ZNSDevice.h"
#include "cachelib/navy/zone_hash/utils/Types.h"
#include "cachelib/navy/zone_hash/utils/ZoneBucket.h"

namespace facebook {
namespace cachelib {
namespace navy {

class ZoneManager;

class Zone {
  friend class ZoneManager;

 public:
  Zone(ZNSDevice& device,
       uint32_t zone_id,
       uint64_t page_size_byte,
       uint64_t segment_size_byte);

  auto appendPage(SetIdT set_id, Buffer& buffer)
      -> std::tuple<bool, FlashByteAddressT>;

  auto appendSegment(uint32_t logical_segment_id, Buffer& buffer)
      -> std::tuple<bool, FlashByteAddressT>;

  void changeIntoSLC();

  void changeIntoQLC();

  void reset();

  bool tryAcquire() { return !busy_.test_and_set(std::memory_order_acq_rel); }

  void acquire() {
    while (busy_.test_and_set(std::memory_order_acq_rel))
      ;
  }

  void release() { busy_.clear(std::memory_order_release); }

  bool haveFreeSpace() {
    return last_entry_end_byte_offset_.get() < capacity_byte_;
  }

  uint32_t zoneId() const { return zone_id_; }

  ZoneNandType zoneType() const { return zone_type_; }

 private:
  ZNSDevice& device_;
  const uint64_t page_size_byte_;
  const uint64_t segment_size_byte_;
  const uint32_t zone_id_;
  const uint64_t zone_size_byte_;
  AtomicCounter last_entry_end_byte_offset_;

  std::atomic_flag busy_;
  AtomicCounter timestamp_;
  uint64_t capacity_byte_;
  ZoneNandType zone_type_;

  // metadata
  std::mutex mutex_;
  std::vector<uint32_t> segment_arr_;
  std::vector<std::pair<SetIdT, FlashPageOffsetT>> page_arr_;

  static AtomicIncrementCounter curr_timestamp_;

  FlashByteAddressT getZoneStartByteAddress() {
    return zone_id_ * zone_size_byte_;
  }

  void addChecksum(MutableBufferView mutable_view);
};

} // namespace navy
} // namespace cachelib
} // namespace facebook