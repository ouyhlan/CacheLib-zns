#pragma once

#include <folly/logging/xlog.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <tuple>
#include <vector>

#include "cachelib/common/BloomFilter.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/zone_hash/storage/ZNSDevice.h"
#include "cachelib/navy/zone_hash/storage/Zone.h"
#include "cachelib/navy/zone_hash/utils/Types.h"
#include "cachelib/navy/zone_hash/utils/ZoneBucket.h"

namespace facebook {
namespace cachelib {
namespace navy {

class SetStorageManager;
class AdaptiveStorageManager;

class ZoneManager {
 public:
  ZoneManager(ZNSDevice& device,
              uint64_t page_size_byte,
              uint64_t segment_size_byte);

  Buffer makeIOBuffer(size_t size_byte) const {
    return device_.makeIOBuffer(size_byte);
  }

  std::vector<uint32_t> allocate(uint32_t num_allocate_zones,
                                 ZoneNandType zone_type);

  Buffer readPage(FlashByteAddressT flash_byte_address);

  Buffer readSegment(FlashByteAddressT flash_byte_address);

  void reset(uint32_t zone_id) { zone_arr_[zone_id]->reset(); }

  void flush() { device_.flush(); }

  void changeZoneType(uint32_t zone_id, ZoneNandType zone_type);

  Zone* getZone(uint32_t zone_id) { return zone_arr_[zone_id].get(); }

  std::vector<uint32_t> segmentArr(uint32_t zone_id) {
    std::unique_lock<std::mutex> lock(zone_arr_[zone_id]->mutex_);

    return zone_arr_[zone_id]->segment_arr_;
  }

  std::vector<std::pair<SetIdT, FlashPageOffsetT>> pageArr(uint32_t zone_id) {
    std::unique_lock<std::mutex> lock(zone_arr_[zone_id]->mutex_);

    return zone_arr_[zone_id]->page_arr_;
  }

  uint64_t timestamp(uint32_t zone_id) {
    return zone_arr_[zone_id]->timestamp_.get();
  }

  uint32_t numZones() const { return device_.numZones(); }

  uint64_t zoneSize() const { return device_.getIOZoneSize(); }

  void registerSetStorageManager(SetStorageManager& set_mgr) {
    set_mgr_ = &set_mgr;
  }

  void registerAdaptiveStorageManager(AdaptiveStorageManager& adaptive_mgr) {
    adaptive_mgr_ = &adaptive_mgr;
  }

  auto getFreeZoneFromSet() -> std::tuple<Status, uint32_t>;

  void returnZoneToSet(uint32_t zone_id);

 private:
  ZNSDevice& device_;
  const uint32_t num_zones_;
  const uint64_t page_size_byte_;
  const uint64_t segment_size_byte_;
  std::vector<std::unique_ptr<Zone>> zone_arr_;

  SetStorageManager* set_mgr_;
  AdaptiveStorageManager* adaptive_mgr_;

  bool verifyChecksum(BufferView view);
};

} // namespace navy
} // namespace cachelib
} // namespace facebook