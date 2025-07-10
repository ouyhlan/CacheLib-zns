#pragma once

#include <folly/logging/xlog.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/zone_hash/storage/ZNSDevice.h"
#include "cachelib/navy/zone_hash/storage/Zone.h"
#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

class SetAsyncStorageManager;
class AdaptiveAsyncStorageManager;

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

  Task<Buffer> asyncRead(IOExecutor& io_executor,
                         FlashByteAddressT flash_byte_address,
                         uint32_t size) {
    auto buffer =
        co_await device_.asyncRead(io_executor, flash_byte_address, size);
    if (!verifyChecksum(buffer.view())) {
      XLOG(INFO, "Read checksum error!");
      co_return Buffer{};
    }

    co_return buffer;
  }

  auto asyncSchedule(AsyncJobType job_type, bool need_new_schedule = false) {
    return device_.asyncSchedule(job_type, need_new_schedule);
  }

  Task<> asyncChangeZoneType(uint32_t zone_id, ZoneNandType zone_type) {
    if (zone_type == ZoneNandType::QLC) {
      co_await zone_arr_[zone_id]->asyncChangeIntoQLC();
    } else if (zone_type == ZoneNandType::SLC) {
      co_await zone_arr_[zone_id]->asyncChangeIntoSLC();
    }
  }

  Task<> asyncReset(uint32_t zone_id) {
    co_await zone_arr_[zone_id]->asyncReset();
    co_return;
  }

  void reset(uint32_t zone_id) { zone_arr_[zone_id]->reset(); }

  void flush() { device_.flush(); }

  void changeZoneType(uint32_t zone_id, ZoneNandType zone_type);

  Zone* getZone(uint32_t zone_id) const { return zone_arr_[zone_id].get(); }

  std::vector<uint32_t> segmentArr(uint32_t zone_id) {
    return zone_arr_[zone_id]->segmentArr();
  }

  std::vector<std::pair<SetIdT, FlashPageOffsetT>> pageArr(uint32_t zone_id) {
    std::vector<std::pair<SetIdT, FlashPageOffsetT>> res;
    auto page_arr = zone_arr_[zone_id]->pageArr();
    FlashPageOffsetT page_addr =
        getFlashPageOffsetFromByteAddress(zone_id * zone_size_byte_);
    for (uint64_t i = 0; i < page_arr.size(); i++) {
      res.push_back({page_arr[i], page_addr});
      page_addr++;
    }
    return res;
  }

  uint64_t timestamp(uint32_t zone_id) {
    return zone_arr_[zone_id]->timestamp_.get();
  }

  uint32_t numZones() const { return device_.numZones(); }

  uint64_t zoneSize() const { return device_.getIOZoneSize(); }

  void registerSetAsyncStorageManager(SetAsyncStorageManager& set_mgr) {
    set_mgr_ = &set_mgr;
  }

  void registerAdaptiveAsyncStorageManager(
      AdaptiveAsyncStorageManager& adaptive_mgr) {
    adaptive_mgr_ = &adaptive_mgr;
  }

  // void registerAdaptiveStorageManager(AdaptiveStorageManager& adaptive_mgr) {
  //   adaptive_mgr_ = &adaptive_mgr;
  // }

  Task<uint32_t> getFreeZoneFromSet();

  void returnZoneToSet(uint32_t zone_id);

  Task<uint32_t> getFreeZoneFromAdaptive();

  uint64_t capacity(ZoneNandType zone_type) {
    if (zone_type == ZoneNandType::QLC) {
      return zone_size_byte_;
    } else if (zone_type == ZoneNandType::SLC) {
      return zone_size_byte_ / 4;
    }

    return 0;
  }

 private:
  ZNSDevice& device_;
  const uint32_t num_zones_;
  const uint64_t page_size_byte_;
  const uint64_t segment_size_byte_;
  const uint64_t zone_size_byte_;
  std::vector<std::unique_ptr<Zone>> zone_arr_;

  SetAsyncStorageManager* set_mgr_;
  AdaptiveAsyncStorageManager* adaptive_mgr_;

  bool verifyChecksum(BufferView view);

  FlashPageOffsetT getFlashPageOffsetFromByteAddress(
      FlashByteAddressT flash_byte_address) {
    XDCHECK(flash_byte_address % page_size_byte_ == 0);
    return flash_byte_address / page_size_byte_;
  }
};

} // namespace navy
} // namespace cachelib
} // namespace facebook