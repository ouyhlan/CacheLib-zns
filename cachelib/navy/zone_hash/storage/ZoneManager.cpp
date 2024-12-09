#include "cachelib/navy/zone_hash/storage/ZoneManager.h"

#include <folly/Format.h>
#include <folly/logging/xlog.h>

#include <cstdint>
#include <vector>

#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/zone_hash/storage/SetStorageManager.h"
#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

ZoneManager::ZoneManager(ZNSDevice& device,
                         uint64_t page_size_byte,
                         uint64_t segment_size_byte)
    : device_(device),
      num_zones_(device_.numZones()),
      page_size_byte_(page_size_byte),
      segment_size_byte_(segment_size_byte),
      zone_arr_(num_zones_),
      set_mgr_(nullptr),
      adaptive_mgr_(nullptr) {
  for (uint32_t i = 0; i < num_zones_; i++) {
    zone_arr_[i] =
        std::make_unique<Zone>(device, i, page_size_byte_, segment_size_byte_);
  }
}

std::vector<uint32_t> ZoneManager::allocate(uint32_t num_allocate_zones,
                                            ZoneNandType zone_type) {
  std::vector<uint32_t> arr;
  std::vector<uint32_t> zone_need_to_change;
  for (size_t i = 0; i < num_zones_ && num_allocate_zones > 0; i++) {
    if (zone_arr_[i]->tryAcquire()) {
      num_allocate_zones--;
      arr.push_back(zone_arr_[i]->zoneId());

      if (zone_type != zone_arr_[i]->zone_type_) {
        zone_need_to_change.push_back(zone_arr_[i]->zoneId());
      }
    }
  }

  if (zone_type == ZoneNandType::SLC) {
    device_.changeZoneIntoSLC(zone_need_to_change);
  } else if (zone_type == ZoneNandType::QLC) {
    device_.changeZoneIntoQLC(zone_need_to_change);
  }

  for (auto zone_id : zone_need_to_change) {
    zone_arr_[zone_id]->zone_type_ = zone_type;
    zone_arr_[zone_id]->capacity_byte_ = device_.getCapacity(zone_id);
  }

  return arr;
}

Buffer ZoneManager::readPage(FlashByteAddressT flash_byte_address) {
  auto buffer = device_.makeIOBuffer(page_size_byte_);
  XDCHECK(!buffer.isNull());

  bool res = device_.read(flash_byte_address, buffer.size(), buffer.data());

  if (!verifyChecksum(buffer.view())) {
    XLOG(INFO, "Read checksum error!");
    return {};
  }

  return buffer;
}

Buffer ZoneManager::readSegment(FlashByteAddressT flash_byte_address) {
  auto buffer = device_.makeIOBuffer(segment_size_byte_);
  XDCHECK(!buffer.isNull());

  bool res = device_.read(flash_byte_address, buffer.size(), buffer.data());

  if (!verifyChecksum(buffer.view())) {
    XLOG(INFO, "Read checksum error!");
    return {};
  }

  return buffer;
}

void ZoneManager::changeZoneType(uint32_t zone_id, ZoneNandType zone_type) {
  if (zone_type == ZoneNandType::QLC) {
    zone_arr_[zone_id]->changeIntoQLC();
  } else if (zone_type == ZoneNandType::SLC) {
    zone_arr_[zone_id]->changeIntoSLC();
  }
}

auto ZoneManager::getFreeZoneFromSet() -> std::tuple<Status, uint32_t> {
  XDCHECK(set_mgr_ != nullptr);

  uint32_t free_zone_id = set_mgr_->getFreeZoneId();
  return {Status::Ok, free_zone_id};
}

void ZoneManager::returnZoneToSet(uint32_t zone_id) {
  XDCHECK(set_mgr_ != nullptr);

  set_mgr_->addFreeZone(zone_id);
}

bool ZoneManager::verifyChecksum(BufferView view) {
  XDCHECK(view.size() % page_size_byte_ == 0);

  uint64_t num_buckets = view.size() / page_size_byte_;
  for (uint64_t i = 0; i < num_buckets; i++) {
    uint64_t offset = i * page_size_byte_;
    const auto* bucket =
        reinterpret_cast<const ZoneBucket*>(view.data() + offset);
    auto checksum =
        ZoneBucket::computeChecksum({page_size_byte_, view.data() + offset});
    if (checksum != bucket->getChecksum()) {
      XLOG(INFO, "Read page checksum error!");
      return false;
    }
  }

  return true;
}

} // namespace navy
} // namespace cachelib
} // namespace facebook