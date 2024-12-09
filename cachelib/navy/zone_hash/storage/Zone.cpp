#include "cachelib/navy/zone_hash/storage/Zone.h"

#include <cstdint>
#include <mutex>

#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

AtomicIncrementCounter Zone::curr_timestamp_(UINT32_MAX, 1);

Zone::Zone(ZNSDevice& device,
           uint32_t zone_id,
           uint64_t page_size_byte,
           uint64_t segment_size_byte)
    : device_(device),
      page_size_byte_(page_size_byte),
      segment_size_byte_(segment_size_byte),
      zone_id_(zone_id),
      zone_size_byte_(device_.getIOZoneSize()),
      last_entry_end_byte_offset_(0),
      busy_(false),
      capacity_byte_(device_.getCapacity(zone_id_)),
      zone_type_(zone_size_byte_ == capacity_byte_ ? ZoneNandType::QLC
                                                   : ZoneNandType::SLC) {}

auto Zone::appendPage(SetIdT set_id, Buffer& buffer)
    -> std::tuple<bool, FlashByteAddressT> {
  XDCHECK(buffer.size() == page_size_byte_);

  uint64_t next_entry_byte_offset_ = last_entry_end_byte_offset_.add_fetch(page_size_byte_);
  if (next_entry_byte_offset_ == page_size_byte_) {
    // first append page
    timestamp_.set(curr_timestamp_.get());
  } else if (next_entry_byte_offset_ > capacity_byte_) {
    return {false, 0};
  } 
  

  addChecksum(buffer.mutableView());

  FlashByteAddressT append_byte_address = device_.append(zone_id_, buffer);

  std::unique_lock<std::mutex> lock(mutex_);
  page_arr_.push_back({set_id, append_byte_address});
  return {true, append_byte_address};
}

auto Zone::appendSegment(uint32_t logical_segment_id, Buffer& buffer)
    -> std::tuple<bool, FlashByteAddressT> {
  XDCHECK(buffer.size() == segment_size_byte_);

  if (last_entry_end_byte_offset_.add_fetch(segment_size_byte_) >
      capacity_byte_) {
    return {false, 0};
  }

  addChecksum(buffer.mutableView());
  FlashByteAddressT append_byte_address = device_.append(zone_id_, buffer);

  std::unique_lock<std::mutex> lock(mutex_);
  segment_arr_.push_back(logical_segment_id);
  return {true, append_byte_address};
}

void Zone::changeIntoSLC() {
  if (last_entry_end_byte_offset_.get() != 0) {
    throw std::runtime_error("Cannot change non-empty zone!");
  }

  if (zone_type_ != ZoneNandType::SLC) {
    device_.changeZoneIntoSLC(zone_id_);
    zone_type_ = ZoneNandType::SLC;
    capacity_byte_ = device_.getCapacity(zone_id_);
  }
}

void Zone::changeIntoQLC() {
  if (last_entry_end_byte_offset_.get() != 0) {
    throw std::runtime_error("Cannot change non-empty zone!");
  }

  if (zone_type_ != ZoneNandType::QLC) {
    device_.changeZoneIntoQLC(zone_id_);
    zone_type_ = ZoneNandType::QLC;
    capacity_byte_ = device_.getCapacity(zone_id_);
  }
}

void Zone::reset() {
  {
    std::unique_lock<std::mutex> lock(mutex_);
    segment_arr_.clear();
    page_arr_.clear();
  }

  device_.reset(getZoneStartByteAddress(), zone_size_byte_);
  last_entry_end_byte_offset_.set(0);
}

void Zone::addChecksum(MutableBufferView mutable_view) {
  XDCHECK(mutable_view.size() % page_size_byte_ == 0);

  uint64_t num_buckets = mutable_view.size() / page_size_byte_;
  for (uint64_t i = 0; i < num_buckets; i++) {
    uint64_t offset = i * page_size_byte_;
    auto* bucket = reinterpret_cast<ZoneBucket*>(mutable_view.data() + offset);
    auto checksum = ZoneBucket::computeChecksum(
        {page_size_byte_, mutable_view.data() + offset});
    bucket->setChecksum(checksum);
  }
}

} // namespace navy
} // namespace cachelib
} // namespace facebook