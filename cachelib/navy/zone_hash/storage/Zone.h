#pragma once

#include <folly/Format.h>
#include <folly/logging/xlog.h>

#include <cstdint>
#include <vector>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/zone_hash/scheduler/IOExecutor.h"
#include "cachelib/navy/zone_hash/storage/ZNSDevice.h"
#include "cachelib/navy/zone_hash/utils/Types.h"

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

  auto appendPage(SetIdT set_id,
                  Buffer& buffer) -> std::tuple<bool, FlashByteAddressT>;

  auto appendSegment(uint32_t logical_segment_id,
                     Buffer& buffer) -> std::tuple<bool, FlashByteAddressT>;

  // return the begin address of the zone
  uint64_t allocate(uint64_t size) {
    uint64_t curr_zone_wp_ = last_entry_end_byte_offset_.fetch_add(size);
    if (size == page_size_byte_ && curr_zone_wp_ == 0) {
      // first append page
      timestamp_.set(curr_timestamp_.get());
    }

    return curr_zone_wp_;
  }

  uint64_t appendedBytes(uint64_t size) {
    return appended_byte_.add_fetch(size);
  }

  Task<FlashPageOffsetT> asyncAppendPage(IOExecutor& io_executor,
                                         SetIdT set_id,
                                         Buffer buffer) {
    XDCHECK(buffer.size() == page_size_byte_);

    addChecksum(buffer.mutableView());

    FlashByteAddressT append_byte_address =
        co_await device_.asyncAppend(io_executor, zone_id_, std::move(buffer));
    uint32_t index = getPageArrIndex(append_byte_address);
    page_arr_[index] = set_id;

    co_return getFlashPageOffsetFromByteAddress(append_byte_address);
  }

  Task<FlashSegmentOffsetT> asyncAppendSegment(IOExecutor& io_executor,
                                               uint32_t logical_segment_id,
                                               Buffer buffer) {
    XDCHECK(buffer.size() == segment_size_byte_);

    addChecksum(buffer.mutableView());

    FlashByteAddressT append_byte_address =
        co_await device_.asyncAppend(io_executor, zone_id_, std::move(buffer));

    uint32_t index = getSegmentArrIndex(append_byte_address);
    segment_arr_[index] = logical_segment_id;

    co_return getFlashSegmentOffsetFromByteAddress(append_byte_address);
  }

  Task<> asyncReset() {
    co_await device_.asyncReset(zone_id_);
    last_entry_end_byte_offset_.set(0);
    appended_byte_.set(0);
  }

  Task<> asyncChangeIntoSLC() {
    if (last_entry_end_byte_offset_.get() != 0) {
      throw std::runtime_error(
          folly::sformat("Cannot change non-empty zone! {}",
                         last_entry_end_byte_offset_.get()));
    }

    if (zone_type_ != ZoneNandType::SLC) {
      co_await device_.asyncChangeZoneIntoSLC(zone_id_);
      zone_type_ = ZoneNandType::SLC;
      capacity_byte_ = device_.getCapacity(zone_id_);
    }
  }

  Task<> asyncChangeIntoQLC() {
    if (last_entry_end_byte_offset_.get() != 0) {
      throw std::runtime_error(
          folly::sformat("Cannot change non-empty zone! {}",
                         last_entry_end_byte_offset_.get()));
    }

    if (zone_type_ != ZoneNandType::QLC) {
      co_await device_.asyncChangeZoneIntoQLC(zone_id_);
      zone_type_ = ZoneNandType::QLC;
      capacity_byte_ = device_.getCapacity(zone_id_);
    }
  }

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

  std::vector<uint32_t> segmentArr() {
    auto res = segment_arr_;
    res.resize(capacity_byte_ / segment_size_byte_);
    return res;
  }

  std::vector<SetIdT> pageArr() {
    auto res = page_arr_;
    res.resize(capacity_byte_ / page_size_byte_);
    return res;
  }

  uint32_t zoneId() const { return zone_id_; }

  ZoneNandType zoneType() const { return zone_type_; }

  uint64_t capacity() const { return capacity_byte_; }

 private:
  ZNSDevice& device_;
  const uint64_t page_size_byte_;
  const uint64_t segment_size_byte_;
  const uint32_t zone_id_;
  const uint64_t zone_size_byte_;
  AtomicCounter last_entry_end_byte_offset_;
  AtomicCounter appended_byte_;

  std::atomic_flag busy_;
  AtomicCounter timestamp_;
  uint64_t capacity_byte_;
  ZoneNandType zone_type_;

  // metadata
  std::vector<uint32_t> segment_arr_;
  std::vector<SetIdT> page_arr_;

  static AtomicIncrementCounter curr_timestamp_;

  FlashByteAddressT getZoneStartByteAddress() {
    return zone_id_ * zone_size_byte_;
  }

  uint32_t getPageArrIndex(FlashByteAddressT flash_byte_address) {
    return (flash_byte_address % zone_size_byte_) / page_size_byte_;
  }

  uint32_t getSegmentArrIndex(FlashByteAddressT flash_byte_address) {
    return (flash_byte_address % zone_size_byte_) / segment_size_byte_;
  }

  FlashPageOffsetT getFlashPageOffsetFromByteAddress(
      FlashByteAddressT flash_byte_address) {
    XDCHECK(flash_byte_address % page_size_byte_ == 0);
    return flash_byte_address / page_size_byte_;
  }

  FlashSegmentOffsetT getFlashSegmentOffsetFromByteAddress(
      FlashByteAddressT flash_byte_address) {
    XDCHECK(flash_byte_address % segment_size_byte_ == 0);
    return flash_byte_address / segment_size_byte_;
  }

  void addChecksum(MutableBufferView mutable_view);
};

} // namespace navy
} // namespace cachelib
} // namespace facebook