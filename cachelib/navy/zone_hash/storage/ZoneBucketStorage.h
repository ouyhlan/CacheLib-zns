#pragma once

#include <cstdint>

#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/CompilerUtils.h"

namespace facebook {
namespace cachelib {
namespace navy {

// clone from cachelib/navy/bighash/BucketStorage.h
class FOLLY_PACK_ATTR ZoneBucketStorage {
 public:
  class Allocation {
   public:
    Allocation() = default;

    // indicate if the end of storage is reached.
    bool done() const { return view_.isNull(); }

    // return a mutable view where caller can read or modify data
    MutableBufferView view() const { return view_; }

    BufferView buffer_view() const { return BufferView{view_.size(), view_.data()}; }

    // return the index of this allocation in the BucketStorage
    uint32_t position() const { return position_; }

   private:
    friend ZoneBucketStorage;

    Allocation(MutableBufferView v, uint32_t p) : view_(v), position_(p) {}

    MutableBufferView view_;
    uint32_t position_;
  };

  static uint32_t slotSize(uint32_t size) { return kAllocationOverhead + size; }

  // construct a ZoneBucketStorage with given capacity, a placement new is
  // required.
  explicit ZoneBucketStorage(uint32_t capacity) : capacity_(capacity) {}

  // allocate a space under this bucket storage
  // @param size  the required size for the space
  // @return      an Allocation for the allocated space, empty Allocation is
  //              returned if remaining space is not enough
  Allocation allocate(uint32_t size);

  uint32_t capacity() const { return capacity_; }

  uint32_t remainingCapacity() const { return capacity_ - end_offset_; }

  uint32_t numAllocations() const { return num_allocations_; }

  void clear() {
    end_offset_ = 0;
    num_allocations_ = 0;
  }

  // remove the give allocation in the bucket storage.
  void remove(Allocation alloc);

  // Removes every single allocation from the beginning, including this one.
  void removeUntil(Allocation alloc);

  // iterate the storage using Allocation
  Allocation getFirst() const;
  Allocation getNext(Allocation alloc) const;

 private:
  const uint32_t capacity_ = 0;
  uint32_t num_allocations_ = 0;
  uint32_t end_offset_ = 0;
  mutable uint8_t data_[];

  struct FOLLY_PACK_ATTR Slot {
    uint32_t size;
    uint8_t data[];
    explicit Slot(uint32_t s) : size(s) {}
  };

  static constexpr uint32_t kAllocationOverhead = sizeof(Slot);

  bool canAllocate(uint32_t size) const {
    return (static_cast<uint64_t>(end_offset_) + slotSize(size)) <= capacity_;
  }
};

} // namespace navy
} // namespace cachelib
} // namespace facebook