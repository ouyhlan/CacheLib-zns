#include "cachelib/navy/zone_hash/storage/ZoneBucketStorage.h"

#include "cachelib/navy/common/Buffer.h"

namespace facebook {
namespace cachelib {
namespace navy {

// This is very simple as it only tries to allocate starting from the
// tail of the storage. Returns null view() if we don't have any more space.
ZoneBucketStorage::Allocation ZoneBucketStorage::allocate(uint32_t size) {
  if (!canAllocate(size)) {
    return {};
  }

  auto* slot = new (data_ + end_offset_) Slot(size);
  end_offset_ += slotSize(size);
  num_allocations_++;

  return {MutableBufferView(slot->size, slot->data), num_allocations_ - 1};
}

void ZoneBucketStorage::remove(Allocation alloc) {
  // Remove triggers a compaction.
  //
  //                         tail
  //  |--------|REMOVED|-----|~~~~|
  //
  // after compaction
  //                  tail
  //  |---------------|~~~~~~~~~~~|
  if (alloc.done()) {
    return;
  }

  const uint32_t removedSize = slotSize(alloc.view().size());
  uint8_t* removed = alloc.view().data() - kAllocationOverhead;
  std::memmove(removed,
               removed + removedSize,
               (data_ + end_offset_) - removed - removedSize);
  end_offset_ -= removedSize;
  num_allocations_--;
}

void ZoneBucketStorage::removeUntil(Allocation alloc) {
  // Remove everything until (and include) "alloc"
  //
  //                         tail
  //  |----------------|-----|~~~~|
  //  ^                ^
  //  begin            offset
  //  remove this whole range
  //
  //        tail
  //  |-----|~~~~~~~~~~~~~~~~~~~~~|
  if (alloc.done()) {
    return;
  }

  uint32_t offset = alloc.view().data() + alloc.view().size() - data_;
  if (offset > end_offset_) {
    return;
  }

  std::memmove(data_, data_ + offset, end_offset_ - offset);
  end_offset_ -= offset;
  num_allocations_ -= alloc.position() + 1;
}

ZoneBucketStorage::Allocation ZoneBucketStorage::getFirst() const {
  if (end_offset_ == 0) {
    return {};
  }
  auto* slot = reinterpret_cast<Slot*>(data_);
  return {MutableBufferView{slot->size, slot->data}, 0};
}

ZoneBucketStorage::Allocation ZoneBucketStorage::getNext(
    ZoneBucketStorage::Allocation alloc) const {
  if (alloc.done()) {
    return {};
  }

  auto* next =
      reinterpret_cast<Slot*>(alloc.view().data() + alloc.view().size());
  if (reinterpret_cast<uint8_t*>(next) - data_ >= end_offset_) {
    return {};
  }
  return {MutableBufferView{next->size, next->data}, alloc.position() + 1};
}

} // namespace navy
} // namespace cachelib
} // namespace facebook