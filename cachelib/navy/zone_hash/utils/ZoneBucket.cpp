

#include "cachelib/navy/zone_hash/utils/ZoneBucket.h"

#include <folly/Format.h>
#include <folly/logging/xlog.h>

#include <stdexcept>

#include "cachelib/common/Hash.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/zone_hash/storage/ZoneBucketStorage.h"
#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

namespace {
const details::LogBucketEntry* getIteratorEntry(
    ZoneBucketStorage::Allocation itr) {
  return reinterpret_cast<const details::LogBucketEntry*>(itr.view().data());
}
} // namespace

BufferView ZoneBucket::Iterator::key() const {
  return getIteratorEntry(itr_)->key();
}

uint64_t ZoneBucket::Iterator::keyHash() const {
  return getIteratorEntry(itr_)->keyHash();
}

HashedKey ZoneBucket::Iterator::hashedKey() const {
  return getIteratorEntry(itr_)->hashedKey();
}

BufferView ZoneBucket::Iterator::value() const {
  return getIteratorEntry(itr_)->value();
}

bool ZoneBucket::Iterator::keyEqualsTo(HashedKey hk) const {
  return getIteratorEntry(itr_)->keyEqualsTo(hk);
}

uint32_t ZoneBucket::computeChecksum(BufferView view) {
  constexpr auto kChecksumStart = sizeof(checksum_);
  auto data = view.slice(kChecksumStart, view.size() - kChecksumStart);
  return navy::checksum(data);
}

ZoneBucket& ZoneBucket::initNew(MutableBufferView view,
                                uint64_t generationTime) {
  return *new (view.data())
      ZoneBucket(generationTime, view.size() - sizeof(ZoneBucket));
}

BufferView ZoneBucket::find(HashedKey hk) const {
  auto itr = storage_.getFirst();
  while (!itr.done()) {
    auto* entry = getIteratorEntry(itr);
    if (entry->keyEqualsTo(hk)) {
      return entry->value();
    }
    itr = storage_.getNext(itr);
  }
  return {};
}

BufferView ZoneBucket::findTag(uint32_t tag) const {
  auto itr = storage_.getFirst();
  while (!itr.done()) {
    auto* entry = getIteratorEntry(itr);
    if (createTag(entry->hashedKey()) == tag) {
      return itr.buffer_view();
    }
    itr = storage_.getNext(itr);
  }
  return {};
}

uint32_t ZoneBucket::insert(HashedKey hk,
                            BufferView value,
                            const DestructorCallback& destructorCb) {
  const auto size =
      details::LogBucketEntry::computeSize(hk.key().size(), value.size());
  XDCHECK_LE(size, storage_.capacity());

  const auto evictions = makeSpace(size, destructorCb);
  auto alloc = storage_.allocate(size);
  XDCHECK(!alloc.done());
  details::LogBucketEntry::create(alloc.view(), hk, value);

  return evictions;
}

uint32_t ZoneBucket::makeSpace(uint32_t size,
                               const DestructorCallback& destructorCb) {
  const auto requiredSize = ZoneBucketStorage::slotSize(size);
  XDCHECK_LE(requiredSize, storage_.capacity());

  auto curFreeSpace = storage_.remainingCapacity();
  if (curFreeSpace >= requiredSize) {
    return 0;
  }

  uint32_t evictions = 0;
  auto itr = storage_.getFirst();
  while (true) {
    evictions++;

    if (destructorCb) {
      auto* entry = getIteratorEntry(itr);
      destructorCb(entry->hashedKey(), entry->value(),
                   DestructorEvent::Recycled);
    }

    curFreeSpace += ZoneBucketStorage::slotSize(itr.view().size());
    if (curFreeSpace >= requiredSize) {
      storage_.removeUntil(itr);
      break;
    }
    itr = storage_.getNext(itr);
    XDCHECK(!itr.done());
  }
  return evictions;
}

uint32_t ZoneBucket::remove(HashedKey hk,
                            const DestructorCallback& destructorCb) {
  auto itr = storage_.getFirst();
  while (!itr.done()) {
    auto* entry = getIteratorEntry(itr);
    if (entry->keyEqualsTo(hk)) {
      if (destructorCb) {
        destructorCb(entry->hashedKey(), entry->value(),
                     DestructorEvent::Removed);
      }
      storage_.remove(itr);
      return 1;
    }
    itr = storage_.getNext(itr);
  }
  return 0;
}

bool ZoneBucket::isSpace(HashedKey hk, BufferView value) {
  const auto size =
      details::LogBucketEntry::computeSize(hk.key().size(), value.size());
  const auto required_size = ZoneBucketStorage::slotSize(size);
  XDCHECK_LE(required_size, storage_.capacity());

  auto curr_free_space = storage_.remainingCapacity();
  return curr_free_space >= required_size;
}

ZoneBucketStorage::Allocation ZoneBucket::allocate(HashedKey hk,
                                                   BufferView value) {
  const auto size =
      details::LogBucketEntry::computeSize(hk.key().size(), value.size());
  XDCHECK_LE(size, storage_.remainingCapacity());

  auto alloc = storage_.allocate(size);
  XDCHECK(!alloc.done());

  return alloc;
}

void ZoneBucket::insert(ZoneBucketStorage::Allocation alloc,
                        HashedKey hk,
                        BufferView value) {
  XDCHECK(!alloc.done());
  details::LogBucketEntry::create(alloc.view(), hk, value);
}

void ZoneBucket::clear() { storage_.clear(); }

ZoneBucket::Iterator ZoneBucket::getFirst() const {
  return Iterator{storage_.getFirst()};
}

ZoneBucket::Iterator ZoneBucket::getNext(Iterator itr) const {
  return Iterator{storage_.getNext(itr.itr_)};
}

} // namespace navy
} // namespace cachelib
} // namespace facebook