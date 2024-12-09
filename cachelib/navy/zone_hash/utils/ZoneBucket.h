#pragma once

#include <folly/Portability.h>

#include <cstdint>

#include "cachelib/common/Hash.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Hash.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/zone_hash/storage/ZoneBucketStorage.h"

namespace facebook {
namespace cachelib {
namespace navy {

// clone from cachelib/navy/bighash/Bucket.h
class FOLLY_PACK_ATTR ZoneBucket {
 public:
  class Iterator {
   public:
    Iterator() = default;

    // return whether the iteration has reached the end
    bool done() const { return itr_.done(); }

    BufferView key() const;
    uint64_t keyHash() const;
    HashedKey hashedKey() const;
    BufferView value() const;

    bool keyEqualsTo(HashedKey hk) const;

   private:
    friend ZoneBucket;

    explicit Iterator(ZoneBucketStorage::Allocation itr) : itr_(itr) {}

    ZoneBucketStorage::Allocation itr_;
  };

  // User will pass in a view that contains the memory that is a Bucket
  static uint32_t computeChecksum(BufferView view);

  // Initialize a brand new Bucket given a piece of memory in the case
  // that the existing bucket is invalid. (I.e. checksum or generation
  // mismatch). Refer to comments at the top on what do we use checksum
  // and generation time for.
  static ZoneBucket& initNew(MutableBufferView view, uint64_t generationTime);

  uint32_t getChecksum() const { return checksum_; }

  void setChecksum(uint32_t checksum) { checksum_ = checksum; }

  // return the generation time of the bucket, if this is mismatch with
  // the one in BigHash data in the bucket is invalid.
  uint64_t generationTime() const { return generationTime_; }

  uint32_t size() const { return storage_.numAllocations(); }

  uint32_t remainingBytes() const { return storage_.remainingCapacity(); }

  // Look up for the value corresponding to a key.
  // BufferView::isNull() == true if not found.
  BufferView find(HashedKey hk) const;

  // return LogBucketEntry BufferView
  BufferView findTag(uint32_t tag) const;

  // Note: this does *not* replace an existing key! User must make sure to
  //       remove an existing key before calling insert.
  //
  // Insert into the bucket. Trigger eviction and invoke @destructorCb if
  // not enough space. Return number of entries evicted.
  uint32_t insert(HashedKey hk,
                  BufferView value,
                  const DestructorCallback& destructorCb);

  // Remove an entry corresponding to the key. If found, invoke @destructorCb
  // before returning true. Return number of entries removed.
  uint32_t remove(HashedKey hk, const DestructorCallback& destructorCb);

  bool isSpace(HashedKey hk, BufferView value);
  ZoneBucketStorage::Allocation allocate(HashedKey hk, BufferView value);
  void insert(ZoneBucketStorage::Allocation alloc,
              HashedKey hk,
              BufferView value);
  void clear();

  // return an iterator of items in the bucket
  Iterator getFirst() const;
  Iterator getNext(Iterator itr) const;

 private:
  ZoneBucket(uint64_t generationTime, uint32_t capacity)
      : generationTime_{generationTime}, storage_{capacity} {}

  // Reserve enough space for @size by evicting. Return number of evictions.
  uint32_t makeSpace(uint32_t size, const DestructorCallback& destructorCb);

  uint32_t checksum_{};
  uint64_t generationTime_{};
  ZoneBucketStorage storage_;
};

namespace details {
// This maps to exactly how an entry is stored in a bucket on device.
class FOLLY_PACK_ATTR LogBucketEntry {
 public:
  static uint32_t computeSize(uint32_t keySize, uint32_t valueSize) {
    return sizeof(LogBucketEntry) + keySize + valueSize;
  }

  // construct the LogBucketEntry with given memory using placement new
  // @param storage  the mutable memory used to create LogBucketEntry
  // @param hk       the item's key and its hash
  // @param value    the item's value
  static LogBucketEntry& create(MutableBufferView storage,
                                HashedKey hk,
                                BufferView value) {
    new (storage.data()) LogBucketEntry{hk, value};
    return reinterpret_cast<LogBucketEntry&>(*storage.data());
  }

  BufferView key() const { return {keySize_, data_}; }

  HashedKey hashedKey() const {
    return HashedKey::precomputed(toStringPiece(key()), keyHash_);
  }

  bool keyEqualsTo(HashedKey hk) const { return hk == hashedKey(); }

  uint64_t keyHash() const { return keyHash_; }

  BufferView value() const { return {valueSize_, data_ + keySize_}; }

 private:
  LogBucketEntry(HashedKey hk, BufferView value)
      : keySize_{static_cast<uint32_t>(hk.key().size())},
        valueSize_{static_cast<uint32_t>(value.size())},
        keyHash_{hk.keyHash()} {
    static_assert(sizeof(LogBucketEntry) == 16, "LogBucketEntry overhead");
    makeView(hk.key()).copyTo(data_);
    value.copyTo(data_ + keySize_);
  }

  const uint32_t keySize_{};
  const uint32_t valueSize_{};
  const uint64_t keyHash_{};
  uint8_t data_[];
};
} // namespace details

} // namespace navy
} // namespace cachelib
} // namespace facebook