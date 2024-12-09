#pragma once

#include <folly/SharedMutex.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "SegmentIndexEntry.h"
#include "cachelib/common/Hash.h"
#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

class SegmentIndex {
 public:
  class BucketIterator {
   public:
    BucketIterator() : end_(true) {}

    bool done() const { return end_; }

    SetIdT setid() const { return set_id_; }

    uint32_t tag() const { return snapshot_vec_[snapshot_offset_].tag; }

    uint32_t hits() const { return snapshot_vec_[snapshot_offset_].hits; }

    LogicalPageOffset logical_page_offset() const {
      return snapshot_vec_[snapshot_offset_].logicalPageOffset();
    }

    uint64_t countBucket() {
      return snapshot_vec_.size();
    }

    BucketIterator& operator++() {
      snapshot_offset_++;
      if (snapshot_offset_ >= snapshot_vec_.size()) {
        end_ = true;
      }

      return *this;
    }

   private:
    friend SegmentIndex;

    struct LockDeleter {
      void operator()(std::mutex* l) const { l->unlock(); }
    };

    using LockManager = std::unique_ptr<std::mutex, LockDeleter>;

    bool end_;
    SetIdT set_id_;
    uint32_t snapshot_offset_;
    const std::vector<SegmentIndexEntry> snapshot_vec_;
    LockManager lock_manager_;

    BucketIterator(SetIdT id,
                   std::vector<SegmentIndexEntry>&& snapshot_vec,
                   std::mutex& lock)
        : set_id_(id),
          snapshot_offset_(0),
          snapshot_vec_(std::move(snapshot_vec)),
          end_(false),
          lock_manager_(&lock) {
      if (snapshot_offset_ == snapshot_vec_.size()) {
        end_ = true;
      }
    }
  };

  explicit SegmentIndex(uint64_t num_buckets_per_partition,
                        uint16_t allocation_size,
                        SetIdFunctionT setid_fn);
  ~SegmentIndex();

  SegmentIndex(const SegmentIndex&) = delete;
  SegmentIndex& operator=(const SegmentIndex&) = delete;

  std::tuple<Status, LogicalPageOffset, uint32_t> lookup(HashedKey hk,
                                                         bool hit);

  Status insert(HashedKey hk, LogicalPageOffset logical_page_offset);

  Status insert(uint32_t tag,
                SetIdT set_id,
                LogicalPageOffset logical_page_offset);

  Status remove(HashedKey hk, LogicalPageOffset logical_page_offset);

  Status remove(uint32_t tag,
                SetIdT set_id,
                LogicalPageOffset logical_page_offset);


  BucketIterator getHashBucketIterator(HashedKey hk);

 private:
  const uint64_t num_mutexes_;
  const uint64_t num_buckets_;
  const uint16_t allocation_size_; // default 1024
  const SetIdFunctionT setid_fn_;

  std::unique_ptr<folly::SharedMutex[]> mutexes_;
  std::vector<uint16_t> bucket_head_index_;

  folly::SharedMutex allocation_mutex_;

  std::unique_ptr<std::mutex[]> iter_mutexes_;

  const uint16_t null_entry_offset_;
  uint16_t max_slot_used_;
  uint16_t next_empty_;
  uint16_t num_allocations_;
  std::vector<SegmentIndexEntry*> allocation_arr_;

  void allocate();

  Status removeFromBucketId(uint32_t tag,
                            SegmentIndexBucketIdT bucket_id,
                            LogicalPageOffset logical_page_offset);

  folly::SharedMutex& getMutex(SegmentIndexBucketIdT bucket_id) const {
    return mutexes_[bucket_id % num_mutexes_];
  }

  std::mutex& getIterMutex(SegmentIndexBucketIdT bucket_id) const {
    return iter_mutexes_[bucket_id % num_mutexes_];
  }

  SegmentIndexBucketIdT getBucketId(SetIdT set_id) {
    return set_id % num_buckets_;
  }

  SegmentIndexBucketIdT getBucketId(HashedKey hk) {
    return getBucketId(setid_fn_(hk.keyHash()));
  }

  // Entry related function
  std::tuple<SegmentIndexEntry*, uint16_t> allocateEntry();

  SegmentIndexEntry* findEntry(uint16_t offset);
  SegmentIndexEntry* findEntryNoLock(uint16_t offset);

  uint16_t releaseEntry(uint16_t offset);
};

} // namespace navy
} // namespace cachelib
} // namespace facebook