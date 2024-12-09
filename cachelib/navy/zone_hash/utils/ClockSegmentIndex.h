#pragma once

#include <folly/SharedMutex.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <vector>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/Hash.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/zone_hash/utils/ClockPolicy.h"
#include "cachelib/navy/zone_hash/utils/ClockSegmentIndexEntry.h"
#include "cachelib/navy/zone_hash/utils/ReuseDistanceAdmissionPolicy.h"
#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

class ClockSegmentIndex {
  using Entry = ClockSegmentIndexEntry;

 public:
  explicit ClockSegmentIndex(uint64_t num_buckets_per_partition,
                             uint16_t allocation_size,
                             uint64_t hot_data_threshold,
                             uint32_t bf_num_hashes,
                             uint32_t bf_bucket_bytes,
                             SetIdFunctionT setid_fn,
                             CollectValidFn collect_fn);
  ~ClockSegmentIndex();

  ClockSegmentIndex(const ClockSegmentIndex&) = delete;
  ClockSegmentIndex& operator=(const ClockSegmentIndex&) = delete;

  void track(HashedKey hk);

  bool admissionTest(HashedKey hk);

  bool determineEviction(HashedKey hk);

  auto lookup(HashedKey hk, bool hit) -> std::tuple<Status, LogicalPageOffset>;

  Status insert(HashedKey hk,
                size_t size,
                LogicalPageOffset logical_page_offset);

  Status readmit(HashedKey hk,
                 size_t size,
                 LogicalPageOffset logical_page_offset);

  Status remove(HashedKey hk);

  Status remove(HashedKey hk, LogicalPageOffset logical_page_offset);

  Status remove(uint32_t tag,
                SetIdT set_id,
                LogicalPageOffset logical_page_offset);

 private:
  const uint64_t num_mutexes_;
  const uint64_t num_buckets_;
  const uint16_t allocation_size_;
  const uint16_t null_entry_offset_;
  const SetIdFunctionT setid_fn_;
  const CollectValidFn collect_valid_fn_;
  ReuseDistanceAdmissonPolicy admission_policy_;
  ClockPolicy eviction_policy_;

  std::unique_ptr<folly::SharedMutex[]> mutexes_;
  std::vector<uint16_t> bucket_head_index_;

  folly::SharedMutex allocation_mutex_;
  uint16_t max_slot_used_;
  uint16_t next_empty_;
  uint16_t num_allocations_;
  std::vector<Entry*> allocation_arr_;

  void allocate();
  auto allocateEntry() -> std::tuple<Entry*, uint16_t>;

  Entry* findEntry(uint16_t offset);
  Entry* findEntryNoLock(uint16_t offset);

  void setupEntry(uint16_t curr_offset,
                  Entry* curr_entry,
                  LogicalPageOffset logical_page_offset,
                  uint32_t tag,
                  size_t size);

  void updateEntry(uint16_t curr_offset,
                    Entry* curr_entry,
                    LogicalPageOffset logical_page_offset,
                    uint32_t tag);

  uint16_t releaseEntry(uint16_t offset);

  folly::SharedMutex& getBucketMutex(SegmentIndexBucketIdT bucket_id) const {
    return mutexes_[bucket_id % num_mutexes_];
  }

  SegmentIndexBucketIdT getBucketId(SetIdT set_id) {
    return set_id % num_buckets_;
  }

  SegmentIndexBucketIdT getBucketId(HashedKey hk) {
    return getBucketId(setid_fn_(hk.keyHash()));
  }

  LogicalPageOffset getLogicalPageOffsetFromOffset(uint16_t offset) {
    auto* entry = findEntry(offset);
    return entry->logicalPageOffset();
  }
};

} // namespace navy
} // namespace cachelib
} // namespace facebook