#include "cachelib/navy/zone_hash/utils/SegmentIndex.h"

#include <folly/SharedMutex.h>
#include <folly/logging/xlog.h>

#include <cassert>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <vector>

#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/zone_hash/utils/SegmentIndexEntry.h"
#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

SegmentIndex::SegmentIndex(uint64_t num_buckets_per_partition,
                           uint16_t allocation_size,
                           SetIdFunctionT setid_fn)
    : num_mutexes_(num_buckets_per_partition / 10 + 1),
      num_buckets_(num_buckets_per_partition),
      allocation_size_(allocation_size),
      setid_fn_(setid_fn),
      bucket_head_index_(num_buckets_, UINT16_MAX),
      null_entry_offset_(UINT16_MAX),
      max_slot_used_(0),
      next_empty_(0),
      num_allocations_(0) {
  mutexes_ = std::make_unique<folly::SharedMutex[]>(num_mutexes_);
  iter_mutexes_ = std::make_unique<std::mutex[]>(num_mutexes_);
  {
    std::unique_lock<folly::SharedMutex> lock(allocation_mutex_);
    allocate();
  }
}

void SegmentIndex::allocate() {
  XDCHECK((num_allocations_ + 1) * allocation_size_ < null_entry_offset_);

  num_allocations_++;
  allocation_arr_.resize(num_allocations_);
  allocation_arr_[num_allocations_ - 1] =
      new SegmentIndexEntry[allocation_size_];
}

SegmentIndex::~SegmentIndex() {
  {
    std::unique_lock<folly::SharedMutex> lock(allocation_mutex_);
    for (uint16_t i = 0; i < num_allocations_; i++) {
      delete allocation_arr_[i];
    }
  }
}

std::tuple<Status, LogicalPageOffset, uint32_t> SegmentIndex::lookup(
    HashedKey hk, bool hit) {
  const auto bucket_id = getBucketId(hk);
  uint32_t tag = createTag(hk);
  {
    std::shared_lock<folly::SharedMutex> lock(getMutex(bucket_id));
    SegmentIndexEntry* curr = findEntry(bucket_head_index_[bucket_id]);
    while (curr != nullptr) {
      if (curr->valid && curr->tag == tag) {
        if (hit) {
          curr->incrementHits();
        }

        uint32_t hits = curr->hits;
        return {Status::Ok, curr->logicalPageOffset(), hits};
      }

      curr = findEntry(curr->next);
    }
  }

  return {Status::NotFound, 0, 0};
}

Status SegmentIndex::insert(HashedKey hk,
                            LogicalPageOffset logical_page_offset) {
  const auto bucket_id = getBucketId(hk);
  uint32_t tag = createTag(hk);
  {
    std::unique_lock<folly::SharedMutex> lock(getMutex(bucket_id));

    if (findEntry(bucket_head_index_[bucket_id]) == nullptr) {
      // bucket_head_index_[bucket_id] points to nullptr
      auto [head, head_offset] = allocateEntry();
      head->populateEntry(logical_page_offset, tag, 0);
      bucket_head_index_[bucket_id] = head_offset;
    } else {
      SegmentIndexEntry* pre = nullptr;
      SegmentIndexEntry* curr = findEntry(bucket_head_index_[bucket_id]);

      while (curr != nullptr) {
        if (curr->valid && curr->tag == tag) {
          curr->populateEntry(logical_page_offset, tag, 0);
          return Status::Ok;
        }
        pre = curr;
        curr = findEntry(curr->next);
      }

      auto [new_entry, new_entry_offset] = allocateEntry();
      new_entry->populateEntry(logical_page_offset, tag, 0);
      pre->next = new_entry_offset;
    }
  }

  return Status::Ok;
}

Status SegmentIndex::insert(uint32_t tag,
                            SetIdT set_id,
                            LogicalPageOffset logical_page_offset) {
  const auto bucket_id = getBucketId(set_id);
  {
    std::unique_lock<folly::SharedMutex> lock(getMutex(bucket_id));

    if (findEntry(bucket_head_index_[bucket_id]) == nullptr) {
      // bucket_head_index_[bucket_id] points to nullptr
      auto [head, head_offset] = allocateEntry();
      head->populateEntry(logical_page_offset, tag, 0);
      bucket_head_index_[bucket_id] = head_offset;
    } else {
      SegmentIndexEntry* pre = nullptr;
      SegmentIndexEntry* curr = findEntry(bucket_head_index_[bucket_id]);

      while (curr != nullptr) {
        if (curr->valid && curr->tag == tag) {
          curr->populateEntry(logical_page_offset, tag, 0);
          return Status::Ok;
        }
        pre = curr;
        curr = findEntry(curr->next);
      }

      auto [new_entry, new_entry_offset] = allocateEntry();
      new_entry->populateEntry(logical_page_offset, tag, 0);
      pre->next = new_entry_offset;
    }
  }

  return Status::Ok;
}

Status SegmentIndex::remove(HashedKey hk,
                            LogicalPageOffset logical_page_offset) {
  uint64_t tag = createTag(hk);
  auto bucket_id = getBucketId(hk);
  return remove(tag, bucket_id, logical_page_offset);
}

Status SegmentIndex::remove(uint32_t tag,
                            SetIdT set_id,
                            LogicalPageOffset logical_page_offset) {
  auto bucket_id = getBucketId(set_id);
  return removeFromBucketId(tag, bucket_id, logical_page_offset);
}

Status SegmentIndex::removeFromBucketId(uint32_t tag,
                                        SegmentIndexBucketIdT bucket_id,
                                        LogicalPageOffset logical_page_offset) {
  {
    std::unique_lock<folly::SharedMutex> lock(getMutex(bucket_id));

    SegmentIndexEntry* head = findEntry(bucket_head_index_[bucket_id]);
    if (head == nullptr) {
      return Status::NotFound;
    } else if (head->valid && head->tag == tag &&
               head->logicalPageOffset() == logical_page_offset) {
      bucket_head_index_[bucket_id] =
          releaseEntry(bucket_head_index_[bucket_id]);
      return Status::Ok;
    }

    SegmentIndexEntry* pre = head;
    SegmentIndexEntry* curr = findEntry(head->next);

    while (curr != nullptr) {
      if (curr->valid && curr->tag == tag &&
          curr->logicalPageOffset() == logical_page_offset) {
        pre->next = releaseEntry(pre->next);
        return Status::Ok;
      }

      pre = curr;
      curr = findEntry(curr->next);
    }
  }

  return Status::NotFound;
}

SegmentIndex::BucketIterator SegmentIndex::getHashBucketIterator(HashedKey hk) {
  auto bucket_id = getBucketId(hk);
  auto set_id = setid_fn_(hk.keyHash());

  // only one garbage collection is needed at the same time
  if (!getIterMutex(bucket_id).try_lock()) {
    return BucketIterator();
  }

  std::vector<SegmentIndexEntry> snapshot;
  {
    std::shared_lock<folly::SharedMutex> lock(getMutex(bucket_id));

    auto curr = findEntry(bucket_head_index_[bucket_id]);
    while (curr != nullptr) {
      if (curr->valid) {
        snapshot.push_back(*curr);
      }

      curr = findEntry(curr->next);
    }
  }

  return BucketIterator(set_id, std::move(snapshot), getIterMutex(bucket_id));
}

// Entry related function
std::tuple<SegmentIndexEntry*, uint16_t> SegmentIndex::allocateEntry() {
  std::unique_lock<folly::SharedMutex> lock(allocation_mutex_);
  if (next_empty_ >= num_allocations_ * allocation_size_) {
    allocate();
  }

  uint16_t allocated_offset = next_empty_;
  SegmentIndexEntry* allocated_entry = findEntryNoLock(allocated_offset);
  XDCHECK(allocated_entry != nullptr);

  if (next_empty_ == max_slot_used_) {
    next_empty_++;
    max_slot_used_++;
  } else {
    next_empty_ = allocated_entry->next;
  }

  allocated_entry->next = null_entry_offset_;
  return {allocated_entry, allocated_offset};
}

SegmentIndexEntry* SegmentIndex::findEntry(uint16_t offset) {
  std::shared_lock<folly::SharedMutex> lock(allocation_mutex_);
  return findEntryNoLock(offset);
}

SegmentIndexEntry* SegmentIndex::findEntryNoLock(uint16_t offset) {
  uint16_t row = offset / allocation_size_;
  uint16_t col = offset % allocation_size_;
  if (row > num_allocations_) {
    return nullptr;
  }

  return &allocation_arr_[row][col];
}

uint16_t SegmentIndex::releaseEntry(uint16_t offset) {
  std::unique_lock<folly::SharedMutex> lock(allocation_mutex_);
  SegmentIndexEntry* entry = findEntryNoLock(offset);
  uint16_t pre_next = entry->next;
  entry->valid = 0;
  entry->next = next_empty_;
  next_empty_ = offset;

  return pre_next;
}

} // namespace navy
} // namespace cachelib
} // namespace facebook