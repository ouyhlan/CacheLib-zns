#include "cachelib/navy/zone_hash/utils/ClockSegmentIndex.h"

#include <folly/SharedMutex.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>

#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/zone_hash/utils/ClockSegmentIndexEntry.h"
#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

ClockSegmentIndex::ClockSegmentIndex(uint64_t num_buckets_per_partition,
                                     uint16_t allocation_size,
                                     uint64_t hot_data_threshold,
                                     uint32_t bf_num_hashes,
                                     uint32_t bf_bucket_bytes,
                                     SetIdFunctionT setid_fn,
                                     CollectValidFn collect_fn)
    : num_mutexes_(num_buckets_per_partition / 10 + 1),
      num_buckets_(num_buckets_per_partition),
      allocation_size_(allocation_size),
      null_entry_offset_(UINT16_MAX),
      setid_fn_(setid_fn),
      collect_valid_fn_(collect_fn),
      admission_policy_(
          num_buckets_per_partition, bf_num_hashes, bf_bucket_bytes, setid_fn_),
      eviction_policy_(
          allocation_size,
          hot_data_threshold,
          admission_policy_,
          [this](uint16_t offset) {
            return getLogicalPageOffsetFromOffset(offset);
          },
          collect_fn),
      bucket_head_index_(num_buckets_, UINT16_MAX),
      max_slot_used_(0),
      next_empty_(0),
      num_allocations_(0) {
  mutexes_ = std::make_unique<folly::SharedMutex[]>(num_mutexes_);
  {
    std::unique_lock<folly::SharedMutex> lock(allocation_mutex_);
    allocate();
  }
}

void ClockSegmentIndex::allocate() {
  XDCHECK((num_allocations_ + 1) * allocation_size_ < null_entry_offset_);

  num_allocations_++;
  allocation_arr_.resize(num_allocations_);
  allocation_arr_[num_allocations_ - 1] = new Entry[allocation_size_];
}

ClockSegmentIndex::~ClockSegmentIndex() {
  {
    std::unique_lock<folly::SharedMutex> lock(allocation_mutex_);
    for (uint16_t i = 0; i < num_allocations_; i++) {
      delete allocation_arr_[i];
    }
  }
}

void ClockSegmentIndex::track(HashedKey hk) { admission_policy_.touch(hk); }

bool ClockSegmentIndex::admissionTest(HashedKey hk) {
  return admission_policy_.accept(hk);
}

bool ClockSegmentIndex::determineEviction(HashedKey hk) {
  const auto bucket_id = getBucketId(hk);
  uint32_t tag = createTag(hk);
  {
    std::shared_lock<folly::SharedMutex> lock(getBucketMutex(bucket_id));
    uint16_t curr_offset = bucket_head_index_[bucket_id];
    Entry* curr = findEntry(curr_offset);
    while (curr != nullptr) {
      if (curr->valid() && curr->tag() == tag) {
        return eviction_policy_.entryIsGhost(curr_offset);
      }

      curr_offset = curr->next();
      curr = findEntry(curr_offset);
    }
  }

  // not exist on index, approve to evict
  return true;
}

auto ClockSegmentIndex::lookup(HashedKey hk, bool hit)
    -> std::tuple<Status, LogicalPageOffset> {
  const auto bucket_id = getBucketId(hk);
  uint32_t tag = createTag(hk);
  {
    std::shared_lock<folly::SharedMutex> lock(getBucketMutex(bucket_id));
    uint16_t curr_offset = bucket_head_index_[bucket_id];
    Entry* curr = findEntry(curr_offset);
    while (curr != nullptr) {
      if (curr->valid() && curr->tag() == tag) {
        if (hit) {
          // record look up
          eviction_policy_.touch(curr_offset);
        }

        return {Status::Ok, curr->logicalPageOffset()};
      }

      curr_offset = curr->next();
      curr = findEntry(curr_offset);
    }
  }

  return {Status::NotFound, 0};
}

Status ClockSegmentIndex::insert(HashedKey hk,
                                 size_t size,
                                 LogicalPageOffset logical_page_offset) {
  const auto bucket_id = getBucketId(hk);
  uint32_t tag = createTag(hk);
  {
    std::unique_lock<folly::SharedMutex> bucket_lock(getBucketMutex(bucket_id));
    if (findEntry(bucket_head_index_[bucket_id]) == nullptr) {
      // bucket_head_index_[bucket_id] points to nullptr
      auto [head, head_offset] = allocateEntry();
      setupEntry(head_offset, head, logical_page_offset, tag, size);
      bucket_head_index_[bucket_id] = head_offset;
    } else {
      Entry* pre = nullptr;
      uint16_t curr_offset = bucket_head_index_[bucket_id];
      Entry* curr = findEntry(curr_offset);

      while (curr != nullptr) {
        if (curr->valid() && curr->tag() == tag) {
          eviction_policy_.remove(curr_offset);
          setupEntry(curr_offset, curr, logical_page_offset, tag, size);
          return Status::Ok;
        }
        pre = curr;
        curr_offset = curr->next();
        curr = findEntry(curr_offset);
      }

      auto [new_entry, new_entry_offset] = allocateEntry();
      setupEntry(new_entry_offset, new_entry, logical_page_offset, tag, size);
      pre->setNext(new_entry_offset);
    }
  }

  return Status::Ok;
}

Status ClockSegmentIndex::readmit(HashedKey hk,
                                  size_t size,
                                  LogicalPageOffset logical_page_offset) {
  const auto bucket_id = getBucketId(hk);
  uint32_t tag = createTag(hk);

  {
    std::unique_lock<folly::SharedMutex> bucket_lock(getBucketMutex(bucket_id));

    if (findEntry(bucket_head_index_[bucket_id]) == nullptr) {
      // bucket_head_index_[bucket_id] points to nullptr
      auto [head, head_offset] = allocateEntry();
      setupEntry(head_offset, head, logical_page_offset, tag, size);
      bucket_head_index_[bucket_id] = head_offset;
    } else {
      Entry* pre = nullptr;
      uint16_t curr_offset = bucket_head_index_[bucket_id];
      Entry* curr = findEntry(curr_offset);

      while (curr != nullptr) {
        if (curr->valid() && curr->tag() == tag) {
          eviction_policy_.lock();
          updateEntry(curr_offset, curr, logical_page_offset, tag);
          collect_valid_fn_(logical_page_offset, size);
          eviction_policy_.unlock();
          return Status::Ok;
        }
        pre = curr;
        curr_offset = curr->next();
        curr = findEntry(curr_offset);
      }

      auto [new_entry, new_entry_offset] = allocateEntry();
      setupEntry(new_entry_offset, new_entry, logical_page_offset, tag, size);
      pre->setNext(new_entry_offset);
    }
  }

  return Status::Ok;
}

Status ClockSegmentIndex::remove(HashedKey hk) {
  uint64_t tag = createTag(hk);
  auto bucket_id = getBucketId(hk);
  {
    std::unique_lock<folly::SharedMutex> bucket_lock(getBucketMutex(bucket_id));

    Entry* head = findEntry(bucket_head_index_[bucket_id]);
    if (head == nullptr) {
      return Status::NotFound;
    } else if (head->valid() && head->tag() == tag) {
      bucket_head_index_[bucket_id] =
          releaseEntry(bucket_head_index_[bucket_id]);
      return Status::Ok;
    }

    Entry* pre = head;
    Entry* curr = findEntry(head->next());

    while (curr != nullptr) {
      if (curr->valid() && curr->tag() == tag) {
        pre->setNext(releaseEntry(pre->next()));
        return Status::Ok;
      }

      pre = curr;
      curr = findEntry(curr->next());
    }
  }

  return Status::NotFound;
}

Status ClockSegmentIndex::remove(HashedKey hk,
                                 LogicalPageOffset logical_page_offset) {
  uint64_t tag = createTag(hk);
  auto bucket_id = getBucketId(hk);
  return remove(tag, bucket_id, logical_page_offset);
}

Status ClockSegmentIndex::remove(uint32_t tag,
                                 SetIdT set_id,
                                 LogicalPageOffset logical_page_offset) {
  auto bucket_id = getBucketId(set_id);
  {
    std::unique_lock<folly::SharedMutex> bucket_lock(getBucketMutex(bucket_id));

    Entry* head = findEntry(bucket_head_index_[bucket_id]);
    if (head == nullptr) {
      return Status::NotFound;
    } else if (head->valid() && head->tag() == tag &&
               head->logicalPageOffset() == logical_page_offset) {
      bucket_head_index_[bucket_id] =
          releaseEntry(bucket_head_index_[bucket_id]);
      return Status::Ok;
    }

    Entry* pre = head;
    Entry* curr = findEntry(head->next());

    while (curr != nullptr) {
      if (curr->valid() && curr->tag() == tag &&
          curr->logicalPageOffset() == logical_page_offset) {
        pre->setNext(releaseEntry(pre->next()));
        return Status::Ok;
      }

      pre = curr;
      curr = findEntry(curr->next());
    }
  }

  return Status::NotFound;
}

auto ClockSegmentIndex::allocateEntry() -> std::tuple<Entry*, uint16_t> {
  std::unique_lock<folly::SharedMutex> lock(allocation_mutex_);
  if (next_empty_ >= num_allocations_ * allocation_size_) {
    allocate();
  }

  uint16_t allocated_offset = next_empty_;
  Entry* allocated_entry = findEntryNoLock(allocated_offset);
  XDCHECK(allocated_entry != nullptr);

  if (next_empty_ == max_slot_used_) {
    next_empty_++;
    max_slot_used_++;
  } else {
    next_empty_ = allocated_entry->next();
  }

  allocated_entry->setNext(null_entry_offset_);
  return {allocated_entry, allocated_offset};
}

ClockSegmentIndex::Entry* ClockSegmentIndex::findEntry(uint16_t offset) {
  std::shared_lock<folly::SharedMutex> lock(allocation_mutex_);
  return findEntryNoLock(offset);
}

ClockSegmentIndex::Entry* ClockSegmentIndex::findEntryNoLock(uint16_t offset) {
  uint16_t row = offset / allocation_size_;
  uint16_t col = offset % allocation_size_;
  if (row > num_allocations_) {
    return nullptr;
  }

  return &allocation_arr_[row][col];
}

void ClockSegmentIndex::setupEntry(uint16_t curr_offset,
                                   Entry* curr_entry,
                                   LogicalPageOffset logical_page_offset,
                                   uint32_t tag,
                                   size_t size) {
  curr_entry->setEntry(logical_page_offset, tag);
  eviction_policy_.track(curr_offset, logical_page_offset, size);
}

void ClockSegmentIndex::updateEntry(uint16_t curr_offset,
                    Entry* curr_entry,
                    LogicalPageOffset logical_page_offset,
                    uint32_t tag) {
  curr_entry->setEntry(logical_page_offset, tag);
}

uint16_t ClockSegmentIndex::releaseEntry(uint16_t offset) {
  eviction_policy_.remove(offset);

  std::unique_lock<folly::SharedMutex> lock(allocation_mutex_);
  Entry* entry = findEntryNoLock(offset);
  uint16_t pre_next = entry->next();

  entry->setInvalid();
  entry->setNext(next_empty_);
  next_empty_ = offset;

  return pre_next;
}

} // namespace navy
} // namespace cachelib
} // namespace facebook