#pragma once

#include <folly/SharedMutex.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <vector>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/navy/zone_hash/utils/ReuseDistanceAdmissionPolicy.h"
#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

using GetLogicalPageOffsetFn = std::function<LogicalPageOffset(uint16_t)>;

class ClockPolicy {
  class __attribute__((__packed__)) Entry {
   public:
    Entry() {}

    void initialize(size_t size) {
      size_ = size;
      clock_bits_ = 1;
      ghost_ = 0;
      valid_ = 1;
    }

    bool ghost() const { return ghost_; }
    bool valid() const { return valid_; }
    uint16_t size() const { return size_; }
    bool canEvict() const { return clock_bits_ == 0; }

    void promote() {
      if (clock_bits_ < 3) {
        clock_bits_++;
      }
    }

    void demote() {
      XDCHECK(clock_bits_ > 0);
      clock_bits_--;
    }

    void setGhost() { ghost_ = 1; }

    void clearGhost() {
      XDCHECK(ghost_ == 1);
      ghost_ = 0;
      clock_bits_ = 0;
    }

    void setInvalid() { valid_ = 0; }

   private:
    uint16_t size_ : 12;
    uint16_t clock_bits_ : 2;
    uint16_t ghost_ : 1;
    uint16_t valid_ : 1;
  };

 public:
  ClockPolicy(uint16_t allocation_size,
              uint64_t threshold,
              ReuseDistanceAdmissonPolicy& admission_policy,
              GetLogicalPageOffsetFn get_fn,
              CollectValidFn collect_fn)
      : admission_policy_(admission_policy),
        allocate_size_(allocation_size),
        hot_data_size_(0),
        hot_data_threshold_(threshold),
        get_logical_page_offset_fn_(get_fn),
        collect_valid_fn_(collect_fn) {
    allocate();
  }

  void track(uint16_t offset,
             LogicalPageOffset logical_page_offset,
             size_t size) {
    std::unique_lock<folly::SharedMutex> lock{mutex_};

    while (offset >= num_allocations_ * allocate_size_) {
      allocate();
    }

    max_entry_offset_ = std::max(max_entry_offset_, offset);
    auto* curr = findEntry(offset);
    curr->initialize(size);
    recordHotData(logical_page_offset, size);
  }

  void touch(uint16_t offset) {
    std::unique_lock<folly::SharedMutex> lock{mutex_};

    auto* curr = findEntry(offset);
    if (curr->valid()) {
      if (curr->ghost()) {
        curr->clearGhost();
        recordHotData(get_logical_page_offset_fn_(offset), curr->size());
      } else {
        curr->promote();
      }
    }
  }

  void remove(uint16_t offset) {
    std::unique_lock<folly::SharedMutex> lock{mutex_};

    auto* curr = findEntry(offset);

    if (curr->valid()) {
      hot_data_size_ -= curr->size();
      curr->setInvalid();
    }
  }

  bool entryIsGhost(uint16_t offset) {
    std::shared_lock<folly::SharedMutex> lock{mutex_};

    auto* curr = findEntry(offset);
    return curr->ghost();
  }

  void lock() { mutex_.lock(); }
  void unlock() { mutex_.unlock(); }

 private:
  ReuseDistanceAdmissonPolicy& admission_policy_;
  const uint16_t allocate_size_;
  const GetLogicalPageOffsetFn get_logical_page_offset_fn_;
  const CollectValidFn collect_valid_fn_;

  folly::SharedMutex mutex_;
  uint16_t clock_pointer_;
  uint16_t max_entry_offset_;
  uint16_t num_allocations_;
  std::vector<Entry*> allocation_arr_;

  uint64_t hot_data_size_;
  uint64_t hot_data_threshold_;

  void recordHotData(LogicalPageOffset logical_page_offset,
                     uint64_t new_data_size) {
    hot_data_size_ += new_data_size;
    collect_valid_fn_(logical_page_offset, new_data_size);

    // check if need eviction
    if (hot_data_size_ > hot_data_threshold_) {
      evictItems();
    }
  }

  void evictItems() {
    while (hot_data_size_ > hot_data_threshold_) {
      Entry* curr = findEntry(clock_pointer_);
      if (curr->valid() && !curr->ghost()) {
        if (curr->canEvict()) {
          collect_valid_fn_(get_logical_page_offset_fn_(clock_pointer_),
                            -curr->size());
          hot_data_size_ -= curr->size();
          curr->setGhost();
        } else {
          curr->demote();
        }
      }

      clock_pointer_ = (clock_pointer_ + 1) % max_entry_offset_;
      if (clock_pointer_ == 0) {
        admission_policy_.update();
      }
    }
  }

  void allocate() {
    num_allocations_++;
    allocation_arr_.resize(num_allocations_);
    allocation_arr_[num_allocations_ - 1] = new Entry[allocate_size_];
  }

  Entry* findEntry(uint16_t offset) {
    uint16_t row = offset / allocate_size_;
    uint16_t col = offset % allocate_size_;
    if (row > num_allocations_) {
      return nullptr;
    }

    return &allocation_arr_[row][col];
  }
};

} // namespace navy
} // namespace cachelib
} // namespace facebook