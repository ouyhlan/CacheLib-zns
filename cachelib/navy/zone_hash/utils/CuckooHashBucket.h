#pragma once

#include <folly/logging/xlog.h>

#include <array>
#include <cstdint>

#include "cachelib/navy/zone_hash/utils/CuckooHashConfig.h"
#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

class CuckooHashBucket {
  using SizeType = uint32_t;

 public:
  CuckooHashBucket() noexcept : occupied_() {}

  void setEntry(SizeType idx,
                FlashPageOffsetT flash_page_offset,
                Partial partial,
                bool inplace,
                bool occupied) {
    flash_page_offset_[idx] = flash_page_offset;
    partials_[idx] = partial;
    inplace_[idx] = inplace;
    occupied_[idx] = occupied;
  }

  void clearEntry(SizeType idx) {
    XDCHECK(occupied_[idx]);
    occupied_[idx] = false;
  }

  FlashPageOffsetT flashPageOffset(SizeType idx) const {
    return flash_page_offset_[idx];
  }

  bool isOccupied(SizeType idx) const { return occupied_[idx]; }
  // void setOccupied(SizeType idx) { occupied_[idx] = true; }

  bool isInplace(SizeType idx) const { return inplace_[idx]; }

  Partial partial(SizeType idx) const { return partials_[idx]; }
  Partial& partial(SizeType idx) { return partials_[idx]; }

  bool isIdentical(SizeType idx, Partial partial, bool inplace) const {
    // theoretically when insertion, b.isInplace() and (b.partial(i) ==
    // hk.partial) will not happen together, since partial is strictly
    // increasing
    return (occupied_[idx]) && (inplace_[idx] == inplace) &&
           (partials_[idx] == partial);
  }

 private:
  std::array<FlashPageOffsetT, kDefaultSlotPerBucket> flash_page_offset_;
  std::array<Partial, kDefaultSlotPerBucket> partials_;

  std::array<bool, kDefaultSlotPerBucket> inplace_;
  std::array<bool, kDefaultSlotPerBucket> occupied_;
};

class CuckooHashTimestamp {
 public:
  CuckooHashTimestamp() noexcept : next_timestamp_(0), oldest_timestamp_(0) {}

  void initialize(uint8_t t) {
    next_timestamp_ = t;
    oldest_timestamp_ = t;
  }

  Partial getNewTimestamp() {
    Partial res = next_timestamp_;
    ++next_timestamp_;
    return res;
  }

  bool tryReadOldestTimestamp(Partial& p) const {
    if (next_timestamp_ == oldest_timestamp_) {
      return false;
    }

    p = oldest_timestamp_;
    return true;
  }

  bool tryReadNewestTimestamp(Partial& p) const {
    // check empty
    if (next_timestamp_ == oldest_timestamp_) {
      return false;
    }
    p = next_timestamp_ - 1;
    return true;
  }

  bool tryReadYoungerTimestamp(Partial& p) const {
    Partial prevTimestamp = p;

    if (next_timestamp_ == oldest_timestamp_ || prevTimestamp == next_timestamp_) {
      return false;
    }

    p = prevTimestamp + 1;
    if (static_cast<Partial>(p - oldest_timestamp_) < static_cast<Partial>(next_timestamp_ - oldest_timestamp_)) {
      return true;
    } else {
      return false;
    }
  }

  // input prev timestamp and change it into next timestamp
  bool tryReadOlderTimestamp(Partial& p) const {
    Partial prevTimestamp = p;

    // check empty or reach the end of the timestamp
    if (next_timestamp_ == oldest_timestamp_ ||
        prevTimestamp == oldest_timestamp_ ||
        prevTimestamp == next_timestamp_) {
      return false;
    }

    // check valid
    // since next != a
    // (next - a) < (next - old) ==> valid
    if (static_cast<Partial>(next_timestamp_ - prevTimestamp) <
        static_cast<Partial>(next_timestamp_ - oldest_timestamp_)) {
      p = prevTimestamp - 1;
      return true;
    } else {
      return false;
    }
  }

  void removeTimestamp(const Partial curr) {
    if (curr == oldest_timestamp_) {
      ++oldest_timestamp_;
    }
  }

 private:
  Partial next_timestamp_;
  Partial oldest_timestamp_;
};

// Lock Implementation
// Copied from libcuckoo
using CounterType = int64_t;
class __attribute__((aligned(64))) SpinLock {
 public:
  SpinLock() : elem_counter_(0) { lock_.clear(); }

  SpinLock(const SpinLock& other) noexcept
      : elem_counter_(other.elemCounter()) {
    lock_.clear();
  }

  SpinLock& operator=(const SpinLock& other) noexcept {
    elem_counter_ = other.elemCounter();
    return *this;
  }

  void lock() noexcept {
    while (lock_.test_and_set(std::memory_order_acq_rel))
      ;
  }

  void unlock() noexcept { lock_.clear(std::memory_order_release); }

  bool tryLock() noexcept {
    return !lock_.test_and_set(std::memory_order_acq_rel);
  }

  void decrElemCounter() noexcept { --elem_counter_; }
  void incrElemCounter() noexcept { ++elem_counter_; }
  CounterType elemCounter() const noexcept { return elem_counter_; }

 private:
  std::atomic_flag lock_;
  CounterType elem_counter_;
};

} // namespace navy
} // namespace cachelib
} // namespace facebook