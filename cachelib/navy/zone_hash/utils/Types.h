#pragma once

#include <folly/SharedMutex.h>

#include <atomic>
#include <cstdint>
#include <functional>
#include <vector>

#include "cachelib/common/Hash.h"
#include "cachelib/navy/common/Buffer.h"

namespace facebook {
namespace cachelib {
namespace navy {

using SetIdT = uint32_t;
using SegmentIndexBucketIdT = SetIdT;
using SetIdFunctionT = std::function<SetIdT(uint64_t)>;
using LogicalPageOffset = uint32_t;

// Segment Flash related
using FlashSegmentOffsetT = uint32_t;
constexpr uint32_t kBufferedFlashSegmentOffsetMask = ((uint32_t)1 << 31);
constexpr uint32_t kNullFlashSegmentOffset = UINT32_MAX;

using FlashPageOffsetT = uint32_t;
using FlashByteAddressT = uint64_t;

// struct LogSegmentIdT {
//   uint32_t logical_id;
//   uint64_t flash_partition_id;

//   LogSegmentIdT() = default;

//   LogSegmentIdT(uint32_t segment_id, uint64_t partition_id)
//       : logical_id(segment_id), flash_partition_id(partition_id) {}

//   LogSegmentIdT(const LogSegmentIdT& rhs)
//       : logical_id(rhs.logical_id),
//         flash_partition_id(rhs.flash_partition_id) {}

//   bool operator==(const LogSegmentIdT& rhs) const noexcept {
//     return logical_id == rhs.logical_id &&
//            flash_partition_id == rhs.flash_partition_id;
//   }
// };

uint32_t createTag(HashedKey hk);
uint32_t createTag(BufferView key);
uint32_t createTag(folly::StringPiece key);

class AtomicIncrementCounter {
 public:
  explicit AtomicIncrementCounter(uint64_t total_count,
                                  uint64_t initial_value = 0)
      : val_(initial_value), total_(total_count) {}

  ~AtomicIncrementCounter() = default;

  AtomicIncrementCounter(const AtomicIncrementCounter& rhs) = delete;

  AtomicIncrementCounter& operator=(const AtomicIncrementCounter& rhs) = delete;

  uint64_t get() {
    uint64_t old_value = val_;
    uint64_t new_value = (val_ + 1) % total_;
    while (!val_.compare_exchange_weak(old_value, new_value,
                                       std::memory_order_relaxed)) {
      new_value = (old_value + 1) % total_;
    }

    return old_value;
  }

 private:
  std::atomic<uint64_t> val_{0};
  const uint64_t total_;
};

// Flash Address
using FlashAddressT = uint64_t;

struct ObjectInfo {
  Buffer key;
  HashedKey hk;
  Buffer value;
  LogicalPageOffset logical_page_offset;
  uint32_t tag;
  uint8_t hits;

  uint64_t size() const { return hk.key().size() + value.size(); }

  ObjectInfo(Buffer&& key_buffer,
             uint64_t key_hash,
             Buffer&& value_buffer,
             LogicalPageOffset page_offset,
             uint32_t t,
             uint8_t h)
      : key(std::move(key_buffer)),
        hk(HashedKey::precomputed(toStringPiece(this->key.view()), key_hash)),
        value(std::move(value_buffer)),
        logical_page_offset(page_offset),
        tag(t),
        hits(h) {}

  ObjectInfo(const ObjectInfo&) = delete;
  ObjectInfo& operator=(const ObjectInfo&) = delete;

  ObjectInfo(ObjectInfo&&) noexcept = default;
  ObjectInfo& operator=(ObjectInfo&&) noexcept = default;
};

using LogReadmitCallback = std::function<void(const ObjectInfo&)>;
using SetMultiInsertFn =
    std::function<void(std::vector<ObjectInfo>&&, LogReadmitCallback&&)>;

using LogCleanSegmentFn = std::function<void(uint32_t)>;

using CompareFlashPageAgeFn =
    std::function<bool(FlashPageOffsetT, FlashPageOffsetT)>;

using BloomFilterRejectFn = std::function<bool(FlashPageOffsetT, uint64_t)>;

enum class ZoneNandType { SLC, QLC };

using CalculateInvalidRateFn = std::function<double(std::vector<uint32_t>)>;
using CollectValidFn = std::function<void(LogicalPageOffset, int64_t)>;

using LoanZoneFn = std::function<uint32_t(void)>;
using ReturnZoneFn = std::function<void(uint32_t)>;

using UpdateSegmentIndexFn = std::function<void(uint32_t, FlashSegmentOffsetT)>;
using UpdatePageIndexFn =
    std::function<void(SetIdT, FlashPageOffsetT, uint32_t)>;

using TrackSetInsertFn =
    std::function<void(std::vector<uint64_t>, std::vector<uint16_t>, uint32_t)>;

using TrackAdaptiveEvictFn = std::function<void(uint64_t, uint16_t)>;

} // namespace navy
} // namespace cachelib
} // namespace facebook
