#pragma once

#include <folly/Range.h>
#include <folly/SharedMutex.h>

#include <cstdint>
#include <vector>

#include "cachelib/common/Hash.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/zone_hash/utils/Types.h"
#include "cachelib/navy/zone_hash/utils/ZoneBucket.h"

namespace facebook {
namespace cachelib {
namespace navy {

class LogSegment {
 public:
  class Iterator {
   public:
    Iterator() : done_(false) {}

    bool done() const { return done_; }

    HashedKey hashedKey() const { return it_.hashedKey(); }

    BufferView value() const { return it_.value(); }

   private:
    uint64_t bucket_num_;
    ZoneBucket::Iterator it_;
    bool done_ = false;

    friend LogSegment;

    explicit Iterator(uint64_t bucket_num, ZoneBucket::Iterator it)
        : bucket_num_(bucket_num), it_(it) {
      if (it.done()) {
        done_ = true;
      }
    }
  };

  explicit LogSegment(uint32_t segment_id,
                      uint64_t segment_size_byte,
                      uint64_t page_size_byte,
                      MutableBufferView mutable_view,
                      bool new_bucket);

  BufferView find(HashedKey hk, LogicalPageOffset logical_page_offset);
  BufferView findTag(uint32_t tag, LogicalPageOffset logical_page_offset);

  std::tuple<Status, LogicalPageOffset> insert(HashedKey hk, BufferView value);

  uint32_t getLogicalSegmentId() const { return logical_segment_id_; }

  void reset(uint32_t next_log_segment_id);

  Iterator getFirst();
  Iterator getNext(Iterator it);

  bool tryLockForFlushing() {
    std::unique_lock<folly::SharedMutex> lock(allocation_mutex_);
    
    bool res = !flushing_;
    flushing_ = true;

    return res;
  }

 private:
  const uint64_t segment_size_byte_;
  const uint64_t page_size_byte_;
  const uint64_t num_buckets_;
  uint32_t logical_segment_id_;

  std::vector<ZoneBucket*> bucket_arr_;
  folly::SharedMutex allocation_mutex_;

  bool flushing_;

  uint32_t getLogBucketId(LogicalPageOffset logical_page_offset) {
    return logical_page_offset % num_buckets_;
  }

  LogicalPageOffset getLogicalPageOffer(uint32_t bucket_id) {
    return logical_segment_id_ * num_buckets_ + bucket_id;
  }
};

} // namespace navy
} // namespace cachelib
} // namespace facebook