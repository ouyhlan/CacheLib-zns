#include "cachelib/navy/zone_hash/utils/LogSegment.h"

#include <folly/Conv.h>
#include <folly/SharedMutex.h>
#include <folly/logging/xlog.h>

#include <cstdint>
#include <mutex>

#include "cachelib/common/Hash.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/zone_hash/storage/ZoneBucketStorage.h"
#include "cachelib/navy/zone_hash/utils/Types.h"
#include "cachelib/navy/zone_hash/utils/ZoneBucket.h"

namespace facebook {
namespace cachelib {
namespace navy {

LogSegment::LogSegment(uint32_t logical_segment_id,
                       uint64_t segment_size_byte,
                       uint64_t page_size_byte,
                       MutableBufferView mutable_view,
                       bool new_bucket)
    : segment_size_byte_(segment_size_byte),
      page_size_byte_(page_size_byte),
      num_buckets_(segment_size_byte_ / page_size_byte_),
      logical_segment_id_(logical_segment_id),
      bucket_arr_(num_buckets_),
      flushing_(false) {
  for (uint64_t i = 0; i < num_buckets_; i++) {
    uint64_t offset = i * page_size_byte;
    auto view =
        MutableBufferView(page_size_byte_, mutable_view.data() + offset);
    if (new_bucket) {
      ZoneBucket::initNew(view, 0);
    }
    bucket_arr_[i] =
        reinterpret_cast<ZoneBucket*>(mutable_view.data() + offset);
  }
}

BufferView LogSegment::find(HashedKey hk,
                            LogicalPageOffset logical_page_offset) {
  uint32_t bucket_id = getLogBucketId(logical_page_offset);
  return bucket_arr_[bucket_id]->find(hk);
}

BufferView LogSegment::findTag(uint32_t tag,
                               LogicalPageOffset logical_page_offset) {
  uint32_t bucket_id = getLogBucketId(logical_page_offset);
  return bucket_arr_[bucket_id]->findTag(tag);
}

std::tuple<Status, LogicalPageOffset> LogSegment::insert(HashedKey hk,
                                                         BufferView value) {
  ZoneBucketStorage::Allocation alloc;
  uint32_t i = 0;
  bool found_alloc = false;
  {
    std::unique_lock<folly::SharedMutex> lock(allocation_mutex_);
    if (flushing_) {
      return {Status::Rejected, 0};
    }

    for (; i < num_buckets_; i++) {
      if (bucket_arr_[i]->isSpace(hk, value)) {
        alloc = bucket_arr_[i]->allocate(hk, value);
        found_alloc = true;
        break;
      }
    }
  }

  if (!found_alloc) {
    return {Status::Rejected, 0};
  }

  // current space has been reserved, so no need to hold mutex
  bucket_arr_[i]->insert(alloc, hk, value);
  return {Status::Ok, getLogicalPageOffer(i)};
}

void LogSegment::reset(uint32_t next_log_segment_id) {
  this->logical_segment_id_ = next_log_segment_id;

  for (uint64_t i = 0; i < num_buckets_; i++) {
    bucket_arr_[i]->clear();
  }

  flushing_ = false;
}

LogSegment::Iterator LogSegment::getFirst() {
  uint64_t bucket_num = 0;
  auto it = bucket_arr_[bucket_num]->getFirst();
  return Iterator(bucket_num, it);
}

LogSegment::Iterator LogSegment::getNext(Iterator curr) {
  if (curr.done()) {
    return curr;
  }

  auto next_it = bucket_arr_[curr.bucket_num_]->getNext(curr.it_);
  if (next_it.done() && curr.bucket_num_ >= num_buckets_ - 1) {
    curr.done_ = true;
    return curr;
  } else if (next_it.done()) {
    curr.bucket_num_++;
    curr.it_ = bucket_arr_[curr.bucket_num_]->getFirst();
    return curr;
  } else {
    curr.it_ = next_it;
    return curr;
  }
}

} // namespace navy
} // namespace cachelib
} // namespace facebook