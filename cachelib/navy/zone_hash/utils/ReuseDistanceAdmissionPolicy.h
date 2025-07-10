#pragma once

#include <folly/SharedMutex.h>
#include <folly/logging/xlog.h>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>

#include "cachelib/common/BloomFilter.h"
#include "cachelib/common/Hash.h"
#include "cachelib/navy/zone_hash/utils/Types.h"
namespace facebook {
namespace cachelib {
namespace navy {

class ReuseDistanceAdmissonPolicy {
 public:
  ReuseDistanceAdmissonPolicy(uint64_t num_buckets,
                              uint32_t bf_num_hashes,
                              uint32_t bf_bucket_bytes,
                              SetIdFunctionT setid_fn)
      : num_buckets_(num_buckets), setid_fn_(setid_fn) {
    uint32_t bits_per_hash = bf_bucket_bytes * 8 / bf_num_hashes;
    prev_bf_ = std::make_unique<BloomFilter>(
        num_buckets_, bf_num_hashes, bits_per_hash);
    curr_bf_ = std::make_unique<BloomFilter>(
        num_buckets_, bf_num_hashes, bits_per_hash);
  }

  ~ReuseDistanceAdmissonPolicy() = default;

  ReuseDistanceAdmissonPolicy(const ReuseDistanceAdmissonPolicy&) = delete;
  ReuseDistanceAdmissonPolicy& operator=(const ReuseDistanceAdmissonPolicy&) =
      delete;

  bool accept(HashedKey hk) {
    XDCHECK(prev_bf_ != nullptr && curr_bf_ != nullptr);

    std::shared_lock<folly::SharedMutex> lock(share_mutex_);
    return prev_bf_->couldExist(getBucketId(hk), hk.keyHash()) ||
           curr_bf_->couldExist(getBucketId(hk), hk.keyHash());
  }

  void touch(HashedKey hk) {
    XDCHECK(curr_bf_);

    std::unique_lock<folly::SharedMutex> lock(share_mutex_);
    curr_bf_->set(getBucketId(hk), hk.keyHash());
  }

  void update() {
    std::unique_lock<folly::SharedMutex> lock(share_mutex_);
    std::swap(prev_bf_, curr_bf_);
    curr_bf_->reset();
  }

 private:
  const uint64_t num_buckets_;
  const SetIdFunctionT setid_fn_;

  folly::SharedMutex share_mutex_;
  std::unique_ptr<BloomFilter> prev_bf_;
  std::unique_ptr<BloomFilter> curr_bf_;

  SegmentIndexBucketIdT getBucketId(SetIdT set_id) {
    return set_id % num_buckets_;
  }

  uint32_t getBucketId(HashedKey hk) {
    return getBucketId(setid_fn_(hk.keyHash()));
  }
};

} // namespace navy
} // namespace cachelib
} // namespace facebook