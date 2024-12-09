#pragma once

#include <folly/SharedMutex.h>
#include <folly/logging/xlog.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <vector>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/Hash.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Device.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/zone_hash/storage/LogStorageManager.h"
#include "cachelib/navy/zone_hash/storage/ZNSDevice.h"
#include "cachelib/navy/zone_hash/storage/ZoneManager.h"
#include "cachelib/navy/zone_hash/utils/LogSegment.h"
#include "cachelib/navy/zone_hash/utils/SegmentIndex.h"
#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

class SLog {
 public:
  struct Config {
    // metadata
    uint64_t page_size_byte = 4 * 1024;
    uint64_t segment_size_byte = 256 * 1024;
    uint64_t log_size_byte;

    // index related settings
    uint64_t log_index_partitions;
    uint64_t num_index_buckets;
    uint16_t allocation_size = 1024;

    // flash related settings
    ZoneNandType zone_nand_type = ZoneNandType::QLC;
    uint64_t log_flash_partitions;
    uint32_t num_zones;
    uint32_t num_clean_zones;

    // cache eviction policy related settings
    uint32_t set_admit_threshold;

    uint32_t num_threads = 32;

    Config& validate();
  };

  explicit SLog(Config&& config,
                ZoneManager& zns_mgr,
                SetIdFunctionT setid_fn,
                SetMultiInsertFn set_insert_fn);

  ~SLog() = default;

  SLog(const SLog&) = delete;
  SLog& operator=(const SLog&) = delete;

  Status lookup(HashedKey hk, Buffer& value);

  Status insert(HashedKey hk, BufferView value);

  bool couldExist(HashedKey hk);

  uint64_t getItemCount() const { return item_count_.get(); }

  uint64_t getHitCount() const { return hit_count_.get(); }

 private:
  const uint64_t page_size_byte_;
  const uint64_t segment_size_byte_;
  const uint64_t num_pages_per_segment_;
  const uint64_t log_size_byte_;
  const uint64_t num_flash_partitions_;
  const uint64_t num_index_partitions_;
  const uint64_t num_log_buckets_;
  const uint64_t num_buckets_per_index_partition_;
  const uint64_t num_segments_;
  const SetIdFunctionT setid_fn_;
  const SetMultiInsertFn set_multi_insert_fn_;

  LogStorageManager storage_mgr_;
  AtomicIncrementCounter next_logical_segment_id_;

  std::unique_ptr<folly::SharedMutex[]> flash_partition_mutexes_;
  std::vector<std::unique_ptr<LogSegment>> buffered_partition_segment_arr_;
  std::vector<Buffer> flash_partition_buffer_arr_;
  std::vector<std::unique_ptr<SegmentIndex>> index_;

  static constexpr size_t kNumSegmentMutexes = 16 * 1024;
  std::unique_ptr<folly::SharedMutex[]> segment_mutexes_;
  std::vector<FlashSegmentOffsetT> ftl_; // logical_segment_id ->
                                         // flash_segment_offset

  uint32_t set_admit_threshold_;

  struct ValidConfigTag {};
  SLog(Config&& config,
       ZoneManager& zns_mgr,
       SetIdFunctionT setid_fn,
       SetMultiInsertFn set_insert_fn,
       ValidConfigTag);

  // Buffer related function
  bool isSegmentInBuffer(FlashSegmentOffsetT flash_segment_offset) {
    return flash_segment_offset >= kBufferedFlashSegmentOffsetMask;
  }

  bool isSegmentNull(FlashSegmentOffsetT flash_segment_offset) {
    return flash_segment_offset == kNullFlashSegmentOffset;
  }

  Buffer lookupBuffered(HashedKey hk, LogicalPageOffset logical_page_offset);

  auto lookupBufferedTag(uint32_t tag,
                         uint64_t flash_partition_id,
                         LogicalPageOffset logical_page_offset)
      -> std::tuple<Buffer, uint64_t, Buffer>;

  // Partition related function
  uint64_t getIndexPartitionId(HashedKey hk) const {
    return setid_fn_(hk.keyHash()) / num_buckets_per_index_partition_;
  }

  uint64_t getIndexPartitionId(SetIdT setid) const {
    return setid / num_buckets_per_index_partition_;
  }

  uint64_t getFlashPartitionId(HashedKey hk) const {
    return getIndexPartitionId(hk) % num_flash_partitions_;
  }

  uint64_t getFlashPartitionId(
      FlashSegmentOffsetT masked_flash_segment_offset) {
    if (masked_flash_segment_offset < kBufferedFlashSegmentOffsetMask) {
      throw std::invalid_argument("Cannot convert unmasked flash address!");
    }

    return masked_flash_segment_offset - kBufferedFlashSegmentOffsetMask;
  }

  FlashSegmentOffsetT getBufferedFlashSegmentOffset(
      uint64_t flash_partition_id) {
    return kBufferedFlashSegmentOffsetMask + flash_partition_id;
  }

  void setupLogicalSegmentNewOffset(uint32_t logical_segment_id,
                                    FlashSegmentOffsetT flash_segment_offset) {
    std::unique_lock<folly::SharedMutex> segment_lock(
        getSegmentMutex(logical_segment_id));
    ftl_[logical_segment_id] = flash_segment_offset;
  }

  folly::SharedMutex& getFlashPartitionMutex(
      uint64_t flash_partition_id) const {
    return flash_partition_mutexes_[flash_partition_id];
  }

  // Segment ID related function
  uint32_t getLogicalSegmentId(LogicalPageOffset logical_page_offset) const {
    return logical_page_offset / num_pages_per_segment_;
  }

  folly::SharedMutex& getSegmentMutex(uint32_t logical_segment_id) const {
    return segment_mutexes_[logical_segment_id % kNumSegmentMutexes];
  }

  // uint32_t getNextSegmentId() {
  //   return LogSegmentIdT(getNextLogicalSegmentId(), flash_partition_id);
  // }

  uint32_t getNextLogicalSegmentId() {
    return static_cast<uint32_t>(next_logical_segment_id_.get());
  }

  // Device related function
  FlashPageOffsetT getFlashPageOffset(FlashSegmentOffsetT flash_segment_offset,
                                      LogicalPageOffset logical_page_offset) {
    return flash_segment_offset * num_pages_per_segment_ +
           (logical_page_offset % num_pages_per_segment_);
  }

  Status flushLogSegment(uint32_t logical_segment_id,
                       uint64_t flash_partition_id);

  void cleanSegment(uint32_t segment_id);

  void moveBucket(SegmentIndex::BucketIterator& bucket_iterator,
                  uint32_t logical_segment_id_to_flush);

  void readmit(HashedKey hk, BufferView value);

  Buffer readValueFromSegment(HashedKey hk,
                              LogicalPageOffset logical_page_offset);

  // [key_buffer, key_hash, value_buffer]
  auto readKVFromSegment(uint32_t tag, LogicalPageOffset logical_page_offset)
      -> std::tuple<Buffer, uint64_t, Buffer>;

  Buffer readLogPage(FlashPageOffsetT flash_page_offset);

  mutable AtomicCounter hit_count_;
  mutable AtomicCounter item_count_;
  mutable AtomicCounter flash_item_not_found_in_log_index_;
  mutable AtomicCounter readmit_requests_;
  mutable AtomicCounter readmit_requests_failed_;
  mutable AtomicCounter key_collision_count_;
  mutable AtomicCounter io_error_count_;
  mutable AtomicCounter false_page_read_;
  mutable AtomicCounter index_item_not_found_in_segment_;
};

} // namespace navy
} // namespace cachelib
} // namespace facebook