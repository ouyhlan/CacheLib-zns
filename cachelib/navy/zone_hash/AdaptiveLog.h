#pragma once

#include <cstdint>
#include <vector>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/Hash.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Device.h"
#include "cachelib/navy/scheduler/JobScheduler.h"
#include "cachelib/navy/zone_hash/storage/AdaptiveStorageManager.h"
#include "cachelib/navy/zone_hash/storage/ZoneManager.h"
#include "cachelib/navy/zone_hash/utils/ClockSegmentIndex.h"
#include "cachelib/navy/zone_hash/utils/LogSegment.h"
#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

class AdaptiveLog {
 public:
  struct Config {
    // metadata
    uint64_t page_size_byte = 4 * 1024;
    uint64_t segment_size_byte = 256 * 1024;

    // Bloom Filter related
    uint32_t bf_num_hashes = 4;

    // The bloom filter size per bucket in bytes
    uint32_t bf_bucket_bytes = 8;

    // index related settings
    uint64_t index_partitions;
    uint64_t num_index_buckets;
    uint16_t allocation_size = 1024;
    uint16_t hot_data_pct = 70;

    // flash related settings
    ZoneNandType zone_nand_type = ZoneNandType::QLC;
    uint64_t zone_capacity_byte;
    uint64_t flash_partitions;
    uint32_t initial_num_zones;
    uint32_t num_free_zones = 1;
    uint32_t max_num_zones;

    uint32_t num_threads = 32;

    uint64_t maxSize() const { return max_num_zones * zone_capacity_byte; }

    uint64_t indexPartitionHotDataThreshold() const {
      uint64_t initial_total_capacity = initial_num_zones * zone_capacity_byte;
      uint64_t index_partition_capacity =
          initial_total_capacity / index_partitions;

      return index_partition_capacity * hot_data_pct / 100;
    }

    Config& validate();
  };

  explicit AdaptiveLog(Config&& config,
                       ZoneManager& zns_mgr,
                       SetIdFunctionT setid_fn);

  ~AdaptiveLog() = default;

  AdaptiveLog(const AdaptiveLog&) = delete;
  AdaptiveLog& operator=(const AdaptiveLog&) = delete;

  void track(HashedKey hk);

  bool admissionTest(HashedKey hk);

  Status lookup(HashedKey hk, Buffer& value);

  Status insert(HashedKey hk, BufferView value);

  bool couldExist(HashedKey hk);

  uint64_t getItemCount() const { return item_count_.get(); }

  uint64_t getHitCount() const { return hit_count_.get(); }

 private:
  const uint64_t page_size_byte_;
  const uint64_t segment_size_byte_;
  const uint64_t num_pages_per_segment_;
  const uint64_t num_flash_partitions_;
  const uint64_t num_index_partitions_;
  const uint64_t num_buckets_per_index_partition_;
  const uint64_t max_num_segments_;
  const SetIdFunctionT setid_fn_;

  AdaptiveStorageManager storage_mgr_;

  AtomicIncrementCounter next_logical_segment_id_;

  std::unique_ptr<folly::SharedMutex[]> flash_partition_mutexes_;

  // double num_flash_partitions ->
  // [0, num_flash_partitions_) for first admit
  // [num_flash_partitions_, num_flash_partitions_ * 2) for readmit
  std::vector<std::unique_ptr<LogSegment>> buffered_partition_segment_arr_;
  std::vector<Buffer> flash_partition_buffer_arr_;
  std::vector<AtomicCounter> segment_valid_size_arr_;
  std::vector<std::unique_ptr<ClockSegmentIndex>> index_;

  static constexpr size_t kNumSegmentMutexes = 16 * 1024;
  std::unique_ptr<folly::SharedMutex[]> segment_mutexes_;
  std::vector<FlashSegmentOffsetT> ftl_; // logical_segment_id ->
                                         // flash_segment_offset

  struct ValidConfigTag {};
  AdaptiveLog(Config&& config,
              ZoneManager& zns_mgr,
              SetIdFunctionT setid_fn,
              ValidConfigTag);

  Buffer lookupBuffered(HashedKey hk, LogicalPageOffset logical_page_offset);

  size_t getObjectSize(HashedKey hk, BufferView value) {
    return hk.key().size() + value.size();
  }

  // Buffer related function
  bool isSegmentInBuffer(FlashSegmentOffsetT flash_segment_offset) {
    return flash_segment_offset >= kBufferedFlashSegmentOffsetMask;
  }

  bool isSegmentNull(FlashSegmentOffsetT flash_segment_offset) {
    return flash_segment_offset == kNullFlashSegmentOffset;
  }

  // Partition related function
  uint64_t getIndexPartitionId(HashedKey hk) const {
    return setid_fn_(hk.keyHash()) / num_buckets_per_index_partition_;
  }

  uint64_t getFlashPartitionId(HashedKey hk) const {
    return getIndexPartitionId(hk) % num_flash_partitions_;
  }

  uint64_t getReadmitFlashPartitionId(HashedKey hk) const {
    return getFlashPartitionId(hk) + num_flash_partitions_;
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

  folly::SharedMutex& getPageMutex(
      LogicalPageOffset logical_page_offset) const {
    return getSegmentMutex(getLogicalSegmentId(logical_page_offset));
  }

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

  Status readmitFlushLogSegment(uint32_t flush_zone_id,
                                uint32_t logical_segment_id,
                                uint64_t flash_partition_id);

  Status reclaimSegment(uint32_t new_zone_id,
                        uint32_t logical_segment_id,
                        Buffer& buffer,
                        LogSegment::Iterator& iter);

  Status readmit(HashedKey hk, BufferView value, uint32_t flush_zone_id);

  Buffer readValueFromSegment(HashedKey hk,
                              LogicalPageOffset logical_page_offset);

  Buffer readLogPage(FlashPageOffsetT flash_page_offset);

  void setupNewBufferedPartitionSegment(uint64_t flash_partition_id) {
    // Theoretically, there is no need to acquire segment mutexes for this
    // function Since the caller may hold the partition lock and no new
    // insertment for this newly segment will concurrently happened.
    uint32_t next_logical_segment_id = getNextLogicalSegmentId();
    buffered_partition_segment_arr_[flash_partition_id]->reset(
        next_logical_segment_id);
    ftl_[next_logical_segment_id] =
        getBufferedFlashSegmentOffset(flash_partition_id);
  }

  double calculateInvalidRate(std::vector<uint32_t> logical_segment_arr);

  mutable AtomicCounter item_count_;
  mutable AtomicCounter hit_count_;
};

} // namespace navy
} // namespace cachelib
} // namespace facebook