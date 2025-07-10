#pragma once

#include <cstdint>
#include <vector>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/Hash.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/zone_hash/scheduler/Event.h"
#include "cachelib/navy/zone_hash/storage/AdaptiveAsyncStorageManager.h"
#include "cachelib/navy/zone_hash/storage/ZoneManager.h"
#include "cachelib/navy/zone_hash/utils/ClockSegmentIndex.h"
#include "cachelib/navy/zone_hash/utils/LogSegment.h"
#include "cachelib/navy/zone_hash/utils/Types.h"
#include "coro/latch.hpp"

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
    uint16_t allocation_size = 1024;

    // flash related settings
    ZoneNandType zone_nand_type = ZoneNandType::SLC;
    uint64_t zone_capacity_byte;
    uint64_t flash_partitions;
    uint32_t initial_num_zones;
    uint32_t num_free_zones = 1;
    uint32_t max_num_zones;

    uint32_t num_threads = 32;

    uint64_t maxSize() const { return max_num_zones * zone_capacity_byte; }

    uint64_t maxBuckets() const { return maxSize() / page_size_byte; }

    uint16_t initialHotDataPct() const { return 70; }

    Config& validate();
  };

  explicit AdaptiveLog(Config&& config,
                       ZoneManager& zns_mgr,
                       JobScheduler& scheduler,
                       TrackAdaptiveEvictFn track_fn);

  ~AdaptiveLog() = default;

  AdaptiveLog(const AdaptiveLog&) = delete;
  AdaptiveLog& operator=(const AdaptiveLog&) = delete;

  void track(HashedKey hk);

  bool admissionTest(HashedKey hk);

  Status lookup(HashedKey hk, Buffer& value);

  Task<> insert(Buffer key_buffer, uint64_t key_hash, Buffer value_buffer);

  bool couldExist(HashedKey hk);

  uint64_t getItemCount() const { return item_count_.get(); }

  uint64_t getHitCount() const { return hit_count_.get(); }

  void changeZoneNum(uint32_t new_num_zones);

 private:
  const uint64_t num_buckets_;
  const uint64_t page_size_byte_;
  const uint64_t segment_size_byte_;
  const uint64_t zone_capacity_byte_;
  const uint64_t num_pages_per_segment_;
  const uint64_t num_flash_partitions_;
  const uint64_t num_index_partitions_;
  const uint64_t num_buckets_per_index_partition_;
  const uint64_t max_num_segments_;
  const uint16_t hot_data_pct_;
  uint32_t num_zones_;

  AdaptiveAsyncStorageManager async_mgr_;
  AtomicIncrementCounter next_logical_segment_id_;

  std::unique_ptr<folly::SharedMutex[]> flash_partition_mutexes_;
  std::vector<std::mutex> flush_mutexes_;
  std::vector<Event> flushed_;

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

  JobScheduler& scheduler_;
  TrackAdaptiveEvictFn track_fn_;

  struct ValidConfigTag {};
  AdaptiveLog(Config&& config,
              ZoneManager& zns_mgr,
              JobScheduler& scheduler,
              TrackAdaptiveEvictFn track_fn,
              ValidConfigTag);

  Buffer lookupBuffered(HashedKey hk, LogicalPageOffset logical_page_offset);

  uint64_t indexPartitionHotDataThreshold(uint32_t num_zones) const {
    uint64_t total_capacity = num_zones * zone_capacity_byte_;
    uint64_t index_partition_capacity = total_capacity / num_index_partitions_;

    return index_partition_capacity * hot_data_pct_ / 100;
  }

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
  uint32_t getBucketId(HashedKey hk) const {
    return hk.keyHash() % num_buckets_;
  }

  uint32_t getBucketId(uint64_t key_hash) const {
    return key_hash % num_buckets_;
  }

  uint64_t getIndexPartitionId(HashedKey hk) const {
    return getBucketId(hk.keyHash()) / num_buckets_per_index_partition_;
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

  Task<> flushLogSegment(uint32_t logical_segment_id,
                         uint64_t flash_partition_id);

  Task<> readmitFlushLogSegment(uint32_t flush_zone_id,
                                uint32_t logical_segment_id,
                                uint64_t flash_partition_id);

  Task<> reclaimSegment(uint32_t new_zone_id,
                        uint32_t logical_segment_id,
                        coro::latch& latch);

  Task<> removeSegment(uint32_t logical_segment_id, coro::latch& latch);

  Task<> readmit(Buffer key_buffer,
                 uint64_t key_hash,
                 Buffer value_buffer,
                 uint32_t flush_zone_id,
                 coro::latch& l);

  Buffer readValueFromSegment(HashedKey hk,
                              LogicalPageOffset logical_page_offset);

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