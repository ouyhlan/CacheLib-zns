#include "cachelib/navy/zone_hash/AdaptiveLog.h"

#include <folly/Executor.h>
#include <folly/Format.h>
#include <folly/SharedMutex.h>
#include <folly/logging/xlog.h>

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <vector>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/scheduler/JobScheduler.h"
#include "cachelib/navy/zone_hash/scheduler/IOScheduler.h"
#include "cachelib/navy/zone_hash/utils/ClockSegmentIndex.h"
#include "cachelib/navy/zone_hash/utils/LogSegment.h"
#include "cachelib/navy/zone_hash/utils/Types.h"
#include "coro/latch.hpp"

namespace facebook {
namespace cachelib {
namespace navy {

AdaptiveLog::AdaptiveLog(Config&& config,
                         ZoneManager& zns_mgr,
                         JobScheduler& scheduler,
                         TrackAdaptiveEvictFn track_fn)
    : AdaptiveLog(std::move(config.validate()),
                  zns_mgr,
                  scheduler,
                  track_fn,
                  ValidConfigTag{}) {}

AdaptiveLog::AdaptiveLog(Config&& config,
                         ZoneManager& zns_mgr,
                         JobScheduler& scheduler,
                         TrackAdaptiveEvictFn track_fn,
                         ValidConfigTag)
    : num_buckets_(config.maxBuckets()),
      page_size_byte_(config.page_size_byte),
      segment_size_byte_(config.segment_size_byte),
      zone_capacity_byte_(config.zone_capacity_byte),
      num_pages_per_segment_(segment_size_byte_ / page_size_byte_),
      num_flash_partitions_(config.flash_partitions),
      num_index_partitions_(config.index_partitions),
      num_buckets_per_index_partition_(num_buckets_ / num_index_partitions_),
      max_num_segments_(config.maxSize() / segment_size_byte_ +
                        num_flash_partitions_ * 2),
      hot_data_pct_(config.initialHotDataPct()),
      num_zones_(config.initial_num_zones),
      async_mgr_(
          zns_mgr,
          scheduler,
          config.zone_nand_type,
          config.initial_num_zones,
          config.num_free_zones,
          segment_size_byte_,
          page_size_byte_,
          [this](std::vector<uint32_t> logical_segment_arr) {
            return calculateInvalidRate(logical_segment_arr);
          },
          [this](uint32_t zone_id, uint32_t segment_id, coro::latch& l) {
            reclaimSegment(zone_id, segment_id, l).detach();
          },
          [this](uint32_t remove_id, coro::latch& l) {
            removeSegment(remove_id, l).detach();
          }),
      next_logical_segment_id_(max_num_segments_),
      flush_mutexes_(num_flash_partitions_ * 2),
      flushed_(num_flash_partitions_ * 2),
      buffered_partition_segment_arr_(num_flash_partitions_ * 2),
      flash_partition_buffer_arr_(num_flash_partitions_ * 2),
      segment_valid_size_arr_(max_num_segments_, AtomicCounter(0)),
      index_(num_index_partitions_),
      ftl_(max_num_segments_, kNullFlashSegmentOffset),
      scheduler_(scheduler),
      track_fn_(track_fn) {
  XLOG(INFO,
       folly::sformat(
           "AdaptiveLog created: max num segments: {}, segment size: {}, "
           "flash partitions: {}, index partitions: {}, max buckets: {}, hot "
           "data pct: {}, initial num zones: {}",
           max_num_segments_, segment_size_byte_, num_flash_partitions_,
           num_index_partitions_, config.maxBuckets(), hot_data_pct_,
           config.initial_num_zones));

  flash_partition_mutexes_ =
      std::make_unique<folly::SharedMutex[]>(num_flash_partitions_ * 2);
  segment_mutexes_ = std::make_unique<folly::SharedMutex[]>(kNumSegmentMutexes);

  CollectValidFn collect_fn = [this](LogicalPageOffset logical_page_offset,
                                     int64_t size_diff) {
    uint32_t logical_segment_id = getLogicalSegmentId(logical_page_offset);
    if (size_diff > 0) {
      segment_valid_size_arr_[logical_segment_id].add(size_diff);
    } else {
      uint64_t sub_diff = -size_diff;
      if (segment_valid_size_arr_[logical_segment_id].get() < sub_diff) {
        return;
      }
      segment_valid_size_arr_[logical_segment_id].sub(sub_diff);
    }

    XDCHECK_LE(segment_valid_size_arr_[logical_segment_id].get(),
               segment_size_byte_);
  };

  auto setid_fn = [this](uint64_t key_hash) { return getBucketId(key_hash); };

  for (uint64_t i = 0; i < num_index_partitions_; i++) {
    index_[i] = std::make_unique<ClockSegmentIndex>(
        num_buckets_per_index_partition_, config.allocation_size,
        indexPartitionHotDataThreshold(num_zones_), config.bf_num_hashes,
        config.bf_bucket_bytes, setid_fn, collect_fn);
  }

  for (uint64_t i = 0; i < num_flash_partitions_ * 2; i++) {
    uint32_t curr_logical_segment_id = next_logical_segment_id_.get();

    flash_partition_buffer_arr_[i] =
        async_mgr_.makeIOBuffer(segment_size_byte_);
    buffered_partition_segment_arr_[i] = std::make_unique<LogSegment>(
        curr_logical_segment_id, segment_size_byte_, page_size_byte_,
        flash_partition_buffer_arr_[i].mutableView(), true);
    ftl_[curr_logical_segment_id] = kBufferedFlashSegmentOffsetMask + i;

    flushed_[i].set();
  }
}

void AdaptiveLog::track(HashedKey hk) {
  uint64_t index_partition_id = getIndexPartitionId(hk);
  index_[index_partition_id]->track(hk);
}

bool AdaptiveLog::admissionTest(HashedKey hk) {
  uint64_t index_partition_id = getIndexPartitionId(hk);
  return index_[index_partition_id]->admissionTest(hk);
}

Status AdaptiveLog::lookup(HashedKey hk, Buffer& value) {
  uint64_t index_partition_id = getIndexPartitionId(hk);

  auto [res, logical_page_offset] =
      index_[index_partition_id]->lookup(hk, true);
  if (res != Status::Ok) {
    return res;
  }

  value = readValueFromSegment(hk, logical_page_offset);
  if (value.isNull()) {
    return Status::NotFound;
  }

  hit_count_.inc();
  return Status::Ok;
}

Buffer AdaptiveLog::lookupBuffered(HashedKey hk,
                                   LogicalPageOffset logical_page_offset) {
  uint64_t flash_partition_id = getFlashPartitionId(hk);
  BufferView value_view;
  {
    std::shared_lock<folly::SharedMutex> buffer_lock(
        getFlashPartitionMutex(flash_partition_id));

    value_view = buffered_partition_segment_arr_[flash_partition_id]->find(
        hk, logical_page_offset);
  }
  if (value_view.isNull()) {
    return {};
  }

  return Buffer(value_view);
}

Task<> AdaptiveLog::insert(Buffer key_buffer,
                           uint64_t key_hash,
                           Buffer value_buffer) {
  HashedKey hk =
      HashedKey::precomputed(toStringPiece(key_buffer.view()), key_hash);
  BufferView value = value_buffer.view();

  Status res;
  while (true) {
    LogicalPageOffset inserted_logical_page_offset;
    uint32_t need_flush_logical_segment_id;
    uint64_t flash_partition_id = getFlashPartitionId(hk);

    co_await flushed_[flash_partition_id];
    {
      std::shared_lock<folly::SharedMutex> buffer_lock(
          getFlashPartitionMutex(flash_partition_id));

      auto [status, logical_page_offset] =
          buffered_partition_segment_arr_[flash_partition_id]->insert(hk,
                                                                      value);
      if (status != Status::Ok) {
        need_flush_logical_segment_id =
            buffered_partition_segment_arr_[flash_partition_id]
                ->getLogicalSegmentId();
      }

      res = status;
      inserted_logical_page_offset = logical_page_offset;
    }

    if (res == Status::Ok) {
      uint64_t index_partition_id = getIndexPartitionId(hk);
      index_[index_partition_id]->insert(hk, getObjectSize(hk, value),
                                         inserted_logical_page_offset);

      item_count_.inc();
      co_return;
    }

    if (flush_mutexes_[flash_partition_id].try_lock()) {
      flushLogSegment(need_flush_logical_segment_id, flash_partition_id)
          .detach();
    }
  }
}

bool AdaptiveLog::couldExist(HashedKey hk) {
  uint64_t index_partition_id = getIndexPartitionId(hk);

  auto [res, logical_page_offset] =
      index_[index_partition_id]->lookup(hk, false);
  if (res != Status::Ok) {
    return false;
  }

  return true;
}

void AdaptiveLog::changeZoneNum(uint32_t new_num_zones) {
  if (new_num_zones == num_zones_) {
    return;
  }

  for (uint64_t i = 0; i < num_index_partitions_; i++) {
    index_[i]->setHotDataThreshold(
        indexPartitionHotDataThreshold(new_num_zones));
  }

  if (new_num_zones > num_zones_) {
    async_mgr_.addExternalZoneNum(new_num_zones - num_zones_);
  }
  num_zones_ = new_num_zones;
}

Task<> AdaptiveLog::flushLogSegment(uint32_t logical_segment_id,
                                    uint64_t flash_partition_id) {
  {
    std::unique_lock<folly::SharedMutex> buffer_lock(
        getFlashPartitionMutex(flash_partition_id));

    // determined if current log segment has been flushed
    if (buffered_partition_segment_arr_[flash_partition_id]
            ->getLogicalSegmentId() != logical_segment_id) {
      flush_mutexes_[flash_partition_id].unlock();
      co_return;
    }
    
    buffered_partition_segment_arr_[flash_partition_id]->setFlushing();
  }

  flushed_[flash_partition_id].reset();

  auto update_index_fn = [this](uint32_t logical_segment_id,
                                FlashSegmentOffsetT flash_segment_offset) {
    setupLogicalSegmentNewOffset(logical_segment_id, flash_segment_offset);
  };
  co_await async_mgr_.flushSegment(
      logical_segment_id,
      Buffer(flash_partition_buffer_arr_[flash_partition_id].view(),
             page_size_byte_),
      update_index_fn);

  uint32_t next_logical_segment_id = getNextLogicalSegmentId();
  {
    // update new buffer segment
    std::unique_lock<folly::SharedMutex> segment_lock(
        getSegmentMutex(next_logical_segment_id));

    std::unique_lock<folly::SharedMutex> buffer_lock(
        getFlashPartitionMutex(flash_partition_id));

    buffered_partition_segment_arr_[flash_partition_id]->reset(
        next_logical_segment_id);

    ftl_[next_logical_segment_id] =
        getBufferedFlashSegmentOffset(flash_partition_id);
  }

  flush_mutexes_[flash_partition_id].unlock();
  flushed_[flash_partition_id].set(scheduler_, JobType::Write);
}

Task<> AdaptiveLog::readmitFlushLogSegment(uint32_t flush_zone_id,
                                           uint32_t logical_segment_id,
                                           uint64_t flash_partition_id) {
  {
    std::unique_lock<folly::SharedMutex> buffer_lock(
        getFlashPartitionMutex(flash_partition_id));

    // determined if current log segment has been flushed
    if (buffered_partition_segment_arr_[flash_partition_id]
            ->getLogicalSegmentId() != logical_segment_id) {
      flush_mutexes_[flash_partition_id].unlock();
      co_return;
    }

    buffered_partition_segment_arr_[flash_partition_id]->setFlushing();
  }

  flushed_[flash_partition_id].reset();

  auto update_index_fn = [this](uint32_t logical_segment_id,
                                FlashSegmentOffsetT flash_segment_offset) {
    setupLogicalSegmentNewOffset(logical_segment_id, flash_segment_offset);
  };
  co_await async_mgr_.flushSegment(
      flush_zone_id, logical_segment_id,
      Buffer(flash_partition_buffer_arr_[flash_partition_id].view(),
             page_size_byte_),
      update_index_fn);

  uint32_t next_logical_segment_id = getNextLogicalSegmentId();
  {
    // update new buffer segment
    std::unique_lock<folly::SharedMutex> segment_lock(
        getSegmentMutex(next_logical_segment_id));

    std::unique_lock<folly::SharedMutex> buffer_lock(
        getFlashPartitionMutex(flash_partition_id));

    buffered_partition_segment_arr_[flash_partition_id]->reset(
        next_logical_segment_id);

    ftl_[next_logical_segment_id] =
        getBufferedFlashSegmentOffset(flash_partition_id);
  }

  flush_mutexes_[flash_partition_id].unlock();
  flushed_[flash_partition_id].set(scheduler_, JobType::Write);
}

Task<> AdaptiveLog::reclaimSegment(uint32_t zone_id,
                                   uint32_t logical_segment_id,
                                   coro::latch& l) {
  Buffer segment_buffer;
  FlashSegmentOffsetT flash_segment_offset;
  {
    std::shared_lock<folly::SharedMutex> segment_lock(
        getSegmentMutex(logical_segment_id));

    flash_segment_offset = ftl_[logical_segment_id];
    XDCHECK(!isSegmentInBuffer(flash_segment_offset) &&
            !isSegmentNull(flash_segment_offset));
  }

  auto& io_executor = co_await async_mgr_.asyncSchedule(AsyncJobType::Backend);
  segment_buffer =
      co_await async_mgr_.readSegment(io_executor, flash_segment_offset);
  if (segment_buffer.isNull()) {
    l.count_down();
    co_return;
  }

  LogSegment log_segment(logical_segment_id, segment_size_byte_,
                         page_size_byte_, segment_buffer.mutableView(), false);

  coro::latch segment_latch(log_segment.size());
  for (auto it = log_segment.getFirst(); !it.done();
       it = log_segment.getNext(it)) {
    uint64_t index_partition_id = getIndexPartitionId(it.hashedKey());
    if (index_[index_partition_id]->determineEviction(it.hashedKey())) {
      index_[index_partition_id]->remove(it.hashedKey());
      track_fn_(it.hashedKey().keyHash(),
                getObjectSize(it.hashedKey(), it.value()));
      item_count_.dec();
      segment_latch.count_down();
    } else {
      readmit(Buffer(makeView(it.hashedKey().key())), it.hashedKey().keyHash(),
              Buffer(it.value()), zone_id, segment_latch)
          .detach();
    }
  }

  co_await segment_latch;
  {
    std::unique_lock<folly::SharedMutex> segment_lock(
        getSegmentMutex(logical_segment_id));
    ftl_[logical_segment_id] = kNullFlashSegmentOffset;
    segment_valid_size_arr_[logical_segment_id].set(0);
  }

  l.count_down();
}

Task<> AdaptiveLog::removeSegment(uint32_t logical_segment_id,
                                  coro::latch& latch) {
  Buffer segment_buffer;
  FlashSegmentOffsetT flash_segment_offset;
  {
    std::shared_lock<folly::SharedMutex> segment_lock(
        getSegmentMutex(logical_segment_id));

    flash_segment_offset = ftl_[logical_segment_id];
    XDCHECK(!isSegmentInBuffer(flash_segment_offset) &&
            !isSegmentNull(flash_segment_offset));
  }

  auto& io_executor = co_await async_mgr_.asyncSchedule(AsyncJobType::Backend);
  segment_buffer =
      co_await async_mgr_.readSegment(io_executor, flash_segment_offset);
  if (segment_buffer.isNull()) {
    co_return;
  }

  LogSegment log_segment(logical_segment_id, segment_size_byte_,
                         page_size_byte_, segment_buffer.mutableView(), false);
  for (auto it = log_segment.getFirst(); !it.done();
       it = log_segment.getNext(it)) {
    uint64_t index_partition_id = getIndexPartitionId(it.hashedKey());

    // remove directly
    index_[index_partition_id]->remove(it.hashedKey());
    track_fn_(it.hashedKey().keyHash(),
              getObjectSize(it.hashedKey(), it.value()));
    item_count_.dec();
  }

  {
    std::unique_lock<folly::SharedMutex> segment_lock(
        getSegmentMutex(logical_segment_id));
    ftl_[logical_segment_id] = kNullFlashSegmentOffset;
    segment_valid_size_arr_[logical_segment_id].set(0);
  }

  latch.count_down();
}

Task<> AdaptiveLog::readmit(Buffer key_buffer,
                            uint64_t key_hash,
                            Buffer value_buffer,
                            uint32_t flush_zone_id,
                            coro::latch& l) {
  HashedKey hk =
      HashedKey::precomputed(toStringPiece(key_buffer.view()), key_hash);
  BufferView value = value_buffer.view();

  Status res;
  while (true) {
    LogicalPageOffset inserted_logical_page_offset;
    uint64_t index_partition_id = getIndexPartitionId(hk);
    uint32_t need_flush_logical_segment_id;
    uint64_t flash_partition_id = getReadmitFlashPartitionId(hk);

    co_await flushed_[flash_partition_id];
    if (index_[index_partition_id]->determineEviction(hk)) {
      index_[index_partition_id]->remove(hk);
      item_count_.dec();
      l.count_down();
      co_return;
    } else {
      std::shared_lock<folly::SharedMutex> buffer_lock(
          getFlashPartitionMutex(flash_partition_id));

      auto [status, logical_page_offset] =
          buffered_partition_segment_arr_[flash_partition_id]->insert(hk,
                                                                      value);
      if (status != Status::Ok) {
        need_flush_logical_segment_id =
            buffered_partition_segment_arr_[flash_partition_id]
                ->getLogicalSegmentId();
      }

      res = status;
      inserted_logical_page_offset = logical_page_offset;
    }

    if (res == Status::Ok) {
      index_[index_partition_id]->readmit(hk, getObjectSize(hk, value),
                                          inserted_logical_page_offset);
      l.count_down();
      co_return;
    }

    if (flush_mutexes_[flash_partition_id].try_lock()) {
      readmitFlushLogSegment(flush_zone_id, need_flush_logical_segment_id,
                             flash_partition_id)
          .detach();
    }
  }
}

Buffer AdaptiveLog::readValueFromSegment(
    HashedKey hk, LogicalPageOffset logical_page_offset) {
  Buffer page_buffer;
  uint32_t logical_segment_id = getLogicalSegmentId(logical_page_offset);
  FlashSegmentOffsetT flash_segment_offset;
  {
    std::shared_lock<folly::SharedMutex> segment_lock(
        getSegmentMutex(logical_segment_id));

    // translate logical address into physical address
    flash_segment_offset = ftl_[logical_segment_id];
    if (isSegmentNull(flash_segment_offset)) {
      return {};
    } else if (isSegmentInBuffer(flash_segment_offset)) {
      return lookupBuffered(hk, logical_page_offset);
    }
  }

  // fetch kv from ssd
  page_buffer = async_mgr_.readPage(
      getFlashPageOffset(flash_segment_offset, logical_page_offset));
  if (page_buffer.isNull()) {
    return {};
  }

  ZoneBucket* page = reinterpret_cast<ZoneBucket*>(page_buffer.data());
  BufferView value_view = page->find(hk);
  if (value_view.isNull()) {
    return {};
  }
  return Buffer(value_view);
}

double AdaptiveLog::calculateInvalidRate(
    std::vector<uint32_t> logical_segment_arr) {
  uint64_t valid_size = 0;
  uint64_t total_size = logical_segment_arr.size() * segment_size_byte_;
  for (auto logical_segment_id : logical_segment_arr) {
    valid_size += segment_valid_size_arr_[logical_segment_id].get();
  }

  XDCHECK(valid_size <= total_size);
  return (double)(total_size - valid_size) / total_size;
}

AdaptiveLog::Config& AdaptiveLog::Config::validate() {
  uint32_t max_num_segments =
      maxSize() / segment_size_byte + flash_partitions * 2;
  if (max_num_segments >= kBufferedFlashSegmentOffsetMask) {
    throw std::invalid_argument(
        folly::sformat("num segment {} cannot be greater than {}",
                       max_num_segments, kBufferedFlashSegmentOffsetMask));
  }
  return *this;
}

} // namespace navy
} // namespace cachelib
} // namespace facebook