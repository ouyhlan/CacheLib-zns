#include "cachelib/navy/zone_hash/AdaptiveLog.h"

#include <folly/Executor.h>
#include <folly/Format.h>
#include <folly/SharedMutex.h>
#include <folly/logging/xlog.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/zone_hash/storage/AdaptiveStorageManager.h"
#include "cachelib/navy/zone_hash/utils/ClockSegmentIndex.h"
#include "cachelib/navy/zone_hash/utils/LogSegment.h"
#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

AdaptiveLog::AdaptiveLog(Config&& config,
                         ZoneManager& zns_mgr,
                         SetIdFunctionT setid_fn)
    : AdaptiveLog(
          std::move(config.validate()), zns_mgr, setid_fn, ValidConfigTag{}) {}

AdaptiveLog::AdaptiveLog(Config&& config,
                         ZoneManager& zns_mgr,
                         SetIdFunctionT setid_fn,
                         ValidConfigTag)
    : page_size_byte_(config.page_size_byte),
      segment_size_byte_(config.segment_size_byte),
      num_pages_per_segment_(segment_size_byte_ / page_size_byte_),
      num_flash_partitions_(config.flash_partitions),
      num_index_partitions_(config.index_partitions),
      num_buckets_per_index_partition_(config.num_index_buckets /
                                       num_index_partitions_),
      max_num_segments_(config.maxSize() / config.page_size_byte +
                        num_flash_partitions_ * 2),
      setid_fn_(setid_fn),
      storage_mgr_(
          zns_mgr,
          config.zone_nand_type,
          config.initial_num_zones,
          config.num_free_zones,
          segment_size_byte_,
          page_size_byte_,
          [this](std::vector<uint32_t> logical_segment_arr) {
            return calculateInvalidRate(logical_segment_arr);
          },
          [this](uint32_t zone_id,
                 uint32_t segment_id,
                 Buffer& buf,
                 LogSegment::Iterator& it) {
            return reclaimSegment(zone_id, segment_id, buf, it);
          },
          config.num_threads),
      next_logical_segment_id_(max_num_segments_),
      buffered_partition_segment_arr_(num_flash_partitions_ * 2),
      flash_partition_buffer_arr_(num_flash_partitions_ * 2),
      segment_valid_size_arr_(max_num_segments_, AtomicCounter(0)),
      index_(num_index_partitions_),
      ftl_(max_num_segments_, kNullFlashSegmentOffset) {
  XLOG(INFO, folly::sformat(
                 "AdaptiveLog created: max num segments: {}, segment size: {}, "
                 "flash partitions: {}, index partitions: {}, buckets: {}",
                 max_num_segments_, segment_size_byte_, num_flash_partitions_,
                 num_index_partitions_, config.num_index_buckets));

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
      XDCHECK(segment_valid_size_arr_[logical_segment_id].get() >= sub_diff);
      segment_valid_size_arr_[logical_segment_id].sub(sub_diff);
    }

    XDCHECK(segment_valid_size_arr_[logical_segment_id].get() <=
            segment_size_byte_);
  };

  for (uint64_t i = 0; i < num_index_partitions_; i++) {
    index_[i] = std::make_unique<ClockSegmentIndex>(
        num_buckets_per_index_partition_, config.allocation_size,
        config.indexPartitionHotDataThreshold(), config.bf_num_hashes,
        config.bf_bucket_bytes, setid_fn_, collect_fn);
  }

  for (uint64_t i = 0; i < num_flash_partitions_ * 2; i++) {
    uint32_t curr_logical_segment_id = next_logical_segment_id_.get();

    flash_partition_buffer_arr_[i] =
        storage_mgr_.makeIOBuffer(segment_size_byte_);
    buffered_partition_segment_arr_[i] = std::make_unique<LogSegment>(
        curr_logical_segment_id, segment_size_byte_, page_size_byte_,
        flash_partition_buffer_arr_[i].mutableView(), true);
    ftl_[curr_logical_segment_id] = kBufferedFlashSegmentOffsetMask + i;
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

Status AdaptiveLog::insert(HashedKey hk, BufferView value) {
  Status res;
  LogicalPageOffset inserted_logical_page_offset;
  uint32_t need_flush_logical_segment_id;
  uint64_t flash_partition_id = getFlashPartitionId(hk);
  {
    std::shared_lock<folly::SharedMutex> buffer_lock(
        getFlashPartitionMutex(flash_partition_id));

    auto [status, logical_page_offset] =
        buffered_partition_segment_arr_[flash_partition_id]->insert(hk, value);
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
    auto ret = index_[index_partition_id]->insert(hk, getObjectSize(hk, value),
                                                  inserted_logical_page_offset);

    item_count_.inc();

    return ret;
  }

  Status flush_status =
      flushLogSegment(need_flush_logical_segment_id, flash_partition_id);

  // retry the whole procedure since flush is completed
  if (flush_status == Status::Ok) {
    return insert(hk, value);
  }

  return flush_status;
}

bool AdaptiveLog::couldExist(HashedKey hk) {
  uint64_t index_partition_id = getIndexPartitionId(hk);
  uint64_t flash_partition_id = getFlashPartitionId(hk);

  auto [res, logical_page_offset] =
      index_[index_partition_id]->lookup(hk, false);
  if (res != Status::Ok) {
    return false;
  }

  return true;
}

Status AdaptiveLog::flushLogSegment(uint32_t logical_segment_id,
                                    uint64_t flash_partition_id) {
  {
    std::unique_lock<folly::SharedMutex> buffer_lock(
        getFlashPartitionMutex(flash_partition_id));

    // determined if current log segment has been flushed
    if (buffered_partition_segment_arr_[flash_partition_id]
            ->getLogicalSegmentId() != logical_segment_id) {
      return Status::Ok;
    }

    if (!buffered_partition_segment_arr_[flash_partition_id]
             ->tryLockForFlushing()) {
      return Status::Retry;
    }
  }

  auto callback = [this, logical_segment_id, flash_partition_id](
                      FlashSegmentOffsetT flash_segment_offset) {
    setupLogicalSegmentNewOffset(logical_segment_id, flash_segment_offset);
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
  };

  // if not, flush it by this thread
  storage_mgr_.flushSegment(
      logical_segment_id,
      Buffer(flash_partition_buffer_arr_[flash_partition_id].view(),
             page_size_byte_),
      std::move(callback));
  return Status::Retry;
}

Status AdaptiveLog::readmitFlushLogSegment(uint32_t flush_zone_id,
                                           uint32_t logical_segment_id,
                                           uint64_t flash_partition_id) {
  {
    std::unique_lock<folly::SharedMutex> buffer_lock(
        getFlashPartitionMutex(flash_partition_id));

    // determined if current log segment has been flushed
    if (buffered_partition_segment_arr_[flash_partition_id]
            ->getLogicalSegmentId() != logical_segment_id) {
      return Status::Ok;
    }

    if (!buffered_partition_segment_arr_[flash_partition_id]
             ->tryLockForFlushing()) {
      return Status::Retry;
    }
  }

  // if not, flush it by this thread
  FlashSegmentOffsetT flash_segment_offset = storage_mgr_.segmentAppend(
      flush_zone_id,
      logical_segment_id,
      Buffer(flash_partition_buffer_arr_[flash_partition_id].view(),
             page_size_byte_));
  setupLogicalSegmentNewOffset(logical_segment_id, flash_segment_offset);

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

  return Status::Ok;
}

Status AdaptiveLog::reclaimSegment(uint32_t zone_id,
                                   uint32_t logical_segment_id,
                                   Buffer& buffer,
                                   LogSegment::Iterator& iter) {
  bool initial = false;
  if (buffer.isNull()) {
    std::shared_lock<folly::SharedMutex> segment_lock(
        getSegmentMutex(logical_segment_id));

    FlashSegmentOffsetT flash_segment_offset = ftl_[logical_segment_id];
    XDCHECK(!isSegmentInBuffer(flash_segment_offset) &&
            !isSegmentNull(flash_segment_offset));

    buffer = storage_mgr_.readSegment(flash_segment_offset);
    if (buffer.isNull()) {
      return Status::DeviceError;
    }

    initial = true;
  }

  LogSegment log_segment(logical_segment_id, segment_size_byte_,
                         page_size_byte_, buffer.mutableView(), false);

  if (initial) {
    iter = log_segment.getFirst();
  }

  for (; !iter.done(); iter = log_segment.getNext(iter)) {
    uint64_t index_partition_id = getIndexPartitionId(iter.hashedKey());
    if (index_[index_partition_id]->determineEviction(iter.hashedKey())) {
      index_[index_partition_id]->remove(iter.hashedKey());
    } else {
      Status status = readmit(iter.hashedKey(), iter.value(), zone_id);
      if (status != Status::Ok) {
        return status;
      }
    }

    item_count_.dec();
  }

  {
    std::unique_lock<folly::SharedMutex> segment_lock(
        getSegmentMutex(logical_segment_id));
    ftl_[logical_segment_id] = kNullFlashSegmentOffset;
    segment_valid_size_arr_[logical_segment_id].set(0);
  }

  return Status::Ok;
}

Status AdaptiveLog::readmit(HashedKey hk,
                            BufferView value,
                            uint32_t flush_zone_id) {
  Status res;
  LogicalPageOffset inserted_logical_page_offset;
  uint32_t need_flush_logical_segment_id;
  uint64_t flash_partition_id = getReadmitFlashPartitionId(hk);
  {
    std::shared_lock<folly::SharedMutex> buffer_lock(
        getFlashPartitionMutex(flash_partition_id));

    auto [status, logical_page_offset] =
        buffered_partition_segment_arr_[flash_partition_id]->insert(hk, value);
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
    auto ret = index_[index_partition_id]->readmit(
        hk, getObjectSize(hk, value), inserted_logical_page_offset);

    item_count_.inc();

    return ret;
  }

  Status flush_status = readmitFlushLogSegment(
      flush_zone_id, need_flush_logical_segment_id, flash_partition_id);

  // retry the whole procedure since flush is completed
  if (flush_status == Status::Ok) {
    return readmit(hk, value, flush_zone_id);
  }

  return flush_status;
}

Buffer AdaptiveLog::readValueFromSegment(
    HashedKey hk, LogicalPageOffset logical_page_offset) {
  Buffer page_buffer;
  uint32_t logical_segment_id = getLogicalSegmentId(logical_page_offset);
  {
    std::shared_lock<folly::SharedMutex> segment_lock(
        getSegmentMutex(logical_segment_id));

    // translate logical address into physical address
    FlashSegmentOffsetT flash_segment_offset = ftl_[logical_segment_id];
    if (isSegmentNull(flash_segment_offset)) {
      return {};
    } else if (isSegmentInBuffer(flash_segment_offset)) {
      return lookupBuffered(hk, logical_page_offset);
    }
    // fetch kv from ssd
    page_buffer = readLogPage(
        getFlashPageOffset(flash_segment_offset, logical_page_offset));
    if (page_buffer.isNull()) {
      return {};
    }
  }

  ZoneBucket* page = reinterpret_cast<ZoneBucket*>(page_buffer.data());
  BufferView value_view = page->find(hk);
  if (value_view.isNull()) {
    return {};
  }
  return Buffer(value_view);
}

Buffer AdaptiveLog::readLogPage(FlashPageOffsetT flash_page_offset) {
  return storage_mgr_.readPage(flash_page_offset);
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