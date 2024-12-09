#include "cachelib/navy/zone_hash/SLog.h"

#include <folly/Format.h>
#include <folly/SharedMutex.h>
#include <folly/logging/xlog.h>

#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <tuple>
#include <vector>

#include "cachelib/common/Hash.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/zone_hash/utils/LogSegment.h"
#include "cachelib/navy/zone_hash/utils/SegmentIndex.h"
#include "cachelib/navy/zone_hash/utils/Types.h"
#include "cachelib/navy/zone_hash/utils/ZoneBucket.h"

namespace facebook {
namespace cachelib {
namespace navy {

SLog::SLog(Config&& config,
           ZoneManager& zns_mgr,
           SetIdFunctionT setid_fn,
           SetMultiInsertFn set_insert_fn)
    : SLog(std::move(config.validate()),
           zns_mgr,
           setid_fn,
           set_insert_fn,
           ValidConfigTag{}) {}

SLog::SLog(Config&& config,
           ZoneManager& zns_mgr,
           SetIdFunctionT setid_fn,
           SetMultiInsertFn set_insert_fn,
           ValidConfigTag)
    : page_size_byte_(config.page_size_byte),
      segment_size_byte_(config.segment_size_byte),
      num_pages_per_segment_(segment_size_byte_ / page_size_byte_),
      log_size_byte_(config.log_size_byte),
      num_flash_partitions_(config.log_flash_partitions),
      num_index_partitions_(config.log_index_partitions),
      num_log_buckets_(config.num_index_buckets),
      num_buckets_per_index_partition_(num_log_buckets_ /
                                       num_index_partitions_),
      num_segments_(log_size_byte_ / segment_size_byte_ +
                    config.log_flash_partitions),
      setid_fn_(setid_fn),
      set_multi_insert_fn_(set_insert_fn),
      storage_mgr_(zns_mgr,
                   config.zone_nand_type,
                   config.num_zones,
                   config.num_clean_zones,
                   segment_size_byte_,
                   page_size_byte_,
                   config.num_threads,
                   [&](uint32_t logical_segment_id) {
                     cleanSegment(logical_segment_id);
                   }),
      next_logical_segment_id_(num_segments_),
      buffered_partition_segment_arr_(num_flash_partitions_),
      flash_partition_buffer_arr_(num_flash_partitions_),
      index_(num_index_partitions_),
      ftl_(num_segments_, kNullFlashSegmentOffset),
      set_admit_threshold_(config.set_admit_threshold) {
  XDCHECK(num_log_buckets_ ==
          (num_buckets_per_index_partition_ * num_index_partitions_));

  XLOG(
      INFO,
      folly::sformat(
          "SLog created: log size: {}, segment size: {}, flash partitions: {}, "
          "index partitions: {}, log buckets: {}, set admit threshold:{}",
          log_size_byte_, segment_size_byte_, num_flash_partitions_,
          num_index_partitions_, num_log_buckets_, set_admit_threshold_));

  flash_partition_mutexes_ =
      std::make_unique<folly::SharedMutex[]>(num_flash_partitions_);

  segment_mutexes_ = std::make_unique<folly::SharedMutex[]>(kNumSegmentMutexes);

  // since sharemutex has move constructor = delete, we can only use pointer
  for (uint64_t i = 0; i < num_index_partitions_; i++) {
    index_[i] = std::make_unique<SegmentIndex>(
        num_buckets_per_index_partition_, config.allocation_size, setid_fn_);
  }

  for (uint64_t i = 0; i < num_flash_partitions_; i++) {
    uint32_t current_logical_segment_id = next_logical_segment_id_.get();

    flash_partition_buffer_arr_[i] =
        storage_mgr_.makeIOBuffer(segment_size_byte_);
    buffered_partition_segment_arr_[i] = std::make_unique<LogSegment>(
        current_logical_segment_id, segment_size_byte_, page_size_byte_,
        flash_partition_buffer_arr_[i].mutableView(), true);
    ftl_[current_logical_segment_id] = kBufferedFlashSegmentOffsetMask + i;
  }
}

Buffer SLog::lookupBuffered(HashedKey hk,
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
    key_collision_count_.inc();
    return {};
  }

  return Buffer(value_view);
}

auto SLog::lookupBufferedTag(uint32_t tag,
                             uint64_t flash_partition_id,
                             LogicalPageOffset logical_page_offset)
    -> std::tuple<Buffer, uint64_t, Buffer> {
  BufferView view;
  std::shared_lock<folly::SharedMutex> buffer_lock(
      getFlashPartitionMutex(flash_partition_id));

  view = buffered_partition_segment_arr_[flash_partition_id]->findTag(
      tag, logical_page_offset);
  if (view.isNull()) {
    key_collision_count_.inc();
    return {};
  }

  auto* entry = reinterpret_cast<const details::LogBucketEntry*>(view.data());
  return {Buffer(entry->key()), entry->keyHash(), Buffer(entry->value())};
}

Status SLog::lookup(HashedKey hk, Buffer& value) {
  uint64_t index_partition_id = getIndexPartitionId(hk);

  auto [res, logical_page_offset, hits] =
      index_[index_partition_id]->lookup(hk, true);
  if (res != Status::Ok) {
    return res;
  }

  value = readValueFromSegment(hk, logical_page_offset);
  if (value.isNull()) {
    index_[index_partition_id]->remove(hk, logical_page_offset);
    return Status::NotFound;
  }

  hit_count_.inc();
  return Status::Ok;
}

Status SLog::insert(HashedKey hk, BufferView value) {
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
    auto ret =
        index_[index_partition_id]->insert(hk, inserted_logical_page_offset);

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

bool SLog::couldExist(HashedKey hk) {
  uint64_t index_partition_id = getIndexPartitionId(hk);
  uint64_t flash_partition_id = getFlashPartitionId(hk);

  auto [res, logical_page_offset, hits] =
      index_[index_partition_id]->lookup(hk, false);
  if (res != Status::Ok) {
    return false;
  }

  return true;
}

Status SLog::flushLogSegment(uint32_t logical_segment_id,
                             uint64_t flash_partition_id) {
  {
    std::unique_lock<folly::SharedMutex> buffer_lock(
        getFlashPartitionMutex(flash_partition_id));

    // determined if current log segment has been flushed
    if (buffered_partition_segment_arr_[flash_partition_id]
            ->getLogicalSegmentId() != logical_segment_id) {
      // if flushed just insert the new item
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

void SLog::cleanSegment(uint32_t logical_segment_id) {
  Buffer segment_buffer;
  {
    std::shared_lock<folly::SharedMutex> segment_lock(
        getSegmentMutex(logical_segment_id));

    FlashSegmentOffsetT flash_segment_offset = ftl_[logical_segment_id];
    XDCHECK(!isSegmentInBuffer(flash_segment_offset) &&
            !isSegmentNull(flash_segment_offset));
    segment_buffer = storage_mgr_.readSegment(flash_segment_offset);
    if (segment_buffer.isNull()) {
      return;
    }
  }

  LogSegment log_segment(logical_segment_id, segment_size_byte_,
                         page_size_byte_, segment_buffer.mutableView(), false);

  for (auto it = log_segment.getFirst(); !it.done();
       it = log_segment.getNext(it)) {
    uint64_t index_partition_id = getIndexPartitionId(it.hashedKey());
    auto [status, logical_page_offset, hits] =
        index_[index_partition_id]->lookup(it.hashedKey(), false);
    if (status != Status::Ok) {
      flash_item_not_found_in_log_index_.inc();
      continue;
    }

    auto bucket_iter =
        index_[index_partition_id]->getHashBucketIterator(it.hashedKey());
    if (bucket_iter.countBucket() < set_admit_threshold_) {
      if (hits) {
        readmit(it.hashedKey(), it.value());
      } else {
        index_[index_partition_id]->remove(it.hashedKey(), logical_page_offset);
        item_count_.dec();
      }
    } else {
      moveBucket(bucket_iter, logical_segment_id);
    }
  }

  setupLogicalSegmentNewOffset(logical_segment_id, kNullFlashSegmentOffset);
}

// caller must hold the segment mutex
void SLog::moveBucket(SegmentIndex::BucketIterator& bucket_iterator,
                      uint32_t logical_segment_id_to_flush) {
  SetIdT set_id = bucket_iterator.setid();
  uint64_t index_partition_id = getIndexPartitionId(set_id);

  std::vector<ObjectInfo> object_vec;
  // Iterate all the candidate and remove the invalid item
  for (; !bucket_iterator.done(); ++bucket_iterator) {
    uint32_t hits = bucket_iterator.hits();
    uint32_t tag = bucket_iterator.tag();

    LogicalPageOffset logical_page_offset =
        bucket_iterator.logical_page_offset();
    auto [key_buffer, key_hash, value_buffer] =
        readKVFromSegment(tag, logical_page_offset);

    if (value_buffer.isNull()) {
      index_item_not_found_in_segment_.inc();
      index_[index_partition_id]->remove(tag, set_id, logical_page_offset);
      item_count_.dec();
    } else if (setid_fn_(key_hash) != set_id) {
      // false_page_read_.inc();
      continue;
    } else {
      index_[index_partition_id]->remove(tag, set_id, logical_page_offset);
      item_count_.dec();
      object_vec.push_back(ObjectInfo(std::move(key_buffer), key_hash,
                                      std::move(value_buffer),
                                      logical_page_offset, tag, hits));
    }
  }

  // TODO: log readmit policy
  LogReadmitCallback readmit_cb = [&](const ObjectInfo& object) {
    if (getLogicalSegmentId(object.logical_page_offset) !=
        logical_segment_id_to_flush) {
      // object not in the flush segment, can be wrote back to index
      index_[index_partition_id]->insert(object.tag, set_id,
                                         object.logical_page_offset);
      item_count_.inc();
    } else if (object.hits > 0) {
      readmit(object.hk, object.value.view());
    }
  };

  if (object_vec.size() < set_admit_threshold_) {
    for (auto& item : object_vec) {
      readmit_cb(item);
    }
  } else {
    constexpr auto comp_fn = [](const ObjectInfo& o1, const ObjectInfo& o2) {
      if (o1.hits != o2.hits) {
        return o1.hits > o2.hits;
      }

      return o1.size() < o2.size();
    };

    std::sort(object_vec.begin(), object_vec.end(), comp_fn);

    set_multi_insert_fn_(std::move(object_vec), readmit_cb);
  }
}

void SLog::readmit(HashedKey hk, BufferView value) {
  readmit_requests_.inc();
  uint64_t flash_partition_id = getFlashPartitionId(hk);
  LogicalPageOffset inserted_logical_page_offset;
  {
    std::shared_lock<folly::SharedMutex> buffer_lock(
        getFlashPartitionMutex(flash_partition_id));
    auto [res, logical_page_offset] =
        buffered_partition_segment_arr_[flash_partition_id]->insert(hk, value);
    if (res != Status::Ok) {
      readmit_requests_failed_.inc();
      return;
    }

    inserted_logical_page_offset = logical_page_offset;
  }

  uint64_t index_partition_id = getIndexPartitionId(hk);
  auto ret =
      index_[index_partition_id]->insert(hk, inserted_logical_page_offset);
  item_count_.inc();
}

Buffer SLog::readValueFromSegment(HashedKey hk,
                                  LogicalPageOffset logical_page_offset) {
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

auto SLog::readKVFromSegment(uint32_t tag,
                             LogicalPageOffset logical_page_offset)
    -> std::tuple<Buffer, uint64_t, Buffer> {
  Buffer page_buffer;
  Buffer kv_buffer;
  uint32_t logical_segment_id = getLogicalSegmentId(logical_page_offset);
  {
    std::shared_lock<folly::SharedMutex> segment_lock(
        getSegmentMutex(logical_segment_id));

    FlashSegmentOffsetT flash_segment_offset = ftl_[logical_segment_id];
    if (isSegmentNull(flash_segment_offset)) {
      return {};
    } else if (isSegmentInBuffer(flash_segment_offset)) {
      return lookupBufferedTag(tag, getFlashPartitionId(flash_segment_offset),
                               logical_page_offset);
    }
    // fetch kv from ssd
    page_buffer = readLogPage(
        getFlashPageOffset(flash_segment_offset, logical_page_offset));
    if (page_buffer.isNull()) {
      return {};
    }
  }

  ZoneBucket* page = reinterpret_cast<ZoneBucket*>(page_buffer.data());
  BufferView view = page->findTag(tag);
  if (view.isNull()) {
    return {};
  }

  auto* entry = reinterpret_cast<const details::LogBucketEntry*>(view.data());
  return {Buffer(entry->key()), entry->keyHash(), Buffer(entry->value())};
}

Buffer SLog::readLogPage(FlashPageOffsetT flash_page_offset) {
  return storage_mgr_.readPage(flash_page_offset);
}

// Config related
SLog::Config& SLog::Config::validate() {
  uint32_t num_segments =
      log_size_byte / segment_size_byte + log_flash_partitions;
  if (num_segments >= kBufferedFlashSegmentOffsetMask) {
    throw std::invalid_argument(
        folly::sformat("num segment {} cannot be greater than {}", num_segments,
                       kBufferedFlashSegmentOffsetMask));
  }
  return *this;
}

} // namespace navy
} // namespace cachelib
} // namespace facebook