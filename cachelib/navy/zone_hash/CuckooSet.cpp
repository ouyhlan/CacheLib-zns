#include "cachelib/navy/zone_hash/CuckooSet.h"

#include <folly/Format.h>
#include <folly/logging/xlog.h>

#include <algorithm>
#include <cstdint>
#include <stdexcept>
#include <utility>
#include <vector>

#include "cachelib/common/Hash.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/zone_hash/storage/SetStorageManager.h"
#include "cachelib/navy/zone_hash/storage/ZoneBucketStorage.h"
#include "cachelib/navy/zone_hash/utils/CuckooHashConfig.h"
#include "cachelib/navy/zone_hash/utils/CuckooHashMap.h"
#include "cachelib/navy/zone_hash/utils/Types.h"
#include "cachelib/navy/zone_hash/utils/ZoneBucket.h"

namespace facebook {
namespace cachelib {
namespace navy {

CuckooSet::CuckooSet(Config&& config, ZoneManager& zns_mgr)
    : CuckooSet(std::move(config.validate()), zns_mgr, ValidConfigTag()) {}

CuckooSet::CuckooSet(Config&& config, ZoneManager& zns_mgr, ValidConfigTag)
    : zns_mgr_(zns_mgr),
      num_buckets_(config.numPages() / kDefaultSlotPerBucket),
      page_size_byte_(config.page_size_byte),
      admit_size_byte_(config.admit_size_byte),
      storage_mgr_(zns_mgr,
                   config.zone_nand_type,
                   config.initial_num_zones,
                   config.num_clean_zones,
                   page_size_byte_,
                   config.num_threads,
                   [&](uint32_t set_id, FlashPageOffsetT offset) {
                     cleanSet(set_id, offset);
                   }),
      index_(num_buckets_, [&](FlashPageOffsetT a, FlashPageOffsetT b) {
        return storage_mgr_.comparePageAge(a, b);
      }) {
  XLOG(INFO,
       folly::sformat("CuckooSet created: buckets: {}, page size: {},",
                      num_buckets_,
                      page_size_byte_));

  // Initialize Bloom Filter
  uint32_t num_pages =
      zns_mgr.numZones() * zns_mgr.zoneSize() / page_size_byte_;
  uint32_t bits_per_hash = config.bf_bucket_bytes * 8 / config.bf_num_hashes;
  bloom_filter_ = std::make_unique<BloomFilter>(
      num_pages, config.bf_num_hashes, bits_per_hash);
}

void CuckooSet::insertPage(std::vector<ObjectInfo>&& object_vec,
                           LogReadmitCallback readmit) {
  // check if admit
  if (object_vec.size() == 0) {
    return;
  }

  SetIdT set_id = getSetId(object_vec[0].hk);

  auto buffer = storage_mgr_.makeIOBuffer(page_size_byte_);
  XDCHECK(!buffer.isNull());

  ZoneBucket::initNew(buffer.mutableView(), 0);
  auto* page_bucket = reinterpret_cast<ZoneBucket*>(buffer.data());

  uint64_t omit_size = 0;
  std::vector<ObjectInfo> omit_object_arr;
  std::vector<uint64_t> inserted_key_hash_arr;
  auto it = object_vec.begin();
  for (auto& object : object_vec) {
    if (getSetId(object.hk) != set_id) {
      throw std::runtime_error("Set insert wrong items!");
    }

    if (page_bucket->isSpace(object.hk, object.value.view())) {
      ZoneBucketStorage::Allocation alloc =
          page_bucket->allocate(object.hk, object.value.view());
      page_bucket->insert(alloc, object.hk, object.value.view());

      inserted_key_hash_arr.push_back(object.hk.keyHash());
    } else {
      omit_size += object.size();
      omit_object_arr.push_back(std::move(object));
    }
  }

  auto callback =
      [this, set_id, key_hash_arr = std::move(inserted_key_hash_arr)](
          FlashPageOffsetT flash_page_offset) mutable {
        bfBuild(flash_page_offset, key_hash_arr);
        index_.insert(set_id, flash_page_offset);
      };
  storage_mgr_.pageAppend(set_id, std::move(buffer), std::move(callback));

  // check if need to insert again
  if (omit_size >= admit_size_byte_) {
    insertPage(std::move(omit_object_arr), readmit);
  } else {
    for (auto& object : omit_object_arr) {
      readmit(object);
    }
  }
}

Status CuckooSet::lookup(HashedKey hk, Buffer& value) {
  lookup_count_.inc();

  SetIdT set_id = getSetId(hk);
  for (auto it = index_.getFirstIterator(set_id); !it.done();
       it = index_.getNext(it)) {
    if (bfReject(it.flash_page_offset(), hk.keyHash())) {
      continue;
    }

    Buffer buffer = storage_mgr_.readPage(it.flash_page_offset());
    if (buffer.isNull()) {
      io_error_count_.inc();
      return Status::DeviceError;
    }
    lookup_io_count_.inc();

    auto* bucket = reinterpret_cast<ZoneBucket*>(buffer.data());
    BufferView value_view = bucket->find(hk);
    if (!value_view.isNull()) {
      value = Buffer(value_view);

      hit_count_.inc();
      return Status::Ok;
    }
  }

  return Status::NotFound;
}

bool CuckooSet::couldExist(HashedKey hk) {
  SetIdT set_id = getSetId(hk);
  for (auto it = index_.getFirstIterator(set_id); !it.done();
       it = index_.getNext(it)) {
    if (!bfReject(it.flash_page_offset(), hk.keyHash())) {
      return true;
    }
  }

  return false;
}

void CuckooSet::cleanSet(SetIdT set_id,
                         FlashPageOffsetT removed_flash_page_offset) {
  index_.remove(set_id, removed_flash_page_offset);
  bfClear(removed_flash_page_offset);
}

bool CuckooSet::bfReject(FlashPageOffsetT flash_page_offset,
                         uint64_t key_hash) {
  if (bloom_filter_) {
    if (!bloom_filter_->couldExist(flash_page_offset, key_hash)) {
      return true;
    }
  }

  return false;
}

void CuckooSet::bfBuild(FlashPageOffsetT flash_page_offset,
                        std::vector<uint64_t> key_hash_arr) {
  XDCHECK(bloom_filter_);
  bloom_filter_->clear(flash_page_offset);

  for (auto key_hash : key_hash_arr) {
    bloom_filter_->set(flash_page_offset, key_hash);
  }
}

void CuckooSet::bfClear(FlashPageOffsetT flash_page_offset) {
  XDCHECK(bloom_filter_);
  bloom_filter_->clear(flash_page_offset);
}

CuckooSet::Config& CuckooSet::Config::validate() { return *this; }

} // namespace navy
} // namespace cachelib
} // namespace facebook