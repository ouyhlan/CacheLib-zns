#pragma once

#include <cstdint>
#include <memory>
#include <tuple>
#include <vector>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/Hash.h"
#include "cachelib/navy/zone_hash/storage/SetStorageManager.h"
#include "cachelib/navy/zone_hash/storage/ZNSDevice.h"
#include "cachelib/navy/zone_hash/storage/ZoneManager.h"
#include "cachelib/navy/zone_hash/utils/CuckooHashMap.h"
#include "cachelib/navy/zone_hash/utils/Types.h"
#include "cachelib/navy/zone_hash/utils/ZoneBucket.h"

namespace facebook {
namespace cachelib {
namespace navy {

class CuckooSet {
 public:
  struct Config {
    // metadata
    uint32_t page_size_byte = 4 * 1024;
    uint64_t max_size_byte;

    uint64_t admit_size_byte = 2 * 1024;

    // Bloom Filter related
    uint32_t bf_num_hashes = 4;

    // The bloom filter size per bucket in bytes
    uint32_t bf_bucket_bytes = 8;

    // Device related
    ZoneNandType zone_nand_type = ZoneNandType::QLC;
    uint32_t initial_num_zones;
    uint32_t num_clean_zones;

    uint32_t num_threads = 32;

    uint64_t numPages() const { return max_size_byte / page_size_byte; }

    Config& validate();
  };

  explicit CuckooSet(Config&& config, ZoneManager& zns_mgr);

  ~CuckooSet() = default;

  CuckooSet(const CuckooSet&) = delete;
  CuckooSet& operator=(const CuckooSet&) = delete;

  void insertPage(std::vector<ObjectInfo>&& object_vec,
                  LogReadmitCallback readmit);

  Status lookup(HashedKey hk, Buffer& value);

  bool couldExist(HashedKey hk);

  SetIdT getSetId(HashedKey hk) { return hk.keyHash() % num_buckets_; }

  SetIdT getSetId(uint64_t key_hash) { return key_hash % num_buckets_; }

  uint64_t getHitCount() const { return hit_count_.get(); }

 private:
  ZoneManager& zns_mgr_;
  const uint64_t num_buckets_;
  const uint64_t page_size_byte_;
  const uint64_t admit_size_byte_;
  SetStorageManager storage_mgr_;

  std::unique_ptr<BloomFilter> bloom_filter_;

  CuckooHashMap index_;

  struct ValidConfigTag {};
  CuckooSet(Config&& config, ZoneManager& zns_mgr, ValidConfigTag);

  void cleanSet(SetIdT set_id, FlashPageOffsetT removed_flash_page_offset);

  // Bloom Filter related function
  bool bfReject(FlashPageOffsetT flash_page_offset, uint64_t key_hash);

  void bfBuild(FlashPageOffsetT flash_page_offset,
               std::vector<uint64_t> key_hash_arr);

  void bfClear(FlashPageOffsetT flash_page_offset);

  mutable AtomicCounter hit_count_;
  mutable AtomicCounter io_error_count_;
  mutable AtomicCounter lookup_io_count_;
  mutable AtomicCounter lookup_count_;
};

} // namespace navy
} // namespace cachelib
} // namespace facebook