#pragma once

#include <cstdint>
#include <memory>

#include "cachelib/common/Hash.h"
#include "cachelib/common/Serialization.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Device.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/engine/Engine.h"
#include "cachelib/navy/zone_hash/AdaptiveLog.h"
#include "cachelib/navy/zone_hash/CuckooSet.h"
#include "cachelib/navy/zone_hash/SLog.h"
#include "cachelib/navy/zone_hash/storage/ZNSDevice.h"
#include "cachelib/navy/zone_hash/storage/ZoneManager.h"
#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

class ZoneHash final : public Engine {
 public:
  struct Config {
    // metadata
    uint64_t page_size_byte = 4 * 1024;
    
    // log related
    SLog::Config log_config;

    CuckooSet::Config set_config;

    AdaptiveLog::Config adaptive_config;

    Device* device;

    uint32_t num_insert_adaptive_threads = 32;

    Config& validate();
  };

  explicit ZoneHash(Config&& config);

  ~ZoneHash() override = default;

  ZoneHash(const ZoneHash&) = delete;
  ZoneHash& operator=(const ZoneHash&) = delete;

  bool couldExist(HashedKey hk) override;

  Status lookup(HashedKey hk, Buffer& value) override;

  Status insert(HashedKey hk, BufferView value) override;

  Status remove(HashedKey hk) override;

  void flush() override;

  void reset() override;

  void persist(RecordWriter& rw) override;

  bool recover(RecordReader& rr) override;

  void getCounters(const CounterVisitor& visitor) const override;

  uint64_t getMaxItemSize() const override;

 private:
  ZoneManager zns_mgr_;
  const uint64_t page_size_byte_;
  uint64_t curr_num_adaptive_zones_;

  CuckooSet set_;
  SLog log_;
  AdaptiveLog adaptive_log_;

  ThreadPoolExecutor adaptive_threads_;

  struct ValidConfigTag {};
  ZoneHash(Config&& config, ValidConfigTag);
};

} // namespace navy
} // namespace cachelib
} // namespace facebook