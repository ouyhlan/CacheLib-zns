#pragma once

#include <cstdint>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/common/Hash.h"
#include "cachelib/common/Serialization.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Device.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/engine/Engine.h"
#include "cachelib/navy/scheduler/JobScheduler.h"
#include "cachelib/navy/zone_hash/AdaptiveLog.h"
#include "cachelib/navy/zone_hash/CuckooSet.h"
#include "cachelib/navy/zone_hash/SLog.h"
#include "cachelib/navy/zone_hash/adapter/ShadowCache.h"
#include "cachelib/navy/zone_hash/storage/ZoneManager.h"

namespace facebook {
namespace cachelib {
namespace navy {

class ZoneHash final : public Engine {
 public:
  struct Config {
    JobScheduler* scheduler;

    // metadata
    uint64_t page_size_byte = 4 * 1024;

    // log related
    SLog::Config log_config;

    CuckooSet::Config set_config;

    AdaptiveLog::Config adaptive_config;

    ShadowCache::Config shadow_config;

    Device* device;
    std::string char_device_path;

    uint32_t num_insert_adaptive_threads = 32;

    uint32_t setInitialZonesNum() const {
      return set_config.initial_num_zones - set_config.num_clean_zones;
    }

    uint32_t adaptiveZonesNum() const {
      return adaptive_config.initial_num_zones;
    }

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
  JobScheduler& scheduler_;
  ZoneManager zns_mgr_;
  const uint64_t page_size_byte_;

  // current settings
  uint32_t curr_set_num_zones_;
  uint32_t curr_adaptive_num_zones_;

  ShadowCache shadow_;
  CuckooSet set_;
  SLog log_;
  AdaptiveLog adaptive_log_;

  struct ValidConfigTag {};
  ZoneHash(Config&& config, ValidConfigTag);

  void changeAdaptiveRatio(uint32_t new_adaptive_num_zones,
                           uint32_t new_set_num_zones);
  
  mutable AtomicCounter logical_written_count_{0};
  mutable AtomicCounter lookup_count_{0};
};

} // namespace navy
} // namespace cachelib
} // namespace facebook