#include "cachelib/navy/zone_hash/ZoneHash.h"

#include <folly/logging/xlog.h>

#include <cstdint>
#include <stdexcept>

#include "cachelib/common/Hash.h"
#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/scheduler/JobScheduler.h"
#include "cachelib/navy/zone_hash/AdaptiveLog.h"
#include "cachelib/navy/zone_hash/CuckooSet.h"
#include "cachelib/navy/zone_hash/adapter/ShadowCache.h"
#include "cachelib/navy/zone_hash/storage/ZNSDevice.h"
#include "cachelib/navy/zone_hash/utils/ZoneBucket.h"

namespace facebook {
namespace cachelib {
namespace navy {

ZoneHash::ZoneHash(Config&& config)
    : ZoneHash(std::move(config.validate()), ValidConfigTag{}) {}

ZoneHash::ZoneHash(Config&& config, ValidConfigTag)
    : scheduler_(*config.scheduler),
      zns_mgr_(*dynamic_cast<ZNSDevice*>(config.device),
               config.page_size_byte,
               config.log_config.segment_size_byte),
      page_size_byte_(config.page_size_byte),
      curr_set_num_zones_(config.setInitialZonesNum()),
      curr_adaptive_num_zones_(config.adaptiveZonesNum()),
      shadow_(
          config.shadow_config,
          [this](uint32_t new_adaptive_num_zones, uint32_t new_set_num_zones) {
            changeAdaptiveRatio(new_adaptive_num_zones, new_set_num_zones);
          }),
      set_(std::move(config.set_config),
           zns_mgr_,
           scheduler_,
           [this](std::vector<uint64_t> key_hash_arr,
                  std::vector<uint16_t> size_arr,
                  uint32_t timestamp) {
             shadow_.setInsertPage(key_hash_arr, size_arr, timestamp);
           }),
      log_(
          std::move(config.log_config),
          zns_mgr_,
          scheduler_,
          [&](uint64_t key_hash) { return set_.getSetId(key_hash); },
          [&](std::vector<ObjectInfo>&& object_vec,
              LogReadmitCallback&& readmit) {
            set_.insert(std::move(object_vec), std::move(readmit));
          }),
      adaptive_log_(std::move(config.adaptive_config),
                    zns_mgr_,
                    scheduler_,
                    [this](uint64_t key_hash, uint16_t size) {
                      shadow_.adaptiveGhostInsert(key_hash, size);
                    }) {
  XLOG(INFO, "ZoneHash initialized finished!");
}

bool ZoneHash::couldExist(HashedKey hk) {
  bool res = log_.couldExist(hk) ||
             (curr_adaptive_num_zones_ > 0 && adaptive_log_.couldExist(hk)) ||
             set_.couldExist(hk);
  if (!res) {
    shadow_.addRecord(hk.keyHash(), CacheHitType::Miss);
    lookup_count_.inc();
  }

  if (lookup_count_.get() % 5000000 == 0) {
    XLOGF(INFO,
          "Lookup count {}, set hits {} hot set "
          "hits {} log hits {}",
          lookup_count_.get(), set_.getHitCount(), adaptive_log_.getHitCount(),
          log_.getHitCount());
  }
  return res;
}

Status ZoneHash::lookup(HashedKey hk, Buffer& value) {
  lookup_count_.inc();

  Status log_status = log_.lookup(hk, value);
  if (log_status == Status::Ok) {
    return log_status;
  }

  if (curr_adaptive_num_zones_ > 0) {
    Status adaptive_status = adaptive_log_.lookup(hk, value);
    if (adaptive_status == Status::Ok) {
      shadow_.addRecord(hk.keyHash(), CacheHitType::AdaptiveHit);
      return adaptive_status;
    }
  }

  Status set_status = set_.lookup(hk, value);
  if (set_status == Status::Ok) {
    // check promotion
    if (curr_adaptive_num_zones_ > 0) {
      scheduler_.enqueue(
          [this, key_buffer = Buffer(makeView(hk.key())),
           key_hash = hk.keyHash(), value_buffer = Buffer(value.view())]() {
            HashedKey hk(HashedKey::precomputed(
                toStringPiece(key_buffer.view()), key_hash));

            if (adaptive_log_.admissionTest(hk)) {
              shadow_.addRecord(key_hash, CacheHitType::SetHitPromote);
              adaptive_log_
                  .insert(Buffer(key_buffer.view()),
                          hk.keyHash(),
                          Buffer(value_buffer.view()))
                  .detach();
            } else {
              shadow_.addRecord(key_hash, CacheHitType::SetHitNotPromote);
            }
            return JobExitCode::Done;
          },
          "adaptive related",
          JobType::Write);
    } else {
      shadow_.addRecord(hk.keyHash(), CacheHitType::SetHitNotPromote);
    }
  } else {
    shadow_.addRecord(hk.keyHash(), CacheHitType::Miss);
  }

  adaptive_log_.track(hk);
  return set_status;
}

Status ZoneHash::insert(HashedKey hk, BufferView value) {
  // directly insert into log
  log_.insert(hk, value).detach();
  logical_written_count_.add(hk.key().size() + value.size());
  return Status::Ok;
}

Status ZoneHash::remove([[maybe_unused]] HashedKey hk) {
  // TODO: implement later
  return Status::NotFound;
}

void ZoneHash::flush() { zns_mgr_.flush(); }

void ZoneHash::reset() { XLOG(INFO, "ZoneHash called reset()"); }

void ZoneHash::persist([[maybe_unused]] RecordWriter& rw) {
  // TODO: implement later
}

bool ZoneHash::recover([[maybe_unused]] RecordReader& rr) {
  // TODO: implement later
  return true;
}

void ZoneHash::getCounters(const CounterVisitor& visitor) const {
  // hit count related
  visitor("navy_zh_log_hit_counts", log_.getHitCount());
  visitor("navy_zh_set_hit_counts", set_.getHitCount());
  visitor("navy_zh_adaptive_hit_counts", adaptive_log_.getHitCount());

  visitor("navy_bh_items", adaptive_log_.getItemCount());
  visitor("navy_bh_logical_written", logical_written_count_.get());
}

uint64_t ZoneHash::getMaxItemSize() const {
  // does not include per item overhead
  return page_size_byte_ - sizeof(ZoneBucket);
}

void ZoneHash::changeAdaptiveRatio(uint32_t new_adaptive_num_zones,
                                   uint32_t new_set_num_zones) {
  XDCHECK_EQ(new_adaptive_num_zones + new_set_num_zones,
             curr_set_num_zones_ + curr_adaptive_num_zones_);
  adaptive_log_.changeZoneNum(new_adaptive_num_zones);
  set_.changeZoneNum(new_set_num_zones);

  curr_set_num_zones_ = new_set_num_zones;
  curr_adaptive_num_zones_ = new_adaptive_num_zones;
}

ZoneHash::Config& ZoneHash::Config::validate() {
  if (const ZNSDevice* d = dynamic_cast<ZNSDevice*>(device); d == nullptr) {
    throw std::invalid_argument("Device must be a ZNS Device");
  }

  return *this;
}

} // namespace navy
} // namespace cachelib
} // namespace facebook