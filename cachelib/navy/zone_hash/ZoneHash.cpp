#include "cachelib/navy/zone_hash/ZoneHash.h"

#include <folly/logging/xlog.h>

#include <cstdint>
#include <memory>
#include <stdexcept>

#include "cachelib/navy/common/Buffer.h"
#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/scheduler/JobScheduler.h"
#include "cachelib/navy/scheduler/ThreadPoolJobQueue.h"
#include "cachelib/navy/zone_hash/AdaptiveLog.h"
#include "cachelib/navy/zone_hash/storage/ZNSDevice.h"
#include "cachelib/navy/zone_hash/utils/CuckooHashConfig.h"
#include "cachelib/navy/zone_hash/utils/ZoneBucket.h"

namespace facebook {
namespace cachelib {
namespace navy {

ZoneHash::ZoneHash(Config&& config)
    : ZoneHash(std::move(config.validate()), ValidConfigTag{}) {}

ZoneHash::ZoneHash(Config&& config, ValidConfigTag)
    : zns_mgr_(*dynamic_cast<ZNSDevice*>(config.device),
               config.page_size_byte,
               config.log_config.segment_size_byte),
      page_size_byte_(config.page_size_byte),
      curr_num_adaptive_zones_(config.adaptive_config.initial_num_zones),
      set_(std::move(config.set_config), zns_mgr_),
      log_(
          std::move(config.log_config),
          zns_mgr_,
          [&](uint64_t key_hash) { return set_.getSetId(key_hash); },
          [&](std::vector<ObjectInfo>&& object_vec,
              LogReadmitCallback readmit) {
            set_.insertPage(std::move(object_vec), readmit);
          }),
      adaptive_log_(std::move(config.adaptive_config),
                    zns_mgr_,
                    [&](uint64_t key_hash) { return set_.getSetId(key_hash); }),
      adaptive_threads_(config.num_insert_adaptive_threads, "insert_pool") {
  XLOG(INFO, "ZoneHash initialized finished!");
}

bool ZoneHash::couldExist(HashedKey hk) {
  return log_.couldExist(hk) ||
         (curr_num_adaptive_zones_ > 0 && adaptive_log_.couldExist(hk)) ||
         set_.couldExist(hk);
}

Status ZoneHash::lookup(HashedKey hk, Buffer& value) {
  Status log_status = log_.lookup(hk, value);
  if (log_status == Status::Ok) {
    return log_status;
  }

  if (curr_num_adaptive_zones_ > 0) {
    Status adaptive_status = adaptive_log_.lookup(hk, value);
    if (adaptive_status == Status::Ok) {
      return adaptive_status;
    }
  }

  Status set_status = set_.lookup(hk, value);
  if (set_status == Status::Ok) {
    // check promotion
    if (curr_num_adaptive_zones_ > 0) {
      adaptive_log_.track(hk);

      if (adaptive_log_.admissionTest(hk)) {
        // promote
        adaptive_threads_.enqueue(
            [this,
             key_buffer = Buffer(makeView(hk.key())),
             key_hash = hk.keyHash(),
             value_buffer = Buffer(value.view())]() {
              HashedKey hk(HashedKey::precomputed(
                  toStringPiece(key_buffer.view()), key_hash));

              Status status = adaptive_log_.insert(hk, value_buffer.view());
              if (status == Status::Retry) {
                return JobExitCode::Reschedule;
              }

              return JobExitCode::Done;
            },
            "adaptive_thread",
            JobQueue::QueuePos::Back);
      }
    }
  }
  return set_status;
}

Status ZoneHash::insert(HashedKey hk, BufferView value) {
  // directly insert into log
  return log_.insert(hk, value);
}

Status ZoneHash::remove(HashedKey hk) {
  // TODO: implement later
  return Status::NotFound;
}

void ZoneHash::flush() { zns_mgr_.flush(); }

void ZoneHash::reset() { XLOG(INFO, "ZoneHash called reset()"); }

void ZoneHash::persist(RecordWriter& rw) {
  // TODO: implement later
}

bool ZoneHash::recover(RecordReader& rr) {
  // TODO: implement later
  return true;
}

void ZoneHash::getCounters(const CounterVisitor& visitor) const {
  // hit count related
  visitor("navy_zh_log_hit_counts", log_.getHitCount());
  visitor("navy_zh_set_hit_counts", set_.getHitCount());
  visitor("navy_zh_adaptive_hit_counts", adaptive_log_.getHitCount());

  visitor("navy_bh_items", adaptive_log_.getItemCount());
}

uint64_t ZoneHash::getMaxItemSize() const {
  // does not include per item overhead
  return page_size_byte_ - sizeof(ZoneBucket);
}

ZoneHash::Config& ZoneHash::Config::validate() {
  if (ZNSDevice* d = dynamic_cast<ZNSDevice*>(device); d == nullptr) {
    throw std::invalid_argument("Device must be a ZNS Device");
  }

  return *this;
}

} // namespace navy
} // namespace cachelib
} // namespace facebook