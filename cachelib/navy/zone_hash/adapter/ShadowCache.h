#pragma once

#include <cstdint>

#include "cachelib/navy/scheduler/ThreadPoolJobScheduler.h"
#include "cachelib/navy/zone_hash/adapter/ShadowAdaptive.h"
#include "cachelib/navy/zone_hash/adapter/ShadowSet.h"

namespace facebook {
namespace cachelib {
namespace navy {

using ChangeAdaptiveRatioFn = std::function<void(uint32_t, uint32_t)>;
enum class CacheHitType { AdaptiveHit, SetHitNotPromote, SetHitPromote, Miss };

class ShadowCache {
 public:
  struct Config {
    uint32_t total_zone_num;
    uint32_t initial_set_zone_num;
    uint32_t min_set_pct = 10;
    uint64_t set_zone_capacity;
    uint64_t adaptive_zone_capacity;

    uint32_t miss_cost = 500;    // us
    uint32_t set_cost = 181;     // us
    uint32_t adaptive_cost = 71; // us

    uint64_t scale_factor = 128;

    uint64_t scaledSetZoneCapacity() const {
      return set_zone_capacity / scale_factor;
    }

    uint64_t scaledAdaptiveZoneCapacity() const {
      return adaptive_zone_capacity / scale_factor;
    }

    uint32_t minSetZoneNum() const {
      return total_zone_num * min_set_pct / 100;
    }
  };

  ShadowCache(Config config, ChangeAdaptiveRatioFn regulate_fn)
      : scale_factor_(config.scale_factor),
        scaled_set_zone_capacity_(config.scaledSetZoneCapacity()),
        total_zone_num_(config.total_zone_num),
        adaptive_cost_(config.adaptive_cost),
        set_cost_(config.set_cost),
        miss_cost_(config.miss_cost),
        min_set_size_(config.minSetZoneNum() * config.scaledSetZoneCapacity()),
        max_set_size_(total_zone_num_ * config.scaledSetZoneCapacity()),
        curr_set_size_((config.initial_set_zone_num + 0.5) *
                       config.scaledSetZoneCapacity()),
        curr_timestamp_(0),
        shadow_set_(curr_timestamp_, config.total_zone_num),
        shadow_adaptive_(curr_timestamp_,
                         config.total_zone_num,
                         config.total_zone_num - config.initial_set_zone_num,
                         config.scaledAdaptiveZoneCapacity()),
        executor_(1, "shadow"),
        regulate_fn_(regulate_fn) {}

  void setInsertPage(std::vector<uint64_t> key_hash_arr,
                     std::vector<uint16_t> size_arr,
                     uint32_t zone_timestamp) {
    std::vector<std::pair<uint64_t, uint16_t>> arr;
    for (uint32_t i = 0; i < key_hash_arr.size(); i++) {
      if (inSampleRange(key_hash_arr[i])) {
        arr.emplace_back(key_hash_arr[i], size_arr[i]);
      }
    }

    if (arr.size() > 0) {
      executor_.enqueue(
          [this, arr, zone_timestamp] {
            shadow_set_.insertPage(arr, zone_timestamp);
            return JobExitCode::Done;
          },
          "set insert", JobQueue::QueuePos::Back);
    }
  }

  void adaptiveGhostInsert(uint64_t key_hash, uint16_t size) {
    if (inSampleRange(key_hash)) {
      executor_.enqueue(
          [this, key_hash, size] {
            shadow_adaptive_.ghostInsert(key_hash, size);
            return JobExitCode::Done;
          },
          "adaptive ghost insert", JobQueue::QueuePos::Back);
    }
  }

  void addRecord(uint64_t key_hash, CacheHitType hit_type) {
    if (inSampleRange(key_hash)) {
      executor_.enqueue(
          [this, key_hash, hit_type] {
            record(key_hash, hit_type);

            return JobExitCode::Done;
          },
          "record", JobQueue::QueuePos::Back);
    }
  }

 private:
  const bool enable_regulate_ = false;
  const uint64_t scale_factor_;
  const uint64_t scaled_set_zone_capacity_;
  const uint32_t total_zone_num_;
  const uint32_t adaptive_cost_;
  const uint32_t set_cost_;
  const uint32_t miss_cost_;
  const uint64_t min_set_size_;
  const uint64_t max_set_size_;

  uint64_t curr_set_size_;

  uint32_t curr_timestamp_;
  ShadowSet shadow_set_; // keep all the set entry
  ShadowAdaptive shadow_adaptive_;

  ThreadPoolExecutor executor_;
  const ChangeAdaptiveRatioFn regulate_fn_;

  bool inSampleRange(uint64_t key_hash) const {
    return (key_hash % scale_factor_) == 0;
  }

  void record(uint64_t key_hash, CacheHitType hit_type) {
    if (hit_type == CacheHitType::AdaptiveHit) {
      // no regret, just return
      return;
    }

    auto [zone_distance, last_timestamp, obj_size] = shadow_set_.find(key_hash);
    if (hit_type == CacheHitType::SetHitPromote) {
      if (shadow_adaptive_.find(key_hash, false, obj_size)) {
        adjustWeight(set_cost_ - adaptive_cost_, obj_size);
      }
    } else if (hit_type == CacheHitType::SetHitNotPromote) {
      if (shadow_adaptive_.find(key_hash, true, obj_size)) {
        adjustWeight(set_cost_ - adaptive_cost_, obj_size);
      } else if (last_timestamp < curr_timestamp_ &&
                 zone_distance < total_zone_num_) {
        shadow_adaptive_.promote(key_hash, obj_size, last_timestamp,
                                 zone_distance);
      }
    } else { // Miss
      if (shadow_adaptive_.find(key_hash, true, obj_size)) {
        adjustWeight(miss_cost_ - adaptive_cost_, obj_size);
      } else if (zone_distance < total_zone_num_) {
        adjustWeight(-(miss_cost_ - set_cost_), obj_size);
        shadow_adaptive_.promote(key_hash, obj_size, last_timestamp,
                                 zone_distance);
      }
    }

    shadow_set_.updateTimestamp(key_hash);
    curr_timestamp_++;
  }

  void adjustWeight(int32_t reward, uint16_t obj_size) {
    if (enable_regulate_) {
      uint64_t prev_set_num = getZoneNum(curr_set_size_);
      uint64_t prev_adaptive_num = total_zone_num_ - prev_set_num;
      if (reward < 0) {
        double steps =
            (double)(-reward) / (set_cost_ - adaptive_cost_) * obj_size;
        if (prev_set_num < prev_adaptive_num) {
          steps *= (double)prev_adaptive_num / prev_set_num;
        }

        curr_set_size_ =
            std::min((uint64_t)(curr_set_size_ + steps), max_set_size_);
      } else {
        uint64_t steps =
            (double)reward / (set_cost_ - adaptive_cost_) * obj_size;
        if (prev_adaptive_num < prev_set_num) {
          steps *= (double)prev_set_num / std::max(1ul, prev_adaptive_num);
        }

        curr_set_size_ =
            std::max((uint64_t)(curr_set_size_ - steps), min_set_size_);
      }

      if (getZoneNum(curr_set_size_) != prev_set_num) {
        uint32_t new_set_num_zones = getZoneNum(curr_set_size_);
        uint32_t new_adaptive_num_zones = total_zone_num_ - new_set_num_zones;
        XLOG(INFO,
             folly::sformat("Regulate into adaptive zones: {}, set zones: {}.",
                            new_adaptive_num_zones, new_set_num_zones));
        regulate_fn_(new_adaptive_num_zones, new_set_num_zones);
        shadow_adaptive_.changeZoneNum(new_adaptive_num_zones);

        curr_set_size_ =
            (getZoneNum(curr_set_size_) + 0.5) * scaled_set_zone_capacity_;
      }
    }
  }

  uint32_t getZoneNum(uint64_t size_byte) {
    return size_byte / scaled_set_zone_capacity_;
  }
};

} // namespace navy
} // namespace cachelib
} // namespace facebook