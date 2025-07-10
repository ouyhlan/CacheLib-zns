#pragma once

#include <folly/Format.h>
#include <folly/logging/xlog.h>

#include <cstdint>
#include <functional>
#include <memory>
#include <vector>

#include "cachelib/navy/scheduler/JobScheduler.h"
#include "cachelib/navy/scheduler/ThreadPoolJobScheduler.h"
#include "cachelib/navy/zone_hash/adapter/CacheStat.h"
#include "cachelib/navy/zone_hash/adapter/GhostAdaptive.h"
#include "cachelib/navy/zone_hash/adapter/GhostSet.h"

namespace facebook {
namespace cachelib {
namespace navy {

using ChangeAdaptiveRatioFn = std::function<void(uint32_t, uint32_t)>;

class Simulator {
  static constexpr double kRegulateTolerance = 10.0;
  static constexpr double kHitRateChangeTolerance = 0.05;

 public:
  struct Config {
    uint32_t total_zone_num;
    uint64_t set_zone_size;
    uint64_t adaptive_zone_size;
    uint16_t hot_data_pct;

    uint32_t miss_cost = 500;    // us
    uint32_t set_cost = 181;     // us
    uint32_t adaptive_cost = 71; // us

    uint64_t page_size_byte = 4 * 1024;
    uint32_t num_searches = 8;
    uint64_t scale_factor = 128;

    uint64_t scaleAdaptiveZoneSize() const {
      return adaptive_zone_size / scale_factor;
    }
  };

  Simulator(Config config, ChangeAdaptiveRatioFn regulate_fn)
      : scale_factor_(config.scale_factor),
        total_zone_num_(config.total_zone_num),
        adaptive_cost_(config.adaptive_cost),
        set_cost_(config.set_cost),
        miss_cost_(config.miss_cost),
        ghost_adaptive_arr_(config.num_searches - 1),
        ghost_cache_stats(config.num_searches),
        steps_(0),
        executor_(1, "adapter"),
        regulated(false),
        regulate_fn_(regulate_fn) {
    ghost_set_ = std::make_unique<GhostSet>(config.total_zone_num);
    XLOG(INFO,
         folly::sformat("{}: num_set_zone: {}", 0, config.total_zone_num));

    for (uint32_t i = 1; i <= config.num_searches - 1; i++) {
      uint32_t num_adaptive_zone =
          config.total_zone_num * i / config.num_searches;

      XLOG(INFO,
           folly::sformat("{}: num_adaptive_zone: {}", i, num_adaptive_zone));
      ghost_adaptive_arr_[i - 1] = std::make_unique<GhostAdaptive>(
          num_adaptive_zone, config.scaleAdaptiveZoneSize(),
          config.page_size_byte, config.hot_data_pct);
    }
  }

  void setInsertPage(std::vector<uint64_t> key_hash_arr,
                     std::vector<uint16_t> size_arr,
                     uint32_t timestamp) {
    std::vector<std::pair<uint64_t, uint16_t>> arr;
    for (uint32_t i = 0; i < key_hash_arr.size(); i++) {
      if (inSampleRange(key_hash_arr[i])) {
        arr.emplace_back(key_hash_arr[i], size_arr[i]);
      }
    }

    if (arr.size() > 0) {
      executor_.enqueue(
          [this, arr, timestamp] {
            ghost_set_->insertPage(arr, timestamp);
            return JobExitCode::Done;
          },
          "set insert", JobQueue::QueuePos::Back);
    }
  }

  void addRecord(uint64_t key_hash, GhostCacheType cache_type, bool hit) {
    cache_stat.record(cache_type, hit);

    if (inSampleRange(key_hash)) {
      executor_.enqueue(
          [this, key_hash] {
            record(key_hash);

            return JobExitCode::Done;
          },
          "record", JobQueue::QueuePos::Back);
    }
  }

 private:
  const uint64_t scale_factor_;
  const uint32_t total_zone_num_;
  const uint32_t adaptive_cost_;
  const uint32_t set_cost_;
  const uint32_t miss_cost_;
  std::unique_ptr<GhostSet> ghost_set_;
  std::vector<std::unique_ptr<GhostAdaptive>> ghost_adaptive_arr_;

  AtomicCacheStat cache_stat;
  std::vector<CacheStat> ghost_cache_stats;
  uint64_t steps_;

  ThreadPoolExecutor executor_;

  bool regulated;
  const ChangeAdaptiveRatioFn regulate_fn_;

  bool inSampleRange(uint64_t key_hash) const {
    return (key_hash % scale_factor_) == 0;
  }

  uint32_t calculateLatency(double adaptive_rate, double set_rate) {
    double avg_latency = adaptive_rate * adaptive_cost_ + set_rate * set_cost_ +
                         (1 - adaptive_rate - set_rate) * miss_cost_;
    return avg_latency;
  }

  void printStat() {
    XLOG(INFO, "====== Current Result ======");
    auto [curr_adaptive_rate, curr_set_rate] = cache_stat.calculate();
    double curr_avg_latency =
        calculateLatency(curr_adaptive_rate, curr_set_rate);
    
    cache_stat.recordHistory(curr_adaptive_rate + curr_set_rate);
    XLOG(INFO,
         folly::sformat(
             "Adaptive : Set = {:6.2f}% : {:6.2f}%, avg latency: {:6.2f}us, {}",
             curr_adaptive_rate * 100, curr_set_rate * 100, curr_avg_latency,
             (cache_stat.stable() ? "stable" : "unstable")));

    cache_stat.clear();

    uint32_t stable_count = 0;
    if (cache_stat.stable()) {
      double prev_hit_rate = cache_stat.fetchPrevHitRate();
      if (std::abs(prev_hit_rate - curr_adaptive_rate - curr_set_rate) >=
          kHitRateChangeTolerance) {
        cache_stat.resetHistory();
        for (auto& stat : ghost_cache_stats) {
          stat.resetHistory();
        }

        regulated = false;
      } else {
        stable_count++;
      }
    }

    uint32_t best_index = ghost_cache_stats.size();
    double lowest_latency = miss_cost_;
    XLOG(INFO, "====== Current simulate Result ======");
    for (uint32_t i = 0; i < ghost_cache_stats.size(); i++) {
      auto& stat = ghost_cache_stats[i];
      auto [adaptive_rate, set_rate] = stat.calculate();
      double avg_latency = calculateLatency(adaptive_rate, set_rate);
      
      stat.recordHistory(adaptive_rate + set_rate);
      XLOG(INFO,
           folly::sformat("Adaptive : Set = {:6.2f}% : {:6.2f}%, avg latency: "
                          "{:6.2f}us, {}",
                          adaptive_rate * 100, set_rate * 100, avg_latency,
                          (stat.stable() ? "stable" : "unstable")));
      stat.clear();

      if (stat.stable()) {
        stable_count++;
      }

      if (avg_latency < lowest_latency) {
        lowest_latency = avg_latency;
        best_index = i;
      }
    }

    // check if simulate cache stable
    
    // if (stable_count == ghost_cache_stats.size() + 1) {
    //   // check if improvement
    //   if (!regulated && curr_avg_latency > lowest_latency + kRegulateTolerance) {
    //     uint32_t new_adaptive_num_zones = 0;
    //     if (best_index > 0) {
    //       new_adaptive_num_zones =
    //           ghost_adaptive_arr_[best_index - 1]->zoneNum();
    //     }

    //     uint32_t new_set_num_zones = total_zone_num_ - new_adaptive_num_zones;
    //     XLOG(INFO,
    //          folly::sformat("Regulate into adaptive zones: {}, set zones: {}.",
    //                         new_adaptive_num_zones, new_set_num_zones));
    //     regulate_fn_(new_adaptive_num_zones, new_set_num_zones);
    //     regulated = true;
    //   }
    // }
  }

  void record(uint64_t key_hash) {
    uint32_t index = 0;

    // lookup at ghost_set
    auto [distance, object_size] = ghost_set_->find(key_hash);

    // for pure set setting
    if (distance < total_zone_num_) {
      ghost_cache_stats[index].record(GhostCacheType::Set, true);
    } else {
      ghost_cache_stats[index].record(GhostCacheType::Set, false);
    }

    for (auto& curr_adaptive : ghost_adaptive_arr_) {
      index++;

      uint64_t set_accept_distance =
          total_zone_num_ - curr_adaptive->zoneNum() - 1;

      if (curr_adaptive->find(key_hash)) {
        ghost_cache_stats[index].record(GhostCacheType::Adaptive, true);
      } else if (distance <= set_accept_distance) {
        ghost_cache_stats[index].record(GhostCacheType::Set, true);

        // hit on set, check if promote
        if (curr_adaptive->accept(key_hash)) {
          curr_adaptive->insert(key_hash, object_size);
        }
      } else {
        ghost_cache_stats[index].record(GhostCacheType::Set, false);
      }

      curr_adaptive->track(key_hash);
    }

    if ((++steps_) % 20000 == 0) {
      printStat();
    }
  }
};

} // namespace navy
} // namespace cachelib
} // namespace facebook