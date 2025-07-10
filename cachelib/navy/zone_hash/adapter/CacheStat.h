#pragma once

#include <folly/Format.h>
#include <folly/logging/xlog.h>

#include <cstdint>

#include "cachelib/common/AtomicCounter.h"

namespace facebook {
namespace cachelib {
namespace navy {

namespace {

double pct_fn(uint64_t numer, uint64_t denom) {
  return denom == 0 ? 0.0 : (double)numer / denom;
}

} // namespace

//[0%, 100%] -> [0, 10000]
class CacheHistory {
  static constexpr uint16_t kRecordEntries = 3;
  static constexpr uint16_t kConvergeTolerance = 200;

 public:
  CacheHistory(uint16_t initial = 0)
      : stable_(false), initial_(initial), idx(0) {
    for (auto& stat : history_stats_) {
      stat = initial;
    }
  }

  uint16_t fetchPrevHitRate() {
    if (idx > 0) {
      return history_stats_[idx - 1];
    } else {
      return history_stats_[kRecordEntries - 1];
    }
  }

  void record(uint16_t curr_stat) {
    history_stats_[idx] = curr_stat;
    idx = (idx + 1) % kRecordEntries;

    if (!stable_) {
      checkStable();
    }
  }

  void checkStable() {
    uint16_t max = 0;
    uint16_t min = 10000;

    for (auto stat : history_stats_) {
      if (stat == initial_) {
        return;
      }
      max = std::max(max, stat);
      min = std::min(min, stat);
    }

    if ((max - min) < kConvergeTolerance) {
      stable_ = true;
    }
  }

  void reset(uint16_t num = 0) {
    stable_ = false;
    initial_ = num;

    for (auto& stat : history_stats_) {
      stat = num;
    }
  }

  bool stable() const { return stable_; }

 private:
  bool stable_;
  uint16_t initial_;
  std::array<uint16_t, kRecordEntries> history_stats_;
  uint16_t idx;
};

enum class GhostCacheType { Adaptive, Set };
class CacheStat {
 public:
  CacheStat() : adaptive_hit_num_(0), set_hit_num_(0), miss_num_(0) {}

  void record(GhostCacheType hit_type, bool hit) {
    if (hit) {
      if (hit_type == GhostCacheType::Adaptive) {
        adaptive_hit_num_++;
      } else {
        set_hit_num_++;
      }
    } else {
      miss_num_++;
    }
  }

  void resetHistory() {
    hit_history_.reset();
  }

  void clear() {
    adaptive_hit_num_ = 0;
    set_hit_num_ = 0;
    miss_num_ = 0;
  }

  bool stable() const { return hit_history_.stable(); }

  // adaptive : set : miss
  auto calculate() -> std::tuple<double, double> {
    uint32_t total_num = adaptive_hit_num_ + set_hit_num_ + miss_num_;
    double adaptive_rate = pct_fn(adaptive_hit_num_, total_num);
    double set_rate = pct_fn(set_hit_num_, total_num);

    return {adaptive_rate, set_rate};
  }

  void recordHistory(double hit_rate) {
    uint16_t hit_record = hit_rate * 10000;
    hit_history_.record(hit_record);
  }

  double fetchPrevHitRate() {
    uint16_t prev = hit_history_.fetchPrevHitRate();
    return (double)prev / 10000;
  }

 private:
  CacheHistory hit_history_;
  uint64_t adaptive_hit_num_{0};
  uint64_t set_hit_num_{0};
  uint64_t miss_num_{0};
};

class AtomicCacheStat {
 public:
  AtomicCacheStat() : adaptive_hit_num_(0), set_hit_num_(0), miss_num_(0) {}

  void record(GhostCacheType hit_type, bool hit) {
    if (hit) {
      if (hit_type == GhostCacheType::Adaptive) {
        adaptive_hit_num_.inc();
      } else {
        set_hit_num_.inc();
      }
    } else {
      miss_num_.inc();
    }
  }

  void resetHistory() {
    hit_history_.reset();
  }

  void clear() {
    adaptive_hit_num_.set(0);
    set_hit_num_.set(0);
    miss_num_.set(0);
  }

  bool stable() const { return hit_history_.stable(); }

  // adaptive : set : miss
  auto calculate() -> std::tuple<double, double> {
    uint64_t adaptive_hit_num = adaptive_hit_num_.get();
    uint64_t set_hit_num = set_hit_num_.get();
    uint64_t miss_num = miss_num_.get();

    uint32_t total_num = adaptive_hit_num + set_hit_num + miss_num;
    double adaptive_rate = pct_fn(adaptive_hit_num, total_num);
    double set_rate = pct_fn(set_hit_num, total_num);

    return {adaptive_rate, set_rate};
  }

  void recordHistory(double hit_rate) {
    uint16_t hit_record = hit_rate * 10000;
    hit_history_.record(hit_record);
  }

  double fetchPrevHitRate() {
    uint16_t prev = hit_history_.fetchPrevHitRate();
    return (double)prev / 10000;
  }

 private:
  CacheHistory hit_history_;
  AtomicCounter adaptive_hit_num_{0};
  AtomicCounter set_hit_num_{0};
  AtomicCounter miss_num_{0};
};

} // namespace navy
} // namespace cachelib
} // namespace facebook