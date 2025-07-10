#pragma once

#include <folly/logging/xlog.h>

#include <cstdint>
#include <unordered_map>
#include <vector>

namespace facebook {
namespace cachelib {
namespace navy {

static constexpr uint32_t kNullShadowTimestamp = UINT32_MAX;
class ShadowSet {
  class __attribute__((__packed__)) Entry {
   public:
    Entry() = default;

    explicit Entry(uint32_t zone_timestamp, uint16_t size)
        : zone_timestamp_(zone_timestamp), size_(size) {}

    uint32_t zoneTimestamp() const { return zone_timestamp_; }
    uint32_t lastTimestamp() const { return last_timestamp_; }
    uint16_t size() const { return size_; }

    void setTimestamp(uint32_t curr_timestamp) {
      last_timestamp_ = curr_timestamp;
    }

   private:
    uint32_t last_timestamp_{kNullShadowTimestamp};
    uint32_t zone_timestamp_{0};
    uint16_t size_{0};
  };

 public:
  ShadowSet(uint32_t& curr_timestamp, uint32_t total_zone_num)
      : curr_timestamp_(curr_timestamp),
        total_zone_num_(total_zone_num),
        curr_zone_timestamp_(0) {}

  // [key: u64, size: u16]
  void insertPage(std::vector<std::pair<uint64_t, uint16_t>> arr,
                  uint32_t zone_timestamp) {
    curr_zone_timestamp_ = std::max(curr_zone_timestamp_, zone_timestamp);
    for (auto& [key_hash, size] : arr) {
      entry_table_[key_hash] = Entry(zone_timestamp, size);
    }
  }

  // return [zone_distance, last_timestamp, size]
  auto find(uint64_t key_hash) -> std::tuple<uint32_t, uint32_t, uint16_t> {
    if (entry_table_.count(key_hash) == 0) {
      return {total_zone_num_, kNullShadowTimestamp, 0};
    }

    auto& entry = entry_table_[key_hash];
    uint32_t zone_distance = curr_zone_timestamp_ - entry.zoneTimestamp();
    if (zone_distance >= total_zone_num_) {
      entry_table_.erase(key_hash);
      return {total_zone_num_, kNullShadowTimestamp, 0};
    }
    
    return {zone_distance, entry.lastTimestamp(), entry.size()};
  }

  void updateTimestamp(uint64_t key_hash) {
    if (entry_table_.count(key_hash) > 0) {
       entry_table_[key_hash].setTimestamp(curr_timestamp_);
    }
  }

 private:
  uint32_t& curr_timestamp_;
  const uint32_t total_zone_num_;
  uint32_t curr_zone_timestamp_;

  std::unordered_map<uint64_t, Entry> entry_table_;
};

} // namespace navy
} // namespace cachelib
} // namespace facebook