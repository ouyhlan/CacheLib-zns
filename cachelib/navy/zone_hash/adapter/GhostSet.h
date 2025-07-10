#pragma once

#include <folly/logging/xlog.h>

#include <cstdint>
#include <unordered_map>
#include <vector>

namespace facebook {
namespace cachelib {
namespace navy {

class GhostSet {
 public:
  GhostSet(uint32_t total_zone_num)
      : total_zone_num_(total_zone_num), curr_timestamp_(0) {}

  void insertPage(std::vector<std::pair<uint64_t, uint16_t>> arr,
                  uint32_t timestamp) {
    curr_timestamp_ = std::max(curr_timestamp_, timestamp);
    for (auto& [key_hash, size] : arr) {
      entries_arr_[key_hash] = {timestamp, size};
    }
  }

  // return [distance, size]
  // distance >= zone_num => not exists
  auto find(uint64_t key_hash) -> std::tuple<uint32_t, uint16_t> {
    if (entries_arr_.count(key_hash)) {
      auto [timestamp, size] = entries_arr_[key_hash];
      uint32_t distance = curr_timestamp_ - timestamp;

      if (distance >= total_zone_num_) {
        entries_arr_.erase(key_hash);
      }
      return {distance, size};
    }

    return {total_zone_num_, 0};
  }

 private:
  const uint32_t total_zone_num_;
  uint32_t curr_timestamp_;

  // key_hash -> {timestamp, size}
  std::unordered_map<uint64_t, std::pair<uint32_t, uint16_t>> entries_arr_;
};

} // namespace navy
} // namespace cachelib
} // namespace facebook