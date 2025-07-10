#pragma once

#include <algorithm>
#include <cstdint>
#include <list>
#include <optional>
#include <unordered_map>
#include <vector>

namespace facebook {
namespace cachelib {
namespace navy {

class ShadowAdaptive {
  class __attribute__((__packed__)) Entry {
   public:
    Entry() = default;

    explicit Entry(uint16_t zone_id, uint16_t size, uint32_t timestamp)
        : last_timestamp_(timestamp), zone_id_(zone_id), size_(size) {}

    uint32_t lastTimestamp() const { return last_timestamp_; }
    uint16_t zoneId() const { return zone_id_; }
    uint16_t size() const { return size_; }

    void setZoneId(uint16_t zone_id) { zone_id_ = zone_id; }

   private:
    uint32_t last_timestamp_{0};
    uint16_t zone_id_{0};
    uint16_t size_{0};
  };

 public:
  ShadowAdaptive(uint32_t& curr_timestamp,
                 uint32_t total_zone_num,
                 uint32_t zone_num,
                 uint64_t zone_capacity)
      : curr_timestamp_(curr_timestamp),
        total_zone_num_(total_zone_num),
        ghost_zone_num_(total_zone_num_ - zone_num),
        zones_list_(total_zone_num_),
        zones_remaining_size_(total_zone_num_, zone_capacity) {}

  void promote(uint64_t key,
               uint16_t size,
               uint32_t timestamp,
               uint32_t set_zone_distance) {
    auto result = selectInsertZone(timestamp);
    if (!result) {
      return;
    }

    uint32_t insert_zone_id = *result;
    if (set_zone_distance + 1 + insert_zone_id + 1 > total_zone_num_) {
      return;
    }

    allocateEntry(insert_zone_id, key, size);
    insertToZoneList(insert_zone_id, key);
  }

  void ghostInsert(uint64_t key, uint16_t size) {
    uint16_t ghost_insert_zone_id = 0;

    allocateEntry(ghost_insert_zone_id, key, size);
    insertToZoneList(ghost_insert_zone_id, key);
  }

  bool find(uint64_t key, bool self_promote, uint16_t& size) {
    if (entry_table_.count(key) == 0) {
      return false;
    }

    auto entry = entry_table_[key];
    releaseEntry(key);
    if (entry.zoneId() < total_zone_num_) {
      size = entry.size();
      if (self_promote) {
        promote(key, entry.size(), entry.lastTimestamp(), 0);
      }
      return true;
    }

    return false;
  }

  void changeZoneNum(uint32_t curr_zone_num) {
    ghost_zone_num_ = total_zone_num_ - curr_zone_num;
    for (uint32_t i = ghost_zone_num_; i < total_zone_num_; i++) {
      clearZoneList(i);
    }
  }

 private:
  uint32_t& curr_timestamp_;
  const uint32_t total_zone_num_;
  uint32_t ghost_zone_num_;

  std::unordered_map<uint64_t, Entry> entry_table_;
  std::vector<std::list<uint64_t>> zones_list_;
  std::vector<uint64_t> zones_remaining_size_;

  std::optional<uint32_t> selectInsertZone(uint32_t timestamp) {
    for (uint32_t i = 0; i < ghost_zone_num_; i++) {
      if (zones_list_.size() == 0) {
        return i;
      }

      auto oldest_key = zones_list_[i].back();
      if (timestamp >= entry_table_[oldest_key].lastTimestamp()) {
        return i;
      }
    }

    return std::nullopt;
  }

  void makeSpace(uint16_t zone_id, uint16_t size) {
    if (zones_remaining_size_[zone_id] >= size) {
      return;
    }

    uint64_t evicted_size = 0;
    std::vector<uint64_t> evicted;

    uint64_t remaining_size = zones_remaining_size_[zone_id];
    auto zone_list = zones_list_[zone_id];
    for (auto it = zone_list.rbegin();
         it != zone_list.rend() && remaining_size < size;
         it++) {
      uint64_t curr_key = *it;
      uint64_t curr_size = entry_table_[curr_key].size();

      evicted_size += curr_size;
      remaining_size += curr_size;
      evicted.push_back(curr_key);
    }

    if (zone_id == ghost_zone_num_ - 1) {
      for (auto evict_key : evicted) {
        releaseEntry(evict_key);
      }
    } else {
      makeSpace(zone_id + 1, evicted_size);
      for (auto evicted_key : evicted) {
        moveEntry(zone_id + 1, evicted_key);
      }
    }
  }

  // have already make space for zone_id
  void insertToZoneList(uint16_t zone_id, uint64_t key) {
    auto& entry = entry_table_[key];
    if (zones_remaining_size_[zone_id] < entry.size()) {
      makeSpace(zone_id, entry.size());
    }

    auto& zone_list = zones_list_[zone_id];
    zone_list.push_front(key);

    zones_remaining_size_[zone_id] -= entry.size();
    entry.setZoneId(zone_id);
  }

  void removeFromZoneList(uint64_t key) {
    if (entry_table_.count(key) == 0) {
      return;
    }

    auto entry = entry_table_[key];
    uint16_t zone_id = entry.zoneId();
    auto& zone_list = zones_list_[zone_id];

    auto it = std::find(zone_list.begin(), zone_list.end(), key);
    if (it != zone_list.end()) {
      zones_remaining_size_[zone_id] += entry.size();
      zone_list.erase(it);
    }
  }

  void clearZoneList(uint16_t zone_id) {
    auto& zone_list = zones_list_[zone_id];

    std::vector<uint64_t> evicted;
    for (auto key : zone_list) {
      evicted.push_back(key);
    }

    for (auto evict_key : evicted) {
      releaseEntry(evict_key);
    }

    zone_list.clear();
  }

  void releaseEntry(uint64_t key) {
    if (entry_table_.count(key) == 0) {
      return;
    }

    removeFromZoneList(key);
    entry_table_.erase(key);
  }

  // have already make space for zone_id
  void moveEntry(uint16_t zone_id, uint64_t key) {
    if (entry_table_.count(key) == 0) {
      return;
    }

    removeFromZoneList(key);
    insertToZoneList(zone_id, key);
  }

  void allocateEntry(uint16_t zone_id, uint64_t key, uint16_t size) {
    releaseEntry(key);
    entry_table_[key] = Entry(zone_id, size, curr_timestamp_);
  }
};

} // namespace navy
} // namespace cachelib
} // namespace facebook