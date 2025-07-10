#pragma once

#include <folly/logging/xlog.h>

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "cachelib/common/BloomFilter.h"

namespace facebook {
namespace cachelib {
namespace navy {

struct SimulateZone {
  std::unordered_set<uint64_t> items;
  const uint64_t capacity;
  uint64_t curr_size;

  // the following variable controlled by related function
  uint64_t valid_size;

  SimulateZone(uint64_t capacity)
      : capacity(capacity), curr_size(0), valid_size(0) {}

  void erase(uint64_t item, uint64_t size) {
    XDCHECK_LE(size, curr_size);
    curr_size -= size;
    items.erase(item);
  }

  void insert(uint64_t item, uint64_t size) {
    XDCHECK_LE(curr_size + size, capacity);
    curr_size += size;
    items.insert(item);
  }

  void addValidData(uint64_t size) {
    XDCHECK_LE(valid_size + size, capacity);
    valid_size += size;
  }

  void decrValidData(uint64_t size) {
    XDCHECK_GE(valid_size, size);
    valid_size -= size;
  }
};

struct __attribute__((__packed__)) ClockTableEntry {
  uint32_t zone_id;
  uint16_t size : 12;
  uint16_t clock_bit : 2;
  uint16_t ghost : 1;
  uint16_t valid : 1;

  ClockTableEntry() : valid(0) {}

  void setEntry(uint32_t zone_id, uint16_t size) {
    this->zone_id = zone_id;
    this->size = size;
    this->clock_bit = 1;
    this->ghost = 0;
    this->valid = 1;
  }

  void promote() {
    if (clock_bit < 3) {
      clock_bit++;
    }
  }
};

class GhostAdaptive {
 public:
  GhostAdaptive(uint32_t zone_num,
                uint64_t zone_capacity,
                uint64_t bucket_size,
                uint16_t hot_data_pct)
      : num_buckets_(zone_num * zone_capacity / bucket_size),
        hot_data_threshold_(zone_num * zone_capacity * hot_data_pct / 100),
        hot_data_size_(0),
        clock_pointer_(0),
        active_zone_id_(0),
        zone_table(zone_num, SimulateZone(zone_capacity)) {
    XDCHECK(zone_num > 0);

    prev_bf_ = std::make_unique<BloomFilter>(num_buckets_, 4, 8 * 8 / 4);
    curr_bf_ = std::make_unique<BloomFilter>(num_buckets_, 4, 8 * 8 / 4);
  }

  void insert(uint64_t key_hash, uint16_t size) {
    if (items_map_.count(key_hash)) {
      // key collision
      releaseEntry(key_hash);
    }

    if (zone_table[active_zone_id_].curr_size + size >
        zone_table[active_zone_id_].capacity) {
      selectFreeZone();
    }

    allocateEntry(key_hash, size);
  }

  bool find(uint64_t key_hash) {
    if (items_map_.count(key_hash)) {
      ClockTableEntry& entry = clock_table_[items_map_[key_hash]];
      XDCHECK(entry.valid);

      if (entry.ghost) {
        entry.ghost = 0;
        recordNewHotData(key_hash);
      } else {
        entry.promote();
      }
      return true;
    }
    return false;
  }

  void track(uint64_t key_hash) { admissionPolicyTrack(key_hash); }

  bool accept(uint64_t key_hash) { return admissionPolicyTest(key_hash); }

  uint32_t zoneNum() const { return zone_table.size(); }

 private:
  const uint64_t num_buckets_;
  const uint64_t hot_data_threshold_;
  uint64_t hot_data_size_;

  std::unordered_map<uint64_t, uint32_t> items_map_; // [key_hash,
                                                     // clock_table_index]
  uint32_t clock_pointer_;
  std::deque<uint32_t> empty_clock_id_;
  std::vector<ClockTableEntry> clock_table_;

  uint32_t active_zone_id_;
  std::vector<SimulateZone> zone_table;

  std::unique_ptr<BloomFilter> prev_bf_;
  std::unique_ptr<BloomFilter> curr_bf_;

  // clock related function
  void recordNewHotData(uint64_t key_hash) {
    ClockTableEntry& entry = clock_table_[items_map_[key_hash]];
    hot_data_size_ += entry.size;
    zone_table[entry.zone_id].addValidData(entry.size);

    if (hot_data_size_ > hot_data_threshold_) {
      clockEvictItem();
    }
  }

  void clockEvictItem() {
    while (hot_data_size_ > hot_data_threshold_) {
      ClockTableEntry& entry = clock_table_[clock_pointer_];
      if (entry.valid && !entry.ghost) {
        if (entry.clock_bit == 0) {
          zone_table[entry.zone_id].decrValidData(entry.size);
          hot_data_size_ -= entry.size;
          entry.ghost = true;
        } else {
          entry.clock_bit--;
        }
      }

      clock_pointer_ = (clock_pointer_ + 1) % clock_table_.size();
      if (clock_pointer_ == 0) {
        admissionPolicyUpdate();
      }
    }
  }

  void allocateEntry(uint64_t key_hash, uint16_t size) {
    uint32_t clock_offset = clock_table_.size();
    if (empty_clock_id_.size() > 0) {
      clock_offset = empty_clock_id_.front();
      empty_clock_id_.pop_front();
    } else {
      clock_table_.resize(clock_offset + 1);
    }

    zone_table[active_zone_id_].insert(key_hash, size);
    items_map_[key_hash] = clock_offset;

    ClockTableEntry& entry = clock_table_[clock_offset];
    entry.setEntry(active_zone_id_, size);
    recordNewHotData(key_hash);
  }

  void releaseEntry(uint64_t key_hash) {
    uint32_t offset = items_map_[key_hash];
    ClockTableEntry& entry = clock_table_[offset];
    SimulateZone& zone = zone_table[entry.zone_id];

    entry.valid = 0;
    if (!entry.ghost) {
      hot_data_size_ -= entry.size;
      zone.decrValidData(entry.size);
    }

    zone.erase(key_hash, entry.size);
    empty_clock_id_.push_back(offset);
    items_map_.erase(key_hash);
  }

  // zone-related function
  void selectFreeZone() {
    bool hasEmptyZone = false;
    for (size_t i = 0; i < zone_table.size(); i++) {
      if (zone_table[i].curr_size == 0) {
        active_zone_id_ = i;
        hasEmptyZone = true;
        break;
      }
    }

    if (!hasEmptyZone) {
      // garbage collection
      garbageCollection();
    }
  }

  void garbageCollection() {
    double min_valid_rate = 1.0;
    size_t selected_zone_id = 0;

    for (size_t i = 0; i < zone_table.size(); i++) {
      double current_valid_rate =
          (double)zone_table[i].valid_size / (double)zone_table[i].capacity;

      if (current_valid_rate < min_valid_rate) {
        selected_zone_id = i;
        min_valid_rate = current_valid_rate;
      }
    }

    std::unordered_set<uint64_t> valid_items;
    SimulateZone& selected_zone = zone_table[selected_zone_id];
    auto curr_items = selected_zone.items;
    for (uint64_t item : curr_items) {
      if (items_map_.count(item)) {
        ClockTableEntry& curr_entry = clock_table_[items_map_[item]];
        if (curr_entry.ghost == 1) {
          releaseEntry(item);
        }
      }
    }

    active_zone_id_ = selected_zone_id;
  }

  // admission policy related function
  void admissionPolicyUpdate() {
    std::swap(prev_bf_, curr_bf_);
    curr_bf_->reset();
  }

  void admissionPolicyTrack(uint64_t key_hash) {
    uint64_t index = key_hash % num_buckets_;
    curr_bf_->set(index, key_hash);
  }

  bool admissionPolicyTest(uint64_t key_hash) {
    uint64_t index = key_hash % num_buckets_;
    return prev_bf_->couldExist(index, key_hash) ||
           curr_bf_->couldExist(index, key_hash);
  }
};

} // namespace navy
} // namespace cachelib
} // namespace facebook