#pragma once

#include <cstdint>

#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

struct __attribute__((__packed__)) SegmentIndexEntry {
  static constexpr uint8_t hitBits = 3;

  uint32_t logical_page_offset : 28;
  uint32_t valid : 1;
  uint32_t hits : hitBits;
  uint16_t tag;
  uint16_t next;

  uint32_t logicalPageOffset() const { return logical_page_offset; }

  void incrementHits() {
    if (hits < ((1 << hitBits) - 1)) {
      hits++;
    }
  }

  void populateEntry(LogicalPageOffset logical_page_offset,
                     uint32_t tag,
                     uint8_t hits) {
    this->logical_page_offset = logical_page_offset;
    this->tag = tag;
    this->valid = 1;
    this->hits = hits;
  }
};

} // namespace navy
} // namespace cachelib
} // namespace facebook