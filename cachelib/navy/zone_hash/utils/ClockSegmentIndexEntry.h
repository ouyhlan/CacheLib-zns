#pragma once

#include <folly/logging/xlog.h>

#include <cstdint>

#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

class __attribute__((__packed__)) ClockSegmentIndexEntry {
 public:
  ClockSegmentIndexEntry() : valid_(0) {}

  bool valid() const { return valid_; }

  uint32_t tag() const { return tag_; }

  uint16_t next() const { return next_; }

  uint32_t logicalPageOffset() const { return logical_page_offset_; }

  void setNext(uint16_t next) { next_ = next; }

  void setInvalid() { valid_ = 0; }

  void setEntry(LogicalPageOffset logical_page_offset, uint32_t tag) {
    this->logical_page_offset_ = logical_page_offset;
    this->tag_ = tag;
    this->valid_ = 1;
  }

 private:
  static constexpr uint8_t clockBits = 2;

  uint32_t logical_page_offset_ : 31;
  uint32_t valid_ : 1;
  uint16_t tag_;
  uint16_t next_;
};

} // namespace navy
} // namespace cachelib
} // namespace facebook