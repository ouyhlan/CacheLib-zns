#pragma once

#include <cstddef>
#include <cstdint>

namespace facebook {
namespace cachelib {
namespace navy {

// The default maximum number of keys per bucket
constexpr size_t kDefaultSlotPerBucket = 16;

// maximum number of locks
constexpr size_t kMaxNumLocks = 1ul << 16;

// Cuckoo replace maximum times
constexpr uint8_t kMaxBFSPathLen = 3;

using Partial = uint8_t;

} // namespace navy
} // namespace cachelib
} // namespace facebook