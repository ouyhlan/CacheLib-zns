#include "cachelib/navy/common/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

constexpr uint32_t tagBits = 16;
static constexpr uint32_t maxTagValue = 1 << tagBits;
static constexpr int tagSeed = 111;
uint32_t createTag(HashedKey hk) {
  return hashBuffer(makeView(hk.key()), tagSeed) % maxTagValue;
}

uint32_t createTag(BufferView key) {
  return hashBuffer(key, tagSeed) % maxTagValue;
}

uint32_t createTag(folly::StringPiece key) {
  return folly::hash::SpookyHashV2::Hash64(key.data(), key.size(), tagSeed) % maxTagValue;
}

} // namespace navy
} // namespace cachelib
} // namespace facebook