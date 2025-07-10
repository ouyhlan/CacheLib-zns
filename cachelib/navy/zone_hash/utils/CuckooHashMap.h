#pragma once

// Concurrent Cuckoo Hash for ZNS Cache
// forked from https://github.com/efficient/libcuckoo

#include <cstdint>
#include <tuple>
#include <vector>

#include "cachelib/common/Hash.h"
#include "cachelib/navy/zone_hash/utils/CuckooHashBucket.h"
#include "cachelib/navy/zone_hash/utils/CuckooHashConfig.h"
#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

namespace {
constexpr static size_t constPow(size_t a, size_t b) {
  return (b == 0) ? 1 : a * constPow(a, b - 1);
}
} // namespace

class CuckooHashMap {
  using Bucket = CuckooHashBucket;
  using Timestamp = CuckooHashTimestamp;
  using Lock = SpinLock;
  using LocksArray = std::vector<Lock>;

 public:
  class Iterator {
   public:
    Iterator() : end_(true) {}

    bool done() const { return end_; }

    FlashPageOffsetT flash_page_offset() const { return flash_page_offset_; }

   private:
    friend CuckooHashMap;

    SetIdT set_id_;
    Partial partial_;
    FlashPageOffsetT flash_page_offset_;
    bool end_;

    Iterator(SetIdT set_id, Partial partial, FlashPageOffsetT flash_page_offset)
        : set_id_(set_id),
          partial_(partial),
          flash_page_offset_(flash_page_offset),
          end_(false) {}
  };

  CuckooHashMap(uint64_t num_buckets, CompareFlashPageAgeFn compare_fn);

  bool insert(SetIdT set_id, FlashPageOffsetT flash_page_offset);

  // std::tuple<bool, FlashPageOffsetT> lookup(HashedKey hk);

  void remove(SetIdT set_id, FlashPageOffsetT removed_flash_page_offset);

  Iterator getFirstIterator(SetIdT set_id);
  Iterator getNext(Iterator it);

 private:
  const uint64_t index_mask_;
  const uint64_t num_buckets_;
  std::vector<Bucket> buckets_;
  std::vector<Timestamp> timestamps_;
  mutable LocksArray locks_;

  const CompareFlashPageAgeFn compare_flash_page_age_fn_;

  struct FOLLY_PACK_ATTR Set {
    uint32_t id;
    Partial partial;
  };

  enum class CuckooStatus {
    ok,
    failure,
    failureKeyNotFound,
    failureBucketFull
  };

  struct TablePosition {
    uint32_t index;
    uint32_t slot;
    CuckooStatus status;
  };

  struct CuckooRecord {
    uint32_t bucket_id;
    uint32_t slot;
    Set set;
    bool to_delete{false};
  };

  using CuckooRecordsArray = std::array<CuckooRecord, kMaxBFSPathLen>;

  struct SlotInfo {
    uint32_t bucket_id;
    uint16_t pathcode;
    uint8_t depth;
    bool is_oldest;

    SlotInfo() {}
    SlotInfo(const uint32_t b,
             const uint16_t p,
             const uint8_t d,
             const bool t = false)
        : bucket_id(b), pathcode(p), depth(d), is_oldest(t) {
      assert(d < kMaxBFSPathLen);
    }
  };

  class BQueue {
   public:
    BQueue() noexcept : first_(0), last_(0) {}

    void enqueue(SlotInfo x) {
      assert(!isFull());
      slots_[last_++] = x;
    }

    SlotInfo dequeue() {
      assert(!isEmpty());
      assert(first_ < last_);
      SlotInfo& x = slots_[first_++];

      return x;
    }

    bool isEmpty() const { return first_ == last_; }

    bool isFull() const { return last_ == maxCuckooCount; }

    static constexpr uint64_t maxCuckooCount =
        2 * ((kDefaultSlotPerBucket == 1)
                 ? kMaxBFSPathLen
                 : (constPow(kDefaultSlotPerBucket, kMaxBFSPathLen) - 1) /
                       (kDefaultSlotPerBucket - 1));

   private:
    SlotInfo slots_[maxCuckooCount];
    uint32_t first_;
    uint32_t last_;
  };

  // Hash function -- MurmurHash2
  // partial should be nonzero
  uint32_t altIndex(const uint32_t index, Partial partial) const {
    // ensure tag is nonzero for the multiply. 0xc6a4a7935bd1e995 is the
    // hash constant from 64-bit MurmurHash2
    const uint64_t nonzeroPartial = static_cast<uint64_t>(partial);
    return static_cast<uint32_t>(
        (index ^ (nonzeroPartial * 0xc6a4a7935bd1e995)) & index_mask_);
  }

  // uint32_t setIndex(const uint64_t key) {
  //   return static_cast<uint32_t>(key & index_mask_);
  // }

  Set getSetFromBucket(const Bucket& b,
                       const uint32_t bucket_id,
                       const uint32_t slot) {
    assert(b.isOccupied(slot));
    Partial partial = b.partial(slot);
    return {b.isInplace(slot) ? bucket_id : altIndex(bucket_id, partial),
            partial};
  }

  // Lock utils functions
  static uint32_t lockIndex(const uint32_t bucket_id) {
    return bucket_id & (kMaxNumLocks - 1);
  }

  struct LockDeleter {
    void operator()(SpinLock* l) const { l->unlock(); }
  };
  using LockManager = std::unique_ptr<SpinLock, LockDeleter>;

  // Give two buckets interface
  class TwoBuckets {
   public:
    TwoBuckets() {}

    TwoBuckets(LocksArray& locks, uint32_t i1, uint32_t i2)
        : i1(i1),
          i2(i2),
          first_manager_(&locks[lockIndex(i1)]),
          second_manager_((lockIndex(i1) != lockIndex(i2))
                              ? &locks[lockIndex(i2)]
                              : nullptr) {}

    void unlock() {
      first_manager_.reset();
      second_manager_.reset();
    }

    uint32_t i1, i2;

   private:
    LockManager first_manager_, second_manager_;
  };

  LockManager lockOne(uint32_t i) const;
  TwoBuckets lockTwo(uint32_t i1, uint32_t i2) const;
  TwoBuckets lockTwoForOneSet(const Set& set) const;
  std::pair<TwoBuckets, LockManager> lockThree(uint32_t i1,
                                               uint32_t i2,
                                               uint32_t i3) const;

  // utils function
  TablePosition cuckooInsert(TwoBuckets& b, const Set& set);
  TablePosition cuckooFind(FlashPageOffsetT& flash_page_offset,
                           const Set& set,
                           const uint32_t i1,
                           const uint32_t i2);

  // runCuckoo related function
  CuckooStatus runCuckoo(TwoBuckets& b,
                         uint32_t& insert_bucket,
                         uint32_t& insert_slot);
  int cuckooPathSearch(CuckooRecordsArray& cuckoo_path,
                       const uint32_t i1,
                       const uint32_t i2);
  bool cuckooPathMove(CuckooRecordsArray& cuckoo_path,
                      uint32_t depth,
                      TwoBuckets& b);
  SlotInfo slotSearch(const uint32_t i1, const uint32_t i2);

  // inplace means the key store in the buckets_[key]
  bool tryFindInsertBucket(const Bucket& b,
                           int& slot,
                           const Set& set,
                           bool inplace) const;
  bool tryReadPageOffsetFromBucket(FlashPageOffsetT& flash_page_offset,
                                   Bucket& b,
                                   int& slot,
                                   const Set& set,
                                   bool inplace) const;

  void deleteHashKeyFromBucket(const Set& set,
                               const uint32_t bucket_id,
                               const uint32_t slot);
  void addHashKeyToBucket(const Set& set,
                          const FlashPageOffsetT flash_page_offset,
                          const uint32_t bucket_id,
                          const uint32_t slot);
};

} // namespace navy
} // namespace cachelib
} // namespace facebook
