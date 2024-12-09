#include "cachelib/navy/zone_hash/utils/CuckooHashMap.h"

#include <folly/Format.h>
#include <folly/logging/xlog.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <random>
#include <stdexcept>

#include "cachelib/navy/common/Types.h"
#include "cachelib/navy/zone_hash/utils/CuckooHashConfig.h"
#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

// utils function
#define LOW_MASK(n) ((1ull << (n)) - 1ull)

namespace {
static uint64_t reserve_calc(const uint64_t n) {
  uint64_t blog2;
  for (blog2 = 0; ((uint64_t)1 << blog2) < n; ++blog2)
    ;

  return blog2;
}
} // namespace

CuckooHashMap::CuckooHashMap(uint64_t n, CompareFlashPageAgeFn compare_fn)
    : index_mask_(LOW_MASK(reserve_calc(n))),
      num_buckets_(uint64_t(1) << reserve_calc(n)),
      compare_flash_page_age_fn_(compare_fn) {
  buckets_.resize(num_buckets_);
  timestamps_.resize(num_buckets_);
  locks_.resize(std::min((uint64_t)kMaxNumLocks, num_buckets_));

  // Random Initialize the timestamp of each bucket
  std::random_device rd;
  std::mt19937 gen(rd());
  std::uniform_int_distribution<Partial> dis(
      0, std::numeric_limits<Partial>::max());
  for (uint64_t i = 0; i < num_buckets_; i++) {
    timestamps_[i].initialize(dis(gen));
  }

  XLOG(INFO, folly::sformat("CuckooHashMap create: bucket size: {}", num_buckets_));
}

bool CuckooHashMap::insert(SetIdT set_id, FlashPageOffsetT flash_page_offset) {
  Set set;

  {
    auto lock_manager = lockOne(set_id);
    const Partial p = timestamps_[set_id].getNewTimestamp();
    set = {set_id, p};
  }

  auto b = lockTwoForOneSet(set);
  TablePosition pos = cuckooInsert(b, set);
  if (pos.status == CuckooStatus::ok) {
    addHashKeyToBucket(set, flash_page_offset, pos.index, pos.slot);
  } else {
    throw std::runtime_error(folly::sformat("Insert into cuckoo failed!"));
  }

  return pos.status == CuckooStatus::ok;
}

// std::tuple<bool, FlashPageOffsetT> CuckooHashMap::lookup(HashedKey hk) {
//   Partial p;
//   FlashPageOffsetT flash_page_offset;

//   uint32_t set_id = setIndex(hk.keyHash());
//   {
//     auto lock_manager = lockOne(set_id);
//     if (!timestamps_[set_id].tryReadNewestTimestamp(p)) {
//       return {false, 0};
//     }
//   }

//   Set set = {set_id, p};
//   bool done = false;
//   while (!done) {
//     TablePosition pos;
//     {
//       auto b = lockTwoForOneSet(set);
//       pos = cuckooFind(flash_page_offset, set, b.i1, b.i2);
//       if (pos.status == CuckooStatus::ok &&
//           !bf_reject_fn_(flash_page_offset, hk.keyHash())) {
//         done = true;
//         break;
//       }
//     }

//     XDCHECK(pos.status == CuckooStatus::failureKeyNotFound);
//     {
//       auto lock_manager = lockOne(set_id);
//       if (timestamps_[set_id].tryReadOlderTimestamp(p)) {
//         set = {set_id, p};
//       } else {
//         break;
//       }
//     }
//   }

//   return {done, flash_page_offset};
// }

void CuckooHashMap::remove(SetIdT set_id,
                           FlashPageOffsetT removed_flash_page_offset) {
  Partial p;
  {
    auto lock_manager = lockOne(set_id);
    if (!timestamps_[set_id].tryReadOldestTimestamp(p)) {
      // no page exist in current set_id
      return;
    }
  }

  Set set = {set_id, p};
  while (true) {
    TablePosition pos;
    {
      auto b = lockTwoForOneSet(set);
      FlashPageOffsetT curr_flash_page_offset;
      pos = cuckooFind(curr_flash_page_offset, set, b.i1, b.i2);
      if (pos.status == CuckooStatus::ok &&
          curr_flash_page_offset == removed_flash_page_offset) {
        deleteHashKeyFromBucket(set, pos.index, pos.slot);
        break;
      }
    }

    {
      auto lock_manager = lockOne(set_id);
      if (timestamps_[set_id].tryReadYoungerTimestamp(p)) {
        set = {set_id, p};
      } else {
        break;
      }
    }
  }
}

CuckooHashMap::Iterator CuckooHashMap::getFirstIterator(SetIdT set_id) {
  Partial p;
  {
    auto lock_manager = lockOne(set_id);
    if (!timestamps_[set_id].tryReadNewestTimestamp(p)) {
      return Iterator();
    }
  }

  FlashPageOffsetT flash_page_offset;
  Set set = {set_id, p};
  while (true) {
    TablePosition pos;
    {
      auto b = lockTwoForOneSet(set);
      pos = cuckooFind(flash_page_offset, set, b.i1, b.i2);
      if (pos.status == CuckooStatus::ok) {
        return Iterator(set_id, p, flash_page_offset);
      }
    }

    XDCHECK(pos.status == CuckooStatus::failureKeyNotFound);
    {
      auto lock_manager = lockOne(set_id);
      if (timestamps_[set_id].tryReadOlderTimestamp(p)) {
        set = {set_id, p};
      } else {
        break;
      }
    }
  }

  return Iterator();
}

CuckooHashMap::Iterator CuckooHashMap::getNext(Iterator it) {
  SetIdT set_id = it.set_id_;
  Partial p = it.partial_;
  {
    auto lock_manager = lockOne(set_id);
    if (!timestamps_[set_id].tryReadOlderTimestamp(p)) {
      return Iterator();
    }
  }

  FlashPageOffsetT flash_page_offset;
  Set set = {set_id, p};
  while (true) {
    TablePosition pos;
    {
      auto b = lockTwoForOneSet(set);
      pos = cuckooFind(flash_page_offset, set, b.i1, b.i2);
      if (pos.status == CuckooStatus::ok) {
        return Iterator(set_id, p, flash_page_offset);
      }
    }

    XDCHECK(pos.status == CuckooStatus::failureKeyNotFound);
    {
      auto lock_manager = lockOne(set_id);
      if (timestamps_[set_id].tryReadOlderTimestamp(p)) {
        set = {set_id, p};
      } else {
        break;
      }
    }
  }

  return Iterator();
}

// Lock functions
CuckooHashMap::LockManager CuckooHashMap::lockOne(uint32_t i) const {
  Lock& lock = locks_[lockIndex(i)];
  lock.lock();
  return LockManager(&lock);
}

CuckooHashMap::TwoBuckets CuckooHashMap::lockTwo(uint32_t i1,
                                                 uint32_t i2) const {
  uint32_t l1 = lockIndex(i1);
  uint32_t l2 = lockIndex(i2);

  if (l2 < l1) {
    std::swap(l1, l2);
  }

  locks_[l1].lock();
  if (l2 != l1) {
    locks_[l2].lock();
  }

  return TwoBuckets(locks_, i1, i2);
}

// Bucket1 = key, Bucket2 = key ^ Hash(partial)
CuckooHashMap::TwoBuckets CuckooHashMap::lockTwoForOneSet(
    const Set& set) const {
  const uint32_t i1 = set.id;
  const uint32_t i2 = altIndex(set.id, set.partial);

  return lockTwo(i1, i2);
}

std::pair<CuckooHashMap::TwoBuckets, CuckooHashMap::LockManager>
CuckooHashMap::lockThree(uint32_t i1, uint32_t i2, uint32_t i3) const {
  std::array<uint32_t, 3> l{lockIndex(i1), lockIndex(i2), lockIndex(i3)};
  // Lock in order
  if (l[2] < l[1]) {
    std::swap(l[2], l[1]);
  }
  if (l[2] < l[0]) {
    std::swap(l[2], l[0]);
  }
  if (l[1] < l[0]) {
    std::swap(l[1], l[0]);
  }

  locks_[l[0]].lock();
  if (l[1] != l[0]) {
    locks_[l[1]].lock();
  }
  if (l[2] != l[1]) {
    locks_[l[2]].lock();
  }
  return std::make_pair(TwoBuckets(locks_, i1, i2),
                        LockManager((lockIndex(i3) == lockIndex(i1) ||
                                     lockIndex(i3) == lockIndex(i2))
                                        ? nullptr
                                        : &locks_[lockIndex(i3)]));
}

// utils function
CuckooHashMap::TablePosition CuckooHashMap::cuckooInsert(TwoBuckets& b,
                                                         const Set& set) {
  int slot1, slot2;
  Bucket& b1 = buckets_[b.i1];
  if (tryFindInsertBucket(b1, slot1, set, true)) {
    return TablePosition{b.i1, static_cast<uint32_t>(slot1)};
  }
  Bucket& b2 = buckets_[b.i2];
  if (tryFindInsertBucket(b2, slot2, set, false)) {
    return TablePosition{b.i2, static_cast<uint32_t>(slot2)};
  }

  // We are unlucky, so let's perform cuckoo hashing.
  uint32_t insert_bucket = 0;
  uint32_t insert_slot = 0;

  // std::cout << "runCuckoo begin" << std::endl;
  CuckooStatus st = runCuckoo(b, insert_bucket, insert_slot);
  if (st == CuckooStatus::ok) {
    XDCHECK(!locks_[lockIndex(b.i1)].tryLock());
    XDCHECK(!locks_[lockIndex(b.i2)].tryLock());
    XDCHECK(!buckets_[insert_bucket].isOccupied(insert_slot));
    XDCHECK(insert_bucket == set.id ||
            insert_bucket == altIndex(set.id, set.partial));

    // Since we unlocked the buckets during run_cuckoo, another insert could
    // have inserted the same key into either b.i1 or b.i2, so we check for that
    // before doing the insert.
    // TODO: but the parital is keeped by the lock, so there might not have same
    // key

    // TablePosition pos = cuckooFind(set, b.i1, b.i2);
    // assert(pos.status != CuckooStatus::ok);

    return TablePosition{insert_bucket, insert_slot, CuckooStatus::ok};
  }
  XDCHECK(st == CuckooStatus::failure);
  return TablePosition{0, 0, CuckooStatus::failure};
}

CuckooHashMap::TablePosition CuckooHashMap::cuckooFind(
    FlashPageOffsetT& flash_page_offset,
    const Set& set,
    const uint32_t i1,
    const uint32_t i2) {
  int slot1, slot2;
  Bucket& b1 = buckets_[i1];
  if (tryReadPageOffsetFromBucket(flash_page_offset, b1, slot1, set,
                                  (set.id == i1))) {
    return TablePosition{i1, static_cast<uint32_t>(slot1), CuckooStatus::ok};
  }
  Bucket& b2 = buckets_[i2];
  if (tryReadPageOffsetFromBucket(flash_page_offset, b2, slot2, set,
                                  (set.id == i2))) {
    return TablePosition{i2, static_cast<uint32_t>(slot2), CuckooStatus::ok};
  }
  return TablePosition{0, 0, CuckooStatus::failureKeyNotFound};
}

CuckooHashMap::CuckooStatus CuckooHashMap::runCuckoo(TwoBuckets& b,
                                                     uint32_t& insert_bucket,
                                                     uint32_t& insert_slot) {
  b.unlock();
  CuckooRecordsArray cuckoo_path;
  bool done = false;
  while (!done) {
    // std::cout << "cuckooPathSearch(" << b.i1 << ", " << b.i2 << ") begin"
    //           << std::endl;
    const int depth = cuckooPathSearch(cuckoo_path, b.i1, b.i2);
    // if (depth < 0) {  // Search failed
    //   break;
    // }

    // std::cout << "cuckooPath[depth] = {" << cuckooPath[depth].bucketId << ","
    //           << cuckooPath[depth].toDelete << "} depth = " << depth
    //           << std::endl;
    // std::cout << "cuckooPathMove begin" << std::endl;
    if (cuckooPathMove(cuckoo_path, depth, b)) {
      insert_bucket = cuckoo_path[0].bucket_id;
      insert_slot = cuckoo_path[0].slot;
      XDCHECK(insert_bucket == b.i1 || insert_bucket == b.i2);
      XDCHECK(!locks_[lockIndex(b.i1)].tryLock());
      XDCHECK(!locks_[lockIndex(b.i2)].tryLock());
      // assert(!buckets_[insertBucket].isOccupied(insertSlot));
      done = true;
      break;
    }
  }
  return done ? CuckooStatus::ok : CuckooStatus::failure;
}

int CuckooHashMap::cuckooPathSearch(CuckooRecordsArray& cuckoo_path,
                                    const uint32_t i1,
                                    const uint32_t i2) {
  SlotInfo x = slotSearch(i1, i2);
  // if (x.isOldest == true) {
  //   return -1;
  // }
  // std::cout << "slotSearch end" << std::endl;

  // Fill in the cuckoo path slots from the end to the beginning.
  for (int i = x.depth; i >= 0; i--) {
    cuckoo_path[i].slot = x.pathcode % kDefaultSlotPerBucket;
    x.pathcode /= kDefaultSlotPerBucket;
  }

  CuckooRecord& first = cuckoo_path[0];
  if (x.pathcode == 0) {
    first.bucket_id = i1;
  } else {
    XDCHECK(x.pathcode == 1);
    first.bucket_id = i2;
  }
  {
    const auto lock_manager = lockOne(first.bucket_id);
    const Bucket& b = buckets_[first.bucket_id];

    // TODO:check if the code is valid
    for (int k = 0; k < kDefaultSlotPerBucket; ++k) {
      uint32_t temp_slot = (first.slot + k) % kDefaultSlotPerBucket;
      if (!b.isOccupied(temp_slot)) {
        first.slot = temp_slot;
        return 0; // find a free slot along the path
      }
    }

    first.set = getSetFromBucket(b, first.bucket_id, first.slot);
    first.to_delete = (x.depth == 0) ? x.is_oldest : false;
  }
  for (int i = 1; i <= x.depth; ++i) {
    CuckooRecord& curr = cuckoo_path[i];
    const CuckooRecord& prev = cuckoo_path[i - 1];
    XDCHECK(prev.bucket_id == prev.set.id ||
            prev.bucket_id == altIndex(prev.set.id, prev.set.partial));
    curr.bucket_id = altIndex(prev.bucket_id, prev.set.partial);

    const auto lock_manager = lockOne(curr.bucket_id);
    const Bucket& b = buckets_[curr.bucket_id];

    // TODO:check if the code is valid
    for (int k = 0; k < kDefaultSlotPerBucket; ++k) {
      uint32_t temp_slot = (curr.slot + k) % kDefaultSlotPerBucket;
      if (!b.isOccupied(temp_slot)) {
        curr.slot = temp_slot;
        return i;
      }
    }
    curr.set = getSetFromBucket(b, curr.bucket_id, curr.slot);
    curr.to_delete = (x.depth == i) ? x.is_oldest : false;
  }
  return x.depth;
}

// At the end of the function TwoBuckets lock must hold
bool CuckooHashMap::cuckooPathMove(CuckooRecordsArray& cuckoo_path,
                                   uint32_t depth,
                                   TwoBuckets& b) {
  if (depth == 0) {
    const uint32_t curr_id = cuckoo_path[0].bucket_id;
    XDCHECK(curr_id == b.i1 || curr_id == b.i2);

    b = lockTwo(b.i1, b.i2);

    // TODO:check if the code is valid
    // check if there is a free slot for us to avoid the deletion
    for (int k = 0; k < kDefaultSlotPerBucket; ++k) {
      uint32_t tempSlot = (cuckoo_path[0].slot + k) % kDefaultSlotPerBucket;
      if (!buckets_[curr_id].isOccupied(tempSlot)) {
        cuckoo_path[0].slot = tempSlot;
        return true;
      }
    }

    XDCHECK(buckets_[curr_id].isOccupied(cuckoo_path[0].slot));

    // Delete the selected oldest entry
    CuckooRecord& first = cuckoo_path[0];
    const uint32_t fs = first.slot;
    Bucket& fb = buckets_[first.bucket_id];
    if (!cuckoo_path[0].to_delete ||
        !fb.isIdentical(fs, first.set.partial,
                        (first.bucket_id == first.set.id))) {
      // Current Entry has been changed, search again
      b.unlock();
      return false;
    }

    XDCHECK(cuckoo_path[0].to_delete);
    // replaced_page = buckets_[first.bucketId].page(first.slot);
    deleteHashKeyFromBucket(first.set, first.bucket_id, first.slot);
    return true;
  }

  while (depth > 0) {
    CuckooRecord& from = cuckoo_path[depth - 1];
    CuckooRecord& to = cuckoo_path[depth];
    const uint32_t fs = from.slot;
    uint32_t ts = to.slot;
    TwoBuckets two_b;
    LockManager extraManager;
    if (depth == 1) {
      std::tie(two_b, extraManager) = lockThree(b.i1, b.i2, to.bucket_id);
    } else {
      two_b = lockTwo(from.bucket_id, to.bucket_id);
    }

    Bucket& fb = buckets_[from.bucket_id];
    Bucket& tb = buckets_[to.bucket_id];

    // TODO:check if the code is valid
    // Delete the selected oldest entry
    if (cuckoo_path[depth].to_delete) {
      bool done = false;
      for (int k = 0; k < kDefaultSlotPerBucket; ++k) {
        uint32_t tempSlot = (ts + k) % kDefaultSlotPerBucket;
        if (!tb.isOccupied(tempSlot)) {
          ts = tempSlot;
          done = true;
          break;
        }
      }

      if (!done &&
          tb.isIdentical(ts, to.set.partial, (to.bucket_id == to.set.id))) {
        // replaced_page = tb.page(ts);
        deleteHashKeyFromBucket(to.set, to.bucket_id, ts);
      }
    }

    if (tb.isOccupied(ts) || !fb.isOccupied(fs) ||
        !fb.isIdentical(fs, from.set.partial,
                        (from.bucket_id == from.set.id))) {
      return false;
    }

    tb.setEntry(ts, fb.flashPageOffset(fs), fb.partial(fs), !fb.isInplace(fs),
                fb.isOccupied(fs));
    fb.clearEntry(fs);
    if (depth == 1) {
      b = std::move(two_b);
    }
    depth--;
  }
  return true;
}

// BFS search for empty slot
CuckooHashMap::SlotInfo CuckooHashMap::slotSearch(const uint32_t i1,
                                                  const uint32_t i2) {
  // std::cout << "slotSearch(" << i1 << ", " << i2 << ") begin" << std::endl;
  bool have_oldest_page = false;
  FlashPageOffsetT oldest_page_offset;
  SlotInfo oldest_entry(0, 0, 0);

  BQueue q;
  std::unordered_set<uint32_t> iter_set; // avoid search loop
  iter_set.reserve(BQueue::maxCuckooCount);

  q.enqueue(SlotInfo(i1, 0, 0));
  q.enqueue(SlotInfo(i2, 1, 0));
  iter_set.emplace(i1);
  iter_set.emplace(i2);
  while (!q.isEmpty()) {
    SlotInfo x = q.dequeue();
    // std::cout << "x {" << x.bucketId << ", " << (uint64_t)x.depth << ", "
    //           << x.isOldest << ", " << x.pathcode << "} dequeue" <<
    //           std::endl;

    auto lock_manager = lockOne(x.bucket_id);
    Bucket& b = buckets_[x.bucket_id];

    uint16_t startSlot = x.pathcode % kDefaultSlotPerBucket;
    for (uint16_t i = 0; i < kDefaultSlotPerBucket; ++i) {
      uint16_t slot = (startSlot + i) % kDefaultSlotPerBucket;
      if (!b.isOccupied(slot)) {
        x.pathcode = x.pathcode * kDefaultSlotPerBucket + slot;
        // std::cout << "x {" << x.bucketId << ", " << (uint64_t)x.depth << ", "
        //           << x.isOldest << ", " << x.pathcode << "} return"
        //           << std::endl;
        return x;
      }

      XDCHECK(b.isOccupied(slot));

      FlashPageOffsetT curr_flash_page_offset = b.flashPageOffset(slot);
      if (!have_oldest_page ||
          compare_flash_page_age_fn_(oldest_page_offset,
                                     curr_flash_page_offset)) {
        oldest_entry = x;
        oldest_entry.pathcode = x.pathcode * kDefaultSlotPerBucket + slot;
        oldest_page_offset = curr_flash_page_offset;
        have_oldest_page = true;
      }

      const Partial partial = b.partial(slot);
      const uint32_t y_bucket_id = altIndex(x.bucket_id, partial);
      if ((x.depth < kMaxBFSPathLen - 1) &&
          (iter_set.count(y_bucket_id) == 0)) {
        XDCHECK(!q.isFull());
        SlotInfo y(y_bucket_id, x.pathcode * kDefaultSlotPerBucket + slot,
                   x.depth + 1);
        iter_set.emplace(y_bucket_id);
        q.enqueue(y);
      }
    }
  }

  // Exceed the max search depth, can't find any free slot
  // return the oldest entry
  oldest_entry.is_oldest = true;
  // std::cout << "oldestEntry {" << oldestEntry.bucketId << ", "
  //           << (uint64_t)oldestEntry.depth << ", " << oldestEntry.isOldest
  //           << ", " << oldestEntry.pathcode << "} return" << std::endl;
  return oldest_entry;
}

bool CuckooHashMap::tryFindInsertBucket(const Bucket& b,
                                        int& slot,
                                        const Set& set,
                                        bool inplace) const {
  for (int i = 0; i < kDefaultSlotPerBucket; ++i) {
    XDCHECK(!b.isIdentical(i, set.partial, inplace));
    if (!b.isOccupied(i)) {
      slot = i;
      return true;
    }
  }

  return false;
}

// get the flash page offset from Bucket
bool CuckooHashMap::tryReadPageOffsetFromBucket(
    FlashPageOffsetT& flash_page_offset,
    Bucket& b,
    int& slot,
    const Set& set,
    bool inplace) const {
  for (int i = 0; i < kDefaultSlotPerBucket; ++i) {
    if (b.isIdentical(i, set.partial, inplace)) {
      slot = i;
      flash_page_offset = b.flashPageOffset(i);
      return true;
    }
  }

  return false;
}

void CuckooHashMap::deleteHashKeyFromBucket(const Set& set,
                                            const uint32_t bucket_id,
                                            const uint32_t slot) {
  XDCHECK(bucket_id == set.id || bucket_id == altIndex(set.id, set.partial));

  // std::cout << "deleteHashKeyFromBucket(" << set.id << ", "
  //           << static_cast<uint64_t>(set.partial) << "}, " << bucketId << ",
  //           "
  //           << slot << ")" << std::endl;
  // std::cout << "delete page value = " << buckets_[bucketId].value(slot)
  //           << std::endl;

  timestamps_[set.id].removeTimestamp(set.partial);
  buckets_[bucket_id].clearEntry(slot);
  locks_[lockIndex(bucket_id)].decrElemCounter();
}

void CuckooHashMap::addHashKeyToBucket(const Set& set,
                                       const FlashPageOffsetT flash_page_offset,
                                       const uint32_t bucket_id,
                                       const uint32_t slot) {
  XDCHECK(bucket_id == set.id || bucket_id == altIndex(set.id, set.partial));

  // std::cout << "addHashKeyToBucket({" << set.id << ", "
  //           << static_cast<uint64_t>(set.partial) << "}, " << value << ", "
  //           << bucketId << ", " << slot << ")" << std::endl;

  buckets_[bucket_id].setEntry(slot, flash_page_offset, set.partial,
                               (bucket_id == set.id), true);
  locks_[lockIndex(bucket_id)].incrElemCounter();
}

} // namespace navy
} // namespace cachelib
} // namespace facebook