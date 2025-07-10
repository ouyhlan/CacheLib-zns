#pragma once
#include <cstdint>

#include "cachelib/navy/zone_hash/storage/ZonedAsyncStorageManager.h"
#include "cachelib/navy/zone_hash/utils/Types.h"

namespace facebook {
namespace cachelib {
namespace navy {

using SetCleanPageFn = std::function<void(uint32_t, FlashPageOffsetT)>;

class SetAsyncStorageManager : public ZonedAsyncStorageManager {
 public:
  SetAsyncStorageManager(ZoneManager& zns_mgr,
                         JobScheduler& scheduler,
                         ZoneNandType zone_type,
                         uint32_t initial_num_zones,
                         uint32_t num_clean_zones,
                         uint64_t page_size_byte,
                         SetCleanPageFn clean_fn);

  Task<> asyncPageAppend(SetIdT set_id,
                         Buffer buffer,
                         UpdatePageIndexFn update_fn);

  bool comparePageAge(FlashPageOffsetT a, FlashPageOffsetT b);

  Task<> addFreeZone(uint32_t zone_id);

  Task<uint32_t> getFreeZoneId();

  void addExternalZoneNum(uint32_t new_external_zones) {
    if (new_external_zones == 0) {
      return;
    }

    getFreeZoneFromExternal().detach();

    {
      auto clean_lock = std::unique_lock<std::mutex>(clean_mutex_);
      num_external_zones_ += new_external_zones - 1;
    }
  }

 private:
  const SetCleanPageFn set_clean_page_fn_;

  uint32_t num_external_zones_;

  void checkIfNeedGarbageCollection() override;

  Task<> garbageCollection() override;

  Task<> getFreeZoneFromExternal();

  uint32_t getZoneIdFromPageOffset(FlashPageOffsetT flash_page_offset) {
    return flash_page_offset / (zone_size_byte_ / page_size_byte_);
  }
};

} // namespace navy
} // namespace cachelib
} // namespace facebook