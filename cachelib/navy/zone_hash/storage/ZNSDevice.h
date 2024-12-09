#pragma once

#include <folly/Format.h>

#include <cstdint>
#include <stdexcept>
#include <vector>

#include "cachelib/navy/common/Device.h"
#include "libzbd/zbd.h"

namespace facebook {
namespace cachelib {
namespace navy {

class ZNSDevice : public Device {
 public:
  explicit ZNSDevice(int dev,
                     struct zbd_info* info,
                     struct zbd_zone* report,
                     uint32_t nr_zones,
                     uint64_t zoneMask,
                     uint64_t size,
                     uint32_t ioAlignSize,
                     uint64_t ioZoneCapSize,
                     uint64_t ioZoneSize,
                     std::shared_ptr<DeviceEncryptor> encryptor,
                     uint32_t maxDeviceWriteSize);

  ZNSDevice(const ZNSDevice&) = delete;
  ZNSDevice& operator=(const ZNSDevice&) = delete;

  ~ZNSDevice() override = default;

  uint64_t getCapacity(uint32_t zone_id) { return report_[zone_id].capacity; }

  uint32_t numZones() const { return nr_zones_; }

  void changeZoneIntoSLC(uint32_t zone_id);
  void changeZoneIntoSLC(std::vector<uint32_t> zone_ids);

  void changeZoneIntoQLC(uint32_t zone_id);
  void changeZoneIntoQLC(std::vector<uint32_t> zone_ids);

  // return append byte address
  uint64_t append(uint32_t zone_id, Buffer& buffer);

  bool write(uint64_t offset, Buffer buffer) = delete;

 private:
  const int dev_{};
  const uint32_t lba_size = 512;
  uint32_t nsid_;
  struct zbd_info* info_;
  struct zbd_zone* report_;
  unsigned int nr_zones_;

  bool finishImpl(uint64_t offset, uint32_t len) override;

  bool resetImpl(uint64_t offset, uint32_t len) override;

  bool writeImpl(uint64_t offset, uint32_t size, const void* value) override {
    ssize_t bytesWritten;

    bytesWritten = ::pwrite(dev_, value, size, offset);
    if (bytesWritten != size)
      XLOG(INFO) << "Error Writing to zone! offset: " << offset
                 << " size: " << size << " bytesWritten: " << bytesWritten;
    return bytesWritten == size;
  }

  bool readImpl(uint64_t offset, uint32_t size, void* value) override {
    ssize_t bytesRead;
    bytesRead = ::pread(dev_, value, size, offset);
    return bytesRead == size;
  }

  void flushImpl() override { ::fsync(dev_); }
};

} // namespace navy
} // namespace cachelib
} // namespace facebook