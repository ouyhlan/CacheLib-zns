#include "cachelib/navy/zone_hash/storage/ZNSDevice.h"

#include <folly/Format.h>
#include <folly/logging/xlog.h>
#include <sys/socket.h>

#include <cstdint>
#include <stdexcept>

#include "libnvme.h"
#include "libzbd/zbd.h"

namespace facebook {
namespace cachelib {
namespace navy {

ZNSDevice::ZNSDevice(int dev,
                     struct zbd_info* info,
                     struct zbd_zone* report,
                     uint32_t nr_zones,
                     uint64_t zoneMask,
                     uint64_t size,
                     uint32_t ioAlignSize,
                     uint64_t ioZoneCapSize,
                     uint64_t ioZoneSize,
                     std::shared_ptr<DeviceEncryptor> encryptor,
                     uint32_t maxDeviceWriteSize)
    : Device{size,     std::move(encryptor), ioAlignSize,  maxDeviceWriteSize,
             nr_zones, ioZoneSize,           ioZoneCapSize},
      dev_{std::move(dev)},
      info_{info},
      report_{std::move(report)},
      nr_zones_{std::move(nr_zones)} {
  if (nvme_get_nsid(dev_, &nsid_) < 0) {
    throw std::runtime_error("Failed to get ZNS Device namespace id");
  }
}

void ZNSDevice::changeZoneIntoSLC(uint32_t zone_id) {
  nvme_zns_mgmt_send_args args = {.slba = zone_id * info_->zone_sectors,
                                  .result = NULL,
                                  .data = NULL,
                                  .args_size = sizeof(nvme_zns_mgmt_send_args),
                                  .fd = dev_,
                                  .timeout = 0,
                                  .nsid = nsid_,
                                  .zsa = (enum nvme_zns_send_action)0x12,
                                  .data_len = 0,
                                  .select_all = false,
                                  .zsaso = 0};
  int err = nvme_zns_mgmt_send(&args);
  if (err) {
    throw std::runtime_error(
        folly::sformat("Failed to change zone #{} into SLC", zone_id));
  }

  uint32_t nr_zones;
  zbd_list_zones(dev_, 0, 0, ZBD_RO_ALL, &report_, &nr_zones);
}

void ZNSDevice::changeZoneIntoSLC(std::vector<uint32_t> zone_ids) {
  nvme_zns_mgmt_send_args args = {.result = NULL,
                                  .data = NULL,
                                  .args_size = sizeof(nvme_zns_mgmt_send_args),
                                  .fd = dev_,
                                  .timeout = 0,
                                  .nsid = nsid_,
                                  .zsa = (enum nvme_zns_send_action)0x12,
                                  .data_len = 0,
                                  .select_all = false,
                                  .zsaso = 0};

  for (auto zone_id : zone_ids) {
    args.slba = zone_id * info_->zone_sectors;
    int err = nvme_zns_mgmt_send(&args);
    if (err) {
      throw std::runtime_error(
          folly::sformat("Failed to change zone #{} into SLC", zone_id));
    }
  }

  uint32_t nr_zones;
  zbd_list_zones(dev_, 0, 0, ZBD_RO_ALL, &report_, &nr_zones);
}

void ZNSDevice::changeZoneIntoQLC(uint32_t zone_id) {
  nvme_zns_mgmt_send_args args = {.slba = zone_id * info_->zone_sectors,
                                  .result = NULL,
                                  .data = NULL,
                                  .args_size = sizeof(nvme_zns_mgmt_send_args),
                                  .fd = dev_,
                                  .timeout = 0,
                                  .nsid = nsid_,
                                  .zsa = (enum nvme_zns_send_action)0x13,
                                  .data_len = 0,
                                  .select_all = false,
                                  .zsaso = 0};
  int err = nvme_zns_mgmt_send(&args);
  if (err) {
    throw std::runtime_error(
        folly::sformat("Failed to change zone #{} into QLC", zone_id));
  }

  uint32_t nr_zones;
  zbd_list_zones(dev_, 0, 0, ZBD_RO_ALL, &report_, &nr_zones);
}

void ZNSDevice::changeZoneIntoQLC(std::vector<uint32_t> zone_ids) {
  nvme_zns_mgmt_send_args args = {.result = NULL,
                                  .data = NULL,
                                  .args_size = sizeof(nvme_zns_mgmt_send_args),
                                  .fd = dev_,
                                  .timeout = 0,
                                  .nsid = nsid_,
                                  .zsa = (enum nvme_zns_send_action)0x13,
                                  .data_len = 0,
                                  .select_all = false,
                                  .zsaso = 0};
  for (auto zone_id : zone_ids) {
    args.slba = zone_id * info_->zone_sectors;
    int err = nvme_zns_mgmt_send(&args);
    if (err) {
      throw std::runtime_error(
          folly::sformat("Failed to change zone #{} into QLC", zone_id));
    }
  }

  uint32_t nr_zones;
  zbd_list_zones(dev_, 0, 0, ZBD_RO_ALL, &report_, &nr_zones);
}

uint64_t ZNSDevice::append(uint32_t zone_id, Buffer& buffer) {
  const uint32_t size = buffer.size();
  XDCHECK_LE(size, maxWriteSize_);

  unsigned long long result;
  uint16_t nblocks = (size / lba_size) - 1;
  nvme_zns_append_args args = {.zslba = zone_id * info_->zone_sectors,
                               .result = &result,
                               .data = buffer.data(),
                               .metadata = NULL,
                               .args_size = sizeof(nvme_zns_append_args),
                               .fd = dev_,
                               .timeout = 0,
                               .nsid = nsid_,
                               .ilbrt = 0,
                               .data_len = size,
                               .metadata_len = 0,
                               .nlb = nblocks,
                               .control = 0,
                               .lbat = 0,
                               .lbatm = 0,
                               .ilbrt_u64 = 0};

  auto timeBegin = getSteadyClock();
  int err = nvme_zns_append(&args);
  writeLatencyEstimator_.trackValue(
      toMicros((getSteadyClock() - timeBegin)).count());

  if (err) {
    throw std::runtime_error(
        folly::sformat("Failed to append zone {}", zone_id));
  } else {
    bytesWritten_.add(size);
  }

  return result * lba_size;
}

bool ZNSDevice::finishImpl(uint64_t offset, uint32_t len) {
  if (zbd_finish_zones(dev_, offset, len) < 0)
    return false;
  return true;
}

bool ZNSDevice::resetImpl(uint64_t offset, uint32_t len) {
  if (!finishImpl(offset, len))
    return false;
  if (zbd_reset_zones(dev_, offset, len) < 0)
    return false;
  return true;
}

} // namespace navy
} // namespace cachelib
} // namespace facebook