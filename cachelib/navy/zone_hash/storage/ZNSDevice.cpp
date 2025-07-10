#include "cachelib/navy/zone_hash/storage/ZNSDevice.h"

#include <folly/Format.h>
#include <folly/logging/xlog.h>
#include <liburing.h>
#include <sys/socket.h>

#include <cstdint>
#include <memory>
#include <stdexcept>

#include "cachelib/navy/zone_hash/scheduler/IOScheduler.h"
#include "cachelib/navy/zone_hash/utils/Types.h"
#include "libzbd/zbd.h"

namespace facebook {
namespace cachelib {
namespace navy {

ZNSDevice::ZNSDevice(struct zbd_info* info,
                     struct zbd_zone* report,
                     uint32_t nr_zones,
                     uint64_t size,
                     uint32_t ioAlignSize,
                     uint64_t ioZoneCapSize,
                     uint64_t ioZoneSize,
                     std::shared_ptr<DeviceEncryptor> encryptor,
                     uint32_t maxDeviceWriteSize,
                     std::string char_device_path)
    : Device{size,     std::move(encryptor), ioAlignSize,  maxDeviceWriteSize,
             nr_zones, ioZoneSize,           ioZoneCapSize},
      info_{info},
      nr_zones_{std::move(nr_zones)} {
  for (uint32_t i = 0; i < nr_zones_; i++) {
    capacity_.push_back(report[i].capacity);
    if (report[i].capacity == ioZoneSize) {
      type_arr_.push_back(ZoneNandType::QLC);
    } else {
      type_arr_.push_back(ZoneNandType::SLC);
    }
  }

  fd_ = open(char_device_path.c_str(), O_RDWR);
  if (fd_ < 0) {
    throw std::invalid_argument(
        folly::sformat("Open Char Device {} failed!", char_device_path));
  }

  int err = nvme_get_nsid(fd_, &nsid_);
  if (err < 0) {
    throw std::invalid_argument("Error get namespace id!");
  }

  io_uring_params p = {};
  p.flags = IORING_SETUP_SQE128 | IORING_SETUP_CQE32 | IORING_SETUP_SQPOLL;
  p.sq_thread_idle = 2000;
  err = io_uring_queue_init_params(queue_depth, &initialer_uring_, &p);
  if (err) {
    throw std::runtime_error(folly::sformat(
        "io_uring_queue_init_params failed, error code: {}", err));
  }
  if (!(p.features & IORING_FEAT_SQPOLL_NONFIXED)) {
    throw std::invalid_argument("No SQPOLL sharing, skipping");
  }

  io_scheduler_ = std::make_unique<IOScheduler>(
      initialer_uring_, fd_, nsid_, queue_depth, lba_size, flush_num_threads,
      backend_num_threads);

  poll_io_context_ = std::make_unique<PollIOContext>(
      initialer_uring_, fd_, nsid_, lba_size, page_size, queue_depth);
}

void ZNSDevice::changeZonesIntoSLC(std::vector<uint32_t> zone_ids) {
  std::vector<uint64_t> slba_arr(zone_ids.size());
  for (uint32_t i = 0; i < zone_ids.size(); i++) {
    slba_arr[i] = zone_ids[i] * info_->zone_sectors;
  }

  poll_io_context_->changeZonesIntoSLC(slba_arr);
  for (auto zone_id : zone_ids) {
    capacity_[zone_id] = info_->zone_size / 4;
    type_arr_[zone_id] = ZoneNandType::SLC;
  }
}

void ZNSDevice::changeZonesIntoQLC(std::vector<uint32_t> zone_ids) {
  std::vector<uint64_t> slba_arr(zone_ids.size());
  for (uint32_t i = 0; i < zone_ids.size(); i++) {
    slba_arr[i] = zone_ids[i] * info_->zone_sectors;
  }

  poll_io_context_->changeZonesIntoQLC(slba_arr);
  for (auto zone_id : zone_ids) {
    capacity_[zone_id] = info_->zone_size;
    type_arr_[zone_id] = ZoneNandType::QLC;
  }
}

void ZNSDevice::changeZoneIntoSLC(uint32_t zone_id) {
  uint64_t slba = zone_id * info_->zone_sectors;
  poll_io_context_->changeZoneIntoSLC(slba);
  capacity_[zone_id] = info_->zone_size / 4;
  type_arr_[zone_id] = ZoneNandType::SLC;
}

void ZNSDevice::changeZoneIntoQLC(uint32_t zone_id) {
  uint64_t slba = zone_id * info_->zone_sectors;
  poll_io_context_->changeZoneIntoQLC(slba);
  capacity_[zone_id] = info_->zone_size;
  type_arr_[zone_id] = ZoneNandType::QLC;
}

uint64_t ZNSDevice::append(uint32_t zone_id, Buffer& buffer) {
  const uint32_t size = buffer.size();
  XDCHECK_LE(size, maxWriteSize_);

  auto timeBegin = getSteadyClock();
  uint64_t result = poll_io_context_->append(zone_id * info_->zone_sectors,
                                             buffer.data(), size);
  writeLatencyEstimator_.trackValue(
      toMicros((getSteadyClock() - timeBegin)).count());

  bytesWritten_.add(size);
  return result * lba_size;
}

bool ZNSDevice::finishImpl(uint64_t offset, uint32_t len) {
  if (zbd_finish_zones(fd_, offset, len) < 0)
    return false;
  return true;
}

bool ZNSDevice::resetImpl(uint64_t offset, uint32_t len) {
  if (!finishImpl(offset, len))
    return false;
  if (zbd_reset_zones(fd_, offset, len) < 0)
    return false;
  return true;
}

} // namespace navy
} // namespace cachelib
} // namespace facebook