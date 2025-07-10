#pragma once

#include <folly/Format.h>
#include <folly/logging/xlog.h>

#include <cstdint>
#include <memory>
#include <vector>

#include "cachelib/common/AtomicCounter.h"
#include "cachelib/navy/common/Device.h"
#include "cachelib/navy/common/Utils.h"
#include "cachelib/navy/zone_hash/scheduler/IOExecutor.h"
#include "cachelib/navy/zone_hash/scheduler/IOScheduler.h"
#include "cachelib/navy/zone_hash/scheduler/PollIOContext.h"
#include "cachelib/navy/zone_hash/scheduler/Task.h"
#include "cachelib/navy/zone_hash/utils/Types.h"
#include "libzbd/zbd.h"

namespace facebook {
namespace cachelib {
namespace navy {

class ZNSDevice : public Device {
  constexpr static uint32_t queue_depth = 256;
  constexpr static uint32_t lba_size = 512;
  constexpr static uint32_t page_size = 4096;
  // constexpr static uint32_t read_num_threads = 2;
  constexpr static uint32_t flush_num_threads = 2;
  constexpr static uint32_t backend_num_threads = 6;

 public:
  explicit ZNSDevice(struct zbd_info* info,
                     struct zbd_zone* report,
                     uint32_t nr_zones,
                     uint64_t size,
                     uint32_t ioAlignSize,
                     uint64_t ioZoneCapSize,
                     uint64_t ioZoneSize,
                     std::shared_ptr<DeviceEncryptor> encryptor,
                     uint32_t maxDeviceWriteSize,
                     std::string char_device_path);

  ZNSDevice(const ZNSDevice&) = delete;
  ZNSDevice& operator=(const ZNSDevice&) = delete;

  ~ZNSDevice() {
    if (io_scheduler_) {
      io_scheduler_->finish();
    }

    close(fd_);
  }

  void setupIOScheduler(std::unique_ptr<IOScheduler> io_scheduler) {
    io_scheduler_ = std::move(io_scheduler);
  }

  uint64_t getCapacity(uint32_t zone_id) { return capacity_[zone_id]; }

  uint32_t numZones() const { return nr_zones_; }

  void changeZonesIntoSLC(std::vector<uint32_t> zone_ids);
  void changeZonesIntoQLC(std::vector<uint32_t> zone_ids);

  void changeZoneIntoSLC(uint32_t zone_id);
  void changeZoneIntoQLC(uint32_t zone_id);

  // return append byte address
  uint64_t append(uint32_t zone_id, Buffer& buffer);

  auto asyncSchedule(AsyncJobType job_type, bool need_new_schedule) {
    return io_scheduler_->schedule(job_type, need_new_schedule);
  }

  Task<> asyncChangeZoneIntoSLC(uint32_t zone_id) {
    uint64_t slba = zone_id * info_->zone_sectors;

    auto& io_executor = co_await asyncSchedule(AsyncJobType::Backend, false);
    co_await io_executor.changeZoneIntoSLC(slba);
    capacity_[zone_id] = info_->zone_size / 4;
    type_arr_[zone_id] = ZoneNandType::SLC;
  }

  Task<> asyncChangeZoneIntoQLC(uint32_t zone_id) {
    uint64_t slba = zone_id * info_->zone_sectors;

    auto& io_executor = co_await asyncSchedule(AsyncJobType::Backend, false);
    co_await io_executor.changeZoneIntoQLC(slba);
    capacity_[zone_id] = info_->zone_size;
    type_arr_[zone_id] = ZoneNandType::QLC;
  }

  // Task<uint64_t> asyncAppend(uint32_t zone_id, Buffer buffer) {
  //   const uint32_t size = buffer.size();
  //   XDCHECK_LE(size, maxWriteSize_);

  //   auto& io_executor = co_await
  //   io_scheduler_->schedule(AsyncJobType::Backend); auto time_begin =
  //   getSteadyClock(); auto result = co_await io_executor.append(zone_id *
  //   info_->zone_sectors,
  //                                             buffer.data(), size);
  //   writeLatencyEstimator_.trackValue(
  //       toMicros((getSteadyClock() - time_begin)).count());
  //   bytesWritten_.add(size);

  //   co_return getFlashByteAddressFromLBAAddress(result);
  // }

  Task<uint64_t> asyncAppend(IOExecutor& io_executor,
                             uint32_t zone_id,
                             Buffer buffer) {
    const uint32_t size = buffer.size();
    XDCHECK_LE(size, maxWriteSize_);

    auto time_begin = getSteadyClock();
    auto result = co_await io_executor.append(zone_id * info_->zone_sectors,
                                              buffer.data(), size);
    writeLatencyEstimator_.trackValue(
        toMicros((getSteadyClock() - time_begin)).count());
    bytesWritten_.add(size);
    if (type_arr_[zone_id] == ZoneNandType::QLC) {
      numQLCBytesWritten_.add(size);
    } else {
      numSLCBytesWritten_.add(size);
    }

    co_return getFlashByteAddressFromLBAAddress(result);
  }

  Task<Buffer> asyncRead(IOExecutor& io_executor,
                         FlashByteAddressT flash_byte_address,
                         uint32_t size) {
    uint64_t slba = getLBAAddressFromByteAddress(flash_byte_address);
    auto buffer = makeIOBuffer(size);
    XDCHECK(!buffer.isNull() &&
            flash_byte_address <= nr_zones_ * info_->zone_size);

    auto time_begin = getSteadyClock();
    co_await io_executor.read(slba, buffer.data(), buffer.size());
    readLatencyEstimator_.trackValue(
        toMicros((getSteadyClock() - time_begin)).count());
    bytesRead_.add(size);

    co_return buffer;
  }

  Task<> asyncReset(uint32_t zone_id) {
    auto& io_executor = co_await asyncSchedule(AsyncJobType::Backend, false);
    co_await io_executor.reset(zone_id * info_->zone_sectors);
  }

  bool write(uint64_t offset, Buffer buffer) = delete;

 private:
  std::unique_ptr<IOScheduler> io_scheduler_;
  std::unique_ptr<PollIOContext> poll_io_context_;
  int fd_;
  uint32_t nsid_;
  struct zbd_info* info_;
  std::vector<uint64_t> capacity_;
  std::vector<ZoneNandType> type_arr_;
  unsigned int nr_zones_;

  io_uring initialer_uring_;

  bool finishImpl(uint64_t offset, uint32_t len) override;

  bool resetImpl(uint64_t offset, uint32_t len) override;

  bool writeImpl(uint64_t offset, uint32_t size, const void* value) override {
    ssize_t bytesWritten;

    bytesWritten = ::pwrite(fd_, value, size, offset);
    if (bytesWritten != size)
      XLOG(INFO) << "Error Writing to zone! offset: " << offset
                 << " size: " << size << " bytesWritten: " << bytesWritten;
    return bytesWritten == size;
  }

  bool readImpl(uint64_t offset, uint32_t size, void* value) override {
    uint64_t slba = getLBAAddressFromByteAddress(offset);
    poll_io_context_->read(slba, value, size);
    return true;
  }

  void flushImpl() override { /* do nothing since directio no need to flush*/ }

  uint64_t getLBAAddressFromByteAddress(FlashByteAddressT flash_byte_address) {
    return flash_byte_address / lba_size;
  }

  FlashByteAddressT getFlashByteAddressFromLBAAddress(uint64_t lba_address) {
    return lba_address * lba_size;
  }
};

} // namespace navy
} // namespace cachelib
} // namespace facebook