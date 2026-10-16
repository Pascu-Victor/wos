#include <array>
#include <cerrno>
#include <dev/ahci.hpp>
#include <dev/ahci_async.hpp>
#include <dev/block_device.hpp>
#include <dev/block_request.hpp>
#include <memory>
#include <new>
#include <test/ktest.hpp>

namespace ahci = ker::dev::ahci;
namespace rules = ahci::async_rules;

KTEST(AhciAsync, SlotRetirementRequiresCopyAndResetProof) {
    rules::Port port;
    port.depth = rules::MAX_SLOTS;
    for (size_t i = 0; i < rules::MAX_SLOTS; ++i) {
        KREQUIRE_EQ(rules::reserve(port, {1, i + 1, static_cast<uint16_t>(i)}, 512, 100, true), i);
        KREQUIRE_TRUE(rules::publish(port, i, 1));
        KEXPECT_FALSE(rules::claim(port, i, 1, 1, 0, uint32_t{1} << i));
    }
    KREQUIRE_TRUE(rules::timeout(port, 100));
    KEXPECT_FALSE(rules::finish_reset(port, true));
    for (size_t i = 0; i < rules::MAX_SLOTS; ++i) {
        KEXPECT_FALSE(rules::claim(port, i, 1, 1, 0, 0));
        KREQUIRE_TRUE(rules::release(port, i, 1));
    }
    KREQUIRE_TRUE(rules::finish_reset(port, true));
    KEXPECT_EQ(port.epoch, uint64_t{2});
}

#ifdef WOS_ASYNC_BLOCK_IO
namespace {
template <size_t Bytes = 512>
struct NativeRequests {
    ker::dev::BlockDevice* disk{};
    std::array<ker::dev::BlockRequest, rules::MAX_SLOTS> requests{};
    std::array<std::array<std::byte, Bytes>, rules::MAX_SLOTS> bytes{};

    ~NativeRequests() {
        // KREQUIRE returns through this scope. Never leave a stack descriptor
        // or SG behind an accepted hardware/queue request on early failure.
        for (auto& request : requests) {
            while (request.queue != nullptr) {
                if (!request.completed.load(std::memory_order_acquire)) static_cast<void>(ker::dev::block_request_cancel(request));
                static_cast<void>(ker::dev::block_request_wait(request));
                KEXPECT_EQ(ker::dev::block_request_release(request), 0);
                ker::dev::block_queue_service(disk);
            }
        }
    }
};
}  // namespace

KTEST(AhciAsync, NativeReadBatchRetiresEveryRequestAndDma) {
    auto* disk = ker::dev::block_device_find_by_name("sda");
    ahci::AhciAsyncSnapshot before{};
    KREQUIRE_TRUE(ahci::ahci_async_snapshot(disk, before));
    constexpr size_t TRANSFER_BYTES = size_t{512} * 1024;
    std::unique_ptr<NativeRequests<TRANSFER_BYTES>> fixture(new (std::nothrow) NativeRequests<TRANSFER_BYTES>{disk});
    KREQUIRE_TRUE(fixture != nullptr);
    auto& requests = fixture->requests;
    auto& bytes = fixture->bytes;
    for (size_t i = 0; i < requests.size(); ++i) {
        ker::dev::BlockSegment const sg{bytes[i].data(), bytes[i].size()};
        KREQUIRE_EQ(ker::dev::block_request_submit(disk, requests[i], ker::dev::BlockRequestOperation::READ, i * (TRANSFER_BYTES / 512),
                                                   TRANSFER_BYTES / 512, {&sg, 1}),
                    0);
    }
    for (auto& request : requests) {
        KEXPECT_EQ(ker::dev::block_request_wait(request), 0);
        KEXPECT_EQ(request.bytes, TRANSFER_BYTES);
        KEXPECT_EQ(ker::dev::block_request_release(request), 0);
    }
    ahci::AhciAsyncSnapshot after{};
    KREQUIRE_TRUE(ahci::ahci_async_snapshot(disk, after));
    KEXPECT_EQ(after.active, size_t{0});
    KEXPECT_TRUE(after.submitted >= before.submitted + requests.size());
    KEXPECT_TRUE(after.completed >= before.completed + requests.size());
    KEXPECT_TRUE(after.high_water >= after.depth);
    // Hardware may retire before MMIO sampling; negotiated depth still bounds the sample.
    KEXPECT_TRUE(after.hardware_high_water <= after.depth);
    KEXPECT_EQ(after.quarantines, before.quarantines);
    ker::mod::dbg::logger<"ahci">::info(
        "KTEST native batch depth=%zu software_high=%zu hardware_high=%zu ci=0x%x sact=0x%x cap=0x%x words75=0x%x words76=0x%x "
        "words83=0x%x",
        after.depth, after.high_water, after.hardware_high_water, after.current_ci, after.current_sact, after.hba_cap,
        after.identify_queue_depth, after.identify_sata_caps, after.identify_command_sets);
}

KTEST(AhciAsync, TimeoutResetFailsReadAndRestoresAdmission) {
    auto* disk = ker::dev::block_device_find_by_name("sda");
    ahci::AhciAsyncSnapshot before{};
    KREQUIRE_TRUE(ahci::ahci_async_snapshot(disk, before));
    NativeRequests fixture{disk};
    auto& bytes = fixture.bytes[0];
    ker::dev::BlockSegment const sg{bytes.data(), bytes.size()};
    auto& request = fixture.requests[0];
    KREQUIRE_TRUE(ahci::selftest_inject_async_fault(disk, ahci::SelftestAsyncFault::timeout));
    KREQUIRE_EQ(ker::dev::block_request_submit(disk, request, ker::dev::BlockRequestOperation::READ, 0, 1, {&sg, 1}), 0);
    KEXPECT_EQ(ker::dev::block_request_wait(request), -ETIMEDOUT);
    KEXPECT_EQ(request.bytes, size_t{0});
    KREQUIRE_EQ(ker::dev::block_request_release(request), 0);
    ker::dev::block_queue_service(disk);  // Retire callbacks and the queue reset epoch.
    KEXPECT_EQ(ker::dev::block_read(disk, 0, 1, bytes.data()), 0);
    ahci::AhciAsyncSnapshot after{};
    KREQUIRE_TRUE(ahci::ahci_async_snapshot(disk, after));
    KEXPECT_TRUE(after.timeouts > before.timeouts);
    KEXPECT_TRUE(after.resets > before.resets);
    KEXPECT_EQ(after.active, size_t{0});
    KEXPECT_FALSE(after.offline);
}

KTEST(AhciAsync, TaskfileErrorFailsEveryRequestBeforeReadCopyAndReopensAfterReset) {
    auto* disk = ker::dev::block_device_find_by_name("sda");
    ahci::AhciAsyncSnapshot before{};
    KREQUIRE_TRUE(ahci::ahci_async_snapshot(disk, before));
    NativeRequests fixture{disk};
    for (size_t i = 0; i < fixture.requests.size(); ++i) {
        fixture.bytes[i].fill(std::byte{0xcc});
        ker::dev::BlockSegment const sg{fixture.bytes[i].data(), fixture.bytes[i].size()};
        KREQUIRE_EQ(ker::dev::block_request_submit(disk, fixture.requests[i], ker::dev::BlockRequestOperation::READ, i, 1, {&sg, 1}), 0);
    }
    KREQUIRE_TRUE(ahci::selftest_inject_async_fault(disk, ahci::SelftestAsyncFault::taskfile));
    for (size_t i = 0; i < fixture.requests.size(); ++i) {
        KEXPECT_EQ(ker::dev::block_request_wait(fixture.requests[i]), -EIO);
        KEXPECT_EQ(fixture.requests[i].bytes, size_t{0});
        for (auto const byte : fixture.bytes[i]) KEXPECT_EQ(byte, std::byte{0xcc});
        KREQUIRE_EQ(ker::dev::block_request_release(fixture.requests[i]), 0);
    }
    ker::dev::block_queue_service(disk);
    KEXPECT_EQ(ker::dev::block_read(disk, 0, 1, fixture.bytes[0].data()), 0);
    ahci::AhciAsyncSnapshot after{};
    KREQUIRE_TRUE(ahci::ahci_async_snapshot(disk, after));
    KEXPECT_TRUE(after.resets > before.resets);
    KEXPECT_EQ(after.active, size_t{0});
    KEXPECT_EQ(after.quarantines, before.quarantines);
    KEXPECT_FALSE(after.offline);
}

KTEST(AhciAsync, QueuedCancellationRetiresWithoutDmaAndAllowsExclusiveFlush) {
    auto* disk = ker::dev::block_device_find_by_name("sda");
    ahci::AhciAsyncSnapshot before{};
    KREQUIRE_TRUE(ahci::ahci_async_snapshot(disk, before));
    NativeRequests fixture{disk};
    fixture.bytes[0].fill(std::byte{0xcc});
    ker::dev::BlockSegment const sg{fixture.bytes[0].data(), fixture.bytes[0].size()};
    auto& request = fixture.requests[0];
    KREQUIRE_EQ(ker::dev::block_request_submit(disk, request, ker::dev::BlockRequestOperation::READ, 0, 1, {&sg, 1}), 0);
    KREQUIRE_EQ(ker::dev::block_request_cancel(request), 0);
    KEXPECT_EQ(ker::dev::block_request_wait(request), -ECANCELED);
    KEXPECT_EQ(request.bytes, size_t{0});
    for (auto const byte : fixture.bytes[0]) KEXPECT_EQ(byte, std::byte{0xcc});
    KREQUIRE_EQ(ker::dev::block_request_release(request), 0);
    ahci::AhciAsyncSnapshot cancelled{};
    KREQUIRE_TRUE(ahci::ahci_async_snapshot(disk, cancelled));
    KEXPECT_EQ(cancelled.submitted, before.submitted);
    KEXPECT_EQ(cancelled.completed, before.completed);
    auto& flush = fixture.requests[1];
    KREQUIRE_EQ(ker::dev::block_request_submit(disk, flush, ker::dev::BlockRequestOperation::FLUSH, 0, 0, {}), 0);
    bool const FLUSH_SUPPORTED = (before.identify_command_sets & 0xc000U) == 0x4000U && (before.identify_command_sets & (1U << 13)) != 0;
    KEXPECT_EQ(ker::dev::block_request_wait(flush), FLUSH_SUPPORTED ? 0 : -EOPNOTSUPP);
    KEXPECT_EQ(flush.bytes, size_t{0});
    KREQUIRE_EQ(ker::dev::block_request_release(flush), 0);
    ahci::AhciAsyncSnapshot after{};
    KREQUIRE_TRUE(ahci::ahci_async_snapshot(disk, after));
    KEXPECT_EQ(after.active, size_t{0});
    KEXPECT_EQ(after.quarantines, before.quarantines);
}
#endif
