#pragma once

#include <cstddef>
#include <cstdint>
#include <dev/dma/dma.hpp>

namespace ker::dev::virtio::managed {
constexpr uint64_t LEGACY_QUEUE_MASK = (uint64_t{1} << 44) - 1;  // 32-bit PFN, 4096-byte pages.
constexpr size_t PAGE = 4096;
struct Layout {
    size_t available = 0;
    size_t used = 0;
    size_t bytes = 0;
};
[[nodiscard]] constexpr auto layout(uint16_t size) -> Layout {
    if (size == 0 || size > 256 || (size & (size - 1)) != 0) return {};
    size_t const AVAILABLE = size_t{16} * size;
    size_t const USED = (AVAILABLE + size_t{2} * (3 + size) + PAGE - 1) & ~(PAGE - 1);
    return {AVAILABLE, USED, USED + ((size_t{6} + size_t{8} * size + PAGE - 1) & ~(PAGE - 1))};
}

// No destructor frees memory. A prepared coherent object can outlive both a
// timed-out command and the code which submitted it. Keep its owner until reset.
struct Region {
    dma::Allocation allocation{};
    dma::Submission submission{};
    bool exposed = false;
    Region() = default;
    Region(const Region&) = delete;
    auto operator=(const Region&) -> Region& = delete;

    template <typename Api>
    [[nodiscard]] auto allocate(Api& api, const pci::PCIDevice* device, size_t bytes, uint64_t mask = UINT64_MAX) -> bool {
        if (allocation.handle.generation != 0) return false;
        auto const RESULT = api.allocate_coherent(device, bytes, PAGE, mask);
        if (RESULT.error != dma::Error::none) return false;
        allocation = RESULT.allocation;
        return true;
    }
    template <typename Api>
    [[nodiscard]] auto prepare(Api& api) -> bool {
        if (submission.sequence != 0) return false;
        auto const RESULT = api.prepare(allocation.handle);
        if (RESULT.error != dma::Error::none) return false;
        submission = RESULT.submission;
        return true;
    }
    // Call immediately before publishing any address or making a descriptor eligible.
    [[nodiscard]] auto expose() -> bool {
        if (submission.sequence == 0 || exposed) return false;
        exposed = true;
        return true;
    }
    // Caller must drain every software reader first. quiesced means reset
    // acknowledged AND BME disabled, not merely a FAILED status or timeout.
    template <typename Api>
    [[nodiscard]] auto retire(Api& api, bool quiesced) -> bool {
        if (allocation.handle.generation == 0) return true;
        if (exposed && !quiesced) {
            (void)api.quarantine(allocation.handle);
            return false;
        }
        if (submission.sequence != 0) {
            auto const PROOF = exposed ? dma::Retirement::device_quiesced : dma::Retirement::never_submitted;
            if (api.complete(submission, 0, PROOF) != dma::Error::none) {
                (void)api.quarantine(allocation.handle);
                return false;
            }
            submission = {};
        }
        if (api.release(allocation.handle) != dma::Error::none) return false;
        allocation = {};
        exposed = false;
        return true;
    }
};

// Explicit adapter keeps production and host ownership transitions identical.
struct DmaApi {
    decltype(&dma::allocate_coherent) allocate_coherent = dma::allocate_coherent;
    decltype(&dma::prepare) prepare = dma::prepare;
    decltype(&dma::complete) complete = dma::complete;
    decltype(&dma::release) release = dma::release;
    decltype(&dma::quarantine) quarantine = dma::quarantine;
};

// Deadline and iteration limits both apply: a stalled clock cannot hang probe.
template <typename Backend>
[[nodiscard]] auto reset(Backend& backend, size_t max_polls) -> bool {
    backend.write_reset();
    for (size_t i = 0; i < max_polls; ++i) {
        if (backend.read_status() == 0) return true;
        if (backend.expired()) break;
        backend.relax();
    }
    return false;
}
}  // namespace ker::dev::virtio::managed
