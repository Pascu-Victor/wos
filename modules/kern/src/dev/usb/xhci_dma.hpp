#pragma once

#include <algorithm>
#include <array>
#include <dev/dma/dma.hpp>

namespace ker::dev::usb::xhci_dma {
constexpr size_t DCBAA = 0, SCRATCH_ARRAY = 1, COMMAND = 2, EVENT = 3, ERST = 4, SCRATCH_FIRST = 5;
constexpr size_t CAPACITY = SCRATCH_FIRST + 1024;
constexpr size_t MAX_TRANSFER = 0x1ffff;
constexpr size_t MAX_SEGMENTS = 3;
struct Segment {
    uint64_t address{};
    size_t offset{}, length{};
    uint32_t td_size{};
};
struct TransferPlan {
    std::array<Segment, MAX_SEGMENTS> segments{};
    size_t count{};
    bool valid{};
};
constexpr auto plan_transfer(uint64_t address, size_t bytes, size_t max_packet, size_t first_segment_limit = SIZE_MAX) -> TransferPlan {
    TransferPlan plan;
    if (bytes > MAX_TRANSFER || max_packet == 0 || first_segment_limit == 0 || (bytes != 0 && bytes - 1 > UINT64_MAX - address))
        return plan;
    size_t const PACKETS = bytes / max_packet + (bytes % max_packet != 0);
    for (size_t offset = 0; offset < bytes;) {
        if (plan.count == plan.segments.size()) return {};
        size_t length = std::min(bytes - offset, size_t{65536} - static_cast<size_t>((address + offset) & 65535U));
        if (offset == 0) length = std::min(length, first_segment_limit);
        size_t const LENGTH = length;
        auto const REMAINING = offset + LENGTH == bytes ? 0 : PACKETS - (offset + LENGTH) / max_packet;
        plan.segments.at(plan.count++) = {address + offset, offset, LENGTH, static_cast<uint32_t>(std::min(REMAINING, size_t{31}))};
        offset += LENGTH;
    }
    plan.valid = true;
    return plan;
}

// HCSPARAMS2: bits 25:21 are Hi; bits 31:27 are Lo (SPR bit 26 is separate).
constexpr auto scratchpad_count(uint32_t parameters) -> size_t {
    return (((parameters >> 21U) & 0x1fU) << 5U) | ((parameters >> 27U) & 0x1fU);
}

enum class StopAction : uint8_t { none, command, unproven };
// Disabled and Stopped have no running ring. Other non-Running states need
// recovery beyond this slot-retirement path; never infer stop from an error.
constexpr auto endpoint_stop_action(uint32_t context_state) -> StopAction {
    switch (context_state & 7U) {
        case 0:
        case 3:
            return StopAction::none;
        case 1:
            return StopAction::command;
        default:
            return StopAction::unproven;
    }
}

struct Api {
    decltype(&dma::bind_streaming) bind_streaming = dma::bind_streaming;
    decltype(&dma::prepare) prepare = dma::prepare;
    decltype(&dma::complete) complete = dma::complete;
    decltype(&dma::release) release = dma::release;
    decltype(&dma::quarantine) quarantine = dma::quarantine;
};

// The binding is always our owned CPU view, so a timeout cannot retain a
// caller's stack/PacketBuffer. Explicit retirement only; no destructor free.
struct Streaming {
    dma::Allocation allocation{};
    dma::Submission submission{};
    bool exposed = false;
    Streaming() = default;
    Streaming(const Streaming&) = delete;
    auto operator=(const Streaming&) -> Streaming& = delete;
    template <typename A>
    auto prepare(A& api, dma::Direction direction) -> bool {
        if (submission.sequence != 0 || allocation.cpu == nullptr) return false;
        if (api.bind_streaming(allocation.handle, {allocation.cpu, allocation.size}, direction) != dma::Error::none) return false;
        auto const R = api.prepare(allocation.handle);
        if (R.error != dma::Error::none) return false;
        submission = R.submission;
        return true;
    }
    void expose() { exposed = submission.sequence != 0; }
    template <typename A>
    auto complete(A& api, size_t bytes, dma::Retirement proof) -> bool {
        if (submission.sequence == 0) return allocation.handle.generation == 0;
        if ((exposed && proof == dma::Retirement::never_submitted) || bytes > submission.size ||
            api.complete(submission, bytes, proof) != dma::Error::none) {
            (void)api.quarantine(allocation.handle);
            return false;
        }
        submission = {};
        exposed = false;
        return true;
    }
    template <typename A>
    auto release(A& api) -> bool {
        if (allocation.handle.generation == 0) return true;
        if (submission.sequence != 0) return false;
        if (api.release(allocation.handle) != dma::Error::none) {
            (void)api.quarantine(allocation.handle);
            return false;
        }
        allocation = {};
        return true;
    }
    template <typename A>
    auto retire(A& api, bool hardware_and_readers_stopped) -> bool {
        if (exposed && !hardware_and_readers_stopped) {
            (void)api.quarantine(allocation.handle);
            return false;
        }
        if (submission.sequence != 0 && !complete(api, 0, exposed ? dma::Retirement::device_quiesced : dma::Retirement::never_submitted))
            return false;
        return release(api);
    }
};

// Explicit ownership, with no destructor release. Exposure is monotonic.
struct Coherent {
    dma::Allocation allocation{};
    dma::Submission submission{};
    bool exposed = false;
    Coherent() = default;
    Coherent(const Coherent&) = delete;
    auto operator=(const Coherent&) -> Coherent& = delete;

    template <typename A>
    auto prepare(A& api) -> bool {
        if (submission.sequence != 0) return false;
        auto const R = api.prepare(allocation.handle);
        if (R.error != dma::Error::none) return false;
        submission = R.submission;
        return true;
    }
    void expose() { exposed = true; }
    template <typename A>
    auto retire(A& api, bool hardware_and_readers_stopped) -> bool {
        if (allocation.handle.generation == 0) return true;
        if (exposed && !hardware_and_readers_stopped) {
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
        if (api.release(allocation.handle) != dma::Error::none) {
            (void)api.quarantine(allocation.handle);
            return false;
        }
        allocation = {};
        exposed = false;
        return true;
    }
};
}  // namespace ker::dev::usb::xhci_dma
