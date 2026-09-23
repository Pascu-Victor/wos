#pragma once

#include <dev/dma/dma.hpp>
#include <span>

namespace ker::dev::e1000e::managed {
constexpr size_t RX_BYTES = 2048;

// DD retires a buffer even for a dropped frame. Never copy an unchecked length.
constexpr auto rx_valid(uint8_t status, uint8_t errors, size_t bytes, bool discarding) -> bool {
    return (status & 3U) == 3U && errors == 0 && bytes != 0 && bytes <= RX_BYTES && !discarding;
}
constexpr auto tx_valid(uintptr_t storage, size_t capacity, uintptr_t data, size_t bytes) -> bool {
    return data >= storage && data - storage <= capacity && bytes != 0 && bytes <= UINT16_MAX && bytes <= capacity - (data - storage);
}

struct Api {
    decltype(&dma::prepare) prepare = dma::prepare;
    decltype(&dma::bind_streaming) bind_streaming = dma::bind_streaming;
    decltype(&dma::complete) complete = dma::complete;
    decltype(&dma::release) release = dma::release;
    decltype(&dma::quarantine) quarantine = dma::quarantine;
};

// Explicit terminal retirement only; destruction never frees exposed storage.
// 'exposed' is monotonic, including across streaming completion/reuse.
struct Buffer {
    dma::Allocation allocation{};
    dma::Submission submission{};
    bool exposed = false;
    Buffer() = default;
    Buffer(const Buffer&) = delete;
    auto operator=(const Buffer&) -> Buffer& = delete;

    template <typename A>
    auto prepare(A& api) -> bool {
        if (submission.sequence != 0) return false;
        auto const R = api.prepare(allocation.handle);
        if (R.error != dma::Error::none) return false;
        submission = R.submission;
        return true;
    }
    template <typename A>
    auto bind(A& api, std::span<std::byte> bytes, dma::Direction direction) -> bool {
        return submission.sequence == 0 && api.bind_streaming(allocation.handle, bytes, direction) == dma::Error::none && prepare(api);
    }
    auto expose() -> bool {
        if (submission.sequence == 0) return false;
        exposed = true;
        return true;
    }
    template <typename A>
    auto complete(A& api, size_t bytes) -> bool {
        if (submission.sequence == 0 || !exposed || api.complete(submission, bytes, dma::Retirement::command_completed) != dma::Error::none)
            return false;
        submission = {};
        return true;
    }
    // True also means any borrowed CPU packet may now be freed. A failed
    // quarantine retains it; ordinary quarantine returns Error::quarantined.
    template <typename A>
    auto retire(A& api, bool quiesced) -> bool {
        if (allocation.handle.generation == 0) return true;
        if (exposed && !quiesced) return api.quarantine(allocation.handle) == dma::Error::quarantined;
        if (submission.sequence != 0) {
            auto const PROOF = exposed ? dma::Retirement::device_quiesced : dma::Retirement::never_submitted;
            if (api.complete(submission, 0, PROOF) != dma::Error::none) return api.quarantine(allocation.handle) == dma::Error::quarantined;
            submission = {};
        }
        auto const RESULT = api.release(allocation.handle);
        if (RESULT == dma::Error::none) allocation = {};
        return RESULT == dma::Error::none || RESULT == dma::Error::quarantined;
    }
};
}  // namespace ker::dev::e1000e::managed
