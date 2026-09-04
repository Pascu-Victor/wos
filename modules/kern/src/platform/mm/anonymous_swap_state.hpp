#pragma once

#include <cerrno>
#include <cstdint>
#include <platform/mm/swap.hpp>

namespace ker::mod::mm::swap {

// Pure ownership model for one private anonymous page.  It deliberately does
// not choose the production side-table or PTE representation; callers may use
// it under whichever page-state lock owns that representation.
enum class AnonymousPageState : uint8_t {
    RESIDENT,
    PAGEOUT_IN_PROGRESS,
    SWAPPED,
    PAGEIN_IN_PROGRESS,
    UNMAPPED,
    FAILED,
};

enum class AnonymousTransitionStatus : uint8_t {
    APPLIED,
    STALE,
    INVALID,
};

constexpr uint64_t INVALID_FRAME = UINT64_MAX;

struct AnonymousPageRecord {
    AnonymousPageState state = AnonymousPageState::UNMAPPED;
    uint64_t frame = INVALID_FRAME;
    SwapSlot slot{};
    uint64_t operation_generation = 0;
    int error = 0;
    bool cancel_requested = false;
};

[[nodiscard]] constexpr auto anonymous_page_state_valid(const AnonymousPageRecord& page) -> bool {
    bool const HAS_FRAME = page.frame != INVALID_FRAME;
    bool const HAS_SLOT = slot_valid(page.slot);
    switch (page.state) {
        case AnonymousPageState::RESIDENT:
            return HAS_FRAME && !HAS_SLOT && !page.cancel_requested && page.error == 0;
        case AnonymousPageState::PAGEOUT_IN_PROGRESS:
        case AnonymousPageState::PAGEIN_IN_PROGRESS:
            return HAS_FRAME && HAS_SLOT && page.error == 0;
        case AnonymousPageState::SWAPPED:
            return !HAS_FRAME && HAS_SLOT && !page.cancel_requested && page.error == 0;
        case AnonymousPageState::UNMAPPED:
            return !HAS_FRAME && !HAS_SLOT && !page.cancel_requested && page.error == 0;
        case AnonymousPageState::FAILED:
            return !HAS_FRAME && !HAS_SLOT && !page.cancel_requested && page.error < 0;
    }
    return false;
}

[[nodiscard]] constexpr auto anonymous_initialize_resident(AnonymousPageRecord& page, uint64_t frame) -> AnonymousTransitionStatus {
    if ((page.state != AnonymousPageState::UNMAPPED && page.state != AnonymousPageState::FAILED) || frame == INVALID_FRAME) {
        return AnonymousTransitionStatus::INVALID;
    }
    page.state = AnonymousPageState::RESIDENT;
    page.frame = frame;
    page.slot = invalid_slot();
    page.error = 0;
    page.cancel_requested = false;
    return AnonymousTransitionStatus::APPLIED;
}

[[nodiscard]] constexpr auto anonymous_begin_pageout(AnonymousPageRecord& page, SwapSlot slot, uint64_t* generation_out = nullptr)
    -> AnonymousTransitionStatus {
    if (!anonymous_page_state_valid(page) || page.state != AnonymousPageState::RESIDENT || !slot_valid(slot) ||
        page.operation_generation == UINT64_MAX) {
        return AnonymousTransitionStatus::INVALID;
    }
    page.operation_generation++;
    page.state = AnonymousPageState::PAGEOUT_IN_PROGRESS;
    page.slot = slot;
    if (generation_out != nullptr) {
        *generation_out = page.operation_generation;
    }
    return AnonymousTransitionStatus::APPLIED;
}

[[nodiscard]] constexpr auto anonymous_complete_pageout(AnonymousPageRecord& page, uint64_t generation, int result)
    -> AnonymousTransitionStatus {
    if (page.state != AnonymousPageState::PAGEOUT_IN_PROGRESS || generation != page.operation_generation) {
        return AnonymousTransitionStatus::STALE;
    }
    if (result == 0 && !page.cancel_requested) {
        page.state = AnonymousPageState::SWAPPED;
        page.frame = INVALID_FRAME;
    } else if (page.cancel_requested) {
        page.state = AnonymousPageState::UNMAPPED;
        page.frame = INVALID_FRAME;
        page.slot = invalid_slot();
        page.cancel_requested = false;
    } else {
        // A failed pageout retains the original resident frame and gives the
        // reserved slot back to the caller.
        page.state = AnonymousPageState::RESIDENT;
        page.slot = invalid_slot();
    }
    return AnonymousTransitionStatus::APPLIED;
}

[[nodiscard]] constexpr auto anonymous_begin_pagein(AnonymousPageRecord& page, uint64_t frame, uint64_t* generation_out = nullptr)
    -> AnonymousTransitionStatus {
    if (!anonymous_page_state_valid(page) || page.state != AnonymousPageState::SWAPPED || frame == INVALID_FRAME ||
        page.operation_generation == UINT64_MAX) {
        return AnonymousTransitionStatus::INVALID;
    }
    page.operation_generation++;
    page.state = AnonymousPageState::PAGEIN_IN_PROGRESS;
    page.frame = frame;
    if (generation_out != nullptr) {
        *generation_out = page.operation_generation;
    }
    return AnonymousTransitionStatus::APPLIED;
}

[[nodiscard]] constexpr auto anonymous_complete_pagein(AnonymousPageRecord& page, uint64_t generation, int result)
    -> AnonymousTransitionStatus {
    if (page.state != AnonymousPageState::PAGEIN_IN_PROGRESS || generation != page.operation_generation) {
        return AnonymousTransitionStatus::STALE;
    }
    if (result == 0 && !page.cancel_requested) {
        page.state = AnonymousPageState::RESIDENT;
        page.slot = invalid_slot();
    } else {
        page.frame = INVALID_FRAME;
        page.slot = invalid_slot();
        if (page.cancel_requested) {
            page.state = AnonymousPageState::UNMAPPED;
            page.error = 0;
        } else {
            page.state = AnonymousPageState::FAILED;
            page.error = result < 0 ? result : -EIO;
        }
        page.cancel_requested = false;
    }
    return AnonymousTransitionStatus::APPLIED;
}

[[nodiscard]] constexpr auto anonymous_request_unmap(AnonymousPageRecord& page) -> AnonymousTransitionStatus {
    if (!anonymous_page_state_valid(page)) {
        return AnonymousTransitionStatus::INVALID;
    }
    switch (page.state) {
        case AnonymousPageState::PAGEOUT_IN_PROGRESS:
        case AnonymousPageState::PAGEIN_IN_PROGRESS:
            page.cancel_requested = true;
            return AnonymousTransitionStatus::APPLIED;
        case AnonymousPageState::RESIDENT:
        case AnonymousPageState::SWAPPED:
        case AnonymousPageState::FAILED:
            page.state = AnonymousPageState::UNMAPPED;
            page.frame = INVALID_FRAME;
            page.slot = invalid_slot();
            page.error = 0;
            page.cancel_requested = false;
            return AnonymousTransitionStatus::APPLIED;
        case AnonymousPageState::UNMAPPED:
            return AnonymousTransitionStatus::STALE;
    }
    return AnonymousTransitionStatus::INVALID;
}

}  // namespace ker::mod::mm::swap
