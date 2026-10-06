#pragma once

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <dev/block_request_model.hpp>

namespace ker::dev::ahci::async_rules {

inline constexpr size_t MAX_SLOTS = 8;
enum class Phase : uint8_t { free, preparing, active, completing };
struct Slot {
    Phase phase{Phase::free};
    BlockRequestToken token{};
    uint64_t generation{};
    uint64_t epoch{};
    uint64_t deadline_us{};
    size_t bytes{};
    int cancel_reason{};
    bool ncq{};
};
struct Port {
    std::array<Slot, MAX_SLOTS> slots{};
    uint64_t epoch{1};
    size_t depth{1};
    bool frozen{};
    bool offline{};
    int reset_reason{};
};

// The production port guard serializes these allocation-free rules. Metadata
// generations reject old software observations; COMRESET is separately needed
// to prevent an old hardware FIS from clearing a newly reused NCQ tag.
inline auto owned(const Port& port) -> size_t {
    size_t count = 0;
    for (auto const& slot : port.slots) {
        if (slot.phase != Phase::free) ++count;
    }
    return count;
}
inline auto reserve(Port& port, BlockRequestToken token, size_t bytes, uint64_t deadline_us, bool ncq) -> size_t {
    if (port.frozen || port.offline || port.epoch == 0 || port.depth == 0 || port.depth > MAX_SLOTS) return MAX_SLOTS;
    size_t const COUNT = owned(port);
    if (COUNT >= port.depth || (!ncq && COUNT != 0)) return MAX_SLOTS;
    for (auto const& slot : port.slots) {
        if (slot.phase != Phase::free && !slot.ncq) return MAX_SLOTS;
    }
    for (size_t i = 0; i < port.depth; ++i) {
        auto& slot = port.slots[i];
        if (slot.phase != Phase::free || slot.generation == UINT64_MAX) continue;
        uint64_t const GENERATION = slot.generation + 1;
        slot = {Phase::preparing, token, GENERATION, port.epoch, deadline_us, bytes, 0, ncq};
        return i;
    }
    // Free slots with exhausted generations can never be used again.
    if (COUNT == 0) port.offline = true;
    return MAX_SLOTS;
}
inline auto publish(Port& port, size_t index, uint64_t generation) -> bool {
    if (index >= port.slots.size() || port.frozen || port.offline) return false;
    auto& slot = port.slots[index];
    if (slot.phase != Phase::preparing || slot.generation != generation || slot.epoch != port.epoch || slot.cancel_reason != 0)
        return false;
    slot.phase = Phase::active;
    return true;
}
inline auto claim(Port& port, size_t index, uint64_t generation, uint64_t epoch, uint32_t ci, uint32_t sact) -> bool {
    if (index >= port.slots.size() || port.frozen || port.offline) return false;
    auto& slot = port.slots[index];
    uint32_t const BIT = uint32_t{1} << index;
    if (slot.phase != Phase::active || slot.generation != generation || slot.epoch != epoch || epoch != port.epoch ||
        ((ci | sact) & BIT) != 0)
        return false;
    slot.phase = Phase::completing;
    return true;
}
inline auto release(Port& port, size_t index, uint64_t generation) -> bool {
    if (index >= port.slots.size()) return false;
    auto& slot = port.slots[index];
    if (slot.phase == Phase::free || slot.generation != generation) return false;
    uint64_t const GENERATION = slot.generation;
    slot = {};
    slot.generation = GENERATION;
    return true;
}
inline auto cancel(Port& port, BlockRequestToken token, int reason) -> bool {
    if (reason >= 0) return false;
    for (auto& slot : port.slots) {
        if ((slot.phase == Phase::preparing || slot.phase == Phase::active) && slot.token == token) {
            if (slot.cancel_reason == 0) slot.cancel_reason = reason;
            port.frozen = true;
            if (port.reset_reason == 0) port.reset_reason = reason;
            return true;
        }
    }
    return false;
}
inline auto timeout(Port& port, uint64_t now_us) -> bool {
    for (auto& slot : port.slots) {
        if ((slot.phase == Phase::preparing || slot.phase == Phase::active) && slot.deadline_us != 0 && now_us >= slot.deadline_us) {
            slot.cancel_reason = -ETIMEDOUT;
            port.frozen = true;
            if (port.reset_reason == 0) port.reset_reason = -ETIMEDOUT;
            return true;
        }
    }
    return false;
}
inline void freeze(Port& port, int reason = -EIO) {
    port.frozen = true;
    if (port.reset_reason == 0) port.reset_reason = reason;
}
// Hardware must be stopped, its queue reset, all DMA and CPU completion readers
// retired, and every owned slot released before this can reopen admission.
inline auto finish_reset(Port& port, bool hardware_reset_proven) -> bool {
    if (!port.frozen || owned(port) != 0) return false;
    if (!hardware_reset_proven || port.epoch == UINT64_MAX) {
        port.offline = true;
        return false;
    }
    ++port.epoch;
    port.frozen = false;
    port.reset_reason = 0;
    return true;
}
}  // namespace ker::dev::ahci::async_rules
