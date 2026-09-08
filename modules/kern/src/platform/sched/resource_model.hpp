#pragma once

#include <abi/resource.hpp>
#include <array>
#include <cerrno>

namespace ker::mod::sched::resource {
using abi::resource::Denial;
using abi::resource::Id;
using abi::resource::Limit;
using abi::resource::Usage;
inline constexpr auto INFINITY_VALUE = abi::resource::INFINITY_VALUE;

// No offset + count addition: both a finite endpoint and UINT64_MAX remain
// safe at the boundary. Empty writes never require space, even beyond EOF.
constexpr auto file_write_budget(uint64_t offset, uint64_t count, uint64_t limit) -> uint64_t {
    if (count == 0 || offset >= limit) {
        return 0;
    }
    return count < limit - offset ? count : limit - offset;
}

constexpr auto add_extent(uint64_t& total, uint64_t count, uint64_t unit) -> bool {
    if (unit != 0 && count > (UINT64_MAX - total) / unit) {
        return false;
    }
    total += count * unit;
    return true;
}

// Backing whole pages must not exceed either the policy or reserved geometry.
constexpr auto stack_backing_budget(uint64_t soft, uint64_t capacity, uint64_t page_size) -> uint64_t {
    uint64_t const BOUND = soft < capacity ? soft : capacity;
    return BOUND - BOUND % page_size;
}

constexpr auto saturating_cpu_add(uint64_t first, uint64_t second) -> uint64_t {
    return second > UINT64_MAX - first ? UINT64_MAX : first + second;
}

enum class CpuAction { NONE, SOFT_SIGNAL, HARD_KILL };
inline constexpr uint64_t CPU_US_PER_SECOND = 1'000'000;

// Pure bounded state machine. The caller serializes every access, including
// inheritance, with the process resource lock. No allocation or callbacks.
struct State {
    std::array<Usage, abi::resource::COUNT> entries{};
    uint64_t cpu_time_us = 0;
    uint64_t cpu_user_time_us = 0;
    uint64_t cpu_system_time_us = 0;
    uint64_t cpu_last_soft_second = UINT64_MAX;
    bool cpu_hard_notified = false;

    constexpr explicit State(uint64_t max_fds = 256) {
        for (auto& entry : entries) {
            entry.limit = {INFINITY_VALUE, INFINITY_VALUE};
        }
        entries[static_cast<unsigned>(Id::NOFILE)].limit = {max_fds, max_fds};
    }

    auto set(unsigned id, Limit value, bool privileged, uint64_t max_fds) -> int {
        if (id >= entries.size() || value.soft > value.hard) {
            return -EINVAL;
        }
        auto& entry = entries[id];
        if ((!privileged && value.hard > entry.limit.hard) || (id == static_cast<unsigned>(Id::NOFILE) && value.hard > max_fds)) {
            return -EPERM;
        }
        entry.limit = value;
        return 0;
    }

    void deny(unsigned id, Denial reason) {
        auto& entry = entries[id];
        if (entry.denied != UINT64_MAX) {
            ++entry.denied;
        }
        entry.reason = reason;
    }

    [[nodiscard]] auto admit_extent(unsigned id, uint64_t size) -> bool {
        if (size > entries[id].limit.soft) {
            deny(id, Denial::SOFT_LIMIT);
            return false;
        }
        return true;
    }

    void commit_extent(unsigned id, uint64_t size) {
        auto& entry = entries[id];
        entry.current = size;
        if (size > entry.peak) {
            entry.peak = size;
        }
    }

    [[nodiscard]] auto admit_process(uint64_t current) -> bool {
        constexpr unsigned NPROC = static_cast<unsigned>(Id::NPROC);
        if (current == UINT64_MAX) {
            deny(NPROC, Denial::OVERFLOW);
            return false;
        }
        return admit_extent(NPROC, current + 1);
    }

    // Admission is serialized with the mapping transaction. The live page
    // tables own the reservation; no second charge survives a failed mapping.
    [[nodiscard]] auto admit_address_space(uint64_t current, uint64_t removed, uint64_t added) -> bool {
        constexpr unsigned AS = static_cast<unsigned>(Id::AS);
        if (removed > current || added > UINT64_MAX - (current - removed)) {
            deny(AS, Denial::OVERFLOW);
            return false;
        }
        uint64_t const RESULT = current - removed + added;
        if (RESULT > current && RESULT > entries[AS].limit.soft) {
            deny(AS, Denial::SOFT_LIMIT);
            return false;
        }
        return true;
    }

    [[nodiscard]] auto reserve(unsigned id, uint64_t amount) -> bool {
        auto& entry = entries[id];
        if (amount > UINT64_MAX - entry.current) {
            deny(id, Denial::OVERFLOW);
            return false;
        }
        if (amount != 0 && (entry.current > entry.limit.soft || amount > entry.limit.soft - entry.current)) {
            deny(id, Denial::SOFT_LIMIT);
            return false;
        }
        entry.current += amount;
        if (entry.current > entry.peak) {
            entry.peak = entry.current;
        }
        return true;
    }

    // A false result is an accounting bug; never wrap or silently erase it.
    [[nodiscard]] auto release(unsigned id, uint64_t amount) -> bool {
        if (amount > entries[id].current) {
            return false;
        }
        entries[id].current -= amount;
        return true;
    }

    [[nodiscard]] auto charge_cpu(uint64_t elapsed_us, uint64_t user_us = 0) -> CpuAction {
        if (elapsed_us == 0) {
            return CpuAction::NONE;
        }
        // Scheduler classification supplies a user portion of this slice.
        // Bound it defensively so a malformed internal caller cannot underflow.
        uint64_t const USER_US = user_us < elapsed_us ? user_us : elapsed_us;
        cpu_user_time_us = saturating_cpu_add(cpu_user_time_us, USER_US);
        cpu_system_time_us = saturating_cpu_add(cpu_system_time_us, elapsed_us - USER_US);
        constexpr unsigned CPU = static_cast<unsigned>(Id::CPU);
        bool const OVERFLOW = elapsed_us > UINT64_MAX - cpu_time_us;
        cpu_time_us = OVERFLOW ? UINT64_MAX : cpu_time_us + elapsed_us;
        uint64_t const SECONDS = cpu_time_us / CPU_US_PER_SECOND;
        commit_extent(CPU, SECONDS);
        auto const LIMIT = entries[CPU].limit;
        if (OVERFLOW || (LIMIT.hard != INFINITY_VALUE && SECONDS >= LIMIT.hard)) {
            if (!cpu_hard_notified) {
                deny(CPU, OVERFLOW ? Denial::OVERFLOW : Denial::HARD_LIMIT);
                cpu_hard_notified = true;
            }
            // Every consuming sibling must observe an uncatchable hard stop.
            return CpuAction::HARD_KILL;
        }
        if (LIMIT.soft != INFINITY_VALUE && SECONDS >= LIMIT.soft &&
            (cpu_last_soft_second == UINT64_MAX || SECONDS > cpu_last_soft_second)) {
            cpu_last_soft_second = SECONDS;
            deny(CPU, Denial::SOFT_LIMIT);
            return CpuAction::SOFT_SIGNAL;
        }
        return CpuAction::NONE;
    }

    void inherit_limits(const State& parent) {
        cpu_time_us = 0;
        cpu_user_time_us = 0;
        cpu_system_time_us = 0;
        cpu_last_soft_second = UINT64_MAX;
        cpu_hard_notified = false;
        for (unsigned id = 0; id < entries.size(); ++id) {
            entries[id] = Usage{.limit = parent.entries[id].limit};
        }
    }
};
}  // namespace ker::mod::sched::resource
