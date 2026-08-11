#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>

namespace ker::mod::mm::reclaim {

enum class ReclaimPriority : uint8_t {
    LOW = 0,
    NORMAL,
    CRITICAL,
};

enum class ReclaimUnit : uint8_t {
    PAGES = 0,
    BYTES,
    OBJECTS,
};

enum class PressureLevel : uint8_t {
    NONE = 0,
    LOW,
    CRITICAL,
    UNSATISFIABLE,
};

struct Watermarks {
    uint64_t critical_pages{};
    uint64_t low_pages{};
    uint64_t high_pages{};
};

[[nodiscard]] constexpr auto saturating_add(uint64_t lhs, uint64_t rhs) -> uint64_t {
    return rhs > std::numeric_limits<uint64_t>::max() - lhs ? std::numeric_limits<uint64_t>::max() : lhs + rhs;
}

[[nodiscard]] constexpr auto saturating_mul(uint64_t lhs, uint64_t rhs) -> uint64_t {
    return lhs != 0 && rhs > std::numeric_limits<uint64_t>::max() / lhs ? std::numeric_limits<uint64_t>::max() : lhs * rhs;
}

[[nodiscard]] constexpr auto order_pages(uint8_t order) -> uint64_t {
    return order >= 63 ? std::numeric_limits<uint64_t>::max() : uint64_t{1} << order;
}

// Start with the allocator's historical 256 MiB reserve, distributed by each
// zone's share of managed memory. Small systems cap the reserve at one quarter
// of managed pages so the high watermark remains attainable.
[[nodiscard]] constexpr auto zone_watermarks(uint64_t zone_pages, uint64_t managed_pages, uint8_t requested_order,
                                             uint64_t legacy_reserve_pages = 65536) -> Watermarks {
    if (zone_pages == 0 || managed_pages == 0) {
        return {};
    }

    uint64_t const ORDER_PAGES = order_pages(requested_order);
    uint64_t const ORDER_LOW_FLOOR = saturating_mul(ORDER_PAGES, 4);
    uint64_t const TOTAL_LOW = std::min(legacy_reserve_pages, std::max<uint64_t>(managed_pages / 16, ORDER_LOW_FLOOR));
    uint64_t proportional_low = saturating_mul(TOTAL_LOW, zone_pages) / managed_pages;
    proportional_low = std::max<uint64_t>(proportional_low, std::min(zone_pages, ORDER_LOW_FLOOR));
    proportional_low = std::min(proportional_low, zone_pages);

    uint64_t critical = std::max<uint64_t>(saturating_mul(ORDER_PAGES, 2), proportional_low / 2);
    critical = std::min(critical, proportional_low);

    uint64_t high_delta = std::max<uint64_t>(ORDER_LOW_FLOOR, proportional_low / 2);
    uint64_t high = std::min(zone_pages, saturating_add(proportional_low, high_delta));
    return {.critical_pages = critical, .low_pages = proportional_low, .high_pages = high};
}

[[nodiscard]] constexpr auto classify_zone_pressure(uint64_t free_pages, uint64_t zone_pages, int largest_free_order,
                                                    uint8_t requested_order, const Watermarks& watermarks) -> PressureLevel {
    uint64_t const ORDER_PAGES = order_pages(requested_order);
    if (zone_pages < ORDER_PAGES) {
        return PressureLevel::UNSATISFIABLE;
    }
    if (largest_free_order < 0 || static_cast<uint8_t>(largest_free_order) < requested_order || free_pages < ORDER_PAGES) {
        return PressureLevel::CRITICAL;
    }
    if (free_pages <= watermarks.critical_pages) {
        return PressureLevel::CRITICAL;
    }
    if (free_pages <= watermarks.low_pages) {
        return PressureLevel::LOW;
    }
    return PressureLevel::NONE;
}

[[nodiscard]] constexpr auto pressure_priority(PressureLevel level) -> ReclaimPriority {
    switch (level) {
        case PressureLevel::NONE:
        case PressureLevel::LOW:
            return ReclaimPriority::LOW;
        case PressureLevel::CRITICAL:
        case PressureLevel::UNSATISFIABLE:
            return ReclaimPriority::CRITICAL;
    }
    return ReclaimPriority::CRITICAL;
}

[[nodiscard]] constexpr auto callback_budget(ReclaimUnit unit, uint64_t target_pages, uint64_t max_batch_units, uint64_t page_size = 4096)
    -> uint64_t {
    if (max_batch_units == 0) {
        return 0;
    }
    uint64_t requested_units = target_pages;
    if (unit == ReclaimUnit::BYTES) {
        requested_units = saturating_mul(std::max<uint64_t>(target_pages, 1), page_size);
    } else if (unit == ReclaimUnit::OBJECTS) {
        requested_units = max_batch_units;
    }
    return std::min(std::max<uint64_t>(requested_units, 1), max_batch_units);
}

[[nodiscard]] constexpr auto backoff_rounds(uint32_t no_progress_streak) -> uint64_t {
    uint32_t const SHIFT = std::min<uint32_t>(no_progress_streak, 6);
    return uint64_t{1} << SHIFT;
}

}  // namespace ker::mod::mm::reclaim
