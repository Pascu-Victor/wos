#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>

namespace ker::dev::pci::inventory_limits {

// One explicitly sized, nonmoving boot registry and its requester ledgers.
// Independent firmware-region, hardware-unit and DMA-storage limits stay separate.
constexpr size_t FUNCTION_CAPACITY = 256;
constexpr size_t BUS_CAPACITY = 256;
static_assert(FUNCTION_CAPACITY != 0 && FUNCTION_CAPACITY < UINT16_MAX);
static_assert(FUNCTION_CAPACITY <= static_cast<size_t>(std::numeric_limits<int>::max()));
static_assert(FUNCTION_CAPACITY <= std::numeric_limits<size_t>::max() / 2);
constexpr size_t REQUESTER_CAPACITY = 2 * FUNCTION_CAPACITY;
static_assert(REQUESTER_CAPACITY / 2 == FUNCTION_CAPACITY);
static_assert(BUS_CAPACITY == size_t{UINT8_MAX} + 1);

}  // namespace ker::dev::pci::inventory_limits
