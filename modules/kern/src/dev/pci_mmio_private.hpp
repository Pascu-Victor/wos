#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <platform/mm/mmio_mapping.hpp>
#include <utility>

namespace ker::mod::mm::mmio::detail {
class Runtime;
}

namespace ker::dev::pci::detail {
class ColdMmioAccess;

struct Span {
    uint64_t start{};
    uint64_t bytes{};
};

// Exact probed BAR metadata accompanies each page-rounded exclusion span.
// High words of 64-bit BARs never receive a separate entry.
struct MemoryBar {
    uint8_t index{};
    uint8_t attributes{};
    uint64_t physical{};
    uint64_t bytes{};
};

struct PreparedView {
    uint8_t bar{};
    uint64_t offset{};
    uint64_t physical{};
    uint64_t bytes{};
};

// Only the actual claim/config transaction can issue this capability. Its
// immutable identity names kernel-lifetime storage; no driver/Task/controller
// pointer or caller-supplied physical range can adopt that storage.
class ColdFunctionClaim {
    friend class ColdMmioAccess;
    friend class ker::mod::mm::mmio::detail::Runtime;
    uint16_t function_index{UINT16_MAX};
    uint64_t owner_id{};
    uint64_t claim_epoch{};
    uint64_t inventory_generation{};
    uint64_t bar0_physical{};
    uint64_t bar0_bytes{};
    size_t span_count{};
    std::array<Span, 6> spans{};
    std::array<MemoryBar, 6> bars{};
    size_t view_count{};
    std::array<PreparedView, ker::mod::mm::mmio::VIEW_CAPACITY> views{};

    ColdFunctionClaim(uint16_t index, uint64_t id, uint64_t epoch, uint64_t inventory, uint64_t physical, uint64_t bytes, size_t count,
                      const std::array<Span, 6>& memory, const std::array<MemoryBar, 6>& geometry, size_t mappings,
                      const std::array<PreparedView, ker::mod::mm::mmio::VIEW_CAPACITY>& plan) noexcept
        : function_index(index),
          owner_id(id),
          claim_epoch(epoch),
          inventory_generation(inventory),
          bar0_physical(physical),
          bar0_bytes(bytes),
          span_count(count),
          spans(memory),
          bars(geometry),
          view_count(mappings),
          views(plan) {}

   public:
    ColdFunctionClaim() = default;
    ColdFunctionClaim(const ColdFunctionClaim&) = delete;
    auto operator=(const ColdFunctionClaim&) -> ColdFunctionClaim& = delete;
    ColdFunctionClaim(ColdFunctionClaim&& other) noexcept
        : function_index(std::exchange(other.function_index, UINT16_MAX)),
          owner_id(std::exchange(other.owner_id, 0)),
          claim_epoch(std::exchange(other.claim_epoch, 0)),
          inventory_generation(std::exchange(other.inventory_generation, 0)),
          bar0_physical(std::exchange(other.bar0_physical, 0)),
          bar0_bytes(std::exchange(other.bar0_bytes, 0)),
          span_count(std::exchange(other.span_count, 0)),
          spans(other.spans),
          bars(other.bars),
          view_count(std::exchange(other.view_count, 0)),
          views(other.views) {}
    auto operator=(ColdFunctionClaim&& other) noexcept -> ColdFunctionClaim& {
        if (this != &other) {
            function_index = std::exchange(other.function_index, UINT16_MAX);
            owner_id = std::exchange(other.owner_id, 0);
            claim_epoch = std::exchange(other.claim_epoch, 0);
            inventory_generation = std::exchange(other.inventory_generation, 0);
            bar0_physical = std::exchange(other.bar0_physical, 0);
            bar0_bytes = std::exchange(other.bar0_bytes, 0);
            span_count = std::exchange(other.span_count, 0);
            spans = other.spans;
            bars = other.bars;
            view_count = std::exchange(other.view_count, 0);
            views = other.views;
        }
        return *this;
    }
};

// Fixed terminal adapter, called by actual MM outside its PTE locks after the
// authentic driver's hardware-retirement witness. No generic callback hook.
[[nodiscard]] auto disable_cold_decode(const ColdFunctionClaim&) -> ker::mod::mm::mmio::Status;
[[nodiscard]] auto validate_cold_activation(const ColdFunctionClaim&) -> ker::mod::mm::mmio::Status;
[[nodiscard]] auto cold_configuration_sealed() noexcept -> bool;
[[nodiscard]] auto cold_inventory_allows_arena(uint64_t virtual_start, uint64_t bytes) noexcept -> bool;
}  // namespace ker::dev::pci::detail
