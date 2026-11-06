#pragma once

#include <extern/limine.h>

#include <platform/mm/paging.hpp>

namespace ker::mod::mm::mmio::detail::platform {

// Native definitions share virt's actual leaf-pin lock and allocator. Host
// fixtures replace finite memory/instruction/allocator ports, not a mapping or
// physical-owner algorithm. No caller supplies an authorization callback.
[[nodiscard]] auto lock_pin() noexcept -> uint64_t;
void unlock_pin(uint64_t flags) noexcept;
[[nodiscard]] auto boot_root_building() noexcept -> bool;
[[nodiscard]] auto pre_task_boot_context() noexcept -> bool;
[[nodiscard]] auto command_line() noexcept -> const char*;
[[nodiscard]] auto boot_memory_map() noexcept -> const limine_memmap_response*;
[[nodiscard]] auto kernel_root() noexcept -> paging::PageTable*;
[[nodiscard]] auto hhdm_offset() noexcept -> uint64_t;
[[nodiscard]] auto nx_ready() noexcept -> bool;
[[nodiscard]] auto arena_layout_safe(uint64_t start, uint64_t bytes) noexcept -> bool;
[[nodiscard]] auto table_physical(const paging::PageTable*, uint64_t& output) noexcept -> bool;
[[nodiscard]] auto table_virtual(uint64_t physical) noexcept -> paging::PageTable*;
[[nodiscard]] auto table_frame_valid(uint64_t physical) noexcept -> bool;
[[nodiscard]] auto allocate_zeroed_table() noexcept -> paging::PageTable*;
// Only an unpublished, never-registered fresh allocation may use this port.
void release_unpublished_table(paging::PageTable*) noexcept;
[[nodiscard]] auto ensure_table_addressable(paging::PageTable*) noexcept -> bool;
void reload_current_root() noexcept;
// Runtime has already checked the genuine borrow, generation and byte bounds.
// The native port is exactly one volatile device access; finite host ports
// translate the canonical device VA to their bounded emulated register bank.
[[nodiscard]] auto read_register32(uint64_t virtual_address) noexcept -> uint32_t;
void write_register32(uint64_t virtual_address, uint32_t value) noexcept;

}  // namespace ker::mod::mm::mmio::detail::platform
