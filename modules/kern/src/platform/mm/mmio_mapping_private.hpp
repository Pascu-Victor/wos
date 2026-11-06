#pragma once

#include <platform/mm/mmio_mapping.hpp>
#include <platform/mm/paging.hpp>

namespace ker::dev::pci::detail {
class ColdFunctionClaim;
}

namespace ker::mod::mm::mmio::detail {

// Retained only in the actual permanent PCI record until full MEM activation
// and normal-borrow publication. A timeout resumes this same affine handle.
class PreparedMapping {
    uint64_t id_{};
    uint16_t slot_{UINT16_MAX};
    friend class Runtime;

   public:
    PreparedMapping() = default;
    ~PreparedMapping();
    PreparedMapping(const PreparedMapping&) = delete;
    auto operator=(const PreparedMapping&) -> PreparedMapping& = delete;
    PreparedMapping(PreparedMapping&& other) noexcept;
    auto operator=(PreparedMapping&& other) noexcept -> PreparedMapping&;
    [[nodiscard]] auto empty() const noexcept -> bool { return id_ == 0; }
};

[[nodiscard]] auto prepare_from_pci(ker::dev::pci::detail::ColdFunctionClaim&&, const cpu_maintenance::detail::Controller&,
                                    PreparedMapping& empty_output) -> Status;
[[nodiscard]] auto resume_prepared(PreparedMapping&, const cpu_maintenance::detail::Controller&) -> Status;
[[nodiscard]] auto publish_from_pci(PreparedMapping&, const cpu_maintenance::detail::Controller&, Mapping& empty_output) -> Status;

// begin is called inside actual init_pagemap before its first publication.
// finish is called after phase3 PCI inventory, outside ConfigGuard and before
// AHCI/AP/Task publication. Native inputs establish context, never caller flags.
void begin_boot_history() noexcept;
void finish_boot_arena() noexcept;

// The generic mutator preflight owns no lock on return. Locked predicates are
// called only inside the existing native user_mapping_pin_lock transaction.
// They grant no typed write exemption and never allocate or perform CPU waits.
[[nodiscard]] auto generic_virtual_allowed(const paging::PageTable*, uint64_t virtual_start, uint64_t bytes) noexcept -> bool;
[[nodiscard]] auto record_generic_mapping(const paging::PageTable*, uint64_t virtual_start, uint64_t physical_start, uint64_t bytes,
                                          uint64_t flags) noexcept -> bool;
[[nodiscard]] auto record_boot_physical_range(uint64_t physical_start, uint64_t bytes) noexcept -> bool;
// Conservative preflight before an off-lock backing-page read/ref acquisition.
// Captures exposure before releasing pin; never manufactures page ownership.
[[nodiscard]] auto record_generic_physical_access(uint64_t physical_start, uint64_t bytes) noexcept -> bool;
[[nodiscard]] auto generic_table_allowed(const paging::PageTable*) noexcept -> bool;
[[nodiscard]] auto generic_leaf_allowed_locked(const paging::PageTable* destination_table, uint64_t virtual_address, uint64_t old_raw,
                                               uint64_t new_raw, unsigned leaf_level) noexcept -> bool;
[[nodiscard]] auto generic_root_allowed_locked(const paging::PageTable*) noexcept -> bool;

}  // namespace ker::mod::mm::mmio::detail
