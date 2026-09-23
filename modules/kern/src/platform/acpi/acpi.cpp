#include "acpi.hpp"

#include <cstddef>
#include <cstdint>
#include <platform/acpi/bounded_tables.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/paging.hpp>
#include <platform/mm/virt.hpp>

#include "extern/limine.h"
#include "platform/acpi/tables/rdst.hpp"
#include "platform/acpi/tables/rsdp.hpp"
#include "platform/acpi/tables/sdt.hpp"
#include "platform/mm/addr.hpp"

namespace {

__attribute__((used, section(".requests"))) volatile limine_rsdp_request rsdp_request = {
    // NOLINT
    .id = LIMINE_RSDP_REQUEST_ID,
    .revision = 0,
    .response = nullptr,
};

}  // namespace

namespace ker::mod::acpi {

namespace {

auto validate_checksum(const Sdt* sdt) -> bool {
    uint8_t sum = 0;
    for (size_t i = 0; i < sdt->length; i++) {
        sum += reinterpret_cast<const uint8_t*>(sdt)[i];
    }
    return sum == 0;
}

auto signature_matches(const Sdt& sdt, const char* ident) -> bool {
    for (size_t i = 0; i < 4; ++i) {
        if (sdt.signature[i] != ident[i]) {
            return false;
        }
    }
    return true;
}

}  // namespace

void init() { ker::mod::acpi::rsdp::init(reinterpret_cast<uint64_t>(rsdp_request.response->address)); }

auto find_table_bounded(std::string_view ident) -> bounded::Result {
    auto const* memory = mm::boot_memory_map();
    if (memory == nullptr || memory->entries == nullptr || memory->entry_count > 4096 || rsdp_request.response == nullptr) {
        return {.error = bounded::Error::inaccessible};
    }
    uint64_t const HHDM = mm::addr::get_hhdm_offset();
    auto reader = [memory, HHDM](uint64_t physical, size_t size) -> std::span<const uint8_t> {
        if (size == 0 || size > bounded::MAX_TABLE_BYTES || physical > UINT64_MAX - HHDM || size - 1 > UINT64_MAX - HHDM - physical) {
            return {};
        }
        bool covered = false;
        for (size_t i = 0; i < memory->entry_count; ++i) {
            auto const* entry = memory->entries[i];
            if (entry == nullptr || entry->length > UINT64_MAX - entry->base) {
                return {};
            }
            bool const FIRMWARE = entry->type == LIMINE_MEMMAP_ACPI_RECLAIMABLE || entry->type == LIMINE_MEMMAP_ACPI_NVS ||
                                  entry->type == LIMINE_MEMMAP_BOOTLOADER_RECLAIMABLE;
            bool const OVERLAP = entry->length != 0 && physical < entry->base + entry->length && entry->base <= physical + (size - 1);
            if (OVERLAP && !FIRMWARE) {
                return {};
            }
            covered |= FIRMWARE && bounded::contains(entry->base, entry->length, physical, size);
        }
        if (!covered) {
            return {};
        }
        constexpr uint64_t PAGE_MASK = mm::paging::PAGE_SIZE - 1;
        uint64_t const LAST = (physical + size - 1) & ~PAGE_MASK;
        for (uint64_t page = physical & ~PAGE_MASK;; page += mm::paging::PAGE_SIZE) {
            if (mm::virt::translate(mm::virt::get_kernel_pagemap(), page + HHDM) != page) {
                return {};
            }
            if (page == LAST) {
                break;
            }
        }
        return {reinterpret_cast<const uint8_t*>(physical + HHDM), size};
    };
    auto const RSDP = reinterpret_cast<uint64_t>(rsdp_request.response->address);
    return bounded::find(reader, RSDP >= HHDM ? RSDP - HHDM : RSDP, ident);
}

ACPIResult parse_acpi_tables(const char* ident) {
    rsdp::Rsdp const RSDP = rsdp::get();

    Rsdt const* rsdt = nullptr;
    Xsdt const* xsdt = nullptr;
    Sdt header{};
    bool const USE_XSDT = rsdp::use_xsdt();

    if (USE_XSDT) {
        xsdt = reinterpret_cast<Xsdt*>(mm::addr::get_virt_pointer(RSDP.xsdt_addr));
        header = xsdt->header;
    } else {
        rsdt = reinterpret_cast<Rsdt*>(mm::addr::get_virt_pointer(static_cast<uint64_t>(RSDP.rsdt_addr)));
        header = rsdt->header;
    }

    size_t const ENTRIES = (header.length - sizeof(Sdt)) / (USE_XSDT ? sizeof(uint64_t) : sizeof(uint32_t));
    for (size_t i = 0; i < ENTRIES; i++) {
        auto const TABLE_PHYS = USE_XSDT ? xsdt->next[i] : static_cast<uint64_t>(rsdt->next[i]);
        auto* sdt = reinterpret_cast<Sdt*>(mm::addr::get_virt_pointer(TABLE_PHYS));
        if (signature_matches(*sdt, ident) && validate_checksum(sdt)) {
            ACPIResult result{};
            result.success = true;
            result.data = sdt;
            return result;
        }
    }

    ACPIResult result{};
    result.success = false;
    result.data = nullptr;
    return result;
}

}  // namespace ker::mod::acpi
