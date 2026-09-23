#pragma once

#include <extern/limine.h>

#include <defines/defines.hpp>
#include <platform/acpi/bounded_tables.hpp>
#include <platform/acpi/madt/madt.hpp>
#include <platform/acpi/tables/rdst.hpp>
#include <platform/acpi/tables/rsdp.hpp>
#include <platform/mm/addr.hpp>

namespace ker::mod::acpi {
struct ACPIResult {
    bool success;
    void* data;
};

void init();

ACPIResult parse_acpi_tables(const char* ident);

// Boot-only bounded alternative for security-sensitive firmware consumers.
// Returned bytes are borrowed firmware RAM; copy before leaving boot discovery.
[[nodiscard]] auto find_table_bounded(std::string_view ident) -> bounded::Result;
}  // namespace ker::mod::acpi
