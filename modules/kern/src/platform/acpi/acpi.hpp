#pragma once

#include <extern/limine.h>

#include <defines/defines.hpp>
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
}  // namespace ker::mod::acpi
