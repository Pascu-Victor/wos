#pragma once

#include <limine.h>
#include <defines/defines.hpp>
#include <platform/acpi/madt/madt.hpp>
#include <platform/acpi/tables/rsdp.hpp>
#include <platform/acpi/tables/rdst.hpp>
#include <platform/mm/addr.hpp>

namespace ker::mod::acpi {
    struct ACPIResult {
        bool success;
        void* data;
    };

    void init();

    ACPIResult parseAcpiTables(const char* ident);
}