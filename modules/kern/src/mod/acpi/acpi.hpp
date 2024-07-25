#pragma once

#include <limine.h>
#include <defines/defines.hpp>
#include <mod/acpi/madt/madt.hpp>
#include <mod/acpi/tables/rsdp.hpp>
#include <mod/acpi/tables/rdst.hpp>
#include <mod/mm/addr.hpp>

namespace ker::mod::acpi {
    struct ACPIResult {
        bool success;
        void* data;
    };

    void init();

    ACPIResult parseAcpiTables(char* ident);
}