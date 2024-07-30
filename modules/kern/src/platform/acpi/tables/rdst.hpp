#pragma once

#include <defines/defines.hpp>
#include <platform/acpi/tables/sdt.hpp>

namespace ker::mod::acpi {
    struct Rsdt {
        Sdt header;
        uint32_t next[];
    } __attribute__((packed));

    struct Xsdt {
        Sdt header;
        uint64_t next[];
    } __attribute__((packed));
}