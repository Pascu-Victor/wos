#pragma once

#include <cstddef>
#include <defines/defines.hpp>
#include <platform/acpi/tables/sdt.hpp>

namespace ker::mod::acpi {
struct Rsdt {
    Sdt header;
    uint32_t next[];  // NOLINT(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays): ACPI trailing table entries.
} __attribute__((packed));

struct Xsdt {
    Sdt header;
    uint64_t next[];  // NOLINT(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays): ACPI trailing table entries.
} __attribute__((packed));

static_assert(offsetof(Rsdt, next) == sizeof(Sdt));
static_assert(offsetof(Xsdt, next) == sizeof(Sdt));
}  // namespace ker::mod::acpi
