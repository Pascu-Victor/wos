#pragma once

#include <cstddef>
#include <defines/defines.hpp>

namespace ker::mod::acpi {
struct Sdt {
    char signature[4];  // NOLINT(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays): ACPI firmware table bytes.
    uint32_t length;
    uint8_t revision;
    uint8_t checksum;
    char oem_id[6];        // NOLINT(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays): ACPI firmware table bytes.
    char oem_table_id[8];  // NOLINT(modernize-avoid-c-arrays,cppcoreguidelines-avoid-c-arrays): ACPI firmware table bytes.
    uint32_t oem_revision;
    uint32_t creator_id;
    uint32_t creator_revision;
} __attribute__((packed));

static_assert(offsetof(Sdt, length) == 4);
static_assert(offsetof(Sdt, oem_id) == 10);
static_assert(offsetof(Sdt, oem_table_id) == 16);
static_assert(offsetof(Sdt, creator_revision) == 32);
static_assert(sizeof(Sdt) == 36);
}  // namespace ker::mod::acpi
