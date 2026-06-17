#include "power.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <mod/io/port/port.hpp>
#include <platform/acpi/acpi.hpp>
#include <platform/acpi/tables/sdt.hpp>
#include <platform/mm/addr.hpp>

namespace ker::mod::acpi::power {
namespace {

constexpr uint8_t AML_NAME_OP = 0x08;
constexpr uint8_t AML_PACKAGE_OP = 0x12;
constexpr uint8_t AML_ZERO_OP = 0x00;
constexpr uint8_t AML_ONE_OP = 0x01;
constexpr uint8_t AML_BYTE_PREFIX = 0x0a;
constexpr uint8_t AML_WORD_PREFIX = 0x0b;
constexpr uint8_t AML_DWORD_PREFIX = 0x0c;

constexpr uint8_t GAS_SYSTEM_MEMORY = 0;
constexpr uint8_t GAS_SYSTEM_IO = 1;
constexpr uint32_t FADT_RESET_REG_SUP = 1U << 10;
constexpr uint16_t ACPI_PM1_SLP_EN = 1U << 13;
constexpr uint16_t ACPI_PM1_SLP_TYP_SHIFT = 10;

struct GenericAddress {
    uint8_t address_space_id;
    uint8_t register_bit_width;
    uint8_t register_bit_offset;
    uint8_t access_size;
    uint64_t address;
} __attribute__((packed));

struct Fadt {
    Sdt header;
    uint32_t firmware_ctrl;
    uint32_t dsdt;
    uint8_t reserved0;
    uint8_t preferred_pm_profile;
    uint16_t sci_int;
    uint32_t smi_cmd;
    uint8_t acpi_enable;
    uint8_t acpi_disable;
    uint8_t s4bios_req;
    uint8_t pstate_cnt;
    uint32_t pm1a_event_block;
    uint32_t pm1b_event_block;
    uint32_t pm1a_control_block;
    uint32_t pm1b_control_block;
    uint32_t pm2_control_block;
    uint32_t pm_timer_block;
    uint32_t gpe0_block;
    uint32_t gpe1_block;
    uint8_t pm1_event_length;
    uint8_t pm1_control_length;
    uint8_t pm2_control_length;
    uint8_t pm_timer_length;
    uint8_t gpe0_block_length;
    uint8_t gpe1_block_length;
    uint8_t gpe1_base;
    uint8_t cstate_control;
    uint16_t worst_c2_latency;
    uint16_t worst_c3_latency;
    uint16_t flush_size;
    uint16_t flush_stride;
    uint8_t duty_offset;
    uint8_t duty_width;
    uint8_t day_alarm;
    uint8_t month_alarm;
    uint8_t century;
    uint16_t boot_arch_flags;
    uint8_t reserved1;
    uint32_t flags;
    GenericAddress reset_reg;
    uint8_t reset_value;
    uint16_t arm_boot_arch;
    uint8_t fadt_minor_version;
    uint64_t x_firmware_ctrl;
    uint64_t x_dsdt;
    GenericAddress x_pm1a_event_block;
    GenericAddress x_pm1b_event_block;
    GenericAddress x_pm1a_control_block;
    GenericAddress x_pm1b_control_block;
} __attribute__((packed));

static_assert(offsetof(Fadt, dsdt) == 40);
static_assert(offsetof(Fadt, pm1a_control_block) == 64);
static_assert(offsetof(Fadt, flags) == 112);
static_assert(offsetof(Fadt, reset_reg) == 116);
static_assert(offsetof(Fadt, x_dsdt) == 140);
static_assert(offsetof(Fadt, x_pm1a_control_block) == 172);

auto table_has_length(const Sdt* table, size_t length) -> bool { return table != nullptr && table->length >= length; }

auto parse_pkg_len(const uint8_t* aml, uint32_t length, uint32_t& offset, uint32_t& pkg_len) -> bool {
    if (offset >= length) {
        return false;
    }
    uint8_t const LEAD = aml[offset++];
    uint8_t const BYTE_COUNT = LEAD >> 6;
    pkg_len = LEAD & (BYTE_COUNT == 0 ? 0x3fU : 0x0fU);
    for (uint8_t i = 0; i < BYTE_COUNT; ++i) {
        if (offset >= length) {
            return false;
        }
        pkg_len |= static_cast<uint32_t>(aml[offset++]) << (4U + (8U * i));
    }
    return true;
}

auto parse_small_aml_integer(const uint8_t* aml, uint32_t length, uint32_t& offset, uint64_t& value) -> bool {
    if (offset >= length) {
        return false;
    }

    uint8_t const OP = aml[offset++];
    switch (OP) {
        case AML_ZERO_OP:
            value = 0;
            return true;
        case AML_ONE_OP:
            value = 1;
            return true;
        case AML_BYTE_PREFIX:
            if (offset + 1 > length) {
                return false;
            }
            value = aml[offset++];
            return true;
        case AML_WORD_PREFIX:
            if (offset + 2 > length) {
                return false;
            }
            value = static_cast<uint64_t>(aml[offset]) | (static_cast<uint64_t>(aml[offset + 1]) << 8);
            offset += 2;
            return true;
        case AML_DWORD_PREFIX:
            if (offset + 4 > length) {
                return false;
            }
            value = static_cast<uint64_t>(aml[offset]) | (static_cast<uint64_t>(aml[offset + 1]) << 8) |
                    (static_cast<uint64_t>(aml[offset + 2]) << 16) | (static_cast<uint64_t>(aml[offset + 3]) << 24);
            offset += 4;
            return true;
        default:
            return false;
    }
}

auto gas_is_valid(const GenericAddress& gas) -> bool {
    return gas.address != 0 && gas.register_bit_offset == 0 &&
           (gas.address_space_id == GAS_SYSTEM_IO || gas.address_space_id == GAS_SYSTEM_MEMORY);
}

auto gas_width_bits(const GenericAddress& gas) -> uint8_t {
    if (gas.access_size == 1) {
        return 8;
    }
    if (gas.access_size == 2) {
        return 16;
    }
    if (gas.access_size == 3) {
        return 32;
    }
    if (gas.access_size == 4) {
        return 64;
    }
    return gas.register_bit_width;
}

auto gas_read(const GenericAddress& gas, uint64_t& out) -> bool {
    if (!gas_is_valid(gas)) {
        return false;
    }
    uint8_t const WIDTH = gas_width_bits(gas);
    if (gas.address_space_id == GAS_SYSTEM_IO) {
        uint16_t const PORT = static_cast<uint16_t>(gas.address);
        if (WIDTH <= 8) {
            out = inb(PORT);
            return true;
        }
        if (WIDTH <= 16) {
            out = inw(PORT);
            return true;
        }
        if (WIDTH <= 32) {
            out = inl(PORT);
            return true;
        }
        return false;
    }

    auto* ptr = ker::mod::mm::addr::get_virt_pointer(gas.address);
    if (WIDTH <= 8) {
        out = *reinterpret_cast<volatile uint8_t*>(ptr);
        return true;
    }
    if (WIDTH <= 16) {
        out = *reinterpret_cast<volatile uint16_t*>(ptr);
        return true;
    }
    if (WIDTH <= 32) {
        out = *reinterpret_cast<volatile uint32_t*>(ptr);
        return true;
    }
    if (WIDTH <= 64) {
        out = *reinterpret_cast<volatile uint64_t*>(ptr);
        return true;
    }
    return false;
}

auto gas_write(const GenericAddress& gas, uint64_t value) -> bool {
    if (!gas_is_valid(gas)) {
        return false;
    }
    uint8_t const WIDTH = gas_width_bits(gas);
    if (gas.address_space_id == GAS_SYSTEM_IO) {
        uint16_t const PORT = static_cast<uint16_t>(gas.address);
        if (WIDTH <= 8) {
            outb(PORT, static_cast<uint8_t>(value));
            return true;
        }
        if (WIDTH <= 16) {
            outw(PORT, static_cast<uint16_t>(value));
            return true;
        }
        if (WIDTH <= 32) {
            outl(PORT, static_cast<uint32_t>(value));
            return true;
        }
        return false;
    }

    auto* ptr = ker::mod::mm::addr::get_virt_pointer(gas.address);
    if (WIDTH <= 8) {
        *reinterpret_cast<volatile uint8_t*>(ptr) = static_cast<uint8_t>(value);
        return true;
    }
    if (WIDTH <= 16) {
        *reinterpret_cast<volatile uint16_t*>(ptr) = static_cast<uint16_t>(value);
        return true;
    }
    if (WIDTH <= 32) {
        *reinterpret_cast<volatile uint32_t*>(ptr) = static_cast<uint32_t>(value);
        return true;
    }
    if (WIDTH <= 64) {
        *reinterpret_cast<volatile uint64_t*>(ptr) = value;
        return true;
    }
    return false;
}

auto legacy_pm1_control_gas(uint32_t address, uint8_t length) -> GenericAddress {
    GenericAddress gas{};
    gas.address_space_id = GAS_SYSTEM_IO;
    gas.register_bit_width = length >= 2 ? 16 : 8;
    gas.access_size = length >= 2 ? 2 : 1;
    gas.address = address;
    return gas;
}

auto pm1a_control_gas(const Fadt& fadt) -> GenericAddress {
    if (table_has_length(&fadt.header, offsetof(Fadt, x_pm1b_control_block)) && fadt.x_pm1a_control_block.address != 0) {
        return fadt.x_pm1a_control_block;
    }
    return legacy_pm1_control_gas(fadt.pm1a_control_block, fadt.pm1_control_length);
}

auto pm1b_control_gas(const Fadt& fadt) -> GenericAddress {
    if (table_has_length(&fadt.header, offsetof(Fadt, x_pm1b_control_block) + sizeof(GenericAddress)) &&
        fadt.x_pm1b_control_block.address != 0) {
        return fadt.x_pm1b_control_block;
    }
    return legacy_pm1_control_gas(fadt.pm1b_control_block, fadt.pm1_control_length);
}

auto dsdt_from_fadt(const Fadt& fadt) -> const Sdt* {
    uint64_t phys = 0;
    if (table_has_length(&fadt.header, offsetof(Fadt, x_dsdt) + sizeof(uint64_t)) && fadt.x_dsdt != 0) {
        phys = fadt.x_dsdt;
    } else {
        phys = fadt.dsdt;
    }
    return phys != 0 ? reinterpret_cast<const Sdt*>(ker::mod::mm::addr::get_virt_pointer(phys)) : nullptr;
}

auto get_fadt() -> const Fadt* {
    ACPIResult const RESULT = parse_acpi_tables("FACP");
    if (!RESULT.success || !table_has_length(static_cast<const Sdt*>(RESULT.data), offsetof(Fadt, reset_reg))) {
        return nullptr;
    }
    return static_cast<const Fadt*>(RESULT.data);
}

}  // namespace

auto find_s5_sleep_type(const uint8_t* aml, uint32_t length, SleepType& out) -> bool {
    if (aml == nullptr || length < 8) {
        return false;
    }

    for (uint32_t i = 1; i + 7 < length; ++i) {
        if (aml[i - 1] != AML_NAME_OP || std::memcmp(aml + i, "_S5_", 4) != 0) {
            continue;
        }
        uint32_t offset = i + 4;
        if (offset >= length || aml[offset++] != AML_PACKAGE_OP) {
            continue;
        }
        uint32_t pkg_len = 0;
        if (!parse_pkg_len(aml, length, offset, pkg_len) || offset >= length) {
            continue;
        }
        (void)pkg_len;

        uint8_t const ELEMENT_COUNT = aml[offset++];
        if (ELEMENT_COUNT < 2) {
            continue;
        }

        uint64_t pm1a = 0;
        uint64_t pm1b = 0;
        if (!parse_small_aml_integer(aml, length, offset, pm1a) || !parse_small_aml_integer(aml, length, offset, pm1b)) {
            continue;
        }
        if (pm1a > 7 || pm1b > 7) {
            continue;
        }
        out.pm1a = static_cast<uint8_t>(pm1a);
        out.pm1b = static_cast<uint8_t>(pm1b);
        return true;
    }
    return false;
}

auto reboot_via_fadt() -> bool {
    const Fadt* fadt = get_fadt();
    if (fadt == nullptr || !table_has_length(&fadt->header, offsetof(Fadt, reset_value) + sizeof(uint8_t)) ||
        (fadt->flags & FADT_RESET_REG_SUP) == 0) {
        return false;
    }
    return gas_write(fadt->reset_reg, fadt->reset_value);
}

auto poweroff_via_s5() -> bool {
    const Fadt* fadt = get_fadt();
    if (fadt == nullptr) {
        return false;
    }
    const Sdt* dsdt = dsdt_from_fadt(*fadt);
    if (!table_has_length(dsdt, sizeof(Sdt))) {
        return false;
    }
    SleepType s5{};
    auto const* AML = reinterpret_cast<const uint8_t*>(dsdt) + sizeof(Sdt);
    uint32_t const AML_LEN = dsdt->length - sizeof(Sdt);
    if (!find_s5_sleep_type(AML, AML_LEN, s5)) {
        return false;
    }

    GenericAddress pm1a = pm1a_control_gas(*fadt);
    GenericAddress pm1b = pm1b_control_gas(*fadt);
    uint64_t current = 0;
    bool wrote = false;
    if (gas_read(pm1a, current)) {
        uint16_t const VALUE = static_cast<uint16_t>((current & ~(0x7U << ACPI_PM1_SLP_TYP_SHIFT)) |
                                                     (static_cast<uint16_t>(s5.pm1a) << ACPI_PM1_SLP_TYP_SHIFT) | ACPI_PM1_SLP_EN);
        wrote = gas_write(pm1a, VALUE) || wrote;
    }
    if (gas_read(pm1b, current)) {
        uint16_t const VALUE = static_cast<uint16_t>((current & ~(0x7U << ACPI_PM1_SLP_TYP_SHIFT)) |
                                                     (static_cast<uint16_t>(s5.pm1b) << ACPI_PM1_SLP_TYP_SHIFT) | ACPI_PM1_SLP_EN);
        wrote = gas_write(pm1b, VALUE) || wrote;
    }
    return wrote;
}

}  // namespace ker::mod::acpi::power
