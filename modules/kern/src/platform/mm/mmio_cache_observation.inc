#pragma once

namespace ker::mod::mm::mmio_cache::detail {
namespace observation_impl {
inline constexpr uint32_t CONTROL_FAILURE = UINT32_MAX;
inline constexpr uint64_t ENABLE = uint64_t{1} << 11;
inline constexpr uint64_t VALID = uint64_t{1} << 11;
inline constexpr uint64_t FIXED_ENABLE = uint64_t{1} << 10;
inline constexpr uint64_t FIXED_CAP = uint64_t{1} << 8;
inline constexpr uint64_t AMD_TOP = uint64_t{1} << 20;
inline constexpr uint64_t AMD_TOP2 = uint64_t{1} << 21;
inline constexpr uint64_t CR0_RELEVANT = (uint64_t{1} << 31) | (uint64_t{1} << 30) | (uint64_t{1} << 29) | 1;
inline constexpr uint64_t CR4_RELEVANT = (uint64_t{1} << 5) | (uint64_t{1} << 7) | (uint64_t{1} << 12) | (uint64_t{1} << 17);
inline constexpr std::array<uint32_t, 11> FIXED_MSRS{0x250, 0x258, 0x259, 0x268, 0x269, 0x26a, 0x26b, 0x26c, 0x26d, 0x26e, 0x26f};

inline auto vendor(const Cpuid& basic) -> cache_arch::Vendor {
    if (basic.ebx == 0x756e6547 && basic.edx == 0x49656e69 && basic.ecx == 0x6c65746e) return cache_arch::Vendor::intel;
    if (basic.ebx == 0x68747541 && basic.edx == 0x69746e65 && basic.ecx == 0x444d4163) return cache_arch::Vendor::amd;
    return cache_arch::Vendor::unknown;
}

inline auto same(const Cpuid& left, const Cpuid& right) -> bool {
    return left.eax == right.eax && left.ebx == right.ebx && left.ecx == right.ecx && left.edx == right.edx;
}

inline auto same_os_features(Cpuid left, Cpuid right, uint32_t ignored_ecx) -> bool {
    // These CPUID bits mirror incidental OS control state; all architecture
    // capabilities, signatures, geometry and other raw fields remain exact.
    left.ecx &= ~ignored_ecx;
    right.ecx &= ~ignored_ecx;
    return same(left, right);
}

inline auto features_available(const RawCpu& raw) -> bool {
    constexpr uint32_t REQUIRED = (uint32_t{1} << 5) | (uint32_t{1} << 12) | (uint32_t{1} << 16);
    return raw.basic.eax >= 1 && raw.extended.eax >= 0x80000001U && (raw.features.edx & REQUIRED) == REQUIRED &&
           (raw.long_mode.edx & (uint32_t{1} << 29)) != 0;
}

inline auto header_status(const RawCpu& raw) -> Status {
    auto const VENDOR = raw.mtrr.vendor;
    if (VENDOR != cache_arch::Vendor::intel && VENDOR != cache_arch::Vendor::amd) return Status::unsupported_vendor;
    if (vendor(raw.basic) != VENDOR) return Status::unsupported_vendor;
    if (raw.mtrr.physical_bits < 32 || raw.mtrr.physical_bits > 52) return Status::invalid_geometry;
    uint64_t const CAP_ALLOWED = VENDOR == cache_arch::Vendor::intel ? 0x9dff : 0x5ff;
    if ((raw.mtrr.capability & ~CAP_ALLOWED) != 0) return Status::unsupported_capability;
    if ((raw.mtrr.default_type & ~uint64_t{0xc07}) != 0) return Status::invalid_reserved;
    if ((raw.mtrr.default_type & ENABLE) != 0 && (raw.mtrr.capability & 255U) > cache_arch::variable_layout_count(VENDOR))
        return Status::unsupported_layout;
    return Status::ready;
}

template <typename CpuIo>
auto read_scalar(CpuIo& io, uint32_t index, uint64_t& output, uint32_t& failed) -> Status {
    uint64_t value = 0;
    if (!io.read_msr(index, value)) {
        failed = index;
        return Status::read_fault;
    }
    output = value;
    return Status::ready;
}

template <typename CpuIo>
auto read_register(CpuIo& io, uint32_t index, Register& output, uint32_t& failed) -> Status {
    uint64_t value = 0;
    auto const STATUS = read_scalar(io, index, value, failed);
    if (STATUS != Status::ready) {
        output.observation = Observation::fault;
        return STATUS;
    }
    output = {value, Observation::value};
    return Status::ready;
}

template <typename CpuIo>
auto compare_scalar(CpuIo& io, uint32_t index, uint64_t expected, uint32_t& failed) -> Status {
    uint64_t actual = 0;
    auto const STATUS = read_scalar(io, index, actual, failed);
    if (STATUS != Status::ready) return STATUS;
    if (actual != expected) {
        failed = index;
        return Status::state_drift;
    }
    return Status::ready;
}

template <typename CpuIo>
auto compare_register(CpuIo& io, uint32_t index, const Register& expected, uint32_t& failed) -> Status {
    if (expected.observation == Observation::fault) {
        failed = index;
        return Status::read_fault;
    }
    return expected.observed() ? compare_scalar(io, index, expected.value, failed) : Status::missing_observation;
}
}  // namespace observation_impl

template <typename CpuIo>
auto revalidate_registers(CpuIo& io, const RawCpu& expected, uint32_t& failed_msr) -> Status {
    using namespace observation_impl;
    failed_msr = CONTROL_FAILURE;
    if (!io.local_idt_ready()) return Status::local_idt_unavailable;
    if (((io.read_cr0() ^ expected.cr0) & CR0_RELEVANT) != 0 || ((io.read_cr4() ^ expected.cr4) & CR4_RELEVANT) != 0)
        return Status::state_drift;
    if (!features_available(expected)) return Status::missing_features;
    auto status = header_status(expected);
    if (status != Status::ready) return status;
    if (!same(io.cpuid(0, 0), expected.basic) || !same_os_features(io.cpuid(1, 0), expected.features, uint32_t{1} << 27) ||
        !same(io.cpuid(0x80000000U, 0), expected.extended) || !same(io.cpuid(0x80000001U, 0), expected.long_mode))
        return Status::state_drift;
    if (expected.basic.eax >= 7 && !same_os_features(io.cpuid(7, 0), expected.structured, uint32_t{1} << 4)) return Status::state_drift;
    if (expected.extended.eax >= 0x80000008U && !same(io.cpuid(0x80000008U, 0), expected.address_width)) return Status::state_drift;
    bool const AMD = expected.mtrr.vendor == cache_arch::Vendor::amd;
    if (AMD && expected.extended.eax >= 0x8000001fU && !same(io.cpuid(0x8000001fU, 0), expected.encryption)) return Status::state_drift;

    status = compare_scalar(io, 0x277, expected.pat, failed_msr);
    if (status != Status::ready) return status;
    status = compare_scalar(io, 0xfe, expected.mtrr.capability, failed_msr);
    if (status != Status::ready) return status;
    status = compare_scalar(io, 0x2ff, expected.mtrr.default_type, failed_msr);
    if (status != Status::ready) return status;

    if (!AMD) {
        if ((expected.structured.ecx & (uint32_t{1} << 13)) != 0) {
            status = compare_register(io, 0x982, expected.tme_activate, failed_msr);
            if (status != Status::ready) return status;
        }
        if ((expected.mtrr.capability & (uint64_t{1} << 12)) != 0 || (expected.structured.ebx & (uint32_t{1} << 2)) != 0)
            return Status::protected_ranges_unobservable;
    }

    if ((expected.mtrr.default_type & ENABLE) != 0) {
        if ((expected.mtrr.default_type & FIXED_ENABLE) != 0 && (expected.mtrr.capability & FIXED_CAP) != 0) {
            if (!expected.mtrr.observed_fixed) return Status::missing_observation;
            for (size_t i = 0; i < FIXED_MSRS.size(); ++i) {
                status = compare_scalar(io, FIXED_MSRS[i], expected.mtrr.fixed[i], failed_msr);
                if (status != Status::ready) return status;
            }
        }
        size_t const COUNT = expected.mtrr.capability & 255U;
        for (size_t i = 0; i < COUNT; ++i) {
            uint16_t const BIT = uint16_t{1} << i;
            if ((expected.mtrr.observed_masks & BIT) == 0) return Status::missing_observation;
            uint32_t const BASE_INDEX = 0x200 + static_cast<uint32_t>(2 * i);
            status = compare_scalar(io, BASE_INDEX + 1, expected.mtrr.variable[i].mask, failed_msr);
            if (status != Status::ready) return status;
            if ((expected.mtrr.variable[i].mask & VALID) != 0) {
                if ((expected.mtrr.observed_bases & BIT) == 0) return Status::missing_observation;
                status = compare_scalar(io, BASE_INDEX, expected.mtrr.variable[i].base, failed_msr);
                if (status != Status::ready) return status;
            }
        }
    }

    if (AMD) {
        status = compare_register(io, 0xc0010010U, expected.amd.syscfg, failed_msr);
        if (status != Status::ready) return status;
        if ((expected.amd.syscfg.value & AMD_TOP) != 0) {
            status = compare_register(io, 0xc001001aU, expected.amd.top_mem, failed_msr);
            if (status != Status::ready) return status;
            for (size_t i = 0; i < expected.amd.iorr_mask.size(); ++i) {
                uint32_t const BASE_INDEX = 0xc0010016U + static_cast<uint32_t>(2 * i);
                status = compare_register(io, BASE_INDEX + 1, expected.amd.iorr_mask[i], failed_msr);
                if (status != Status::ready) return status;
                if ((expected.amd.iorr_mask[i].value & VALID) != 0) {
                    status = compare_register(io, BASE_INDEX, expected.amd.iorr_base[i], failed_msr);
                    if (status != Status::ready) return status;
                }
            }
        }
        if ((expected.amd.syscfg.value & AMD_TOP2) != 0) {
            status = compare_register(io, 0xc001001dU, expected.amd.top_mem2, failed_msr);
            if (status != Status::ready) return status;
        }
        if ((expected.encryption.eax & (uint32_t{1} << 1)) != 0) {
            status = compare_register(io, 0xc0010131U, expected.amd.sev_status, failed_msr);
            if (status != Status::ready) return status;
        }
        status = compare_register(io, 0xc0010113U, expected.amd_protected.smm_mask, failed_msr);
        if (status != Status::ready) return status;
        if ((expected.amd_protected.smm_mask.value & 2U) != 0) {
            status = compare_register(io, 0xc0010112U, expected.amd_protected.smm_base, failed_msr);
            if (status != Status::ready) return status;
        }
    } else {
        if ((expected.mtrr.capability & (uint64_t{1} << 11)) != 0) {
            status = compare_register(io, 0x1f3, expected.intel_protected.smrr_mask, failed_msr);
            if (status != Status::ready) return status;
            if ((expected.intel_protected.smrr_mask.value & VALID) != 0) {
                status = compare_register(io, 0x1f2, expected.intel_protected.smrr_base, failed_msr);
                if (status != Status::ready) return status;
            }
        }
        if ((expected.mtrr.capability & (uint64_t{1} << 15)) != 0) {
            status = compare_register(io, 0x1401, expected.intel_protected.seamrr_mask, failed_msr);
            if (status != Status::ready) return status;
            if ((expected.intel_protected.seamrr_mask.value & VALID) != 0) {
                status = compare_register(io, 0x1400, expected.intel_protected.seamrr_base, failed_msr);
                if (status != Status::ready) return status;
            }
        }
    }
    if (!io.local_idt_ready()) return Status::local_idt_unavailable;
    if (((io.read_cr0() ^ expected.cr0) & CR0_RELEVANT) != 0 || ((io.read_cr4() ^ expected.cr4) & CR4_RELEVANT) != 0) {
        failed_msr = CONTROL_FAILURE;
        return Status::state_drift;
    }
    return Status::ready;
}

template <typename CpuIo>
auto collect_registers(CpuIo& io, RawCpu& output) -> Status {
    using namespace observation_impl;
    output = {};
    output.failed_msr = CONTROL_FAILURE;
    if (!io.local_idt_ready()) return Status::local_idt_unavailable;
    output.basic = io.cpuid(0, 0);
    output.mtrr.vendor = vendor(output.basic);
    if (output.mtrr.vendor == cache_arch::Vendor::unknown) return Status::unsupported_vendor;
    if (output.basic.eax < 1) return Status::missing_features;
    output.features = io.cpuid(1, 0);
    output.extended = io.cpuid(0x80000000U, 0);
    if (output.extended.eax < 0x80000001U) return Status::missing_features;
    output.long_mode = io.cpuid(0x80000001U, 0);
    if (!features_available(output)) return Status::missing_features;
    if (output.basic.eax >= 7) output.structured = io.cpuid(7, 0);
    bool const AMD = output.mtrr.vendor == cache_arch::Vendor::amd;
    if (output.extended.eax >= 0x80000008U) {
        output.address_width = io.cpuid(0x80000008U, 0);
        output.mtrr.physical_bits = output.address_width.eax & 255U;
    } else {
        if (AMD) return Status::needs_address_geometry;
        output.mtrr.physical_bits = 36;
    }
    if (output.mtrr.physical_bits < 32 || output.mtrr.physical_bits > 52) return Status::invalid_geometry;
    if (AMD && output.extended.eax >= 0x8000001fU) {
        output.encryption = io.cpuid(0x8000001fU, 0);
        output.amd.encryption_eax = output.encryption.eax;
        output.amd.encryption_ebx = output.encryption.ebx;
    }
    output.amd_protected.signature = output.features.eax;
    output.cr0 = io.read_cr0();
    output.cr4 = io.read_cr4();
    auto status = validate_controls(output.cr0, output.cr4);
    if (status != Status::ready) return status;
    status = read_scalar(io, 0x277, output.pat, output.failed_msr);
    if (status != Status::ready) return status;
    status = validate_pat(output.pat);
    if (status != Status::ready) return status;
    status = read_scalar(io, 0xfe, output.mtrr.capability, output.failed_msr);
    if (status != Status::ready) return status;
    status = read_scalar(io, 0x2ff, output.mtrr.default_type, output.failed_msr);
    if (status != Status::ready) return status;
    status = header_status(output);
    if (status != Status::ready) return status;
    output.intel_protected.capability = output.mtrr.capability;
    if (!AMD) {
        if ((output.structured.ecx & (uint32_t{1} << 13)) != 0) {
            status = read_register(io, 0x982, output.tme_activate, output.failed_msr);
            if (status != Status::ready) return status;
            if ((output.tme_activate.value & 2U) != 0) return Status::needs_address_geometry;
        }
        if ((output.mtrr.capability & (uint64_t{1} << 12)) != 0 || (output.structured.ebx & (uint32_t{1} << 2)) != 0)
            return Status::protected_ranges_unobservable;
    }
    if ((output.mtrr.default_type & ENABLE) != 0) {
        if ((output.mtrr.default_type & FIXED_ENABLE) != 0 && (output.mtrr.capability & FIXED_CAP) != 0) {
            for (size_t i = 0; i < FIXED_MSRS.size(); ++i) {
                status = read_scalar(io, FIXED_MSRS[i], output.mtrr.fixed[i], output.failed_msr);
                if (status != Status::ready) return status;
            }
            output.mtrr.observed_fixed = true;
        }
        size_t const COUNT = output.mtrr.capability & 255U;
        for (size_t i = 0; i < COUNT; ++i) {
            uint32_t const BASE_INDEX = 0x200 + static_cast<uint32_t>(2 * i);
            status = read_scalar(io, BASE_INDEX + 1, output.mtrr.variable[i].mask, output.failed_msr);
            if (status != Status::ready) return status;
            output.mtrr.observed_masks |= uint16_t{1} << i;
            if ((output.mtrr.variable[i].mask & VALID) != 0) {
                status = read_scalar(io, BASE_INDEX, output.mtrr.variable[i].base, output.failed_msr);
                if (status != Status::ready) return status;
                output.mtrr.observed_bases |= uint16_t{1} << i;
            }
        }
    }
    if (AMD) {
        status = read_register(io, 0xc0010010U, output.amd.syscfg, output.failed_msr);
        if (status != Status::ready) return status;
        if ((output.amd.syscfg.value & AMD_TOP) != 0) {
            status = read_register(io, 0xc001001aU, output.amd.top_mem, output.failed_msr);
            if (status != Status::ready) return status;
            for (size_t i = 0; i < output.amd.iorr_mask.size(); ++i) {
                uint32_t const BASE_INDEX = 0xc0010016U + static_cast<uint32_t>(2 * i);
                status = read_register(io, BASE_INDEX + 1, output.amd.iorr_mask[i], output.failed_msr);
                if (status != Status::ready) return status;
                if ((output.amd.iorr_mask[i].value & VALID) != 0) {
                    status = read_register(io, BASE_INDEX, output.amd.iorr_base[i], output.failed_msr);
                    if (status != Status::ready) return status;
                }
            }
        }
        if ((output.amd.syscfg.value & AMD_TOP2) != 0) {
            status = read_register(io, 0xc001001dU, output.amd.top_mem2, output.failed_msr);
            if (status != Status::ready) return status;
        }
        if ((output.encryption.eax & (uint32_t{1} << 1)) != 0) {
            status = read_register(io, 0xc0010131U, output.amd.sev_status, output.failed_msr);
            if (status != Status::ready) return status;
        }
        status = read_register(io, 0xc0010113U, output.amd_protected.smm_mask, output.failed_msr);
        if (status != Status::ready) return status;
        if ((output.amd_protected.smm_mask.value & 2U) != 0) {
            status = read_register(io, 0xc0010112U, output.amd_protected.smm_base, output.failed_msr);
            if (status != Status::ready) return status;
        }
    } else {
        if ((output.mtrr.capability & (uint64_t{1} << 11)) != 0) {
            status = read_register(io, 0x1f3, output.intel_protected.smrr_mask, output.failed_msr);
            if (status != Status::ready) return status;
            if ((output.intel_protected.smrr_mask.value & VALID) != 0) {
                status = read_register(io, 0x1f2, output.intel_protected.smrr_base, output.failed_msr);
                if (status != Status::ready) return status;
            }
        }
        if ((output.mtrr.capability & (uint64_t{1} << 15)) != 0) {
            status = read_register(io, 0x1401, output.intel_protected.seamrr_mask, output.failed_msr);
            if (status != Status::ready) return status;
            if ((output.intel_protected.seamrr_mask.value & VALID) != 0) {
                status = read_register(io, 0x1400, output.intel_protected.seamrr_base, output.failed_msr);
                if (status != Status::ready) return status;
            }
        }
    }
    return revalidate_registers(io, output, output.failed_msr);
}
}  // namespace ker::mod::mm::mmio_cache::detail
