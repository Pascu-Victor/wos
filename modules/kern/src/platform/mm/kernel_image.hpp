#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

extern "C" {
extern char __kernel_image_start[];       // NOLINT(readability-identifier-naming)
extern char __kernel_end[];               // NOLINT(readability-identifier-naming)
extern char __kernel_text_start[];        // NOLINT(readability-identifier-naming)
extern char __kernel_text_end[];          // NOLINT(readability-identifier-naming)
extern char __kernel_rodata_start[];      // NOLINT(readability-identifier-naming)
extern char __kernel_rodata_end[];        // NOLINT(readability-identifier-naming)
extern char __kernel_data_start[];        // NOLINT(readability-identifier-naming)
extern char __kernel_data_end[];          // NOLINT(readability-identifier-naming)
extern char __kernel_dynamic_start[];     // NOLINT(readability-identifier-naming)
extern char __kernel_build_id_start[];    // NOLINT(readability-identifier-naming)
extern char __kernel_build_id_end[];      // NOLINT(readability-identifier-naming)
extern char __kernel_relocation_start[];  // NOLINT(readability-identifier-naming)
extern char __kernel_relocation_end[];    // NOLINT(readability-identifier-naming)
}

namespace ker::mod::mm::kernel_image {

struct RuntimeLayout {
    uintptr_t link_base{};
    uintptr_t load_base{};
    uintptr_t slide{};
    uintptr_t image_end{};
    uintptr_t text_start{};
    uintptr_t text_end{};
    uintptr_t rodata_start{};
    uintptr_t rodata_end{};
    uintptr_t data_start{};
    uintptr_t data_end{};
    uintptr_t dynamic_start{};
};

[[nodiscard]] inline auto layout() -> RuntimeLayout {
    auto const LOAD_BASE = reinterpret_cast<uintptr_t>(__kernel_image_start);
    return {
        .link_base = 0,
        .load_base = LOAD_BASE,
        .slide = LOAD_BASE,
        .image_end = reinterpret_cast<uintptr_t>(__kernel_end),
        .text_start = reinterpret_cast<uintptr_t>(__kernel_text_start),
        .text_end = reinterpret_cast<uintptr_t>(__kernel_text_end),
        .rodata_start = reinterpret_cast<uintptr_t>(__kernel_rodata_start),
        .rodata_end = reinterpret_cast<uintptr_t>(__kernel_rodata_end),
        .data_start = reinterpret_cast<uintptr_t>(__kernel_data_start),
        .data_end = reinterpret_cast<uintptr_t>(__kernel_data_end),
        .dynamic_start = reinterpret_cast<uintptr_t>(__kernel_dynamic_start),
    };
}

[[nodiscard]] inline auto contains(uintptr_t address, size_t size = 1) -> bool {
#ifdef WOS_HOST_TEST
    // Host-only unit binaries do not link the kernel script. Their synthetic
    // pointer-validation fixtures retain the historical canonical aperture.
    constexpr uintptr_t HOST_FIXTURE_START = 0xffffffff80000000ULL;
    constexpr uintptr_t HOST_FIXTURE_END = 0xffffffffc0000000ULL;
    return size <= HOST_FIXTURE_END - HOST_FIXTURE_START && address >= HOST_FIXTURE_START && address <= HOST_FIXTURE_END - size;
#else
    RuntimeLayout const LAYOUT = layout();
    return size <= LAYOUT.image_end - LAYOUT.load_base && address >= LAYOUT.load_base && address <= LAYOUT.image_end - size;
#endif
}

[[nodiscard]] inline auto build_id_note() -> std::span<const std::byte> {
    auto const* const START = reinterpret_cast<const std::byte*>(__kernel_build_id_start);
    auto const SIZE =
        static_cast<size_t>(reinterpret_cast<uintptr_t>(__kernel_build_id_end) - reinterpret_cast<uintptr_t>(__kernel_build_id_start));
    return {START, SIZE};
}

[[nodiscard]] inline auto relocation_table() -> std::span<const std::byte> {
    auto const* const START = reinterpret_cast<const std::byte*>(__kernel_relocation_start);
    auto const SIZE =
        static_cast<size_t>(reinterpret_cast<uintptr_t>(__kernel_relocation_end) - reinterpret_cast<uintptr_t>(__kernel_relocation_start));
    return {START, SIZE};
}

}  // namespace ker::mod::mm::kernel_image
