#pragma once

#include <cstddef>
#include <cstdint>

namespace wos::abi {

// QEMU's RSP endpoint cannot expose WOS's runtime-image catalog.  Keep one
// read-only page at a stable diagnostic address so a paused, raw-RSP debugger
// can discover the relocated kernel and authenticate it by GNU build ID.
inline constexpr uint64_t KERNEL_DEBUG_METADATA_ADDRESS = 0xfffffe0000000000ULL;
inline constexpr uint64_t KERNEL_DEBUG_METADATA_MAGIC = 0x574f534b44424731ULL;  // "WOSKDBG1"
inline constexpr uint32_t KERNEL_DEBUG_METADATA_VERSION = 1;
inline constexpr size_t KERNEL_DEBUG_BUILD_ID_MAX = 32;

struct KernelDebugMetadata {
    uint64_t magic{};
    uint32_t version{};
    uint32_t size{};
    uint64_t load_base{};
    uint64_t image_end{};
    uint64_t text_start{};
    uint64_t text_end{};
    uint64_t rodata_start{};
    uint64_t rodata_end{};
    uint64_t data_start{};
    uint64_t data_end{};
    uint64_t entry{};
    uint64_t dynamic_addr{};
    uint32_t flags{};
    uint8_t build_id_size{};
    uint8_t reserved[3]{};
    uint8_t build_id[KERNEL_DEBUG_BUILD_ID_MAX]{};
};

static_assert(sizeof(KernelDebugMetadata) == 136);

enum KernelDebugMetadataFlags : uint32_t {
    KERNEL_DEBUG_METADATA_EXACT_MAPPING = 1U << 0,
    KERNEL_DEBUG_METADATA_BUILD_ID_PRESENT = 1U << 1,
};

}  // namespace wos::abi
