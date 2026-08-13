#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace ker::loader::runtime {

inline constexpr size_t MAX_IMAGES = 32;
inline constexpr size_t IMAGE_PATH_MAX = 256;
inline constexpr size_t BUILD_ID_MAX = 32;

enum ImageFlags : uint32_t {
    IMAGE_MAIN = 1U << 0,
    IMAGE_INTERPRETER = 1U << 1,
    IMAGE_SHARED_OBJECT = 1U << 2,
    IMAGE_EXACT_MAPPING = 1U << 3,
    IMAGE_BUILD_ID_PRESENT = 1U << 4,
};

struct RuntimeImage {
    uint64_t load_base{};
    uint64_t image_start{};
    uint64_t image_end{};
    uint64_t text_start{};
    uint64_t text_end{};
    uint64_t entry{};
    uint64_t dynamic_addr{};
    uint32_t flags{};
    uint8_t build_id_size{};
    std::array<uint8_t, BUILD_ID_MAX> build_id{};
    std::array<char, IMAGE_PATH_MAX> path{};
};

// The reader must copy exactly size bytes from the target address space and
// return false without side effects when any byte is unavailable. This keeps
// the catalog usable both by ptrace and the allocation-free coredump path.
using TargetReader = auto (*)(void* opaque, uint64_t address, void* destination, size_t size) -> bool;

struct SnapshotSource {
    uint64_t program_header_addr{};
    uint64_t program_header_count{};
    uint64_t program_header_ent_size{};
    uint64_t main_elf_header_addr{};
    uint64_t main_load_base{};
    uint64_t main_image_start{};
    uint64_t main_image_end{};
    uint64_t main_entry{};
    uint64_t interpreter_base{};
    uint64_t interpreter_image_start{};
    uint64_t interpreter_image_end{};
    const char* main_path{};
    const char* interpreter_path{};
};

enum class SnapshotStatus : uint8_t {
    COMPLETE,
    STATIC_IMAGE,
    UNAVAILABLE,
    INCONSISTENT,
    TRUNCATED,
};

// Always attempts to emit authoritative main/interpreter fallback records from
// SnapshotSource. For dynamic processes, a COMPLETE result additionally proves
// that the stable GDB link_map chain was walked to its null terminator.
[[nodiscard]] auto snapshot_images(TargetReader reader, void* opaque, const SnapshotSource& source,
                                   std::array<RuntimeImage, MAX_IMAGES>& images, size_t& count) -> SnapshotStatus;

#ifdef WOS_SELFTEST
// Resolve the dynamic-loader rendezvous using the same bounded target-memory
// parser as production. This covers WOS's -z rodynamic ABI, where DT_DEBUG is
// intentionally absent and mlibc exports `_dl_debug_addr` instead.
[[nodiscard]] auto selftest_find_debug_interface(TargetReader reader, void* opaque, const SnapshotSource& source, uint64_t& address)
    -> bool;
#endif

}  // namespace ker::loader::runtime
