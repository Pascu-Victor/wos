#include "boot_source.hpp"

#include <extern/limine.h>

#include <array>
#include <atomic>
#include <dev/firmware/archive.hpp>
#include <dev/firmware/boot_source_model.hpp>
#include <platform/init/limine_requests.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/mm.hpp>
#include <platform/mm/virt.hpp>

namespace ker::dev::firmware {
namespace {
std::array<ArchiveEntry, MAX_INDEX_ENTRIES> index{};
std::array<boot::MemoryRegion, boot::MAX_MEMORY_REGIONS> memory_scratch{};
std::atomic<Error> status{Error::unavailable};
size_t index_count = 0;
bool attempted = false;  // Serialized BSP boot only.

template <typename T>
auto readable(const T* pointer, size_t count = 1) -> bool {
    uint64_t const ADDRESS = reinterpret_cast<uint64_t>(pointer);
    return pointer != nullptr && ADDRESS % alignof(T) == 0 && count <= UINT64_MAX / sizeof(T) &&
           mod::mm::virt::boot_range_readable(ADDRESS, count * sizeof(T));
}

auto capture() -> Error {
    auto const* response_pointer = init::get_kernel_module_request().response;
    if (response_pointer == nullptr) return Error::unavailable;
    if (!readable(response_pointer)) return Error::unsafe_source;
    limine_module_response const RESPONSE = *response_pointer;
    if (RESPONSE.module_count > boot::MAX_MODULES) return Error::source_limit;
    if (RESPONSE.module_count == 0) return Error::unavailable;
    if (!readable(RESPONSE.modules, RESPONSE.module_count)) return Error::unsafe_source;

    struct Candidate {
        uint64_t address = 0, bytes = 0;
    } candidate;
    size_t candidates = 0;
    bool invalid = false;
    for (size_t i = 0; i < RESPONSE.module_count; ++i) {
        auto const* module = RESPONSE.modules[i];
        auto const KIND = module_kind(module);
        if (KIND == ModuleKind::invalid_reserved) invalid = true;
        if (KIND != ModuleKind::firmware) continue;
        ++candidates;
        // Metadata was qualified by module_kind; copy scalars while boot is
        // serialized. Neither this record nor the selector pointer is retained.
        candidate = {reinterpret_cast<uint64_t>(module->address), module->size};
    }
    if (invalid) return Error::unsafe_source;
    if (candidates > 1) return Error::ambiguous_source;
    if (candidates == 0) return Error::unavailable;
    if (candidate.bytes > MAX_SOURCE_BYTES) return Error::source_limit;
    auto const RANGE = boot::source_range(candidate.address, candidate.bytes, mod::mm::addr::get_hhdm_offset());
    if (!RANGE) return Error::unsafe_source;

    auto const* memory_pointer = mod::mm::boot_memory_map();
    if (!readable(memory_pointer)) return Error::unsafe_source;
    limine_memmap_response const MEMORY = *memory_pointer;
    if (MEMORY.entry_count == 0 || MEMORY.entry_count > memory_scratch.size() || !readable(MEMORY.entries, MEMORY.entry_count))
        return Error::unsafe_source;
    for (size_t i = 0; i < MEMORY.entry_count; ++i) {
        auto const* entry = MEMORY.entries[i];
        if (!readable(entry)) return Error::unsafe_source;
        memory_scratch[i] = {entry->base, entry->length, entry->type == LIMINE_MEMMAP_EXECUTABLE_AND_MODULES};
    }
    if (!boot::reserved_source(*RANGE, MEMORY.entry_count,
                               [](size_t i) -> std::optional<boot::MemoryRegion> { return memory_scratch[i]; }) ||
        !mod::mm::virt::boot_module_range_immutable(RANGE->virtual_start, RANGE->physical_start, RANGE->bytes))
        return Error::unsafe_source;
    auto const SOURCE = std::span<const uint8_t>{reinterpret_cast<const uint8_t*>(candidate.address), candidate.bytes};
    auto const SCANNED = scan_archive(SOURCE, index);
    if (!SCANNED.valid()) return SCANNED.error;
    if (SCANNED.count > index.size()) return Error::limit;
    index_count = SCANNED.count;
    return Error::none;
}
}  // namespace

auto module_kind(const limine_file* module) -> ModuleKind {
    if (!readable(module)) return ModuleKind::invalid_reserved;
    limine_file const METADATA = *module;
    if (METADATA.string == nullptr) return ModuleKind::ordinary;
    uint64_t const ADDRESS = reinterpret_cast<uint64_t>(METADATA.string);
    return boot::classify_marker([ADDRESS](size_t i) -> std::optional<char> {
        if (i > UINT64_MAX - ADDRESS || !mod::mm::virt::boot_range_readable(ADDRESS + i, 1)) return std::nullopt;
        return *reinterpret_cast<const char*>(ADDRESS + i);
    });
}

void capture_boot_source() {
    if (attempted) return;
    attempted = true;
    status.store(capture(), std::memory_order_release);
}
auto source_status() -> Error { return status.load(std::memory_order_acquire); }
auto source_index() -> std::span<const ArchiveEntry> {
    return source_status() == Error::none ? std::span<const ArchiveEntry>{index}.first(index_count) : std::span<const ArchiveEntry>{};
}
}  // namespace ker::dev::firmware
