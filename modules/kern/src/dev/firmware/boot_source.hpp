#pragma once

#include <dev/firmware/types.hpp>

struct limine_file;

namespace ker::dev::firmware {
enum class ModuleKind : uint8_t { ordinary, firmware, invalid_reserved };

// Serialized BSP boot only, before SMP and generic module unpacking. Every
// reserved marker is excluded from unpacking regardless of capture success.
[[nodiscard]] auto module_kind(const limine_file* module) -> ModuleKind;
void capture_boot_source();
// Immutable publication; no Limine metadata or page-table access after capture.
[[nodiscard]] auto source_status() -> Error;
[[nodiscard]] auto source_index() -> std::span<const ArchiveEntry>;
}  // namespace ker::dev::firmware
