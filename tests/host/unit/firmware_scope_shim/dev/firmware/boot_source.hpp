#pragma once
#include <dev/firmware/types.hpp>
namespace ker::dev::firmware {
auto source_status() -> Error;
auto source_index() -> std::span<const ArchiveEntry>;
}  // namespace ker::dev::firmware
