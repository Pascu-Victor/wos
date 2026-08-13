#include <QByteArray>
#include <cstddef>
#include <cstdint>
#include <limits>

#include "coredump_parser.h"

namespace {

auto checked_range(uint64_t offset, uint64_t size, uint64_t total) -> bool { return offset <= total && size <= total - offset; }

[[noreturn]] void invariant_failure() { __builtin_trap(); }

}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > static_cast<size_t>(std::numeric_limits<qsizetype>::max())) {
        return 0;
    }
    wosdbg::CoreDumpParseLimits limits;
    limits.max_file_bytes = 1024 * 1024;
    limits.max_segments = 4096;
    limits.max_segment_bytes = 1024 * 1024;
    limits.max_embedded_elf_bytes = 1024 * 1024;

    const QByteArray bytes(reinterpret_cast<const char*>(data), static_cast<qsizetype>(size));
    const auto result = wosdbg::parse_core_dump_checked(bytes, limits);
    if (!result.ok()) {
        if (result.dump.has_value()) {
            invariant_failure();
        }
        return 0;
    }

    const auto& dump = *result.dump;
    const uint64_t raw_size = static_cast<uint64_t>(dump.raw.size());
    if (!dump.is_valid() || dump.version < 1 || dump.version > 4 || result.detected_version != dump.version ||
        dump.segment_count != dump.segments.size() || dump.module_count != dump.modules.size()) {
        invariant_failure();
    }
    if (dump.elf_size != 0 && !checked_range(dump.elf_offset, dump.elf_size, raw_size)) {
        invariant_failure();
    }
    for (const auto& segment : dump.segments) {
        if (segment.is_present() && !checked_range(segment.file_offset, segment.size, raw_size)) {
            invariant_failure();
        }
    }
    return 0;
}
