#include <cstddef>
#include <cstdint>
#include <string_view>

#include "perf_data.hpp"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    std::string_view const input(reinterpret_cast<const char*>(data), size);
    auto parsed = perf::perf_data::parse(input);
    if (!parsed.file.has_value()) {
        return 0;
    }

    if (parsed.file->format == perf::perf_data::Format::STRUCTURED_V1) {
        auto reencoded =
            perf::perf_data::encode_v1(parsed.file->legacy_snapshot, parsed.file->typed_events_jsonl, parsed.file->typed_event_records);
        if (reencoded.bytes.has_value()) {
            (void)perf::perf_data::parse(*reencoded.bytes);
        }
    }
    return 0;
}
