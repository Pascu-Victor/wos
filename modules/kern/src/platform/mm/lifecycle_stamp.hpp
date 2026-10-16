#pragma once

#ifdef WOS_SELFTEST
#include <cstdint>

namespace ker::mod::mm::lifecycle_census {

// A version is compared only within one complete double collect, never to the
// original baseline. Invalid/exhausted stamps permanently refuse acceptance.
struct Stamp {
    uint64_t version{};
    uint64_t inflight{};
    bool valid{};
    auto operator==(const Stamp&) const -> bool = default;
};

[[nodiscard]] inline auto quiescent(const Stamp& value) noexcept -> bool {
    return value.valid && value.version != 0 && value.inflight == 0;
}

[[nodiscard]] inline auto unchanged(const Stamp& before, const Stamp& after) noexcept -> bool {
    return quiescent(before) && quiescent(after) && before.version == after.version;
}

}  // namespace ker::mod::mm::lifecycle_census
#endif
