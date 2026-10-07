#pragma once

#include <array>
#include <cstdint>

namespace ker::vfs::coherency {

struct AppendOutcome {
    int32_t status = 0;
    uint32_t bytes = 0;
    uint64_t offset = 0;
    constexpr auto operator==(const AppendOutcome&) const -> bool = default;
};

enum class AppendClaim : uint8_t { NEW, REPLAY, PENDING, STALE, MISMATCH, INVALID };

// The caller's exact-open admission and serialization qualify this fixed state.
// A newer operation acknowledges the last completed outcome; lower IDs never
// become new effects, even after the retained reply has been superseded.
class AppendReceipt {
   public:
    using Digest = std::array<uint8_t, 32>;

    auto claim(uint64_t operation, uint32_t requested, const Digest& digest, AppendOutcome& replay) -> AppendClaim {
        if (operation == 0) return AppendClaim::INVALID;
        if (operation < operation_) return AppendClaim::STALE;
        if (operation == operation_) {
            if (requested != requested_ || digest != digest_) return AppendClaim::MISMATCH;
            if (pending_) return AppendClaim::PENDING;
            replay = outcome_;
            return AppendClaim::REPLAY;
        }
        if (pending_) return AppendClaim::PENDING;
        operation_ = operation;
        requested_ = requested;
        digest_ = digest;
        pending_ = true;
        return AppendClaim::NEW;
    }

    auto complete(uint64_t operation, const AppendOutcome& outcome) -> bool {
        if (!pending_ || operation != operation_ || outcome.status > 0 || outcome.bytes > requested_ ||
            (outcome.status < 0 && (outcome.bytes != 0 || outcome.offset != 0)) || outcome.offset > INT64_MAX ||
            outcome.bytes > static_cast<uint64_t>(INT64_MAX) - outcome.offset)
            return false;
        outcome_ = outcome;
        pending_ = false;
        return true;
    }

   private:
    uint64_t operation_ = 0;
    uint32_t requested_ = 0;
    Digest digest_{};
    bool pending_ = false;
    AppendOutcome outcome_{};
};

}  // namespace ker::vfs::coherency
