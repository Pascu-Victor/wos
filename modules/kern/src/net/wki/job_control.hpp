#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <net/wki/job_state.hpp>

namespace ker::net::wki::jobs {

struct SignalHandle {
    size_t index{};
    uint64_t generation{};
    constexpr auto operator==(const SignalHandle&) const -> bool = default;
};
enum class SignalPhase : uint8_t { PROPOSED, CLAIMED, DONE };
struct SignalView {
    uint32_t signal{};
    SignalPhase phase{SignalPhase::PROPOSED};
    int32_t status{};  // Checked native result; meaningful only in DONE.
};
struct SignalResult {
    Admission admission{Admission::INVALID};
    SignalHandle handle{};
};

// One exact job owns these values under its registry lock. Native work must
// retain that job and exact Task before claim, carry BOTH job and signal handles,
// deliver outside locks, and complete once. No numeric PID becomes authority.
template <size_t Count>
class SignalReceipts {
    static_assert(Count != 0);

   public:
    constexpr explicit SignalReceipts(uint32_t max_signal, uint64_t first_generation = 1)
        : max_signal_(max_signal), next_generation_(first_generation) {}
    SignalReceipts(const SignalReceipts&) = delete;
    auto operator=(const SignalReceipts&) -> SignalReceipts& = delete;
    SignalReceipts(SignalReceipts&&) = delete;
    auto operator=(SignalReceipts&&) -> SignalReceipts& = delete;

    constexpr auto propose(uint64_t serial, uint32_t signal) -> SignalResult {
        if (signal == 0 || signal > max_signal_) return {Admission::INVALID};
        auto const EXISTING = lookup(serial);
        if (EXISTING.admission == Admission::REPLAY)
            return records_[EXISTING.handle.index].signal == signal ? EXISTING : SignalResult{Admission::CONFLICT};
        if (EXISTING.admission != Admission::UNKNOWN) return EXISTING;
        if (next_generation_ == 0) return {Admission::CAPACITY};
        for (size_t index = 0; index < records_.size(); ++index) {
            auto& record = records_[index];
            if (record.used) continue;
            auto const GENERATION = next_generation_;
            next_generation_ = GENERATION == UINT64_MAX ? 0 : GENERATION + 1;
            record = {.used = true, .serial = serial, .generation = GENERATION, .signal = signal};
            high_water_ = serial;
            return {Admission::NEW, {index, GENERATION}};
        }
        return {Admission::CAPACITY};
    }
    [[nodiscard]] constexpr auto lookup(uint64_t serial) const -> SignalResult {
        if (serial == 0 || max_signal_ == 0) return {Admission::INVALID};
        for (size_t index = 0; index < records_.size(); ++index) {
            auto const& record = records_[index];
            if (record.used && record.serial == serial) return {Admission::REPLAY, {index, record.generation}};
        }
        return {serial <= high_water_ ? Admission::EXPIRED : Admission::UNKNOWN};
    }
    constexpr auto claim(SignalHandle handle) -> bool {
        auto* record = find(handle);
        if (record == nullptr || record->phase != SignalPhase::PROPOSED) return false;
        record->phase = SignalPhase::CLAIMED;
        return true;
    }
    constexpr auto complete(SignalHandle handle, int32_t status) -> bool {
        auto* record = find(handle);
        if (record == nullptr || record->phase != SignalPhase::CLAIMED || status > 0) return false;
        record->status = status;
        record->phase = SignalPhase::DONE;
        return true;
    }
    [[nodiscard]] constexpr auto inspect(SignalHandle handle, SignalView& view) const -> bool {
        if (handle.index >= records_.size() || handle.generation == 0) return false;
        auto const& record = records_[handle.index];
        if (!record.used || record.generation != handle.generation) return false;
        view = {record.signal, record.phase, record.status};
        return true;
    }

    // Only acknowledged/TTL-eligible completed receipts may be retired. Keep a
    // compact watermark for the whole job lifetime; expiry never redelivers.
    constexpr auto retire(SignalHandle handle) -> bool {
        auto* record = find(handle);
        if (record == nullptr || record->phase != SignalPhase::DONE) return false;
        *record = {};
        return true;
    }

   private:
    struct Record {
        bool used{};
        uint64_t serial{};
        uint64_t generation{};
        uint32_t signal{};
        SignalPhase phase{SignalPhase::PROPOSED};
        int32_t status{};
    };
    constexpr auto find(SignalHandle handle) -> Record* {
        if (handle.index >= records_.size() || handle.generation == 0) return nullptr;
        auto& record = records_[handle.index];
        return record.used && record.generation == handle.generation ? &record : nullptr;
    }
    uint32_t max_signal_{};
    uint64_t next_generation_{};
    uint64_t high_water_{};
    std::array<Record, Count> records_{};
};

}  // namespace ker::net::wki::jobs
