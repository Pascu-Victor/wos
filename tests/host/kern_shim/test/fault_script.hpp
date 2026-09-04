#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>

namespace wos::test {

enum class FaultKind : uint8_t {
    ALLOCATION_FAILURE,
    CANCELLATION,
    TIMEOUT,
    DISCONNECT,
    STALE_GENERATION,
    REORDER,
    DUPLICATE_COMPLETION,
    CONCURRENT_TEARDOWN,
};

struct FaultStep {
    FaultKind kind{};
    uint32_t occurrence{1};
    uint64_t generation{};
    int result{};
};

// A fixed-capacity, allocation-free fault schedule shared by host models and
// production-source shim tests. Each row fires once on its exact occurrence.
template <size_t Capacity>
class FaultScript {
   public:
    [[nodiscard]] auto add(FaultStep step) -> bool {
        if (count_ >= Capacity || step.occurrence == 0) {
            return false;
        }
        steps_.at(count_) = step;
        count_++;
        return true;
    }

    [[nodiscard]] auto hit(FaultKind kind, uint64_t generation = 0) -> std::optional<FaultStep> {
        bool generation_relevant = false;
        bool generation_matches = false;
        for (size_t index = 0; index < count_; ++index) {
            FaultStep const& step = steps_.at(index);
            if (fired_.at(index) || step.kind != kind) {
                continue;
            }
            generation_relevant = generation_relevant || step.generation != 0;
            generation_matches = generation_matches || step.generation == 0 || step.generation == generation;
        }
        if (generation_relevant && !generation_matches) {
            return std::nullopt;
        }
        uint32_t& seen = seen_.at(static_cast<size_t>(kind));
        seen++;
        for (size_t index = 0; index < count_; ++index) {
            FaultStep const& step = steps_.at(index);
            if (!fired_.at(index) && step.kind == kind && step.occurrence == seen &&
                (step.generation == 0 || step.generation == generation)) {
                fired_.at(index) = true;
                return step;
            }
        }
        return std::nullopt;
    }

    [[nodiscard]] auto exhausted() const -> bool {
        for (size_t index = 0; index < count_; ++index) {
            if (!fired_.at(index)) {
                return false;
            }
        }
        return true;
    }

   private:
    static constexpr size_t KIND_COUNT = static_cast<size_t>(FaultKind::CONCURRENT_TEARDOWN) + 1;
    std::array<FaultStep, Capacity> steps_{};
    std::array<bool, Capacity> fired_{};
    std::array<uint32_t, KIND_COUNT> seen_{};
    size_t count_{};
};

class CompletionLatch {
   public:
    [[nodiscard]] auto publish(uint64_t generation, int result) -> bool {
        if (complete_ || generation != generation_) {
            return false;
        }
        result_ = result;
        complete_ = true;
        return true;
    }

    void reset(uint64_t generation) {
        generation_ = generation;
        result_ = 0;
        complete_ = false;
    }

    [[nodiscard]] auto result() const -> int { return result_; }
    [[nodiscard]] auto complete() const -> bool { return complete_; }

   private:
    uint64_t generation_{};
    int result_{};
    bool complete_{};
};

class AdmissionGate {
   public:
    [[nodiscard]] auto enter() -> bool {
        if (!accepting_) {
            return false;
        }
        active_++;
        return true;
    }

    [[nodiscard]] auto leave() -> bool {
        if (active_ == 0) {
            return false;
        }
        active_--;
        return true;
    }

    void close() { accepting_ = false; }
    [[nodiscard]] auto quiesced() const -> bool { return !accepting_ && active_ == 0; }

   private:
    size_t active_{};
    bool accepting_{true};
};

}  // namespace wos::test
