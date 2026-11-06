#pragma once

#include "cpu_maintenance.hpp"

namespace ker::init {
struct InitExecutor;
}
namespace ker::mod::power {
class FinalizerExecution;
}

namespace ker::mod::mm::cpu_maintenance::detail {
class ControllerCall;

struct ProtectionObservation {
    uint64_t ready_mask{};
    mmio_cache::Status error{mmio_cache::Status::ready};
};
[[nodiscard]] auto protection_observation() -> ProtectionObservation;

// Private synchronous permission from the actual boot/finalizer continuation.
// Neither a request nor a mapping may retain this object or its issuer.
class Controller final {
    enum class Origin : uint8_t { closed, boot, finalizer };
    Origin origin_{Origin::closed};
    union Issuer {
        const ker::init::InitExecutor* boot;
        const ker::mod::power::FinalizerExecution* finalizer;
        constexpr Issuer() : boot(nullptr) {}
    } issuer_{};
    uint64_t lineage_{};
    Status birth_status_{Status::invalid_context};

    friend struct ker::init::InitExecutor;
    friend class ker::mod::power::FinalizerExecution;
    friend class ControllerCall;
    explicit Controller(const ker::init::InitExecutor&) noexcept;
    explicit Controller(const ker::mod::power::FinalizerExecution&) noexcept;
    [[nodiscard]] auto validate_boot() const noexcept -> Status;
    [[nodiscard]] auto validate_finalizer() const noexcept -> Status;

   public:
    ~Controller();
    Controller(const Controller&) = delete;
    Controller(Controller&&) = delete;
    auto operator=(const Controller&) -> Controller& = delete;
    auto operator=(Controller&&) -> Controller& = delete;
    [[nodiscard]] auto context_status() const noexcept -> Status;
};

// Readiness observes genuine collection/service publication, not a preexisting
// cache-policy seal. Qualification seals afterward. No request exists on timeout.
[[nodiscard]] auto await_ready(const Controller&, uint64_t absolute_ns) -> Status;
[[nodiscard]] auto prepare(const Controller&, mmio_cache::QualifiedUcRange&&, Kind, Ticket&) -> Status;
[[nodiscard]] auto publish(const Controller&, Ticket&) -> Status;
[[nodiscard]] auto observe(const Controller&, const Ticket&) -> Report;
[[nodiscard]] auto wait_until(const Controller&, Ticket&, uint64_t absolute_ns) -> Report;
[[nodiscard]] auto release_complete(const Controller&, Ticket&) -> Status;
#ifdef WOS_SELFTEST
[[nodiscard]] auto diagnostic_defer(const Controller&, Ticket&, Phase, size_t logical_cpu) -> Status;
[[nodiscard]] auto diagnostic_resume(const Controller&, Ticket&, Phase) -> Status;
#endif
}  // namespace ker::mod::mm::cpu_maintenance::detail
