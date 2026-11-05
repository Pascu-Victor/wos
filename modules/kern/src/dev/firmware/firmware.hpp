#pragma once

#include <dev/firmware/types.hpp>
#include <platform/sched/shutdown_ownership.hpp>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::dev::firmware {
// Borrowed immutable bytes. Use only while the issuing Scope retains this
// lease. Copying a View does not acquire another reference or device authority.
struct View {
    std::span<const uint8_t> bytes{};
    Handle object{};
    uint64_t scope = 0;
    uint64_t lease = 0;
};

// Ordinary task context only, outside driver/VM/IRQ/spin locks. A Scope is bound
// to its constructing task and must be destroyed there. It protects ALL leases
// from cooperative task exit, including across voluntary scheduling. Explicit
// task exit with a live Scope is forbidden, as with other kernel RAII resources.
class Scope {
   public:
    Scope();
    ~Scope();
    Scope(const Scope&) = delete;
    Scope(Scope&&) = delete;
    auto operator=(const Scope&) -> Scope& = delete;
    auto operator=(Scope&&) -> Scope& = delete;

    // Failure leaves out unchanged. Policy must come from a trusted kernel
    // catalog; an archive's own manifest is never a policy authority.
    [[nodiscard]] auto request(const Policy& policy, uint32_t required_revision, View& out) -> Error;
    [[nodiscard]] auto release(const View& view) -> Error;

   private:
    struct Lease {
        Handle object{};
        uint64_t sequence = 0;
    };
    [[nodiscard]] auto valid_context() const -> bool;

    ker::mod::sched::task::Task* owner_;
    ker::mod::sched::KernelShutdownHold hold_;
    uint64_t identity_ = 0;
    uint64_t next_lease_ = 0;
    Error error_ = Error::context;
    std::array<Lease, SCOPE_LEASES> leases_{};
};

// A coherent metadata snapshot; failure leaves out unchanged. Task-only.
[[nodiscard]] auto snapshot(Stats& out) -> Error;
}  // namespace ker::dev::firmware
