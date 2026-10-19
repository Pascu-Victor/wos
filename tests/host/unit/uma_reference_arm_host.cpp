// Explicit Linux-only seams. Real fork/session/FD/wait/signal paths stay in the
// owner. WOS-only profile/status/procfs authority is controlled input, not credit.
#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "supervisor_model.h"
#include "uma_reference_arm.hpp"

// Implemented by the whole unmodified production classifier selected by the
// Python driver; this seam supplies finite actual model types, not its formula.
extern auto uma_arm_host_aggregate(const wos::init::SupervisorModel&) -> ker::abi::init_control::SupervisorState;

namespace {
pid_t const original = ::getpid();
uint64_t since{}, completed_since{};
unsigned pending_snapshots{};
}  // namespace
auto uma_arm_host_profile(uma_reference::arm::Profile& p, pid_t expected, bool witness) -> bool {
    p = {};
    p.pid = uint64_t(::getpid());
    p.parent = ::getpid() == original ? uint64_t(expected) : uint64_t(::getppid());
    p.tid = p.pid;
    p.cpus = 8;
    p.affinity = 255;
    p.authority = witness ? 16 : 65535;
    p.nnp = witness ? 1 : 0;
    p.privileges = witness ? 256 : 0;
    p.target_flags = 2;
    p.empty_target = true;
    if (::getpid() != original && std::getenv("UMA_HOST_REJECT_CHILD")) p.affinity = 1;
    return true;
}
void uma_arm_host_state(ker::abi::init_control::StatusSnapshot& s) {
    namespace init = ker::abi::init_control;
    namespace model = wos::init;
    model::SupervisorModel value{};
    value.service_count = static_cast<uint8_t>(s.service_count);
    // Fixed manifest inputs: keygen is skipped, testd disabled, the controller
    // stops being desired on completion; the exit-witness remains desired.
    for (size_t i = 0; i < s.service_count; ++i) {
        auto const& row = s.services[i];
        auto& service = value.services[i];
        service.desired_running = i != 1 && i != 5 && !(i == 6 && row.state == init::ServiceState::EXITED);
        switch (row.state) {
            case init::ServiceState::WAITING:
                service.state = model::ServiceState::WAITING;
                break;
            case init::ServiceState::STARTING:
                service.state = model::ServiceState::STARTING;
                break;
            case init::ServiceState::RUNNING:
                service.state = row.readiness == init::ReadinessState::READY ? model::ServiceState::READY : model::ServiceState::RUNNING;
                break;
            case init::ServiceState::STOPPING:
                service.state = model::ServiceState::STOPPING;
                break;
            case init::ServiceState::BACKOFF:
                service.state = model::ServiceState::BACKOFF;
                break;
            case init::ServiceState::EXITED:
                service.state = model::ServiceState::EXITED;
                break;
            case init::ServiceState::FAILED:
                service.state = model::ServiceState::FAILED;
                break;
            case init::ServiceState::DISABLED:
                service.state = model::ServiceState::DISABLED;
                break;
            default:
                ::_exit(98);
        }
    }
    s.state = uma_arm_host_aggregate(value);
}
void uma_arm_host_complete(ker::abi::init_control::StatusSnapshot& s, pid_t observer) {
    namespace arm = uma_reference::arm;
    namespace init = arm::init;
    if (!completed_since) completed_since = s.published_mono_ns;
    uint64_t const now = completed_since;
    ++s.sequence;
    auto& selected = s.services[6];
    selected.state = init::ServiceState::EXITED;
    selected.readiness = init::ReadinessState::READY;
    selected.pid = -1;
    selected.pgid = -1;
    selected.deadline_mono_ns = 0;
    selected.state_since_mono_ns = now;
    std::memset(selected.history, 0, sizeof(selected.history));
    selected.history_count = 1;
    selected.history[0] = {now, init::ServiceState::STOPPING, init::ServiceState::EXITED, init::TransitionReason::EXIT, 0};
    auto& witness = s.services[7];
    witness.state = init::ServiceState::RUNNING;
    witness.pid = observer;
    witness.pgid = observer;
    witness.generation = 1;
    witness.state_since_mono_ns = now;
    witness.deadline_mono_ns = now + arm::MARGIN;
    std::memset(witness.history, 0, sizeof(witness.history));
    witness.history_count = 1;
    witness.history[0] = {now, init::ServiceState::STARTING, init::ServiceState::RUNNING, init::TransitionReason::SPAWN, 0};
    uma_arm_host_state(s);
}
void uma_arm_host_pending(ker::abi::init_control::StatusSnapshot& s) {
    namespace init = ker::abi::init_control;
    auto& witness = s.services[7];
    witness.state = init::ServiceState::STARTING;
    witness.history[0] = {witness.state_since_mono_ns, init::ServiceState::WAITING, init::ServiceState::STARTING,
                          init::TransitionReason::DEPENDENCY, 0};
    uma_arm_host_state(s);
}
auto uma_arm_host_snapshot(ker::abi::init_control::StatusSnapshot& s) -> int64_t {
    namespace arm = uma_reference::arm;
    namespace init = arm::init;
    uint64_t now{};
    if (!arm::now(now)) return -1;
    if (!since) since = now - 1'000'000;
    s = {};
    s.size = sizeof(s);
    s.version = 1;
    s.sequence = 2;
    s.published_mono_ns = now;
    s.service_count = 8;
    for (size_t i = 0; i < arm::SERVICES.size(); ++i) {
        auto& r = s.services[i];
        std::strcpy(r.service, arm::SERVICES[i]);
        r.state = init::ServiceState::RUNNING;
        r.readiness = init::ReadinessState::READY;
        r.flags = 3;
        r.pid = int64_t(::getpid()) + 1000 + int64_t(i);
        r.pgid = r.pid;
        r.generation = 1;
        r.state_since_mono_ns = since;
        r.history_count = 1;
        r.history[0] = {since, init::ServiceState::STARTING, init::ServiceState::RUNNING, init::TransitionReason::SPAWN, 0};
    }
    auto& keygen = s.services[1];
    keygen.state = init::ServiceState::EXITED;
    keygen.flags = 0;
    keygen.pid = keygen.pgid = -1;
    keygen.generation = 0;
    keygen.history[0] = {since, init::ServiceState::UNKNOWN, init::ServiceState::EXITED, init::TransitionReason::START_REQUEST, 0};
    auto& testd = s.services[5];
    testd.state = init::ServiceState::DISABLED;
    testd.readiness = init::ReadinessState::NOT_READY;
    testd.flags = 0;
    testd.pid = testd.pgid = -1;
    testd.generation = 0;
    testd.history[0] = {since, init::ServiceState::UNKNOWN, init::ServiceState::DISABLED, init::TransitionReason::START_REQUEST, 0};
    auto& r = s.services[6];
    r.readiness = init::ReadinessState::NOT_READY;
    r.pid = original;
    r.pgid = original;
    r.deadline_mono_ns = since + 600 * arm::SECOND;
    auto& witness = s.services[7];
    witness.state = init::ServiceState::WAITING;
    witness.readiness = init::ReadinessState::NOT_READY;
    witness.pid = witness.pgid = -1;
    witness.generation = 0;
    witness.history[0] = {since, init::ServiceState::UNKNOWN, init::ServiceState::WAITING, init::TransitionReason::START_REQUEST, 0};
    if (std::getenv("UMA_HOST_CONSUMED")) {
        size_t size{};
        init::StatusSnapshot initial{};
        if (!arm::read("init-start.bin", &initial, sizeof(initial), size) || size != sizeof(initial)) return -1;
        s = initial;
        s.published_mono_ns = now;
        uma_arm_host_complete(s, ::getpid());
        if (std::getenv("UMA_HOST_PENDING_EXEC") && ++pending_snapshots <= 3) uma_arm_host_pending(s);
        if (std::getenv("UMA_HOST_BAD_INIT")) s.services[6].readiness = init::ReadinessState::NOT_READY;
    } else {
        uma_arm_host_state(s);
    }
    return 0;
}
auto uma_arm_host_poweroff() -> bool {
    if (std::getenv("UMA_HOST_PENDING_EXEC") && pending_snapshots < 4) return false;
    static constexpr char text[] = "HOST_ONLY_POWEROFF_REQUEST\n";
    return uma_reference::arm::save("host-poweroff.txt", text, sizeof(text) - 1);
}
