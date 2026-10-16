// Explicit Linux-only seams. Real fork/session/FD/wait/signal paths stay in the
// owner. WOS-only profile/status/procfs authority is controlled input, not credit.
#include <fcntl.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>

#include "uma_reference_arm.hpp"

namespace {
pid_t const original = ::getpid();
uint64_t since{};
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
auto uma_arm_host_snapshot(ker::abi::init_control::StatusSnapshot& s) -> int64_t {
    namespace arm = uma_reference::arm;
    namespace init = arm::init;
    uint64_t now{};
    if (!arm::now(now)) return -1;
    if (!since) since = now - 1'000'000;
    s = {};
    s.size = sizeof(s);
    s.version = 1;
    s.state = init::SupervisorState::RUNNING;
    s.sequence = 2;
    s.published_mono_ns = now;
    s.service_count = 8;
    for (size_t i = 0; i < arm::SERVICES.size(); ++i) {
        auto& r = s.services[i];
        std::strcpy(r.service, arm::SERVICES[i]);
        r.state = init::ServiceState::RUNNING;
        r.readiness = init::ReadinessState::READY;
        r.flags = 3;
        r.pid = 100 + int64_t(i);
        r.pgid = r.pid;
        r.generation = 1;
        r.state_since_mono_ns = since;
    }
    auto& r = s.services[6];
    r.readiness = init::ReadinessState::NOT_READY;
    r.pid = original;
    r.pgid = original;
    r.deadline_mono_ns = since + 600 * arm::SECOND;
    r.history_count = 1;
    r.history[0] = {since, init::ServiceState::STARTING, init::ServiceState::RUNNING, init::TransitionReason::SPAWN, 0};
    if (std::getenv("UMA_HOST_CONSUMED")) {
        size_t size{};
        init::StatusSnapshot initial{};
        if (!arm::read("init-start.bin", &initial, sizeof(initial), size) || size != sizeof(initial)) return -1;
        s = initial;
        s.sequence = initial.sequence + 1;
        s.published_mono_ns = now;
        auto& selected = s.services[6];
        selected.state = init::ServiceState::EXITED;
        selected.readiness = init::ReadinessState::READY;
        selected.pid = -1;
        selected.pgid = -1;
        selected.deadline_mono_ns = 0;
        selected.state_since_mono_ns = now;
        selected.history_count = 1;
        selected.history[0] = {now, init::ServiceState::RUNNING, init::ServiceState::EXITED, init::TransitionReason::EXIT, 0};
        if (std::getenv("UMA_HOST_BAD_INIT")) selected.readiness = init::ReadinessState::NOT_READY;
    }
    return 0;
}
auto uma_arm_host_poweroff() -> bool {
    static constexpr char text[] = "HOST_ONLY_POWEROFF_REQUEST\n";
    return uma_reference::arm::save("host-poweroff.txt", text, sizeof(text) - 1);
}
