#include <sys/init_control.h>
#include <sys/process.h>
#include <time.h>

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <initializer_list>

namespace {

namespace init_abi = ker::abi::init_control;

auto program_basename(const char* path) -> const char* {
    if (path == nullptr) {
        return "servicectl";
    }
    const char* base = path;
    for (const char* current = path; *current != '\0'; ++current) {
        if (*current == '/') {
            base = current + 1;
        }
    }
    return base;
}

void usage(const char* argv0) {
    std::fprintf(stderr, "usage: %s status [SERVICE]\n       %s {start|stop|restart} SERVICE\n", program_basename(argv0),
                 program_basename(argv0));
}

auto supervisor_state_name(init_abi::SupervisorState state) -> const char* {
    switch (state) {
        case init_abi::SupervisorState::UNKNOWN:
            return "unknown";
        case init_abi::SupervisorState::STARTING:
            return "starting";
        case init_abi::SupervisorState::RUNNING:
            return "running";
        case init_abi::SupervisorState::STOPPING:
            return "stopping";
        case init_abi::SupervisorState::STOPPED:
            return "stopped";
        case init_abi::SupervisorState::FAILED:
            return "failed";
    }
    return "invalid";
}

auto service_state_name(init_abi::ServiceState state) -> const char* {
    switch (state) {
        case init_abi::ServiceState::UNKNOWN:
            return "unknown";
        case init_abi::ServiceState::WAITING:
            return "waiting";
        case init_abi::ServiceState::STARTING:
            return "starting";
        case init_abi::ServiceState::RUNNING:
            return "running";
        case init_abi::ServiceState::STOPPING:
            return "stopping";
        case init_abi::ServiceState::BACKOFF:
            return "backoff";
        case init_abi::ServiceState::EXITED:
            return "exited";
        case init_abi::ServiceState::FAILED:
            return "failed";
        case init_abi::ServiceState::DISABLED:
            return "disabled";
    }
    return "invalid";
}

auto readiness_name(init_abi::ReadinessState readiness) -> const char* {
    switch (readiness) {
        case init_abi::ReadinessState::UNKNOWN:
            return "unknown";
        case init_abi::ReadinessState::NOT_READY:
            return "not-ready";
        case init_abi::ReadinessState::READY:
            return "ready";
        case init_abi::ReadinessState::FAILED:
            return "failed";
    }
    return "invalid";
}

auto transition_reason_name(init_abi::TransitionReason reason) -> const char* {
    switch (reason) {
        case init_abi::TransitionReason::NONE:
            return "none";
        case init_abi::TransitionReason::DEPENDENCY:
            return "dependency";
        case init_abi::TransitionReason::START_REQUEST:
            return "start-request";
        case init_abi::TransitionReason::SPAWN:
            return "spawn";
        case init_abi::TransitionReason::READINESS:
            return "readiness";
        case init_abi::TransitionReason::EXIT:
            return "exit";
        case init_abi::TransitionReason::RESTART:
            return "restart";
        case init_abi::TransitionReason::STOP_REQUEST:
            return "stop-request";
        case init_abi::TransitionReason::SHUTDOWN:
            return "shutdown";
        case init_abi::TransitionReason::TIMEOUT:
            return "timeout";
    }
    return "invalid";
}

auto report_syscall_error(const char* operation, int64_t result) -> int {
    int const ERROR = result < 0 && result >= -4095 ? static_cast<int>(-result) : EIO;
    std::fprintf(stderr, "servicectl: %s: %s\n", operation, std::strerror(ERROR));
    return 1;
}

void print_service(const init_abi::ServiceStatus& service) {
    std::printf("%-20s %-10s %-10s pid=%lld pgid=%lld gen=%llu restarts=%llu wait=%d error=%d\n", service.service,
                service_state_name(service.state), readiness_name(service.readiness), static_cast<long long>(service.pid),
                static_cast<long long>(service.pgid), static_cast<unsigned long long>(service.generation),
                static_cast<unsigned long long>(service.restart_count), service.last_wait_status, service.last_error);
}

void print_history(const init_abi::ServiceStatus& service) {
    uint32_t const COUNT =
        service.history_count <= init_abi::MAX_TRANSITION_HISTORY ? service.history_count : init_abi::MAX_TRANSITION_HISTORY;
    std::printf("  history=%u\n", COUNT);
    for (uint32_t index = 0; index < COUNT; ++index) {
        auto const& transition = service.history[index];
        std::printf("    t=%llu %s -> %s reason=%s\n", static_cast<unsigned long long>(transition.timestamp_mono_ns),
                    service_state_name(transition.from_state), service_state_name(transition.to_state),
                    transition_reason_name(transition.reason));
    }
}

auto print_resource_status(uint32_t slot) -> int {
    namespace abi = ker::abi::service_resource;
    abi::Snapshot snapshot{};
    int64_t const RESULT = ker::process::service_resource_read(slot, &snapshot);
    if (RESULT < 0) return report_syscall_error("read service resource usage", RESULT);
    if (snapshot.size != sizeof(snapshot) || snapshot.version != abi::VERSION || snapshot.slot != slot) {
        std::fprintf(stderr, "servicectl: kernel returned an unsupported resource snapshot\n");
        return 1;
    }
    constexpr const char* NAMES[abi::RESOURCE_COUNT] = {"CPU", "FSIZE", "STACK", "CORE", "NPROC", "NOFILE", "AS"};
    std::printf("  resources: cpu-us=%llu live-processes=%llu (CPU spans service restarts)\n",
                static_cast<unsigned long long>(snapshot.cpu_time_us), static_cast<unsigned long long>(snapshot.live_processes));
    if ((snapshot.flags & abi::ACCOUNTING_INCOMPLETE) != 0)
        std::printf("  Remote accounting is incomplete; cumulative usage totals are lower bounds.\n");
    std::printf("  Resource Soft Hard Current Peak Denied Reason\n");
    for (unsigned i = 0; i < abi::RESOURCE_COUNT; ++i) {
        auto const& usage = snapshot.usage[i];
        std::printf("  %s ", NAMES[i]);
        for (auto const value : {usage.limit.soft, usage.limit.hard}) {
            if (value == ker::abi::resource::INFINITY_VALUE)
                std::printf("infinity ");
            else
                std::printf("%llu ", static_cast<unsigned long long>(value));
        }
        const char* reason = "none";
        switch (usage.reason) {
            case ker::abi::resource::Denial::NONE:
                break;
            case ker::abi::resource::Denial::SOFT_LIMIT:
                reason = "soft_limit";
                break;
            case ker::abi::resource::Denial::HARD_LIMIT:
                reason = "hard_limit";
                break;
            case ker::abi::resource::Denial::OVERFLOW:
                reason = "overflow";
                break;
        }
        std::printf("%llu %llu %llu %s\n", static_cast<unsigned long long>(usage.current), static_cast<unsigned long long>(usage.peak),
                    static_cast<unsigned long long>(usage.denied), reason);
    }
    return 0;
}

auto show_status(const char* requested_service) -> int {
    init_abi::StatusSnapshot status{};
    int64_t const RESULT = ker::process::init_status_read(&status);
    if (RESULT < 0) {
        return report_syscall_error("read supervisor status", RESULT);
    }
    if (status.size != sizeof(status) || status.version != init_abi::ABI_VERSION || status.service_count > init_abi::MAX_STATUS_SERVICES) {
        std::fprintf(stderr, "servicectl: kernel returned an unsupported status snapshot\n");
        return 1;
    }

    std::printf("supervisor=%s sequence=%llu services=%u%s\n", supervisor_state_name(status.state),
                static_cast<unsigned long long>(status.sequence), status.service_count,
                (status.flags & init_abi::SNAPSHOT_FLAG_SHUTTING_DOWN) != 0 ? " shutting-down" : "");
    bool found = false;
    for (uint32_t index = 0; index < status.service_count; ++index) {
        auto const& service = status.services[index];
        if (requested_service == nullptr || std::strcmp(requested_service, service.service) == 0) {
            print_service(service);
            if (requested_service != nullptr) {
                print_history(service);
                if (geteuid() == 0 && print_resource_status(index) != 0) return 1;
            }
            found = true;
        }
    }
    if (requested_service != nullptr && !found) {
        std::fprintf(stderr, "servicectl: unknown service: %s\n", requested_service);
        return 1;
    }
    return 0;
}

auto parse_action(const char* text, init_abi::Action& action) -> bool {
    if (std::strcmp(text, "start") == 0) {
        action = init_abi::Action::START;
        return true;
    }
    if (std::strcmp(text, "stop") == 0) {
        action = init_abi::Action::STOP;
        return true;
    }
    if (std::strcmp(text, "restart") == 0) {
        action = init_abi::Action::RESTART;
        return true;
    }
    return false;
}

auto request_id() -> uint64_t {
    timespec now{};
    uint64_t value = ker::process::getpid() << 32U;
    if (clock_gettime(CLOCK_MONOTONIC, &now) == 0) {
        value ^= static_cast<uint64_t>(now.tv_sec) * 1'000'000'000ULL;
        value ^= static_cast<uint64_t>(now.tv_nsec);
    }
    return value != 0 ? value : 1;
}

auto submit(init_abi::Action action, const char* service) -> int {
    size_t const LENGTH = std::strlen(service);
    if (LENGTH == 0 || LENGTH >= init_abi::SERVICE_NAME_CAPACITY) {
        std::fprintf(stderr, "servicectl: invalid service name\n");
        return 2;
    }

    init_abi::Request request{};
    request.size = sizeof(request);
    request.version = init_abi::ABI_VERSION;
    request.action = action;
    request.request_id = request_id();
    std::memcpy(request.service, service, LENGTH + 1);

    int64_t const RESULT = ker::process::init_control_submit(&request);
    if (RESULT < 0) {
        return report_syscall_error("submit request", RESULT);
    }
    std::printf("accepted request=%llu service=%s\n", static_cast<unsigned long long>(request.request_id), service);
    return 0;
}

}  // namespace

auto main(int argc, char** argv) -> int {
    if (argc < 2) {
        usage(argc > 0 ? argv[0] : "servicectl");
        return 2;
    }
    if (std::strcmp(argv[1], "status") == 0) {
        if (argc > 3) {
            usage(argv[0]);
            return 2;
        }
        return show_status(argc == 3 ? argv[2] : nullptr);
    }

    init_abi::Action action{};
    if (argc != 3 || !parse_action(argv[1], action)) {
        usage(argv[0]);
        return 2;
    }
    return submit(action, argv[2]);
}
