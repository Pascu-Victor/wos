#include "services.h"

#include <abi-bits/resource.h>
#include <callnums/sys_log.h>
#include <fcntl.h>
#include <signal.h>  // NOLINT(modernize-deprecated-headers): WOS signal constants live here.
#include <sys/init_control.h>
#include <sys/logging.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>  // NOLINT(modernize-deprecated-headers): WOS POSIX sleep declarations live here.
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <span>
#include <string_view>

#include "env.h"
#include "network.h"
#include "service_manifest.h"
#include "supervisor_model.h"

namespace {

namespace control_abi = ker::abi::init_control;
using init_log = wos::journal<"init">;
using namespace std::string_view_literals;
using namespace wos::init;

constexpr char SERVICE_MANIFEST_PATH[] = "/etc/wos-services.conf";
constexpr size_t PIPE_READ = 0;
constexpr size_t PIPE_WRITE = 1;
constexpr size_t OUTPUT_LINE_BYTES = 512;
constexpr size_t OUTPUT_READ_BYTES_PER_TICK = 4096;
constexpr size_t MAX_REAPS_PER_TICK = 128;
constexpr uint32_t MAX_STALE_EVENT_LOGS = 8;
constexpr uint64_t NSEC_PER_MSEC = 1000000ULL;
constexpr uint64_t SHUTDOWN_PHASE_SLACK_MS = 1000;
constexpr uint64_t FAILED_RUNTIME_SHUTDOWN_MS = 2000;
constexpr uint32_t INIT_WKI_TARGET_FLAGS = ker::process::WKI_TARGET_FLAG_LOCAL | ker::process::WKI_TARGET_FLAG_NOINHERIT;
constexpr uint32_t SERVICE_WKI_TARGET_FLAGS = ker::process::WKI_TARGET_FLAG_LOCAL;
constexpr size_t MAX_RUNTIME_ENVIRONMENT = MAX_ENVIRONMENT + 5 + 1;

enum class ChildFailureStage : int32_t {
    PROCESS_GROUP = 1,
    WKI_TARGET = 2,
    PRIORITY = 3,
    STDIO = 4,
    EXEC = 5,
};

struct ExecFailureReport {
    ChildFailureStage stage{ChildFailureStage::EXEC};
    int32_t error{EIO};
};

static_assert(sizeof(ExecFailureReport) <= 16);

struct RuntimeService {
    int64_t pid{-1};
    int64_t pgid{-1};
    uint32_t generation{};
    int exec_report_fd{-1};
    int output_fd{-1};
    NetworkProbe probe{};
    std::array<char, OUTPUT_LINE_BYTES> output_line{};
    size_t output_line_length{};
    uint64_t restart_count{};
    int32_t last_wait_status{};
    int32_t last_error{};
    bool ever_spawned{};
    bool leader_reaped{true};
    bool exec_resolved{true};
    bool output_complete{true};
    bool quiescence_reported{true};
    bool exit_pending{};
    bool output_forced_closed{};
    bool cleanup_only{};
};

struct SupervisorRuntime {
    ServiceManifest manifest{};
    ServiceTopology topology{};
    SupervisorModel model{};
    std::array<EnablementResult, MAX_SERVICES> enablement{};
    std::array<RuntimeService, MAX_SERVICES> services{};
    std::array<char, MAX_MANIFEST_BYTES> manifest_bytes{};
    uint64_t now_ms{};
    uint32_t stale_event_count{};
    int64_t last_status_publish_error{};
    bool clock_initialized{};
    bool clock_fallback_logged{};
    bool start_attempted{};
    bool model_ready{};
    bool failed{};
    bool non_journal_shutdown_complete{};
    bool shutdown_complete{};
    bool init_wki_target_valid{};
};

SupervisorRuntime runtime{};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto restore_init_wki_target() -> bool {
    for (uint8_t attempt = 0; attempt < 3; ++attempt) {
        if (ker::process::setwkitarget(nullptr, 0, INIT_WKI_TARGET_FLAGS) == 0) {
            runtime.init_wki_target_valid = true;
            return true;
        }
    }
    runtime.init_wki_target_valid = false;
    return false;
}

void close_fd(int& fd) {
    int const OLD_FD = fd;
    fd = -1;
    if (OLD_FD >= 0) {
        (void)::close(OLD_FD);
    }
}

auto saturating_add(uint64_t left, uint64_t right) -> uint64_t {
    if (left > std::numeric_limits<uint64_t>::max() - right) {
        return std::numeric_limits<uint64_t>::max();
    }
    return left + right;
}

auto to_nanoseconds(uint64_t milliseconds) -> uint64_t {
    if (milliseconds > std::numeric_limits<uint64_t>::max() / NSEC_PER_MSEC) {
        return std::numeric_limits<uint64_t>::max();
    }
    return milliseconds * NSEC_PER_MSEC;
}

auto read_monotonic_ms(uint64_t fallback) -> uint64_t {
    timespec ts{};
    if (::clock_gettime(CLOCK_MONOTONIC, &ts) == 0 && ts.tv_sec >= 0 && ts.tv_nsec >= 0) {
        uint64_t const SECONDS = static_cast<uint64_t>(ts.tv_sec);
        uint64_t const MILLIS = (SECONDS > std::numeric_limits<uint64_t>::max() / 1000ULL)
                                    ? std::numeric_limits<uint64_t>::max()
                                    : (SECONDS * 1000ULL) + static_cast<uint64_t>(ts.tv_nsec / 1000000L);
        if (!runtime.clock_initialized || MILLIS > runtime.now_ms) {
            runtime.clock_initialized = true;
            return MILLIS;
        }
    }

    if (!runtime.clock_fallback_logged) {
        runtime.clock_fallback_logged = true;
        init_log::warn("init supervisor: monotonic clock stalled or failed; using bounded tick fallback");
    }
    return fallback;
}

auto advance_runtime_clock() -> uint64_t {
    uint64_t const FALLBACK = saturating_add(runtime.now_ms, static_cast<uint64_t>(SERVICE_SUPERVISOR_TICK_MS));
    runtime.now_ms = read_monotonic_ms(FALLBACK);
    return runtime.now_ms;
}

void sleep_one_tick() {
    timespec const SLEEP{
        .tv_sec = SERVICE_SUPERVISOR_TICK_MS / 1000,
        .tv_nsec = (SERVICE_SUPERVISOR_TICK_MS % 1000) * 1000L * 1000L,
    };
    (void)::nanosleep(&SLEEP, nullptr);
}

auto set_close_on_exec(int fd) -> bool {
    int const FLAGS = ::fcntl(fd, F_GETFD);
    return FLAGS >= 0 && ::fcntl(fd, F_SETFD, FLAGS | FD_CLOEXEC) == 0;
}

auto set_nonblocking(int fd) -> bool {
    int const FLAGS = ::fcntl(fd, F_GETFL);
    return FLAGS >= 0 && ::fcntl(fd, F_SETFL, FLAGS | O_NONBLOCK) == 0;
}

auto move_above_standard_io(int& fd) -> bool {
    if (fd > STDERR_FILENO) {
        return true;
    }
    int const REPLACEMENT = ::fcntl(fd, F_DUPFD_CLOEXEC, STDERR_FILENO + 1);
    if (REPLACEMENT < 0) {
        return false;
    }
    (void)::close(fd);
    fd = REPLACEMENT;
    return true;
}

auto create_parent_read_pipe(std::array<int, 2>& pipefd) -> bool {
    if (::pipe(pipefd.data()) != 0) {
        return false;
    }
    if (!move_above_standard_io(pipefd.at(PIPE_READ)) || !move_above_standard_io(pipefd.at(PIPE_WRITE)) ||
        !set_close_on_exec(pipefd.at(PIPE_READ)) || !set_close_on_exec(pipefd.at(PIPE_WRITE)) || !set_nonblocking(pipefd.at(PIPE_READ))) {
        int const ERROR = errno;
        close_fd(pipefd.at(PIPE_READ));
        close_fd(pipefd.at(PIPE_WRITE));
        errno = ERROR;
        return false;
    }
    return true;
}

auto positive_error(int64_t result) -> int32_t {
    if (result < 0 && result >= -static_cast<int64_t>(std::numeric_limits<int32_t>::max())) {
        return static_cast<int32_t>(-result);
    }
    return errno != 0 ? errno : EIO;
}

[[noreturn]] void report_child_failure(int fd, ChildFailureStage stage, int32_t error) {
    ExecFailureReport const REPORT{
        .stage = stage,
        .error = error > 0 ? error : EIO,
    };
    ssize_t written = 0;
    do {
        written = ::write(fd, &REPORT, sizeof(REPORT));
    } while (written < 0 && errno == EINTR);
    (void)written;
    ker::process::exit(127);
    __builtin_unreachable();
}

auto environment_key(std::string_view entry) -> std::string_view {
    size_t const EQUALS = entry.find('=');
    return EQUALS == std::string_view::npos ? entry : entry.substr(0, EQUALS);
}

auto manifest_overrides_environment(const ServiceSpec& service, std::string_view base_entry) -> bool {
    std::string_view const BASE_KEY = environment_key(base_entry);
    for (uint8_t index = 0; index < service.environment_count; ++index) {
        if (environment_key(service.environment.at(index).view()) == BASE_KEY) {
            return true;
        }
    }
    return false;
}

void append_init_environment(const ServiceSpec& service, const InitEnv& init_env,
                             std::array<const char*, MAX_RUNTIME_ENVIRONMENT>& environment, size_t& count) {
    auto append = [&](const auto& storage) {
        if (storage.front() == '\0' || count + 1 >= environment.size()) {
            return;
        }
        std::string_view const ENTRY(storage.data());
        if (!manifest_overrides_environment(service, ENTRY)) {
            environment.at(count++) = storage.data();
        }
    };
    append(init_env.user);
    append(init_env.home);
    append(init_env.hostname);
    append(init_env.llvm_profile_file);
}

auto runtime_service_for_pid(int64_t pid) -> uint8_t {
    for (uint8_t service = 0; service < runtime.manifest.service_count; ++service) {
        if (runtime.services.at(service).pid == pid) {
            return service;
        }
    }
    return INVALID_SERVICE_ID;
}

auto runtime_service_for_name(const char* name) -> uint8_t {
    if (name == nullptr) {
        return INVALID_SERVICE_ID;
    }
    for (uint8_t service = 0; service < runtime.manifest.service_count; ++service) {
        if (runtime.manifest.services.at(service).name.view() == name) {
            return service;
        }
    }
    return INVALID_SERVICE_ID;
}

void mark_runtime_failed(const char* operation, SupervisorResult result) {
    runtime.failed = true;
    init_log::critical("init supervisor: %s failed with model result %u", operation, static_cast<unsigned>(result));
}

void dispatch_actions(const SupervisorActionList& actions, uint64_t now_ms);

void ignore_stale_event(const SupervisorEvent& event) {
    ++runtime.stale_event_count;
    if (runtime.stale_event_count <= MAX_STALE_EVENT_LOGS) {
        init_log::warn("init supervisor: ignored stale event kind=%u service=%u generation=%u", static_cast<unsigned>(event.kind),
                       static_cast<unsigned>(event.service), event.generation);
        if (runtime.stale_event_count == MAX_STALE_EVENT_LOGS) {
            init_log::warn("init supervisor: suppressing further stale-event warnings");
        }
    }
}

auto emit_model_event(const SupervisorEvent& event, uint64_t now_ms) -> bool {
    if (!runtime.model_ready || runtime.failed) {
        return false;
    }
    SupervisorActionList actions{};
    SupervisorResult const RESULT = supervisor_handle_event(runtime.model, now_ms, event, actions);
    if (RESULT == SupervisorResult::STALE_GENERATION) {
        ignore_stale_event(event);
        return true;
    }
    if (RESULT != SupervisorResult::OK) {
        mark_runtime_failed("event", RESULT);
        return false;
    }
    dispatch_actions(actions, now_ms);
    return !runtime.failed;
}

void flush_output_line(uint8_t service) {
    auto& slot = runtime.services.at(service);
    if (slot.output_line_length == 0) {
        return;
    }
    auto const& name = runtime.manifest.services.at(service).name;
    ker::logging::logEx(name.c_str(), ker::abi::sys_log::sys_log_level::INFO, slot.output_line.data(),
                        static_cast<uint64_t>(slot.output_line_length));
    slot.output_line_length = 0;
}

void close_service_output(uint8_t service, bool forced) {
    auto& slot = runtime.services.at(service);
    flush_output_line(service);
    close_fd(slot.output_fd);
    slot.output_complete = true;
    slot.output_forced_closed = slot.output_forced_closed || forced;
}

void drain_service_output(uint8_t service) {
    auto& slot = runtime.services.at(service);
    if (slot.output_fd < 0 || slot.output_complete) {
        return;
    }

    std::array<char, 512> buffer{};
    size_t bytes_this_tick = 0;
    while (bytes_this_tick < OUTPUT_READ_BYTES_PER_TICK) {
        size_t const CAPACITY = OUTPUT_READ_BYTES_PER_TICK - bytes_this_tick;
        size_t const REQUEST = CAPACITY < buffer.size() ? CAPACITY : buffer.size();
        ssize_t const READ = ::read(slot.output_fd, buffer.data(), REQUEST);
        if (READ > 0) {
            bytes_this_tick += static_cast<size_t>(READ);
            for (char const value : std::span<const char>(buffer.data(), static_cast<size_t>(READ))) {
                if (value == '\n' || value == '\r') {
                    flush_output_line(service);
                    continue;
                }
                if (slot.output_line_length == slot.output_line.size()) {
                    flush_output_line(service);
                }
                slot.output_line.at(slot.output_line_length++) = value;
            }
            continue;
        }
        if (READ == 0) {
            close_service_output(service, false);
            return;
        }
        if (errno == EINTR) {
            continue;
        }
        if (errno == EAGAIN || errno == EWOULDBLOCK) {
            return;
        }
        slot.last_error = errno;
        init_log::warn("init supervisor: output read failed for %s: errno=%d", runtime.manifest.services.at(service).name.c_str(), errno);
        close_service_output(service, true);
        return;
    }
}

auto service_group_gone(const RuntimeService& slot) -> bool {
    if (slot.pgid <= 1) {
        return true;
    }
    int64_t const RESULT = ker::process::kill(-slot.pgid, 0);
    return RESULT == -ESRCH;
}

void maybe_report_quiescence(uint8_t service, uint64_t now_ms) {
    auto& slot = runtime.services.at(service);
    if (!runtime.model_ready || runtime.failed || slot.cleanup_only || slot.quiescence_reported || !slot.leader_reaped ||
        !slot.exec_resolved || !slot.output_complete || !service_group_gone(slot)) {
        return;
    }

    SupervisorEvent const EVENT{
        .kind = SupervisorEventKind::QUIESCED,
        .service = service,
        .generation = slot.generation,
    };
    SupervisorActionList actions{};
    SupervisorResult const RESULT = supervisor_handle_event(runtime.model, now_ms, EVENT, actions);
    if (RESULT == SupervisorResult::STALE_GENERATION) {
        ignore_stale_event(EVENT);
        return;
    }
    if (RESULT != SupervisorResult::OK) {
        mark_runtime_failed("quiescence event", RESULT);
        return;
    }

    // The model accepted the generation and released its ownership. Clear the
    // low-level identity before dispatching actions, because acceptance may
    // immediately make a controlled restart eligible to spawn.
    network_probe_close(slot.probe);
    slot.quiescence_reported = true;
    slot.pgid = -1;
    dispatch_actions(actions, now_ms);
}

void deliver_pending_exit(uint8_t service, uint64_t now_ms) {
    auto& slot = runtime.services.at(service);
    if (!slot.exit_pending || !slot.exec_resolved) {
        return;
    }
    slot.exit_pending = false;

    SupervisorEvent event{
        .kind = SupervisorEventKind::EXITED,
        .service = service,
        .generation = slot.generation,
    };
    if (WIFEXITED(slot.last_wait_status)) {
        event.exit_kind = SupervisorExitKind::EXIT_CODE;
        event.detail = WEXITSTATUS(slot.last_wait_status);
    } else if (WIFSIGNALED(slot.last_wait_status)) {
        event.exit_kind = SupervisorExitKind::SIGNAL;
        event.detail = WTERMSIG(slot.last_wait_status);
    } else {
        event.exit_kind = SupervisorExitKind::SIGNAL;
        event.detail = SIGKILL;
    }
    (void)emit_model_event(event, now_ms);
    maybe_report_quiescence(service, now_ms);
}

void process_exec_report(uint8_t service, uint64_t now_ms) {
    auto& slot = runtime.services.at(service);
    if (slot.exec_report_fd < 0 || slot.exec_resolved) {
        return;
    }

    ExecFailureReport report{};
    ssize_t const READ = ::read(slot.exec_report_fd, &report, sizeof(report));
    if (READ < 0 && (errno == EAGAIN || errno == EWOULDBLOCK || errno == EINTR)) {
        return;
    }

    close_fd(slot.exec_report_fd);
    slot.exec_resolved = true;
    SupervisorEvent event{
        .service = service,
        .generation = slot.generation,
    };
    if (READ == 0) {
        event.kind = SupervisorEventKind::EXEC_SUCCEEDED;
        slot.last_error = 0;
    } else {
        event.kind = SupervisorEventKind::EXEC_FAILED;
        if (READ == static_cast<ssize_t>(sizeof(report)) && report.error > 0) {
            slot.last_error = report.error;
            event.detail = report.error;
            init_log::error("init supervisor: %s child setup/exec stage %d failed: errno=%d",
                            runtime.manifest.services.at(service).name.c_str(), static_cast<int>(report.stage), report.error);
        } else {
            slot.last_error = EIO;
            event.detail = EIO;
            init_log::error("init supervisor: malformed exec report for %s", runtime.manifest.services.at(service).name.c_str());
        }
    }
    (void)emit_model_event(event, now_ms);
    deliver_pending_exit(service, now_ms);
}

void signal_service_group(const SupervisorAction& action, int signal) {
    if (action.service >= runtime.manifest.service_count) {
        return;
    }
    auto const& slot = runtime.services.at(action.service);
    if (slot.generation != action.generation || slot.pgid <= 1 || slot.quiescence_reported) {
        return;
    }

    if (!slot.leader_reaped) {
        if (slot.pid <= 1 || ker::process::getpgid(slot.pid) != slot.pgid) {
            init_log::warn("init supervisor: refusing stale group signal for %s generation %u",
                           runtime.manifest.services.at(action.service).name.c_str(), action.generation);
            return;
        }
    } else if (service_group_gone(slot)) {
        return;
    }

    int64_t const RESULT = ker::process::kill(-slot.pgid, signal);
    if (RESULT < 0 && RESULT != -ESRCH) {
        init_log::warn("init supervisor: signal %d failed for %s pgid %lld: %lld", signal,
                       runtime.manifest.services.at(action.service).name.c_str(), static_cast<long long>(slot.pgid),
                       static_cast<long long>(RESULT));
    }
}

void child_setup_stdio(const ServiceSpec& service, const std::array<int, 2>& output_pipe, int exec_report_fd) {
    if (service.stdio == StdioPolicy::JOURNAL) {
        (void)::close(output_pipe.at(PIPE_READ));
        if (::dup2(output_pipe.at(PIPE_WRITE), STDOUT_FILENO) < 0 || ::dup2(output_pipe.at(PIPE_WRITE), STDERR_FILENO) < 0) {
            report_child_failure(exec_report_fd, ChildFailureStage::STDIO, errno);
        }
        (void)::close(output_pipe.at(PIPE_WRITE));
        return;
    }
    if (service.stdio != StdioPolicy::NULL_DEVICE) {
        return;
    }

    int const NULL_FD = ::open("/dev/null", O_RDWR | O_CLOEXEC);
    if (NULL_FD < 0 || ::dup2(NULL_FD, STDIN_FILENO) < 0 || ::dup2(NULL_FD, STDOUT_FILENO) < 0 || ::dup2(NULL_FD, STDERR_FILENO) < 0) {
        int const ERROR = errno;
        if (NULL_FD >= 0) {
            (void)::close(NULL_FD);
        }
        report_child_failure(exec_report_fd, ChildFailureStage::STDIO, ERROR);
    }
    if (NULL_FD > STDERR_FILENO) {
        (void)::close(NULL_FD);
    }
}

auto spawn_service(uint8_t service, uint32_t generation, int32_t& failure, bool& fatal) -> bool {
    auto& slot = runtime.services.at(service);
    auto const& spec = runtime.manifest.services.at(service);
    fatal = false;
    if (!slot.quiescence_reported || slot.pid > 0 || slot.pgid > 0 || slot.exec_report_fd >= 0 || slot.output_fd >= 0) {
        failure = EBUSY;
        return false;
    }
    if (!runtime.init_wki_target_valid && !restore_init_wki_target()) {
        failure = EPERM;
        fatal = true;
        return false;
    }

    std::array<const char*, MAX_ARGUMENTS + 1> arguments{};
    for (uint8_t index = 0; index < spec.argument_count; ++index) {
        arguments.at(index) = spec.arguments.at(index).c_str();
    }

    InitEnv init_environment{};
    std::array<const char*, MAX_RUNTIME_ENVIRONMENT> environment{};
    size_t environment_count = 0;
    if (spec.environment_profile == EnvironmentProfile::INIT) {
        init_environment = make_init_env();
        append_init_environment(spec, init_environment, environment, environment_count);
    }
    for (uint8_t index = 0; index < spec.environment_count; ++index) {
        if (environment_count + 1 >= environment.size()) {
            failure = E2BIG;
            return false;
        }
        environment.at(environment_count++) = spec.environment.at(index).c_str();
    }
    environment.at(environment_count) = nullptr;

    std::array<int, 2> exec_pipe{-1, -1};
    if (!create_parent_read_pipe(exec_pipe)) {
        failure = errno;
        return false;
    }
    std::array<int, 2> output_pipe{-1, -1};
    if (spec.stdio == StdioPolicy::JOURNAL && !create_parent_read_pipe(output_pipe)) {
        failure = errno;
        close_fd(exec_pipe.at(PIPE_READ));
        close_fd(exec_pipe.at(PIPE_WRITE));
        return false;
    }

    int64_t const TARGET_RESULT = ker::process::setwkitarget(nullptr, 0, SERVICE_WKI_TARGET_FLAGS);
    if (TARGET_RESULT < 0) {
        failure = positive_error(TARGET_RESULT);
        close_fd(exec_pipe.at(PIPE_READ));
        close_fd(exec_pipe.at(PIPE_WRITE));
        close_fd(output_pipe.at(PIPE_READ));
        close_fd(output_pipe.at(PIPE_WRITE));
        return false;
    }
    runtime.init_wki_target_valid = false;

    int64_t const PID = ker::process::fork();
    bool parent_target_restored = true;
    if (PID != 0) {
        parent_target_restored = restore_init_wki_target();
    }

    if (PID == 0) {
        (void)::close(exec_pipe.at(PIPE_READ));
        if (spec.stdio == StdioPolicy::JOURNAL) {
            child_setup_stdio(spec, output_pipe, exec_pipe.at(PIPE_WRITE));
        }
        int64_t const PGID_RESULT = ker::process::setpgid(0, 0);
        if (PGID_RESULT < 0) {
            report_child_failure(exec_pipe.at(PIPE_WRITE), ChildFailureStage::PROCESS_GROUP, positive_error(PGID_RESULT));
        }
        int64_t const CHILD_TARGET_RESULT = ker::process::setwkitarget(nullptr, 0, SERVICE_WKI_TARGET_FLAGS);
        if (CHILD_TARGET_RESULT < 0) {
            report_child_failure(exec_pipe.at(PIPE_WRITE), ChildFailureStage::WKI_TARGET, positive_error(CHILD_TARGET_RESULT));
        }
        int64_t const PRIORITY_RESULT = ker::process::setpriority(PRIO_PROCESS, 0, spec.priority);
        if (PRIORITY_RESULT < 0) {
            report_child_failure(exec_pipe.at(PIPE_WRITE), ChildFailureStage::PRIORITY, positive_error(PRIORITY_RESULT));
        }
        if (spec.stdio != StdioPolicy::JOURNAL) {
            child_setup_stdio(spec, output_pipe, exec_pipe.at(PIPE_WRITE));
        }
        int64_t const EXEC_RESULT = ker::process::execve(spec.executable.c_str(), arguments.data(), environment.data());
        report_child_failure(exec_pipe.at(PIPE_WRITE), ChildFailureStage::EXEC, positive_error(EXEC_RESULT));
    }

    close_fd(exec_pipe.at(PIPE_WRITE));
    close_fd(output_pipe.at(PIPE_WRITE));
    if (PID < 0) {
        failure = positive_error(PID);
        fatal = !parent_target_restored;
        close_fd(exec_pipe.at(PIPE_READ));
        close_fd(output_pipe.at(PIPE_READ));
        return false;
    }

    auto retain_cleanup_child = [&]() {
        close_fd(exec_pipe.at(PIPE_READ));
        close_fd(output_pipe.at(PIPE_READ));
        slot.pid = PID;
        slot.pgid = -1;
        slot.generation = generation;
        slot.exec_report_fd = -1;
        slot.output_fd = -1;
        slot.output_line_length = 0;
        slot.leader_reaped = false;
        slot.exec_resolved = true;
        slot.output_complete = true;
        slot.quiescence_reported = false;
        slot.exit_pending = false;
        slot.output_forced_closed = true;
        slot.cleanup_only = true;
        network_probe_reset(slot.probe);
    };

    if (!parent_target_restored) {
        init_log::critical("init supervisor: PID 1 WKI target restoration failed after forking %s; failing closed", spec.name.c_str());
        (void)ker::process::kill(PID, SIGKILL);
        retain_cleanup_child();
        failure = EPERM;
        fatal = true;
        return false;
    }

    int64_t const PARENT_PGID_RESULT = ker::process::setpgid(PID, PID);
    int64_t const OBSERVED_PGID = ker::process::getpgid(PID);
    if (OBSERVED_PGID != PID) {
        init_log::error("init supervisor: could not isolate %s pid %lld into its own process group (setpgid=%lld observed=%lld)",
                        spec.name.c_str(), static_cast<long long>(PID), static_cast<long long>(PARENT_PGID_RESULT),
                        static_cast<long long>(OBSERVED_PGID));
        (void)ker::process::kill(PID, SIGKILL);
        retain_cleanup_child();
        failure = OBSERVED_PGID < 0 ? positive_error(OBSERVED_PGID) : (PARENT_PGID_RESULT < 0 ? positive_error(PARENT_PGID_RESULT) : EPERM);
        return false;
    }

    if (slot.ever_spawned) {
        ++slot.restart_count;
    }
    slot.ever_spawned = true;
    slot.pid = PID;
    slot.pgid = PID;
    slot.generation = generation;
    slot.exec_report_fd = exec_pipe.at(PIPE_READ);
    exec_pipe.at(PIPE_READ) = -1;
    slot.output_fd = output_pipe.at(PIPE_READ);
    output_pipe.at(PIPE_READ) = -1;
    slot.output_line_length = 0;
    slot.last_wait_status = 0;
    slot.last_error = 0;
    slot.leader_reaped = false;
    slot.exec_resolved = false;
    slot.output_complete = spec.stdio != StdioPolicy::JOURNAL;
    slot.quiescence_reported = false;
    slot.exit_pending = false;
    slot.output_forced_closed = false;
    slot.cleanup_only = false;
    network_probe_reset(slot.probe);

    init_log::info("init supervisor: spawned %s pid=%lld pgid=%lld generation=%u priority=%d", spec.name.c_str(),
                   static_cast<long long>(slot.pid), static_cast<long long>(slot.pgid), generation, spec.priority);
    return true;
}

void begin_network_probe(const SupervisorAction& action, uint64_t now_ms) {
    if (action.service >= runtime.manifest.service_count) {
        return;
    }
    auto& slot = runtime.services.at(action.service);
    if (slot.generation != action.generation || slot.quiescence_reported) {
        return;
    }
    auto const& spec = runtime.manifest.services.at(action.service);
    if (!network_probe_begin(slot.probe, spec.readiness_interface.c_str())) {
        slot.last_error = errno != 0 ? errno : EIO;
        SupervisorEvent const EVENT{
            .kind = SupervisorEventKind::PROBE_ERROR,
            .service = action.service,
            .generation = action.generation,
            .detail = slot.last_error,
        };
        (void)emit_model_event(EVENT, now_ms);
    }
}

void dispatch_action(const SupervisorAction& action, uint64_t now_ms) {
    switch (action.kind) {
        case SupervisorActionKind::SPAWN: {
            if (action.service >= runtime.manifest.service_count) {
                mark_runtime_failed("spawn action", SupervisorResult::INVALID_SERVICE);
                return;
            }
            int32_t failure = 0;
            bool fatal = false;
            bool const SPAWNED = spawn_service(action.service, action.generation, failure, fatal);
            SupervisorEvent const EVENT{
                .kind = SPAWNED ? SupervisorEventKind::SPAWN_SUCCEEDED : SupervisorEventKind::SPAWN_FAILED,
                .service = action.service,
                .generation = action.generation,
                .pid = SPAWNED ? runtime.services.at(action.service).pid : -1,
                .pgid = SPAWNED ? runtime.services.at(action.service).pgid : -1,
                .detail = failure,
            };
            if (!SPAWNED) {
                runtime.services.at(action.service).last_error = failure;
                init_log::error("init supervisor: spawn failed for %s: errno=%d", runtime.manifest.services.at(action.service).name.c_str(),
                                failure);
            }
            if (fatal) {
                SupervisorActionList ignored_actions{};
                SupervisorResult const RESULT = supervisor_handle_event(runtime.model, now_ms, EVENT, ignored_actions);
                if (RESULT != SupervisorResult::OK) {
                    init_log::critical("init supervisor: fatal spawn failure could not be recorded by model: %u",
                                       static_cast<unsigned>(RESULT));
                }
                runtime.failed = true;
                return;
            }
            (void)emit_model_event(EVENT, now_ms);
            return;
        }
        case SupervisorActionKind::START_IPV4_PROBE:
            begin_network_probe(action, now_ms);
            return;
        case SupervisorActionKind::STOP_IPV4_PROBE:
            if (action.service < runtime.manifest.service_count && runtime.services.at(action.service).generation == action.generation) {
                auto const& lifecycle = runtime.model.services.at(action.service);
                if (lifecycle.stop_reason == SupervisorStopReason::READINESS_FAILURE && lifecycle.last_detail == ETIMEDOUT) {
                    runtime.services.at(action.service).last_error = ETIMEDOUT;
                    network_probe_dump_diagnostics(runtime.services.at(action.service).probe, "readiness timeout",
                                                   static_cast<uint64_t>(lifecycle.pid > 0 ? lifecycle.pid : 0));
                }
                network_probe_close(runtime.services.at(action.service).probe);
            }
            return;
        case SupervisorActionKind::SEND_TERM:
            signal_service_group(action, SIGTERM);
            return;
        case SupervisorActionKind::SEND_KILL:
            signal_service_group(action, SIGKILL);
            return;
        case SupervisorActionKind::CLOSE_OUTPUT:
            if (action.service < runtime.manifest.service_count && runtime.services.at(action.service).generation == action.generation) {
                close_service_output(action.service, true);
                maybe_report_quiescence(action.service, now_ms);
            }
            return;
        case SupervisorActionKind::NON_JOURNAL_SHUTDOWN_COMPLETE:
            runtime.non_journal_shutdown_complete = true;
            return;
        case SupervisorActionKind::SHUTDOWN_COMPLETE:
            runtime.shutdown_complete = true;
            return;
    }
}

void dispatch_actions(const SupervisorActionList& actions, uint64_t now_ms) {
    size_t const COUNT = actions.count <= actions.entries.size() ? actions.count : actions.entries.size();
    for (size_t index = 0; index < COUNT && !runtime.failed; ++index) {
        dispatch_action(actions.entries.at(index), now_ms);
    }
    if (actions.overflowed && !runtime.failed) {
        mark_runtime_failed("action list overflow", SupervisorResult::ACTION_OVERFLOW);
    }
}

void poll_network_probes(uint64_t now_ms) {
    for (uint8_t service = 0; service < runtime.manifest.service_count; ++service) {
        auto& slot = runtime.services.at(service);
        if (slot.probe.socket_fd < 0 || slot.quiescence_reported) {
            continue;
        }
        NetworkProbeResult const RESULT = network_probe_poll(slot.probe);
        SupervisorEvent event{
            .service = service,
            .generation = slot.generation,
        };
        switch (RESULT) {
            case NetworkProbeResult::PENDING:
                event.kind = SupervisorEventKind::PROBE_PENDING;
                break;
            case NetworkProbeResult::READY:
                event.kind = SupervisorEventKind::PROBE_READY;
                break;
            case NetworkProbeResult::ERROR:
                event.kind = SupervisorEventKind::PROBE_ERROR;
                slot.last_error = errno != 0 ? errno : EIO;
                event.detail = slot.last_error;
                break;
        }
        (void)emit_model_event(event, now_ms);
    }
}

void reap_children(uint64_t now_ms) {
    for (size_t reaped = 0; reaped < MAX_REAPS_PER_TICK; ++reaped) {
        int32_t status = 0;
        int64_t const PID = ker::process::waitpid(-1, &status, WNOHANG, nullptr);
        if (PID <= 0) {
            if (PID < 0 && PID != -ECHILD && PID != -EINTR) {
                init_log::warn("init supervisor: waitpid(-1, WNOHANG) failed: %lld", static_cast<long long>(PID));
            }
            return;
        }

        uint8_t const SERVICE = runtime_service_for_pid(PID);
        if (SERVICE == INVALID_SERVICE_ID) {
            init_log::warn("init supervisor: reaped unknown/adopted child pid=%lld status=%d", static_cast<long long>(PID), status);
            continue;
        }

        auto& slot = runtime.services.at(SERVICE);
        slot.last_wait_status = status;
        slot.pid = -1;
        slot.leader_reaped = true;
        if (slot.cleanup_only) {
            close_fd(slot.exec_report_fd);
            close_service_output(SERVICE, true);
            network_probe_close(slot.probe);
            slot.pgid = -1;
            slot.exec_resolved = true;
            slot.output_complete = true;
            slot.quiescence_reported = true;
            slot.exit_pending = false;
            slot.cleanup_only = false;
            init_log::warn("init supervisor: reaped fenced setup-failure child pid=%lld status=%d", static_cast<long long>(PID), status);
            continue;
        }
        slot.exit_pending = true;
        process_exec_report(SERVICE, now_ms);
        deliver_pending_exit(SERVICE, now_ms);
    }
    init_log::warn("init supervisor: reap budget exhausted; remaining children will be handled next tick");
}

void drain_control_mailbox(uint64_t now_ms) {
    // At most MAILBOX_CAPACITY requests can predate this tick. The extra
    // receive observes -EAGAIN and proves that fixed batch was fully drained.
    for (uint32_t index = 0; index <= control_abi::MAILBOX_CAPACITY; ++index) {
        control_abi::Request request{};
        int64_t const RESULT = ker::process::init_control_receive(&request);
        if (RESULT == -EAGAIN) {
            return;
        }
        if (RESULT < 0) {
            init_log::warn("init supervisor: control receive failed: %lld", static_cast<long long>(RESULT));
            return;
        }

        uint8_t const SERVICE = runtime_service_for_name(request.service);
        if (!runtime.model_ready || runtime.failed || SERVICE == INVALID_SERVICE_ID) {
            init_log::warn("init supervisor: rejected control request %llu from pid %llu for unknown/unavailable service %s",
                           static_cast<unsigned long long>(request.request_id), static_cast<unsigned long long>(request.sender_pid),
                           request.service);
            continue;
        }

        SupervisorControl control{};
        switch (request.action) {
            case control_abi::Action::START:
                control = SupervisorControl::START;
                break;
            case control_abi::Action::STOP:
                control = SupervisorControl::STOP;
                break;
            case control_abi::Action::RESTART:
                control = SupervisorControl::RESTART;
                break;
            case control_abi::Action::INVALID:
                init_log::warn("init supervisor: rejected invalid control request %llu",
                               static_cast<unsigned long long>(request.request_id));
                continue;
        }

        SupervisorActionList actions{};
        SupervisorResult const CONTROL_RESULT = supervisor_control(runtime.model, now_ms, SERVICE, control, actions);
        if (CONTROL_RESULT != SupervisorResult::OK) {
            init_log::warn("init supervisor: control request %llu for %s rejected by model: %u",
                           static_cast<unsigned long long>(request.request_id), request.service, static_cast<unsigned>(CONTROL_RESULT));
            continue;
        }
        dispatch_actions(actions, now_ms);
    }
}

auto map_service_state(ServiceState state) -> control_abi::ServiceState {
    switch (state) {
        case ServiceState::WAITING:
            return control_abi::ServiceState::WAITING;
        case ServiceState::STARTING:
            return control_abi::ServiceState::STARTING;
        case ServiceState::RUNNING:
        case ServiceState::READY:
            return control_abi::ServiceState::RUNNING;
        case ServiceState::STOPPING:
            return control_abi::ServiceState::STOPPING;
        case ServiceState::EXITED:
            return control_abi::ServiceState::EXITED;
        case ServiceState::FAILED:
            return control_abi::ServiceState::FAILED;
        case ServiceState::BACKOFF:
            return control_abi::ServiceState::BACKOFF;
        case ServiceState::DISABLED:
            return control_abi::ServiceState::DISABLED;
    }
    return control_abi::ServiceState::UNKNOWN;
}

auto map_transition_reason(TransitionReason reason) -> control_abi::TransitionReason {
    switch (reason) {
        case TransitionReason::DEPENDENCIES_READY:
        case TransitionReason::DEPENDENCY_LOST:
            return control_abi::TransitionReason::DEPENDENCY;
        case TransitionReason::INITIAL_ENABLED:
        case TransitionReason::INITIAL_DISABLED:
        case TransitionReason::INITIAL_SKIPPED_SUCCESS:
        case TransitionReason::CONTROL_START:
            return control_abi::TransitionReason::START_REQUEST;
        case TransitionReason::SPAWN_FAILED:
        case TransitionReason::EXEC_SUCCEEDED:
        case TransitionReason::EXEC_FAILED:
            return control_abi::TransitionReason::SPAWN;
        case TransitionReason::READINESS_READY:
        case TransitionReason::READINESS_LOST:
            return control_abi::TransitionReason::READINESS;
        case TransitionReason::READINESS_TIMEOUT:
        case TransitionReason::START_TIMEOUT:
        case TransitionReason::STOP_TIMEOUT:
        case TransitionReason::DRAIN_TIMEOUT:
            return control_abi::TransitionReason::TIMEOUT;
        case TransitionReason::PROCESS_EXIT_SUCCESS:
        case TransitionReason::PROCESS_EXIT_FAILURE:
        case TransitionReason::QUIESCED:
            return control_abi::TransitionReason::EXIT;
        case TransitionReason::STOP_REQUESTED:
        case TransitionReason::STOPPED:
            return control_abi::TransitionReason::STOP_REQUEST;
        case TransitionReason::RESTART_BACKOFF:
        case TransitionReason::BACKOFF_EXPIRED:
        case TransitionReason::RESTART_BUDGET_EXHAUSTED:
        case TransitionReason::STABLE_RUN_RESET:
        case TransitionReason::CONTROL_RESTART:
            return control_abi::TransitionReason::RESTART;
        case TransitionReason::SHUTDOWN:
            return control_abi::TransitionReason::SHUTDOWN;
    }
    return control_abi::TransitionReason::NONE;
}

auto service_deadline_ms(const ServiceLifecycle& lifecycle) -> uint64_t {
    uint64_t deadline = SUPERVISOR_NO_DEADLINE;
    auto consider = [&](uint64_t value) {
        if (value != SUPERVISOR_NO_DEADLINE && value < deadline) {
            deadline = value;
        }
    };
    consider(lifecycle.state_deadline_ms);
    consider(lifecycle.drain_deadline_ms);
    consider(lifecycle.restart_deadline_ms);
    return deadline;
}

void copy_service_name(char* destination, size_t capacity, std::string_view name) {
    size_t const COUNT = name.size() < capacity - 1 ? name.size() : capacity - 1;
    if (COUNT != 0) {
        std::memcpy(destination, name.data(), COUNT);
    }
    destination[COUNT] = '\0';
}

void append_status_history(uint8_t service, control_abi::ServiceStatus& status) {
    std::array<const SupervisorTransition*, control_abi::MAX_TRANSITION_HISTORY> recent{};
    size_t count = 0;
    size_t const HISTORY_SIZE = supervisor_history_size(runtime.model);
    for (size_t index = 0; index < HISTORY_SIZE; ++index) {
        auto const* transition = supervisor_history_at(runtime.model, index);
        if (transition == nullptr || transition->service != service) {
            continue;
        }
        if (count < recent.size()) {
            recent.at(count++) = transition;
        } else {
            for (size_t move = 1; move < recent.size(); ++move) {
                recent.at(move - 1) = recent.at(move);
            }
            recent.back() = transition;
        }
    }

    status.history_count = static_cast<uint32_t>(count);
    for (size_t index = 0; index < count; ++index) {
        auto const& source = *recent.at(index);
        status.history[index] = {
            .timestamp_mono_ns = to_nanoseconds(source.now_ms),
            .from_state = map_service_state(source.from),
            .to_state = map_service_state(source.to),
            .reason = map_transition_reason(source.reason),
            .reserved0 = 0,
        };
    }
}

auto supervisor_status_state() -> control_abi::SupervisorState {
    if (runtime.failed || !runtime.model_ready) {
        return control_abi::SupervisorState::FAILED;
    }
    if (runtime.model.shutdown_phase == SupervisorShutdownPhase::COMPLETE || runtime.shutdown_complete) {
        return control_abi::SupervisorState::STOPPED;
    }
    if (runtime.model.shutdown_phase != SupervisorShutdownPhase::NONE) {
        return control_abi::SupervisorState::STOPPING;
    }

    bool starting = false;
    bool failed = false;
    for (uint8_t service = 0; service < runtime.manifest.service_count; ++service) {
        auto const& lifecycle = runtime.model.services.at(service);
        failed = failed || lifecycle.state == ServiceState::FAILED;
        if (!lifecycle.desired_running) {
            continue;
        }
        starting = starting || lifecycle.state == ServiceState::WAITING || lifecycle.state == ServiceState::STARTING ||
                   lifecycle.state == ServiceState::RUNNING || lifecycle.state == ServiceState::BACKOFF;
    }
    if (failed) {
        return control_abi::SupervisorState::FAILED;
    }
    return starting ? control_abi::SupervisorState::STARTING : control_abi::SupervisorState::RUNNING;
}

void publish_status(uint64_t now_ms) {
    control_abi::StatusSnapshot snapshot{};
    snapshot.size = sizeof(snapshot);
    snapshot.version = control_abi::ABI_VERSION;
    snapshot.state = supervisor_status_state();
    snapshot.flags =
        runtime.model_ready && runtime.model.shutdown_phase != SupervisorShutdownPhase::NONE ? control_abi::SNAPSHOT_FLAG_SHUTTING_DOWN : 0;
    snapshot.sequence = 0;
    snapshot.published_mono_ns = to_nanoseconds(now_ms);

    if (runtime.model_ready) {
        snapshot.service_count = runtime.manifest.service_count;
        for (uint8_t service = 0; service < runtime.manifest.service_count; ++service) {
            auto const& spec = runtime.manifest.services.at(service);
            auto const& lifecycle = runtime.model.services.at(service);
            auto const& slot = runtime.services.at(service);
            auto& status = snapshot.services[service];
            copy_service_name(status.service, sizeof(status.service), spec.name.view());
            status.state = map_service_state(lifecycle.state);
            if (lifecycle.state == ServiceState::READY || lifecycle.completion_success) {
                status.readiness = control_abi::ReadinessState::READY;
            } else if (lifecycle.state == ServiceState::FAILED) {
                status.readiness = control_abi::ReadinessState::FAILED;
            } else {
                status.readiness = control_abi::ReadinessState::NOT_READY;
            }
            if (lifecycle.condition_met) {
                status.flags |= control_abi::SERVICE_FLAG_ENABLED;
            }
            if (spec.enablement == EnablementKind::ALWAYS) {
                status.flags |= control_abi::SERVICE_FLAG_REQUIRED;
            }
            if (lifecycle.restart_scheduled) {
                status.flags |= control_abi::SERVICE_FLAG_RESTART_PENDING;
            }
            status.pid = lifecycle.pid;
            status.pgid = lifecycle.pgid;
            status.generation = lifecycle.generation;
            status.restart_count = lifecycle.spawn_attempts > 0 ? lifecycle.spawn_attempts - 1 : 0;
            status.last_wait_status = slot.last_wait_status;
            status.last_error = slot.last_error;
            status.state_since_mono_ns = to_nanoseconds(lifecycle.state_since_ms);
            uint64_t const DEADLINE = service_deadline_ms(lifecycle);
            status.deadline_mono_ns = DEADLINE == SUPERVISOR_NO_DEADLINE ? 0 : to_nanoseconds(DEADLINE);
            append_status_history(service, status);
        }
    }

    int64_t const RESULT = ker::process::init_status_publish(&snapshot);
    if (RESULT < 0 && RESULT != runtime.last_status_publish_error) {
        init_log::warn("init supervisor: status publish failed: %lld", static_cast<long long>(RESULT));
    }
    runtime.last_status_publish_error = RESULT;
}

auto read_manifest(size_t& size) -> bool {
    size = 0;
    int const FD = ::open(SERVICE_MANIFEST_PATH, O_RDONLY | O_CLOEXEC);
    if (FD < 0) {
        init_log::critical("init supervisor: unable to open %s: errno=%d", SERVICE_MANIFEST_PATH, errno);
        return false;
    }

    while (size < runtime.manifest_bytes.size()) {
        ssize_t const READ = ::read(FD, runtime.manifest_bytes.data() + size, runtime.manifest_bytes.size() - size);
        if (READ > 0) {
            size += static_cast<size_t>(READ);
            continue;
        }
        if (READ == 0) {
            (void)::close(FD);
            return true;
        }
        if (errno == EINTR) {
            continue;
        }
        int const ERROR = errno;
        (void)::close(FD);
        init_log::critical("init supervisor: failed reading %s: errno=%d", SERVICE_MANIFEST_PATH, ERROR);
        return false;
    }

    char extra = '\0';
    ssize_t read_extra = 0;
    do {
        read_extra = ::read(FD, &extra, 1);
    } while (read_extra < 0 && errno == EINTR);
    int const ERROR = errno;
    (void)::close(FD);
    if (read_extra > 0) {
        init_log::critical("init supervisor: %s exceeds %llu-byte manifest limit", SERVICE_MANIFEST_PATH,
                           static_cast<unsigned long long>(runtime.manifest_bytes.size()));
        return false;
    }
    if (read_extra < 0) {
        init_log::critical("init supervisor: failed checking %s size: errno=%d", SERVICE_MANIFEST_PATH, ERROR);
        return false;
    }
    return true;
}

auto resolve_enablement(const ServiceSpec& service) -> EnablementResult {
    switch (service.enablement) {
        case EnablementKind::ALWAYS:
            return EnablementResult::CONDITION_MET;
        case EnablementKind::DISABLED:
            return EnablementResult::CONDITION_NOT_MET;
        case EnablementKind::PATH_EXISTS:
        case EnablementKind::PATH_MISSING: {
            struct stat status{};
            bool const EXISTS = ::stat(service.enablement_path.c_str(), &status) == 0;
            if (!EXISTS && errno != ENOENT) {
                init_log::warn("init supervisor: enablement probe failed for %s path %s: errno=%d", service.name.c_str(),
                               service.enablement_path.c_str(), errno);
                return EnablementResult::CONDITION_NOT_MET;
            }
            bool const MET = service.enablement == EnablementKind::PATH_EXISTS ? EXISTS : !EXISTS;
            return MET ? EnablementResult::CONDITION_MET : EnablementResult::CONDITION_NOT_MET;
        }
    }
    return EnablementResult::CONDITION_NOT_MET;
}

void advance_model(uint64_t now_ms) {
    if (!runtime.model_ready || runtime.failed) {
        return;
    }
    SupervisorActionList actions{};
    SupervisorResult const RESULT = supervisor_advance(runtime.model, now_ms, actions);
    if (RESULT != SupervisorResult::OK) {
        mark_runtime_failed("advance", RESULT);
        return;
    }
    dispatch_actions(actions, now_ms);
}

void force_stop_phase(bool journal_phase) {
    if (!runtime.model_ready) {
        return;
    }
    for (uint8_t service = 0; service < runtime.manifest.service_count; ++service) {
        bool const IS_JOURNAL = service == runtime.topology.journal_service;
        if (IS_JOURNAL != journal_phase) {
            continue;
        }
        auto& slot = runtime.services.at(service);
        SupervisorAction const ACTION{
            .kind = SupervisorActionKind::SEND_KILL,
            .service = service,
            .generation = slot.generation,
        };
        signal_service_group(ACTION, SIGKILL);
        network_probe_close(slot.probe);
        close_service_output(service, true);
        close_fd(slot.exec_report_fd);
        slot.exec_resolved = true;
    }
}

auto shutdown_phase_budget_ms(bool journal_phase) -> uint64_t {
    uint64_t budget = SHUTDOWN_PHASE_SLACK_MS;
    if (!runtime.model_ready) {
        return FAILED_RUNTIME_SHUTDOWN_MS;
    }
    for (uint8_t service = 0; service < runtime.manifest.service_count; ++service) {
        bool const IS_JOURNAL = service == runtime.topology.journal_service;
        if (IS_JOURNAL != journal_phase) {
            continue;
        }
        auto const& spec = runtime.manifest.services.at(service);
        budget = saturating_add(budget, spec.stop_term_ms);
        budget = saturating_add(budget, spec.stop_kill_ms);
        budget = saturating_add(budget, spec.drain_timeout_ms);
        budget = saturating_add(budget, static_cast<uint64_t>(SERVICE_SUPERVISOR_TICK_MS) * 2ULL);
    }
    return budget;
}

void wait_for_shutdown_phase(bool journal_phase) {
    bool const* const COMPLETE = journal_phase ? &runtime.shutdown_complete : &runtime.non_journal_shutdown_complete;
    uint64_t const DEADLINE = saturating_add(runtime.now_ms, shutdown_phase_budget_ms(journal_phase));
    while (!*COMPLETE && !runtime.failed && runtime.now_ms < DEADLINE) {
        service_supervisor_tick();
        if (!*COMPLETE) {
            sleep_one_tick();
        }
    }
    if (*COMPLETE) {
        return;
    }

    init_log::critical("init supervisor: %s shutdown phase exceeded bounded deadline; forcing remaining groups closed",
                       journal_phase ? "journal" : "service");
    force_stop_phase(journal_phase);
    uint64_t const FORCED_DEADLINE = saturating_add(runtime.now_ms, FAILED_RUNTIME_SHUTDOWN_MS);
    while (!*COMPLETE && runtime.now_ms < FORCED_DEADLINE) {
        service_supervisor_tick();
        if (!*COMPLETE) {
            sleep_one_tick();
        }
    }
}

}  // namespace

auto start_service_supervisor() -> bool {
    if (runtime.start_attempted) {
        return runtime.model_ready && !runtime.failed;
    }
    runtime.start_attempted = true;
    runtime.now_ms = read_monotonic_ms(0);
    if (!restore_init_wki_target()) {
        runtime.failed = true;
        init_log::critical("init supervisor: unable to establish PID 1 LOCAL|NOINHERIT WKI target; failing closed");
        publish_status(runtime.now_ms);
        return false;
    }

    size_t manifest_size = 0;
    if (!read_manifest(manifest_size)) {
        runtime.failed = true;
        publish_status(runtime.now_ms);
        return false;
    }

    ManifestParseResult const PARSE =
        parse_service_manifest(std::span<const char>(runtime.manifest_bytes.data(), manifest_size), runtime.manifest);
    if (!PARSE) {
        runtime.failed = true;
        init_log::critical("init supervisor: manifest parse failed: error=%u line=%llu", static_cast<unsigned>(PARSE.error),
                           static_cast<unsigned long long>(PARSE.line));
        publish_status(runtime.now_ms);
        return false;
    }
    ManifestValidationResult const VALIDATION = validate_service_manifest(runtime.manifest, runtime.topology);
    if (!VALIDATION) {
        runtime.failed = true;
        init_log::critical("init supervisor: manifest validation failed: error=%u service=%u related=%u",
                           static_cast<unsigned>(VALIDATION.error), static_cast<unsigned>(VALIDATION.service),
                           static_cast<unsigned>(VALIDATION.related_service));
        publish_status(runtime.now_ms);
        return false;
    }

    for (uint8_t service = 0; service < runtime.manifest.service_count; ++service) {
        runtime.enablement.at(service) = resolve_enablement(runtime.manifest.services.at(service));
    }
    SupervisorResult const INITIALIZED =
        initialize_supervisor(runtime.model, runtime.manifest, runtime.topology,
                              std::span<const EnablementResult>(runtime.enablement.data(), runtime.manifest.service_count), runtime.now_ms);
    if (INITIALIZED != SupervisorResult::OK) {
        runtime.failed = true;
        init_log::critical("init supervisor: model initialization failed: %u", static_cast<unsigned>(INITIALIZED));
        publish_status(runtime.now_ms);
        return false;
    }

    runtime.model_ready = true;
    init_log::info("init supervisor: loaded %u services from %s", static_cast<unsigned>(runtime.manifest.service_count),
                   SERVICE_MANIFEST_PATH);
    advance_model(runtime.now_ms);
    publish_status(runtime.now_ms);
    return !runtime.failed;
}

void service_supervisor_tick() {
    uint64_t const NOW_MS = advance_runtime_clock();
    if (!runtime.init_wki_target_valid) {
        (void)restore_init_wki_target();
    }

    // Exec reports are ordered before waitpid so an immediately exiting child
    // always delivers EXEC_* before EXITED to the model.
    for (uint8_t service = 0; service < runtime.manifest.service_count; ++service) {
        process_exec_report(service, NOW_MS);
    }
    for (uint8_t service = 0; service < runtime.manifest.service_count; ++service) {
        drain_service_output(service);
    }
    poll_network_probes(NOW_MS);
    reap_children(NOW_MS);
    for (uint8_t service = 0; service < runtime.manifest.service_count; ++service) {
        deliver_pending_exit(service, NOW_MS);
        maybe_report_quiescence(service, NOW_MS);
    }
    drain_control_mailbox(NOW_MS);
    advance_model(NOW_MS);
    publish_status(NOW_MS);
}

void stop_services_for_shutdown() {
    if (!runtime.model_ready) {
        return;
    }
    SupervisorActionList actions{};
    SupervisorResult const RESULT = supervisor_begin_shutdown(runtime.model, advance_runtime_clock(), actions);
    if (RESULT != SupervisorResult::OK) {
        mark_runtime_failed("begin shutdown", RESULT);
        force_stop_phase(false);
    } else {
        dispatch_actions(actions, runtime.now_ms);
    }
    wait_for_shutdown_phase(false);
}

void stop_journald_for_shutdown() {
    if (!runtime.model_ready) {
        return;
    }
    SupervisorActionList actions{};
    SupervisorResult const RESULT = supervisor_begin_journal_shutdown(runtime.model, advance_runtime_clock(), actions);
    if (RESULT != SupervisorResult::OK) {
        mark_runtime_failed("begin journal shutdown", RESULT);
        force_stop_phase(true);
    } else {
        dispatch_actions(actions, runtime.now_ms);
    }
    wait_for_shutdown_phase(true);
}
