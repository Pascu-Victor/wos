#include "perf.hpp"
namespace perf {
namespace {
// NOLINTNEXTLINE(misc-include-cleaner): WOS signal constants are provided by signal.h.
constexpr std::array<int, 4> PERF_RUN_FORWARD_SIGNALS{SIGINT, SIGTERM, SIGHUP, SIGQUIT};
volatile sig_atomic_t g_perf_run_signal = 0;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

using SignalHandler = void (*)(int);

struct PerfRunSignalHandlers {
    std::array<SignalHandler, PERF_RUN_FORWARD_SIGNALS.size()> previous{};
    bool installed = false;
};

void handle_perf_run_signal(int signum) {
    if (g_perf_run_signal == 0) {
        g_perf_run_signal = signum;
    }
}

void install_perf_run_signal_handlers(PerfRunSignalHandlers& handlers) {
    g_perf_run_signal = 0;
    for (std::size_t i = 0; i < PERF_RUN_FORWARD_SIGNALS.size(); ++i) {
        handlers.previous.at(i) = ::signal(PERF_RUN_FORWARD_SIGNALS.at(i), handle_perf_run_signal);
    }
    handlers.installed = true;
}

void restore_perf_run_signal_handlers(PerfRunSignalHandlers& handlers) {
    if (!handlers.installed) {
        return;
    }
    for (std::size_t i = 0; i < PERF_RUN_FORWARD_SIGNALS.size(); ++i) {
        if (handlers.previous.at(i) != SIG_ERR) {
            ::signal(PERF_RUN_FORWARD_SIGNALS.at(i), handlers.previous.at(i));
        }
    }
    handlers.installed = false;
    g_perf_run_signal = 0;
}

auto consume_perf_run_signal() -> int {
    int const SIGNUM = static_cast<int>(g_perf_run_signal);
    g_perf_run_signal = 0;
    return SIGNUM;
}

// Resolve a command name to a full path by searching PATH.
// If cmd already contains '/', returns it as-is (if accessible).
// Returns empty string if not found.
auto resolve_command(const char* cmd) -> std::string {
    if (cmd == nullptr || cmd[0] == '\0') {
        return {};
    }

    // If cmd contains '/', treat as a path (absolute or relative)
    if (std::strchr(cmd, '/') != nullptr) {
        if (access(cmd, X_OK) == 0) {
            return {cmd};
        }
        return {};
    }

    // Search PATH
    const char* path_env = getenv("PATH");
    if (path_env == nullptr) {
        path_env = "/bin";
    }

    std::string_view const PATH_SV(path_env);
    std::size_t start = 0;
    while (start <= PATH_SV.size()) {
        auto colon = PATH_SV.find(':', start);
        auto dir = PATH_SV.substr(start, colon == std::string_view::npos ? std::string_view::npos : colon - start);
        if (dir.empty()) {
            dir = ".";
        }

        std::string candidate;
        candidate.reserve(dir.size() + 1 + std::strlen(cmd));
        candidate.append(dir);
        candidate.push_back('/');
        candidate.append(cmd);

        if (access(candidate.c_str(), X_OK) == 0) {
            return candidate;
        }

        if (colon == std::string_view::npos) {
            break;
        }
        start = colon + 1;
    }
    return {};
}

// Read the shebang line from a script file. Returns the interpreter path,
// or empty string if the file is not a script.
auto read_shebang(const char* path) -> std::string {
    int const FD = open(path, O_RDONLY);
    if (FD < 0) {
        return {};
    }
    std::array<char, 256> hdr{};
    ssize_t const N = read(FD, hdr.data(), hdr.size() - 1);
    close(FD);
    if (N < 3 || hdr.at(0) != '#' || hdr.at(1) != '!') {
        return {};
    }
    hdr.at(static_cast<size_t>(N)) = '\0';
    // Find end of first line
    char* nl = std::strchr(hdr.data() + 2, '\n');
    if (nl != nullptr) {
        *nl = '\0';
    }
    // Skip whitespace after #!
    const char* p = hdr.data() + 2;
    while (*p == ' ' || *p == '\t') {
        p++;
    }
    if (*p == '\0') {
        return {};
    }
    // Take only the interpreter path (stop at first space for args)
    const char* end = p;
    while (*end != '\0' && *end != ' ' && *end != '\t') {
        end++;
    }
    return {p, end};
}

}  // namespace

void cmd_run(int argc, char** argv) {
    if (argc < 1) {
        std::println("perf run: no command specified");
        return;
    }

    // Parse --filter=... option (strip from argv before exec)
    const char* filter = "switch,wake,sleep";  // default: no container spam
    WkiDisplayOptions display_options{};
    std::vector<char*> cmd_argv;
    for (int i = 0; i < argc; i++) {
        std::string_view const ARG(argv[i]);
        if (ARG.starts_with("--filter=")) {
            filter = argv[i] + 9;
        } else if (parse_display_arg(ARG, display_options)) {
            continue;
        } else {
            cmd_argv.push_back(argv[i]);
        }
    }
    argc = static_cast<int>(cmd_argv.size());
    argv = cmd_argv.data();

    if (argc < 1) {
        std::println("perf run: no command specified");
        return;
    }

    // Resolve the command to a full path (PATH search)
    std::string resolved = resolve_command(argv[0]);
    if (resolved.empty()) {
        std::println("perf run: command not found: {}", argv[0]);
        return;
    }

    // Check if this is a script with a shebang - if so, wrap with the interpreter
    std::string interp = read_shebang(resolved.c_str());
    std::vector<char*> new_argv;
    if (!interp.empty()) {
        // Resolve the interpreter too
        std::string interp_resolved = resolve_command(interp.c_str());
        if (interp_resolved.empty()) {
            std::println("perf run: interpreter not found: {} (from shebang in {})", interp, resolved);
            return;
        }
        interp = std::move(interp_resolved);

        // Build new argv: [interpreter, script_path, original_args[1:]]
        new_argv.push_back(interp.data());
        new_argv.push_back(resolved.data());
        for (int i = 1; i < argc; i++) {
            new_argv.push_back(argv[i]);
        }
        new_argv.push_back(nullptr);
    } else {
        // ELF binary - use resolved path as argv[0], keep rest
        new_argv.push_back(resolved.data());
        for (int i = 1; i < argc; i++) {
            new_argv.push_back(argv[i]);
        }
        new_argv.push_back(nullptr);
    }

    char* const* exec_argv = new_argv.data();
    const char* exec_path = exec_argv[0];

    auto before = collect_main_stats();
    int64_t const START_MS = now_ms();
    set_recording_enabled(true, filter);

    int64_t child_pid = ker::process::fork();
    if (child_pid == 0) {
        ker::process::setpgid(0, 0);
        const auto* ea = const_cast<const char* const*>(exec_argv);
        const auto* exec_envp = const_cast<const char* const*>(environ);
        ker::process::execve(exec_path, ea, exec_envp);
        // If we get here, exec failed
        std::println("perf run: exec failed for '{}'", exec_path);
        _exit(EXEC_FAILURE_EXIT_CODE);
    }

    if (child_pid < 0) {
        std::println("perf run: fork failed ({})", child_pid);
        set_recording_enabled(false);
        return;
    }

    ker::process::setpgid(child_pid, child_pid);
    int64_t target_pgid = child_pid;
    std::println("perf run: tracing pgid={} cmd={}", target_pgid, exec_path);

    PerfRunSignalHandlers signal_handlers{};
    install_perf_run_signal_handlers(signal_handlers);

    std::vector<TrackedProc> tracked;
    auto upsert_tracked = [&](const StatInfo& stat) {
        for (auto& proc : tracked) {
            if (proc.pid == stat.pid) {
                proc.last_utime = stat.utime;
                proc.last_stime = stat.stime;
                proc.comm = stat.comm;
                return;
            }
        }

        tracked.push_back(TrackedProc{
            .pid = stat.pid,
            .comm = stat.comm,
            .cmdline = read_cmdline(stat.pid),
            .last_utime = stat.utime,
            .last_stime = stat.stime,
        });
    };

    ScopedFd data_fd = open_write_trunc(PERF_DATA_FILE);
    if (data_fd.valid()) {
        write_section_timebase(data_fd.get());
        write_all(data_fd.get(), SECTION_EVENTS);
    }

    int32_t status = 0;
    int64_t last_drain_ms = now_ms();
    ssize_t total_event_bytes = 0;
    bool command_exited = false;
    bool last_group_alive = false;
    int64_t last_proc_scan_ms = 0;
    bool cancel_forwarded = false;
    bool cancel_escalated = false;
    int64_t cancel_forwarded_ms = 0;

    auto scan_target_group = [&]() {
        bool any_alive = false;
        for (const auto& stat : collect_main_stats()) {
            if (std::cmp_equal(stat.pgid, target_pgid)) {
                upsert_tracked(stat);
                if (stat.state != EXITED_STATE) {
                    any_alive = true;
                }
            }
        }
        last_group_alive = any_alive;
        last_proc_scan_ms = now_ms();
    };

    scan_target_group();

    for (;;) {
        for (;;) {
            int64_t const REAPED = waitpid_nohang(-1, &status);
            if (REAPED <= 0) {
                break;
            }
            if (REAPED == child_pid) {
                command_exited = true;
            }
        }

        int64_t const LOOP_NOW_MS = now_ms();
        int const FORWARD_SIGNAL = consume_perf_run_signal();
        if (FORWARD_SIGNAL != 0 && !command_exited) {
            if (!cancel_forwarded) {
                std::println("perf run: forwarding signal {} to pgid {}", FORWARD_SIGNAL, target_pgid);
                (void)ker::process::kill(-target_pgid, FORWARD_SIGNAL);
                cancel_forwarded = true;
                cancel_forwarded_ms = LOOP_NOW_MS;
            } else if (!cancel_escalated) {
                std::println("perf run: traced group still alive after cancel; sending SIGKILL to pgid {}", target_pgid);
                (void)ker::process::kill(-target_pgid, SIGKILL);  // NOLINT(misc-include-cleaner)
                cancel_escalated = true;
            }
        }
        if (command_exited || LOOP_NOW_MS - last_proc_scan_ms >= PERF_RUN_PROC_SCAN_INTERVAL_MS) {
            scan_target_group();
        }

        if (cancel_forwarded && !cancel_escalated && last_group_alive &&
            LOOP_NOW_MS - cancel_forwarded_ms >= PERF_RUN_CANCEL_KILL_AFTER_MS) {
            std::println("perf run: traced group still alive after cancel; sending SIGKILL to pgid {}", target_pgid);
            (void)ker::process::kill(-target_pgid, SIGKILL);  // NOLINT(misc-include-cleaner)
            cancel_escalated = true;
        }

        if (command_exited && !last_group_alive) {
            break;
        }

        wall_sleep_ms(SLEEP_SLICE_MS);
        if (data_fd.valid() && now_ms() - last_drain_ms >= DRAIN_INTERVAL_MS) {
            auto events = read_file(KPERF_PATH, PERF_DRAIN_CAPACITY);
            if (events.has_value() && !events->empty()) {
                write_all(data_fd.get(), *events);
                total_event_bytes += static_cast<ssize_t>(events->size());
            }
            last_drain_ms = now_ms();
        }
    }

    while (waitpid_nohang(-1, &status) > 0) {
    }

    int64_t elapsed_ms = now_ms() - START_MS;
    elapsed_ms = std::max<int64_t>(elapsed_ms, 1);

    set_recording_enabled(false);
    restore_perf_run_signal_handlers(signal_handlers);
    auto after = collect_main_stats();
    std::vector<CpuRow> rows;

    for (const auto& proc : tracked) {
        uint64_t const TICKS_AFTER = proc.last_utime + proc.last_stime;
        uint64_t ticks_before = 0;
        for (const auto& prior : before) {
            if (prior.pid == proc.pid) {
                ticks_before = prior.utime + prior.stime;
                break;
            }
        }

        uint64_t const DELTA = TICKS_AFTER >= ticks_before ? TICKS_AFTER - ticks_before : TICKS_AFTER;
        double const CPU = static_cast<double>(DELTA) * static_cast<double>(MILLISECONDS_PER_SECOND) / static_cast<double>(elapsed_ms);
        if (CPU < MIN_CPU_PCT) {
            continue;
        }

        char state = EXITED_STATE;
        for (const auto& current : after) {
            if (current.pid == proc.pid) {
                state = current.state;
                break;
            }
        }

        rows.push_back(CpuRow{
            .pid = proc.pid,
            .comm = proc.comm,
            .state = state,
            .cpu_pct = CPU,
            .in_group = true,
        });
    }

    for (const auto& prior : before) {
        bool was_tracked = false;
        for (const auto& proc : tracked) {
            if (proc.pid == prior.pid) {
                was_tracked = true;
                break;
            }
        }
        if (was_tracked) {
            continue;
        }

        for (const auto& current : after) {
            if (current.pid != prior.pid) {
                continue;
            }

            uint64_t const DELTA = (current.utime + current.stime) - (prior.utime + prior.stime);
            double const CPU = static_cast<double>(DELTA) * static_cast<double>(MILLISECONDS_PER_SECOND) / static_cast<double>(elapsed_ms);
            if (CPU >= MIN_CPU_PCT) {
                rows.push_back(CpuRow{
                    .pid = current.pid,
                    .comm = current.comm,
                    .state = current.state,
                    .cpu_pct = CPU,
                    .in_group = false,
                });
            }
            break;
        }
    }

    std::ranges::sort(rows, [](const CpuRow& lhs, const CpuRow& rhs) { return lhs.cpu_pct > rhs.cpu_pct; });

    std::println("=== perf run stat (actual {}ms) =====================================", elapsed_ms);
    std::println("{:>6}  {:<20}  {:>5}  {:>7}  {}", "PID", "NAME", "STATE", "CPU%", "GROUP");
    std::println("{:->6}  {:->20}  {:->5}  {:->7}  {:->5}", "", "", "", "", "");
    for (const auto& row : rows) {
        std::println("{:>6}  {:<20}  {:>5}  {:>6.1f}%  {}", row.pid, row.comm, row.state, row.cpu_pct, row.in_group ? "*" : "");
    }
    std::println("");

    if (data_fd.valid()) {
        auto events = read_file(KPERF_PATH, PERF_DRAIN_CAPACITY);
        if (events.has_value() && !events->empty()) {
            write_all(data_fd.get(), *events);
            total_event_bytes += static_cast<ssize_t>(events->size());
        }

        write_all(data_fd.get(), SECTION_EVENTS_END);
        ssize_t const SUMMARY_BYTES = write_section_wki_summary(data_fd.get());
        ssize_t const IPC_BYTES = write_section_ipc_stats(data_fd.get());
        ssize_t const CONTSTAT_BYTES = write_section_contstat(data_fd.get());
        ssize_t const MEMACC_BYTES = write_section_memacc_alloc_totals(data_fd.get());
        ssize_t const DIAG_BYTES = CONTSTAT_BYTES + MEMACC_BYTES;
        write_section_peer_map(data_fd.get());
        write_all(data_fd.get(), SECTION_PROC_MAP);

        for (const auto& stat : collect_main_stats()) {
            write_all(data_fd.get(), proc_map_line(stat.pid, stat.comm, read_cmdline(stat.pid)));
        }

        for (const auto& proc : tracked) {
            bool alive = false;
            for (const auto& current : after) {
                if (current.pid == proc.pid) {
                    alive = true;
                    break;
                }
            }
            if (!alive) {
                write_all(data_fd.get(), proc_map_line(proc.pid, proc.comm, proc.cmdline));
            }
        }

        write_all(data_fd.get(), SECTION_PROC_MAP_END);

        if (total_event_bytes <= 0 && SUMMARY_BYTES <= 0 && IPC_BYTES <= 0 && DIAG_BYTES <= 0) {
            std::println("perf: ring buffer empty - PROC_MAP saved, no events");
        } else {
            std::println("perf: saved to {} ({} event bytes, {} summary bytes, {} IPC bytes, {} diag bytes)", PERF_DATA_FILE,
                         total_event_bytes, SUMMARY_BYTES, IPC_BYTES, DIAG_BYTES);
        }
    } else {
        std::println("perf: cannot write {}", PERF_DATA_FILE);
    }

    cmd_sched(DEFAULT_MAX_EVENTS, display_options);
}

}  // namespace perf
