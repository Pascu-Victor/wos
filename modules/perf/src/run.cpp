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

auto cmd_run(int argc, char** argv) -> bool {
    if (argc < 1) {
        std::println("perf run: no command specified");
        return false;
    }

    // Parse --filter=... option (strip from argv before exec)
    const char* filter = "switch,wake,sleep";  // default: no container spam
    bool structured = false;
    bool observer_stages = false;
    bool options_done = false;
    WkiDisplayOptions display_options{};
    std::vector<char*> cmd_argv;
    for (int i = 0; i < argc; i++) {
        std::string_view const ARG(argv[i]);
        if (options_done) {
            cmd_argv.push_back(argv[i]);
        } else if (ARG == "--") {
            options_done = true;
        } else if (ARG.starts_with("--filter=")) {
            filter = argv[i] + 9;
        } else if (ARG == "--structured") {
            structured = true;
        } else if (ARG == "--observer-stages") {
            observer_stages = true;
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
        return false;
    }

    // Resolve the command to a full path (PATH search)
    std::string resolved = resolve_command(argv[0]);
    if (resolved.empty()) {
        std::println("perf run: command not found: {}", argv[0]);
        return false;
    }

    // Check if this is a script with a shebang - if so, wrap with the interpreter
    std::string interp = read_shebang(resolved.c_str());
    std::vector<char*> new_argv;
    if (!interp.empty()) {
        // Resolve the interpreter too
        std::string interp_resolved = resolve_command(interp.c_str());
        if (interp_resolved.empty()) {
            std::println("perf run: interpreter not found: {} (from shebang in {})", interp, resolved);
            return false;
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
    auto capture = begin_capture(filter);
    if (capture.control_failed) {
        std::println("perf run: cannot enable /proc/kperfctl");
        return false;
    }
    if (observer_stages) {
        auto& trace = capture.run_observer.emplace();
        trace.parent_pid = static_cast<uint64_t>(getpid());
        trace.spans.reserve(RUN_OBSERVER_SPAN_LIMIT);
    }
    auto observe = [&](std::optional<RunObserverPhase> next, std::optional<int64_t> result = std::nullopt) {
        if (!capture.run_observer) return;
        auto& trace = *capture.run_observer;
        auto sample = trace.spans.size() < RUN_OBSERVER_SPAN_LIMIT ? read_clock_ns(CLOCK_MONOTONIC) : std::nullopt;
        advance_run_observer(trace, next, sample, result);
    };

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
        return false;
    }

    if (capture.run_observer) capture.run_observer->child_pid = static_cast<uint64_t>(child_pid);
    observe(RunObserverPhase::PARENT_SETUP);
    ker::process::setpgid(child_pid, child_pid);
    int64_t target_pgid = child_pid;
    std::println("perf run: tracing pgid={} cmd={}", target_pgid, exec_path);

    PerfRunSignalHandlers signal_handlers{};
    install_perf_run_signal_handlers(signal_handlers);

    std::vector<TrackedProc> tracked;
    auto scan_stats = [&](RunObserverPhase scan_phase) {
        observe(RunObserverPhase::PROC_STATS);
        auto stats = collect_main_stats();
        observe(scan_phase);
        return stats;
    };
    auto scan_cmdline = [&](uint64_t pid, RunObserverPhase scan_phase) {
        observe(RunObserverPhase::PROC_CMDLINE);
        auto cmdline = read_cmdline(pid);
        observe(scan_phase);
        return cmdline;
    };
    auto upsert_tracked = [&](const StatInfo& stat, RunObserverPhase scan_phase) {
        observe(RunObserverPhase::PROC_IMAGES);
        std::string image_map = read_proc_images(stat.pid);
        observe(scan_phase);
        for (auto& proc : tracked) {
            if (proc.pid == stat.pid) {
                proc.last_utime = stat.utime;
                proc.last_stime = stat.stime;
                proc.comm = stat.comm;
                if (!image_map.empty()) {
                    proc.image_map = std::move(image_map);
                }
                return;
            }
        }

        tracked.push_back(TrackedProc{
            .pid = stat.pid,
            .comm = stat.comm,
            .cmdline = scan_cmdline(stat.pid, scan_phase),
            .image_map = std::move(image_map),
            .last_utime = stat.utime,
            .last_stime = stat.stime,
        });
    };

    observe(RunObserverPhase::DATA_OPEN);
    ScopedFd data_fd = open_write_trunc(PERF_DATA_FILE);
    bool output_ok = data_fd.valid();
    observe(RunObserverPhase::TIMEBASE_WRITE);
    if (data_fd.valid()) {
        capture.write_failed = !write_section_timebase(data_fd.get());
        capture.write_failed = !write_all(data_fd.get(), SECTION_EVENTS) || capture.write_failed;
    }

    int32_t status = 0;
    int64_t last_drain_ms = now_ms();

    bool command_exited = false;
    bool last_group_alive = false;
    int64_t last_proc_scan_ms = 0;
    bool cancel_forwarded = false;
    bool cancel_escalated = false;
    int64_t cancel_forwarded_ms = 0;

    auto scan_target_group = [&](RunObserverPhase scan_phase) {
        bool any_alive = false;
        for (const auto& stat : scan_stats(scan_phase)) {
            if (std::cmp_equal(stat.pgid, target_pgid)) {
                upsert_tracked(stat, scan_phase);
                if (stat.state != EXITED_STATE) {
                    any_alive = true;
                }
            }
        }
        last_group_alive = any_alive;
        last_proc_scan_ms = now_ms();
    };

    observe(RunObserverPhase::INITIAL_SCAN);
    scan_target_group(RunObserverPhase::INITIAL_SCAN);

    for (;;) {
        for (;;) {
            observe(RunObserverPhase::WAIT_PROBE);
            int64_t const REAPED = waitpid_nohang(-1, &status);
            observe(RunObserverPhase::LOOP_CONTROL, REAPED);
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
            observe(RunObserverPhase::GROUP_SCAN);
            scan_target_group(RunObserverPhase::GROUP_SCAN);
            observe(RunObserverPhase::LOOP_CONTROL);
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

        observe(RunObserverPhase::SLEEP);
        wall_sleep_ms(SLEEP_SLICE_MS);
        observe(RunObserverPhase::LOOP_CONTROL);
        if (data_fd.valid() && now_ms() - last_drain_ms >= DRAIN_INTERVAL_MS) {
            observe(RunObserverPhase::RING_DRAIN);
            (void)drain_capture(data_fd.get(), capture);
            last_drain_ms = now_ms();
            observe(RunObserverPhase::LOOP_CONTROL);
        }
    }

    observe(RunObserverPhase::FINAL_REAP);
    while (waitpid_nohang(-1, &status) > 0) {
    }

    int64_t elapsed_ms = now_ms() - START_MS;
    elapsed_ms = std::max<int64_t>(elapsed_ms, 1);

    observe(RunObserverPhase::CAPTURE_STOP);
    capture.control_failed = !set_recording_enabled(false) || capture.control_failed;
    restore_perf_run_signal_handlers(signal_handlers);
    observe(RunObserverPhase::FINAL_STATS);
    auto after = collect_main_stats();
    observe(RunObserverPhase::FINAL_REPORT);
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
        observe(RunObserverPhase::FINAL_DRAIN);
        finish_capture(data_fd.get(), capture);
        observe(std::nullopt);
        capture.write_failed = !write_all(data_fd.get(), SECTION_EVENTS_END) || capture.write_failed;
        auto const TOTAL_EVENT_BYTES = static_cast<ssize_t>(capture.saved_bytes);
        ssize_t const SUMMARY_BYTES = write_section_wki_summary(data_fd.get());
        ssize_t const IPC_BYTES = write_section_ipc_stats(data_fd.get());
        ssize_t const CONTSTAT_BYTES = write_section_contstat(data_fd.get());
        ssize_t const MEMACC_BYTES = write_section_memacc_alloc_totals(data_fd.get());
        ssize_t const DIAG_BYTES = CONTSTAT_BYTES + MEMACC_BYTES;
        capture.write_failed = !write_section_peer_map(data_fd.get()) || capture.write_failed;
        capture.write_failed = !write_all(data_fd.get(), SECTION_PROC_MAP) || capture.write_failed;

        for (const auto& stat : collect_main_stats()) {
            capture.write_failed =
                !write_all(data_fd.get(), proc_map_line(stat.pid, stat.comm, read_cmdline(stat.pid))) || capture.write_failed;
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
                capture.write_failed = !write_all(data_fd.get(), proc_map_line(proc.pid, proc.comm, proc.cmdline)) || capture.write_failed;
            }
        }

        capture.write_failed = !write_all(data_fd.get(), SECTION_PROC_MAP_END) || capture.write_failed;
        capture.write_failed = !write_section_image_map(data_fd.get(), &tracked) || capture.write_failed;
        capture.write_failed = SUMMARY_BYTES < 0 || IPC_BYTES < 0 || CONTSTAT_BYTES < 0 || MEMACC_BYTES < 0 || capture.write_failed;
        output_ok = write_capture_receipt(data_fd.get(), capture) && output_ok;

        if (TOTAL_EVENT_BYTES <= 0 && SUMMARY_BYTES <= 0 && IPC_BYTES <= 0 && DIAG_BYTES <= 0) {
            std::println("perf: ring buffer empty - PROC_MAP saved, no events");
        } else {
            std::println("perf: saved to {} ({} event bytes, {} summary bytes, {} IPC bytes, {} diag bytes)", PERF_DATA_FILE,
                         TOTAL_EVENT_BYTES, SUMMARY_BYTES, IPC_BYTES, DIAG_BYTES);
        }
        data_fd.reset();
        if (structured && !finalize_structured_perf_data()) {
            output_ok = false;
        }
    } else {
        std::println("perf: cannot write {}", PERF_DATA_FILE);
    }

    cmd_sched(DEFAULT_MAX_EVENTS, display_options);
    return output_ok;
}

}  // namespace perf
