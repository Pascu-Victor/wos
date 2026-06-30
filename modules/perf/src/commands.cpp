#include "perf.hpp"
namespace perf {
namespace {

using CommandHandler = int (*)(int argc, char** argv);

struct CommandSpec {
    std::string_view name;
    std::string_view args;
    std::string_view summary;
    CommandHandler handler;
};

int run_stat_command(int argc, char** argv);
int run_record_command(int argc, char** argv);
int run_report_command(int argc, char** argv);
int run_cpustat_command(int argc, char** argv);
int run_contstat_command(int argc, char** argv);
int run_ipc_report_command(int argc, char** argv);
int run_wki_report_command(int argc, char** argv);
int run_local_report_command(int argc, char** argv);
int run_all_report_command(int argc, char** argv);
int run_vmem_report_command(int argc, char** argv);
int run_checkout_report_command(int argc, char** argv);
int run_wki_launch_command(int argc, char** argv);
int run_wki_tail_command(int argc, char** argv);
int run_local_tail_command(int argc, char** argv);
int run_all_tail_command(int argc, char** argv);
int run_vmem_tail_command(int argc, char** argv);
int run_wki_trace_command(int argc, char** argv);
int run_local_trace_command(int argc, char** argv);
int run_all_trace_command(int argc, char** argv);
int run_vmem_trace_command(int argc, char** argv);
int run_run_command(int argc, char** argv);
int run_show_map_command(int argc, char** argv);
int run_help_command(int argc, char** argv);

constexpr std::array<CommandSpec, 24> COMMANDS = {{
    {.name = "stat", .args = "[ms=1000]", .summary = "CPU% per process over a sampling window", .handler = run_stat_command},
    {.name = "record",
     .args = "[ms=1000] [--filter=<filters>]",
     .summary = "Record kernel perf events to perf.data",
     .handler = run_record_command},
    {.name = "report",
     .args = "[n=2000] [--time=FMT] [--peer-ids]",
     .summary = "Display events from perf.data or live /proc/kperf",
     .handler = run_report_command},
    {.name = "sched",
     .args = "[n=2000] [--time=FMT] [--peer-ids]",
     .summary = "Alias for report with scheduler hotspot tables",
     .handler = run_report_command},
    {.name = "cpustat", .args = "", .summary = "Per-CPU aggregate scheduler statistics", .handler = run_cpustat_command},
    {.name = "contstat", .args = "", .summary = "Per-subsystem container statistics", .handler = run_contstat_command},
    {.name = "ipc-report",
     .args = "[n=15] [filters]",
     .summary = "Remote IPC and local pipe latency, jitter, throughput, memory, and queue pressure",
     .handler = run_ipc_report_command},
    {.name = "wki-report", .args = "[filters]", .summary = "Remote/WKI summary statistics", .handler = run_wki_report_command},
    {.name = "local-report", .args = "[filters]", .summary = "Local pipe/process summary statistics", .handler = run_local_report_command},
    {.name = "all-report", .args = "[filters]", .summary = "Combined WKI and local summary statistics", .handler = run_all_report_command},
    {.name = "vmem-report",
     .args = "[filters]",
     .summary = "Local vmem mmap/COW/cache summary statistics",
     .handler = run_vmem_report_command},
    {.name = "checkout-report",
     .args = "[n=12]",
     .summary = "Git checkout/reset XFS, cache, dirty-writeback, and FD diagnostics",
     .handler = run_checkout_report_command},
    {.name = "wki-launch",
     .args = "[n=20] [--time=FMT] [--peer-ids]",
     .summary = "Remote launch stage timings",
     .handler = run_wki_launch_command},
    {.name = "wki-tail",
     .args = "[n=15] [filters]",
     .summary = "Tail-latency WKI summary and slow remote events",
     .handler = run_wki_tail_command},
    {.name = "local-tail",
     .args = "[n=15] [filters]",
     .summary = "Tail-latency local summary and slow local events",
     .handler = run_local_tail_command},
    {.name = "all-tail",
     .args = "[n=15] [filters]",
     .summary = "Tail-latency combined WKI and local events",
     .handler = run_all_tail_command},
    {.name = "vmem-tail", .args = "[n=15] [filters]", .summary = "Tail-latency local vmem events", .handler = run_vmem_tail_command},
    {.name = "wki-trace", .args = "[n=200] [filters]", .summary = "Raw remote/WKI trace events", .handler = run_wki_trace_command},
    {.name = "local-trace", .args = "[n=200] [filters]", .summary = "Raw local trace events", .handler = run_local_trace_command},
    {.name = "all-trace",
     .args = "[n=200] [filters]",
     .summary = "Raw combined WKI and local trace events",
     .handler = run_all_trace_command},
    {.name = "vmem-trace", .args = "[n=200] [filters]", .summary = "Raw local vmem trace events", .handler = run_vmem_trace_command},
    {.name = "run",
     .args = "[--filter=<filters>] [--time=FMT] <cmd> [args]",
     .summary = "Trace a command and descendants",
     .handler = run_run_command},
    {.name = "show-map", .args = "", .summary = "Show PID to command map from perf.data", .handler = run_show_map_command},
    {.name = "help", .args = "[filters]", .summary = "Show this screen or detailed filter help", .handler = run_help_command},
}};

struct FilterHelp {
    std::string_view name;
    std::string_view summary;
};

constexpr std::array<FilterHelp, 15> RECORD_FILTER_HELP = {{
    {.name = "sample", .summary = "timer CPU samples"},
    {.name = "switch", .summary = "context-switch events"},
    {.name = "wake", .summary = "task wake events"},
    {.name = "sleep", .summary = "task sleep/block events"},
    {.name = "container", .summary = "container and data-structure events"},
    {.name = "wki", .summary = "all WKI transport/RPC/IPC events"},
    {.name = "wki_launch", .summary = "remote-compute launch events only"},
    {.name = "local_pipe", .summary = "local pipe events"},
    {.name = "local_proc", .summary = "local fork/exec/process events"},
    {.name = "vmem", .summary = "local vmem mmap/cache/COW events"},
    {.name = "loader", .summary = "local ELF loader segment/perms events"},
    {.name = "xfs", .summary = "local XFS/buffer-cache/inode events"},
    {.name = "irq", .summary = "slow local interrupt-handler events"},
    {.name = "local", .summary = "all local pipe/process/vmem/loader/XFS/IRQ events"},
    {.name = "all", .summary = "all event classes"},
}};

constexpr std::array<FilterHelp, 12> WKI_FILTER_HELP = {{
    {.name = "--scope=NAME", .summary = "scope name such as remote_ipc, local_pipe, local_vmem, local_xfs, local_irq"},
    {.name = "--op=NAME", .summary = "operation name such as write_io, inode_fetch, buf_disk_write, execve, read"},
    {.name = "--phase=NAME", .summary = "begin, end, or point"},
    {.name = "--peer=N", .summary = "numeric WKI peer id"},
    {.name = "--channel=N", .summary = "WKI channel id"},
    {.name = "--corr=N", .summary = "trace correlation id"},
    {.name = "--pid=N", .summary = "subject process id"},
    {.name = "--min-us=N", .summary = "minimum auxiliary latency in microseconds"},
    {.name = "--from-ns=N", .summary = "minimum event timestamp"},
    {.name = "--to-ns=N", .summary = "maximum event timestamp"},
    {.name = "--time=FMT", .summary = "timestamp display: boot (default), unix-ns, or iso"},
    {.name = "--peer-ids", .summary = "show numeric peer ids instead of hostnames"},
}};

auto joined_record_filter_names() -> std::string {
    std::string out;
    for (const auto& filter : RECORD_FILTER_HELP) {
        if (!out.empty()) {
            out += ',';
        }
        out += filter.name;
    }
    return out;
}

void print_filters() {
    std::println("Event filters for --filter=<list>:");
    for (const auto& filter : RECORD_FILTER_HELP) {
        std::println("  {:<12} {}", filter.name, filter.summary);
    }
    std::println("");
    std::println("Trace filter and display options:");
    for (const auto& filter : WKI_FILTER_HELP) {
        std::println("  {:<14} {}", filter.name, filter.summary);
    }
}

void usage() {
    std::println("Usage: perf <command> [args]");
    std::println("Commands:");
    for (const auto& command : COMMANDS) {
        std::println("  {:<12} {:<34} {}", command.name, command.args, command.summary);
    }
    std::println("");
    std::println("Available --filter names: {}", joined_record_filter_names());
    std::println("Use 'perf help filters' for filter and display details.");
}

int run_stat_command(int argc, char** argv) {
    int const MS = argc >= 3 ? static_cast<int>(strtol(argv[2], nullptr, PARSE_BASE_DECIMAL)) : DEFAULT_SAMPLE_MS;
    cmd_stat(MS);
    return 0;
}

int run_record_command(int argc, char** argv) {
    int ms = DEFAULT_SAMPLE_MS;
    const char* filter = nullptr;
    for (int i = 2; i < argc; ++i) {
        std::string_view const ARG(argv[i]);
        if (ARG.starts_with("--filter=")) {
            filter = argv[i] + 9;
        } else {
            ms = static_cast<int>(strtol(argv[i], nullptr, PARSE_BASE_DECIMAL));
        }
    }
    cmd_record(ms, filter);
    return 0;
}

int run_report_command(int argc, char** argv) {
    int max_events = DEFAULT_MAX_EVENTS;
    WkiDisplayOptions display_options{};
    for (int i = 2; i < argc; ++i) {
        std::string_view const ARG(argv[i]);
        if (parse_display_arg(ARG, display_options)) {
            continue;
        }
        if (!ARG.empty() && ARG.front() != '-') {
            max_events = static_cast<int>(strtol(argv[i], nullptr, PARSE_BASE_DECIMAL));
        } else {
            std::println("perf report: unrecognized argument '{}'", argv[i]);
            return 1;
        }
    }
    cmd_sched(max_events, display_options);
    return 0;
}

int run_cpustat_command(int /*argc*/, char** /*argv*/) {
    cmd_cpustat();
    return 0;
}

int run_contstat_command(int /*argc*/, char** /*argv*/) {
    cmd_contstat();
    return 0;
}

int run_ipc_report_command(int argc, char** argv) {
    int rows = DEFAULT_WKI_TAIL_ROWS;
    WkiTraceFilter filter{};
    WkiDisplayOptions display_options{};
    for (int i = 2; i < argc; ++i) {
        std::string_view const ARG(argv[i]);
        if (parse_wki_filter_arg(ARG, filter, display_options)) {
            continue;
        }
        if (!ARG.empty() && ARG.front() != '-') {
            rows = static_cast<int>(strtol(argv[i], nullptr, PARSE_BASE_DECIMAL));
        } else {
            std::println("perf ipc-report: unrecognized argument '{}'", argv[i]);
            return 1;
        }
    }
    cmd_ipc_report(rows, filter, display_options);
    return 0;
}

int run_view_report_command(int argc, char** argv, WkiDataView view) {
    WkiTraceFilter filter{};
    WkiDisplayOptions display_options{};
    for (int i = 2; i < argc; ++i) {
        if (!parse_wki_filter_arg(argv[i], filter, display_options)) {
            std::println("perf {}-report: unrecognized argument '{}'", wki_view_name(view), argv[i]);
            return 1;
        }
    }
    cmd_wki_report(filter, display_options, view);
    return 0;
}

int run_wki_report_command(int argc, char** argv) { return run_view_report_command(argc, argv, WkiDataView::WKI); }

int run_local_report_command(int argc, char** argv) { return run_view_report_command(argc, argv, WkiDataView::LOCAL); }

int run_all_report_command(int argc, char** argv) { return run_view_report_command(argc, argv, WkiDataView::ALL); }

int run_vmem_report_command(int argc, char** argv) {
    WkiTraceFilter filter{};
    WkiDisplayOptions display_options{};
    for (int i = 2; i < argc; ++i) {
        if (!parse_wki_filter_arg(argv[i], filter, display_options)) {
            std::println("perf vmem-report: unrecognized argument '{}'", argv[i]);
            return 1;
        }
    }
    if (filter.scope.empty()) {
        filter.scope = "local_vmem";
    }
    cmd_vmem_report(filter);
    return 0;
}

int run_checkout_report_command(int argc, char** argv) {
    int rows = DEFAULT_CHECKOUT_ROWS;
    for (int i = 2; i < argc; ++i) {
        std::string_view const ARG(argv[i]);
        if (!ARG.empty() && ARG.front() != '-') {
            rows = static_cast<int>(strtol(argv[i], nullptr, PARSE_BASE_DECIMAL));
        } else {
            std::println("perf checkout-report: unrecognized argument '{}'", argv[i]);
            return 1;
        }
    }
    cmd_checkout_report(rows);
    return 0;
}

int run_wki_launch_command(int argc, char** argv) {
    int rows = DEFAULT_WKI_LAUNCH_ROWS;
    WkiDisplayOptions display_options{};
    for (int i = 2; i < argc; ++i) {
        std::string_view const ARG(argv[i]);
        if (parse_display_arg(ARG, display_options)) {
            continue;
        }
        if (!ARG.empty() && ARG.front() != '-') {
            rows = static_cast<int>(strtol(argv[i], nullptr, PARSE_BASE_DECIMAL));
        } else {
            std::println("perf wki-launch: unrecognized argument '{}'", argv[i]);
            return 1;
        }
    }
    cmd_wki_launch(rows, display_options);
    return 0;
}

int run_view_tail_command(int argc, char** argv, WkiDataView view) {
    int rows = DEFAULT_WKI_TAIL_ROWS;
    WkiTraceFilter filter{};
    WkiDisplayOptions display_options{};
    for (int i = 2; i < argc; ++i) {
        std::string_view const ARG(argv[i]);
        if (parse_wki_filter_arg(ARG, filter, display_options)) {
            continue;
        }
        if (!ARG.empty() && ARG.front() != '-') {
            rows = static_cast<int>(strtol(argv[i], nullptr, PARSE_BASE_DECIMAL));
        } else {
            std::println("perf {}-tail: unrecognized argument '{}'", wki_view_name(view), argv[i]);
            return 1;
        }
    }
    cmd_wki_tail(rows, filter, display_options, view);
    return 0;
}

int run_wki_tail_command(int argc, char** argv) { return run_view_tail_command(argc, argv, WkiDataView::WKI); }

int run_local_tail_command(int argc, char** argv) { return run_view_tail_command(argc, argv, WkiDataView::LOCAL); }

int run_all_tail_command(int argc, char** argv) { return run_view_tail_command(argc, argv, WkiDataView::ALL); }

int run_vmem_tail_command(int argc, char** argv) {
    int rows = DEFAULT_WKI_TAIL_ROWS;
    WkiTraceFilter filter{};
    WkiDisplayOptions display_options{};
    for (int i = 2; i < argc; ++i) {
        std::string_view const ARG(argv[i]);
        if (parse_wki_filter_arg(ARG, filter, display_options)) {
            continue;
        }
        if (!ARG.empty() && ARG.front() != '-') {
            rows = static_cast<int>(strtol(argv[i], nullptr, PARSE_BASE_DECIMAL));
        } else {
            std::println("perf vmem-tail: unrecognized argument '{}'", argv[i]);
            return 1;
        }
    }
    if (filter.scope.empty()) {
        filter.scope = "local_vmem";
    }
    cmd_wki_tail(rows, filter, display_options, WkiDataView::VMEM);
    return 0;
}

int run_view_trace_command(int argc, char** argv, WkiDataView view) {
    int max_events = DEFAULT_WKI_TRACE_EVENTS;
    WkiTraceFilter filter{};
    WkiDisplayOptions display_options{};
    for (int i = 2; i < argc; ++i) {
        std::string_view const ARG(argv[i]);
        if (parse_wki_filter_arg(ARG, filter, display_options)) {
            continue;
        }
        if (!ARG.empty() && ARG.front() != '-') {
            max_events = static_cast<int>(strtol(argv[i], nullptr, PARSE_BASE_DECIMAL));
        } else {
            std::println("perf {}-trace: unrecognized argument '{}'", wki_view_name(view), argv[i]);
            return 1;
        }
    }
    cmd_wki_trace(max_events, filter, display_options, view);
    return 0;
}

int run_wki_trace_command(int argc, char** argv) { return run_view_trace_command(argc, argv, WkiDataView::WKI); }

int run_local_trace_command(int argc, char** argv) { return run_view_trace_command(argc, argv, WkiDataView::LOCAL); }

int run_all_trace_command(int argc, char** argv) { return run_view_trace_command(argc, argv, WkiDataView::ALL); }

int run_vmem_trace_command(int argc, char** argv) {
    int max_events = DEFAULT_WKI_TRACE_EVENTS;
    WkiTraceFilter filter{};
    WkiDisplayOptions display_options{};
    for (int i = 2; i < argc; ++i) {
        std::string_view const ARG(argv[i]);
        if (parse_wki_filter_arg(ARG, filter, display_options)) {
            continue;
        }
        if (!ARG.empty() && ARG.front() != '-') {
            max_events = static_cast<int>(strtol(argv[i], nullptr, PARSE_BASE_DECIMAL));
        } else {
            std::println("perf vmem-trace: unrecognized argument '{}'", argv[i]);
            return 1;
        }
    }
    if (filter.scope.empty()) {
        filter.scope = "local_vmem";
    }
    cmd_wki_trace(max_events, filter, display_options, WkiDataView::VMEM);
    return 0;
}

int run_run_command(int argc, char** argv) {
    if (argc < 3) {
        std::println("perf run: usage: perf run <program> [args...]");
        return 1;
    }
    cmd_run(argc - 2, argv + 2);
    return 0;
}

int run_show_map_command(int /*argc*/, char** /*argv*/) {
    cmd_show_map();
    return 0;
}

int run_help_command(int argc, char** argv) {
    if (argc >= 3 && std::string_view(argv[2]) == "filters") {
        print_filters();
    } else {
        usage();
    }
    return 0;
}

}  // namespace

int run_perf(int argc, char** argv) {
    if (argc < 2) {
        usage();
        return 1;
    }

    std::string_view const CMD(argv[1]);

    for (const auto& command : COMMANDS) {
        if (CMD == command.name) {
            return command.handler(argc, argv);
        }
    }

    usage();
    return 1;
}

}  // namespace perf
