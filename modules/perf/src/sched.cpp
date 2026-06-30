#include "perf.hpp"
namespace perf {

void print_sched_flags(uint8_t flags) {
    bool first = true;
    auto print_flag = [&](std::string_view flag) {
        if (!first) {
            std::print("|");
        }
        std::print("{}", flag);
        first = false;
    };

    if ((flags & FLAG_USER_MODE) != 0U) {
        print_flag("USER");
    }
    if ((flags & FLAG_PREEMPT) != 0U) {
        print_flag("PREEMPT");
    }
    if ((flags & FLAG_YIELD) != 0U) {
        print_flag("YIELD");
    }
    if ((flags & FLAG_BLOCK) != 0U) {
        print_flag("BLOCK");
    }
    if (first) {
        std::print("KERN");
    }
}

void print_wait_flags(uint8_t flags) {
    bool first = true;
    auto print_flag = [&](std::string_view flag) {
        if (!first) {
            std::print("|");
        }
        std::print("{}", flag);
        first = false;
    };

    if ((flags & FLAG_BLOCK) != 0U) {
        print_flag("BLOCK");
    }
    if ((flags & FLAG_TIMED) != 0U) {
        print_flag("TIMED");
    }
    if ((flags & FLAG_EXPLICIT_WAKE) != 0U) {
        print_flag("EXPLICIT");
    }
    if ((flags & FLAG_WAKE_CURRENT) != 0U) {
        print_flag("CURRENT");
    }
    if (first) {
        std::print("NONE");
    }
}

void print_hotspot_tables(const std::vector<HotspotStats>& rows) {
    std::vector<HotspotStats> busy_yield = rows;
    auto removed = std::ranges::remove_if(busy_yield, [](const HotspotStats& row) { return row.yield_count == 0; });
    busy_yield.erase(removed.begin(), removed.end());
    std::ranges::sort(busy_yield, [](const HotspotStats& lhs, const HotspotStats& rhs) {
        if (lhs.short_yield_count != rhs.short_yield_count) {
            return lhs.short_yield_count > rhs.short_yield_count;
        }
        if (lhs.yield_count != rhs.yield_count) {
            return lhs.yield_count > rhs.yield_count;
        }
        return lhs.yield_run_total_us > rhs.yield_run_total_us;
    });

    std::println("=== busy-yield hotspots (short <= {}us) =============================", SHORT_RUN_THRESHOLD_US);
    if (busy_yield.empty()) {
        std::println("none");
    } else {
        constexpr auto BUSY_HDR_FMT = "{:>6}  {:<20}  {:<18}  {:<18}  {:>7}  {:>7}  {:>11}  {:>11}";
        constexpr auto BUSY_SEP_FMT = "{:->6}  {:->20}  {:->18}  {:->18}  {:->7}  {:->7}  {:->11}  {:->11}";
        std::println(BUSY_HDR_FMT, "PID", "NAME", "WAIT", "CALLSITE", "yield", "short", "avg_run(us)", "max_run(us)");
        std::println(BUSY_SEP_FMT, "", "", "", "", "", "", "", "");
        for (std::size_t index = 0; index < busy_yield.size() && index < HOTSPOT_ROW_LIMIT; ++index) {
            const auto& row = busy_yield.at(index);
            uint64_t avg_run = row.yield_count != 0U ? row.yield_run_total_us / row.yield_count : 0;
            std::println("{:>6}  {:<20}  {:<18}  {:<18}  {:>7}  {:>7}  {:>11}  {:>11}", row.pid, row.comm.empty() ? "?" : row.comm,
                         display_wait_channel(row.wait_channel), display_callsite(row.callsite), row.yield_count, row.short_yield_count,
                         avg_run, row.yield_run_max_us);
        }
    }

    std::println("");

    std::vector<HotspotStats> wake_churn = rows;
    auto removed_churn =
        std::ranges::remove_if(wake_churn, [](const HotspotStats& row) { return row.wake_count == 0 && row.sleep_count == 0; });
    wake_churn.erase(removed_churn.begin(), removed_churn.end());
    std::ranges::sort(wake_churn, [](const HotspotStats& lhs, const HotspotStats& rhs) {
        uint64_t const LHS_SCORE = (lhs.short_wake_count * 3U) + (lhs.explicit_wake_count * 2U) + lhs.short_sleep_count;
        uint64_t const RHS_SCORE = (rhs.short_wake_count * 3U) + (rhs.explicit_wake_count * 2U) + rhs.short_sleep_count;
        if (LHS_SCORE != RHS_SCORE) {
            return LHS_SCORE > RHS_SCORE;
        }
        if (lhs.wake_count != rhs.wake_count) {
            return lhs.wake_count > rhs.wake_count;
        }
        return lhs.sleep_count > rhs.sleep_count;
    });

    std::println("=== wake/sleep churn hotspots (short sleep <= {}us) =================", SHORT_SLEEP_THRESHOLD_US);
    if (wake_churn.empty()) {
        std::println("none");
    } else {
        constexpr auto CHURN_HDR_FMT = "{:>6}  {:<20}  {:<18}  {:<18}  {:>5}  {:>5}  {:>8}  {:>7}  {:>13}  {:>11}";
        constexpr auto CHURN_SEP_FMT = "{:->6}  {:->20}  {:->18}  {:->18}  {:->5}  {:->5}  {:->8}  {:->7}  {:->13}  {:->11}";
        std::println(CHURN_HDR_FMT, "PID", "NAME", "WAIT", "CALLSITE", "wake", "short", "explicit", "current", "avg_sleep(us)",
                     "avg_run(us)");
        std::println(CHURN_SEP_FMT, "", "", "", "", "", "", "", "", "", "");
        for (std::size_t index = 0; index < wake_churn.size() && index < HOTSPOT_ROW_LIMIT; ++index) {
            const auto& row = wake_churn.at(index);
            uint64_t avg_sleep = row.wake_count != 0U ? row.wake_sleep_total_us / row.wake_count : 0;
            uint64_t avg_run = row.sleep_count != 0U ? row.sleep_run_total_us / row.sleep_count : 0;
            std::println("{:>6}  {:<20}  {:<18}  {:<18}  {:>5}  {:>5}  {:>8}  {:>7}  {:>13}  {:>11}", row.pid,
                         row.comm.empty() ? "?" : row.comm, display_wait_channel(row.wait_channel), display_callsite(row.callsite),
                         row.wake_count, row.short_wake_count, row.explicit_wake_count, row.current_wake_count, avg_sleep, avg_run);
        }
    }

    std::println("");
}

void cmd_sched(int max_events, const WkiDisplayOptions& display_options) {
    if (max_events < 1) {
        max_events = DEFAULT_MAX_EVENTS;
    }

    std::string_view src = access(PERF_DATA_FILE.begin(), R_OK) == 0 ? PERF_DATA_FILE : KPERF_PATH;
    auto buffer = read_file(src);
    if (!buffer.has_value() || buffer->empty()) {
        std::println("perf: no data (run 'perf record' or 'perf run' first)");
        return;
    }

    std::string_view const VIEW(*buffer);
    bool const SECTIONED = VIEW.starts_with(SECTION_HEADER);
    std::vector<ProcMapEntry> proc_map;
    std::size_t section_start = 0;

    if (SECTIONED) {
        proc_map = parse_proc_map_section(VIEW);
        std::size_t const EVENTS_HDR = VIEW.find(SECTION_EVENTS);
        if (EVENTS_HDR == std::string_view::npos) {
            std::println("perf: perf.data has no EVENTS section");
            return;
        }
        section_start = VIEW.find('\n', EVENTS_HDR);
        if (section_start == std::string_view::npos) {
            return;
        }
        ++section_start;
    } else {
        for (const auto& stat : collect_stats()) {
            proc_map.push_back(ProcMapEntry{.pid = stat.pid, .comm = stat.comm});
        }
    }

    std::string_view const EVENT_VIEW = VIEW.substr(section_start);
    auto peer_resolver = make_wki_peer_resolver(VIEW, SECTIONED, display_options);
    auto hotspot_rows = summarize_hotspots(EVENT_VIEW, SECTIONED, proc_map);
    print_hotspot_tables(hotspot_rows);
    auto const TIME_DISPLAY = make_time_display(display_options, VIEW, SECTIONED);
    int const TIME_WIDTH = time_column_width(TIME_DISPLAY);

    std::println("=== perf report [{}] (up to {} events) ============================", src, max_events);
    std::println("{:>3}  {:>{}}  {:>3}  {}", "EVT", time_column_header(TIME_DISPLAY), TIME_WIDTH, "CPU", "DETAILS");
    std::println("{:->3}  {:->{}}  {:->3}  {:->40}", "", "", TIME_WIDTH, "", "");

    int count = 0;
    EventInfo event{};
    std::size_t event_pos = 0;
    while (count < max_events && next_event(EVENT_VIEW, event_pos, SECTIONED, event)) {
        if (event.type == 'S') {
            std::print("SMP  {:>{}}  {:>3}  {:<24} rip={:#016x} lag={:>10}  ", format_event_time(event.ts_ns, TIME_DISPLAY), TIME_WIDTH,
                       event.cpu, format_pid_name(event.pid, proc_map), event.data, event.lag);
            print_sched_flags(event.flags);
            std::println("");
        } else if (event.type == 'X') {
            std::print("CTX  {:>{}}  {:>3}  {:<24} -> {:<24} lag={:>10} run_us={:>6} site={}  ",
                       format_event_time(event.ts_ns, TIME_DISPLAY), TIME_WIDTH, event.cpu, format_pid_name(event.pid, proc_map),
                       format_pid_name(event.other_pid, proc_map), event.lag, event.aux, display_callsite(event.callsite));
            print_sched_flags(event.flags);
            std::println("");
        } else if (event.type == 'W') {
            std::print("WKE  {:>{}}  {:>3}  {:<24} wake_at_us={} sleep_us={:>6} wait={} site={}  ",
                       format_event_time(event.ts_ns, TIME_DISPLAY), TIME_WIDTH, event.cpu, format_pid_name(event.pid, proc_map),
                       event.data, event.aux, display_wait_channel(event.wait_channel), display_callsite(event.callsite));
            print_wait_flags(event.flags);
            std::println("");
        } else if (event.type == 'B') {
            std::print("SLP  {:>{}}  {:>3}  {:<24} wake_at_us={} run_us={:>6} wait={} site={}  ",
                       format_event_time(event.ts_ns, TIME_DISPLAY), TIME_WIDTH, event.cpu, format_pid_name(event.pid, proc_map),
                       event.data, event.aux, display_wait_channel(event.wait_channel), display_callsite(event.callsite));
            print_wait_flags(event.flags);
            std::println("");
        } else if (event.type == 'C') {
            std::println("CNT  {:>{}}  {:>3}  {:<24} subsys={} count={} cap={} flags={} site={}",
                         format_event_time(event.ts_ns, TIME_DISPLAY), TIME_WIDTH, event.cpu, format_pid_name(event.pid, proc_map),
                         event.subsys_name, event.lag, event.aux, static_cast<unsigned>(event.flags), display_callsite(event.callsite));
        } else if (event.type == 'K') {
            std::println("WKI  {:>{}}  {:>3}  {:<24} {}:{}:{} peer={} ch={} corr={} status={} aux={} site={}",
                         format_event_time(event.ts_ns, TIME_DISPLAY), TIME_WIDTH, event.cpu, format_pid_name(event.pid, proc_map),
                         event.scope_name, event.op_name, event.phase_name, wki_peer_label(event.peer, peer_resolver), event.channel,
                         event.correlation, event.status, event.aux, display_callsite(event.callsite));
        }

        ++count;
    }
    std::println("=== {} events ==", count);
}

void cmd_cpustat() {
    auto buffer = read_file(KCPUSTAT_PATH, CPUSTAT_READ_CAPACITY);
    if (!buffer.has_value() || buffer->empty()) {
        std::println("perf: cannot read /proc/kcpustat");
        return;
    }

    std::println("=== perf cpustat ===================================================");
    std::println(
        "{:>4}  {:>10}  {:>8}  {:>10}  {:>10}  {:>8}  {:>10}  {:>10}  {:>10}  {:>10}  {:>12}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  "
        "{:>10}",
        "CPU", "ctx", "preempt", "sleep", "wake", "sample", "fast_skip", "ring_write", "timer_irq", "sched_arm", "sched_disarm",
        "local_poke", "slow_scan", "wait_scan", "wait_pass", "wait_max", "timer_wake");
    std::println(
        "{:->4}  {:->10}  {:->8}  {:->10}  {:->10}  {:->8}  {:->10}  {:->10}  {:->10}  {:->10}  {:->12}  {:->10}  {:->10}  "
        "{:->10}  {:->10}  {:->10}  {:->10}",
        "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "");

    std::size_t pos = 0;
    while (pos < buffer->size()) {
        std::string_view line = next_line(*buffer, pos);
        if (line.empty()) {
            continue;
        }

        auto get_val = [&](std::string_view key) {
            std::size_t key_pos = line.find(key);
            if (key_pos == std::string_view::npos) {
                return uint64_t{0};
            }
            key_pos += key.size();
            std::size_t const END = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos));
        };

        std::println(
            "{:>4}  {:>10}  {:>8}  {:>10}  {:>10}  {:>8}  {:>10}  {:>10}  {:>10}  {:>10}  {:>12}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  "
            "{:>10}",
            get_val("cpu="), get_val("ctx="), get_val("preempt="), get_val("sleep="), get_val("wake="), get_val("sample="),
            get_val("fast_skip="), get_val("ring_write="), get_val("timer_irq="), get_val("sched_arm="), get_val("sched_disarm="),
            get_val("local_poke="), get_val("slow_scan="), get_val("wait_scan="), get_val("wait_pass="), get_val("wait_max="),
            get_val("timer_wake="));
    }

    std::println("");
    std::println("{:>4}  {:>10}  {:>12}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>8}", "CPU", "idle_arm", "idle_disarm",
                 "idle_wake", "wake_ipi", "wake_coal", "gc_pass", "gc_reclaim", "gc_us", "gc_max_us", "lb_push");
    std::println("{:->4}  {:->10}  {:->12}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->8}", "", "", "", "", "", "",
                 "", "", "", "", "");

    pos = 0;
    while (pos < buffer->size()) {
        std::string_view line = next_line(*buffer, pos);
        if (line.empty()) {
            continue;
        }

        auto get_val = [&](std::string_view key) {
            std::size_t key_pos = line.find(key);
            if (key_pos == std::string_view::npos) {
                return uint64_t{0};
            }
            key_pos += key.size();
            std::size_t const END = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos));
        };

        std::println("{:>4}  {:>10}  {:>12}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>8}", get_val("cpu="),
                     get_val("idle_arm="), get_val("idle_disarm="), get_val("idle_wake="), get_val("wake_ipi="), get_val("wake_coal="),
                     get_val("gc_pass="), get_val("gc_reclaim="), get_val("gc_us="), get_val("gc_max_us="), get_val("lb_push="));
    }

    std::println("");
    std::println("{:>4}  {:>10}  {:>10}  {:>10}  {:>12}  {:>10}  {:>10}", "CPU", "arm_wait", "arm_itimer", "arm_vol", "arm_idlework",
                 "arm_runq", "arm_comp");
    std::println("{:->4}  {:->10}  {:->10}  {:->10}  {:->12}  {:->10}  {:->10}", "", "", "", "", "", "", "");

    pos = 0;
    while (pos < buffer->size()) {
        std::string_view line = next_line(*buffer, pos);
        if (line.empty()) {
            continue;
        }

        auto get_val = [&](std::string_view key) {
            std::size_t key_pos = line.find(key);
            if (key_pos == std::string_view::npos) {
                return uint64_t{0};
            }
            key_pos += key.size();
            std::size_t const END = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos));
        };

        std::println("{:>4}  {:>10}  {:>10}  {:>10}  {:>12}  {:>10}  {:>10}", get_val("cpu="), get_val("arm_wait="), get_val("arm_itimer="),
                     get_val("arm_vol="), get_val("arm_idlework="), get_val("arm_runq="), get_val("arm_comp="));
    }

    std::println("");
    std::println("{:>4}  {:>10}  {:>11}  {:>12}  {:>13}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>11}  {:>10}  {:>10}", "CPU", "gc_task",
                 "gc_task_max", "gc_detach", "gc_detach_max", "gc_pm", "gc_pm_max", "gc_thr", "gc_thr_max", "gc_misc", "gc_misc_max",
                 "gc_dbg", "gc_dbg_max");
    std::println("{:->4}  {:->10}  {:->11}  {:->12}  {:->13}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->11}  {:->10}  {:->10}", "",
                 "", "", "", "", "", "", "", "", "", "", "", "");

    pos = 0;
    while (pos < buffer->size()) {
        std::string_view line = next_line(*buffer, pos);
        if (line.empty()) {
            continue;
        }

        auto get_val = [&](std::string_view key) {
            std::size_t key_pos = line.find(key);
            if (key_pos == std::string_view::npos) {
                return uint64_t{0};
            }
            key_pos += key.size();
            std::size_t const END = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos));
        };

        std::println("{:>4}  {:>10}  {:>11}  {:>12}  {:>13}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>11}  {:>10}  {:>10}",
                     get_val("cpu="), get_val("gc_task_us="), get_val("gc_task_max="), get_val("gc_detach_us="), get_val("gc_detach_max="),
                     get_val("gc_pm_us="), get_val("gc_pm_max="), get_val("gc_thr_us="), get_val("gc_thr_max="), get_val("gc_misc_us="),
                     get_val("gc_misc_max="), get_val("gc_dbg_us="), get_val("gc_dbg_max="));
    }

    std::println("");
    std::println("{:>4}  {:>11}  {:>11}  {:>8}  {:>10}  {:>11}  {:>11}  {:>13}  {:>12}  {:>12}", "CPU", "defer_q", "defer_done", "depth",
                 "depth_max", "slices", "slice_done", "wait_max_us", "idle_boost", "foreground");
    std::println("{:->4}  {:->11}  {:->11}  {:->8}  {:->10}  {:->11}  {:->11}  {:->13}  {:->12}  {:->12}", "", "", "", "", "", "", "", "",
                 "", "");

    pos = 0;
    while (pos < buffer->size()) {
        std::string_view line = next_line(*buffer, pos);
        if (line.empty()) {
            continue;
        }

        auto get_val = [&](std::string_view key) {
            std::size_t key_pos = line.find(key);
            if (key_pos == std::string_view::npos) {
                return uint64_t{0};
            }
            key_pos += key.size();
            std::size_t const END = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos));
        };

        std::println("{:>4}  {:>11}  {:>11}  {:>8}  {:>10}  {:>11}  {:>11}  {:>13}  {:>12}  {:>12}", get_val("cpu="),
                     get_val("gc_defer_queued="), get_val("gc_defer_done="), get_val("gc_defer_depth="), get_val("gc_defer_depth_max="),
                     get_val("gc_defer_slices="), get_val("gc_defer_slice_done="), get_val("gc_defer_wait_max_us="),
                     get_val("gc_idle_boost_pass="), get_val("gc_foreground_pass="));
    }

    std::println("");
    std::println("{:>4}  {:>9}  {:>11}  {:>12}  {:>11}  {:>12}  {:>9}  {:>10}  {:>10}  {:>11}", "CPU", "dus_calls", "collect_us",
                 "collect_max", "data_us", "data_max", "pt_us", "pt_max", "tlb_us", "tlb_max");
    std::println("{:->4}  {:->9}  {:->11}  {:->12}  {:->11}  {:->12}  {:->9}  {:->10}  {:->10}  {:->11}", "", "", "", "", "", "", "", "",
                 "", "");

    pos = 0;
    while (pos < buffer->size()) {
        std::string_view line = next_line(*buffer, pos);
        if (line.empty()) {
            continue;
        }

        auto get_val = [&](std::string_view key) {
            std::size_t key_pos = line.find(key);
            if (key_pos == std::string_view::npos) {
                return uint64_t{0};
            }
            key_pos += key.size();
            std::size_t const END = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos));
        };

        std::println("{:>4}  {:>9}  {:>11}  {:>12}  {:>11}  {:>12}  {:>9}  {:>10}  {:>10}  {:>11}", get_val("cpu="), get_val("dus_calls="),
                     get_val("dus_collect_us="), get_val("dus_collect_max="), get_val("dus_data_us="), get_val("dus_data_max="),
                     get_val("dus_pt_us="), get_val("dus_pt_max="), get_val("dus_tlb_us="), get_val("dus_tlb_max="));
    }

    std::println("");
    std::println("{:>4}  {:>10}  {:>10}  {:>10}  {:>10}  {:>11}  {:>15}  {:>12}", "CPU", "dus_leaf", "dus_refdec", "dus_freed",
                 "dus_ptfree", "huge_skip", "medium_skip", "corrupt_skip");
    std::println("{:->4}  {:->10}  {:->10}  {:->10}  {:->10}  {:->11}  {:->15}  {:->12}", "", "", "", "", "", "", "", "");

    pos = 0;
    while (pos < buffer->size()) {
        std::string_view line = next_line(*buffer, pos);
        if (line.empty()) {
            continue;
        }

        auto get_val = [&](std::string_view key) {
            std::size_t key_pos = line.find(key);
            if (key_pos == std::string_view::npos) {
                return uint64_t{0};
            }
            key_pos += key.size();
            std::size_t const END = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos));
        };

        uint64_t corrupt_skip = get_val("dus_corrupt_skip=");
        if (corrupt_skip == 0) {
            corrupt_skip = get_val("dus_corrupt=");
        }

        std::println("{:>4}  {:>10}  {:>10}  {:>10}  {:>10}  {:>11}  {:>15}  {:>12}", get_val("cpu="), get_val("dus_leaf="),
                     get_val("dus_refdec="), get_val("dus_freed="), get_val("dus_ptfree="), get_val("dus_huge_skip="),
                     get_val("dus_medium_skip="), corrupt_skip);
    }

    std::println("");
    std::println("{:>4}  {:>12}  {:>10}  {:>10}  {:>10}", "CPU", "unknown_skip", "slab_skip", "large_skip", "alias_skip");
    std::println("{:->4}  {:->12}  {:->10}  {:->10}  {:->10}", "", "", "", "", "");

    pos = 0;
    while (pos < buffer->size()) {
        std::string_view line = next_line(*buffer, pos);
        if (line.empty()) {
            continue;
        }

        auto get_val = [&](std::string_view key) {
            std::size_t key_pos = line.find(key);
            if (key_pos == std::string_view::npos) {
                return uint64_t{0};
            }
            key_pos += key.size();
            std::size_t const END = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos));
        };

        std::println("{:>4}  {:>12}  {:>10}  {:>10}  {:>10}", get_val("cpu="), get_val("dus_unknown_skip="), get_val("dus_slab_skip="),
                     get_val("dus_kmalloc_large_skip="), get_val("dus_alias_skip="));
    }

    std::println("");
    std::println("{:>4}  {:>16}  {:>16}  {:>18}  {:>18}", "CPU", "magic_reads", "magic_slab", "magic_medium", "magic_large");
    std::println("{:->4}  {:->16}  {:->16}  {:->18}  {:->18}", "", "", "", "", "");

    pos = 0;
    while (pos < buffer->size()) {
        std::string_view line = next_line(*buffer, pos);
        if (line.empty()) {
            continue;
        }

        auto get_val = [&](std::string_view key) {
            std::size_t key_pos = line.find(key);
            if (key_pos == std::string_view::npos) {
                return uint64_t{0};
            }
            key_pos += key.size();
            std::size_t const END = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos));
        };

        std::println("{:>4}  {:>16}  {:>16}  {:>18}  {:>18}", get_val("cpu="), get_val("dus_magic_unknown_reads="),
                     get_val("dus_magic_unknown_slab="), get_val("dus_magic_unknown_medium="), get_val("dus_magic_unknown_kmalloc_large="));
    }

    std::println("");
    std::println("{:>4}  {:>10}  {:>10}  {:>10}  {:>12}  {:>10}  {:>10}  {:>10}", "CPU", "ref_inc", "inc_retry", "ref_add", "add_refs",
                 "add_retry", "ref_dec", "dec_retry");
    std::println("{:->4}  {:->10}  {:->10}  {:->10}  {:->12}  {:->10}  {:->10}  {:->10}", "", "", "", "", "", "", "", "");

    pos = 0;
    while (pos < buffer->size()) {
        std::string_view line = next_line(*buffer, pos);
        if (line.empty()) {
            continue;
        }

        auto get_val = [&](std::string_view key) {
            std::size_t key_pos = line.find(key);
            if (key_pos == std::string_view::npos) {
                return uint64_t{0};
            }
            key_pos += key.size();
            std::size_t const END = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos));
        };

        std::println("{:>4}  {:>10}  {:>10}  {:>10}  {:>12}  {:>10}  {:>10}  {:>10}", get_val("cpu="), get_val("pa_ref_inc="),
                     get_val("pa_ref_inc_retry="), get_val("pa_ref_add="), get_val("pa_ref_add_refs="), get_val("pa_ref_add_retry="),
                     get_val("pa_ref_dec="), get_val("pa_ref_dec_retry="));
    }

    std::println("");
    std::println("{:>4}  {:>10}  {:>14}  {:>10}  {:>12}  {:>13}  {:>11}  {:>10}  {:>13}", "CPU", "zero", "zero_freed", "zero_bad", "batch",
                 "batch_pages", "batch_zero", "runs", "batch_freed");
    std::println("{:->4}  {:->10}  {:->14}  {:->10}  {:->12}  {:->13}  {:->11}  {:->10}  {:->13}", "", "", "", "", "", "", "", "", "");

    pos = 0;
    while (pos < buffer->size()) {
        std::string_view line = next_line(*buffer, pos);
        if (line.empty()) {
            continue;
        }

        auto get_val = [&](std::string_view key) {
            std::size_t key_pos = line.find(key);
            if (key_pos == std::string_view::npos) {
                return uint64_t{0};
            }
            key_pos += key.size();
            std::size_t const END = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos));
        };

        std::println("{:>4}  {:>10}  {:>14}  {:>10}  {:>12}  {:>13}  {:>11}  {:>10}  {:>13}", get_val("cpu="), get_val("pa_ref_zero="),
                     get_val("pa_ref_zero_freed="), get_val("pa_ref_zero_bad="), get_val("pa_ref_batch="), get_val("pa_ref_batch_pages="),
                     get_val("pa_ref_batch_zero="), get_val("pa_ref_batch_runs="), get_val("pa_ref_batch_freed="));
    }

    std::println("");
    std::println("{:>4}  {:>7}  {:>8}  {:>8}  {:>10}  {:>10}  {:>10}  {:>12}  {:>10}  {:>10}  {:>10}  {:>12}  {:>10}", "CPU", "enabled",
                 "capacity", "cached", "hit", "miss", "refill", "refill_pg", "free", "free_miss", "drain", "drain_pg", "stale");
    std::println("{:->4}  {:->7}  {:->8}  {:->8}  {:->10}  {:->10}  {:->10}  {:->12}  {:->10}  {:->10}  {:->10}  {:->12}  {:->10}", "", "",
                 "", "", "", "", "", "", "", "", "", "", "");

    pos = 0;
    while (pos < buffer->size()) {
        std::string_view line = next_line(*buffer, pos);
        if (line.empty()) {
            continue;
        }

        auto get_val = [&](std::string_view key) {
            std::size_t key_pos = line.find(key);
            if (key_pos == std::string_view::npos) {
                return uint64_t{0};
            }
            key_pos += key.size();
            std::size_t const END = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos));
        };

        std::println("{:>4}  {:>7}  {:>8}  {:>8}  {:>10}  {:>10}  {:>10}  {:>12}  {:>10}  {:>10}  {:>10}  {:>12}  {:>10}", get_val("cpu="),
                     get_val("pa_cache_enabled="), get_val("pa_cache_capacity="), get_val("pa_cache_cached="), get_val("pa_cache_hit="),
                     get_val("pa_cache_miss="), get_val("pa_cache_refill="), get_val("pa_cache_refill_pages="), get_val("pa_cache_free="),
                     get_val("pa_cache_free_miss="), get_val("pa_cache_drain="), get_val("pa_cache_drain_pages="),
                     get_val("pa_cache_stale="));
    }

    std::println("");
    std::println("{:>4}  {:>8}  {:>8}  {:>10}  {:>10}  {:>10}  {:>10}", "CPU", "capacity", "cached", "hit", "miss", "release", "reject");
    std::println("{:->4}  {:->8}  {:->8}  {:->10}  {:->10}  {:->10}  {:->10}", "", "", "", "", "", "", "");

    pos = 0;
    while (pos < buffer->size()) {
        std::string_view line = next_line(*buffer, pos);
        if (line.empty()) {
            continue;
        }

        auto get_val = [&](std::string_view key) {
            std::size_t key_pos = line.find(key);
            if (key_pos == std::string_view::npos) {
                return uint64_t{0};
            }
            key_pos += key.size();
            std::size_t const END = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos));
        };

        std::println("{:>4}  {:>8}  {:>8}  {:>10}  {:>10}  {:>10}  {:>10}", get_val("cpu="), get_val("pt_pool_capacity="),
                     get_val("pt_pool_cached="), get_val("pt_pool_hit="), get_val("pt_pool_miss="), get_val("pt_pool_release="),
                     get_val("pt_pool_reject="));
    }

    std::println("");
    std::println("{:>4}  {:>8}  {:>8}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}", "CPU", "capacity", "entries", "track", "added",
                 "removed", "conflict", "probefail", "purged");
    std::println("{:->4}  {:->8}  {:->8}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}", "", "", "", "", "", "", "", "", "");

    pos = 0;
    while (pos < buffer->size()) {
        std::string_view line = next_line(*buffer, pos);
        if (line.empty()) {
            continue;
        }

        auto get_val = [&](std::string_view key) {
            std::size_t key_pos = line.find(key);
            if (key_pos == std::string_view::npos) {
                return uint64_t{0};
            }
            key_pos += key.size();
            std::size_t const END = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos));
        };

        std::println("{:>4}  {:>8}  {:>8}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}", get_val("cpu="), get_val("of_capacity="),
                     get_val("of_entries="), get_val("of_track="), get_val("of_added="), get_val("of_removed="), get_val("of_conflict="),
                     get_val("of_probe_fail="), get_val("of_purge_removed="));
    }
}

void cmd_contstat() {
    auto buffer = read_file(KCONTSTAT_PATH, CPUSTAT_READ_CAPACITY);
    if (!buffer.has_value() || buffer->empty()) {
        std::println("perf: cannot read /proc/kcontstat (no activity yet?)");
        return;
    }

    std::println("=== perf contstat ==================================================");
    std::println("{:<14}  {:>10}  {:>10}  {:>10}  {:>6}  {:>10}  {:>10}", "subsystem", "inserts", "removes", "resizes", "oom", "peak",
                 "current");
    std::println("{:->14}  {:->10}  {:->10}  {:->10}  {:->6}  {:->10}  {:->10}", "", "", "", "", "", "", "");

    std::size_t pos = 0;
    while (pos < buffer->size()) {
        std::string_view line = next_line(*buffer, pos);
        if (line.empty()) {
            continue;
        }

        auto get_val = [&](std::string_view key) -> uint64_t {
            std::size_t key_pos = line.find(key);
            if (key_pos == std::string_view::npos) {
                return 0;
            }
            key_pos += key.size();
            std::size_t const END = line.find(' ', key_pos);
            return parse_u64(line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos));
        };

        auto get_str = [&](std::string_view key) -> std::string_view {
            std::size_t key_pos = line.find(key);
            if (key_pos == std::string_view::npos) {
                return "?";
            }
            key_pos += key.size();
            std::size_t const END = line.find(' ', key_pos);
            return line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos);
        };

        std::println("{:<14}  {:>10}  {:>10}  {:>10}  {:>6}  {:>10}  {:>10}", get_str("subsys="), get_val("inserts="), get_val("removes="),
                     get_val("resizes="), get_val("oom="), get_val("peak="), get_val("current="));
    }
}

}  // namespace perf
