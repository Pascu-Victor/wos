#include "perf.hpp"
namespace perf {

auto current_proc_map() -> std::vector<ProcMapEntry> {
    std::vector<ProcMapEntry> proc_map;
    for (const auto& stat : collect_stats()) {
        proc_map.push_back(ProcMapEntry{.pid = stat.pid, .comm = stat.comm});
    }
    return proc_map;
}

struct WkiLoadedText {
    std::string source;
    std::string buffer;
    bool sectioned{};
};

auto section_body(std::string_view buffer, std::string_view header, std::string_view footer) -> std::optional<std::string_view> {
    std::size_t const HEADER_POS = buffer.find(header);
    if (HEADER_POS == std::string_view::npos) {
        return std::nullopt;
    }

    std::size_t const BODY_START = HEADER_POS + header.size();
    std::size_t const BODY_END = buffer.find(footer, BODY_START);
    if (BODY_END == std::string_view::npos) {
        return std::nullopt;
    }

    return buffer.substr(BODY_START, BODY_END - BODY_START);
}

auto load_perf_data_section(std::string_view header, std::string_view footer) -> std::optional<std::string> {
    if (access(PERF_DATA_FILE.begin(), R_OK) != 0) {
        return std::nullopt;
    }

    auto saved = read_file(PERF_DATA_FILE);
    if (!saved.has_value() || saved->empty()) {
        return std::nullopt;
    }

    auto body = section_body(*saved, header, footer);
    if (!body.has_value()) {
        return std::nullopt;
    }
    return std::string(*body);
}

auto event_section_view(std::string_view view, bool sectioned) -> std::string_view {
    if (!sectioned) {
        return view;
    }

    std::size_t const EVENTS_HDR = view.find(SECTION_EVENTS);
    if (EVENTS_HDR == std::string_view::npos) {
        return {};
    }

    std::size_t const SECTION_START = view.find('\n', EVENTS_HDR);
    if (SECTION_START == std::string_view::npos) {
        return {};
    }
    return view.substr(SECTION_START + 1);
}

auto collect_wki_events(std::string_view view, bool sectioned) -> std::vector<EventInfo> {
    std::vector<EventInfo> events;
    std::string_view const EVENT_VIEW = event_section_view(view, sectioned);
    std::size_t pos = 0;
    EventInfo event{};
    while (next_event(EVENT_VIEW, pos, sectioned, event)) {
        if (event.type == 'K') {
            events.push_back(event);
        }
    }
    std::ranges::sort(events, [](const EventInfo& lhs, const EventInfo& rhs) { return lhs.ts_ns < rhs.ts_ns; });
    return events;
}

auto load_wki_summary_text() -> std::optional<WkiLoadedText> {
    if (access(PERF_DATA_FILE.begin(), R_OK) == 0) {
        auto saved = read_file(PERF_DATA_FILE, PERF_DRAIN_CAPACITY);
        if (saved.has_value() && !saved->empty()) {
            WkiLoadedText loaded{.source = std::string(PERF_DATA_FILE), .buffer = std::move(*saved)};
            loaded.sectioned = std::string_view(loaded.buffer).starts_with(SECTION_HEADER);
            if (!parse_wki_summary_section(loaded.buffer, loaded.sectioned).empty()) {
                return loaded;
            }
        }
    }

    auto live = read_file(KWKISTAT_PATH, PERF_DRAIN_CAPACITY);
    if (!live.has_value() || live->empty()) {
        return std::nullopt;
    }

    return WkiLoadedText{.source = std::string(KWKISTAT_PATH), .buffer = std::move(*live), .sectioned = false};
}

auto load_ipc_stats_text() -> std::optional<WkiLoadedText> {
    if (access(PERF_DATA_FILE.begin(), R_OK) == 0) {
        auto saved = read_file(PERF_DATA_FILE, PERF_DRAIN_CAPACITY);
        if (saved.has_value() && !saved->empty()) {
            WkiLoadedText loaded{.source = std::string(PERF_DATA_FILE), .buffer = std::move(*saved)};
            loaded.sectioned = std::string_view(loaded.buffer).starts_with(SECTION_HEADER);
            if (parse_ipc_stats_section(loaded.buffer, loaded.sectioned).has_value()) {
                return loaded;
            }
        }
    }

    auto live = read_file(KIPCSTAT_PATH, PROC_READ_CAPACITY);
    if (!live.has_value() || live->empty()) {
        return std::nullopt;
    }

    return WkiLoadedText{.source = std::string(KIPCSTAT_PATH), .buffer = std::move(*live), .sectioned = false};
}

auto load_wki_event_text() -> std::optional<WkiLoadedText> {
    if (access(PERF_DATA_FILE.begin(), R_OK) == 0) {
        auto saved = read_file(PERF_DATA_FILE, PERF_DRAIN_CAPACITY);
        if (saved.has_value() && !saved->empty()) {
            WkiLoadedText loaded{.source = std::string(PERF_DATA_FILE), .buffer = std::move(*saved)};
            loaded.sectioned = std::string_view(loaded.buffer).starts_with(SECTION_HEADER);
            if (!collect_wki_events(loaded.buffer, loaded.sectioned).empty()) {
                return loaded;
            }
        }
    }

    auto live = read_file(KPERF_PATH, PERF_DRAIN_CAPACITY);
    if (!live.has_value() || live->empty()) {
        return std::nullopt;
    }

    return WkiLoadedText{.source = std::string(KPERF_PATH), .buffer = std::move(*live), .sectioned = false};
}

struct WkiSummaryKey {
    std::string scope;
    std::string op;
    uint64_t peer{};
    uint64_t channel{};
};

struct WkiSummaryAccum {
    WkiSummaryKey key;
    uint64_t calls{};
    uint64_t errors{};
    uint64_t retries{};
    uint64_t total_us{};
    std::vector<uint32_t> samples;
};

auto wki_view_name(WkiDataView view) -> std::string_view {
    switch (view) {
        case WkiDataView::WKI:
            return "wki";
        case WkiDataView::LOCAL:
            return "local";
        case WkiDataView::VMEM:
            return "vmem";
        case WkiDataView::ALL:
            return "all";
        default:
            return "wki";
    }
}

auto is_local_data_scope(std::string_view scope) -> bool { return scope.starts_with("local_"); }

auto summary_peer_for_event(const EventInfo& event) -> uint64_t {
    if (event.scope_name == "local_vmem" || event.scope_name == "local_loader" || event.scope_name == "local_xfs") {
        return 0;
    }
    return event.peer;
}

auto summary_channel_for_event(const EventInfo& event) -> uint64_t {
    if (event.scope_name == "local_vmem" || event.scope_name == "local_loader" || event.scope_name == "local_xfs") {
        return 0;
    }
    return event.channel;
}

auto scope_matches_view(std::string_view scope, const WkiTraceFilter& filter, WkiDataView view) -> bool {
    if (!filter.scope.empty()) {
        return true;
    }
    if (view == WkiDataView::ALL) {
        return true;
    }
    if (view == WkiDataView::VMEM) {
        return scope == "local_vmem";
    }
    bool const IS_LOCAL = is_local_data_scope(scope);
    return view == WkiDataView::LOCAL ? IS_LOCAL : !IS_LOCAL;
}

auto wki_summary_matches(const WkiSummaryRow& row, const WkiTraceFilter& filter) -> bool {
    if (!filter.scope.empty() && row.scope != filter.scope) {
        return false;
    }
    if (!filter.op.empty() && row.op != filter.op) {
        return false;
    }
    if (filter.peer.has_value() && row.peer != *filter.peer) {
        return false;
    }
    if (filter.channel.has_value() && row.channel != *filter.channel) {
        return false;
    }
    if (filter.min_us.has_value() && row.max_us < *filter.min_us) {
        return false;
    }
    return true;
}

auto view_summary_matches(const WkiSummaryRow& row, const WkiTraceFilter& filter, WkiDataView view) -> bool {
    if (!scope_matches_view(row.scope, filter, view)) {
        return false;
    }
    return wki_summary_matches(row, filter);
}

auto is_default_ipc_scope(std::string_view scope) -> bool { return scope == "remote_ipc" || scope == "local_pipe"; }

auto ipc_summary_matches(const WkiSummaryRow& row, const WkiTraceFilter& filter) -> bool {
    if (filter.scope.empty() && !is_default_ipc_scope(row.scope)) {
        return false;
    }
    return wki_summary_matches(row, filter);
}

auto wki_summary_event_matches(const EventInfo& event, const WkiTraceFilter& filter) -> bool {
    if (event.type != 'K') {
        return false;
    }
    if (event.phase_name != "end" && event.phase_name != "point") {
        return false;
    }
    return wki_trace_matches(event, filter);
}

auto view_trace_matches(const EventInfo& event, const WkiTraceFilter& filter, WkiDataView view) -> bool {
    if (!scope_matches_view(event.scope_name, filter, view)) {
        return false;
    }
    return wki_trace_matches(event, filter);
}

auto view_summary_event_matches(const EventInfo& event, const WkiTraceFilter& filter, WkiDataView view) -> bool {
    if (event.type != 'K') {
        return false;
    }
    if (event.phase_name != "end" && event.phase_name != "point") {
        return false;
    }
    return view_trace_matches(event, filter, view);
}

auto ipc_summary_event_matches(const EventInfo& event, const WkiTraceFilter& filter) -> bool {
    if (filter.scope.empty() && !is_default_ipc_scope(event.scope_name)) {
        return false;
    }
    return wki_summary_event_matches(event, filter);
}

auto percentile_from_sorted(const std::vector<uint32_t>& sorted, uint64_t numerator, uint64_t denominator) -> uint64_t {
    if (sorted.empty()) {
        return 0;
    }
    if (sorted.size() == 1) {
        return sorted.front();
    }
    uint64_t const N = sorted.size() - 1;
    uint64_t const INDEX = (N * numerator) / denominator;
    return sorted.at(static_cast<std::size_t>(INDEX));
}

auto build_wki_summary_from_events(const std::vector<EventInfo>& events, const WkiTraceFilter& filter, WkiDataView view = WkiDataView::ALL)
    -> std::vector<WkiSummaryRow> {
    std::vector<WkiSummaryAccum> accums;

    auto find_accum = [&](const EventInfo& event) -> WkiSummaryAccum& {
        uint64_t const SUMMARY_PEER = summary_peer_for_event(event);
        uint64_t const SUMMARY_CHANNEL = summary_channel_for_event(event);
        for (auto& accum : accums) {
            if (accum.key.scope == event.scope_name && accum.key.op == event.op_name && accum.key.peer == SUMMARY_PEER &&
                accum.key.channel == SUMMARY_CHANNEL) {
                return accum;
            }
        }

        WkiSummaryAccum accum{};
        accum.key.scope = event.scope_name;
        accum.key.op = event.op_name;
        accum.key.peer = SUMMARY_PEER;
        accum.key.channel = SUMMARY_CHANNEL;
        accums.push_back(std::move(accum));
        return accums.back();
    };

    for (const auto& event : events) {
        if (!view_summary_event_matches(event, filter, view)) {
            continue;
        }
        auto& accum = find_accum(event);
        accum.calls++;
        if (event.status < 0) {
            accum.errors++;
        }
        accum.total_us += event.aux;
        accum.samples.push_back(static_cast<uint32_t>(std::min<uint64_t>(event.aux, std::numeric_limits<uint32_t>::max())));
    }

    std::vector<WkiSummaryRow> rows;
    rows.reserve(accums.size());
    for (auto& accum : accums) {
        if (accum.calls == 0) {
            continue;
        }
        std::ranges::sort(accum.samples);

        WkiSummaryRow row{};
        row.scope = std::move(accum.key.scope);
        row.op = std::move(accum.key.op);
        row.peer = accum.key.peer;
        row.channel = accum.key.channel;
        row.calls = accum.calls;
        row.errors = accum.errors;
        row.retries = accum.retries;
        row.samples = accum.samples.size();
        row.total_us = accum.total_us;
        row.avg_us = accum.calls != 0 ? accum.total_us / accum.calls : 0;
        row.max_us = accum.samples.empty() ? 0 : accum.samples.back();
        row.p50_us = percentile_from_sorted(accum.samples, 50, 100);
        row.p95_us = percentile_from_sorted(accum.samples, 95, 100);
        row.p99_us = percentile_from_sorted(accum.samples, 99, 100);
        row.p999_us = percentile_from_sorted(accum.samples, 999, 1000);
        row.p9999_us = percentile_from_sorted(accum.samples, 9999, 10000);
        row.p99999_us = percentile_from_sorted(accum.samples, 99999, 100000);
        rows.push_back(std::move(row));
    }

    return rows;
}

auto wki_trace_matches(const EventInfo& event, const WkiTraceFilter& filter) -> bool {
    if (!filter.scope.empty() && event.scope_name != filter.scope) {
        return false;
    }
    if (!filter.op.empty() && event.op_name != filter.op) {
        return false;
    }
    if (!filter.phase.empty() && event.phase_name != filter.phase) {
        return false;
    }
    if (filter.peer.has_value() && event.peer != *filter.peer) {
        return false;
    }
    if (filter.channel.has_value() && event.channel != *filter.channel) {
        return false;
    }
    if (filter.correlation.has_value() && event.correlation != *filter.correlation) {
        return false;
    }
    if (filter.pid.has_value() && event.pid != *filter.pid) {
        return false;
    }
    if (filter.min_us.has_value() && event.aux < *filter.min_us) {
        return false;
    }
    if (filter.from_ns.has_value() && event.ts_ns < *filter.from_ns) {
        return false;
    }
    if (filter.to_ns.has_value() && event.ts_ns > *filter.to_ns) {
        return false;
    }
    return true;
}

auto is_wki_launch_event_op(std::string_view op) -> bool {
    return op == "submit_inline" || op == "submit_vfs_ref" || op == "complete_wait" || op == "accept" || op == "reject" ||
           op == "complete" || op == "proxy_ready" || op == "defer_wait" || op == "load_elf" || op == "handle_submit" ||
           op == "task_runtime" || op == "proxy_ready_wait" || op == "complete_hold";
}

auto is_wki_launch_summary_op(std::string_view op) -> bool {
    return op == "submit_inline" || op == "submit_vfs_ref" || op == "complete_wait" || op == "defer_wait" || op == "load_elf" ||
           op == "handle_submit" || op == "task_runtime" || op == "proxy_ready_wait" || op == "complete_hold";
}

auto find_wki_launch_row(std::vector<WkiLaunchRow>& rows, uint64_t peer, uint64_t correlation) -> WkiLaunchRow& {
    for (auto& row : rows) {
        if (row.peer == peer && row.correlation == correlation) {
            return row;
        }
    }

    WkiLaunchRow row{};
    row.peer = peer;
    row.correlation = correlation;
    rows.push_back(row);
    return rows.back();
}

auto format_optional_us(const std::optional<uint32_t>& value) -> std::string {
    return value.has_value() ? std::to_string(*value) : std::string("-");
}

auto format_wki_launch_result(const WkiLaunchRow& row) -> std::string {
    if (row.rejected) {
        if (row.submit_status.has_value()) {
            return std::string("reject(") + std::to_string(*row.submit_status) + ')';
        }
        if (row.handle_status.has_value()) {
            return std::string("reject(") + std::to_string(*row.handle_status) + ')';
        }
        return "reject";
    }
    if (row.completed) {
        if (row.complete_status.has_value()) {
            return std::string("complete(") + std::to_string(*row.complete_status) + ')';
        }
        return "complete";
    }
    if (row.accepted) {
        return row.proxy_ready ? "accepted" : "accept";
    }
    if (row.submit_status.has_value() && *row.submit_status < 0) {
        return std::string("submit(") + std::to_string(*row.submit_status) + ')';
    }
    if (row.handle_status.has_value() && *row.handle_status < 0) {
        return std::string("handle(") + std::to_string(*row.handle_status) + ')';
    }
    if (row.load_status.has_value() && *row.load_status < 0) {
        return std::string("load(") + std::to_string(*row.load_status) + ')';
    }
    return "inflight";
}

auto format_scaled_bytes(uint64_t bytes) -> std::string {
    if (bytes >= 1024ULL * 1024ULL) {
        constexpr uint64_t MIB = 1024ULL * 1024ULL;
        uint64_t whole = bytes / MIB;
        uint64_t frac = (((bytes % MIB) * 10U) + (MIB / 2U)) / MIB;
        if (frac >= 10U) {
            whole++;
            frac = 0;
        }
        return std::to_string(whole) + "." + std::to_string(frac) + "MiB";
    }
    if (bytes >= 1024ULL) {
        constexpr uint64_t KIB = 1024ULL;
        uint64_t whole = bytes / KIB;
        uint64_t frac = (((bytes % KIB) * 10U) + (KIB / 2U)) / KIB;
        if (frac >= 10U) {
            whole++;
            frac = 0;
        }
        return std::to_string(whole) + "." + std::to_string(frac) + "KiB";
    }
    return std::to_string(bytes) + "B";
}

auto format_hundredths(uint64_t scaled) -> std::string {
    return std::to_string(scaled / 100U) + "." + (scaled % 100U < 10U ? "0" : "") + std::to_string(scaled % 100U);
}

auto format_mib_per_s(uint64_t bytes, uint64_t total_us) -> std::string {
    if (bytes == 0 || total_us == 0) {
        return "0.00";
    }

    constexpr uint64_t BYTES_PER_MIB = 1024ULL * 1024ULL;
    constexpr uint64_t HUNDREDTHS_PER_UNIT = 100ULL;
    constexpr uint64_t MICROSECONDS_PER_SECOND_U64 = 1000000ULL;
    auto numerator = static_cast<unsigned __int128>(bytes) * HUNDREDTHS_PER_UNIT * MICROSECONDS_PER_SECOND_U64;
    auto denominator = static_cast<unsigned __int128>(total_us) * BYTES_PER_MIB;
    auto scaled = static_cast<uint64_t>((numerator + (denominator / 2U)) / denominator);
    return format_hundredths(scaled);
}

auto ratio_percent(uint64_t numerator, uint64_t denominator) -> double {
    if (denominator == 0) {
        return 0.0;
    }
    return (static_cast<double>(numerator) * 100.0) / static_cast<double>(denominator);
}

auto xfs_total_ms(const WkiSummaryRow& row) -> uint64_t { return (row.total_us + 500U) / 1000U; }

auto row_jitter_us(const WkiSummaryRow& row) -> uint64_t {
    if (row.p50_us != 0 && row.p99_us >= row.p50_us) {
        return row.p99_us - row.p50_us;
    }
    if (row.p95_us != 0 && row.p99_us >= row.p95_us) {
        return row.p99_us - row.p95_us;
    }
    return 0;
}

void print_ipc_memory_snapshot(const std::optional<WkiLoadedText>& loaded) {
    if (!loaded.has_value()) {
        std::println("perf: no IPC memory snapshot available from perf.data or {}", KIPCSTAT_PATH);
        return;
    }

    auto snapshot = parse_ipc_stats_section(loaded->buffer, loaded->sectioned);
    if (!snapshot.has_value()) {
        std::println("perf: no IPC memory snapshot found in {}", loaded->source);
        return;
    }

    double ring_pct = 0.0;
    if (snapshot->ring_bytes != 0) {
        ring_pct = (static_cast<double>(snapshot->ring_used) * 100.0) / static_cast<double>(snapshot->ring_bytes);
    }

    uint64_t const TOTAL_APPROX_ALLOC = snapshot->approx_alloc_bytes + snapshot->local_pipe_approx_alloc_bytes;

    std::println("=== perf ipc memory [{}] ============================================", loaded->source);
    std::println("total_approx={}  remote_approx={}  local_pipe_approx={}", format_scaled_bytes(TOTAL_APPROX_ALLOC),
                 format_scaled_bytes(snapshot->approx_alloc_bytes), format_scaled_bytes(snapshot->local_pipe_approx_alloc_bytes));
    std::println("remote: exports={} proxies={} pumps={}  ring={}/{} ({:.1f}%)", snapshot->exports, snapshot->proxies, snapshot->pump_tasks,
                 format_scaled_bytes(snapshot->ring_used), format_scaled_bytes(snapshot->ring_bytes), ring_pct);
    std::println("remote: pending={} chunks={} bytes={}  backlog={} chunks={} bytes={}  devq={} payload={}", snapshot->pending_deliveries,
                 snapshot->pending_chunks, format_scaled_bytes(snapshot->pending_bytes), snapshot->export_backlogs,
                 snapshot->export_backlog_chunks, format_scaled_bytes(snapshot->export_backlog_bytes), snapshot->dev_op_queue,
                 format_scaled_bytes(snapshot->dev_op_payload_bytes));
    std::println("remote: blocked_readers={} poll_waiters={} export_flush_queue={}", snapshot->blocked_readers, snapshot->poll_waiters,
                 snapshot->export_flush_queue);
    std::println("remote: pipe_payload={} proxy_write={} no_credit_waits={} write_block_us={} rdma_full_waits={}",
                 format_scaled_bytes(snapshot->pipe_payload_bytes), format_scaled_bytes(snapshot->proxy_write_payload_bytes),
                 snapshot->proxy_write_no_credit_waits, snapshot->proxy_write_block_us, snapshot->proxy_pipe_rdma_full_waits);
    std::println("remote: proxy_ring_full_waits={} proxy_ring_full_bytes={}", snapshot->proxy_ring_full_waits,
                 format_scaled_bytes(snapshot->proxy_ring_full_bytes));
    std::println("local_pipe: active={} created={} peak={}  capacity={} peak_capacity={} buffered={}", snapshot->local_pipe_active,
                 snapshot->local_pipe_created, snapshot->local_pipe_peak, format_scaled_bytes(snapshot->local_pipe_capacity),
                 format_scaled_bytes(snapshot->local_pipe_peak_capacity), format_scaled_bytes(snapshot->local_pipe_buffered));
    std::println("local_pipe: readers={} writers={} poll_waiters={} direct={} read_closed={} write_closed={}",
                 snapshot->local_pipe_reader_waiters, snapshot->local_pipe_writer_waiters, snapshot->local_pipe_poll_waiters,
                 snapshot->local_pipe_direct_writes, snapshot->local_pipe_read_closed, snapshot->local_pipe_write_closed);
    std::println("");
}

auto wki_launch_score(const WkiLaunchRow& row) -> uint32_t {
    uint32_t score = 0;
    if (row.submit_total_us.has_value()) {
        score = std::max(score, *row.submit_total_us);
    }
    if (row.handle_submit_us.has_value()) {
        score = std::max(score, *row.handle_submit_us);
    }
    if (row.load_elf_us.has_value()) {
        score = std::max(score, *row.load_elf_us);
    }
    if (row.defer_wait_us.has_value()) {
        score = std::max(score, *row.defer_wait_us);
    }
    if (row.task_runtime_us.has_value()) {
        score = std::max(score, *row.task_runtime_us);
    }
    if (row.proxy_ready_wait_us.has_value()) {
        score = std::max(score, *row.proxy_ready_wait_us);
    }
    if (row.complete_hold_us.has_value()) {
        score = std::max(score, *row.complete_hold_us);
    }
    if (row.complete_wait_us.has_value()) {
        score = std::max(score, *row.complete_wait_us);
    }
    return score;
}

void cmd_wki_launch(int limit, const WkiDisplayOptions& display_options) {
    if (limit < 1) {
        limit = DEFAULT_WKI_LAUNCH_ROWS;
    }

    auto summary_loaded = load_wki_summary_text();
    std::vector<WkiSummaryRow> summary_rows;
    if (summary_loaded.has_value()) {
        auto rows = parse_wki_summary_section(summary_loaded->buffer, summary_loaded->sectioned);
        for (const auto& row : rows) {
            if (row.scope == "remote_compute" && is_wki_launch_summary_op(row.op)) {
                summary_rows.push_back(row);
            }
        }
        std::ranges::sort(summary_rows, [](const WkiSummaryRow& lhs, const WkiSummaryRow& rhs) {
            if (lhs.p99_us != rhs.p99_us) {
                return lhs.p99_us > rhs.p99_us;
            }
            if (lhs.max_us != rhs.max_us) {
                return lhs.max_us > rhs.max_us;
            }
            return lhs.calls > rhs.calls;
        });

        if (!summary_rows.empty()) {
            std::println("=== perf wki-launch summary [{}] ====================================", summary_loaded->source);
            std::println("{:<16}  {:>7}  {:>6}  {:>9}  {:>9}  {:>9}  {:>9}", "OP", "CALLS", "ERR", "AVG(us)", "P99(us)", "P999(us)",
                         "MAX(us)");
            std::println("{:->16}  {:->7}  {:->6}  {:->9}  {:->9}  {:->9}  {:->9}", "", "", "", "", "", "", "");
            for (const auto& row : summary_rows) {
                std::println("{:<16}  {:>7}  {:>6}  {:>9}  {:>9}  {:>9}  {:>9}", row.op, row.calls, row.errors, row.avg_us, row.p99_us,
                             row.p999_us, row.max_us);
            }
        }
    }

    auto event_loaded = load_wki_event_text();
    if (!event_loaded.has_value()) {
        if (!summary_rows.empty()) {
            std::println("");
            std::println("perf: no raw WKI launch events available in perf.data or /proc/kperf");
            return;
        }
        std::println("perf: no WKI launch data");
        return;
    }

    auto events = collect_wki_events(event_loaded->buffer, event_loaded->sectioned);
    auto peer_resolver = make_wki_peer_resolver(event_loaded->buffer, event_loaded->sectioned, display_options);
    std::vector<WkiLaunchRow> launch_rows;
    for (const auto& event : events) {
        if (event.scope_name != "remote_compute" || !is_wki_launch_event_op(event.op_name)) {
            continue;
        }

        auto& row = find_wki_launch_row(launch_rows, event.peer, event.correlation);
        if (!row.first_ts_ns.has_value() || event.ts_ns < *row.first_ts_ns) {
            row.first_ts_ns = event.ts_ns;
        }
        if (event.phase_name == "end") {
            if (event.op_name == "submit_vfs_ref" || event.op_name == "submit_inline") {
                row.submit_total_us = event.aux;
                row.submit_status = event.status;
            } else if (event.op_name == "handle_submit") {
                row.handle_submit_us = event.aux;
                row.handle_status = event.status;
            } else if (event.op_name == "load_elf") {
                row.load_elf_us = event.aux;
                row.load_status = event.status;
            } else if (event.op_name == "defer_wait") {
                row.defer_wait_us = event.aux;
            } else if (event.op_name == "task_runtime") {
                row.task_runtime_us = event.aux;
                row.completed = true;
                row.complete_status = event.status;
            } else if (event.op_name == "proxy_ready_wait") {
                row.proxy_ready_wait_us = event.aux;
            } else if (event.op_name == "complete_hold") {
                row.complete_hold_us = event.aux;
            } else if (event.op_name == "complete_wait") {
                row.complete_wait_us = event.aux;
            }
        } else if (event.phase_name == "point") {
            if (event.op_name == "accept") {
                row.accepted = true;
            } else if (event.op_name == "reject") {
                row.rejected = true;
                row.submit_status = event.status;
            } else if (event.op_name == "complete") {
                row.completed = true;
                row.complete_status = event.status;
            } else if (event.op_name == "proxy_ready") {
                row.proxy_ready = true;
            }
        }
    }

    if (launch_rows.empty()) {
        if (!summary_rows.empty()) {
            std::println("");
            std::println("perf: no raw WKI launch rows found in {}", event_loaded->source);
            return;
        }
        std::println("perf: no WKI launch rows found in {}", event_loaded->source);
        return;
    }

    std::ranges::sort(launch_rows,
                      [](const WkiLaunchRow& lhs, const WkiLaunchRow& rhs) { return wki_launch_score(lhs) > wki_launch_score(rhs); });

    std::println("");
    std::println("=== slowest WKI launches [{}] ======================================", event_loaded->source);
    auto const TIME_DISPLAY = make_time_display(display_options, event_loaded->buffer, event_loaded->sectioned);
    int const TIME_WIDTH = time_column_width(TIME_DISPLAY);
    std::println("{:<{}}  {:<18}  {:>8}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {}",
                 time_column_header(TIME_DISPLAY), TIME_WIDTH, "PEER", "CORR", "TOTAL(us)", "HANDLE(us)", "LOAD(us)", "SETUP(us)",
                 "QUEUE(us)", "RUN(us)", "READY(us)", "HOLD(us)", "WAIT(us)", "RESULT");
    std::println("{:->{}}  {:->18}  {:->8}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->10}  {:->12}", "",
                 TIME_WIDTH, "", "", "", "", "", "", "", "", "", "", "", "");
    for (int i = 0; std::cmp_less(i, launch_rows.size()) && i < limit; ++i) {
        const auto& row = launch_rows.at(static_cast<std::size_t>(i));
        std::optional<uint32_t> setup_us;
        if (row.handle_submit_us.has_value() && row.load_elf_us.has_value() && *row.handle_submit_us >= *row.load_elf_us) {
            setup_us = *row.handle_submit_us - *row.load_elf_us;
        }
        std::println("{:<{}}  {:<18}  {:>8}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {:>10}  {}",
                     format_optional_event_time(row.first_ts_ns, TIME_DISPLAY), TIME_WIDTH, wki_peer_label(row.peer, peer_resolver),
                     row.correlation, format_optional_us(row.submit_total_us), format_optional_us(row.handle_submit_us),
                     format_optional_us(row.load_elf_us), format_optional_us(setup_us), format_optional_us(row.defer_wait_us),
                     format_optional_us(row.task_runtime_us), format_optional_us(row.proxy_ready_wait_us),
                     format_optional_us(row.complete_hold_us), format_optional_us(row.complete_wait_us), format_wki_launch_result(row));
    }
}

void cmd_ipc_report(int limit, WkiTraceFilter filter, const WkiDisplayOptions& display_options) {
    if (limit < 1) {
        limit = DEFAULT_WKI_TAIL_ROWS;
    }

    auto ipc_stats_loaded = load_ipc_stats_text();
    print_ipc_memory_snapshot(ipc_stats_loaded);

    std::string summary_source;
    std::vector<WkiSummaryRow> rows;
    auto summary_loaded = load_wki_summary_text();
    if (summary_loaded.has_value()) {
        summary_source = summary_loaded->source;
        rows = parse_wki_summary_section(summary_loaded->buffer, summary_loaded->sectioned);
        std::erase_if(rows, [&](const WkiSummaryRow& row) { return !ipc_summary_matches(row, filter); });
    }

    auto event_loaded = load_wki_event_text();
    if (rows.empty() && event_loaded.has_value()) {
        summary_source = event_loaded->source;
        rows = build_wki_summary_from_events(collect_wki_events(event_loaded->buffer, event_loaded->sectioned), filter);
        std::erase_if(rows, [&](const WkiSummaryRow& row) { return !ipc_summary_matches(row, filter); });
    }

    if (rows.empty()) {
        std::println("perf: no IPC summary rows matched the requested filters (default scopes: remote_ipc,local_pipe)");
        return;
    }

    std::ranges::sort(rows, [](const WkiSummaryRow& lhs, const WkiSummaryRow& rhs) {
        if (lhs.p99_us != rhs.p99_us) {
            return lhs.p99_us > rhs.p99_us;
        }
        if (lhs.max_us != rhs.max_us) {
            return lhs.max_us > rhs.max_us;
        }
        return lhs.calls > rhs.calls;
    });

    WkiPeerResolver summary_peer_resolver{};
    if (summary_loaded.has_value()) {
        summary_peer_resolver = make_wki_peer_resolver(summary_loaded->buffer, summary_loaded->sectioned, display_options);
    } else if (event_loaded.has_value()) {
        summary_peer_resolver = make_wki_peer_resolver(event_loaded->buffer, event_loaded->sectioned, display_options);
    } else {
        summary_peer_resolver = make_wki_peer_resolver({}, false, display_options);
    }

    std::println("=== perf ipc-report [{}] ============================================", summary_source);
    std::println("{:<12}  {:<16}  {:<18}  {:>4}  {:>7}  {:>6}  {:>10}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}", "SCOPE", "OP", "PEER",
                 "CH", "CALLS", "ERR", "BYTES", "AVG(us)", "P50(us)", "P99(us)", "JIT(us)", "MAX(us)", "MiB/s");
    std::println("{:->12}  {:->16}  {:->18}  {:->4}  {:->7}  {:->6}  {:->10}  {:->9}  {:->9}  {:->9}  {:->9}  {:->9}  {:->9}", "", "", "",
                 "", "", "", "", "", "", "", "", "", "");
    for (int i = 0; std::cmp_less(i, rows.size()) && i < limit; ++i) {
        const auto& row = rows.at(static_cast<std::size_t>(i));
        std::println("{:<12}  {:<16}  {:<18}  {:>4}  {:>7}  {:>6}  {:>10}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}", row.scope, row.op,
                     wki_peer_label(row.peer, summary_peer_resolver), row.channel, row.calls, row.errors, format_scaled_bytes(row.bytes),
                     row.avg_us, row.p50_us, row.p99_us, row_jitter_us(row), row.max_us, format_mib_per_s(row.bytes, row.total_us));
    }

    if (!event_loaded.has_value()) {
        return;
    }

    auto events = collect_wki_events(event_loaded->buffer, event_loaded->sectioned);
    std::vector<EventInfo> slow_events;
    for (const auto& event : events) {
        if (ipc_summary_event_matches(event, filter) && event.aux > 0) {
            slow_events.push_back(event);
        }
    }
    if (slow_events.empty()) {
        return;
    }
    std::ranges::sort(slow_events, [](const EventInfo& lhs, const EventInfo& rhs) { return lhs.aux > rhs.aux; });

    auto event_peer_resolver = make_wki_peer_resolver(event_loaded->buffer, event_loaded->sectioned, display_options);
    auto const TIME_DISPLAY = make_time_display(display_options, event_loaded->buffer, event_loaded->sectioned);
    int const TIME_WIDTH = time_column_width(TIME_DISPLAY);
    std::println("");
    std::println("=== slowest IPC events [{}] ========================================", event_loaded->source);
    std::println("{:<{}}  {:<12}  {:<16}  {:<6}  {:<18}  {:>4}  {:>8}  {:>8}  {:>10}  {}", time_column_header(TIME_DISPLAY), TIME_WIDTH,
                 "SCOPE", "OP", "PHASE", "PEER", "CH", "AUX(us)", "STATUS", "CORR", "DETAILS");
    std::println("{:->{}}  {:->12}  {:->16}  {:->6}  {:->18}  {:->4}  {:->8}  {:->8}  {:->10}  {:->24}", "", TIME_WIDTH, "", "", "", "", "",
                 "", "", "", "");
    for (int i = 0; std::cmp_less(i, slow_events.size()) && i < limit; ++i) {
        const auto& event = slow_events.at(static_cast<std::size_t>(i));
        std::println("{:<{}}  {:<12}  {:<16}  {:<6}  {:<18}  {:>4}  {:>8}  {:>8}  {:>10}  {}", format_event_time(event.ts_ns, TIME_DISPLAY),
                     TIME_WIDTH, event.scope_name, event.op_name, event.phase_name, wki_peer_label(event.peer, event_peer_resolver),
                     event.channel, event.aux, event.status, event.correlation, display_callsite(event.callsite));
    }
}

void cmd_wki_report(const WkiTraceFilter& filter, const WkiDisplayOptions& display_options, WkiDataView view) {
    std::string source;
    std::vector<WkiSummaryRow> rows;
    std::optional<WkiLoadedText> event_loaded;
    std::optional<WkiLoadedText> summary_loaded;
    bool const PREFER_SAVED_SUMMARY =
        view == WkiDataView::VMEM || filter.scope == "local_vmem" || filter.scope == "local_loader" || filter.scope == "local_xfs";

    auto load_summary_rows = [&]() {
        summary_loaded = load_wki_summary_text();
        if (!summary_loaded.has_value()) {
            return;
        }
        source = summary_loaded->source;
        rows = parse_wki_summary_section(summary_loaded->buffer, summary_loaded->sectioned);
        std::erase_if(rows, [&](const WkiSummaryRow& row) { return !view_summary_matches(row, filter, view); });
    };

    if (PREFER_SAVED_SUMMARY) {
        load_summary_rows();
    }
    if (rows.empty()) {
        event_loaded = load_wki_event_text();
        if (event_loaded.has_value()) {
            source = event_loaded->source;
            rows = build_wki_summary_from_events(collect_wki_events(event_loaded->buffer, event_loaded->sectioned), filter, view);
        }
    }
    if (rows.empty() && !PREFER_SAVED_SUMMARY) {
        load_summary_rows();
        if (!summary_loaded.has_value()) {
            std::println("perf: no {} summary data", wki_view_name(view));
            return;
        }
    }

    if (rows.empty()) {
        std::println("perf: no {} summary rows matched the requested filters", wki_view_name(view));
        return;
    }

    std::ranges::sort(rows, [](const WkiSummaryRow& lhs, const WkiSummaryRow& rhs) {
        if (lhs.p99_us != rhs.p99_us) {
            return lhs.p99_us > rhs.p99_us;
        }
        if (lhs.max_us != rhs.max_us) {
            return lhs.max_us > rhs.max_us;
        }
        return lhs.calls > rhs.calls;
    });

    WkiPeerResolver peer_resolver{};
    if (event_loaded.has_value()) {
        peer_resolver = make_wki_peer_resolver(event_loaded->buffer, event_loaded->sectioned, display_options);
    } else if (summary_loaded.has_value()) {
        peer_resolver = make_wki_peer_resolver(summary_loaded->buffer, summary_loaded->sectioned, display_options);
    } else {
        peer_resolver = make_wki_peer_resolver({}, false, display_options);
    }
    std::println("=== perf {}-report [{}] ============================================", wki_view_name(view), source);
    std::println("{:<14}  {:<16}  {:<18}  {:>4}  {:>7}  {:>6}  {:>7}  {:>10}  {:>9}  {:>9}  {:>9}  {:>9}", "SCOPE", "OP", "PEER", "CH",
                 "CALLS", "ERR", "RETRY", "BYTES", "AVG(us)", "P99(us)", "P999(us)", "MAX(us)");
    std::println("{:->14}  {:->16}  {:->18}  {:->4}  {:->7}  {:->6}  {:->7}  {:->10}  {:->9}  {:->9}  {:->9}  {:->9}", "", "", "", "", "",
                 "", "", "", "", "", "", "");
    for (const auto& row : rows) {
        std::println("{:<14}  {:<16}  {:<18}  {:>4}  {:>7}  {:>6}  {:>7}  {:>10}  {:>9}  {:>9}  {:>9}  {:>9}", row.scope, row.op,
                     wki_peer_label(row.peer, peer_resolver), row.channel, row.calls, row.errors, row.retries,
                     format_scaled_bytes(row.bytes), row.avg_us, row.p99_us, row.p999_us, row.max_us);
    }
}

void cmd_vmem_report(WkiTraceFilter filter) {
    if (filter.scope.empty()) {
        filter.scope = "local_vmem";
    }

    std::string source;
    std::vector<WkiSummaryRow> rows;

    auto loaded = load_wki_summary_text();
    if (loaded.has_value()) {
        source = loaded->source;
        rows = parse_wki_summary_section(loaded->buffer, loaded->sectioned);
        std::erase_if(rows, [&](const WkiSummaryRow& row) { return !view_summary_matches(row, filter, WkiDataView::VMEM); });
    }

    if (rows.empty()) {
        auto event_loaded = load_wki_event_text();
        if (event_loaded.has_value()) {
            source = event_loaded->source;
            rows =
                build_wki_summary_from_events(collect_wki_events(event_loaded->buffer, event_loaded->sectioned), filter, WkiDataView::VMEM);
        }
    }

    if (rows.empty()) {
        std::println("perf: no vmem rows matched the requested filters");
        return;
    }

    std::ranges::sort(rows, [](const WkiSummaryRow& lhs, const WkiSummaryRow& rhs) {
        if (lhs.p99_us != rhs.p99_us) {
            return lhs.p99_us > rhs.p99_us;
        }
        if (lhs.max_us != rhs.max_us) {
            return lhs.max_us > rhs.max_us;
        }
        return lhs.calls > rhs.calls;
    });

    std::println("=== perf vmem-report [{}] ===========================================", source);
    std::println("{:<18}  {:>7}  {:>6}  {:>11}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}", "OP", "CALLS", "ERR", "BYTES", "PAGES",
                 "AVG(us)", "P50(us)", "P99(us)", "P999(us)", "MAX(us)");
    std::println("{:->18}  {:->7}  {:->6}  {:->11}  {:->9}  {:->9}  {:->9}  {:->9}  {:->9}  {:->9}", "", "", "", "", "", "", "", "", "",
                 "");
    for (const auto& row : rows) {
        uint64_t const PAGES = row.bytes / PERF_PAGE_SIZE;
        std::println("{:<18}  {:>7}  {:>6}  {:>11}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}  {:>9}", row.op, row.calls, row.errors,
                     format_scaled_bytes(row.bytes), PAGES, row.avg_us, row.p50_us, row.p99_us, row.p999_us, row.max_us);
    }
}

auto local_xfs_rows_from_summary(std::vector<WkiSummaryRow> rows) -> std::vector<WkiSummaryRow> {
    std::erase_if(rows, [](const WkiSummaryRow& row) { return row.scope != "local_xfs"; });
    return rows;
}

auto load_checkout_xfs_rows() -> CheckoutSummaryRows {
    if (access(PERF_DATA_FILE.begin(), R_OK) == 0) {
        auto saved = read_file(PERF_DATA_FILE);
        if (saved.has_value() && !saved->empty()) {
            bool const SECTIONED = std::string_view(*saved).starts_with(SECTION_HEADER);
            auto rows = local_xfs_rows_from_summary(parse_wki_summary_section(*saved, SECTIONED));
            if (rows.empty()) {
                rows = build_wki_summary_from_events(collect_wki_events(*saved, SECTIONED), WkiTraceFilter{}, WkiDataView::LOCAL);
                rows = local_xfs_rows_from_summary(std::move(rows));
            }
            if (!rows.empty()) {
                return CheckoutSummaryRows{.source = std::string(PERF_DATA_FILE), .rows = std::move(rows)};
            }
        }
    }

    auto live = read_file(KWKISTAT_PATH, PERF_DRAIN_CAPACITY);
    if (!live.has_value() || live->empty()) {
        return {};
    }

    return CheckoutSummaryRows{
        .source = std::string(KWKISTAT_PATH),
        .rows = local_xfs_rows_from_summary(parse_wki_summary_section(*live, false)),
    };
}

auto find_xfs_row(const std::vector<WkiSummaryRow>& rows, std::string_view op) -> const WkiSummaryRow* {
    for (const auto& row : rows) {
        if (row.op == op) {
            return &row;
        }
    }
    return nullptr;
}

void print_checkout_xfs_table(const CheckoutSummaryRows& loaded, int limit) {
    if (loaded.rows.empty()) {
        std::println("=== perf checkout-report xfs ========================================");
        std::println("perf: no local_xfs summary rows found in {} or {}", PERF_DATA_FILE, KWKISTAT_PATH);
        std::println("hint: capture checkout with `perf record <ms> --filter=xfs` or `perf run --filter=xfs <cmd>`");
        return;
    }

    std::vector<WkiSummaryRow> rows = loaded.rows;
    std::ranges::sort(rows, [](const WkiSummaryRow& lhs, const WkiSummaryRow& rhs) {
        if (lhs.total_us != rhs.total_us) {
            return lhs.total_us > rhs.total_us;
        }
        if (lhs.p99_us != rhs.p99_us) {
            return lhs.p99_us > rhs.p99_us;
        }
        if (lhs.max_us != rhs.max_us) {
            return lhs.max_us > rhs.max_us;
        }
        return lhs.calls > rhs.calls;
    });

    std::println("=== perf checkout-report xfs [{}] ==================================", loaded.source);
    std::println("{:<18}  {:>9}  {:>6}  {:>11}  {:>10}  {:>9}  {:>9}  {:>9}  {:>9}", "OP", "CALLS", "ERR", "BYTES", "TOTAL(ms)", "AVG(us)",
                 "P99(us)", "MAX(us)", "MiB/s");
    std::println("{:->18}  {:->9}  {:->6}  {:->11}  {:->10}  {:->9}  {:->9}  {:->9}  {:->9}", "", "", "", "", "", "", "", "", "");
    for (int i = 0; std::cmp_less(i, rows.size()) && i < limit; ++i) {
        const auto& row = rows.at(static_cast<std::size_t>(i));
        std::println("{:<18}  {:>9}  {:>6}  {:>11}  {:>10}  {:>9}  {:>9}  {:>9}  {:>9}", row.op, row.calls, row.errors,
                     format_scaled_bytes(row.bytes), xfs_total_ms(row), row.avg_us, row.p99_us, row.max_us,
                     format_mib_per_s(row.bytes, row.total_us));
    }
}

void print_checkout_focus_rows(const CheckoutSummaryRows& loaded) {
    if (loaded.rows.empty()) {
        return;
    }

    constexpr std::array<std::string_view, 28> FOCUS_OPS{{
        "metadata_lock_wait",
        "metadata_lock_hold",
        "inode_fetch",
        "inode_cache_miss",
        "inode_unavailable",
        "buf_get_miss",
        "buf_read_miss",
        "buf_disk_read",
        "buf_disk_write",
        "buf_dirty",
        "buf_flush",
        "sync_blockdev",
        "write_bmap",
        "write_alloc",
        "write_ilog",
        "write_io",
        "read_bmap",
        "read_io",
        "read_gap",
        "open_create",
        "create_lookup",
        "ialloc",
        "dir_add",
        "open_commit",
        "create_trans_alloc",
        "create_inode_init",
        "create_path_invalidate",
        "create_icache",
    }};

    std::println("");
    std::println("=== checkout key counters ===========================================");
    std::println("{:<18}  {:>9}  {:>11}  {:>10}  {:>9}  {:>9}  {:>11}", "OP", "CALLS", "BYTES", "TOTAL(ms)", "AVG(us)", "P99(us)",
                 "MAX(us)");
    std::println("{:->18}  {:->9}  {:->11}  {:->10}  {:->9}  {:->9}  {:->11}", "", "", "", "", "", "", "");

    int printed = 0;
    for (std::string_view op : FOCUS_OPS) {
        const WkiSummaryRow* row = find_xfs_row(loaded.rows, op);
        if (row == nullptr || row->calls == 0) {
            continue;
        }
        std::println("{:<18}  {:>9}  {:>11}  {:>10}  {:>9}  {:>9}  {:>11}", row->op, row->calls, format_scaled_bytes(row->bytes),
                     xfs_total_ms(*row), row->avg_us, row->p99_us, row->max_us);
        printed++;
    }

    if (printed == 0) {
        std::println("no checkout focus counters were present in {}", loaded.source);
    }
}

auto load_diag_text(std::string_view section_header, std::string_view section_footer, std::string_view live_path,
                    std::size_t initial_capacity = CPUSTAT_READ_CAPACITY) -> std::optional<WkiLoadedText> {
    auto saved = load_perf_data_section(section_header, section_footer);
    if (saved.has_value() && !saved->empty()) {
        return WkiLoadedText{.source = std::string(PERF_DATA_FILE), .buffer = std::move(*saved), .sectioned = true};
    }

    auto live = read_file(live_path, initial_capacity);
    if (!live.has_value() || live->empty()) {
        return std::nullopt;
    }
    return WkiLoadedText{.source = std::string(live_path), .buffer = std::move(*live), .sectioned = false};
}

void print_checkout_cache_state() {
    auto loaded = load_diag_text(SECTION_MEMACC_ALLOC_TOTALS, SECTION_MEMACC_ALLOC_TOTALS_END, MEMACC_ALLOC_TOTALS_PATH);
    std::println("");
    std::println("=== checkout cache and dirty state ==================================");
    if (!loaded.has_value()) {
        std::println("perf: no memacc allocator/cache snapshot found in {} or {}", PERF_DATA_FILE, MEMACC_ALLOC_TOTALS_PATH);
        return;
    }

    auto rows = parse_key_value_rows(loaded->buffer);
    std::println("source: {}", loaded->source);

    if (const KeyValueRow* bcache = find_row_by_record(rows, "buffer_cache"); bcache != nullptr) {
        uint64_t const HITS = get_row_u64(*bcache, "hits");
        uint64_t const MISSES = get_row_u64(*bcache, "misses");
        uint64_t const DIRTY_BYTES = get_row_u64(*bcache, "dirty_bytes");
        uint64_t const DIRTY_TARGET = get_row_u64(*bcache, "dirty_target_bytes");
        uint64_t const DIRTY_HARD = get_row_u64(*bcache, "dirty_hard_bytes");
        std::println("buffer_cache: total={} clean={} dirty={} buffers={} dirty_buffers={} hit={:.1f}%",
                     format_scaled_bytes(get_row_u64(*bcache, "total_bytes")), format_scaled_bytes(get_row_u64(*bcache, "clean_bytes")),
                     format_scaled_bytes(DIRTY_BYTES), get_row_u64(*bcache, "buffers"), get_row_u64(*bcache, "dirty_buffers"),
                     ratio_percent(HITS, HITS + MISSES));
        std::println("dirty limits: target={} ({:.1f}%) hard={} ({:.1f}%) waiters={} dirty_bdevs={}", format_scaled_bytes(DIRTY_TARGET),
                     ratio_percent(DIRTY_BYTES, DIRTY_TARGET), format_scaled_bytes(DIRTY_HARD), ratio_percent(DIRTY_BYTES, DIRTY_HARD),
                     get_row_u64(*bcache, "dirty_waiters"), get_row_u64(*bcache, "dirty_bdevs"));
    } else {
        std::println("buffer_cache: unavailable");
    }

    int dirty_bdevs = 0;
    for (const auto& row : rows) {
        if (row.record != "buffer_cache_bdev") {
            continue;
        }
        uint64_t const DIRTY_BYTES = get_row_u64(row, "dirty_bytes");
        uint64_t const DIRTY_BUFFERS = get_row_u64(row, "dirty_buffers");
        if (DIRTY_BYTES == 0 && DIRTY_BUFFERS == 0) {
            continue;
        }
        std::println("dirty bdev: name={} dirty={} dirty_buffers={} oldest_epoch={}", get_row_string(row, "name"),
                     format_scaled_bytes(DIRTY_BYTES), DIRTY_BUFFERS, get_row_u64(row, "oldest_dirty_epoch"));
        dirty_bdevs++;
    }
    if (dirty_bdevs == 0) {
        std::println("dirty bdev: none");
    }

    if (const KeyValueRow* vfs_cache = find_row_by_record(rows, "vfs_cache"); vfs_cache != nullptr) {
        uint64_t const META_HITS = get_row_u64(*vfs_cache, "metadata_hits");
        uint64_t const META_MISSES = get_row_u64(*vfs_cache, "metadata_misses");
        uint64_t const FSTAT_HITS = get_row_u64(*vfs_cache, "fstat_snapshot_hits");
        uint64_t const FSTAT_MISSES = get_row_u64(*vfs_cache, "fstat_snapshot_misses");
        std::println("vfs_cache: metadata hits={} misses={} stores={} hit={:.1f}%  fstat hits={} misses={} stores={} hit={:.1f}%",
                     META_HITS, META_MISSES, get_row_u64(*vfs_cache, "metadata_stores"), ratio_percent(META_HITS, META_HITS + META_MISSES),
                     FSTAT_HITS, FSTAT_MISSES, get_row_u64(*vfs_cache, "fstat_snapshot_stores"),
                     ratio_percent(FSTAT_HITS, FSTAT_HITS + FSTAT_MISSES));
        std::println(
            "vfs_metadata_miss: empty={} invalidated={} stale_generation={} conflict={} path_invalidations={} generation_resets={}",
            get_row_u64(*vfs_cache, "metadata_miss_empty"), get_row_u64(*vfs_cache, "metadata_miss_invalidated"),
            get_row_u64(*vfs_cache, "metadata_miss_stale_generation"), get_row_u64(*vfs_cache, "metadata_miss_conflict"),
            get_row_u64(*vfs_cache, "metadata_path_invalidations"), get_row_u64(*vfs_cache, "metadata_generation_resets"));
        std::println("vfs_fstat_miss: uncacheable={} empty={} generation={} invalidated={}",
                     get_row_u64(*vfs_cache, "fstat_snapshot_miss_uncacheable"), get_row_u64(*vfs_cache, "fstat_snapshot_miss_empty"),
                     get_row_u64(*vfs_cache, "fstat_snapshot_miss_generation"), get_row_u64(*vfs_cache, "fstat_snapshot_miss_invalidated"));
        std::println("vfs_fstat_uncacheable: bad_args={} no_cache={} pathless={} fs={}",
                     get_row_u64(*vfs_cache, "fstat_snapshot_miss_bad_args"), get_row_u64(*vfs_cache, "fstat_snapshot_miss_no_cache"),
                     get_row_u64(*vfs_cache, "fstat_snapshot_miss_pathless"), get_row_u64(*vfs_cache, "fstat_snapshot_miss_fs"));
        uint64_t const SYMLINK_HITS = get_row_u64(*vfs_cache, "symlink_hits");
        uint64_t const SYMLINK_MISSES = get_row_u64(*vfs_cache, "symlink_misses");
        std::println("vfs_symlink: hits={} misses={} stores={} hit={:.1f}%", SYMLINK_HITS, SYMLINK_MISSES,
                     get_row_u64(*vfs_cache, "symlink_stores"), ratio_percent(SYMLINK_HITS, SYMLINK_HITS + SYMLINK_MISSES));
        std::println("vfs_stream: hits={} misses={} backend_reads={} backend={} copied={} invalidate_empty_skips={}",
                     get_row_u64(*vfs_cache, "stream_hits"), get_row_u64(*vfs_cache, "stream_misses"),
                     get_row_u64(*vfs_cache, "stream_backend_reads"), format_scaled_bytes(get_row_u64(*vfs_cache, "stream_backend_bytes")),
                     format_scaled_bytes(get_row_u64(*vfs_cache, "stream_copied_bytes")),
                     get_row_u64(*vfs_cache, "stream_invalidate_empty_skips"));
    } else {
        std::println("vfs_cache: unavailable");
    }

    if (const KeyValueRow* dentry = find_row_by_record(rows, "xfs_dentry_cache"); dentry != nullptr) {
        uint64_t const HITS = get_row_u64(*dentry, "hits");
        uint64_t const MISSES = get_row_u64(*dentry, "misses");
        std::println("xfs_dentry_cache: hits={} misses={} stores={} invalidations={} hit={:.1f}%", HITS, MISSES,
                     get_row_u64(*dentry, "stores"), get_row_u64(*dentry, "invalidations"), ratio_percent(HITS, HITS + MISSES));
    } else {
        std::println("xfs_dentry_cache: unavailable");
    }
}

void print_checkout_fd_state() {
    auto loaded = load_diag_text(SECTION_CONTSTAT, SECTION_CONTSTAT_END, KCONTSTAT_PATH);
    std::println("");
    std::println("=== checkout fd churn ===============================================");
    if (!loaded.has_value()) {
        std::println("perf: no container snapshot found in {} or {}", PERF_DATA_FILE, KCONTSTAT_PATH);
        return;
    }

    auto rows = parse_key_value_rows(loaded->buffer);
    const KeyValueRow* fd_table = find_row_by_key(rows, "subsys", "fd_table");
    if (fd_table == nullptr) {
        std::println("fd_table: no aggregate activity in {}", loaded->source);
        return;
    }

    std::println("source: {}", loaded->source);
    std::println("fd_table: inserts={} removes={} resizes={} oom={} peak={} current={}", get_row_u64(*fd_table, "inserts"),
                 get_row_u64(*fd_table, "removes"), get_row_u64(*fd_table, "resizes"), get_row_u64(*fd_table, "oom"),
                 get_row_u64(*fd_table, "peak"), get_row_u64(*fd_table, "current"));
}

void cmd_checkout_report(int limit) {
    if (limit < 1) {
        limit = DEFAULT_CHECKOUT_ROWS;
    }

    CheckoutSummaryRows xfs_rows = load_checkout_xfs_rows();
    print_checkout_xfs_table(xfs_rows, limit);
    print_checkout_focus_rows(xfs_rows);
    print_checkout_cache_state();
    print_checkout_fd_state();
}

void cmd_wki_tail(int limit, const WkiTraceFilter& filter, const WkiDisplayOptions& display_options, WkiDataView view) {
    if (limit < 1) {
        limit = DEFAULT_WKI_TAIL_ROWS;
    }

    auto event_loaded = load_wki_event_text();
    std::vector<WkiSummaryRow> rows;
    if (event_loaded.has_value()) {
        rows = build_wki_summary_from_events(collect_wki_events(event_loaded->buffer, event_loaded->sectioned), filter, view);
    }

    auto summary_loaded = load_wki_summary_text();
    if (rows.empty() && summary_loaded.has_value()) {
        rows = parse_wki_summary_section(summary_loaded->buffer, summary_loaded->sectioned);
        std::erase_if(rows, [&](const WkiSummaryRow& row) { return !view_summary_matches(row, filter, view); });
    }

    std::ranges::sort(rows, [](const WkiSummaryRow& lhs, const WkiSummaryRow& rhs) {
        if (lhs.p99999_us != rhs.p99999_us) {
            return lhs.p99999_us > rhs.p99999_us;
        }
        if (lhs.p9999_us != rhs.p9999_us) {
            return lhs.p9999_us > rhs.p9999_us;
        }
        if (lhs.max_us != rhs.max_us) {
            return lhs.max_us > rhs.max_us;
        }
        return lhs.calls > rhs.calls;
    });

    if (!rows.empty()) {
        std::string summary_source = "?";
        WkiPeerResolver summary_peer_resolver = make_wki_peer_resolver({}, false, display_options);
        if (event_loaded.has_value()) {
            summary_source = event_loaded->source;
            summary_peer_resolver = make_wki_peer_resolver(event_loaded->buffer, event_loaded->sectioned, display_options);
        } else if (summary_loaded.has_value()) {
            summary_source = summary_loaded->source;
            summary_peer_resolver = make_wki_peer_resolver(summary_loaded->buffer, summary_loaded->sectioned, display_options);
        }
        std::println("=== perf {}-tail summary [{}] ======================================", wki_view_name(view), summary_source);
        std::println("{:<14}  {:<16}  {:<18}  {:>4}  {:>7}  {:>9}  {:>9}  {:>9}  {:>9}", "SCOPE", "OP", "PEER", "CH", "CALLS", "P999(us)",
                     "P9999(us)", "P99999(us)", "MAX(us)");
        std::println("{:->14}  {:->16}  {:->18}  {:->4}  {:->7}  {:->9}  {:->9}  {:->9}  {:->9}", "", "", "", "", "", "", "", "", "");
        for (int i = 0; std::cmp_less(i, rows.size()) && i < limit; ++i) {
            const auto& row = rows.at(static_cast<std::size_t>(i));
            std::println("{:<14}  {:<16}  {:<18}  {:>4}  {:>7}  {:>9}  {:>9}  {:>9}  {:>9}", row.scope, row.op,
                         wki_peer_label(row.peer, summary_peer_resolver), row.channel, row.calls, row.p999_us, row.p9999_us, row.p99999_us,
                         row.max_us);
        }
    }

    if (!event_loaded.has_value()) {
        if (!rows.empty()) {
            std::println("");
            std::println("perf: no raw {} events available in perf.data or /proc/kperf", wki_view_name(view));
            return;
        }
        std::println("perf: no {} summary or raw trace data", wki_view_name(view));
        return;
    }

    if (rows.empty()) {
        std::println("perf: no {} summary data; showing raw-event tail from {}", wki_view_name(view), event_loaded->source);
    }

    auto events = collect_wki_events(event_loaded->buffer, event_loaded->sectioned);
    auto event_peer_resolver = make_wki_peer_resolver(event_loaded->buffer, event_loaded->sectioned, display_options);
    auto const TIME_DISPLAY = make_time_display(display_options, event_loaded->buffer, event_loaded->sectioned);
    int const TIME_WIDTH = time_column_width(TIME_DISPLAY);
    if (events.empty()) {
        if (!rows.empty()) {
            std::println("");
            std::println("perf: no raw {} events found in {}", wki_view_name(view), event_loaded->source);
            return;
        }
        std::println("perf: no raw {} events found in {}", wki_view_name(view), event_loaded->source);
        return;
    }

    std::vector<EventInfo> slow_events;
    for (const auto& event : events) {
        if (view_summary_event_matches(event, filter, view) && event.aux > 0) {
            slow_events.push_back(event);
        }
    }
    std::ranges::sort(slow_events, [](const EventInfo& lhs, const EventInfo& rhs) { return lhs.aux > rhs.aux; });

    std::println("");
    std::println("=== slowest {} events [{}] ========================================", wki_view_name(view), event_loaded->source);
    std::println("{:<{}}  {:<14}  {:<16}  {:<6}  {:<18}  {:>4}  {:>8}  {:>8}  {:>10}  {}", time_column_header(TIME_DISPLAY), TIME_WIDTH,
                 "SCOPE", "OP", "PHASE", "PEER", "CH", "AUX(us)", "STATUS", "CORR", "DETAILS");
    std::println("{:->{}}  {:->14}  {:->16}  {:->6}  {:->18}  {:->4}  {:->8}  {:->8}  {:->10}  {:->24}", "", TIME_WIDTH, "", "", "", "", "",
                 "", "", "", "");
    for (int i = 0; std::cmp_less(i, slow_events.size()) && i < limit; ++i) {
        const auto& event = slow_events.at(static_cast<std::size_t>(i));
        std::println("{:<{}}  {:<14}  {:<16}  {:<6}  {:<18}  {:>4}  {:>8}  {:>8}  {:>10}  {}", format_event_time(event.ts_ns, TIME_DISPLAY),
                     TIME_WIDTH, event.scope_name, event.op_name, event.phase_name, wki_peer_label(event.peer, event_peer_resolver),
                     event.channel, event.aux, event.status, event.correlation, format_wki_trace_detail(event));
    }

    struct LastBegin {
        std::string scope;
        std::string op;
        uint64_t peer{};
        uint64_t channel{};
        uint64_t last_ts_ns{};
    };

    std::vector<LastBegin> last_begins;
    std::vector<WkiGapRow> gaps;
    for (const auto& event : events) {
        if (event.phase_name != "begin") {
            continue;
        }
        if (!view_trace_matches(event, filter, view)) {
            continue;
        }

        auto it = std::ranges::find_if(last_begins, [&](const LastBegin& row) {
            return row.scope == event.scope_name && row.op == event.op_name && row.peer == event.peer && row.channel == event.channel;
        });

        if (it != last_begins.end()) {
            gaps.push_back(WkiGapRow{
                .scope = event.scope_name,
                .op = event.op_name,
                .peer = event.peer,
                .channel = event.channel,
                .gap_ns = event.ts_ns - it->last_ts_ns,
                .prev_ts_ns = it->last_ts_ns,
                .next_ts_ns = event.ts_ns,
            });
            it->last_ts_ns = event.ts_ns;
        } else {
            last_begins.push_back(LastBegin{
                .scope = event.scope_name, .op = event.op_name, .peer = event.peer, .channel = event.channel, .last_ts_ns = event.ts_ns});
        }
    }

    std::ranges::sort(gaps, [](const WkiGapRow& lhs, const WkiGapRow& rhs) { return lhs.gap_ns > rhs.gap_ns; });

    std::println("");
    std::println("=== largest {} inter-call gaps ====================================", wki_view_name(view));
    std::println("{:<14}  {:<16}  {:<18}  {:>4}  {:>12}  {:<{}}  {:<{}}", "SCOPE", "OP", "PEER", "CH", "GAP(us)",
                 time_column_header(TIME_DISPLAY), TIME_WIDTH, "NEXT", TIME_WIDTH);
    std::println("{:->14}  {:->16}  {:->18}  {:->4}  {:->12}  {:->{}}  {:->{}}", "", "", "", "", "", "", TIME_WIDTH, "", TIME_WIDTH);
    for (int i = 0; std::cmp_less(i, gaps.size()) && i < limit; ++i) {
        const auto& gap = gaps.at(static_cast<std::size_t>(i));
        std::println("{:<14}  {:<16}  {:<18}  {:>4}  {:>12}  {:<{}}  {:<{}}", gap.scope, gap.op,
                     wki_peer_label(gap.peer, event_peer_resolver), gap.channel,
                     gap.gap_ns / static_cast<uint64_t>(NANOSECONDS_PER_MICROSECOND), format_event_time(gap.prev_ts_ns, TIME_DISPLAY),
                     TIME_WIDTH, format_event_time(gap.next_ts_ns, TIME_DISPLAY), TIME_WIDTH);
    }
}

void cmd_wki_trace(int max_events, const WkiTraceFilter& filter, const WkiDisplayOptions& display_options, WkiDataView view) {
    if (max_events < 1) {
        max_events = DEFAULT_WKI_TRACE_EVENTS;
    }

    auto loaded = load_wki_event_text();
    if (!loaded.has_value()) {
        auto summary_loaded = load_wki_summary_text();
        if (summary_loaded.has_value() && !parse_wki_summary_section(summary_loaded->buffer, summary_loaded->sectioned).empty()) {
            std::println("perf: {} summary data exists in {}, but no raw {} events were saved", wki_view_name(view), summary_loaded->source,
                         wki_view_name(view));
        } else {
            std::println("perf: no {} trace data", wki_view_name(view));
        }
        return;
    }

    auto proc_map = loaded->sectioned ? parse_proc_map_section(loaded->buffer) : current_proc_map();
    auto events = collect_wki_events(loaded->buffer, loaded->sectioned);
    auto peer_resolver = make_wki_peer_resolver(loaded->buffer, loaded->sectioned, display_options);
    auto const TIME_DISPLAY = make_time_display(display_options, loaded->buffer, loaded->sectioned);
    int const TIME_WIDTH = time_column_width(TIME_DISPLAY);
    if (events.empty()) {
        auto summary_loaded = load_wki_summary_text();
        if (summary_loaded.has_value() && !parse_wki_summary_section(summary_loaded->buffer, summary_loaded->sectioned).empty()) {
            std::println("perf: {} summary data exists in {}, but no raw {} events were saved", wki_view_name(view), summary_loaded->source,
                         wki_view_name(view));
        } else {
            std::println("perf: no {} trace data", wki_view_name(view));
        }
        return;
    }

    std::println("=== perf {}-trace [{}] ============================================", wki_view_name(view), loaded->source);
    std::println("{:<{}}  {:>3}  {:<24}  {:<14}  {:<16}  {:<6}  {:<18}  {:>4}  {:>8}  {:>8}  {:>8}  {}", time_column_header(TIME_DISPLAY),
                 TIME_WIDTH, "CPU", "PID", "SCOPE", "OP", "PHASE", "PEER", "CH", "AUX(us)", "STATUS", "CORR", "CALLSITE/DETAILS");
    std::println("{:->{}}  {:->3}  {:->24}  {:->14}  {:->16}  {:->6}  {:->18}  {:->4}  {:->8}  {:->8}  {:->8}  {:->28}", "", TIME_WIDTH, "",
                 "", "", "", "", "", "", "", "", "", "");

    int printed = 0;
    for (const auto& event : events) {
        if (!view_trace_matches(event, filter, view)) {
            continue;
        }
        std::println("{:<{}}  {:>3}  {:<24}  {:<14}  {:<16}  {:<6}  {:<18}  {:>4}  {:>8}  {:>8}  {:>8}  {}",
                     format_event_time(event.ts_ns, TIME_DISPLAY), TIME_WIDTH, event.cpu, format_pid_name(event.pid, proc_map),
                     event.scope_name, event.op_name, event.phase_name, wki_peer_label(event.peer, peer_resolver), event.channel, event.aux,
                     event.status, event.correlation, format_wki_trace_detail(event));
        printed++;
        if (printed >= max_events) {
            break;
        }
    }
}

}  // namespace perf
