#include "perf.hpp"
namespace perf {

auto parse_proc_map_section(std::string_view buffer) -> std::vector<ProcMapEntry> {
    std::vector<ProcMapEntry> entries;
    std::size_t const HEADER_POS = buffer.find(SECTION_PROC_MAP);
    if (HEADER_POS == std::string_view::npos) {
        return entries;
    }

    std::size_t pos = buffer.find('\n', HEADER_POS);
    if (pos == std::string_view::npos) {
        return entries;
    }
    ++pos;

    while (pos < buffer.size()) {
        std::string_view const LINE = next_line(buffer, pos);
        if (LINE.starts_with(END_PREFIX)) {
            break;
        }
        if (LINE.empty()) {
            continue;
        }

        std::size_t const PID_POS = LINE.find("pid=");
        std::size_t const COMM_POS = LINE.find(" comm=");
        if (PID_POS == std::string_view::npos || COMM_POS == std::string_view::npos) {
            continue;
        }

        std::size_t const CMD_POS = LINE.find(CMD_FIELD_PREFIX, COMM_POS + 1);
        std::string_view const PID_TEXT = LINE.substr(PID_POS + PID_PREFIX_SIZE, COMM_POS - (PID_POS + PID_PREFIX_SIZE));
        std::string_view const COMM_TEXT = CMD_POS == std::string_view::npos
                                               ? LINE.substr(COMM_POS + COMM_PREFIX_SIZE)
                                               : LINE.substr(COMM_POS + COMM_PREFIX_SIZE, CMD_POS - (COMM_POS + COMM_PREFIX_SIZE));

        entries.push_back(ProcMapEntry{.pid = parse_u64(PID_TEXT), .comm = std::string(COMM_TEXT)});
    }

    return entries;
}

auto parse_peer_map_line(std::string_view line, WkiPeerMapEntry& out) -> bool {
    if (line.empty() || !line.starts_with("peer=")) {
        return false;
    }

    out = {};
    auto peer = extract_value(line, "peer=");
    auto hostname = extract_value(line, "hostname=");
    if (peer.empty() || hostname.empty()) {
        return false;
    }

    out.peer = parse_u64(peer, 0);
    out.hostname = std::string(hostname);
    return true;
}

auto parse_peer_map_section(std::string_view buffer) -> std::vector<WkiPeerMapEntry> {
    std::vector<WkiPeerMapEntry> entries;
    std::size_t const HEADER_POS = buffer.find(SECTION_PEER_MAP);
    if (HEADER_POS == std::string_view::npos) {
        return entries;
    }

    std::size_t pos = buffer.find('\n', HEADER_POS);
    if (pos == std::string_view::npos) {
        return entries;
    }
    ++pos;

    while (pos < buffer.size()) {
        std::string_view const LINE = next_line(buffer, pos);
        if (LINE.starts_with(END_PREFIX)) {
            break;
        }
        if (LINE.empty()) {
            continue;
        }

        WkiPeerMapEntry entry{};
        if (parse_peer_map_line(LINE, entry)) {
            entries.push_back(std::move(entry));
        }
    }

    return entries;
}

auto extract_value(std::string_view line, std::string_view key) -> std::string_view {
    std::size_t key_pos = line.find(key);
    if (key_pos == std::string_view::npos) {
        return {};
    }
    key_pos += key.size();
    std::size_t const END = line.find(' ', key_pos);
    return line.substr(key_pos, END == std::string_view::npos ? line.size() - key_pos : END - key_pos);
}

auto parse_wki_summary_line(std::string_view line, WkiSummaryRow& out) -> bool {
    if (line.empty() || !line.starts_with("scope=")) {
        return false;
    }

    out = {};
    auto scope = extract_value(line, "scope=");
    auto op = extract_value(line, "op=");
    auto peer = extract_value(line, "peer=");
    auto channel = extract_value(line, "channel=");
    auto calls = extract_value(line, "calls=");
    if (scope.empty() || op.empty() || calls.empty()) {
        return false;
    }

    out.scope = std::string(scope);
    out.op = std::string(op);
    out.peer = peer.empty() ? 0 : parse_u64(peer, 0);
    out.channel = channel.empty() ? 0 : parse_u64(channel, 0);
    out.calls = parse_u64(calls);
    out.errors = parse_u64(extract_value(line, "errors="));
    out.retries = parse_u64(extract_value(line, "retries="));
    out.bytes = parse_u64(extract_value(line, "bytes="));
    out.samples = parse_u64(extract_value(line, "samples="));
    out.total_us = parse_u64(extract_value(line, "total_us="));
    out.avg_us = parse_u64(extract_value(line, "avg_us="));
    if (out.total_us == 0 && out.avg_us != 0 && out.calls != 0) {
        out.total_us = out.avg_us * out.calls;
    }
    if (out.samples == 0) {
        out.samples = out.calls;
    }
    out.max_us = parse_u64(extract_value(line, "max_us="));
    out.p50_us = parse_u64(extract_value(line, "p50_us="));
    out.p95_us = parse_u64(extract_value(line, "p95_us="));
    out.p99_us = parse_u64(extract_value(line, "p99_us="));
    out.p999_us = parse_u64(extract_value(line, "p999_us="));
    out.p9999_us = parse_u64(extract_value(line, "p9999_us="));
    out.p99999_us = parse_u64(extract_value(line, "p99999_us="));
    return true;
}

auto parse_wki_summary_section(std::string_view buffer, bool sectioned) -> std::vector<WkiSummaryRow> {
    std::vector<WkiSummaryRow> rows;
    std::size_t pos = 0;

    if (sectioned) {
        std::size_t const HEADER_POS = buffer.find(SECTION_WKI_SUMMARY);
        if (HEADER_POS == std::string_view::npos) {
            return rows;
        }

        pos = buffer.find('\n', HEADER_POS);
        if (pos == std::string_view::npos) {
            return rows;
        }
        ++pos;
    }

    while (pos < buffer.size()) {
        std::string_view const LINE = next_line(buffer, pos);
        if (sectioned && LINE.starts_with(END_PREFIX)) {
            break;
        }
        WkiSummaryRow row{};
        if (parse_wki_summary_line(LINE, row)) {
            rows.push_back(std::move(row));
        }
    }

    return rows;
}

auto hex_value(char ch) -> int {
    if (ch >= '0' && ch <= '9') {
        return ch - '0';
    }
    if (ch >= 'a' && ch <= 'f') {
        return ch - 'a' + 10;
    }
    if (ch >= 'A' && ch <= 'F') {
        return ch - 'A' + 10;
    }
    return -1;
}

auto percent_decode(std::string_view value) -> std::string {
    std::string out;
    out.reserve(value.size());
    for (std::size_t i = 0; i < value.size(); ++i) {
        if (value.at(i) == '%' && i + 2 < value.size()) {
            int const HI = hex_value(value.at(i + 1));
            int const LO = hex_value(value.at(i + 2));
            if (HI >= 0 && LO >= 0) {
                out.push_back(static_cast<char>((HI << 4U) | LO));
                i += 2;
                continue;
            }
        }
        out.push_back(value.at(i));
    }
    return out;
}

auto parse_key_value_rows(std::string_view text) -> std::vector<KeyValueRow> {
    std::vector<KeyValueRow> rows;
    std::size_t pos = 0;
    while (pos < text.size()) {
        std::string_view const LINE = next_line(text, pos);
        if (LINE.empty()) {
            continue;
        }

        std::size_t token_start = 0;
        std::size_t token_end = LINE.find(' ');
        KeyValueRow row;
        std::string_view const FIRST_TOKEN = LINE.substr(0, token_end);
        if (!FIRST_TOKEN.contains('=')) {
            row.record = std::string(FIRST_TOKEN);
            token_start = token_end == std::string_view::npos ? LINE.size() : token_end + 1;
        }

        while (token_start < LINE.size()) {
            token_end = LINE.find(' ', token_start);
            if (token_end == std::string_view::npos) {
                token_end = LINE.size();
            }
            std::string_view const TOKEN = LINE.substr(token_start, token_end - token_start);
            std::size_t const EQ = TOKEN.find('=');
            if (EQ != std::string_view::npos && EQ > 0) {
                row.kv.emplace(std::string(TOKEN.substr(0, EQ)), percent_decode(TOKEN.substr(EQ + 1)));
            }
            token_start = token_end + 1;
        }

        rows.push_back(std::move(row));
    }
    return rows;
}

auto get_row_string(const KeyValueRow& row, std::string_view key) -> std::string_view {
    auto const IT = row.kv.find(std::string(key));
    return IT == row.kv.end() ? std::string_view{} : std::string_view(IT->second);
}

auto get_row_u64(const KeyValueRow& row, std::string_view key) -> uint64_t {
    std::string_view const VALUE = get_row_string(row, key);
    if (VALUE.empty() || VALUE == "-") {
        return 0;
    }
    int base = PARSE_BASE_DECIMAL;
    if (VALUE.size() > 2 && VALUE.at(0) == '0' && (VALUE.at(1) == 'x' || VALUE.at(1) == 'X')) {
        base = 16;
    }
    return parse_u64(VALUE, base);
}

auto find_row_by_record(const std::vector<KeyValueRow>& rows, std::string_view record) -> const KeyValueRow* {
    for (const auto& row : rows) {
        if (row.record == record) {
            return &row;
        }
    }
    return nullptr;
}

auto find_row_by_key(const std::vector<KeyValueRow>& rows, std::string_view key, std::string_view value) -> const KeyValueRow* {
    for (const auto& row : rows) {
        if (get_row_string(row, key) == value) {
            return &row;
        }
    }
    return nullptr;
}

auto parse_ipc_stats_line(std::string_view line, IpcStatsSnapshot& out) -> bool {
    if (line.empty() || !line.starts_with("exports=")) {
        return false;
    }

    out = {};
    out.exports = parse_u64(extract_value(line, "exports="));
    out.proxies = parse_u64(extract_value(line, "proxies="));
    out.pump_tasks = parse_u64(extract_value(line, "pump_tasks="));
    out.ring_bytes = parse_u64(extract_value(line, "ring_bytes="));
    out.ring_used = parse_u64(extract_value(line, "ring_used="));
    out.blocked_readers = parse_u64(extract_value(line, "blocked_readers="));
    out.poll_waiters = parse_u64(extract_value(line, "poll_waiters="));
    out.pending_deliveries = parse_u64(extract_value(line, "pending_deliveries="));
    out.pending_chunks = parse_u64(extract_value(line, "pending_chunks="));
    out.pending_bytes = parse_u64(extract_value(line, "pending_bytes="));
    out.export_backlogs = parse_u64(extract_value(line, "export_backlogs="));
    out.export_backlog_chunks = parse_u64(extract_value(line, "export_backlog_chunks="));
    out.export_backlog_bytes = parse_u64(extract_value(line, "export_backlog_bytes="));
    out.export_flush_queue = parse_u64(extract_value(line, "export_flush_queue="));
    out.dev_op_queue = parse_u64(extract_value(line, "dev_op_queue="));
    out.dev_op_payload_bytes = parse_u64(extract_value(line, "dev_op_payload_bytes="));
    out.proxy_write_payload_bytes = parse_u64(extract_value(line, "proxy_write_payload_bytes="));
    out.proxy_write_no_credit_waits = parse_u64(extract_value(line, "proxy_write_no_credit_waits="));
    out.proxy_write_block_us = parse_u64(extract_value(line, "proxy_write_block_us="));
    out.proxy_pipe_rdma_full_waits = parse_u64(extract_value(line, "proxy_pipe_rdma_full_waits="));
    out.proxy_ring_full_waits = parse_u64(extract_value(line, "proxy_ring_full_waits="));
    out.proxy_ring_full_bytes = parse_u64(extract_value(line, "proxy_ring_full_bytes="));
    out.pipe_payload_bytes = parse_u64(extract_value(line, "pipe_payload_bytes="));
    out.approx_alloc_bytes = parse_u64(extract_value(line, "approx_alloc_bytes="));
    out.local_pipe_active = parse_u64(extract_value(line, "local_pipe_active="));
    out.local_pipe_created = parse_u64(extract_value(line, "local_pipe_created="));
    out.local_pipe_peak = parse_u64(extract_value(line, "local_pipe_peak="));
    out.local_pipe_capacity = parse_u64(extract_value(line, "local_pipe_capacity="));
    out.local_pipe_peak_capacity = parse_u64(extract_value(line, "local_pipe_peak_capacity="));
    out.local_pipe_buffered = parse_u64(extract_value(line, "local_pipe_buffered="));
    out.local_pipe_reader_waiters = parse_u64(extract_value(line, "local_pipe_reader_waiters="));
    out.local_pipe_writer_waiters = parse_u64(extract_value(line, "local_pipe_writer_waiters="));
    out.local_pipe_poll_waiters = parse_u64(extract_value(line, "local_pipe_poll_waiters="));
    out.local_pipe_direct_writes = parse_u64(extract_value(line, "local_pipe_direct_writes="));
    out.local_pipe_read_closed = parse_u64(extract_value(line, "local_pipe_read_closed="));
    out.local_pipe_write_closed = parse_u64(extract_value(line, "local_pipe_write_closed="));
    out.local_pipe_approx_alloc_bytes = parse_u64(extract_value(line, "local_pipe_approx_alloc_bytes="));
    return true;
}

auto parse_ipc_stats_section(std::string_view buffer, bool sectioned) -> std::optional<IpcStatsSnapshot> {
    std::size_t pos = 0;

    if (sectioned) {
        std::size_t const HEADER_POS = buffer.find(SECTION_IPC_STATS);
        if (HEADER_POS == std::string_view::npos) {
            return std::nullopt;
        }

        pos = buffer.find('\n', HEADER_POS);
        if (pos == std::string_view::npos) {
            return std::nullopt;
        }
        ++pos;
    }

    while (pos < buffer.size()) {
        std::string_view const LINE = next_line(buffer, pos);
        if (sectioned && LINE.starts_with(END_PREFIX)) {
            break;
        }

        IpcStatsSnapshot snapshot{};
        if (parse_ipc_stats_line(LINE, snapshot)) {
            return snapshot;
        }
    }

    return std::nullopt;
}

auto parse_event_line(std::string_view line, EventInfo& out) -> bool {
    constexpr int TOK_TS = 0;
    constexpr int TOK_CPU = 1;
    constexpr int TOK_PID = 2;
    constexpr int TOK_DATA = 3;
    constexpr int TOK_LAG = 4;
    constexpr int TOK_FLAGS = 5;
    constexpr int TOK_AUX = 6;
    constexpr int TOK_CALLSITE = 7;
    constexpr int TOK_WAIT = 8;
    constexpr int TOK_SCOPE = 3;
    constexpr int TOK_OP = 4;
    constexpr int TOK_PHASE = 5;
    constexpr int TOK_PEER = 6;
    constexpr int TOK_CHANNEL = 7;
    constexpr int TOK_CORR = 8;
    constexpr int TOK_STATUS = 9;
    constexpr int TOK_WKI_AUX = 10;
    constexpr int TOK_WKI_CALLSITE = 11;

    out = {};
    if (line.size() < 2 || line.at(1) != ' ') {
        return false;
    }

    std::array<std::string_view, MAX_EVENT_TOKENS> tokens{};
    int token_count = 0;
    std::size_t pos = 2;
    while (std::cmp_less(token_count, tokens.size())) {
        std::string_view const TOKEN = next_token(line, pos);
        if (TOKEN.empty()) {
            break;
        }
        tokens.at(static_cast<std::size_t>(token_count)) = TOKEN;
        ++token_count;
    }

    if (token_count < EVENT_MIN_TOKEN_COUNT) {
        return false;
    }

    out.type = line.front();
    out.ts_ns = parse_u64(tokens.at(TOK_TS));
    out.cpu = parse_u32(tokens.at(TOK_CPU));

    switch (out.type) {
        case 'S':
            if (token_count < EVENT_EXTENDED_TOKEN_COUNT) {
                return false;
            }
            out.pid = parse_u64(tokens.at(TOK_PID));
            out.data = parse_u64(tokens.at(TOK_DATA), 0);
            out.lag = parse_i64(tokens.at(TOK_LAG));
            out.flags = parse_u8(tokens.at(TOK_FLAGS));
            return true;

        case 'X':
            if (token_count < EVENT_EXTENDED_TOKEN_COUNT) {
                return false;
            }
            out.pid = parse_u64(tokens.at(TOK_PID));
            out.other_pid = parse_u64(tokens.at(TOK_DATA));
            out.lag = parse_i64(tokens.at(TOK_LAG));
            out.flags = parse_u8(tokens.at(TOK_FLAGS));
            if (token_count >= TOK_AUX + 1) {
                out.aux = parse_u32(tokens.at(TOK_AUX));
            }
            if (token_count >= TOK_CALLSITE + 1) {
                out.callsite = std::string(tokens.at(TOK_CALLSITE));
            }
            return true;

        case 'W':
        case 'B':
            out.pid = parse_u64(tokens.at(TOK_PID));
            out.data = parse_u64(tokens.at(TOK_DATA));
            if (token_count >= EVENT_EXTENDED_TOKEN_COUNT) {
                out.aux = parse_u32(tokens.at(TOK_LAG));
                out.flags = parse_u8(tokens.at(TOK_FLAGS));
                if (token_count >= TOK_CALLSITE + 1) {
                    out.callsite = std::string(tokens.at(TOK_CALLSITE));
                }
                if (token_count >= TOK_WAIT + 1) {
                    out.wait_channel = std::string(tokens.at(TOK_WAIT));
                }
            } else {
                out.flags = static_cast<uint8_t>((out.type == 'B' ? FLAG_BLOCK : 0U) | (out.data != 0 ? FLAG_TIMED : 0U));
            }
            return true;

        case 'C':
            // CONTAINER_STAT: C <ts> <cpu> <pid> <subsys_name> <flags> <count> <capacity> <callsite>
            if (token_count < EVENT_EXTENDED_TOKEN_COUNT) {
                return false;
            }
            out.pid = parse_u64(tokens.at(TOK_PID));
            out.subsys_name = std::string(tokens.at(TOK_DATA));  // tok 3 = subsys_name
            out.flags = parse_u8(tokens.at(TOK_LAG));            // tok 4 = flags
            out.lag = parse_i64(tokens.at(TOK_FLAGS));           // tok 5 = element count
            if (token_count >= TOK_AUX + 1) {
                out.aux = parse_u32(tokens.at(TOK_AUX));  // tok 6 = capacity
            }
            if (token_count >= TOK_CALLSITE + 1) {
                out.callsite = std::string(tokens.at(TOK_CALLSITE));
            }
            return true;

        case 'K':
            if (token_count < TOK_WKI_AUX + 1) {
                return false;
            }
            out.pid = parse_u64(tokens.at(TOK_PID));
            out.scope_name = std::string(tokens.at(TOK_SCOPE));
            out.op_name = std::string(tokens.at(TOK_OP));
            out.phase_name = std::string(tokens.at(TOK_PHASE));
            out.peer = parse_u64(tokens.at(TOK_PEER), 0);
            out.channel = parse_u64(tokens.at(TOK_CHANNEL), 0);
            out.correlation = parse_u64(tokens.at(TOK_CORR), 0);
            out.status = static_cast<int32_t>(parse_i64(tokens.at(TOK_STATUS), 0));
            out.aux = parse_u32(tokens.at(TOK_WKI_AUX), 0);
            if (token_count >= TOK_WKI_CALLSITE + 1) {
                out.callsite = std::string(tokens.at(TOK_WKI_CALLSITE));
            }
            return true;

        default:
            return false;
    }
}

auto next_event(std::string_view text, std::size_t& pos, bool sectioned, EventInfo& out) -> bool {
    while (pos < text.size()) {
        std::size_t const LINE_START = pos;
        std::string_view const LINE = next_line(text, pos);
        if (sectioned && text.substr(LINE_START).starts_with(END_PREFIX)) {
            return false;
        }
        if (LINE.empty()) {
            continue;
        }
        if (parse_event_line(LINE, out)) {
            return true;
        }
    }
    return false;
}

auto comm_of_pid(const std::vector<ProcMapEntry>& proc_map, uint64_t pid) -> std::string_view {
    for (const auto& entry : proc_map) {
        if (entry.pid == pid) {
            return entry.comm;
        }
    }
    return {};
}

auto format_pid_name(uint64_t pid, const std::vector<ProcMapEntry>& proc_map) -> std::string {
    std::string_view const COMM = comm_of_pid(proc_map, pid);
    if (!COMM.empty()) {
        std::string name = std::to_string(pid);
        name += '(';
        name += COMM;
        name += ')';
        return name;
    }
    return std::to_string(pid);
}

auto display_callsite(std::string_view callsite) -> std::string_view { return callsite.empty() ? UNKNOWN_CALLSITE : callsite; }

constexpr uint32_t WKI_STALL_FLAG_OUTSTANDING = 1U << 0;
constexpr uint32_t WKI_STALL_FLAG_NO_CREDITS = 1U << 1;
constexpr uint32_t WKI_STALL_FLAG_ACK_PENDING = 1U << 2;
constexpr uint32_t WKI_STALL_FLAG_HAS_RETRANSMIT = 1U << 3;
constexpr uint32_t WKI_STALL_FLAG_RETRANSMIT_DUE = 1U << 4;
constexpr uint32_t WKI_STALL_FLAG_FAST_RETRANSMIT = 1U << 5;
constexpr uint32_t WKI_STALL_FLAG_ACK_DELAY_DUE = 1U << 6;

auto is_transport_stall_event(const EventInfo& event) -> bool { return event.scope_name == "transport" && event.op_name == "stall"; }

auto is_local_vmem_event(const EventInfo& event) -> bool { return event.scope_name == "local_vmem"; }

auto is_local_loader_event(const EventInfo& event) -> bool { return event.scope_name == "local_loader"; }

auto is_local_xfs_event(const EventInfo& event) -> bool { return event.scope_name == "local_xfs"; }

auto is_local_irq_event(const EventInfo& event) -> bool { return event.scope_name == "local_irq"; }

auto is_vmem_cow_op(std::string_view op) -> bool { return op == "cow_zero" || op == "cow_copy" || op == "cow_promote"; }

auto is_vmem_file_cache_op(std::string_view op) -> bool { return op.starts_with("file_cache_"); }

auto format_cow_ref_category(uint64_t category) -> std::string_view {
    switch (category) {
        case 1:
            return "single";
        case 2:
            return "shared_le4";
        case 3:
            return "shared_le16";
        case 4:
            return "shared_gt16";
        default:
            return "?";
    }
}

auto append_flag_name(std::string& out, bool& first, uint32_t flags, uint32_t bit, std::string_view name) -> void {
    if ((flags & bit) == 0) {
        return;
    }
    if (!first) {
        out += '|';
    }
    out += name;
    first = false;
}

auto format_transport_stall_flags(uint32_t flags) -> std::string {
    std::string out;
    bool first = true;
    append_flag_name(out, first, flags, WKI_STALL_FLAG_OUTSTANDING, "out");
    append_flag_name(out, first, flags, WKI_STALL_FLAG_NO_CREDITS, "cred0");
    append_flag_name(out, first, flags, WKI_STALL_FLAG_ACK_PENDING, "ack");
    append_flag_name(out, first, flags, WKI_STALL_FLAG_HAS_RETRANSMIT, "rt");
    append_flag_name(out, first, flags, WKI_STALL_FLAG_RETRANSMIT_DUE, "rto");
    append_flag_name(out, first, flags, WKI_STALL_FLAG_FAST_RETRANSMIT, "fastrt");
    append_flag_name(out, first, flags, WKI_STALL_FLAG_ACK_DELAY_DUE, "ackdue");
    if (out.empty()) {
        out = "-";
    }
    return out;
}

auto format_transport_stall_detail(const EventInfo& event) -> std::string {
    auto const STATUS = static_cast<uint32_t>(event.status);
    uint32_t const FLAGS = STATUS & 0xFFU;
    uint32_t const CREDITS = (STATUS >> 8U) & 0xFFU;
    uint32_t const RETRANSMIT_COUNT = (STATUS >> 16U) & 0xFFU;
    uint32_t const INFLIGHT = (STATUS >> 24U) & 0x7FU;

    std::string out;
    out += "inflight=";
    out += std::to_string(INFLIGHT);
    out += " credits=";
    out += std::to_string(CREDITS);
    out += " rtq=";
    out += std::to_string(RETRANSMIT_COUNT);
    out += " flags=";
    out += format_transport_stall_flags(FLAGS);
    return out;
}

auto format_local_vmem_detail(const EventInfo& event) -> std::string {
    std::string out;
    out += is_vmem_file_cache_op(event.op_name) ? "offset=" : "addr=";
    out += std::string(display_callsite(event.callsite));
    out += " pages=";
    out += std::to_string(event.peer);
    if (is_vmem_cow_op(event.op_name)) {
        out += " refcat=";
        out += std::string(format_cow_ref_category(event.channel));
        out += " ref=";
        out += std::to_string(event.status);
    } else {
        out += " detail=";
        out += std::to_string(event.channel);
        out += " status=";
        out += std::to_string(event.status);
    }
    return out;
}

auto format_local_loader_detail(const EventInfo& event) -> std::string {
    std::string out;
    out += "pages=";
    out += std::to_string(event.peer);
    if (event.op_name.starts_with("pt_load_")) {
        auto const PACKED = static_cast<uint32_t>(event.status);
        uint32_t const ALLOCATED = (PACKED >> 16U) & 0xFFFFU;
        uint32_t const ALREADY = PACKED & 0xFFFFU;
        out += " seg=";
        out += std::to_string(event.channel);
        out += " alloc=";
        out += std::to_string(ALLOCATED);
        out += " already=";
        out += std::to_string(ALREADY);
        out += " addr=";
        out += std::string(display_callsite(event.callsite));
    } else {
        out += " site=";
        out += std::string(display_callsite(event.callsite));
    }
    return out;
}

auto format_local_xfs_detail(const EventInfo& event) -> std::string {
    std::string out(display_callsite(event.callsite));
    out += " bytes~";
    out += std::to_string(static_cast<uint64_t>(event.channel) * 1024ULL);
    return out;
}

auto format_local_irq_kind(uint64_t kind) -> std::string_view {
    switch (kind) {
        case 1:
            return "context";
        case 2:
            return "legacy";
        case 3:
            return "unhandled";
        case 4:
            return "timer";
        default:
            return "?";
    }
}

auto format_local_irq_detail(const EventInfo& event) -> std::string {
    std::string out;
    out += "vector=";
    out += std::to_string(event.peer);
    out += " kind=";
    out += std::string(format_local_irq_kind(event.channel));
    out += " site=";
    out += std::string(display_callsite(event.callsite));
    return out;
}

auto format_wki_trace_detail(const EventInfo& event) -> std::string {
    std::string out(display_callsite(event.callsite));
    if (is_local_vmem_event(event)) {
        return format_local_vmem_detail(event);
    }
    if (is_local_loader_event(event)) {
        return format_local_loader_detail(event);
    }
    if (is_local_xfs_event(event)) {
        return format_local_xfs_detail(event);
    }
    if (is_local_irq_event(event)) {
        return format_local_irq_detail(event);
    }
    if (!is_transport_stall_event(event)) {
        return out;
    }

    out += " ";
    out += format_transport_stall_detail(event);
    return out;
}

auto display_wait_channel(std::string_view wait_channel) -> std::string_view {
    return wait_channel.empty() ? UNKNOWN_CALLSITE : wait_channel;
}

auto make_wki_peer_resolver(std::string_view buffer, bool sectioned, const WkiDisplayOptions& options) -> WkiPeerResolver {
    WkiPeerResolver resolver{};
    resolver.show_peer_ids = options.show_peer_ids;

    auto insert_entries = [&](const std::vector<WkiPeerMapEntry>& entries) {
        for (const auto& entry : entries) {
            if (entry.hostname.empty()) {
                continue;
            }
            resolver.hostnames.try_emplace(entry.peer, entry.hostname);
        }
    };

    if (sectioned) {
        insert_entries(parse_peer_map_section(buffer));
    }
    insert_entries(collect_live_wki_peer_map());
    resolver.display_cache.reserve(resolver.hostnames.size());
    return resolver;
}

auto wki_peer_label(uint64_t peer, WkiPeerResolver& resolver) -> const std::string& {
    auto cached = resolver.display_cache.find(peer);
    if (cached != resolver.display_cache.end()) {
        return cached->second;
    }

    auto [it, _] = resolver.display_cache.emplace(peer, std::string{});
    auto mapped = resolver.hostnames.find(peer);
    if (!resolver.show_peer_ids && mapped != resolver.hostnames.end() && !mapped->second.empty()) {
        it->second = mapped->second;
    } else {
        it->second = std::to_string(peer);
    }
    return it->second;
}

auto hotspot_for(std::vector<HotspotStats>& rows, uint64_t pid, std::string_view callsite, std::string_view wait_channel,
                 const std::vector<ProcMapEntry>& proc_map) -> HotspotStats& {
    std::string_view const DISPLAY_SITE = display_callsite(callsite);
    std::string_view const DISPLAY_WAIT = display_wait_channel(wait_channel);
    for (auto& row : rows) {
        if (row.pid == pid && row.callsite == DISPLAY_SITE && row.wait_channel == DISPLAY_WAIT) {
            return row;
        }
    }

    HotspotStats row{};
    row.pid = pid;
    row.callsite = std::string(DISPLAY_SITE);
    row.wait_channel = std::string(DISPLAY_WAIT);
    row.comm = std::string(comm_of_pid(proc_map, pid));
    rows.push_back(std::move(row));
    return rows.back();
}

auto summarize_hotspots(std::string_view events, bool sectioned, const std::vector<ProcMapEntry>& proc_map) -> std::vector<HotspotStats> {
    std::vector<HotspotStats> rows;
    std::size_t pos = 0;
    EventInfo event{};
    while (next_event(events, pos, sectioned, event)) {
        auto& row = hotspot_for(rows, event.pid, event.callsite, event.wait_channel, proc_map);
        switch (event.type) {
            case 'X':
                if ((event.flags & FLAG_YIELD) != 0U) {
                    row.yield_count++;
                    row.yield_run_total_us += event.aux;
                    row.yield_run_max_us = std::max(row.yield_run_max_us, event.aux);
                    if (event.aux != 0U && event.aux <= SHORT_RUN_THRESHOLD_US) {
                        row.short_yield_count++;
                    }
                }
                break;

            case 'B':
                row.sleep_count++;
                row.sleep_run_total_us += event.aux;
                if (event.aux != 0U && event.aux <= SHORT_RUN_THRESHOLD_US) {
                    row.short_sleep_count++;
                }
                break;

            case 'W':
                row.wake_count++;
                row.wake_sleep_total_us += event.aux;
                row.wake_sleep_max_us = std::max(row.wake_sleep_max_us, event.aux);
                if (event.aux != 0U && event.aux <= SHORT_SLEEP_THRESHOLD_US) {
                    row.short_wake_count++;
                }
                if ((event.flags & FLAG_EXPLICIT_WAKE) != 0U) {
                    row.explicit_wake_count++;
                }
                if ((event.flags & FLAG_WAKE_CURRENT) != 0U) {
                    row.current_wake_count++;
                }
                break;

            default:
                break;
        }
    }
    return rows;
}

}  // namespace perf
