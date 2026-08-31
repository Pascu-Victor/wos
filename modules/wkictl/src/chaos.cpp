#include "wkictl/chaos.hpp"

#include <abi-bits/fcntl.h>
#include <bits/ssize_t.h>
#include <fcntl.h>
#include <time.h>  // NOLINT(modernize-deprecated-headers): POSIX clock_gettime/nanosleep live here in mlibc.
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <print>

#include "wkictl/peer_resolver.hpp"

namespace {

constexpr const char* WKI_CHAOS_PATH = "/proc/wki/chaos";
constexpr const char* WKI_CHAOS_WORKLOAD_PATH = "/proc/wki/chaos_workload";
constexpr std::size_t WKI_CHAOS_COMMAND_MAX = 512;
constexpr std::size_t WKI_CHAOS_ARGUMENT_MAX = 64;
constexpr std::size_t WKI_CHAOS_SNAPSHOT_MAX = 262144;
constexpr std::size_t WKI_CHAOS_WORKLOAD_COMMAND_MAX = 256;
constexpr std::size_t WKI_CHAOS_WORKLOAD_ARGUMENT_MAX = 16;
constexpr std::size_t WKI_CHAOS_WORKLOAD_RESULT_MAX = 2048;
constexpr uint32_t WKI_CHAOS_WAIT_MAX_MS = 300'000;
constexpr long WKI_CHAOS_WAIT_POLL_NS = 10'000'000L;

struct ChaosWaitRequest {
    uint32_t id = 0;
    uint32_t applied = 0;
    uint32_t timeout_ms = 0;
};

struct WorkloadWaitRequest {
    uint32_t waiters = 0;
    uint32_t timeout_ms = 0;
};

enum class SnapshotWaitState : uint8_t {
    WAITING,
    READY,
    INVALID,
};

auto valid_token(const char* token) -> bool {
    if (token == nullptr || token[0] == '\0') {
        return false;
    }
    for (const unsigned char* cursor = reinterpret_cast<const unsigned char*>(token); *cursor != '\0'; ++cursor) {
        const bool VALID = (*cursor >= 'a' && *cursor <= 'z') || (*cursor >= 'A' && *cursor <= 'Z') || (*cursor >= '0' && *cursor <= '9') ||
                           *cursor == '_' || *cursor == '-' || *cursor == '.' || *cursor == ':' || *cursor == '=' || *cursor == '*' ||
                           *cursor == 'x' || *cursor == 'X';
        if (!VALID) {
            return false;
        }
    }
    return true;
}

auto valid_verb(const char* verb) -> bool {
    constexpr std::array VERBS{"clear", "disable", "enable", "heal", "release", "rule", "wait"};
    for (const char* candidate : VERBS) {
        if (std::strcmp(verb, candidate) == 0) {
            return true;
        }
    }
    return false;
}

auto valid_workload_verb(const char* verb) -> bool {
    constexpr std::array VERBS{"clear",
                               "block",
                               "block-discovered",
                               "block-rdma-discovered",
                               "net-attach",
                               "net-attach-discovered",
                               "net-query",
                               "net-query-discovered",
                               "net-detach",
                               "net-detach-discovered",
                               "vfs-unmount",
                               "vfs-query-discovered",
                               "vfs-unmount-discovered",
                               "compute-publish",
                               "compute-publish-query"};
    for (const char* candidate : VERBS) {
        if (std::strcmp(verb, candidate) == 0) {
            return true;
        }
    }
    return false;
}

enum class ChaosPeerSelector : uint8_t {
    NONE,
    NEIGHBOR,
    SRC,
    DST,
};

auto chaos_peer_selector(const char* token, bool* hostname_alias) -> ChaosPeerSelector {
    if (hostname_alias != nullptr) {
        *hostname_alias = false;
    }
    if (token == nullptr) {
        return ChaosPeerSelector::NONE;
    }
    const char* const EQUALS = std::strchr(token, '=');
    if (EQUALS == nullptr) {
        return ChaosPeerSelector::NONE;
    }
    std::size_t const KEY_LENGTH = static_cast<std::size_t>(EQUALS - token);
    struct SelectorName {
        const char* numeric;
        const char* hostname;
        ChaosPeerSelector selector;
    };
    constexpr std::array SELECTORS{SelectorName{"neighbor", "neighbor_host", ChaosPeerSelector::NEIGHBOR},
                                   SelectorName{"src", "src_host", ChaosPeerSelector::SRC},
                                   SelectorName{"dst", "dst_host", ChaosPeerSelector::DST}};
    for (const auto& selector : SELECTORS) {
        std::size_t const NUMERIC_LENGTH = std::strlen(selector.numeric);
        std::size_t const HOSTNAME_LENGTH = std::strlen(selector.hostname);
        if (KEY_LENGTH == NUMERIC_LENGTH && std::memcmp(token, selector.numeric, KEY_LENGTH) == 0) {
            return selector.selector;
        }
        if (KEY_LENGTH == HOSTNAME_LENGTH && std::memcmp(token, selector.hostname, KEY_LENGTH) == 0) {
            if (hostname_alias != nullptr) {
                *hostname_alias = true;
            }
            return selector.selector;
        }
    }
    return ChaosPeerSelector::NONE;
}

auto chaos_peer_selector_name(ChaosPeerSelector selector) -> const char* {
    switch (selector) {
        case ChaosPeerSelector::NEIGHBOR:
            return "neighbor";
        case ChaosPeerSelector::SRC:
            return "src";
        case ChaosPeerSelector::DST:
            return "dst";
        case ChaosPeerSelector::NONE:
            break;
    }
    return "unknown";
}

auto parse_u32(const char* text, uint32_t* out) -> bool {
    if (text == nullptr || out == nullptr || text[0] == '\0' || text[0] == '+' || text[0] == '-') {
        return false;
    }
    errno = 0;
    char* end = nullptr;
    unsigned long long const VALUE = std::strtoull(text, &end, 0);
    if (errno != 0 || end == text || *end != '\0' || VALUE > UINT32_MAX) {
        return false;
    }
    *out = static_cast<uint32_t>(VALUE);
    return true;
}

auto parse_wait_request(int argc, char** argv, ChaosWaitRequest* out) -> bool {
    if (argc != 6 || out == nullptr) {
        return false;
    }
    bool have_id = false;
    bool have_applied = false;
    bool have_timeout = false;
    for (int index = 3; index < argc; ++index) {
        const char* token = argv[index];
        const char* equals = token != nullptr ? std::strchr(token, '=') : nullptr;
        if (equals == nullptr || equals == token || equals[1] == '\0') {
            return false;
        }
        std::size_t const KEY_LEN = static_cast<std::size_t>(equals - token);
        if (KEY_LEN == 2 && std::strncmp(token, "id", KEY_LEN) == 0 && !have_id) {
            have_id = parse_u32(equals + 1, &out->id);
        } else if (((KEY_LEN == 7 && std::strncmp(token, "applied", KEY_LEN) == 0) ||
                    (KEY_LEN == 7 && std::strncmp(token, "matched", KEY_LEN) == 0)) &&
                   !have_applied) {
            have_applied = parse_u32(equals + 1, &out->applied);
        } else if (KEY_LEN == 10 && std::strncmp(token, "timeout_ms", KEY_LEN) == 0 && !have_timeout) {
            have_timeout = parse_u32(equals + 1, &out->timeout_ms);
        } else {
            return false;
        }
    }
    return have_id && have_applied && have_timeout && out->id != 0 && out->applied != 0 && out->timeout_ms != 0 &&
           out->timeout_ms <= WKI_CHAOS_WAIT_MAX_MS;
}

auto parse_workload_wait_request(int argc, char** argv, WorkloadWaitRequest* out) -> bool {
    if (argc != 5 || out == nullptr) {
        return false;
    }
    bool have_waiters = false;
    bool have_timeout = false;
    for (int index = 3; index < argc; ++index) {
        const char* token = argv[index];
        const char* equals = token != nullptr ? std::strchr(token, '=') : nullptr;
        if (equals == nullptr || equals == token || equals[1] == '\0') {
            return false;
        }
        std::size_t const KEY_LEN = static_cast<std::size_t>(equals - token);
        if (KEY_LEN == 7 && std::strncmp(token, "waiters", KEY_LEN) == 0 && !have_waiters) {
            have_waiters = parse_u32(equals + 1, &out->waiters);
        } else if (KEY_LEN == 10 && std::strncmp(token, "timeout_ms", KEY_LEN) == 0 && !have_timeout) {
            have_timeout = parse_u32(equals + 1, &out->timeout_ms);
        } else {
            return false;
        }
    }
    return have_waiters && have_timeout && out->waiters == 1 && out->timeout_ms != 0 && out->timeout_ms <= WKI_CHAOS_WAIT_MAX_MS;
}

auto monotonic_ms() -> uint64_t {
    timespec now{};
    if (clock_gettime(CLOCK_MONOTONIC, &now) != 0 || now.tv_sec < 0 || now.tv_nsec < 0 || now.tv_nsec >= 1'000'000'000L) {
        return UINT64_MAX;
    }
    constexpr uint64_t MS_PER_SECOND = 1'000;
    uint64_t const SECONDS = static_cast<uint64_t>(now.tv_sec);
    uint64_t const NSEC_MS = static_cast<uint64_t>(now.tv_nsec) / 1'000'000U;
    return SECONDS > (UINT64_MAX - NSEC_MS) / MS_PER_SECOND ? UINT64_MAX : (SECONDS * MS_PER_SECOND) + NSEC_MS;
}

auto read_complete_file(const char* path, char* buffer, std::size_t capacity, std::size_t* size) -> bool {
    if (path == nullptr || buffer == nullptr || capacity < 2 || size == nullptr) {
        errno = EINVAL;
        return false;
    }
    int const FD = open(path, O_RDONLY);
    if (FD < 0) {
        return false;
    }
    std::size_t total = 0;
    bool ok = true;
    while (total < capacity - 1) {
        ssize_t const READ = read(FD, buffer + total, capacity - 1 - total);
        if (READ < 0 && errno == EINTR) {
            continue;
        }
        if (READ < 0) {
            ok = false;
            break;
        }
        if (READ == 0) {
            break;
        }
        total += static_cast<std::size_t>(READ);
    }
    if (ok && total == capacity - 1) {
        char extra = 0;
        ssize_t read_extra = -1;
        do {
            read_extra = read(FD, &extra, 1);
        } while (read_extra < 0 && errno == EINTR);
        ok = read_extra == 0;
        if (!ok && read_extra > 0) {
            errno = EOVERFLOW;
        }
    }
    int saved_errno = ok ? 0 : errno;
    if (close(FD) != 0 && ok) {
        ok = false;
        saved_errno = errno;
    }
    if (!ok || total == 0 || buffer[total - 1] != '\n') {
        errno = ok ? EBADMSG : saved_errno;
        return false;
    }
    buffer[total] = '\0';
    *size = total;
    return true;
}

auto read_chaos_snapshot(char* buffer, std::size_t capacity, std::size_t* size) -> bool {
    return read_complete_file(WKI_CHAOS_PATH, buffer, capacity, size);
}

auto read_workload_snapshot(char* buffer, std::size_t capacity, std::size_t* size) -> bool {
    return read_complete_file(WKI_CHAOS_WORKLOAD_PATH, buffer, capacity, size);
}

auto row_is(const char* line, std::size_t line_len, const char* row) -> bool {
    std::size_t const ROW_LEN = std::strlen(row);
    return line_len >= ROW_LEN && std::memcmp(line, row, ROW_LEN) == 0 && (line_len == ROW_LEN || line[ROW_LEN] == ' ');
}

auto field_value(const char* line, std::size_t line_len, const char* key, char* out, std::size_t out_size) -> bool {
    std::size_t const KEY_LEN = std::strlen(key);
    bool found = false;
    std::size_t offset = 0;
    while (offset < line_len && line[offset] != ' ') {
        ++offset;
    }
    while (offset < line_len) {
        if (line[offset] != ' ') {
            return false;
        }
        ++offset;
        if (offset == line_len || line[offset] == ' ') {
            return false;
        }
        std::size_t const START = offset;
        while (offset < line_len && line[offset] != ' ') {
            ++offset;
        }
        std::size_t const TOKEN_LEN = offset - START;
        const char* equals = static_cast<const char*>(std::memchr(line + START, '=', TOKEN_LEN));
        if (equals == nullptr || equals == line + START || equals == line + START + TOKEN_LEN - 1) {
            return false;
        }
        std::size_t const TOKEN_KEY_LEN = static_cast<std::size_t>(equals - (line + START));
        if (TOKEN_KEY_LEN != KEY_LEN || std::memcmp(line + START, key, KEY_LEN) != 0) {
            continue;
        }
        if (found) {
            return false;
        }
        std::size_t const VALUE_LEN = TOKEN_LEN - TOKEN_KEY_LEN - 1;
        if (VALUE_LEN + 1 > out_size) {
            return false;
        }
        std::memcpy(out, equals + 1, VALUE_LEN);
        out[VALUE_LEN] = '\0';
        found = true;
    }
    return found;
}

auto u32_field(const char* line, std::size_t line_len, const char* key, uint32_t* out) -> bool {
    std::array<char, 32> value{};
    return field_value(line, line_len, key, value.data(), value.size()) && parse_u32(value.data(), out);
}

auto snapshot_wait_state(const char* snapshot, std::size_t size, const ChaosWaitRequest& request) -> SnapshotWaitState {
    bool status_seen = false;
    bool rule_seen = false;
    bool terminal_seen = false;
    uint32_t queued = 0;
    uint32_t applied = 0;
    std::array<char, 32> action{};
    std::size_t offset = 0;
    std::size_t line_index = 0;
    while (offset < size) {
        const char* newline = static_cast<const char*>(std::memchr(snapshot + offset, '\n', size - offset));
        if (newline == nullptr) {
            return SnapshotWaitState::INVALID;
        }
        std::size_t const LINE_LEN = static_cast<std::size_t>(newline - (snapshot + offset));
        const char* line = snapshot + offset;
        if (LINE_LEN == 0 || terminal_seen) {
            return SnapshotWaitState::INVALID;
        }
        if (row_is(line, LINE_LEN, "wki_chaos")) {
            if (status_seen || line_index != 0) {
                return SnapshotWaitState::INVALID;
            }
            uint32_t schema = 0;
            uint32_t supported = 0;
            uint32_t runtime_control = 0;
            uint32_t enabled = 0;
            uint32_t queue_overflow = 0;
            uint32_t trace_overflow = 0;
            uint32_t stream_overflow = 0;
            uint32_t invalid = 0;
            if (!u32_field(line, LINE_LEN, "schema", &schema) || schema != 1 || !u32_field(line, LINE_LEN, "supported", &supported) ||
                supported != 1 || !u32_field(line, LINE_LEN, "runtime_control", &runtime_control) || runtime_control != 1 ||
                !u32_field(line, LINE_LEN, "enabled", &enabled) || enabled != 1 || !u32_field(line, LINE_LEN, "queued", &queued) ||
                !u32_field(line, LINE_LEN, "queue_overflow", &queue_overflow) || queue_overflow != 0 ||
                !u32_field(line, LINE_LEN, "trace_overflow", &trace_overflow) || trace_overflow != 0 ||
                !u32_field(line, LINE_LEN, "stream_overflow", &stream_overflow) || stream_overflow != 0 ||
                !u32_field(line, LINE_LEN, "invalid", &invalid) || invalid != 0) {
                return SnapshotWaitState::INVALID;
            }
            status_seen = true;
        } else if (row_is(line, LINE_LEN, "wki_chaos_rule")) {
            uint32_t id = 0;
            if (!u32_field(line, LINE_LEN, "id", &id)) {
                return SnapshotWaitState::INVALID;
            }
            if (id == request.id) {
                uint32_t active = 0;
                if (rule_seen || !u32_field(line, LINE_LEN, "active", &active) || active != 1 ||
                    !u32_field(line, LINE_LEN, "applied", &applied) ||
                    !field_value(line, LINE_LEN, "action", action.data(), action.size())) {
                    return SnapshotWaitState::INVALID;
                }
                rule_seen = true;
            }
        } else if (row_is(line, LINE_LEN, "wki_chaos_end")) {
            uint32_t complete = 0;
            if (!u32_field(line, LINE_LEN, "complete", &complete) || complete != 1 || newline + 1 != snapshot + size) {
                return SnapshotWaitState::INVALID;
            }
            terminal_seen = true;
        }
        offset = static_cast<std::size_t>(newline + 1 - snapshot);
        ++line_index;
    }
    if (!status_seen || !rule_seen || !terminal_seen || applied > request.applied) {
        return SnapshotWaitState::INVALID;
    }
    if (applied < request.applied) {
        return SnapshotWaitState::WAITING;
    }
    bool const HELD_ACTION = std::strcmp(action.data(), "delay") == 0 || std::strcmp(action.data(), "reorder") == 0;
    if (HELD_ACTION && queued < request.applied) {
        return SnapshotWaitState::INVALID;
    }
    return SnapshotWaitState::READY;
}

auto wait_for_rule(const ChaosWaitRequest& request) -> int {
    auto snapshot = std::unique_ptr<char[]>(new (std::nothrow) char[WKI_CHAOS_SNAPSHOT_MAX + 2]);
    if (snapshot == nullptr) {
        std::println(stderr, "wkictl chaos wait: cannot allocate bounded snapshot buffer");
        return 1;
    }
    uint64_t const START = monotonic_ms();
    if (START == UINT64_MAX || START > UINT64_MAX - request.timeout_ms) {
        std::println(stderr, "wkictl chaos wait: monotonic clock unavailable");
        return 1;
    }
    uint64_t const DEADLINE = START + request.timeout_ms;
    while (true) {
        std::size_t size = 0;
        if (!read_chaos_snapshot(snapshot.get(), WKI_CHAOS_SNAPSHOT_MAX + 1, &size)) {
            std::println(stderr, "wkictl chaos wait: incomplete snapshot: {}", std::strerror(errno));
            return 1;
        }
        SnapshotWaitState const STATE = snapshot_wait_state(snapshot.get(), size, request);
        if (STATE == SnapshotWaitState::READY) {
            return 0;
        }
        if (STATE == SnapshotWaitState::INVALID) {
            std::println(stderr, "wkictl chaos wait: invalid or contradictory snapshot for rule {}", request.id);
            return 1;
        }
        uint64_t const NOW = monotonic_ms();
        if (NOW == UINT64_MAX || NOW >= DEADLINE) {
            std::println(stderr, "wkictl chaos wait: timed out waiting for rule {} applied={}", request.id, request.applied);
            return 1;
        }
        uint64_t const REMAINING_MS = DEADLINE - NOW;
        uint64_t const DELAY_NS = REMAINING_MS >= 10 ? static_cast<uint64_t>(WKI_CHAOS_WAIT_POLL_NS) : REMAINING_MS * 1'000'000U;
        timespec delay{.tv_sec = 0, .tv_nsec = static_cast<long>(DELAY_NS)};
        while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {
        }
    }
}

auto workload_wait_ready(const char* snapshot, std::size_t size, const WorkloadWaitRequest& request) -> SnapshotWaitState {
    if (snapshot == nullptr || size == 0 || snapshot[size - 1] != '\n' || std::memchr(snapshot, '\n', size - 1) != nullptr) {
        return SnapshotWaitState::INVALID;
    }
    std::size_t const LINE_LEN = size - 1;
    if (!row_is(snapshot, LINE_LEN, "wki_chaos_workload")) {
        return SnapshotWaitState::INVALID;
    }
    uint32_t schema = 0;
    uint32_t enabled = 0;
    uint32_t hold = 0;
    uint32_t waiters = 0;
    if (!u32_field(snapshot, LINE_LEN, "schema", &schema) || schema != 1 || !u32_field(snapshot, LINE_LEN, "enabled", &enabled) ||
        enabled != 1 || !u32_field(snapshot, LINE_LEN, "compute_hold", &hold) ||
        !u32_field(snapshot, LINE_LEN, "compute_waiters", &waiters)) {
        return SnapshotWaitState::INVALID;
    }
    if (hold != 1) {
        return SnapshotWaitState::INVALID;
    }
    return waiters >= request.waiters ? SnapshotWaitState::READY : SnapshotWaitState::WAITING;
}

auto wait_for_compute_publish(const WorkloadWaitRequest& request) -> int {
    std::array<char, WKI_CHAOS_WORKLOAD_RESULT_MAX + 1> snapshot{};
    uint64_t const START = monotonic_ms();
    if (START == UINT64_MAX || START > UINT64_MAX - request.timeout_ms) {
        std::println(stderr, "wkictl chaos-workload wait: monotonic clock unavailable");
        return 1;
    }
    uint64_t const DEADLINE = START + request.timeout_ms;
    while (true) {
        std::size_t size = 0;
        if (!read_workload_snapshot(snapshot.data(), snapshot.size(), &size)) {
            std::println(stderr, "wkictl chaos-workload wait: incomplete snapshot: {}", std::strerror(errno));
            return 1;
        }
        SnapshotWaitState const STATE = workload_wait_ready(snapshot.data(), size, request);
        if (STATE == SnapshotWaitState::READY) {
            return 0;
        }
        if (STATE == SnapshotWaitState::INVALID) {
            std::println(stderr, "wkictl chaos-workload wait: invalid, disabled, or released publication gate");
            return 1;
        }
        uint64_t const NOW = monotonic_ms();
        if (NOW == UINT64_MAX || NOW >= DEADLINE) {
            std::println(stderr, "wkictl chaos-workload wait: timed out waiting for receiver submit worker");
            return 1;
        }
        uint64_t const REMAINING_MS = DEADLINE - NOW;
        uint64_t const DELAY_NS = REMAINING_MS >= 10 ? static_cast<uint64_t>(WKI_CHAOS_WAIT_POLL_NS) : REMAINING_MS * 1'000'000U;
        timespec delay{.tv_sec = 0, .tv_nsec = static_cast<long>(DELAY_NS)};
        while (nanosleep(&delay, &delay) != 0 && errno == EINTR) {
        }
    }
}

auto write_command(int fd, const char* data, std::size_t size) -> bool {
    while (true) {
        ssize_t const RESULT = write(fd, data, size);
        if (RESULT < 0 && errno == EINTR) {
            continue;
        }
        if (RESULT >= 0 && static_cast<std::size_t>(RESULT) != size) {
            errno = EIO;
            return false;
        }
        return RESULT >= 0;
    }
}

auto print_workload_result() -> bool {
    std::array<char, WKI_CHAOS_WORKLOAD_RESULT_MAX + 1> snapshot{};
    std::size_t size = 0;
    if (!read_workload_snapshot(snapshot.data(), snapshot.size(), &size)) {
        std::println(stderr, "wkictl chaos-workload: incomplete result: {}", std::strerror(errno));
        return false;
    }
    if (std::fwrite(snapshot.data(), 1, size, stdout) != size || std::fflush(stdout) != 0) {
        std::println(stderr, "wkictl chaos-workload: stdout write failed");
        return false;
    }
    return true;
}

}  // namespace

namespace wkictl {

auto handle_chaos(int argc, char** argv) -> int {
    if (argc < 3 || argc > static_cast<int>(WKI_CHAOS_ARGUMENT_MAX + 2) || !valid_verb(argv[2])) {
        std::println(stderr,
                     "usage: wkictl chaos <clear|disable|enable|heal|release|rule> [key=value ...]\n"
                     "       rule selectors include op=<u16|*> and neighbor_host/src_host/dst_host=<hostname>\n"
                     "       corrupt defaults to checksum; payload mode is corrupt=payload payload_offset=<u16> "
                     "payload_xor=<nonzero-u8>\n"
                     "       wkictl chaos wait id=<u32> applied=<u32> timeout_ms=<1..300000>  (matched= is compatible)");
        return 64;
    }

    if (std::strcmp(argv[2], "wait") == 0) {
        ChaosWaitRequest request{};
        if (!parse_wait_request(argc, argv, &request)) {
            std::println(stderr, "usage: wkictl chaos wait id=<u32> applied=<u32> timeout_ms=<1..300000>");
            return 64;
        }
        return wait_for_rule(request);
    }

    std::array<char, WKI_CHAOS_COMMAND_MAX> command{};
    std::array<std::array<char, 32>, WKI_CHAOS_ARGUMENT_MAX> resolved_peer_tokens{};
    std::array<bool, 3> have_peer_selector{};
    std::size_t length = 0;
    for (int index = 2; index < argc; ++index) {
        const char* token = argv[index];
        if (!valid_token(token)) {
            std::println(stderr, "wkictl chaos: invalid command token");
            return 64;
        }
        const char* emitted_token = token;
        bool hostname_alias = false;
        ChaosPeerSelector const PEER_SELECTOR = chaos_peer_selector(token, &hostname_alias);
        if (PEER_SELECTOR != ChaosPeerSelector::NONE) {
            std::size_t const SELECTOR_INDEX = static_cast<std::size_t>(PEER_SELECTOR) - 1;
            if (std::strcmp(argv[2], "rule") != 0 || have_peer_selector.at(SELECTOR_INDEX)) {
                std::println(stderr, "wkictl chaos: {} and {}_host are mutually exclusive and singular",
                             chaos_peer_selector_name(PEER_SELECTOR), chaos_peer_selector_name(PEER_SELECTOR));
                return 64;
            }
            have_peer_selector.at(SELECTOR_INDEX) = true;
            if (hostname_alias) {
                const char* const HOSTNAME = std::strchr(token, '=') + 1;
                if (*HOSTNAME == '\0') {
                    std::println(stderr, "wkictl chaos: empty {}_host selector", chaos_peer_selector_name(PEER_SELECTOR));
                    return 64;
                }
                uint16_t node_id = 0;
                if (!wkictl::resolve_peer_hostname(HOSTNAME, &node_id)) {
                    std::println(stderr, "wkictl chaos: cannot resolve {}_host '{}': {}", chaos_peer_selector_name(PEER_SELECTOR), HOSTNAME,
                                 std::strerror(errno));
                    return 1;
                }
                auto& resolved = resolved_peer_tokens.at(static_cast<std::size_t>(index - 2));
                int const TOKEN_LENGTH =
                    std::snprintf(resolved.data(), resolved.size(), "%s=%u", chaos_peer_selector_name(PEER_SELECTOR), node_id);
                if (TOKEN_LENGTH <= 0 || static_cast<std::size_t>(TOKEN_LENGTH) >= resolved.size()) {
                    std::println(stderr, "wkictl chaos: resolved {} is not representable", chaos_peer_selector_name(PEER_SELECTOR));
                    return 1;
                }
                emitted_token = resolved.data();
            }
        }
        std::size_t const TOKEN_LENGTH = std::strlen(emitted_token);
        std::size_t const SEPARATOR = index == 2 ? 0 : 1;
        if (TOKEN_LENGTH >= WKI_CHAOS_COMMAND_MAX || length > WKI_CHAOS_COMMAND_MAX - 1 - SEPARATOR - TOKEN_LENGTH) {
            std::println(stderr, "wkictl chaos: command exceeds {} bytes", WKI_CHAOS_COMMAND_MAX - 1);
            return 64;
        }
        if (SEPARATOR != 0) {
            command.at(length++) = ' ';
        }
        std::memcpy(command.data() + length, emitted_token, TOKEN_LENGTH);
        length += TOKEN_LENGTH;
    }

    int const FD = open(WKI_CHAOS_PATH, O_WRONLY);
    if (FD < 0) {
        std::println(stderr, "wkictl chaos: cannot open {}: {}", WKI_CHAOS_PATH, std::strerror(errno));
        return 1;
    }
    bool const WRITTEN = write_command(FD, command.data(), length);
    int const SAVED_ERRNO = errno;
    int const CLOSE_RESULT = close(FD);
    if (!WRITTEN) {
        std::println(stderr, "wkictl chaos: write failed: {}", std::strerror(SAVED_ERRNO));
        return 1;
    }
    if (CLOSE_RESULT != 0) {
        std::println(stderr, "wkictl chaos: close failed: {}", std::strerror(errno));
        return 1;
    }
    return 0;
}

auto handle_chaos_workload(int argc, char** argv) -> int {
    if (argc < 3 || argc > static_cast<int>(WKI_CHAOS_WORKLOAD_ARGUMENT_MAX + 2)) {
        std::println(stderr,
                     "usage: wkictl chaos-workload <strict-command> [key=value ...]\n"
                     "       owner_host=<hostname> resolves one connected /proc/wki/peers row to owner=<node_id>\n"
                     "       wkictl chaos-workload compute-publish-wait waiters=1 timeout_ms=<1..300000>");
        return 64;
    }

    if (std::strcmp(argv[2], "compute-publish-wait") == 0) {
        WorkloadWaitRequest request{};
        if (!parse_workload_wait_request(argc, argv, &request)) {
            std::println(stderr, "usage: wkictl chaos-workload compute-publish-wait waiters=1 timeout_ms=<1..300000>");
            return 64;
        }
        return wait_for_compute_publish(request);
    }
    if (!valid_workload_verb(argv[2])) {
        std::println(stderr, "wkictl chaos-workload: invalid command verb");
        return 64;
    }

    std::array<char, WKI_CHAOS_WORKLOAD_COMMAND_MAX + 1> command{};
    std::size_t length = 0;
    bool have_owner_selector = false;
    for (int index = 2; index < argc; ++index) {
        const char* token = argv[index];
        if (!valid_token(token)) {
            std::println(stderr, "wkictl chaos-workload: invalid command token");
            return 64;
        }

        std::array<char, 32> resolved_owner{};
        const char* emitted_token = token;
        if (std::strncmp(token, "owner_host=", 11) == 0) {
            if (have_owner_selector || token[11] == '\0') {
                std::println(stderr, "wkictl chaos-workload: owner and owner_host are mutually exclusive and singular");
                return 64;
            }
            uint16_t owner = 0;
            if (!resolve_peer_hostname(token + 11, &owner)) {
                std::println(stderr, "wkictl chaos-workload: cannot resolve owner_host '{}': {}", token + 11, std::strerror(errno));
                return 1;
            }
            int const OWNER_LENGTH = std::snprintf(resolved_owner.data(), resolved_owner.size(), "owner=%u", owner);
            if (OWNER_LENGTH <= 0 || static_cast<std::size_t>(OWNER_LENGTH) >= resolved_owner.size()) {
                std::println(stderr, "wkictl chaos-workload: resolved owner is not representable");
                return 1;
            }
            emitted_token = resolved_owner.data();
            have_owner_selector = true;
        } else if (std::strncmp(token, "owner=", 6) == 0) {
            if (have_owner_selector) {
                std::println(stderr, "wkictl chaos-workload: owner and owner_host are mutually exclusive and singular");
                return 64;
            }
            have_owner_selector = true;
        }

        std::size_t const TOKEN_LENGTH = std::strlen(emitted_token);
        std::size_t const SEPARATOR = index == 2 ? 0 : 1;
        if (TOKEN_LENGTH > WKI_CHAOS_WORKLOAD_COMMAND_MAX || SEPARATOR > WKI_CHAOS_WORKLOAD_COMMAND_MAX - length ||
            TOKEN_LENGTH > WKI_CHAOS_WORKLOAD_COMMAND_MAX - length - SEPARATOR) {
            std::println(stderr, "wkictl chaos-workload: command exceeds {} bytes", WKI_CHAOS_WORKLOAD_COMMAND_MAX);
            return 64;
        }
        if (SEPARATOR != 0) {
            command.at(length++) = ' ';
        }
        std::memcpy(command.data() + length, emitted_token, TOKEN_LENGTH);
        length += TOKEN_LENGTH;
    }

    int const FD = open(WKI_CHAOS_WORKLOAD_PATH, O_WRONLY);
    if (FD < 0) {
        std::println(stderr, "wkictl chaos-workload: cannot open {}: {}", WKI_CHAOS_WORKLOAD_PATH, std::strerror(errno));
        return 1;
    }
    bool const WRITTEN = write_command(FD, command.data(), length);
    int const SAVED_ERRNO = errno;
    int const CLOSE_RESULT = close(FD);
    if (!WRITTEN) {
        // The kernel records the exact operation outcome before returning its
        // errno. Preserve the failing exit status, but still emit that bounded
        // row so expected fault rejections and restoration remain observable.
        static_cast<void>(print_workload_result());
        std::println(stderr, "wkictl chaos-workload: write failed: {}", std::strerror(SAVED_ERRNO));
        return 1;
    }
    if (CLOSE_RESULT != 0) {
        std::println(stderr, "wkictl chaos-workload: close failed: {}", std::strerror(errno));
        return 1;
    }

    return print_workload_result() ? 0 : 1;
}

}  // namespace wkictl
