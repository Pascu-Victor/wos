#include "structured_output.hpp"

#include <abi-bits/wait.h>
#include <abi/callnums/process.h>
#include <sys/callnums.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <limits>
#include <optional>
#include <print>
#include <string>
#include <string_view>
#include <wos/telemetry.hpp>

#include "decode.hpp"

namespace wos::strace {
namespace {

// Keep the complete JSONL write at or below WOS PIPE_BUF. This makes one
// record one write even when fork-follow helpers share stdout/a pipe.
constexpr size_t MAX_STRUCTURED_RECORD_BYTES = 4096;
constexpr size_t MAX_DISPLAY_BYTES = 384;

auto timestamp_ns(const timespec& timestamp) -> uint64_t {
    if (timestamp.tv_sec < 0 || timestamp.tv_nsec < 0 || timestamp.tv_nsec >= 1'000'000'000L) {
        return 0;
    }
    const auto SECONDS = static_cast<uint64_t>(timestamp.tv_sec);
    const auto NANOSECONDS = static_cast<uint64_t>(timestamp.tv_nsec);
    if (SECONDS > (std::numeric_limits<uint64_t>::max() - NANOSECONDS) / 1'000'000'000ULL) {
        return std::numeric_limits<uint64_t>::max();
    }
    return (SECONDS * 1'000'000'000ULL) + NANOSECONDS;
}

auto duration_ns(const timespec& start, const timespec& end) -> uint64_t {
    const uint64_t START = timestamp_ns(start);
    const uint64_t END = timestamp_ns(end);
    return END >= START ? END - START : 0;
}

auto node_name() -> const std::string& {
    static const std::string NAME = [] {
        std::array<char, 256> hostname{};
        if (gethostname(hostname.data(), hostname.size() - 1) != 0 || hostname.front() == '\0') {
            return std::string{};
        }
        return std::string(hostname.data());
    }();
    return NAME;
}

auto identity(uint64_t pid, uint64_t tid) -> telemetry::Value::Object {
    telemetry::Value::Object result{{"pid", telemetry::Value::unsigned_integer(pid)}};
    if (tid != 0) {
        result.emplace("tid", telemetry::Value::unsigned_integer(tid));
    }
    if (!node_name().empty()) {
        result.emplace("node_id", node_name());
    }
    return result;
}

auto clock(const timespec& observed_at) -> telemetry::Value::Object {
    return {{"domain", "observer.monotonic"},
            {"value", telemetry::Value::unsigned_integer(timestamp_ns(observed_at))},
            {"unit", "ns"},
            {"quality", "local_observer"}};
}

auto syscall_id(uint64_t pid, uint64_t tid, const PendingSyscall& pending) -> std::string {
    return std::to_string(pid) + ':' + std::to_string(tid) + ':' + std::to_string(pending.sequence);
}

auto correlation(uint64_t pid, uint64_t tid, const PendingSyscall& pending) -> telemetry::Value::Object {
    return {{"trace_pid", telemetry::Value::unsigned_integer(pid)},
            {"trace_tid", telemetry::Value::unsigned_integer(tid)},
            {"syscall_sequence", telemetry::Value::unsigned_integer(pending.sequence)},
            {"syscall_id", syscall_id(pid, tid, pending)}};
}

auto valid_utf8(std::string_view text) -> bool {
    for (size_t offset = 0; offset < text.size();) {
        const auto first = static_cast<unsigned char>(text[offset]);
        if (first < 0x80U) {
            ++offset;
            continue;
        }
        size_t width = 0;
        if (first >= 0xc2U && first <= 0xdfU) {
            width = 2;
        } else if (first >= 0xe0U && first <= 0xefU) {
            width = 3;
        } else if (first >= 0xf0U && first <= 0xf4U) {
            width = 4;
        } else {
            return false;
        }
        if (offset + width > text.size()) {
            return false;
        }
        for (size_t index = 1; index < width; ++index) {
            if ((static_cast<unsigned char>(text[offset + index]) & 0xc0U) != 0x80U) {
                return false;
            }
        }
        const auto second = static_cast<unsigned char>(text[offset + 1]);
        if ((width == 3 && ((first == 0xe0U && second < 0xa0U) || (first == 0xedU && second >= 0xa0U))) ||
            (width == 4 && ((first == 0xf0U && second < 0x90U) || (first == 0xf4U && second >= 0x90U)))) {
            return false;
        }
        offset += width;
    }
    return true;
}

auto display_hex(std::string_view display) -> std::string {
    static constexpr char HEX[] = "0123456789abcdef";
    const size_t SIZE = std::min(display.size(), MAX_DISPLAY_BYTES);
    std::string result;
    result.reserve(SIZE * 2);
    for (const unsigned char byte : display.substr(0, SIZE)) {
        result.push_back(HEX[byte >> 4U]);
        result.push_back(HEX[byte & 0xfU]);
    }
    return result;
}

void add_display(telemetry::Value::Object& payload, std::string_view display) {
    const size_t RAW_SIZE = std::min(display.size(), MAX_DISPLAY_BYTES);
    size_t size = RAW_SIZE;
    bool bounded_valid = valid_utf8(display.substr(0, size));
    // If the output bound split a valid final code point, back up to its lead
    // byte. Do not discard genuinely invalid tracee bytes to make a prefix
    // appear valid: those are retained through the hex fallback below.
    if (!bounded_valid && RAW_SIZE < display.size()) {
        for (size_t backoff = 1; backoff <= 3 && backoff <= RAW_SIZE; ++backoff) {
            const size_t CANDIDATE = RAW_SIZE - backoff;
            const auto LEAD = static_cast<unsigned char>(display[CANDIDATE]);
            const size_t WIDTH = LEAD >= 0xc2U && LEAD <= 0xdfU   ? 2
                                 : LEAD >= 0xe0U && LEAD <= 0xefU ? 3
                                 : LEAD >= 0xf0U && LEAD <= 0xf4U ? 4
                                                                  : 0;
            if (WIDTH != 0 && CANDIDATE + WIDTH > RAW_SIZE && CANDIDATE + WIDTH <= display.size() &&
                valid_utf8(display.substr(0, CANDIDATE)) && valid_utf8(display.substr(CANDIDATE, WIDTH))) {
                size = CANDIDATE;
                bounded_valid = true;
                break;
            }
        }
    }
    const std::string_view BOUNDED = display.substr(0, size);
    if (bounded_valid) {
        std::string text(BOUNDED);
        if (size != display.size()) {
            text += "...<truncated>";
        }
        payload.emplace("display", std::move(text));
        payload.emplace("display_utf8", true);
    } else {
        payload.emplace("display", nullptr);
        payload.emplace("display_utf8", false);
        payload.emplace("display_bytes_hex", display_hex(display.substr(0, RAW_SIZE)));
    }
    payload.emplace("display_bytes_truncated", RAW_SIZE != display.size());
}

auto arguments(const PendingSyscall& pending) -> telemetry::Value {
    telemetry::Value::Array values;
    values.reserve(6);
    for (const uint64_t value : {pending.a1, pending.a2, pending.a3, pending.a4, pending.a5, pending.a6}) {
        values.push_back(telemetry::Value::unsigned_integer(value));
    }
    return telemetry::Value(std::move(values));
}

void emit_envelope(TraceOutput& output, telemetry::Value envelope) {
    telemetry::Limits limits;
    limits.max_input_bytes = MAX_STRUCTURED_RECORD_BYTES - 1;
    const auto serialized = telemetry::serialize(envelope, limits);
    if (!serialized) {
        std::println(stderr, "strace: failed to serialize structured telemetry: {}", serialized.error.message);
        output.write_failed = true;
        return;
    }
    std::string line = *serialized.text;
    line.push_back('\n');
    if (std::fflush(output.stream) != 0) {
        std::println(stderr, "strace: failed to flush structured telemetry output: {}", std::strerror(errno));
        output.write_failed = true;
        return;
    }
    const int FD = fileno(output.stream);
    if (FD < 0) {
        std::println(stderr, "strace: structured telemetry output has no file descriptor: {}", std::strerror(errno));
        output.write_failed = true;
        return;
    }
    ssize_t written = -1;
    do {
        written = write(FD, line.data(), line.size());
    } while (written < 0 && errno == EINTR);
    if (written < 0) {
        std::println(stderr, "strace: failed to write structured telemetry: {}", std::strerror(errno));
        output.write_failed = true;
    } else if (static_cast<size_t>(written) != line.size()) {
        std::println(stderr, "strace: partial structured telemetry write: wrote {} of {} bytes", written, line.size());
        output.write_failed = true;
    }
}

auto base_syscall_payload(const PendingSyscall& pending) -> telemetry::Value::Object {
    return {{"call_number", telemetry::Value::unsigned_integer(pending.callnum)},
            {"name", std::string(callnum_name(pending.callnum))},
            {"arguments", arguments(pending)},
            {"entry_timestamp_ns", telemetry::Value::unsigned_integer(timestamp_ns(pending.duration_started_at))}};
}

}  // namespace

void emit_structured_syscall(TraceOutput& output, uint64_t pid, uint64_t tid, const PendingSyscall& pending, std::optional<int64_t> result,
                             const timespec& observed_at, std::string_view display, bool paired, bool deferred) {
    auto payload = base_syscall_payload(pending);
    payload.emplace("paired", paired);
    payload.emplace("deferred", deferred);
    add_display(payload, display);
    if (result) {
        payload.emplace("result", telemetry::Value::signed_integer(*result));
        payload.emplace("success", *result >= 0);
        if (*result < 0 && *result != std::numeric_limits<int64_t>::min()) {
            payload.emplace("errno", telemetry::Value::unsigned_integer(static_cast<uint64_t>(-*result)));
        }
        payload.emplace("duration_ns", telemetry::Value::unsigned_integer(duration_ns(pending.duration_started_at, observed_at)));
    }
    emit_envelope(output, telemetry::make_envelope("strace", 1, "strace.syscall", identity(pid, tid), clock(observed_at),
                                                   correlation(pid, tid, pending), std::move(payload)));
}

void emit_structured_signal(TraceOutput& output, uint64_t pid, uint64_t tid, uint32_t signal, const timespec& observed_at) {
    telemetry::Value::Object correlation_values{{"trace_pid", telemetry::Value::unsigned_integer(pid)},
                                                {"trace_tid", telemetry::Value::unsigned_integer(tid)}};
    telemetry::Value::Object payload{{"signal", telemetry::Value::unsigned_integer(signal)}, {"stop", true}};
    emit_envelope(output, telemetry::make_envelope("strace", 1, "strace.signal", identity(pid, tid), clock(observed_at),
                                                   std::move(correlation_values), std::move(payload)));
}

void emit_structured_fork(TraceOutput& output, uint64_t pid, uint64_t tid, uint64_t child_pid, const PendingSyscall& pending,
                          const timespec& observed_at) {
    auto correlation_values = correlation(pid, tid, pending);
    correlation_values.emplace("child_pid", telemetry::Value::unsigned_integer(child_pid));
    telemetry::Value::Object payload{{"parent_pid", telemetry::Value::unsigned_integer(pid)},
                                     {"parent_tid", telemetry::Value::unsigned_integer(tid)},
                                     {"child_pid", telemetry::Value::unsigned_integer(child_pid)},
                                     {"observed_via", "successful_fork_syscall"}};
    emit_envelope(output, telemetry::make_envelope("strace", 1, "strace.process.fork", identity(pid, tid), clock(observed_at),
                                                   std::move(correlation_values), std::move(payload)));
}

void emit_structured_exec(TraceOutput& output, uint64_t pid, uint64_t tid, const PendingSyscall& pending, std::string_view phase,
                          std::optional<int64_t> result, const timespec& observed_at) {
    telemetry::Value::Object payload{
        {"phase", std::string(phase)},
        {"operation", pending.a1 == static_cast<uint64_t>(ker::abi::process::procmgmt_ops::EXECVE) ? telemetry::Value("execve")
                                                                                                   : telemetry::Value("exec")},
        {"observed_via", "process_syscall"}};
    if (result) {
        payload.emplace("result", telemetry::Value::signed_integer(*result));
        payload.emplace("success", *result >= 0);
    }
    emit_envelope(output, telemetry::make_envelope("strace", 1, "strace.process.exec", identity(pid, tid), clock(observed_at),
                                                   correlation(pid, tid, pending), std::move(payload)));
}

void emit_structured_termination(TraceOutput& output, uint64_t pid, uint64_t tid, int wait_status, const timespec& observed_at) {
    telemetry::Value::Object payload;
    if (WIFEXITED(wait_status)) {
        payload.emplace("reason", "exit");
        payload.emplace("exit_status", telemetry::Value::signed_integer(WEXITSTATUS(wait_status)));
    } else if (WIFSIGNALED(wait_status)) {
        payload.emplace("reason", "signal");
        payload.emplace("signal", telemetry::Value::signed_integer(WTERMSIG(wait_status)));
    } else {
        payload.emplace("reason", "wait_status");
        payload.emplace("wait_status", telemetry::Value::signed_integer(wait_status));
    }
    telemetry::Value::Object correlation_values{{"trace_pid", telemetry::Value::unsigned_integer(pid)}};
    emit_envelope(output, telemetry::make_envelope("strace", 1, "strace.process.termination", identity(pid, tid), clock(observed_at),
                                                   std::move(correlation_values), std::move(payload)));
}

auto is_exec_syscall(const PendingSyscall& pending) -> bool {
    if (static_cast<ker::abi::callnums>(pending.callnum) != ker::abi::callnums::process) {
        return false;
    }
    const auto OPERATION = static_cast<ker::abi::process::procmgmt_ops>(pending.a1);
    return OPERATION == ker::abi::process::procmgmt_ops::EXEC || OPERATION == ker::abi::process::procmgmt_ops::EXECVE;
}

}  // namespace wos::strace
