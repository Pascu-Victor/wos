#include <abi-bits/in.h>
#include <abi-bits/pid_t.h>
#include <abi-bits/signal.h>
#include <abi-bits/socket.h>
#include <abi-bits/socklen_t.h>
#include <abi-bits/wait.h>
#include <abi/callnums/process.h>
#include <arpa/inet.h>
#include <bits/ssize_t.h>
#include <fcntl.h>
#include <poll.h>
#include <sys/callnums.h>
#include <sys/process.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include <abi/ptrace.hpp>
#include <algorithm>
#include <array>
#include <cerrno>
#include <climits>
#include <csignal>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <format>
#include <print>
#include <string>
#include <string_view>
#include <vector>

namespace {

constexpr uint16_t DEFAULT_PORT = 2159;
constexpr size_t MAX_PACKET = 4096;
constexpr size_t MAX_THREADS = 128;
constexpr uint64_t MAX_MEM_READ = 4096;
constexpr uint8_t X86_INT3 = 0xcc;
constexpr int DEBUGSERVER_PACKET_IO_TIMEOUT_MS = 30000;
constexpr int DEBUGSERVER_EVENT_POLL_MS = 10;
constexpr uint32_t DEBUGSERVER_STARTUP_WAIT_RETRIES = 500;
constexpr useconds_t DEBUGSERVER_STARTUP_WAIT_POLL_US = 10 * 1000;
constexpr int MSEC_PER_SEC = 1000;
constexpr int NSEC_PER_MSEC = 1000000;
constexpr int64_t NSEC_PER_SEC = 1000LL * NSEC_PER_MSEC;

constexpr std::string_view TARGET_XML = R"xml(<?xml version="1.0"?>
<!DOCTYPE target SYSTEM "gdb-target.dtd">
<target>
  <architecture>i386:x86-64</architecture>
  <feature name="org.gnu.gdb.i386.core">
    <reg name="rax" bitsize="64" type="uint64" regnum="0"/>
    <reg name="rbx" bitsize="64" type="uint64" regnum="1"/>
    <reg name="rcx" bitsize="64" type="uint64" regnum="2"/>
    <reg name="rdx" bitsize="64" type="uint64" regnum="3"/>
    <reg name="rsi" bitsize="64" type="uint64" regnum="4"/>
    <reg name="rdi" bitsize="64" type="uint64" regnum="5"/>
    <reg name="rbp" bitsize="64" type="data_ptr" regnum="6"/>
    <reg name="rsp" bitsize="64" type="data_ptr" regnum="7"/>
    <reg name="r8" bitsize="64" type="uint64" regnum="8"/>
    <reg name="r9" bitsize="64" type="uint64" regnum="9"/>
    <reg name="r10" bitsize="64" type="uint64" regnum="10"/>
    <reg name="r11" bitsize="64" type="uint64" regnum="11"/>
    <reg name="r12" bitsize="64" type="uint64" regnum="12"/>
    <reg name="r13" bitsize="64" type="uint64" regnum="13"/>
    <reg name="r14" bitsize="64" type="uint64" regnum="14"/>
    <reg name="r15" bitsize="64" type="uint64" regnum="15"/>
    <reg name="rip" bitsize="64" type="code_ptr" regnum="16" group="general"/>
    <reg name="rflags" bitsize="64" type="i386_eflags" regnum="17" group="general"/>
    <reg name="cs" bitsize="64" type="uint64" regnum="18"/>
    <reg name="ss" bitsize="64" type="uint64" regnum="19"/>
    <reg name="fs_base" bitsize="64" type="data_ptr" regnum="20"/>
    <reg name="gs_base" bitsize="64" type="data_ptr" regnum="21"/>
  </feature>
</target>
)xml";

struct SoftwareBreakpoint {
    uint64_t address = 0;
    uint8_t original_byte = 0;
    bool installed = false;
};

struct HardwareBreakpoint {
    uint64_t address = 0;
    uint64_t kind = 0;
    ker::abi::ptrace::hw_break_type type = ker::abi::ptrace::hw_break_type::EXECUTE;
    uint32_t slot = 0;
};

struct Session {
    int fd = -1;
    uint64_t pid = 0;
    bool attached = true;
    bool tracing_active = true;
    bool no_ack = false;
    bool received_interrupt = false;
    std::vector<SoftwareBreakpoint> software_breakpoints;
    std::vector<HardwareBreakpoint> hardware_breakpoints;
};

struct LaunchOptions {
    uint16_t port = DEFAULT_PORT;
    uint64_t attach_pid = 0;
    bool break_on_start = false;
    char** command_argv = nullptr;
};

auto hex_digit(uint8_t value) -> char {
    value &= 0xf;
    return value < 10 ? static_cast<char>('0' + value) : static_cast<char>('a' + value - 10);
}

auto from_hex(char c) -> int {
    if (c >= '0' && c <= '9') {
        return c - '0';
    }
    if (c >= 'a' && c <= 'f') {
        return c - 'a' + 10;
    }
    if (c >= 'A' && c <= 'F') {
        return c - 'A' + 10;
    }
    return -1;
}

auto checksum(std::string_view payload) -> uint8_t {
    uint8_t sum = 0;
    for (const char C : payload) {
        sum = static_cast<uint8_t>(sum + static_cast<uint8_t>(C));
    }
    return sum;
}

auto monotonic_now_ms() -> int64_t {
    timespec ts{};
    if (clock_gettime(CLOCK_MONOTONIC, &ts) != 0) {
        return -1;
    }

    if (ts.tv_sec < 0 || ts.tv_nsec < 0 || ts.tv_nsec >= NSEC_PER_SEC) {
        return -1;
    }

    int64_t const NSEC_MS = static_cast<int64_t>(ts.tv_nsec) / NSEC_PER_MSEC;
    auto const SEC = static_cast<int64_t>(ts.tv_sec);
    if (SEC > (INT64_MAX - NSEC_MS) / MSEC_PER_SEC) {
        return INT64_MAX;
    }

    return (SEC * MSEC_PER_SEC) + NSEC_MS;
}

auto deadline_after_ms(int timeout_ms) -> int64_t {
    int64_t const NOW_MS = monotonic_now_ms();
    if (NOW_MS < 0) {
        return -1;
    }
    if (timeout_ms <= 0) {
        return NOW_MS;
    }

    auto const TIMEOUT_MS = static_cast<int64_t>(timeout_ms);
    if (INT64_MAX - NOW_MS < TIMEOUT_MS) {
        return INT64_MAX;
    }
    return NOW_MS + TIMEOUT_MS;
}

auto remaining_ms_until(int64_t deadline_ms, int fallback_timeout_ms) -> int {
    if (deadline_ms < 0) {
        return fallback_timeout_ms;
    }
    int64_t const NOW_MS = monotonic_now_ms();
    if (NOW_MS < 0) {
        return fallback_timeout_ms;
    }
    if (deadline_ms <= NOW_MS) {
        errno = ETIMEDOUT;
        return 0;
    }
    int64_t const REMAINING_MS = deadline_ms - NOW_MS;
    return REMAINING_MS > INT_MAX ? INT_MAX : static_cast<int>(REMAINING_MS);
}

auto wait_fd_ready_until(int fd, short events, int64_t deadline_ms, int fallback_timeout_ms) -> int {
    for (;;) {
        int const TIMEOUT_MS = remaining_ms_until(deadline_ms, fallback_timeout_ms);
        if (TIMEOUT_MS <= 0) {
            return 0;
        }

        pollfd pfd{
            .fd = fd,
            .events = events,
            .revents = 0,
        };
        int const READY = poll(&pfd, 1, TIMEOUT_MS);
        if (READY < 0 && errno == EINTR) {
            continue;
        }
        if (READY == 0) {
            errno = ETIMEDOUT;
        }
        return READY;
    }
}

auto set_nonblocking_for_timeout(int fd, int& old_flags) -> bool {
    old_flags = fcntl(fd, F_GETFL, 0);
    if (old_flags < 0) {
        return false;
    }
    if ((old_flags & O_NONBLOCK) != 0) {
        return true;
    }
    return fcntl(fd, F_SETFL, old_flags | O_NONBLOCK) == 0;
}

void restore_fd_flags(int fd, int old_flags) {
    if (old_flags >= 0) {
        (void)fcntl(fd, F_SETFL, old_flags);
    }
}

auto io_error_from_result(ssize_t result) -> int {
    if (result < -1) {
        return static_cast<int>(-result);
    }
    return errno;
}

auto retryable_io_error(int err) -> bool { return err == EAGAIN || err == EWOULDBLOCK || err == EINTR; }

auto retryable_io_result(ssize_t result) -> bool { return result < 0 && retryable_io_error(io_error_from_result(result)); }

auto write_all_timeout(int fd, std::string_view data, int timeout_ms) -> bool {
    int old_flags = -1;
    if (!set_nonblocking_for_timeout(fd, old_flags)) {
        return false;
    }

    size_t done = 0;
    int64_t const DEADLINE_MS = deadline_after_ms(timeout_ms);
    while (done < data.size()) {
        if (wait_fd_ready_until(fd, POLLOUT, DEADLINE_MS, timeout_ms) <= 0) {
            restore_fd_flags(fd, old_flags);
            return false;
        }

        errno = 0;
        ssize_t const BYTES_WRITTEN = write(fd, data.data() + done, data.size() - done);
        if (BYTES_WRITTEN < 0) {
            if (retryable_io_result(BYTES_WRITTEN)) {
                continue;
            }
            restore_fd_flags(fd, old_flags);
            return false;
        }
        if (BYTES_WRITTEN == 0) {
            errno = ETIMEDOUT;
            restore_fd_flags(fd, old_flags);
            return false;
        }
        done += static_cast<size_t>(BYTES_WRITTEN);
    }

    restore_fd_flags(fd, old_flags);
    return true;
}

auto write_all(int fd, std::string_view data) -> bool { return write_all_timeout(fd, data, DEBUGSERVER_PACKET_IO_TIMEOUT_MS); }

auto send_packet(Session& session, std::string_view payload) -> bool {
    std::string framed;
    framed.reserve(payload.size() + 4);
    framed.push_back('$');
    framed.append(payload);
    framed.push_back('#');
    uint8_t const SUM = checksum(payload);
    framed.push_back(hex_digit(SUM >> 4U));
    framed.push_back(hex_digit(SUM));
    return write_all(session.fd, framed);
}

auto read_exact_timeout(int fd, void* data, size_t len, int64_t deadline_ms, int timeout_ms) -> bool {
    auto* out = static_cast<char*>(data);
    size_t done = 0;
    while (done < len) {
        if (wait_fd_ready_until(fd, POLLIN, deadline_ms, timeout_ms) <= 0) {
            return false;
        }

        errno = 0;
        ssize_t const BYTES_READ = read(fd, out + done, len - done);
        if (BYTES_READ < 0) {
            if (retryable_io_result(BYTES_READ)) {
                continue;
            }
            return false;
        }
        if (BYTES_READ == 0) {
            errno = ECONNRESET;
            return false;
        }
        done += static_cast<size_t>(BYTES_READ);
    }
    return true;
}

auto read_byte_timeout(int fd, char& c, int64_t deadline_ms, int timeout_ms) -> bool {
    return read_exact_timeout(fd, &c, sizeof(c), deadline_ms, timeout_ms);
}

auto recv_packet(Session& session, std::string& out) -> bool {
    out.clear();
    session.received_interrupt = false;
    char c = 0;
    int64_t const DEADLINE_MS = deadline_after_ms(DEBUGSERVER_PACKET_IO_TIMEOUT_MS);
    while (true) {
        if (!read_byte_timeout(session.fd, c, DEADLINE_MS, DEBUGSERVER_PACKET_IO_TIMEOUT_MS)) {
            return false;
        }
        if (c == '$' || c == 0x03) {
            break;
        }
    }

    if (c == 0x03) {
        (void)ker::process::ptrace(static_cast<uint64_t>(ker::abi::ptrace::request::INTERRUPT), session.pid, 0, 0);
        session.received_interrupt = true;
        out = "?";
        return true;
    }

    uint8_t sum = 0;
    while (true) {
        if (!read_byte_timeout(session.fd, c, DEADLINE_MS, DEBUGSERVER_PACKET_IO_TIMEOUT_MS)) {
            return false;
        }
        if (c == '#') {
            break;
        }
        if (out.size() >= MAX_PACKET) {
            return false;
        }
        out.push_back(c);
        sum = static_cast<uint8_t>(sum + static_cast<uint8_t>(c));
    }

    std::array<char, 2> csum{};
    if (!read_exact_timeout(session.fd, csum.data(), csum.size(), DEADLINE_MS, DEBUGSERVER_PACKET_IO_TIMEOUT_MS)) {
        return false;
    }
    int const HI = from_hex(csum.at(0));
    int const LO = from_hex(csum.at(1));
    if (HI < 0 || LO < 0 || sum != static_cast<uint8_t>((HI << 4U) | LO)) {
        if (!session.no_ack) {
            (void)write_all(session.fd, "-");
        }
        return false;
    }
    if (!session.no_ack) {
        (void)write_all(session.fd, "+");
    }
    return true;
}

auto hex_bytes(const void* data, size_t len) -> std::string {
    const auto* bytes = static_cast<const uint8_t*>(data);
    std::string out;
    out.reserve(len * 2);
    for (size_t i = 0; i < len; ++i) {
        out.push_back(hex_digit(bytes[i] >> 4U));
        out.push_back(hex_digit(bytes[i]));
    }
    return out;
}

auto parse_hex_u64(std::string_view text, uint64_t& value) -> bool {
    value = 0;
    if (text.empty()) {
        return false;
    }
    for (const char C : text) {
        int const DECODED_HEX = from_hex(C);
        if (DECODED_HEX < 0) {
            return false;
        }
        value = (value << 4U) | static_cast<uint64_t>(DECODED_HEX);
    }
    return true;
}

auto parse_decimal_u64(std::string_view text, uint64_t max, uint64_t& value) -> bool {
    value = 0;
    if (text.empty()) {
        return false;
    }
    for (char const C : text) {
        if (C < '0' || C > '9') {
            return false;
        }
        auto const DIGIT = static_cast<uint64_t>(C - '0');
        if (value > (max - DIGIT) / 10U) {
            return false;
        }
        value = (value * 10U) + DIGIT;
    }
    return true;
}

auto ptrace_call(ker::abi::ptrace::request request, uint64_t pid, uint64_t addr, uint64_t data) -> int64_t {
    return ker::process::ptrace(static_cast<uint64_t>(request), pid, addr, data);
}

auto fetch_event_for_pid(uint64_t pid, ker::abi::ptrace::Event& event) -> bool {
    int64_t const GETEVENTMSG_RESULT = ptrace_call(ker::abi::ptrace::request::GETEVENTMSG, pid, 0, reinterpret_cast<uint64_t>(&event));
    return GETEVENTMSG_RESULT >= 0;
}

auto fetch_event(Session& session, ker::abi::ptrace::Event& event) -> bool { return fetch_event_for_pid(session.pid, event); }

auto stop_reason_name(ker::abi::ptrace::stop_reason reason) -> std::string_view {
    switch (reason) {
        case ker::abi::ptrace::stop_reason::BREAKPOINT:
            return "breakpoint";
        case ker::abi::ptrace::stop_reason::TRACE:
            return "trace";
        case ker::abi::ptrace::stop_reason::WATCHPOINT:
            return "watchpoint";
        case ker::abi::ptrace::stop_reason::EXEC:
            return "exec";
        case ker::abi::ptrace::stop_reason::FORK:
            return "fork";
        case ker::abi::ptrace::stop_reason::CLONE:
            return "clone";
        case ker::abi::ptrace::stop_reason::EXIT:
            return "exit";
        case ker::abi::ptrace::stop_reason::EXCEPTION:
            return "exception";
        case ker::abi::ptrace::stop_reason::INTERRUPT:
        case ker::abi::ptrace::stop_reason::SIGNAL:
        case ker::abi::ptrace::stop_reason::NONE:
        case ker::abi::ptrace::stop_reason::SYSCALL_ENTER:
        case ker::abi::ptrace::stop_reason::SYSCALL_EXIT:
            return "signal";
    }
    return "signal";
}

auto exception_address_field(const ker::abi::ptrace::Event& event) -> std::string {
    if (event.address == 0) {
        return {};
    }
    if (event.signal == SIGSYS) {
        return std::format("syscall:{:x};", event.address);
    }
    return std::format("fault:{:x};", event.address);
}

auto stop_reply_from_event(Session& session, const ker::abi::ptrace::Event& event) -> std::string {
    if (event.reason == ker::abi::ptrace::stop_reason::EXIT) {
        session.tracing_active = false;
        if (event.signal != 0) {
            return std::format("X{:02x}", event.signal);
        }
        return std::format("W{:02x}", static_cast<uint32_t>(event.message & 0xffU));
    }

    uint32_t const WOS_SIGNAL = event.signal != 0 ? event.signal : SIGTRAP;
    std::string reply = std::format("T{:02x}thread:{:x};reason:{};", WOS_SIGNAL, session.pid, stop_reason_name(event.reason));
    if (event.reason == ker::abi::ptrace::stop_reason::INTERRUPT) {
        reply += "description:interrupted;";
    }
    if (event.reason == ker::abi::ptrace::stop_reason::EXCEPTION) {
        reply += "description:fatal-exception;";
        reply += exception_address_field(event);
    }
    if (event.reason == ker::abi::ptrace::stop_reason::WATCHPOINT && event.address != 0) {
        reply += std::format("watch:{:x};description:watchpoint;", event.address);
    }
    return reply;
}

auto stop_reply(Session& session) -> std::string {
    ker::abi::ptrace::Event event{};
    if (!fetch_event(session, event)) {
        event.signal = SIGTRAP;
        event.reason = ker::abi::ptrace::stop_reason::SIGNAL;
        event.tid = session.pid;
    }
    return stop_reply_from_event(session, event);
}

auto read_gprs_for_pid(uint64_t pid, ker::abi::ptrace::X86_64GprState& gprs) -> bool {
    ker::abi::ptrace::RegsetIo io{
        .kind = ker::abi::ptrace::regset::X86_64_GPR,
        .buffer = &gprs,
        .size = sizeof(gprs),
    };
    int64_t const REGISTER_SET_RESULT = ptrace_call(ker::abi::ptrace::request::GETREGSET, pid, 0, reinterpret_cast<uint64_t>(&io));
    return REGISTER_SET_RESULT >= 0;
}

auto get_gprs(Session& session, ker::abi::ptrace::X86_64GprState& gprs) -> bool { return read_gprs_for_pid(session.pid, gprs); }

auto set_gprs(Session& session, ker::abi::ptrace::X86_64GprState& gprs) -> bool {
    ker::abi::ptrace::RegsetIo io{
        .kind = ker::abi::ptrace::regset::X86_64_GPR,
        .buffer = &gprs,
        .size = sizeof(gprs),
    };
    int64_t const REGISTER_SET_RESULT =
        ker::process::ptrace(static_cast<uint64_t>(ker::abi::ptrace::request::SETREGSET), session.pid, 0, reinterpret_cast<uint64_t>(&io));
    return REGISTER_SET_RESULT >= 0;
}

auto read_registers(Session& session) -> std::string {
    ker::abi::ptrace::X86_64GprState gprs{};
    if (!get_gprs(session, gprs)) {
        return "E01";
    }
    return hex_bytes(&gprs, sizeof(gprs));
}

auto decode_hex_bytes(std::string_view text, void* output, size_t size) -> bool {
    if (text.size() != size * 2) {
        return false;
    }
    auto* bytes = static_cast<uint8_t*>(output);
    for (size_t i = 0; i < size; ++i) {
        int const HI = from_hex(text[i * 2]);
        int const LO = from_hex(text[i * 2 + 1]);
        if (HI < 0 || LO < 0) {
            return false;
        }
        bytes[i] = static_cast<uint8_t>((HI << 4U) | LO);
    }
    return true;
}

auto write_registers(Session& session, std::string_view packet) -> std::string {
    ker::abi::ptrace::X86_64GprState gprs{};
    if (!decode_hex_bytes(packet.substr(1), &gprs, sizeof(gprs))) {
        return "E22";
    }
    return set_gprs(session, gprs) ? "OK" : "E01";
}

auto register_slot(ker::abi::ptrace::X86_64GprState& gprs, uint64_t regno) -> uint64_t* {
    auto* words = reinterpret_cast<uint64_t*>(&gprs);
    size_t constexpr WORD_COUNT = sizeof(ker::abi::ptrace::X86_64GprState) / sizeof(uint64_t);
    return regno < WORD_COUNT ? &words[regno] : nullptr;
}

auto read_one_register(Session& session, std::string_view packet) -> std::string {
    uint64_t regno = 0;
    if (!parse_hex_u64(packet.substr(1), regno)) {
        return "E22";
    }
    ker::abi::ptrace::X86_64GprState gprs{};
    if (!get_gprs(session, gprs)) {
        return "E01";
    }
    auto* slot = register_slot(gprs, regno);
    return slot == nullptr ? "E45" : hex_bytes(slot, sizeof(*slot));
}

auto write_one_register(Session& session, std::string_view packet) -> std::string {
    size_t const EQUALS = packet.find('=');
    if (EQUALS == std::string_view::npos) {
        return "E22";
    }
    uint64_t regno = 0;
    if (!parse_hex_u64(packet.substr(1, EQUALS - 1), regno)) {
        return "E22";
    }
    ker::abi::ptrace::X86_64GprState gprs{};
    if (!get_gprs(session, gprs)) {
        return "E01";
    }
    auto* slot = register_slot(gprs, regno);
    if (slot == nullptr || !decode_hex_bytes(packet.substr(EQUALS + 1), slot, sizeof(*slot))) {
        return "E22";
    }
    return set_gprs(session, gprs) ? "OK" : "E01";
}

auto read_memory_exact(Session& session, uint64_t addr, void* buffer, size_t size) -> bool {
    ker::abi::ptrace::MemIo io{
        .address = addr,
        .buffer = buffer,
        .size = size,
        .transferred = 0,
    };
    int64_t const READ_MEM_RESULT =
        ker::process::ptrace(static_cast<uint64_t>(ker::abi::ptrace::request::READ_MEM), session.pid, 0, reinterpret_cast<uint64_t>(&io));
    return READ_MEM_RESULT >= 0 && io.transferred == size;
}

auto write_memory_exact(Session& session, uint64_t addr, void* buffer, size_t size) -> bool {
    ker::abi::ptrace::MemIo io{
        .address = addr,
        .buffer = buffer,
        .size = size,
        .transferred = 0,
    };
    int64_t const WRITE_MEM_RESULT =
        ker::process::ptrace(static_cast<uint64_t>(ker::abi::ptrace::request::WRITE_MEM), session.pid, 0, reinterpret_cast<uint64_t>(&io));
    return WRITE_MEM_RESULT >= 0 && io.transferred == size;
}

auto read_memory_packet(Session& session, std::string_view packet) -> std::string {
    size_t const COMMA = packet.find(',');
    if (COMMA == std::string_view::npos) {
        return "E22";
    }
    uint64_t addr = 0;
    uint64_t len = 0;
    if (!parse_hex_u64(packet.substr(1, COMMA - 1), addr) || !parse_hex_u64(packet.substr(COMMA + 1), len) || len > MAX_MEM_READ) {
        return "E22";
    }
    std::vector<uint8_t> buffer(static_cast<size_t>(len));
    if (!read_memory_exact(session, addr, buffer.data(), buffer.size())) {
        return "E14";
    }
    return hex_bytes(buffer.data(), buffer.size());
}

auto write_memory_hex_packet(Session& session, std::string_view packet) -> std::string {
    size_t const COMMA = packet.find(',');
    size_t const COLON = packet.find(':');
    if (COMMA == std::string_view::npos || COLON == std::string_view::npos || COMMA > COLON) {
        return "E22";
    }
    uint64_t addr = 0;
    uint64_t len = 0;
    if (!parse_hex_u64(packet.substr(1, COMMA - 1), addr) || !parse_hex_u64(packet.substr(COMMA + 1, COLON - COMMA - 1), len)) {
        return "E22";
    }
    std::vector<uint8_t> buffer(static_cast<size_t>(len));
    if (!decode_hex_bytes(packet.substr(COLON + 1), buffer.data(), buffer.size())) {
        return "E22";
    }
    return write_memory_exact(session, addr, buffer.data(), buffer.size()) ? "OK" : "E14";
}

auto decode_binary_packet_data(std::string_view data, std::vector<uint8_t>& out) -> bool {
    out.clear();
    out.reserve(data.size());
    for (size_t i = 0; i < data.size(); ++i) {
        uint8_t byte = static_cast<uint8_t>(data[i]);
        if (byte == 0x7d) {
            if (++i >= data.size()) {
                return false;
            }
            byte = static_cast<uint8_t>(static_cast<uint8_t>(data[i]) ^ 0x20U);
        }
        out.push_back(byte);
    }
    return true;
}

auto write_memory_binary_packet(Session& session, std::string_view packet) -> std::string {
    size_t const COMMA = packet.find(',');
    size_t const COLON = packet.find(':');
    if (COMMA == std::string_view::npos || COLON == std::string_view::npos || COMMA > COLON) {
        return "E22";
    }
    uint64_t addr = 0;
    uint64_t len = 0;
    if (!parse_hex_u64(packet.substr(1, COMMA - 1), addr) || !parse_hex_u64(packet.substr(COMMA + 1, COLON - COMMA - 1), len)) {
        return "E22";
    }
    std::vector<uint8_t> buffer;
    if (!decode_binary_packet_data(packet.substr(COLON + 1), buffer) || buffer.size() != static_cast<size_t>(len)) {
        return "E22";
    }
    return write_memory_exact(session, addr, buffer.data(), buffer.size()) ? "OK" : "E14";
}

auto get_images(Session& session, std::vector<ker::abi::ptrace::ImageRecord>& images) -> bool {
    images.assign(8, ker::abi::ptrace::ImageRecord{});
    ker::abi::ptrace::ImageList list{
        .images = images.data(),
        .capacity = images.size(),
        .count = 0,
    };
    int64_t result = ker::process::ptrace(static_cast<uint64_t>(ker::abi::ptrace::request::GET_IMAGES), session.pid, 0,
                                          reinterpret_cast<uint64_t>(&list));
    if (result == -ENOSPC && list.count > images.size()) {
        images.assign(list.count, ker::abi::ptrace::ImageRecord{});
        list.images = images.data();
        list.capacity = images.size();
        result = ker::process::ptrace(static_cast<uint64_t>(ker::abi::ptrace::request::GET_IMAGES), session.pid, 0,
                                      reinterpret_cast<uint64_t>(&list));
    }
    if (result < 0) {
        images.clear();
        return false;
    }
    images.resize(std::min(list.count, images.size()));
    return true;
}

auto main_load_base(Session& session, uint64_t& load_base) -> bool {
    std::vector<ker::abi::ptrace::ImageRecord> images;
    if (!get_images(session, images) || images.empty()) {
        return false;
    }
    load_base = images.front().load_base;
    return true;
}

auto xml_escape(std::string_view text) -> std::string {
    std::string out;
    out.reserve(text.size());
    for (char c : text) {
        switch (c) {
            case '&':
                out += "&amp;";
                break;
            case '<':
                out += "&lt;";
                break;
            case '>':
                out += "&gt;";
                break;
            case '"':
                out += "&quot;";
                break;
            default:
                out.push_back(c);
                break;
        }
    }
    return out;
}

auto image_path(const ker::abi::ptrace::ImageRecord& image) -> std::string_view {
    size_t len = 0;
    while (len < ker::abi::ptrace::ImageRecord::PATH_LEN && image.path[len] != '\0') {
        ++len;
    }
    return {image.path, len};
}

auto libraries_svr4_xml(Session& session) -> std::string {
    std::vector<ker::abi::ptrace::ImageRecord> images;
    if (!get_images(session, images)) {
        return R"xml(<?xml version="1.0"?><library-list-svr4 version="1.0"/>)xml";
    }

    std::string out = R"xml(<?xml version="1.0"?><library-list-svr4 version="1.0">)xml";
    for (const auto& image : images) {
        std::string const PATH = xml_escape(image_path(image));
        out += std::format(R"xml(<library name="{}" lm="0x0" l_addr="0x{:x}" l_ld="0x0"/>)xml", PATH, image.load_base);
    }
    out += "</library-list-svr4>";
    return out;
}

auto read_qxfer(std::string_view packet, std::string_view object, std::string_view annex, std::string_view content) -> std::string {
    std::string const PREFIX = std::format("qXfer:{}:read:{}:", object, annex);
    if (!packet.starts_with(PREFIX)) {
        return "";
    }
    std::string_view const RANGE = packet.substr(PREFIX.size());
    size_t const COMMA = RANGE.find(',');
    if (COMMA == std::string_view::npos) {
        return "E22";
    }
    uint64_t offset = 0;
    uint64_t length = 0;
    if (!parse_hex_u64(RANGE.substr(0, COMMA), offset) || !parse_hex_u64(RANGE.substr(COMMA + 1), length)) {
        return "E22";
    }
    if (offset > content.size()) {
        return "E22";
    }
    size_t const START = static_cast<size_t>(offset);
    size_t const COUNT = std::min(static_cast<size_t>(length), content.size() - START);
    bool const LAST = START + COUNT >= content.size();
    std::string out;
    out.reserve(COUNT + 1);
    out.push_back(LAST ? 'l' : 'm');
    out.append(content.substr(START, COUNT));
    return out;
}

auto process_info(Session& session) -> std::string {
    return std::format(
        "pid:{:x};parent-pid:0;real-uid:0;effective-uid:0;cputype:1000007;cpusubtype:3;ptrsize:8;endian:little;"
        "ostype:wos;triple:x86_64-unknown-wos;",
        session.pid);
}

auto offsets(Session& session) -> std::string {
    uint64_t load_base = 0;
    if (!main_load_base(session, load_base)) {
        return "Text=0;Data=0;Bss=0";
    }
    return std::format("Text={:x};Data={:x};Bss={:x}", load_base, load_base, load_base);
}

auto thread_is_alive(Session& session, std::string_view packet) -> std::string {
    uint64_t tid = 0;
    if (!parse_hex_u64(packet.substr(1), tid)) {
        return "E22";
    }
    std::array<uint64_t, MAX_THREADS> tids{};
    ker::abi::ptrace::ThreadList list{
        .tids = tids.data(),
        .capacity = tids.size(),
        .count = 0,
    };
    int64_t const LIST_THREADS_RESULT = ker::process::ptrace(static_cast<uint64_t>(ker::abi::ptrace::request::LIST_THREADS), session.pid, 0,
                                                             reinterpret_cast<uint64_t>(&list));
    if (LIST_THREADS_RESULT < 0) {
        return "E01";
    }
    size_t const COUNT = list.count < tids.size() ? list.count : tids.size();
    for (size_t i = 0; i < COUNT; ++i) {
        if (tids.at(i) == tid) {
            return "OK";
        }
    }
    return "E44";
}

auto event_from_wait_status(Session& session, int status, ker::abi::ptrace::Event& event) -> bool {
    event = {};
    event.tid = session.pid;

    if (WIFEXITED(status)) {
        event.reason = ker::abi::ptrace::stop_reason::EXIT;
        event.signal = 0;
        event.message = static_cast<uint64_t>(WEXITSTATUS(status));
        session.tracing_active = false;
        return true;
    }
    if (WIFSIGNALED(status)) {
        event.reason = ker::abi::ptrace::stop_reason::EXIT;
        event.signal = static_cast<uint32_t>(WTERMSIG(status));
        event.message = event.signal;
        session.tracing_active = false;
        return true;
    }
    if (!WIFSTOPPED(status)) {
        return false;
    }
    if (fetch_event(session, event)) {
        return true;
    }
    {
        event.reason = ker::abi::ptrace::stop_reason::SIGNAL;
        event.signal = static_cast<uint32_t>(WSTOPSIG(status));
        event.tid = session.pid;
        return true;
    }
}

auto wait_for_debug_event(Session& session, ker::abi::ptrace::Event& event) -> bool {
    int status = 0;
    pid_t const WAITED = waitpid(static_cast<pid_t>(session.pid), &status, WUNTRACED);
    if (WAITED < 0) {
        return false;
    }
    if (event_from_wait_status(session, status, event)) {
        return true;
    }
    return false;
}

auto poll_debug_event(Session& session, ker::abi::ptrace::Event& event) -> bool {
    int status = 0;
    pid_t const WAITED = waitpid(static_cast<pid_t>(session.pid), &status, WUNTRACED | WNOHANG);
    if (WAITED <= 0) {
        return false;
    }
    if (event_from_wait_status(session, status, event)) {
        return true;
    }
    return false;
}

auto find_breakpoint(Session& session, uint64_t address) -> SoftwareBreakpoint* {
    for (auto& bp : session.software_breakpoints) {
        if (bp.address == address) {
            return &bp;
        }
    }
    return nullptr;
}

auto install_breakpoint(Session& session, SoftwareBreakpoint& bp) -> bool {
    if (bp.installed) {
        return true;
    }
    uint8_t byte = X86_INT3;
    if (!write_memory_exact(session, bp.address, &byte, sizeof(byte))) {
        return false;
    }
    bp.installed = true;
    return true;
}

auto uninstall_breakpoint(Session& session, SoftwareBreakpoint& bp) -> bool {
    if (!bp.installed) {
        return true;
    }
    uint8_t byte = bp.original_byte;
    if (!write_memory_exact(session, bp.address, &byte, sizeof(byte))) {
        return false;
    }
    bp.installed = false;
    return true;
}

auto parse_breakpoint_packet(std::string_view packet, uint64_t& address, uint64_t& kind) -> bool {
    size_t const FIRST_COMMA = packet.find(',');
    if (FIRST_COMMA == std::string_view::npos) {
        return false;
    }
    size_t const SECOND_COMMA = packet.find(',', FIRST_COMMA + 1);
    if (SECOND_COMMA == std::string_view::npos) {
        return false;
    }
    return parse_hex_u64(packet.substr(FIRST_COMMA + 1, SECOND_COMMA - FIRST_COMMA - 1), address) &&
           parse_hex_u64(packet.substr(SECOND_COMMA + 1), kind);
}

auto insert_software_breakpoint(Session& session, std::string_view packet) -> std::string {
    uint64_t address = 0;
    uint64_t kind = 0;
    if (!parse_breakpoint_packet(packet, address, kind) || kind == 0) {
        return "E22";
    }
    if (find_breakpoint(session, address) != nullptr) {
        return "OK";
    }
    uint8_t original = 0;
    if (!read_memory_exact(session, address, &original, sizeof(original))) {
        return "E14";
    }
    session.software_breakpoints.push_back(SoftwareBreakpoint{
        .address = address,
        .original_byte = original,
        .installed = false,
    });
    auto* bp = find_breakpoint(session, address);
    if (bp == nullptr || !install_breakpoint(session, *bp)) {
        return "E14";
    }
    return "OK";
}

auto remove_software_breakpoint(Session& session, std::string_view packet) -> std::string {
    uint64_t address = 0;
    uint64_t kind = 0;
    if (!parse_breakpoint_packet(packet, address, kind)) {
        return "E22";
    }
    for (auto it = session.software_breakpoints.begin(); it != session.software_breakpoints.end(); ++it) {
        if (it->address == address) {
            if (!uninstall_breakpoint(session, *it)) {
                return "E14";
            }
            session.software_breakpoints.erase(it);
            return "OK";
        }
    }
    return "OK";
}

auto hardware_type_for_packet(std::string_view packet, ker::abi::ptrace::hw_break_type& type) -> bool {
    if (packet.starts_with("Z1") || packet.starts_with("z1")) {
        type = ker::abi::ptrace::hw_break_type::EXECUTE;
        return true;
    }
    if (packet.starts_with("Z2") || packet.starts_with("z2")) {
        type = ker::abi::ptrace::hw_break_type::WRITE;
        return true;
    }
    if (packet.starts_with("Z3") || packet.starts_with("z3") || packet.starts_with("Z4") || packet.starts_with("z4")) {
        // x86 has execute, write, and read/write data breakpoints.  A pure
        // read watchpoint is represented as access watchpoint.
        type = ker::abi::ptrace::hw_break_type::READ_WRITE;
        return true;
    }
    return false;
}

auto find_hardware_breakpoint(Session& session, uint64_t address, uint64_t kind, ker::abi::ptrace::hw_break_type type)
    -> HardwareBreakpoint* {
    for (auto& bp : session.hardware_breakpoints) {
        if (bp.address == address && bp.kind == kind && bp.type == type) {
            return &bp;
        }
    }
    return nullptr;
}

auto hardware_slot_in_use(Session& session, uint32_t slot) -> bool {
    for (const auto& bp : session.hardware_breakpoints) {
        if (bp.slot == slot) {
            return true;
        }
    }
    return false;
}

auto allocate_hardware_slot(Session& session, uint32_t& slot) -> bool {
    for (uint32_t candidate = 0; candidate < 4; ++candidate) {
        if (!hardware_slot_in_use(session, candidate)) {
            slot = candidate;
            return true;
        }
    }
    return false;
}

auto apply_hardware_breakpoint(Session& session, const HardwareBreakpoint& bp, bool enable) -> bool {
    uint32_t const LENGTH = bp.type == ker::abi::ptrace::hw_break_type::EXECUTE ? 1U : static_cast<uint32_t>(bp.kind);
    ker::abi::ptrace::HwBreak desc{
        .address = bp.address,
        .length = LENGTH,
        .type = bp.type,
        .slot = bp.slot,
        .reserved = 0,
    };
    auto const REQUEST = enable ? ker::abi::ptrace::request::SET_HW_BREAK : ker::abi::ptrace::request::DEL_HW_BREAK;
    int64_t const RESULT = ker::process::ptrace(static_cast<uint64_t>(REQUEST), session.pid, 0, reinterpret_cast<uint64_t>(&desc));
    return RESULT >= 0;
}

auto insert_hardware_breakpoint(Session& session, std::string_view packet) -> std::string {
    ker::abi::ptrace::hw_break_type type = ker::abi::ptrace::hw_break_type::EXECUTE;
    uint64_t address = 0;
    uint64_t kind = 0;
    if (!hardware_type_for_packet(packet, type) || !parse_breakpoint_packet(packet, address, kind) || kind == 0) {
        return "E22";
    }
    if (find_hardware_breakpoint(session, address, kind, type) != nullptr) {
        return "OK";
    }
    uint32_t slot = 0;
    if (!allocate_hardware_slot(session, slot)) {
        return "E28";
    }
    HardwareBreakpoint bp{
        .address = address,
        .kind = kind,
        .type = type,
        .slot = slot,
    };
    if (!apply_hardware_breakpoint(session, bp, true)) {
        return "E22";
    }
    session.hardware_breakpoints.push_back(bp);
    return "OK";
}

auto remove_hardware_breakpoint(Session& session, std::string_view packet) -> std::string {
    ker::abi::ptrace::hw_break_type type = ker::abi::ptrace::hw_break_type::EXECUTE;
    uint64_t address = 0;
    uint64_t kind = 0;
    if (!hardware_type_for_packet(packet, type) || !parse_breakpoint_packet(packet, address, kind)) {
        return "E22";
    }
    for (auto it = session.hardware_breakpoints.begin(); it != session.hardware_breakpoints.end(); ++it) {
        if (it->address == address && it->kind == kind && it->type == type) {
            if (!apply_hardware_breakpoint(session, *it, false)) {
                return "E14";
            }
            session.hardware_breakpoints.erase(it);
            return "OK";
        }
    }
    return "OK";
}

auto breakpoint_at_pc(Session& session, ker::abi::ptrace::X86_64GprState& gprs) -> SoftwareBreakpoint* {
    auto* bp = find_breakpoint(session, gprs.rip);
    if (bp != nullptr && bp->installed) {
        return bp;
    }
    return nullptr;
}

auto single_step_one_instruction(Session& session, ker::abi::ptrace::Event& event) -> bool {
    int64_t const STEP_RESULT = ker::process::ptrace(static_cast<uint64_t>(ker::abi::ptrace::request::SINGLESTEP), session.pid, 0, 0);
    return STEP_RESULT >= 0 && wait_for_debug_event(session, event);
}

auto stop_for_internal_update(Session& session, ker::abi::ptrace::Event& event) -> bool {
    int64_t const INTERRUPT_RESULT = ker::process::ptrace(static_cast<uint64_t>(ker::abi::ptrace::request::INTERRUPT), session.pid, 0, 0);
    return INTERRUPT_RESULT >= 0 && wait_for_debug_event(session, event);
}

auto continue_tracee(Session& session) -> bool {
    int64_t const CONT_RESULT = ker::process::ptrace(static_cast<uint64_t>(ker::abi::ptrace::request::CONT), session.pid, 0, 0);
    return CONT_RESULT >= 0;
}

auto continue_event_loop(Session& session) -> std::string;

auto resume_with_breakpoint_step_over(Session& session, bool user_requested_step) -> std::string {
    ker::abi::ptrace::X86_64GprState gprs{};
    if (!get_gprs(session, gprs)) {
        return "E01";
    }

    auto* bp = breakpoint_at_pc(session, gprs);
    if (bp != nullptr) {
        if (!uninstall_breakpoint(session, *bp)) {
            return "E14";
        }

        ker::abi::ptrace::Event step_event{};
        if (!single_step_one_instruction(session, step_event)) {
            (void)install_breakpoint(session, *bp);
            return "E01";
        }
        if (!install_breakpoint(session, *bp)) {
            return "E14";
        }
        if (user_requested_step || step_event.reason != ker::abi::ptrace::stop_reason::TRACE) {
            return stop_reply_from_event(session, step_event);
        }
    }

    if (user_requested_step) {
        ker::abi::ptrace::Event step_event{};
        return single_step_one_instruction(session, step_event) ? stop_reply_from_event(session, step_event) : "E01";
    }

    return continue_tracee(session) ? continue_event_loop(session) : "E01";
}

auto handle_vcont(Session& session, std::string_view packet) -> std::string {
    if (packet == "vCont?") {
        return "vCont;c;s";
    }
    if (packet.starts_with("vCont;s")) {
        return resume_with_breakpoint_step_over(session, true);
    }
    if (packet.starts_with("vCont;c")) {
        return resume_with_breakpoint_step_over(session, false);
    }
    return "";
}

auto packet_needs_internal_stop(std::string_view packet) -> bool {
    return packet.starts_with("Z0") || packet.starts_with("z0") || packet.starts_with("Z1") || packet.starts_with("Z2") ||
           packet.starts_with("Z3") || packet.starts_with("Z4") || packet.starts_with("z1") || packet.starts_with("z2") ||
           packet.starts_with("z3") || packet.starts_with("z4") || packet.starts_with('M') || packet.starts_with('X') ||
           packet.starts_with('G') || packet.starts_with('P');
}

auto handle_packet(Session& session, std::string_view packet) -> std::string {
    if (packet == "?") {
        return stop_reply(session);
    }
    if (packet == "g") {
        return read_registers(session);
    }
    if (packet.starts_with('G')) {
        return write_registers(session, packet);
    }
    if (packet.starts_with('p')) {
        return read_one_register(session, packet);
    }
    if (packet.starts_with('P')) {
        return write_one_register(session, packet);
    }
    if (packet.starts_with('m')) {
        return read_memory_packet(session, packet);
    }
    if (packet.starts_with('M')) {
        return write_memory_hex_packet(session, packet);
    }
    if (packet.starts_with('X')) {
        return write_memory_binary_packet(session, packet);
    }
    if (packet.starts_with("vCont")) {
        return handle_vcont(session, packet);
    }
    if (packet.starts_with("qSupported")) {
        return "PacketSize=1000;QStartNoAckMode+;native-signals+;qXfer:features:read+;qXfer:libraries-svr4:read+;qThreadStopInfo+;"
               "vContSupported+";
    }
    if (packet == "QStartNoAckMode") {
        session.no_ack = true;
        return "OK";
    }
    if (packet == "qLaunchSuccess") {
        return "OK";
    }
    if (packet == "qHostInfo") {
        return "triple:x86_64-unknown-wos;endian:little;ptrsize:8;";
    }
    if (packet == "qAttached") {
        return session.attached ? "1" : "0";
    }
    if (packet == "qC") {
        return std::format("QC{:x}", session.pid);
    }
    if (packet == "qProcessInfo") {
        return process_info(session);
    }
    if (packet == "qOffsets") {
        return offsets(session);
    }
    if (packet.starts_with("qThreadStopInfo")) {
        return stop_reply(session);
    }
    if (packet.starts_with("qXfer:features:read:target.xml:")) {
        return read_qxfer(packet, "features", "target.xml", TARGET_XML);
    }
    if (packet.starts_with("qXfer:libraries-svr4:read::")) {
        return read_qxfer(packet, "libraries-svr4", "", libraries_svr4_xml(session));
    }
    if (packet == "qfThreadInfo") {
        std::array<uint64_t, MAX_THREADS> tids{};
        ker::abi::ptrace::ThreadList list{
            .tids = tids.data(),
            .capacity = tids.size(),
            .count = 0,
        };
        int64_t const LIST_THREADS_RESULT = ker::process::ptrace(static_cast<uint64_t>(ker::abi::ptrace::request::LIST_THREADS),
                                                                 session.pid, 0, reinterpret_cast<uint64_t>(&list));
        if (LIST_THREADS_RESULT < 0 || list.count == 0) {
            return "l";
        }
        std::string out = "m";
        size_t const COUNT = list.count < tids.size() ? list.count : tids.size();
        for (size_t i = 0; i < COUNT; ++i) {
            if (i != 0) {
                out.push_back(',');
            }
            out += std::format("{:x}", tids.at(i));
        }
        return out;
    }
    if (packet == "qsThreadInfo") {
        return "l";
    }
    if (packet == "c") {
        return resume_with_breakpoint_step_over(session, false);
    }
    if (packet == "s") {
        return resume_with_breakpoint_step_over(session, true);
    }
    if (packet.starts_with("Z0")) {
        return insert_software_breakpoint(session, packet);
    }
    if (packet.starts_with("z0")) {
        return remove_software_breakpoint(session, packet);
    }
    if (packet.starts_with("Z1") || packet.starts_with("Z2") || packet.starts_with("Z3") || packet.starts_with("Z4")) {
        return insert_hardware_breakpoint(session, packet);
    }
    if (packet.starts_with("z1") || packet.starts_with("z2") || packet.starts_with("z3") || packet.starts_with("z4")) {
        return remove_hardware_breakpoint(session, packet);
    }
    if (packet.starts_with('H')) {
        return "OK";
    }
    if (packet.starts_with('T')) {
        return thread_is_alive(session, packet);
    }
    if (packet == "k") {
        if (session.tracing_active) {
            (void)ptrace_call(ker::abi::ptrace::request::KILL, session.pid, 0, 0);
            session.tracing_active = false;
        }
        return "OK";
    }
    if (packet == "D") {
        if (session.tracing_active) {
            (void)ptrace_call(ker::abi::ptrace::request::DETACH, session.pid, 0, 0);
            session.tracing_active = false;
        }
        return "OK";
    }
    return "";
}

auto continue_event_loop(Session& session) -> std::string {
    for (;;) {
        ker::abi::ptrace::Event event{};
        if (poll_debug_event(session, event)) {
            return stop_reply_from_event(session, event);
        }

        pollfd pfd{
            .fd = session.fd,
            .events = POLLIN,
            .revents = 0,
        };
        int const POLL_RESULT = poll(&pfd, 1, DEBUGSERVER_EVENT_POLL_MS);
        if (POLL_RESULT < 0) {
            return "E0b";
        }
        if (POLL_RESULT == 0 || (pfd.revents & POLLIN) == 0) {
            continue;
        }

        std::string packet;
        if (!recv_packet(session, packet)) {
            return "E0c";
        }

        if (session.received_interrupt || packet == "?") {
            ker::abi::ptrace::Event interrupt_event{};
            if (!session.received_interrupt) {
                (void)ker::process::ptrace(static_cast<uint64_t>(ker::abi::ptrace::request::INTERRUPT), session.pid, 0, 0);
            }
            return wait_for_debug_event(session, interrupt_event) ? stop_reply_from_event(session, interrupt_event) : "E0a";
        }

        if (packet == "c" || packet.starts_with("vCont;c")) {
            continue;
        }

        if (packet == "s" || packet.starts_with("vCont;s")) {
            ker::abi::ptrace::Event step_base{};
            if (!stop_for_internal_update(session, step_base)) {
                return "E01";
            }
            if (step_base.reason != ker::abi::ptrace::stop_reason::INTERRUPT) {
                return stop_reply_from_event(session, step_base);
            }
            ker::abi::ptrace::Event step_event{};
            return single_step_one_instruction(session, step_event) ? stop_reply_from_event(session, step_event) : "E01";
        }

        if (packet_needs_internal_stop(packet)) {
            ker::abi::ptrace::Event update_base{};
            if (!stop_for_internal_update(session, update_base)) {
                if (!send_packet(session, "E01")) {
                    return "E0c";
                }
                continue;
            }

            std::string const REPLY = handle_packet(session, packet);
            if (!send_packet(session, REPLY)) {
                return "E0c";
            }

            if (update_base.reason != ker::abi::ptrace::stop_reason::INTERRUPT) {
                return stop_reply_from_event(session, update_base);
            }
            if (!continue_tracee(session)) {
                return "E01";
            }
            continue;
        }

        std::string const REPLY = handle_packet(session, packet);
        if (!send_packet(session, REPLY)) {
            return "E0c";
        }
    }
}

auto listen_socket(uint16_t port) -> int {
    int const FD = socket(AF_INET, SOCK_STREAM, 0);
    if (FD < 0) {
        return -1;
    }
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    if (bind(FD, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) < 0) {
        close(FD);
        return -1;
    }
    if (listen(FD, 1) < 0) {
        close(FD);
        return -1;
    }
    return FD;
}

auto is_execve_entry(const ker::abi::ptrace::X86_64GprState& regs) -> bool {
    return regs.rax == static_cast<uint64_t>(ker::abi::callnums::process) &&
           regs.rdi == static_cast<uint64_t>(ker::abi::process::procmgmt_ops::EXECVE);
}

auto wait_for_traced_stop(uint64_t pid, int options, int& status) -> bool {
    for (uint32_t attempt = 0; attempt < DEBUGSERVER_STARTUP_WAIT_RETRIES; attempt++) {
        status = 0;
        pid_t const WAITED = waitpid(static_cast<pid_t>(pid), &status, options | WNOHANG);
        if (WAITED == static_cast<pid_t>(pid)) {
            if (WIFEXITED(status)) {
                std::println(stderr, "debugserver: launched process exited with {}", WEXITSTATUS(status));
                return false;
            }
            if (WIFSIGNALED(status)) {
                std::println(stderr, "debugserver: launched process killed by signal {}", WTERMSIG(status));
                return false;
            }
            if (WIFSTOPPED(status)) {
                return true;
            }
            std::println(stderr, "debugserver: launched process reported unexpected wait status {:#x}", status);
            return false;
        }
        if (WAITED < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::perror("debugserver: waitpid");
            return false;
        }
        usleep(DEBUGSERVER_STARTUP_WAIT_POLL_US);
    }

    std::println(stderr, "debugserver: timed out waiting for launched process {} to stop", static_cast<unsigned long long>(pid));
    return false;
}

void terminate_launched_tracee_after_startup_failure(uint64_t pid) {
    (void)ptrace_call(ker::abi::ptrace::request::KILL, pid, 0, 0);
    for (uint32_t attempt = 0; attempt < DEBUGSERVER_STARTUP_WAIT_RETRIES; attempt++) {
        int status = 0;
        pid_t const WAITED = waitpid(static_cast<pid_t>(pid), &status, WNOHANG);
        if (WAITED == static_cast<pid_t>(pid) || (WAITED < 0 && errno != EINTR)) {
            return;
        }
        usleep(DEBUGSERVER_STARTUP_WAIT_POLL_US);
    }
}

auto launch_tracee(char** command_argv) -> int {
    if (ptrace_call(ker::abi::ptrace::request::TRACEME, 0, 0, 0) < 0) {
        std::perror("debugserver: PTRACE_TRACEME");
        return 1;
    }

    int64_t const TARGET_RC =
        ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL | ker::process::WKI_TARGET_FLAG_NOINHERIT);
    if (TARGET_RC < 0) {
        std::println(stderr, "debugserver: failed to pin tracee locally: {}", static_cast<long long>(TARGET_RC));
        return 1;
    }

    if (ker::process::kill(static_cast<int64_t>(ker::process::getpid()), SIGSTOP) != 0) {
        std::perror("debugserver: SIGSTOP");
        return 1;
    }

    execvp(command_argv[0], command_argv);
    std::perror("debugserver: execvp");
    return 127;
}

auto run_to_exec_start(uint64_t pid) -> bool {
    bool pending_execve = false;

    if (ptrace_call(ker::abi::ptrace::request::SYSCALL, pid, 0, 0) < 0) {
        std::println(stderr, "debugserver: PTRACE_SYSCALL failed");
        return false;
    }

    for (;;) {
        int status = 0;
        if (!wait_for_traced_stop(pid, 0, status)) {
            return false;
        }

        ker::abi::ptrace::Event event{};
        if (!fetch_event_for_pid(pid, event)) {
            std::println(stderr, "debugserver: failed to read launch event");
            return false;
        }

        if (event.reason == ker::abi::ptrace::stop_reason::SYSCALL_ENTER) {
            ker::abi::ptrace::X86_64GprState regs{};
            pending_execve = read_gprs_for_pid(pid, regs) && is_execve_entry(regs);
        } else if (event.reason == ker::abi::ptrace::stop_reason::SYSCALL_EXIT) {
            if (pending_execve) {
                ker::abi::ptrace::X86_64GprState regs{};
                if (!read_gprs_for_pid(pid, regs)) {
                    std::println(stderr, "debugserver: failed to read execve result");
                    return false;
                }
                pending_execve = false;
                if (regs.rax == 0) {
                    return true;
                }
            }
        } else {
            return true;
        }

        if (ptrace_call(ker::abi::ptrace::request::SYSCALL, pid, 0, 0) < 0) {
            std::println(stderr, "debugserver: launch resume failed");
            return false;
        }
    }
}

auto launch_process(char** command_argv, bool break_on_start, uint64_t& pid) -> bool {
    pid_t const CHILD = fork();
    if (CHILD < 0) {
        std::perror("debugserver: fork");
        return false;
    }
    if (CHILD == 0) {
        _exit(launch_tracee(command_argv));
    }

    pid = static_cast<uint64_t>(CHILD);

    int status = 0;
    if (!wait_for_traced_stop(pid, WUNTRACED, status)) {
        terminate_launched_tracee_after_startup_failure(pid);
        return false;
    }

    if (!break_on_start) {
        return true;
    }

    if (!run_to_exec_start(pid)) {
        terminate_launched_tracee_after_startup_failure(pid);
        return false;
    }
    return true;
}

auto parse_port(std::string_view text, uint16_t& port) -> bool {
    size_t const COLON = text.rfind(':');
    size_t const PORT_START = COLON == std::string_view::npos ? 0 : COLON + 1;
    std::string_view const PORT_TEXT{text.data() + PORT_START, text.size() - PORT_START};
    uint64_t parsed = 0;
    if (!parse_decimal_u64(PORT_TEXT, UINT16_MAX, parsed)) {
        return false;
    }
    port = static_cast<uint16_t>(parsed);
    return true;
}

auto parse_pid(std::string_view text, uint64_t& pid) -> bool { return parse_decimal_u64(text, UINT64_MAX, pid) && pid != 0; }

auto parse_args(int argc, char** argv, LaunchOptions& options) -> bool {
    for (int i = 1; i < argc; ++i) {
        const std::string_view ARG = argv[i];
        if (ARG == "--listen" && i + 1 < argc) {
            if (!parse_port(argv[++i], options.port)) {
                return false;
            }
        } else if (ARG == "--attach" && i + 1 < argc) {
            if (options.command_argv != nullptr || !parse_pid(argv[++i], options.attach_pid)) {
                return false;
            }
        } else if (ARG == "--break-on-start") {
            options.break_on_start = true;
        } else if (ARG == "--launch" && i + 1 < argc) {
            if (options.attach_pid != 0 || options.command_argv != nullptr) {
                return false;
            }
            options.command_argv = &argv[++i];
            return true;
        } else if (ARG == "--" && i + 1 < argc) {
            if (options.attach_pid != 0 || options.command_argv != nullptr) {
                return false;
            }
            options.command_argv = &argv[i + 1];
            return true;
        } else {
            return false;
        }
    }

    if ((options.attach_pid != 0) == (options.command_argv != nullptr)) {
        return false;
    }
    return !options.break_on_start || options.command_argv != nullptr;
}

void usage() {
    std::println("usage: debugserver --listen :PORT --attach PID");
    std::println("       debugserver --listen :PORT [--break-on-start] --launch PROGRAM [ARGS...]");
    std::println("       debugserver --listen :PORT [--break-on-start] -- PROGRAM [ARGS...]");
}

}  // namespace

auto main(int argc, char** argv) -> int {
    LaunchOptions options{};
    if (!parse_args(argc, argv, options)) {
        usage();
        return 1;
    }

    uint64_t pid = options.attach_pid;
    bool const ATTACHED = options.command_argv == nullptr;
    if (ATTACHED) {
        int64_t const ATTACH_RET = ptrace_call(ker::abi::ptrace::request::ATTACH, pid, 0, 0);
        if (ATTACH_RET < 0) {
            std::println("debugserver: attach failed: {}", static_cast<long long>(ATTACH_RET));
            return 1;
        }
    } else if (!launch_process(options.command_argv, options.break_on_start, pid)) {
        return 1;
    }

    int const SERVER = listen_socket(options.port);
    if (SERVER < 0) {
        std::println("debugserver: listen failed: errno={}", errno);
        if (ATTACHED) {
            (void)ptrace_call(ker::abi::ptrace::request::DETACH, pid, 0, 0);
        } else {
            (void)ptrace_call(ker::abi::ptrace::request::KILL, pid, 0, 0);
        }
        return 1;
    }
    if (ATTACHED) {
        std::println("debugserver: listening on :{} attached to pid {}", options.port, static_cast<unsigned long long>(pid));
    } else {
        std::println("debugserver: listening on :{} launched pid {} ({})", options.port, static_cast<unsigned long long>(pid),
                     options.command_argv[0]);
    }

    sockaddr_in peer{};
    socklen_t peer_len = sizeof(peer);
    int const CLIENT = accept(SERVER, reinterpret_cast<sockaddr*>(&peer), &peer_len);
    close(SERVER);
    if (CLIENT < 0) {
        return 1;
    }

    Session session{.fd = CLIENT,
                    .pid = pid,
                    .attached = ATTACHED,
                    .tracing_active = true,
                    .no_ack = false,
                    .received_interrupt = false,
                    .software_breakpoints = {},
                    .hardware_breakpoints = {}};
    std::string packet;
    while (recv_packet(session, packet)) {
        const std::string REPLY = handle_packet(session, packet);
        if (!send_packet(session, REPLY)) {
            break;
        }
    }
    close(CLIENT);
    if (session.tracing_active) {
        for (auto& bp : session.hardware_breakpoints) {
            (void)apply_hardware_breakpoint(session, bp, false);
        }
        for (auto& bp : session.software_breakpoints) {
            (void)uninstall_breakpoint(session, bp);
        }
        (void)ptrace_call(session.attached ? ker::abi::ptrace::request::DETACH : ker::abi::ptrace::request::KILL, pid, 0, 0);
    }
    return 0;
}
