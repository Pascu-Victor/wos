#include <abi-bits/pid_t.h>
#include <abi-bits/signal.h>
#include <abi-bits/wait.h>
#include <abi/callnums/futex.h>
#include <abi/callnums/multiproc.h>
#include <abi/callnums/net.h>
#include <abi/callnums/power.h>
#include <abi/callnums/process.h>
#include <abi/callnums/shm.h>
#include <abi/callnums/sys_log.h>
#include <abi/callnums/time.h>
#include <abi/callnums/vfs.h>
#include <abi/callnums/vmem.h>
#include <sys/callnums.h>
#include <sys/process.h>
#include <sys/wait.h>
#include <time.h>  // NOLINT(modernize-deprecated-headers): POSIX clock_gettime/localtime declarations.
#include <unistd.h>

#include <abi/ptrace.hpp>
#include <array>
#include <cerrno>
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
#include <unordered_map>
#include <vector>

namespace {

constexpr uint64_t TRACE_SYSGOOD_OPTION = 0x00000001ULL;
constexpr size_t MAX_STRING_LEN = 256;
constexpr uint32_t STRACE_STARTUP_WAIT_RETRIES = 500;
constexpr useconds_t STRACE_STARTUP_WAIT_POLL_US = 10 * 1000;

enum class TimestampMode : uint8_t {
    NONE,
    TIME_SECONDS,
    TIME_MICROS,
    UNIX_MICROS,
    DATE_MICROS,
};

struct TraceOptions {
    TimestampMode timestamp = TimestampMode::NONE;
    bool follow_forks = false;
    bool output_separately = false;
    bool append_output = false;
    std::string output_path;
};

auto strace_path_arg() -> char* {
    static std::array<char, sizeof("/usr/bin/strace")> arg{"/usr/bin/strace"};
    return arg.data();
}

auto remote_attach_flag_arg() -> char* {
    static std::array<char, sizeof("--wos-remote-attach")> arg{"--wos-remote-attach"};
    return arg.data();
}

auto remote_command_flag_arg() -> char* {
    static std::array<char, sizeof("--wos-remote-command")> arg{"--wos-remote-command"};
    return arg.data();
}

auto follow_forks_arg() -> char* {
    static std::array<char, sizeof("-f")> arg{"-f"};
    return arg.data();
}

auto output_separately_arg() -> char* {
    static std::array<char, sizeof("--output-separately")> arg{"--output-separately"};
    return arg.data();
}

auto output_arg() -> char* {
    static std::array<char, sizeof("-o")> arg{"-o"};
    return arg.data();
}

auto timestamp_option_arg(TimestampMode mode) -> char* {
    static std::array<char, sizeof("-t")> t_arg{"-t"};
    static std::array<char, sizeof("-tt")> tt_arg{"-tt"};
    static std::array<char, sizeof("-ttt")> ttt_arg{"-ttt"};
    static std::array<char, sizeof("-tttt")> tttt_arg{"-tttt"};

    switch (mode) {
        case TimestampMode::NONE:
            return nullptr;
        case TimestampMode::TIME_SECONDS:
            return t_arg.data();
        case TimestampMode::TIME_MICROS:
            return tt_arg.data();
        case TimestampMode::UNIX_MICROS:
            return ttt_arg.data();
        case TimestampMode::DATE_MICROS:
            return tttt_arg.data();
    }
    return nullptr;
}

auto parse_timestamp_option(std::string_view arg, TraceOptions& options) -> bool {
    if (arg == "-t") {
        options.timestamp = TimestampMode::TIME_SECONDS;
        return true;
    }
    if (arg == "-tt") {
        options.timestamp = TimestampMode::TIME_MICROS;
        return true;
    }
    if (arg == "-ttt") {
        options.timestamp = TimestampMode::UNIX_MICROS;
        return true;
    }
    if (arg == "-tttt") {
        options.timestamp = TimestampMode::DATE_MICROS;
        return true;
    }
    return false;
}

void append_timestamp_option(std::vector<char*>& helper_argv, const TraceOptions& options) {
    if (char* arg = timestamp_option_arg(options.timestamp); arg != nullptr) {
        helper_argv.push_back(arg);
    }
}

void append_trace_options(std::vector<char*>& helper_argv, const TraceOptions& options, char* output_path_arg) {
    append_timestamp_option(helper_argv, options);
    if (options.output_separately) {
        helper_argv.push_back(output_separately_arg());
    } else if (options.follow_forks) {
        helper_argv.push_back(follow_forks_arg());
    }
    if (!options.output_path.empty()) {
        helper_argv.push_back(output_arg());
        helper_argv.push_back(output_path_arg);
    }
}

struct PendingSyscall {
    bool valid = false;
    uint64_t callnum = 0;
    uint64_t a1 = 0;
    uint64_t a2 = 0;
    uint64_t a3 = 0;
    uint64_t a4 = 0;
    uint64_t a5 = 0;
    uint64_t a6 = 0;
    timespec entered_at{};
};

void usage() {
    std::println(stderr, "usage: strace [-f|-ff|--output-separately] [-o file] [-t|-tt|-ttt|-tttt] [-p pid] command [args...]");
}

auto command_basename(const char* path) -> std::string_view {
    if (path == nullptr) {
        return {};
    }
    const char* slash = std::strrchr(path, '/');
    return slash == nullptr ? std::string_view(path) : std::string_view(slash + 1);
}

auto parse_pid_arg(const char* text, uint64_t& pid) -> bool {
    if (text == nullptr || text[0] == '\0') {
        return false;
    }
    char* end = nullptr;
    unsigned long long const PARSED = std::strtoull(text, &end, 10);
    if (end == nullptr || *end != '\0') {
        return false;
    }
    pid = PARSED;
    return true;
}

auto parse_trace_option(int argc, char** argv, int& arg_index, TraceOptions& options, bool& error) -> bool {
    std::string_view const ARG = argv[arg_index];
    if (parse_timestamp_option(ARG, options)) {
        return true;
    }
    if (ARG == "-f") {
        options.follow_forks = true;
        return true;
    }
    if (ARG == "-ff" || ARG == "--output-separately") {
        options.follow_forks = true;
        options.output_separately = true;
        return true;
    }
    if (ARG == "-o") {
        if (arg_index + 1 >= argc) {
            usage();
            error = true;
            return true;
        }
        options.output_path = argv[++arg_index];
        return true;
    }
    if (ARG.starts_with("-o") && ARG.size() > 2) {
        options.output_path = std::string(ARG.substr(2));
        return true;
    }
    if (ARG.starts_with("--output=")) {
        options.output_path = std::string(ARG.substr(sizeof("--output=") - 1));
        return true;
    }
    return false;
}

auto timestamp_enabled(const TraceOptions& options) -> bool { return options.timestamp != TimestampMode::NONE; }

auto current_realtime(const TraceOptions& options) -> timespec {
    timespec now{};
    if (timestamp_enabled(options)) {
        (void)clock_gettime(CLOCK_REALTIME, &now);
    }
    return now;
}

auto format_clock_timestamp(const timespec& ts, bool include_date, bool include_micros) -> std::string {
    auto const SECONDS = static_cast<time_t>(ts.tv_sec);
    tm* local = localtime(&SECONDS);
    if (local == nullptr) {
        return include_micros ? std::format("{}.{:06}", static_cast<long long>(ts.tv_sec), ts.tv_nsec / 1000L)
                              : std::format("{}", static_cast<long long>(ts.tv_sec));
    }

    if (include_date) {
        if (include_micros) {
            return std::format("{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:06}", local->tm_year + 1900, local->tm_mon + 1, local->tm_mday,
                               local->tm_hour, local->tm_min, local->tm_sec, ts.tv_nsec / 1000L);
        }
        return std::format("{:04}-{:02}-{:02} {:02}:{:02}:{:02}", local->tm_year + 1900, local->tm_mon + 1, local->tm_mday, local->tm_hour,
                           local->tm_min, local->tm_sec);
    }

    if (include_micros) {
        return std::format("{:02}:{:02}:{:02}.{:06}", local->tm_hour, local->tm_min, local->tm_sec, ts.tv_nsec / 1000L);
    }
    return std::format("{:02}:{:02}:{:02}", local->tm_hour, local->tm_min, local->tm_sec);
}

auto format_timestamp_prefix(const TraceOptions& options, const timespec& timestamp) -> std::string {
    switch (options.timestamp) {
        case TimestampMode::NONE:
            return {};
        case TimestampMode::TIME_SECONDS:
            return std::format("{} ", format_clock_timestamp(timestamp, false, false));
        case TimestampMode::TIME_MICROS:
            return std::format("{} ", format_clock_timestamp(timestamp, false, true));
        case TimestampMode::UNIX_MICROS:
            return std::format("{}.{:06} ", static_cast<long long>(timestamp.tv_sec), timestamp.tv_nsec / 1000L);
        case TimestampMode::DATE_MICROS:
            return std::format("{} ", format_clock_timestamp(timestamp, true, true));
    }
    return {};
}

struct TraceOutput {
    FILE* stream = stdout;
    bool close_stream = false;
    bool valid = true;
};

auto output_file_path(const TraceOptions& options, uint64_t pid) -> std::string {
    if (options.output_path.empty()) {
        return {};
    }
    if (options.output_separately) {
        return std::format("{}.{}", options.output_path, pid);
    }
    return options.output_path;
}

auto open_trace_output(uint64_t pid, const TraceOptions& options) -> TraceOutput {
    std::string const PATH = output_file_path(options, pid);
    if (PATH.empty()) {
        return {};
    }

    if (!options.output_separately && !options.append_output) {
        FILE* truncate = std::fopen(PATH.c_str(), "w");
        if (truncate == nullptr) {
            std::println(stderr, "strace: failed to open '{}': {}", PATH, std::strerror(errno));
            return TraceOutput{.stream = nullptr, .close_stream = false, .valid = false};
        }
        std::fclose(truncate);
    }

    char const* mode = options.output_separately ? "w" : "a";
    FILE* stream = std::fopen(PATH.c_str(), mode);
    if (stream == nullptr) {
        std::println(stderr, "strace: failed to open '{}': {}", PATH, std::strerror(errno));
        return TraceOutput{.stream = nullptr, .close_stream = false, .valid = false};
    }
    setvbuf(stream, nullptr, _IOLBF, 0);
    return TraceOutput{.stream = stream, .close_stream = true, .valid = true};
}

void close_trace_output(TraceOutput& output) {
    if (output.close_stream && output.stream != nullptr) {
        std::fclose(output.stream);
    }
    output.stream = stdout;
    output.close_stream = false;
}

auto should_prefix_pid(const TraceOptions& options) -> bool {
    return options.follow_forks && (options.output_path.empty() || !options.output_separately);
}

void emit_trace_line(const TraceOptions& options, TraceOutput& output, uint64_t pid, const timespec& timestamp, std::string_view line) {
    std::string prefix = format_timestamp_prefix(options, timestamp);
    if (should_prefix_pid(options)) {
        prefix += std::format("[pid {}] ", pid);
    }
    std::println(output.stream, "{}{}", prefix, line);
}

auto ptrace_call(ker::abi::ptrace::request request, uint64_t pid, uint64_t addr, uint64_t data) -> int64_t {
    return ker::process::ptrace(static_cast<uint64_t>(request), pid, addr, data);
}

auto read_mem_partial(uint64_t pid, uint64_t addr, void* buffer, size_t size) -> size_t {
    ker::abi::ptrace::MemIo io{
        .address = addr,
        .buffer = buffer,
        .size = size,
        .transferred = 0,
    };
    (void)ptrace_call(ker::abi::ptrace::request::READ_MEM, pid, 0, reinterpret_cast<uint64_t>(&io));
    return io.transferred <= size ? io.transferred : size;
}

auto syscall_wait(uint64_t pid, ker::abi::ptrace::StopInfo& info) -> bool {
    std::memset(&info, 0, sizeof(info));
    return ptrace_call(ker::abi::ptrace::request::SYSCALL_WAIT, pid, 0, reinterpret_cast<uint64_t>(&info)) >= 0;
}

auto read_remote_info(uint64_t pid, ker::abi::ptrace::RemoteInfo& info) -> bool {
    std::memset(&info, 0, sizeof(info));
    return ptrace_call(ker::abi::ptrace::request::GET_REMOTE_INFO, pid, 0, reinterpret_cast<uint64_t>(&info)) >= 0;
}

auto read_c_string(uint64_t pid, uint64_t addr) -> std::string {
    if (addr == 0) {
        return "NULL";
    }

    std::array<char, MAX_STRING_LEN> buffer{};
    size_t used = 0;
    while (used < buffer.size()) {
        size_t const GOT = read_mem_partial(pid, addr + used, buffer.data() + used, buffer.size() - used);
        if (GOT == 0) {
            break;
        }

        const auto* const NUL = static_cast<const char*>(std::memchr(buffer.data() + used, '\0', GOT));
        if (NUL != nullptr) {
            return {buffer.data(), static_cast<size_t>(NUL - buffer.data())};
        }
        used += GOT;
    }
    std::string out{buffer.data(), used};
    out += "...";
    return out;
}

auto quote_string(std::string_view text) -> std::string {
    std::string out;
    out.reserve(text.size() + 2);
    out.push_back('"');
    for (char ch : text) {
        switch (ch) {
            case '\\':
                out += "\\\\";
                break;
            case '"':
                out += "\\\"";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                out.push_back(ch);
                break;
        }
    }
    out.push_back('"');
    return out;
}

auto callnum_name(uint64_t callnum) -> std::string_view {
    switch (static_cast<ker::abi::callnums>(callnum)) {
        case ker::abi::callnums::sys_log:
            return "sys_log";
        case ker::abi::callnums::futex:
            return "futex";
        case ker::abi::callnums::threading:
            return "threading";
        case ker::abi::callnums::process:
            return "process";
        case ker::abi::callnums::time:
            return "time";
        case ker::abi::callnums::vfs:
            return "vfs";
        case ker::abi::callnums::net:
            return "net";
        case ker::abi::callnums::vmem:
            return "vmem";
        case ker::abi::callnums::vmem_map:
            return "vmem_map";
        case ker::abi::callnums::debug:
            return "debug";
        case ker::abi::callnums::shm:
            return "shm";
        case ker::abi::callnums::personality:
            return "personality";
        case ker::abi::callnums::power:
            return "power";
    }
    return "unknown";
}

auto subop_name(uint64_t callnum, uint64_t op) -> std::string_view {
    switch (static_cast<ker::abi::callnums>(callnum)) {
        case ker::abi::callnums::sys_log:
            switch (static_cast<ker::abi::sys_log::sys_log_ops>(op)) {
                case ker::abi::sys_log::sys_log_ops::LOG:
                    return "log";
                case ker::abi::sys_log::sys_log_ops::LOG_LINE:
                    return "log_line";
                case ker::abi::sys_log::sys_log_ops::LOG_EX:
                    return "log_ex";
                case ker::abi::sys_log::sys_log_ops::LOG_BLOCK_BEGIN:
                    return "log_block_begin";
                case ker::abi::sys_log::sys_log_ops::LOG_BLOCK_END:
                    return "log_block_end";
            }
            break;
        case ker::abi::callnums::futex:
            switch (static_cast<ker::abi::futex::futex_ops>(op)) {
                case ker::abi::futex::futex_ops::FUTEX_WAIT:
                    return "wait";
                case ker::abi::futex::futex_ops::FUTEX_WAKE:
                    return "wake";
            }
            break;
        case ker::abi::callnums::threading:
            if (op >= 0x100) {
                switch (static_cast<ker::abi::multiproc::threadControlOps>(op)) {
                    case ker::abi::multiproc::threadControlOps::SET_TCB:
                        return "set_tcb";
                    case ker::abi::multiproc::threadControlOps::YIELD:
                        return "yield";
                    case ker::abi::multiproc::threadControlOps::THREAD_CREATE:
                        return "thread_create";
                    case ker::abi::multiproc::threadControlOps::THREAD_EXIT:
                        return "thread_exit";
                    case ker::abi::multiproc::threadControlOps::SET_AFFINITY:
                        return "set_affinity";
                    case ker::abi::multiproc::threadControlOps::GET_AFFINITY:
                        return "get_affinity";
                    case ker::abi::multiproc::threadControlOps::CREATE_DOMAIN:
                        return "create_domain";
                    case ker::abi::multiproc::threadControlOps::SET_DOMAIN:
                        return "set_domain";
                    case ker::abi::multiproc::threadControlOps::QUERY_DOMAIN:
                        return "query_domain";
                }
            } else {
                switch (static_cast<ker::abi::multiproc::threadInfoOps>(op)) {
                    case ker::abi::multiproc::threadInfoOps::CURRENT_THREAD_ID:
                        return "current_thread_id";
                    case ker::abi::multiproc::threadInfoOps::NATIVE_THREAD_COUNT:
                        return "native_thread_count";
                    case ker::abi::multiproc::threadInfoOps::CURRENT_CPU:
                        return "current_cpu";
                }
            }
            break;
        case ker::abi::callnums::process:
            switch (static_cast<ker::abi::process::procmgmt_ops>(op)) {
                case ker::abi::process::procmgmt_ops::EXIT:
                    return "exit";
                case ker::abi::process::procmgmt_ops::EXEC:
                    return "exec";
                case ker::abi::process::procmgmt_ops::WAITPID:
                    return "waitpid";
                case ker::abi::process::procmgmt_ops::GETPID:
                    return "getpid";
                case ker::abi::process::procmgmt_ops::GETPPID:
                    return "getppid";
                case ker::abi::process::procmgmt_ops::FORK:
                    return "fork";
                case ker::abi::process::procmgmt_ops::SIGACTION:
                    return "sigaction";
                case ker::abi::process::procmgmt_ops::SIGPROCMASK:
                    return "sigprocmask";
                case ker::abi::process::procmgmt_ops::SIGSUSPEND:
                    return "sigsuspend";
                case ker::abi::process::procmgmt_ops::KILL:
                    return "kill";
                case ker::abi::process::procmgmt_ops::SIGRETURN:
                    return "sigreturn";
                case ker::abi::process::procmgmt_ops::GETUID:
                    return "getuid";
                case ker::abi::process::procmgmt_ops::GETEUID:
                    return "geteuid";
                case ker::abi::process::procmgmt_ops::GETGID:
                    return "getgid";
                case ker::abi::process::procmgmt_ops::GETEGID:
                    return "getegid";
                case ker::abi::process::procmgmt_ops::GETGROUPS:
                    return "getgroups";
                case ker::abi::process::procmgmt_ops::SETUID:
                    return "setuid";
                case ker::abi::process::procmgmt_ops::SETGID:
                    return "setgid";
                case ker::abi::process::procmgmt_ops::SETEUID:
                    return "seteuid";
                case ker::abi::process::procmgmt_ops::SETEGID:
                    return "setegid";
                case ker::abi::process::procmgmt_ops::SETGROUPS:
                    return "setgroups";
                case ker::abi::process::procmgmt_ops::GETUMASK:
                    return "getumask";
                case ker::abi::process::procmgmt_ops::SETUMASK:
                    return "setumask";
                case ker::abi::process::procmgmt_ops::SETSID:
                    return "setsid";
                case ker::abi::process::procmgmt_ops::GETSID:
                    return "getsid";
                case ker::abi::process::procmgmt_ops::SETPGID:
                    return "setpgid";
                case ker::abi::process::procmgmt_ops::GETPGID:
                    return "getpgid";
                case ker::abi::process::procmgmt_ops::EXECVE:
                    return "execve";
                case ker::abi::process::procmgmt_ops::GETHOSTNAME:
                    return "gethostname";
                case ker::abi::process::procmgmt_ops::SETHOSTNAME:
                    return "sethostname";
                case ker::abi::process::procmgmt_ops::SETPRIORITY:
                    return "setpriority";
                case ker::abi::process::procmgmt_ops::SETWKITARGET:
                    return "setwkitarget";
                case ker::abi::process::procmgmt_ops::GETWKITARGET:
                    return "getwkitarget";
                case ker::abi::process::procmgmt_ops::PTRACE:
                    return "ptrace";
                case ker::abi::process::procmgmt_ops::CLONE_VM_PROC:
                    return "clone_vm";
                case ker::abi::process::procmgmt_ops::PRCTL:
                    return "prctl";
                case ker::abi::process::procmgmt_ops::ARCH_PRCTL:
                    return "arch_prctl";
                case ker::abi::process::procmgmt_ops::SIGALTSTACK:
                    return "sigaltstack";
                case ker::abi::process::procmgmt_ops::UNAME:
                    return "uname";
            }
            break;
        case ker::abi::callnums::time:
            switch (static_cast<ker::abi::sys_time_ops>(op)) {
                case ker::abi::sys_time_ops::GETTIMEOFDAY:
                    return "gettimeofday";
                case ker::abi::sys_time_ops::CLOCK_GETTIME:
                    return "clock_gettime";
                case ker::abi::sys_time_ops::NANOSLEEP:
                    return "nanosleep";
                case ker::abi::sys_time_ops::TIMES:
                    return "times";
                case ker::abi::sys_time_ops::SETITIMER:
                    return "setitimer";
                case ker::abi::sys_time_ops::GETITIMER:
                    return "getitimer";
            }
            break;
        case ker::abi::callnums::vfs:
            switch (static_cast<ker::abi::vfs::ops>(op)) {
                case ker::abi::vfs::ops::OPEN:
                    return "open";
                case ker::abi::vfs::ops::READ:
                    return "read";
                case ker::abi::vfs::ops::WRITE:
                    return "write";
                case ker::abi::vfs::ops::CLOSE:
                    return "close";
                case ker::abi::vfs::ops::LSEEK:
                    return "lseek";
                case ker::abi::vfs::ops::ISATTY:
                    return "isatty";
                case ker::abi::vfs::ops::READ_DIR_ENTRIES:
                    return "read_dir_entries";
                case ker::abi::vfs::ops::MOUNT:
                    return "mount";
                case ker::abi::vfs::ops::MKDIR:
                    return "mkdir";
                case ker::abi::vfs::ops::READLINK:
                    return "readlink";
                case ker::abi::vfs::ops::SYMLINK:
                    return "symlink";
                case ker::abi::vfs::ops::SENDFILE:
                    return "sendfile";
                case ker::abi::vfs::ops::STAT:
                    return "stat";
                case ker::abi::vfs::ops::FSTAT:
                    return "fstat";
                case ker::abi::vfs::ops::UMOUNT:
                    return "umount";
                case ker::abi::vfs::ops::DUP:
                    return "dup";
                case ker::abi::vfs::ops::DUP2:
                    return "dup2";
                case ker::abi::vfs::ops::GETCWD:
                    return "getcwd";
                case ker::abi::vfs::ops::CHDIR:
                    return "chdir";
                case ker::abi::vfs::ops::ACCESS:
                    return "access";
                case ker::abi::vfs::ops::UNLINK:
                    return "unlink";
                case ker::abi::vfs::ops::RMDIR:
                    return "rmdir";
                case ker::abi::vfs::ops::RENAME:
                    return "rename";
                case ker::abi::vfs::ops::CHMOD:
                    return "chmod";
                case ker::abi::vfs::ops::TRUNCATE:
                    return "truncate";
                case ker::abi::vfs::ops::PIPE:
                    return "pipe";
                case ker::abi::vfs::ops::PREAD:
                    return "pread";
                case ker::abi::vfs::ops::PWRITE:
                    return "pwrite";
                case ker::abi::vfs::ops::FCNTL:
                    return "fcntl";
                case ker::abi::vfs::ops::FCHMOD:
                    return "fchmod";
                case ker::abi::vfs::ops::CHOWN:
                    return "chown";
                case ker::abi::vfs::ops::FCHOWN:
                    return "fchown";
                case ker::abi::vfs::ops::FACCESSAT:
                    return "faccessat";
                case ker::abi::vfs::ops::UNLINKAT:
                    return "unlinkat";
                case ker::abi::vfs::ops::RENAMEAT:
                    return "renameat";
                case ker::abi::vfs::ops::EPOLL_CREATE:
                    return "epoll_create";
                case ker::abi::vfs::ops::EPOLL_CTL:
                    return "epoll_ctl";
                case ker::abi::vfs::ops::EPOLL_PWAIT:
                    return "epoll_pwait";
                case ker::abi::vfs::ops::IOCTL:
                    return "ioctl";
                case ker::abi::vfs::ops::FSYNC:
                    return "fsync";
                case ker::abi::vfs::ops::LINK:
                    return "link";
                case ker::abi::vfs::ops::WKI_RULE_ADD:
                    return "wki_rule_add";
                case ker::abi::vfs::ops::WKI_RULE_GET:
                    return "wki_rule_get";
                case ker::abi::vfs::ops::WKI_RULE_CLEAR:
                    return "wki_rule_clear";
                case ker::abi::vfs::ops::PIVOT_ROOT:
                    return "pivot_root";
                case ker::abi::vfs::ops::WKI_RULE_GET_DEFAULT:
                    return "wki_rule_get_default";
                case ker::abi::vfs::ops::STATVFS:
                    return "statvfs";
                case ker::abi::vfs::ops::FSTATVFS:
                    return "fstatvfs";
                case ker::abi::vfs::ops::LSTAT:
                    return "lstat";
                case ker::abi::vfs::ops::SYNC:
                    return "sync";
            }
            break;
        case ker::abi::callnums::net:
            switch (static_cast<ker::abi::net::ops>(op)) {
                case ker::abi::net::ops::SOCKET:
                    return "socket";
                case ker::abi::net::ops::BIND:
                    return "bind";
                case ker::abi::net::ops::LISTEN:
                    return "listen";
                case ker::abi::net::ops::ACCEPT:
                    return "accept";
                case ker::abi::net::ops::CONNECT:
                    return "connect";
                case ker::abi::net::ops::SEND:
                    return "send";
                case ker::abi::net::ops::RECV:
                    return "recv";
                case ker::abi::net::ops::CLOSE:
                    return "close";
                case ker::abi::net::ops::SENDTO:
                    return "sendto";
                case ker::abi::net::ops::RECVFROM:
                    return "recvfrom";
                case ker::abi::net::ops::SETSOCKOPT:
                    return "setsockopt";
                case ker::abi::net::ops::GETSOCKOPT:
                    return "getsockopt";
                case ker::abi::net::ops::SHUTDOWN:
                    return "shutdown";
                case ker::abi::net::ops::GETPEERNAME:
                    return "getpeername";
                case ker::abi::net::ops::GETSOCKNAME:
                    return "getsockname";
                case ker::abi::net::ops::SELECT:
                    return "select";
                case ker::abi::net::ops::POLL:
                    return "poll";
                case ker::abi::net::ops::IOCTL_NET:
                    return "ioctl_net";
                case ker::abi::net::ops::SET_DEV_CPU_AFFINITY:
                    return "set_dev_cpu_affinity";
                case ker::abi::net::ops::NETCTL_IF_LIST:
                    return "netctl_if_list";
                case ker::abi::net::ops::NETCTL_ADDR_LIST:
                    return "netctl_addr_list";
                case ker::abi::net::ops::NETCTL_ADDR_SET:
                    return "netctl_addr_set";
                case ker::abi::net::ops::NETCTL_ADDR_DEL:
                    return "netctl_addr_del";
                case ker::abi::net::ops::NETCTL_LINK_SET:
                    return "netctl_link_set";
            }
            break;
        case ker::abi::callnums::vmem:
            switch (static_cast<ker::abi::vmem::ops>(op)) {
                case ker::abi::vmem::ops::ANON_ALLOCATE:
                    return "anon_allocate";
                case ker::abi::vmem::ops::ANON_FREE:
                    return "anon_free";
                case ker::abi::vmem::ops::PROTECT:
                    return "protect";
                case ker::abi::vmem::ops::MREMAP:
                    return "mremap";
                case ker::abi::vmem::ops::MSYNC:
                    return "msync";
            }
            break;
        case ker::abi::callnums::debug:
            return op == 0 ? "cli" : "sti";
        case ker::abi::callnums::shm:
            switch (static_cast<ker::abi::shm::ops>(op)) {
                case ker::abi::shm::ops::GET:
                    return "get";
                case ker::abi::shm::ops::ATTACH:
                    return "attach";
                case ker::abi::shm::ops::DETACH:
                    return "detach";
                case ker::abi::shm::ops::CTL:
                    return "ctl";
            }
            break;
        // NOLINTNEXTLINE(bugprone-branch-clone)
        case ker::abi::callnums::vmem_map:
            break;
        case ker::abi::callnums::personality:
            break;
        case ker::abi::callnums::power:
            switch (static_cast<ker::abi::power::ops>(op)) {
                case ker::abi::power::ops::REBOOT:
                    return "reboot";
                case ker::abi::power::ops::GET_STATE:
                    return "get_state";
                case ker::abi::power::ops::PREPARE:
                    return "prepare";
            }
            break;
    }
    return "op";
}

auto should_decode_string(uint64_t callnum, uint64_t op, int arg_index) -> bool {
    if (static_cast<ker::abi::callnums>(callnum) == ker::abi::callnums::vmem_map) {
        return false;
    }
    if (static_cast<ker::abi::callnums>(callnum) == ker::abi::callnums::process) {
        auto proc_op = static_cast<ker::abi::process::procmgmt_ops>(op);
        return (proc_op == ker::abi::process::procmgmt_ops::EXEC || proc_op == ker::abi::process::procmgmt_ops::EXECVE ||
                proc_op == ker::abi::process::procmgmt_ops::GETHOSTNAME || proc_op == ker::abi::process::procmgmt_ops::SETHOSTNAME ||
                proc_op == ker::abi::process::procmgmt_ops::SETWKITARGET || proc_op == ker::abi::process::procmgmt_ops::GETWKITARGET) &&
               arg_index == 0;
    }
    if (static_cast<ker::abi::callnums>(callnum) == ker::abi::callnums::vfs) {
        auto vfs_op = static_cast<ker::abi::vfs::ops>(op);
        switch (vfs_op) {
            case ker::abi::vfs::ops::OPEN:
            case ker::abi::vfs::ops::MOUNT:
            case ker::abi::vfs::ops::MKDIR:
            case ker::abi::vfs::ops::READLINK:
            case ker::abi::vfs::ops::SYMLINK:
            case ker::abi::vfs::ops::STAT:
            case ker::abi::vfs::ops::GETCWD:
            case ker::abi::vfs::ops::CHDIR:
            case ker::abi::vfs::ops::ACCESS:
            case ker::abi::vfs::ops::UNLINK:
            case ker::abi::vfs::ops::RMDIR:
            case ker::abi::vfs::ops::RENAME:
            case ker::abi::vfs::ops::CHMOD:
            case ker::abi::vfs::ops::TRUNCATE:
            case ker::abi::vfs::ops::LINK:
            case ker::abi::vfs::ops::PIVOT_ROOT:
            case ker::abi::vfs::ops::STATVFS:
                return arg_index == 0;
            case ker::abi::vfs::ops::FACCESSAT:
            case ker::abi::vfs::ops::UNLINKAT:
            case ker::abi::vfs::ops::RENAMEAT:
                return arg_index == 1;
            default:
                break;
        }
    }
    return false;
}  // namespace

auto format_arg(uint64_t pid, uint64_t callnum, uint64_t op, int arg_index, uint64_t value) -> std::string {
    if (should_decode_string(callnum, op, arg_index) && value != 0) {
        return quote_string(read_c_string(pid, value));
    }
    if (value == 0) {
        return "NULL";
    }
    return std::format("0x{:x}", value);
}

auto format_entry(uint64_t pid, const PendingSyscall& pending) -> std::string {
    if (static_cast<ker::abi::callnums>(pending.callnum) == ker::abi::callnums::vmem_map) {
        return std::format("{}(0x{:x}, 0x{:x}, 0x{:x}, 0x{:x}, 0x{:x}, 0x{:x})", callnum_name(pending.callnum), pending.a1, pending.a2,
                           pending.a3, pending.a4, pending.a5, pending.a6);
    }

    std::string const LABEL = std::format("{}.{}", callnum_name(pending.callnum), subop_name(pending.callnum, pending.a1));
    return std::format(
        "{}({}, {}, {}, {}, {})", LABEL, format_arg(pid, pending.callnum, pending.a1, 0, pending.a2),
        format_arg(pid, pending.callnum, pending.a1, 1, pending.a3), format_arg(pid, pending.callnum, pending.a1, 2, pending.a4),
        format_arg(pid, pending.callnum, pending.a1, 3, pending.a5), format_arg(pid, pending.callnum, pending.a1, 4, pending.a6));
}

auto format_result(int64_t result) -> std::string {
    if (result < 0 && result >= -4095) {
        int const ERR = static_cast<int>(-result);
        char const* message = std::strerror(ERR);
        return std::format("-{} ({})", ERR, message != nullptr ? message : "unknown");
    }
    return std::format("{}", result);
}

auto target_is_proxy(uint64_t pid) -> bool {
    ker::abi::ptrace::RemoteInfo info{};
    if (!read_remote_info(pid, info)) {
        return false;
    }
    return info.is_proxy != 0 && info.remote_pid != 0;
}

auto is_proxy_info(const ker::abi::ptrace::RemoteInfo& info) -> bool { return info.is_proxy != 0 && info.remote_pid != 0; }

auto exec_strace_with_command(char** command_argv, const TraceOptions& options) -> int {
    std::string output_path_arg = options.output_path;
    std::vector<char*> helper_argv;
    helper_argv.push_back(strace_path_arg());
    append_trace_options(helper_argv, options, output_path_arg.empty() ? nullptr : output_path_arg.data());
    helper_argv.push_back(remote_command_flag_arg());
    for (char** arg = command_argv; arg != nullptr && *arg != nullptr; ++arg) {
        helper_argv.push_back(*arg);
    }
    helper_argv.push_back(nullptr);

    execvp(strace_path_arg(), helper_argv.data());
    std::perror("strace: exec remote helper");
    return 127;
}

auto exec_strace_remote_attach(const ker::abi::ptrace::RemoteInfo& info, const TraceOptions& options) -> int {
    if (!is_proxy_info(info)) {
        std::println(stderr, "strace: remote attach requested for a non-proxy target");
        return 1;
    }
    if (info.target_hostname.at(0) == '\0') {
        std::println(stderr, "strace: pid {} is a WKI proxy for remote pid {}, but the runner hostname is unavailable", info.proxy_pid,
                     info.remote_pid);
        return 1;
    }

    int64_t const TARGET_RC = ker::process::setwkitarget(info.target_hostname.data(), std::strlen(info.target_hostname.data()),
                                                         ker::process::WKI_TARGET_FLAG_STRICT);
    if (TARGET_RC < 0) {
        std::println(stderr, "strace: failed to target runner '{}': {}", info.target_hostname.data(), format_result(TARGET_RC));
        return 1;
    }

    std::string remote_pid = std::format("{}", info.remote_pid);
    std::string output_path_arg = options.output_path;
    std::vector<char*> helper_argv;
    helper_argv.push_back(strace_path_arg());
    append_trace_options(helper_argv, options, output_path_arg.empty() ? nullptr : output_path_arg.data());
    helper_argv.push_back(remote_attach_flag_arg());
    helper_argv.push_back(remote_pid.data());
    helper_argv.push_back(nullptr);
    execvp(strace_path_arg(), helper_argv.data());
    std::perror("strace: exec remote attach helper");
    return 127;
}

auto wait_for_trace_startup_stop(pid_t pid, int& status) -> bool {
    for (uint32_t attempt = 0; attempt < STRACE_STARTUP_WAIT_RETRIES; attempt++) {
        status = 0;
        pid_t const WAITED = waitpid(pid, &status, WUNTRACED | WNOHANG);
        if (WAITED == pid) {
            if (WIFSTOPPED(status)) {
                return true;
            }
            if (WIFEXITED(status)) {
                std::println(stderr, "strace: pid {} exited with {} before tracing", static_cast<int>(pid), WEXITSTATUS(status));
            } else if (WIFSIGNALED(status)) {
                std::println(stderr, "strace: pid {} was killed by signal {} before tracing", static_cast<int>(pid), WTERMSIG(status));
            } else {
                std::println(stderr, "strace: pid {} reported unexpected startup wait status {:#x}", static_cast<int>(pid), status);
            }
            return false;
        }
        if (WAITED < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::perror("strace: waitpid");
            return false;
        }
        usleep(STRACE_STARTUP_WAIT_POLL_US);
    }

    std::println(stderr, "strace: timed out waiting for pid {} to stop before tracing", static_cast<int>(pid));
    return false;
}

void reap_tracee_after_startup_failure(pid_t pid) {
    (void)kill(pid, SIGKILL);
    for (uint32_t attempt = 0; attempt < STRACE_STARTUP_WAIT_RETRIES; attempt++) {
        int status = 0;
        pid_t const WAITED = waitpid(pid, &status, WNOHANG);
        if (WAITED == pid || (WAITED < 0 && errno != EINTR)) {
            return;
        }
        usleep(STRACE_STARTUP_WAIT_POLL_US);
    }
}

void detach_tracee_after_startup_failure(uint64_t pid) { (void)ptrace_call(ker::abi::ptrace::request::DETACH, pid, 0, 0); }

auto attach_and_trace(uint64_t pid, bool route_proxy, const TraceOptions& options) -> int;

struct TraceState {
    std::vector<pid_t> helper_pids;
    int helper_status = 0;
};

auto helper_exit_code(int status) -> int {
    if (WIFEXITED(status)) {
        return WEXITSTATUS(status);
    }
    if (WIFSIGNALED(status)) {
        return 128 + WTERMSIG(status);
    }
    return 1;
}

void reap_follow_helpers(TraceState& state, bool block) {
    for (size_t idx = 0; idx < state.helper_pids.size();) {
        int status = 0;
        pid_t const WAITED = waitpid(state.helper_pids.at(idx), &status, block ? 0 : WNOHANG);
        if (WAITED == state.helper_pids.at(idx)) {
            int const RC = helper_exit_code(status);
            if (RC != 0 && state.helper_status == 0) {
                state.helper_status = RC;
            }
            state.helper_pids.erase(state.helper_pids.begin() + static_cast<ptrdiff_t>(idx));
            continue;
        }
        if (WAITED < 0) {
            if (errno == EINTR) {
                continue;
            }
            if (state.helper_status == 0) {
                state.helper_status = 1;
            }
            state.helper_pids.erase(state.helper_pids.begin() + static_cast<ptrdiff_t>(idx));
            continue;
        }
        ++idx;
    }
}

auto fork_child_from_syscall_result(const PendingSyscall& pending, int64_t result, uint64_t& child_pid) -> bool {
    if (result <= 0 || static_cast<ker::abi::callnums>(pending.callnum) != ker::abi::callnums::process) {
        return false;
    }
    if (static_cast<ker::abi::process::procmgmt_ops>(pending.a1) != ker::abi::process::procmgmt_ops::FORK) {
        return false;
    }
    child_pid = static_cast<uint64_t>(result);
    return true;
}

void spawn_follow_helper(TraceState& state, TraceOutput& output, uint64_t child_pid, const TraceOptions& options) {
    if (child_pid == 0) {
        return;
    }
    if (output.stream != nullptr) {
        std::fflush(output.stream);
    }

    pid_t const HELPER = fork();
    if (HELPER < 0) {
        std::perror("strace: fork follow helper");
        if (state.helper_status == 0) {
            state.helper_status = 1;
        }
        return;
    }
    if (HELPER == 0) {
        TraceOptions child_options = options;
        child_options.append_output = !child_options.output_path.empty() && !child_options.output_separately;
        int const RC = attach_and_trace(child_pid, false, child_options);
        _exit(RC == 0 ? 0 : 1);
    }
    state.helper_pids.push_back(HELPER);
}

auto trace_loop(uint64_t pid, const TraceOptions& options, bool kill_on_setup_failure) -> int {
    TraceOutput output = open_trace_output(pid, options);
    if (!output.valid) {
        if (kill_on_setup_failure) {
            reap_tracee_after_startup_failure(static_cast<pid_t>(pid));
        } else {
            detach_tracee_after_startup_failure(pid);
        }
        return 1;
    }

    TraceState state{};
    std::unordered_map<uint64_t, PendingSyscall> pending;
    pending.reserve(8);

    (void)ptrace_call(ker::abi::ptrace::request::SETOPTIONS, pid, 0, TRACE_SYSGOOD_OPTION);

    for (;;) {
        ker::abi::ptrace::StopInfo stop{};
        if (!syscall_wait(pid, stop)) {
            std::println(stderr, "strace: PTRACE_SYSCALL_WAIT failed");
            reap_follow_helpers(state, true);
            close_trace_output(output);
            return 1;
        }
        reap_follow_helpers(state, false);

        if ((stop.flags & ker::abi::ptrace::STOP_INFO_EXITED) != 0) {
            if (WIFEXITED(stop.wait_status)) {
                emit_trace_line(options, output, pid, current_realtime(options),
                                std::format("+++ exited with {} +++", WEXITSTATUS(stop.wait_status)));
            } else if (WIFSIGNALED(stop.wait_status)) {
                emit_trace_line(options, output, pid, current_realtime(options),
                                std::format("+++ killed by signal {} +++", WTERMSIG(stop.wait_status)));
            } else {
                emit_trace_line(options, output, pid, current_realtime(options),
                                std::format("+++ exited with status {:#x} +++", stop.wait_status));
            }
            reap_follow_helpers(state, true);
            close_trace_output(output);
            return state.helper_status;
        }

        auto const& event = stop.event;
        if (event.reason == ker::abi::ptrace::stop_reason::SYSCALL_ENTER) {
            auto it = pending.try_emplace(event.tid).first;
            if (!it->second.valid && (stop.flags & ker::abi::ptrace::STOP_INFO_REGS_VALID) != 0) {
                auto const& regs = stop.regs;
                it->second = PendingSyscall{
                    .valid = true,
                    .callnum = regs.rax,
                    .a1 = regs.rdi,
                    .a2 = regs.rsi,
                    .a3 = regs.rdx,
                    .a4 = regs.r8,
                    .a5 = regs.r9,
                    .a6 = regs.r10,
                    .entered_at = current_realtime(options),
                };
            }
        } else if (event.reason == ker::abi::ptrace::stop_reason::SYSCALL_EXIT) {
            if ((stop.flags & ker::abi::ptrace::STOP_INFO_REGS_VALID) != 0) {
                auto const RESULT = static_cast<int64_t>(stop.regs.rax);
                auto it = pending.find(event.tid);
                if (it != pending.end() && it->second.valid) {
                    emit_trace_line(options, output, pid, it->second.entered_at,
                                    std::format("{} = {}", format_entry(pid, it->second), format_result(RESULT)));
                    if (options.follow_forks) {
                        uint64_t child_pid = 0;
                        if (fork_child_from_syscall_result(it->second, RESULT, child_pid)) {
                            spawn_follow_helper(state, output, child_pid, options);
                        }
                    }
                    it->second.valid = false;
                } else {
                    emit_trace_line(options, output, pid, current_realtime(options),
                                    std::format("{} = {}", callnum_name(event.message), format_result(RESULT)));
                }
            }
        } else {
            emit_trace_line(options, output, pid, current_realtime(options), std::format("--- stopped by signal {} ---", event.signal));
        }
    }
}

auto launch_tracee(char** argv) -> int {
    if (ptrace_call(ker::abi::ptrace::request::TRACEME, 0, 0, 0) < 0) {
        std::perror("strace: PTRACE_TRACEME");
        return 1;
    }
    int64_t const TARGET_RC =
        ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL | ker::process::WKI_TARGET_FLAG_NOINHERIT);
    if (TARGET_RC < 0) {
        std::println(stderr, "strace: failed to pin tracee locally: {}", format_result(TARGET_RC));
        return 1;
    }
    if (kill(getpid(), SIGSTOP) != 0) {
        std::perror("strace: SIGSTOP");
        return 1;
    }
    execvp(argv[0], argv);
    std::perror("strace: execvp");
    return 127;
}

auto attach_and_trace(uint64_t pid, bool route_proxy, const TraceOptions& options) -> int {
    if (route_proxy) {
        ker::abi::ptrace::RemoteInfo info{};
        if (read_remote_info(pid, info) && is_proxy_info(info)) {
            return exec_strace_remote_attach(info, options);
        }
    }

    if (ptrace_call(ker::abi::ptrace::request::ATTACH, pid, 0, 0) < 0) {
        std::println(stderr, "strace: attach to {} failed", pid);
        return 1;
    }
    int status = 0;
    if (!wait_for_trace_startup_stop(static_cast<pid_t>(pid), status)) {
        detach_tracee_after_startup_failure(pid);
        return 1;
    }
    return trace_loop(pid, options, false);
}

auto trace_command(char** argv, const TraceOptions& options) -> int {
    pid_t const CHILD = fork();
    if (CHILD < 0) {
        std::perror("strace: fork");
        return 1;
    }
    if (CHILD == 0) {
        return launch_tracee(argv);
    }
    int status = 0;
    if (!wait_for_trace_startup_stop(CHILD, status)) {
        reap_tracee_after_startup_failure(CHILD);
        return 1;
    }
    if (target_is_proxy(static_cast<uint64_t>(CHILD))) {
        std::println(stderr, "strace: tracee became a WKI proxy before syscall tracing could start");
        return 1;
    }
    return trace_loop(static_cast<uint64_t>(CHILD), options, true);
}

auto route_remote_preferred(char** command_argv, const TraceOptions& options) -> int {
    int64_t const TARGET_RC = ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    if (TARGET_RC < 0) {
        std::println(stderr, "strace: failed to set remote policy: {}", format_result(TARGET_RC));
        return 1;
    }
    return exec_strace_with_command(command_argv, options);
}

auto route_to_host(const char* hostname, char** command_argv, const TraceOptions& options) -> int {
    if (hostname == nullptr || hostname[0] == '\0') {
        usage();
        return 1;
    }
    int64_t const TARGET_RC = ker::process::setwkitarget(hostname, std::strlen(hostname), ker::process::WKI_TARGET_FLAG_STRICT);
    if (TARGET_RC < 0) {
        std::println(stderr, "strace: failed to target '{}': {}", hostname, format_result(TARGET_RC));
        return 1;
    }
    return exec_strace_with_command(command_argv, options);
}

auto route_homeward(char** command_argv, const TraceOptions& options) -> int {
    std::array<char, ker::abi::ptrace::RemoteInfo::TARGET_HOSTNAME_LEN> launcher = {};
    int64_t const LAUNCHER_LEN = ker::process::wki_launcher_node(launcher.data(), launcher.size());
    if (LAUNCHER_LEN <= 0 || launcher.front() == '\0') {
        std::println(stderr, "strace: failed to resolve launcher node");
        return 1;
    }
    return route_to_host(launcher.data(), command_argv, options);
}

}  // namespace

auto main(int argc, char** argv) -> int {
    if (argc < 2) {
        usage();
        return 1;
    }

    TraceOptions options{};
    int arg_index = 1;
    bool option_error = false;
    for (; arg_index < argc; arg_index++) {
        std::string_view const ARG = argv[arg_index];
        if (ARG == "--") {
            arg_index++;
            break;
        }
        if (!parse_trace_option(argc, argv, arg_index, options, option_error)) {
            break;
        }
        if (option_error) {
            return 1;
        }
    }

    if (arg_index >= argc) {
        usage();
        return 1;
    }

    if (std::string_view(argv[arg_index]) == remote_attach_flag_arg()) {
        if (argc != arg_index + 2) {
            usage();
            return 1;
        }
        uint64_t pid = 0;
        if (!parse_pid_arg(argv[arg_index + 1], pid)) {
            std::println(stderr, "strace: invalid pid '{}'", argv[arg_index + 1]);
            return 1;
        }
        return attach_and_trace(pid, false, options);
    }

    if (std::string_view(argv[arg_index]) == remote_command_flag_arg()) {
        if (argc < arg_index + 2) {
            usage();
            return 1;
        }
        return trace_command(&argv[arg_index + 1], options);
    }

    if (std::strcmp(argv[arg_index], "-p") == 0) {
        if (argc != arg_index + 2) {
            usage();
            return 1;
        }
        uint64_t pid = 0;
        if (!parse_pid_arg(argv[arg_index + 1], pid)) {
            std::println(stderr, "strace: invalid pid '{}'", argv[arg_index + 1]);
            return 1;
        }
        return attach_and_trace(pid, true, options);
    }

    std::string_view const WRAPPER = command_basename(argv[arg_index]);
    if (WRAPPER == "locally") {
        if (argc < arg_index + 2) {
            usage();
            return 1;
        }
        return trace_command(&argv[arg_index + 1], options);
    }
    if (WRAPPER == "remotely") {
        if (argc < arg_index + 2) {
            usage();
            return 1;
        }
        return route_remote_preferred(&argv[arg_index + 1], options);
    }
    if (WRAPPER == "on") {
        if (argc < arg_index + 3) {
            usage();
            return 1;
        }
        return route_to_host(argv[arg_index + 1], &argv[arg_index + 2], options);
    }
    if (WRAPPER == "homeward") {
        if (argc < arg_index + 2) {
            usage();
            return 1;
        }
        return route_homeward(&argv[arg_index + 1], options);
    }

    return trace_command(&argv[arg_index], options);
}
