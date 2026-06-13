#include <abi-bits/pid_t.h>
#include <abi-bits/signal.h>
#include <abi-bits/wait.h>
#include <abi/callnums/futex.h>
#include <abi/callnums/multiproc.h>
#include <abi/callnums/net.h>
#include <abi/callnums/process.h>
#include <abi/callnums/shm.h>
#include <abi/callnums/sys_log.h>
#include <abi/callnums/time.h>
#include <abi/callnums/vfs.h>
#include <abi/callnums/vmem.h>
#include <sys/callnums.h>
#include <sys/process.h>
#include <sys/wait.h>
#include <unistd.h>

#include <abi/ptrace.hpp>
#include <array>
#include <cerrno>
#include <csignal>
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

struct PendingSyscall {
    uint64_t callnum = 0;
    uint64_t a1 = 0;
    uint64_t a2 = 0;
    uint64_t a3 = 0;
    uint64_t a4 = 0;
    uint64_t a5 = 0;
    uint64_t a6 = 0;
};

void usage() { std::println(stderr, "usage: strace [-p pid] command [args...]"); }

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

auto ptrace_call(ker::abi::ptrace::request request, uint64_t pid, uint64_t addr, uint64_t data) -> int64_t {
    return ker::process::ptrace(static_cast<uint64_t>(request), pid, addr, data);
}

auto read_regs(uint64_t pid, ker::abi::ptrace::X86_64GprState& regs) -> bool {
    ker::abi::ptrace::RegsetIo io{
        .kind = ker::abi::ptrace::regset::X86_64_GPR,
        .buffer = &regs,
        .size = sizeof(regs),
    };
    return ptrace_call(ker::abi::ptrace::request::GETREGSET, pid, 0, reinterpret_cast<uint64_t>(&io)) >= 0;
}

auto read_mem(uint64_t pid, uint64_t addr, void* buffer, size_t size) -> bool {
    ker::abi::ptrace::MemIo io{
        .address = addr,
        .buffer = buffer,
        .size = size,
        .transferred = 0,
    };
    return ptrace_call(ker::abi::ptrace::request::READ_MEM, pid, 0, reinterpret_cast<uint64_t>(&io)) >= 0 && io.transferred == size;
}

auto read_event(uint64_t pid, ker::abi::ptrace::Event& event) -> bool {
    return ptrace_call(ker::abi::ptrace::request::GETEVENTMSG, pid, 0, reinterpret_cast<uint64_t>(&event)) >= 0;
}

auto read_remote_info(uint64_t pid, ker::abi::ptrace::RemoteInfo& info) -> bool {
    std::memset(&info, 0, sizeof(info));
    return ptrace_call(ker::abi::ptrace::request::GET_REMOTE_INFO, pid, 0, reinterpret_cast<uint64_t>(&info)) >= 0;
}

auto read_c_string(uint64_t pid, uint64_t addr) -> std::string {
    if (addr == 0) {
        return "NULL";
    }

    std::string out;
    out.reserve(MAX_STRING_LEN);
    for (size_t i = 0; i < MAX_STRING_LEN; ++i) {
        char ch = '\0';
        if (!read_mem(pid, addr + i, &ch, sizeof(ch))) {
            break;
        }
        if (ch == '\0') {
            return out;
        }
        out.push_back(ch);
    }
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

auto exec_strace_with_command(char** command_argv) -> int {
    std::vector<char*> helper_argv;
    helper_argv.push_back(strace_path_arg());
    helper_argv.push_back(remote_command_flag_arg());
    for (char** arg = command_argv; arg != nullptr && *arg != nullptr; ++arg) {
        helper_argv.push_back(*arg);
    }
    helper_argv.push_back(nullptr);

    execvp(strace_path_arg(), helper_argv.data());
    std::perror("strace: exec remote helper");
    return 127;
}

auto exec_strace_remote_attach(const ker::abi::ptrace::RemoteInfo& info) -> int {
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
    std::array<char*, 4> helper_argv = {
        strace_path_arg(),
        remote_attach_flag_arg(),
        remote_pid.data(),
        nullptr,
    };
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

auto trace_loop(uint64_t pid) -> int {
    std::unordered_map<uint64_t, PendingSyscall> pending;

    (void)ptrace_call(ker::abi::ptrace::request::SETOPTIONS, pid, 0, TRACE_SYSGOOD_OPTION);
    if (ptrace_call(ker::abi::ptrace::request::SYSCALL, pid, 0, 0) < 0) {
        std::println(stderr, "strace: PTRACE_SYSCALL failed");
        return 1;
    }

    for (;;) {
        int status = 0;
        pid_t const WAITED = waitpid(static_cast<pid_t>(pid), &status, 0);
        if (WAITED < 0) {
            if (errno == EINTR) {
                continue;
            }
            std::perror("strace: waitpid");
            return 1;
        }
        if (WIFEXITED(status)) {
            std::println("+++ exited with {} +++", WEXITSTATUS(status));
            return 0;
        }
        if (WIFSIGNALED(status)) {
            std::println("+++ killed by signal {} +++", WTERMSIG(status));
            return 0;
        }
        if (!WIFSTOPPED(status)) {
            continue;
        }

        ker::abi::ptrace::Event event{};
        (void)read_event(pid, event);
        if (event.reason == ker::abi::ptrace::stop_reason::SYSCALL_ENTER) {
            if (!pending.contains(event.tid)) {
                ker::abi::ptrace::X86_64GprState regs{};
                if (read_regs(pid, regs)) {
                    pending[event.tid] = PendingSyscall{
                        .callnum = regs.rax,
                        .a1 = regs.rdi,
                        .a2 = regs.rsi,
                        .a3 = regs.rdx,
                        .a4 = regs.r8,
                        .a5 = regs.r9,
                        .a6 = regs.r10,
                    };
                }
            }
        } else if (event.reason == ker::abi::ptrace::stop_reason::SYSCALL_EXIT) {
            ker::abi::ptrace::X86_64GprState regs{};
            if (read_regs(pid, regs)) {
                auto const RESULT = static_cast<int64_t>(regs.rax);
                auto it = pending.find(event.tid);
                if (it != pending.end()) {
                    std::println("{} = {}", format_entry(pid, it->second), format_result(RESULT));
                    pending.erase(it);
                } else {
                    std::println("{} = {}", callnum_name(event.message), format_result(RESULT));
                }
            }
        } else {
            std::println("--- stopped by signal {} ---", WSTOPSIG(status));
        }

        if (ptrace_call(ker::abi::ptrace::request::SYSCALL, pid, 0, 0) < 0) {
            std::println(stderr, "strace: resume failed");
            return 1;
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

auto attach_and_trace(uint64_t pid, bool route_proxy) -> int {
    if (route_proxy) {
        ker::abi::ptrace::RemoteInfo info{};
        if (read_remote_info(pid, info) && is_proxy_info(info)) {
            return exec_strace_remote_attach(info);
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
    return trace_loop(pid);
}

auto trace_command(char** argv) -> int {
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
    return trace_loop(static_cast<uint64_t>(CHILD));
}

auto route_remote_preferred(char** command_argv) -> int {
    int64_t const TARGET_RC = ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE);
    if (TARGET_RC < 0) {
        std::println(stderr, "strace: failed to set remote policy: {}", format_result(TARGET_RC));
        return 1;
    }
    return exec_strace_with_command(command_argv);
}

auto route_to_host(const char* hostname, char** command_argv) -> int {
    if (hostname == nullptr || hostname[0] == '\0') {
        usage();
        return 1;
    }
    int64_t const TARGET_RC = ker::process::setwkitarget(hostname, std::strlen(hostname), ker::process::WKI_TARGET_FLAG_STRICT);
    if (TARGET_RC < 0) {
        std::println(stderr, "strace: failed to target '{}': {}", hostname, format_result(TARGET_RC));
        return 1;
    }
    return exec_strace_with_command(command_argv);
}

auto route_homeward(char** command_argv) -> int {
    std::array<char, ker::abi::ptrace::RemoteInfo::TARGET_HOSTNAME_LEN> launcher = {};
    int64_t const LAUNCHER_LEN = ker::process::wki_launcher_node(launcher.data(), launcher.size());
    if (LAUNCHER_LEN <= 0 || launcher.front() == '\0') {
        std::println(stderr, "strace: failed to resolve launcher node");
        return 1;
    }
    return route_to_host(launcher.data(), command_argv);
}

}  // namespace

auto main(int argc, char** argv) -> int {
    if (argc < 2) {
        usage();
        return 1;
    }

    if (std::string_view(argv[1]) == remote_attach_flag_arg()) {
        if (argc != 3) {
            usage();
            return 1;
        }
        uint64_t pid = 0;
        if (!parse_pid_arg(argv[2], pid)) {
            std::println(stderr, "strace: invalid pid '{}'", argv[2]);
            return 1;
        }
        return attach_and_trace(pid, false);
    }

    if (std::string_view(argv[1]) == remote_command_flag_arg()) {
        if (argc < 3) {
            usage();
            return 1;
        }
        return trace_command(&argv[2]);
    }

    if (std::strcmp(argv[1], "-p") == 0) {
        if (argc != 3) {
            usage();
            return 1;
        }
        uint64_t pid = 0;
        if (!parse_pid_arg(argv[2], pid)) {
            std::println(stderr, "strace: invalid pid '{}'", argv[2]);
            return 1;
        }
        return attach_and_trace(pid, true);
    }

    std::string_view const WRAPPER = command_basename(argv[1]);
    if (WRAPPER == "locally") {
        if (argc < 3) {
            usage();
            return 1;
        }
        return trace_command(&argv[2]);
    }
    if (WRAPPER == "remotely") {
        if (argc < 3) {
            usage();
            return 1;
        }
        return route_remote_preferred(&argv[2]);
    }
    if (WRAPPER == "on") {
        if (argc < 4) {
            usage();
            return 1;
        }
        return route_to_host(argv[2], &argv[3]);
    }
    if (WRAPPER == "homeward") {
        if (argc < 3) {
            usage();
            return 1;
        }
        return route_homeward(&argv[2]);
    }

    return trace_command(&argv[1]);
}
