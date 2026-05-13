#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace ker::abi::ptrace {

// NOLINTNEXTLINE(performance-enum-size)
enum class request : uint64_t {
    TRACEME = 0,
    PEEKDATA = 2,
    POKEDATA = 5,
    CONT = 7,
    KILL = 8,
    SINGLESTEP = 9,
    GETREGSET = 12,
    SETREGSET = 13,
    ATTACH = 16,
    DETACH = 17,
    SYSCALL = 24,
    SETOPTIONS = 0x4200,
    GETEVENTMSG = 0x4201,
    SEIZE = 0x4206,
    INTERRUPT = 0x4207,

    LIST_THREADS = 0x5700,
    READ_MEM = 0x5701,
    WRITE_MEM = 0x5702,
    GET_MAPS = 0x5703,
    GET_IMAGES = 0x5704,
    GET_REMOTE_INFO = 0x5705,
    SET_HW_BREAK = 0x5706,
    DEL_HW_BREAK = 0x5707,
};

// NOLINTNEXTLINE(performance-enum-size)
enum class regset : uint64_t {
    X86_64_GPR = 1,
    X86_64_XSAVE = 2,
};

// NOLINTNEXTLINE(performance-enum-size)
enum class stop_reason : uint64_t {
    NONE = 0,
    SIGNAL = 1,
    BREAKPOINT = 2,
    TRACE = 3,
    WATCHPOINT = 4,
    EXCEPTION = 5,
    EXEC = 6,
    FORK = 7,
    CLONE = 8,
    EXIT = 9,
    INTERRUPT = 10,
    SYSCALL_ENTER = 11,
    SYSCALL_EXIT = 12,
};

// NOLINTNEXTLINE(performance-enum-size)
enum class hw_break_type : uint64_t {
    EXECUTE = 0,
    WRITE = 1,
    READ_WRITE = 3,
};
// NOLINTNEXTLINE(readability-identifier-naming)
struct X86_64GprState {
    uint64_t rax;
    uint64_t rbx;
    uint64_t rcx;
    uint64_t rdx;
    uint64_t rsi;
    uint64_t rdi;
    uint64_t rbp;
    uint64_t rsp;
    uint64_t r8;
    uint64_t r9;
    uint64_t r10;
    uint64_t r11;
    uint64_t r12;
    uint64_t r13;
    uint64_t r14;
    uint64_t r15;
    uint64_t rip;
    uint64_t rflags;
    uint64_t cs;
    uint64_t ss;
    uint64_t fs_base;
    uint64_t gs_base;
};

struct RegsetIo {
    regset kind;
    void* buffer;
    size_t size;
};

struct MemIo {
    uint64_t address;
    void* buffer;
    size_t size;
    size_t transferred;
};

struct ThreadList {
    uint64_t* tids;
    size_t capacity;
    size_t count;
};

struct ImageRecord {
    static constexpr size_t PATH_LEN = 256;
    char path[PATH_LEN];
    uint64_t load_base;
    uint64_t text_addr;
    uint64_t text_size;
    uint64_t entry;
    uint32_t flags;
    uint32_t reserved;
};

struct ImageList {
    ImageRecord* images;
    size_t capacity;
    size_t count;
};

struct Event {
    stop_reason reason;
    uint32_t signal;
    uint32_t reserved;
    uint64_t tid;
    uint64_t address;
    uint64_t message;
};

struct RemoteInfo {
    constexpr static size_t TARGET_HOSTNAME_LEN = 64;
    uint32_t is_proxy;
    uint32_t state;
    uint64_t proxy_pid;
    uint64_t task_id;
    uint64_t target_node;
    uint64_t remote_pid;
    std::array<char, TARGET_HOSTNAME_LEN> target_hostname;
};

struct HwBreak {
    uint64_t address;
    uint32_t length;
    hw_break_type type;
    uint32_t slot;
    uint32_t reserved;
};

static_assert(sizeof(X86_64GprState) == 176);  // NOLINT
static_assert(sizeof(ImageRecord) == 296);     // NOLINT
static_assert(sizeof(Event) == 40);            // NOLINT
static_assert(sizeof(RemoteInfo) == 104);      // NOLINT

}  // namespace ker::abi::ptrace
