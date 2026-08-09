#include "usercopy_negative.hpp"

#include <abi/callnums/futex.h>
#include <abi/callnums/multiproc.h>
#include <abi/callnums/net.h>
#include <abi/callnums/process.h>
#include <abi/callnums/shm.h>
#include <abi/callnums/sys_log.h>
#include <abi/callnums/time.h>
#include <abi/callnums/vfs.h>
#include <abi/callnums/vmem.h>
#include <fcntl.h>
#include <pthread.h>
#include <sched.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <sys/wait.h>
#include <unistd.h>

#include <abi/ptrace.hpp>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <print>

namespace {

constexpr size_t PAGE_SIZE = 4096;
constexpr uint64_t USER_ADDR_LIMIT = 0x0000800000000000ULL;
constexpr uint64_t KERNEL_RANGE_ADDRESS = 0xFFFF800000000000ULL;
// Keep this aligned for FUTEX while sizeof(int) still wraps the address range.
constexpr uint64_t WRAPPED_RANGE_ADDRESS = UINT64_MAX - 3;
constexpr int RACE_ITERATIONS = 4000;

struct InvalidAddress {
    const char* name;
    uint64_t value;
};

constexpr std::array INVALID_ADDRESSES{
    InvalidAddress{.name = "null", .value = 0},
    InvalidAddress{.name = "user-limit/noncanonical", .value = USER_ADDR_LIMIT},
    InvalidAddress{.name = "kernel-range", .value = KERNEL_RANGE_ADDRESS},
    InvalidAddress{.name = "wrapped-range", .value = WRAPPED_RANGE_ADDRESS},
};

struct ProbeState {
    int checks = 0;
    int failures = 0;

    void expect(const char* family, const char* scenario, int64_t actual, int64_t expected) {
        ++checks;
        if (actual == expected) {
            return;
        }
        ++failures;
        std::println(stderr, "usercopy-negative: FAIL {}/{}: expected {}, got {}", family, scenario, expected, actual);
    }

    void fail(const char* family, const char* scenario, int64_t detail) {
        ++checks;
        ++failures;
        std::println(stderr, "usercopy-negative: FAIL {}/{}: {}", family, scenario, detail);
    }
};

auto raw_syscall(ker::abi::callnums callnum, uint64_t a1 = 0, uint64_t a2 = 0, uint64_t a3 = 0, uint64_t a4 = 0, uint64_t a5 = 0,
                 uint64_t a6 = 0) -> int64_t {
    return static_cast<int64_t>(syscall(callnum, a1, a2, a3, a4, a5, a6));
}

auto pointer_value(const void* ptr) -> uint64_t { return reinterpret_cast<uint64_t>(ptr); }

void probe_invalid_address_matrix(ProbeState& state, int shmid) {
    for (const auto& invalid : INVALID_ADDRESSES) {
        uint64_t const ADDRESS = invalid.value;
        state.expect("SYS_LOG", invalid.name,
                     raw_syscall(ker::abi::callnums::sys_log, static_cast<uint64_t>(ker::abi::sys_log::sys_log_ops::LOG), ADDRESS, 1,
                                 static_cast<uint64_t>(ker::abi::sys_log::sys_log_device::SERIAL)),
                     -EFAULT);
        state.expect("FUTEX", invalid.name,
                     raw_syscall(ker::abi::callnums::futex, static_cast<uint64_t>(ker::abi::futex::futex_ops::FUTEX_WAKE), ADDRESS, 1),
                     -EFAULT);
        state.expect("THREADING", invalid.name,
                     raw_syscall(ker::abi::callnums::threading, static_cast<uint64_t>(ker::abi::multiproc::threadControlOps::CREATE_DOMAIN),
                                 ADDRESS),
                     -EFAULT);
        state.expect(
            "PROCESS", invalid.name,
            raw_syscall(ker::abi::callnums::process, static_cast<uint64_t>(ker::abi::process::procmgmt_ops::GETHOSTNAME), ADDRESS, 64),
            -EFAULT);
        state.expect("PTRACE", invalid.name,
                     raw_syscall(ker::abi::callnums::process, static_cast<uint64_t>(ker::abi::process::procmgmt_ops::PTRACE),
                                 static_cast<uint64_t>(ker::abi::ptrace::request::GET_REMOTE_INFO), 0, 0, ADDRESS),
                     -EFAULT);
        state.expect("TIME", invalid.name,
                     raw_syscall(ker::abi::callnums::time, static_cast<uint64_t>(ker::abi::sys_time_ops::GETTIMEOFDAY), ADDRESS), -EFAULT);
        state.expect("VFS", invalid.name,
                     raw_syscall(ker::abi::callnums::vfs, static_cast<uint64_t>(ker::abi::vfs::ops::OPEN), ADDRESS, O_RDONLY), -EFAULT);
        state.expect("NET", invalid.name,
                     raw_syscall(ker::abi::callnums::net, static_cast<uint64_t>(ker::abi::net::ops::NETCTL_LINK_SET), ADDRESS), -EFAULT);
        state.expect("VMEM", invalid.name,
                     raw_syscall(ker::abi::callnums::vmem, static_cast<uint64_t>(ker::abi::vmem::ops::SWAPON), ADDRESS), -EFAULT);
        state.expect("SHM", invalid.name,
                     raw_syscall(ker::abi::callnums::shm, static_cast<uint64_t>(ker::abi::shm::ops::CTL), static_cast<uint64_t>(shmid),
                                 static_cast<uint64_t>(ker::abi::shm::IPC_STAT), ADDRESS),
                     -EFAULT);
    }
}

void probe_partially_mapped_records(ProbeState& state, int shmid) {
    auto* mapping = static_cast<uint8_t*>(mmap(nullptr, PAGE_SIZE * 2, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    if (mapping == MAP_FAILED) {
        state.fail("COMMON", "partial-map setup mmap", -errno);
        return;
    }

    constexpr size_t MAPPED_PREFIX = 8;
    auto* partial = mapping + PAGE_SIZE - MAPPED_PREFIX;
    std::memset(partial, 'x', MAPPED_PREFIX);
    if (munmap(mapping + PAGE_SIZE, PAGE_SIZE) != 0) {
        state.fail("COMMON", "partial-map setup munmap", -errno);
        munmap(mapping, PAGE_SIZE * 2);
        return;
    }
    uint64_t const ADDRESS = pointer_value(partial);

    state.expect("SYS_LOG", "mapped-prefix progress",
                 raw_syscall(ker::abi::callnums::sys_log, static_cast<uint64_t>(ker::abi::sys_log::sys_log_ops::LOG), ADDRESS,
                             MAPPED_PREFIX * 2, static_cast<uint64_t>(ker::abi::sys_log::sys_log_device::SERIAL)),
                 0);
    state.expect(
        "THREADING", "partially-mapped record",
        raw_syscall(ker::abi::callnums::threading, static_cast<uint64_t>(ker::abi::multiproc::threadControlOps::CREATE_DOMAIN), ADDRESS),
        -EFAULT);
    state.expect("PROCESS", "partially-mapped output",
                 raw_syscall(ker::abi::callnums::process, static_cast<uint64_t>(ker::abi::process::procmgmt_ops::UNAME), ADDRESS), -EFAULT);
    state.expect("PTRACE", "partially-mapped output",
                 raw_syscall(ker::abi::callnums::process, static_cast<uint64_t>(ker::abi::process::procmgmt_ops::PTRACE),
                             static_cast<uint64_t>(ker::abi::ptrace::request::GET_REMOTE_INFO), 0, 0, ADDRESS),
                 -EFAULT);
    state.expect("TIME", "partially-mapped output",
                 raw_syscall(ker::abi::callnums::time, static_cast<uint64_t>(ker::abi::sys_time_ops::GETTIMEOFDAY), ADDRESS), -EFAULT);

    // The preceding partial copyouts are allowed to update the mapped prefix
    // before reporting EFAULT at the page boundary. Restore a non-terminating
    // prefix so each C-string assertion actually reaches the unmapped page.
    std::memset(partial, 'x', MAPPED_PREFIX);
    state.expect("VFS", "partially-mapped C string",
                 raw_syscall(ker::abi::callnums::vfs, static_cast<uint64_t>(ker::abi::vfs::ops::OPEN), ADDRESS, O_RDONLY), -EFAULT);
    state.expect("NET", "partially-mapped record",
                 raw_syscall(ker::abi::callnums::net, static_cast<uint64_t>(ker::abi::net::ops::NETCTL_LINK_SET), ADDRESS), -EFAULT);

    std::memset(partial, 'x', MAPPED_PREFIX);
    state.expect("VMEM", "partially-mapped C string",
                 raw_syscall(ker::abi::callnums::vmem, static_cast<uint64_t>(ker::abi::vmem::ops::SWAPON), ADDRESS), -EFAULT);
    state.expect("SHM", "partially-mapped output",
                 raw_syscall(ker::abi::callnums::shm, static_cast<uint64_t>(ker::abi::shm::ops::CTL), static_cast<uint64_t>(shmid),
                             static_cast<uint64_t>(ker::abi::shm::IPC_STAT), ADDRESS),
                 -EFAULT);

    if (munmap(mapping, PAGE_SIZE) != 0) {
        state.fail("COMMON", "partial-map cleanup", -errno);
    }
}

void probe_readonly_outputs(ProbeState& state, int shmid) {
    auto* mapping = static_cast<uint8_t*>(mmap(nullptr, PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    if (mapping == MAP_FAILED) {
        state.fail("COMMON", "read-only setup mmap", -errno);
        return;
    }
    std::memset(mapping, 0xA5, PAGE_SIZE);
    if (mprotect(mapping, PAGE_SIZE, PROT_READ) != 0) {
        state.fail("COMMON", "read-only setup mprotect", -errno);
        munmap(mapping, PAGE_SIZE);
        return;
    }
    uint64_t const ADDRESS = pointer_value(mapping);

    state.expect("PROCESS", "read-only output",
                 raw_syscall(ker::abi::callnums::process, static_cast<uint64_t>(ker::abi::process::procmgmt_ops::GETHOSTNAME), ADDRESS, 64),
                 -EFAULT);
    state.expect("PTRACE", "read-only output",
                 raw_syscall(ker::abi::callnums::process, static_cast<uint64_t>(ker::abi::process::procmgmt_ops::PTRACE),
                             static_cast<uint64_t>(ker::abi::ptrace::request::GET_REMOTE_INFO), 0, 0, ADDRESS),
                 -EFAULT);
    state.expect("TIME", "read-only output",
                 raw_syscall(ker::abi::callnums::time, static_cast<uint64_t>(ker::abi::sys_time_ops::GETTIMEOFDAY), ADDRESS), -EFAULT);
    state.expect("SHM", "read-only output",
                 raw_syscall(ker::abi::callnums::shm, static_cast<uint64_t>(ker::abi::shm::ops::CTL), static_cast<uint64_t>(shmid),
                             static_cast<uint64_t>(ker::abi::shm::IPC_STAT), ADDRESS),
                 -EFAULT);

    if (munmap(mapping, PAGE_SIZE) != 0) {
        state.fail("COMMON", "read-only cleanup", -errno);
    }
}

void probe_lazy_output(ProbeState& state) {
    void* mapping = mmap(nullptr, PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (mapping == MAP_FAILED) {
        state.fail("COMMON", "lazy setup mmap", -errno);
        return;
    }
    state.expect("TIME", "lazy copyout",
                 raw_syscall(ker::abi::callnums::time, static_cast<uint64_t>(ker::abi::sys_time_ops::GETTIMEOFDAY), pointer_value(mapping)),
                 0);
    if (munmap(mapping, PAGE_SIZE) != 0) {
        state.fail("COMMON", "lazy cleanup", -errno);
    }
}

void probe_cow_output(ProbeState& state) {
    auto* mapping = static_cast<uint8_t*>(mmap(nullptr, PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    if (mapping == MAP_FAILED) {
        state.fail("COMMON", "COW setup mmap", -errno);
        return;
    }
    std::memset(mapping, 0xA5, PAGE_SIZE);

    pid_t const CHILD = fork();
    if (CHILD < 0) {
        state.fail("COMMON", "COW setup fork", -errno);
        munmap(mapping, PAGE_SIZE);
        return;
    }
    if (CHILD == 0) {
        int64_t const RESULT =
            raw_syscall(ker::abi::callnums::time, static_cast<uint64_t>(ker::abi::sys_time_ops::GETTIMEOFDAY), pointer_value(mapping));
        _Exit(RESULT == 0 ? 0 : 1);
    }

    int status = 0;
    if (waitpid(CHILD, &status, 0) != CHILD) {
        state.fail("COMMON", "COW waitpid", -errno);
    } else {
        state.expect("TIME", "COW child copyout", WIFEXITED(status) && WEXITSTATUS(status) == 0 ? 0 : -1, 0);
        bool unchanged = true;
        for (size_t i = 0; i < sizeof(long) * 2; ++i) {
            unchanged = unchanged && mapping[i] == 0xA5;
        }
        state.expect("TIME", "COW parent isolation", unchanged ? 0 : -1, 0);
    }

    if (munmap(mapping, PAGE_SIZE) != 0) {
        state.fail("COMMON", "COW cleanup", -errno);
    }
}

struct RaceContext {
    void* address = nullptr;
    std::atomic<bool> start{false};
    std::atomic<bool> stop{false};
    std::atomic<int> map_failures{0};
};

void* race_mapper(void* raw_context) {
    auto* context = static_cast<RaceContext*>(raw_context);
    while (!context->start.load(std::memory_order_acquire)) {
        sched_yield();
    }
    while (!context->stop.load(std::memory_order_acquire)) {
        void* const MAPPING = mmap(context->address, PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED, -1, 0);
        if (MAPPING == MAP_FAILED) {
            context->map_failures.fetch_add(1, std::memory_order_relaxed);
            continue;
        }
        if (munmap(context->address, PAGE_SIZE) != 0) {
            context->map_failures.fetch_add(1, std::memory_order_relaxed);
            break;
        }
    }
    return nullptr;
}

void probe_concurrent_unmap(ProbeState& state) {
    RaceContext context{};
    context.address = mmap(nullptr, PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    if (context.address == MAP_FAILED) {
        state.fail("COMMON", "race setup mmap", -errno);
        return;
    }

    pthread_t worker{};  // NOLINT(misc-include-cleaner): WOS pthread.h provides pthread_t.
    int const CREATE_RESULT = pthread_create(&worker, nullptr, race_mapper, &context);
    if (CREATE_RESULT != 0) {
        state.fail("COMMON", "race pthread_create", -CREATE_RESULT);
        munmap(context.address, PAGE_SIZE);
        return;
    }
    if (munmap(context.address, PAGE_SIZE) != 0) {
        state.fail("COMMON", "race setup munmap", -errno);
        context.stop.store(true, std::memory_order_release);
        context.start.store(true, std::memory_order_release);
        pthread_join(worker, nullptr);
        return;
    }

    context.start.store(true, std::memory_order_release);
    for (int i = 0; i < RACE_ITERATIONS; ++i) {
        int64_t const RESULT = raw_syscall(ker::abi::callnums::time, static_cast<uint64_t>(ker::abi::sys_time_ops::GETTIMEOFDAY),
                                           pointer_value(context.address));
        if (RESULT != 0 && RESULT != -EFAULT) {
            state.fail("TIME", "concurrent mmap/munmap result", RESULT);
            break;
        }
        ++state.checks;
    }

    context.stop.store(true, std::memory_order_release);
    int const JOIN_RESULT = pthread_join(worker, nullptr);
    if (JOIN_RESULT != 0) {
        state.fail("COMMON", "race pthread_join", -JOIN_RESULT);
    }
    state.expect("VMEM", "concurrent mapper errors", context.map_failures.load(std::memory_order_relaxed), 0);
    static_cast<void>(munmap(context.address, PAGE_SIZE));
}

}  // namespace

auto run_usercopy_negative() -> int {
    ProbeState state{};
    int64_t const SHMID =
        raw_syscall(ker::abi::callnums::shm, static_cast<uint64_t>(ker::abi::shm::ops::GET),
                    static_cast<uint64_t>(ker::abi::shm::IPC_PRIVATE), PAGE_SIZE, static_cast<uint64_t>(ker::abi::shm::IPC_CREAT | 0600));
    if (SHMID < 0) {
        state.fail("SHM", "private segment setup", SHMID);
    } else {
        probe_invalid_address_matrix(state, static_cast<int>(SHMID));
        probe_partially_mapped_records(state, static_cast<int>(SHMID));
        probe_readonly_outputs(state, static_cast<int>(SHMID));
        state.expect("SHM", "private segment cleanup",
                     raw_syscall(ker::abi::callnums::shm, static_cast<uint64_t>(ker::abi::shm::ops::CTL), static_cast<uint64_t>(SHMID),
                                 static_cast<uint64_t>(ker::abi::shm::IPC_RMID)),
                     0);
    }

    probe_lazy_output(state);
    probe_cow_output(state);
    probe_concurrent_unmap(state);

    std::println("usercopy-negative: {} checks, {} failures", state.checks, state.failures);
    std::fflush(nullptr);
    return state.failures == 0 ? 0 : 1;
}
