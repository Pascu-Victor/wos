#include "aslr_hardening.hpp"

#include <elf.h>
#include <link.h>
#include <pthread.h>
#include <signal.h>
#include <sys/auxv.h>
#include <sys/mman.h>
#include <sys/process.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cinttypes>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <print>

namespace {

constexpr size_t PAGE_SIZE = 4096;
constexpr int UNEXPECTED_SURVIVAL = 90;
constexpr int PROBE_SETUP_FAILED = 91;
constexpr int PR_SET_DUMPABLE = 4;

thread_local uint64_t g_tls_marker = 0x41534c52544c5301ULL;

struct ImageLayout {
    uintptr_t main_bias = 0;
    uintptr_t interpreter_bias = 0;
    uintptr_t main_relro = 0;
};

struct MapRange {
    uintptr_t start = 0;
    uintptr_t end = 0;
    std::array<char, 5> permissions{};
};

auto collect_image_layout(dl_phdr_info* info, size_t, void* opaque) -> int {
    auto& layout = *static_cast<ImageLayout*>(opaque);
    const char* const NAME = info->dlpi_name;
    if (NAME == nullptr || NAME[0] == '\0') {
        layout.main_bias = info->dlpi_addr;
        for (size_t i = 0; i < info->dlpi_phnum; ++i) {
            if (info->dlpi_phdr[i].p_type == PT_GNU_RELRO && info->dlpi_phdr[i].p_memsz != 0) {
                layout.main_relro = info->dlpi_addr + info->dlpi_phdr[i].p_vaddr;
                break;
            }
        }
    } else if (std::strstr(NAME, "ld.so") != nullptr) {
        layout.interpreter_bias = info->dlpi_addr;
    }
    return 0;
}

auto wait_for_child(pid_t child, int& status) -> bool {
    while (true) {
        pid_t const WAITED = waitpid(child, &status, 0);
        if (WAITED == child) {
            return true;
        }
        if (WAITED < 0 && errno == EINTR) {
            continue;
        }
        return false;
    }
}

using FaultProbe = void (*)();
using SuccessProbe = auto (*)() -> bool;

auto run_fault_probe(const char* name, FaultProbe probe) -> bool {
    pid_t const CHILD = fork();
    if (CHILD < 0) {
        std::println("{{\"kind\":\"wos-aslr-protection-v1\",\"test\":\"{}\",\"result\":\"fail\",\"error\":{}}}", name, errno);
        return false;
    }
    if (CHILD == 0) {
        if (ker::process::prctl(PR_SET_DUMPABLE, 0, 0, 0, 0) != 0) {
            _Exit(PROBE_SETUP_FAILED);
        }
        probe();
        _Exit(UNEXPECTED_SURVIVAL);
    }

    int status = 0;
    bool const WAITED = wait_for_child(CHILD, status);
    bool const PASSED = WAITED && WIFSIGNALED(status) && WTERMSIG(status) == SIGSEGV;
    std::println("{{\"kind\":\"wos-aslr-protection-v1\",\"test\":\"{}\",\"result\":\"{}\",\"waitStatus\":{}}}", name,
                 PASSED ? "pass" : "fail", WAITED ? status : -1);
    return PASSED;
}

auto run_success_probe(const char* name, SuccessProbe probe) -> bool {
    pid_t const CHILD = fork();
    if (CHILD < 0) {
        std::println("{{\"kind\":\"wos-aslr-protection-v1\",\"test\":\"{}\",\"result\":\"fail\",\"error\":{}}}", name, errno);
        return false;
    }
    if (CHILD == 0) {
        if (ker::process::prctl(PR_SET_DUMPABLE, 0, 0, 0, 0) != 0) {
            _Exit(PROBE_SETUP_FAILED);
        }
        _Exit(probe() ? 0 : UNEXPECTED_SURVIVAL);
    }

    int status = 0;
    bool const WAITED = wait_for_child(CHILD, status);
    bool const PASSED = WAITED && WIFEXITED(status) && WEXITSTATUS(status) == 0;
    std::println("{{\"kind\":\"wos-aslr-protection-v1\",\"test\":\"{}\",\"result\":\"{}\",\"waitStatus\":{}}}", name,
                 PASSED ? "pass" : "fail", WAITED ? status : -1);
    return PASSED;
}

void probe_anonymous_nx() {
    auto* mapping = static_cast<uint8_t*>(mmap(nullptr, PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0));
    if (mapping == MAP_FAILED) {
        _Exit(PROBE_SETUP_FAILED);
    }
    mapping[0] = 0xc3;  // x86-64 ret
    __builtin___clear_cache(reinterpret_cast<char*>(mapping), reinterpret_cast<char*>(mapping + 1));
    asm volatile("call *%0" : : "r"(mapping) : "memory");
}

void probe_stack_nx() {
    std::array<uint8_t, 16> code{};
    code.at(0) = 0xc3;  // x86-64 ret
    __builtin___clear_cache(reinterpret_cast<char*>(code.data()), reinterpret_cast<char*>(code.data() + 1));
    asm volatile("call *%0" : : "r"(code.data()) : "memory");
}

void* probe_guard_thread(void*) {
    pthread_attr_t attr{};
    if (pthread_getattr_np(pthread_self(), &attr) != 0) {
        _Exit(PROBE_SETUP_FAILED);
    }
    void* stack_address = nullptr;
    size_t stack_size = 0;
    size_t guard_size = 0;
    if (pthread_attr_getstack(&attr, &stack_address, &stack_size) != 0 || pthread_attr_getguardsize(&attr, &guard_size) != 0 ||
        pthread_attr_destroy(&attr) != 0 || stack_address == nullptr || stack_size == 0 || guard_size == 0) {
        _Exit(PROBE_SETUP_FAILED);
    }

    auto* guard_byte = reinterpret_cast<volatile uint8_t*>(reinterpret_cast<uintptr_t>(stack_address) - 1);
    *guard_byte = 0x5a;
    return nullptr;
}

auto find_initial_stack_guard() -> volatile uint8_t* {
    std::byte stack_marker{};
    std::array<MapRange, 256> ranges{};
    size_t range_count = 0;

    FILE* const MAPS = std::fopen("/proc/self/maps", "r");
    if (MAPS == nullptr) {
        return nullptr;
    }

    std::array<char, 512> line{};
    while (range_count < ranges.size() && std::fgets(line.data(), line.size(), MAPS) != nullptr) {
        MapRange range{};
        if (std::sscanf(line.data(), "%" SCNxPTR "-%" SCNxPTR " %4s", &range.start, &range.end, range.permissions.data()) == 3) {
            ranges.at(range_count++) = range;
        }
    }
    std::fclose(MAPS);

    uintptr_t const STACK_MARKER = reinterpret_cast<uintptr_t>(&stack_marker);
    size_t current = range_count;
    for (size_t i = 0; i < range_count; ++i) {
        if (STACK_MARKER >= ranges.at(i).start && STACK_MARKER < ranges.at(i).end) {
            current = i;
            break;
        }
    }

    // The initial stack is mapped as:
    //   guard | bottom backing | lazy reserve | top backing (contains SP) | guard.
    // pthread_getattr_np() reports only the top backing on WOS, so identify the
    // actual outer guard from the complete VM map instead of probing the lazy
    // reserve and accidentally growing the stack.
    if (current < 3) {
        return nullptr;
    }
    MapRange const& TOP_BACKING = ranges.at(current);
    MapRange const& RESERVE = ranges.at(current - 1);
    MapRange const& BOTTOM_BACKING = ranges.at(current - 2);
    MapRange const& GUARD = ranges.at(current - 3);
    bool const CONTIGUOUS = GUARD.end == BOTTOM_BACKING.start && BOTTOM_BACKING.end == RESERVE.start && RESERVE.end == TOP_BACKING.start;
    bool const EXPECTED_PERMISSIONS = GUARD.permissions.at(0) == '-' && GUARD.permissions.at(1) == '-' && GUARD.permissions.at(2) == '-' &&
                                      BOTTOM_BACKING.permissions.at(1) == 'w' && RESERVE.permissions.at(0) == '-' &&
                                      RESERVE.permissions.at(1) == '-' && RESERVE.permissions.at(2) == '-' &&
                                      TOP_BACKING.permissions.at(1) == 'w';
    if (!CONTIGUOUS || !EXPECTED_PERMISSIONS || GUARD.end - GUARD.start != PAGE_SIZE) {
        return nullptr;
    }
    return reinterpret_cast<volatile uint8_t*>(GUARD.end - 1);
}

void probe_initial_stack_guard() {
    volatile uint8_t* const GUARD_BYTE = find_initial_stack_guard();
    if (GUARD_BYTE == nullptr) {
        _Exit(PROBE_SETUP_FAILED);
    }
    *GUARD_BYTE = 0x5a;
}

auto probe_guard_vm_mutation_rejected() -> bool {
    volatile uint8_t* const GUARD_BYTE = find_initial_stack_guard();
    if (GUARD_BYTE == nullptr) {
        return false;
    }
    uintptr_t const GUARD_PAGE = reinterpret_cast<uintptr_t>(GUARD_BYTE) & ~(PAGE_SIZE - 1);
    void* const REPLACEMENT =
        mmap(reinterpret_cast<void*>(GUARD_PAGE), PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_FIXED, -1, 0);
    bool const MAP_REJECTED = REPLACEMENT == MAP_FAILED;
    bool const PROTECT_REJECTED = mprotect(reinterpret_cast<void*>(GUARD_PAGE), PAGE_SIZE, PROT_READ | PROT_WRITE) != 0;
    bool const UNMAP_REJECTED = munmap(reinterpret_cast<void*>(GUARD_PAGE), PAGE_SIZE) != 0;
    return MAP_REJECTED && PROTECT_REJECTED && UNMAP_REJECTED;
}

auto probe_at_random_coredump() -> bool {
    uintptr_t const AT_RANDOM_ADDRESS = getauxval(AT_RANDOM);
    if (AT_RANDOM_ADDRESS == 0) {
        return false;
    }
    auto* const ENTROPY = reinterpret_cast<volatile uint8_t*>(AT_RANDOM_ADDRESS);
    uint8_t accumulator = 0;
    for (size_t i = 0; i < 16; ++i) {
        accumulator = static_cast<uint8_t>(accumulator | ENTROPY[i]);
    }
    return accumulator != 0;
}

void probe_thread_guard() {
    pthread_t thread{};
    if (pthread_create(&thread, nullptr, probe_guard_thread, nullptr) != 0) {
        _Exit(PROBE_SETUP_FAILED);
    }
    if (pthread_join(thread, nullptr) != 0) {
        _Exit(PROBE_SETUP_FAILED);
    }
}

void probe_relro() {
    ImageLayout images{};
    (void)dl_iterate_phdr(collect_image_layout, &images);
    if (images.main_relro == 0) {
        _Exit(PROBE_SETUP_FAILED);
    }
    *reinterpret_cast<volatile uint8_t*>(images.main_relro) = 0;
}

auto report_layout() -> int {
    ImageLayout images{};
    (void)dl_iterate_phdr(collect_image_layout, &images);

    std::byte stack_marker{};
    void* const HEAP = std::malloc(64);
    void* const MAPPING = mmap(nullptr, PAGE_SIZE, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    uintptr_t const AT_RANDOM_ADDRESS = getauxval(AT_RANDOM);
    bool const READY = HEAP != nullptr && MAPPING != MAP_FAILED && AT_RANDOM_ADDRESS != 0 && images.main_relro != 0;

    std::println(
        "{{\"kind\":\"wos-aslr-layout-v1\",\"pid\":{},\"mainLoadBias\":\"0x{:x}\",\"interpreterLoadBias\":\"0x{:x}\","
        "\"codeAddress\":\"0x{:x}\",\"stackAddress\":\"0x{:x}\",\"tlsAddress\":\"0x{:x}\",\"heapAddress\":\"0x{:x}\","
        "\"anonymousMappingAddress\":\"0x{:x}\",\"atRandomAddress\":\"0x{:x}\",\"relroAddress\":\"0x{:x}\","
        "\"atRandomPresent\":{},\"result\":\"{}\"}}",
        static_cast<long long>(getpid()), images.main_bias, images.interpreter_bias, reinterpret_cast<uintptr_t>(&run_aslr_hardening),
        reinterpret_cast<uintptr_t>(&stack_marker), reinterpret_cast<uintptr_t>(&g_tls_marker), reinterpret_cast<uintptr_t>(HEAP),
        MAPPING == MAP_FAILED ? 0 : reinterpret_cast<uintptr_t>(MAPPING), AT_RANDOM_ADDRESS, images.main_relro, AT_RANDOM_ADDRESS != 0,
        READY ? "pass" : "fail");

    if (MAPPING != MAP_FAILED) {
        (void)munmap(MAPPING, PAGE_SIZE);
    }
    std::free(HEAP);
    return READY ? 0 : 1;
}

void print_usage() { std::println("usage: testprog aslr-hardening <report|nx-anon|nx-stack|guard|relro|coredump-relro|all>"); }

}  // namespace

auto run_aslr_hardening(int argc, char** argv) -> int {
    const char* const ACTION = argc > 0 ? argv[0] : "report";
    if (std::strcmp(ACTION, "report") == 0) {
        return report_layout();
    }
    if (std::strcmp(ACTION, "nx-anon") == 0) {
        return run_fault_probe("anonymous-nx", probe_anonymous_nx) ? 0 : 1;
    }
    if (std::strcmp(ACTION, "nx-stack") == 0) {
        return run_fault_probe("stack-nx", probe_stack_nx) ? 0 : 1;
    }
    if (std::strcmp(ACTION, "guard") == 0) {
        bool passed = run_fault_probe("initial-stack-guard", probe_initial_stack_guard);
        passed = run_success_probe("guard-vm-mutation-rejected", probe_guard_vm_mutation_rejected) && passed;
        passed = run_fault_probe("pthread-guard", probe_thread_guard) && passed;
        return passed ? 0 : 1;
    }
    if (std::strcmp(ACTION, "relro") == 0) {
        return run_fault_probe("relro-read-only", probe_relro) ? 0 : 1;
    }
    if (std::strcmp(ACTION, "coredump-relro") == 0) {
        if (report_layout() != 0 || !probe_at_random_coredump()) {
            return 1;
        }
        std::fflush(nullptr);
        probe_relro();
        return UNEXPECTED_SURVIVAL;
    }
    if (std::strcmp(ACTION, "all") == 0) {
        bool passed = report_layout() == 0;
        passed = run_fault_probe("anonymous-nx", probe_anonymous_nx) && passed;
        passed = run_fault_probe("stack-nx", probe_stack_nx) && passed;
        passed = run_fault_probe("initial-stack-guard", probe_initial_stack_guard) && passed;
        passed = run_success_probe("guard-vm-mutation-rejected", probe_guard_vm_mutation_rejected) && passed;
        passed = run_fault_probe("pthread-guard", probe_thread_guard) && passed;
        passed = run_fault_probe("relro-read-only", probe_relro) && passed;
        return passed ? 0 : 1;
    }

    print_usage();
    return 2;
}
