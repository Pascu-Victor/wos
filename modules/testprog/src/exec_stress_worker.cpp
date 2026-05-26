#include <sys/mman.h>
#include <sys/multiproc.h>
#include <sys/process.h>
#include <unistd.h>

#include <cstdlib>
#include <new>
#include <print>
#include <string_view>

auto main(int argc, char** argv, char** envp) -> int {
    (void)envp;
    int const TID = ker::multiproc::currentThreadId();
    int const PID = ker::process::getpid();
    std::println("exec_stress_worker[t:{},p:{}]: started, argc={}", TID, PID, argc);

    // Default parameters
    constexpr int DEFAULT_MMAP_COUNT = 10;
    constexpr size_t DEFAULT_MMAP_SIZE = 1 << 20;  // 1 MiB
    constexpr int DEFAULT_ALLOC_COUNT = 512;
    constexpr size_t DEFAULT_ALLOC_SIZE = 4096;  // 4 KiB
    int mmap_count = DEFAULT_MMAP_COUNT;
    size_t mmap_size = DEFAULT_MMAP_SIZE;
    int alloc_count = DEFAULT_ALLOC_COUNT;
    size_t alloc_size = DEFAULT_ALLOC_SIZE;

    for (int i = 1; i < argc; ++i) {
        std::string_view const ARG = argv[i];
        if (ARG == "--mmap-count" && i + 1 < argc) {
            mmap_count = std::atoi(argv[++i]);
        } else if (ARG == "--mmap-size" && i + 1 < argc) {
            mmap_size = static_cast<size_t>(std::strtoul(argv[++i], nullptr, 0));
        } else if (ARG == "--alloc-count" && i + 1 < argc) {
            alloc_count = std::atoi(argv[++i]);
        } else if (ARG == "--alloc-size" && i + 1 < argc) {
            alloc_size = static_cast<size_t>(std::strtoul(argv[++i], nullptr, 0));
        }
    }

    // Do mmap allocations (do not munmap)
    std::println("exec_stress_worker[t:{},p:{}]: performing {} mmaps of {} bytes", TID, PID, mmap_count, mmap_size);
    for (int i = 0; i < mmap_count; ++i) {
        void* addr = mmap(nullptr, mmap_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (addr == MAP_FAILED) {
            std::println("exec_stress_worker[t:{},p:{}]: mmap failed at iter {}", TID, PID, i);
            break;
        }
        // touch first byte to ensure allocation
        *static_cast<volatile char*>(addr) = static_cast<char>(i & 0xff);
    }

    // Do heap allocations (leak them intentionally)
    std::println("exec_stress_worker[t:{},p:{}]: performing {} allocations of {} bytes", TID, PID, alloc_count, alloc_size);
    for (int i = 0; i < alloc_count; ++i) {
        auto* p = new (std::nothrow) char[alloc_size];
        if (p == nullptr) {
            std::println("exec_stress_worker[t:{},p:{}]: malloc failed at iter {}", TID, PID, i);
            break;
        }
        p[0] = static_cast<char>(i & 0xff);
        // intentionally do not free
    }

    std::println("exec_stress_worker[t:{},p:{}]: finished, exiting", TID, PID);
    return 0;
}
