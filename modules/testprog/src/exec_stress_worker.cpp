#include <sys/mman.h>
#include <sys/multiproc.h>
#include <sys/process.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <print>

int main(int argc, char** argv, char** envp) {
    (void)envp;
    int tid = ker::multiproc::currentThreadId();
    int pid = ker::process::getpid();
    std::println("exec_stress_worker[t:{},p:{}]: started, argc={}", tid, pid, argc);

    // Default parameters
    int mmap_count = 10;
    size_t mmap_size = 1 << 20;  // 1 MiB
    int alloc_count = 512;
    size_t alloc_size = 4096;  // 4 KiB

    for (int i = 1; i < argc; ++i) {
        if (strcmp(argv[i], "--mmap-count") == 0 && i + 1 < argc) {
            mmap_count = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--mmap-size") == 0 && i + 1 < argc) {
            mmap_size = (size_t)strtoul(argv[++i], nullptr, 0);
        } else if (strcmp(argv[i], "--alloc-count") == 0 && i + 1 < argc) {
            alloc_count = atoi(argv[++i]);
        } else if (strcmp(argv[i], "--alloc-size") == 0 && i + 1 < argc) {
            alloc_size = (size_t)strtoul(argv[++i], nullptr, 0);
        }
    }

    // Do mmap allocations (do not munmap)
    std::println("exec_stress_worker[t:{},p:{}]: performing {} mmaps of {} bytes", tid, pid, mmap_count, mmap_size);
    for (int i = 0; i < mmap_count; ++i) {
        void* addr = mmap(nullptr, mmap_size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (addr == (void*)-1) {
            std::println("exec_stress_worker[t:{},p:{}]: mmap failed at iter {}", tid, pid, i);
            break;
        }
        // touch first byte to ensure allocation
        *((volatile char*)addr) = (char)(i & 0xff);
    }

    // Do heap allocations (leak them intentionally)
    std::println("exec_stress_worker[t:{},p:{}]: performing {} allocations of {} bytes", tid, pid, alloc_count, alloc_size);
    for (int i = 0; i < alloc_count; ++i) {
        void* p = malloc(alloc_size);
        if (!p) {
            std::println("exec_stress_worker[t:{},p:{}]: malloc failed at iter {}", tid, pid, i);
            break;
        }
        ((char*)p)[0] = (char)(i & 0xff);
        // intentionally do not free
    }

    std::println("exec_stress_worker[t:{},p:{}]: finished, exiting", tid, pid);
    return 0;
}
