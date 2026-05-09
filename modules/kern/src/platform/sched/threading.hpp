#pragma once

#include <defines/defines.hpp>
#include <platform/mm/paging.hpp>

// Forward declaration to avoid circular dependency
namespace ker::loader::elf {
struct TlsModule;
}

namespace ker::mod::sched::threading {

struct Thread {
    uint64_t fsbase{};
    uint64_t gsbase{};

    uint64_t stack{};
    uint64_t stack_size{};

    uint64_t tls_size{};
    uint64_t tls_base_virt{};
    uint64_t safestack_ptr_value{};

    // Physical memory pointers (HHDM addresses) for cleanup
    // These are the actual allocations that need to be freed
    uint64_t tls_phys_ptr{};    // HHDM pointer to TLS+TCB+SafeStack allocation
    uint64_t stack_phys_ptr{};  // HHDM pointer to stack allocation

    uint32_t magic = 0;
} __attribute__((packed));

void init_threading();

Thread* create_thread(uint64_t stack_size, uint64_t tls_size, mm::paging::PageTable* page_table,
                      const ker::loader::elf::TlsModule& tls_info);
void destroy_thread(Thread* thread);

// OOM diagnostics - get count of active threads
auto get_active_thread_count() -> uint64_t;
}  // namespace ker::mod::sched::threading
