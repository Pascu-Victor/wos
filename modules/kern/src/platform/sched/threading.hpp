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
    uint64_t stack_base_virt{};
    uint64_t stack_lowest_backed{};

    uint64_t tls_size{};
    uint64_t tls_base_virt{};
    uint64_t safestack_ptr_value{};

    // Physical memory pointers (HHDM addresses) for legacy direct cleanup.
    // Current process stacks and TLS are page-granular pagemap-owned ranges,
    // so both remain 0 and destroy_user_space() frees committed pages.
    uint64_t tls_phys_ptr{};    // Legacy eager TLS allocation
    uint64_t stack_phys_ptr{};  // HHDM pointer to legacy eager stack allocation

    uint32_t magic = 0;
} __attribute__((packed));

void init_threading();

Thread* create_thread(uint64_t stack_size, uint64_t tls_size, mm::paging::PageTable* page_table, uint64_t initial_tid,
                      const ker::loader::elf::TlsModule& tls_info);
void destroy_thread(Thread* thread);
bool ensure_stack_backing(Thread* thread, mm::paging::PageTable* page_table, uint64_t start, uint64_t end);
bool handle_lazy_stack_fault(Thread* thread, mm::paging::PageTable* page_table, uint64_t fault_addr, uint64_t rsp);

// OOM diagnostics - get count of active threads
auto get_active_thread_count() -> uint64_t;
}  // namespace ker::mod::sched::threading
