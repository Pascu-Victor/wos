#pragma once
#include <cstddef>
#include <cstdint>
#include <mutex>
#include <thread>

namespace object_fixture {
extern thread_local unsigned irq_depth, lock_depth, mutex_depth;
extern std::mutex pub_mutex;
extern thread_local unsigned pub_depth;
extern bool ordinary, available, fail_pin;
extern bool pat_complete, pat_wb, pat_other_bad;
extern int fail_allocate_after, fail_reserve_after;
void check(bool);
auto live_pages() -> size_t;
auto references(void*) -> uint32_t;
void set_references(void*, uint32_t);
auto queued() -> size_t;
auto reserved() -> size_t;
auto callbacks() -> size_t;
auto run_one() -> bool;
void drain();
struct IrqScope {
    IrqScope() : old(irq_depth) { ++irq_depth; }
    ~IrqScope() { irq_depth = old; }
    unsigned old;
};
}  // namespace object_fixture
