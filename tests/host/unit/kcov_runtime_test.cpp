// Host execution of the production KCOV runtime and NASM callback. The runner
// supplies only allocator stubs and a sequence of real call sites.
#include <atomic>
#include <cassert>
#include <cstdlib>
#include <platform/mm/phys.hpp>
#include <sanitizer/kcov.hpp>
#include <thread>
#include <vector>

namespace kcov = ker::sanitizer::kcov;
extern "C" void kcov_test_sites();
extern "C" char kcov_test_sites_end[];

static bool fail_allocation = false;
static uint64_t allocated_bytes = 0;

namespace ker::mod::mm::phys {
auto get_free_mem_bytes() -> uint64_t { return 128ULL * 1024 * 1024; }
auto page_alloc_can_satisfy(uint64_t, uint64_t) -> bool { return true; }
auto page_alloc_may_fail(PhysicalPageOwner, uint64_t bytes, std::string_view) -> void* {
    allocated_bytes = bytes;
    return fail_allocation ? nullptr : std::calloc(1, bytes);
}
}  // namespace ker::mod::mm::phys

int main() {
    // Each call is five bytes; the return address of the first call is bit 0.
    auto const START = reinterpret_cast<uintptr_t>(&kcov_test_sites) + 5;
    auto const END = reinterpret_cast<uintptr_t>(kcov_test_sites_end);
    assert(END - START == 636);
    kcov_test_sites();  // No buffer yet.
    assert(kcov::alloc_text_bitmap(END, START) != 0);
    fail_allocation = true;
    assert(kcov::alloc_text_bitmap(START, END) != 0);
    assert(kcov::current_buffer() == nullptr);
    fail_allocation = false;
    assert(kcov::alloc_text_bitmap(START, END) == 0);
    assert(allocated_bytes == 4096);
    auto* buf = kcov::current_buffer();
    kcov_test_sites();  // Allocated but disabled.
    kcov::disable();
    assert(buf->count == 0);
    kcov::enable();
    std::vector<std::thread> workers;
    for (unsigned i = 0; i < 8; ++i) {
        workers.emplace_back([] {
            for (unsigned repeat = 0; repeat < 2000; ++repeat) {
                kcov_test_sites();
            }
        });
    }
    for (auto& worker : workers) {
        worker.join();
    }
    kcov::disable();
    assert(buf->count == 128);
    assert(!buf->truncated);
    assert(buf->active_writers == 0);
    for (uint64_t bit = 0; bit < allocated_bytes * 8; ++bit) {
        bool const HIT = (buf->bitmap[bit / 64] & (1ULL << (bit % 64))) != 0;
        assert(HIT == (bit < 636 && bit % 5 == 0));
    }
    kcov::reset();
    assert(buf->count == 0);
    kcov_test_sites();
    kcov::disable();
    assert(buf->count == 0);  // Reset preserved disabled state.
    kcov::enable();
    kcov::reset();
    assert(buf->enabled);  // Reset also preserves enabled state.
    std::atomic<bool> stop{false};
    std::atomic<unsigned> ready{0};
    workers.clear();
    for (unsigned i = 0; i < 8; ++i) {
        workers.emplace_back([&] {
            kcov_test_sites();
            ++ready;
            while (!stop.load()) {
                kcov_test_sites();
            }
        });
    }
    while (ready.load() != 8) {
        std::this_thread::yield();
    }
    kcov::disable();  // Drain while callbacks continue arriving.
    assert(buf->count == 128);
    stop = true;
    for (auto& worker : workers) {
        worker.join();
    }
    assert(buf->active_writers == 0);
    assert(!buf->truncated);
    kcov::reset();
    kcov_test_sites();
    kcov::disable();
    assert(buf->count == 0);

    // Exclude both the first and last return addresses: no out-of-bounds write,
    // and dropped PCs must be visible rather than reported as full coverage.
    assert(kcov::alloc_text_bitmap(START + 1, END - 1) == 0);
    buf = kcov::current_buffer();
    kcov::enable();
    kcov_test_sites();
    kcov::disable();
    assert(buf->count == 126);
    assert(buf->truncated);
    kcov::reset();
    assert(!buf->truncated);

    // Raw-PC collection retains repeated visits and its capacity guard.
    assert(kcov::alloc_buffer(512) == 0);
    buf = kcov::current_buffer();
    kcov::enable();
    for (unsigned i = 0; i < 5; ++i) {
        kcov_test_sites();
    }
    kcov::disable();
    assert(buf->count == 512);
    assert(buf->truncated);
    assert(buf->pcs[0] == START && buf->pcs[128] == START);
}
