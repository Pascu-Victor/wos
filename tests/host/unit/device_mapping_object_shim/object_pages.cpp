#include <cstdlib>
#include <cstring>
#include <limits>
#include <map>
#include <object_pages.hpp>
#include <platform/mm/phys.hpp>

namespace object_fixture {
thread_local unsigned irq_depth{}, lock_depth{}, mutex_depth{}, pub_depth{};
std::mutex pub_mutex;
bool ordinary = true, available = true, fail_pin = false;
bool pat_complete = true, pat_wb = true, pat_other_bad = false;
int fail_allocate_after = -1, fail_reserve_after = -1;
namespace {
std::mutex pages_lock;
std::map<void*, uint32_t> pages;
}  // namespace
void check(bool condition) {
    if (!condition) std::abort();
}
auto live_pages() -> size_t {
    std::lock_guard guard(pages_lock);
    return pages.size();
}
auto references(void* page) -> uint32_t {
    std::lock_guard guard(pages_lock);
    auto found = pages.find(page);
    return found == pages.end() ? 0 : found->second;
}
void set_references(void* page, uint32_t value) {
    std::lock_guard guard(pages_lock);
    check(pages.contains(page));
    pages.at(page) = value;
}
}  // namespace object_fixture

namespace ker::mod::mm::phys {
auto can_wait_for_reclaim() -> bool {
    return object_fixture::ordinary && object_fixture::irq_depth == 0 && object_fixture::lock_depth == 0;
}
auto page_alloc_may_fail(PhysicalPageOwner owner, uint64_t size, std::string_view) -> void* {
    using namespace object_fixture;
    check(can_wait_for_reclaim() && owner == PhysicalPageOwner::USER_SHARED_MEMORY && size == 4096);
    if (fail_allocate_after == 0) return nullptr;
    if (fail_allocate_after > 0) --fail_allocate_after;
    void* page = std::aligned_alloc(4096, 4096);
    if (page == nullptr) return nullptr;
    std::memset(page, 0xA5, 4096);  // Actual production Object must initialize it.
    std::lock_guard guard(pages_lock);
    check(pages.emplace(page, 1).second);
    return page;
}
auto page_ref_try_inc(void* page) -> bool {
    using namespace object_fixture;
    check(lock_depth == 0);
    std::lock_guard guard(pages_lock);
    auto found = pages.find(page);
    if (fail_pin || found == pages.end() || found->second == UINT32_MAX) return false;
    ++found->second;
    return true;
}
auto page_ref_dec(void* page) -> uint32_t {
    using namespace object_fixture;
    check(lock_depth == 0);
    std::lock_guard guard(pages_lock);
    auto found = pages.find(page);
    check(found != pages.end() && found->second != 0);
    uint32_t const left = --found->second;
    if (left == 0) {
        check(irq_depth == 0);
        std::free(page);
        pages.erase(found);
    }
    return left;
}
auto page_ref_get(void* page) -> uint32_t { return object_fixture::references(page); }
}  // namespace ker::mod::mm::phys
