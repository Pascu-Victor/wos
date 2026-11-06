#include <vm_fixture.hpp>
#include <platform/mm/file_mapping_lifetime.hpp>
#include <platform/mm/lifetime_retirement_executor.hpp>

namespace object_fixture {
thread_local unsigned irq_depth{}, lock_depth{}, mutex_depth{}, pub_depth{};
std::mutex pub_mutex;
bool ordinary = true, available = true, fail_pin{};
bool pat_complete = true, pat_wb = true, pat_other_bad{};
int fail_allocate_after{-1}, fail_reserve_after{-1};
namespace {
struct Frame { uint32_t references{1}; ker::mod::mm::PageKind kind; };
std::map<void*, Frame> frames;
}
void check(bool value) { if (!value) std::abort(); }
auto live_pages() -> size_t {
    size_t count = 0;
    for (auto const& [page, frame] : frames) { (void)page; count += frame.kind == ker::mod::mm::PageKind::NORMAL; }
    return count;
}
auto references(void* page) -> uint32_t {
    auto found = frames.find(page);
    return found == frames.end() ? 0 : found->second.references;
}
void set_references(void* page, uint32_t value) { check(frames.contains(page)); frames.at(page).references = value; }
}
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
    if (!page) return nullptr;
    std::memset(page, 0xA5, 4096);
    check(frames.emplace(page, Frame{1, PageKind::NORMAL}).second);
    return page;
}
auto page_ref_try_inc(void* page) -> bool {
    using namespace object_fixture;
    check(lock_depth == 0);
    auto found = frames.find(page);
    if (fail_pin || found == frames.end() || found->second.references == UINT32_MAX) return false;
    ++found->second.references;
    return true;
}
auto page_ref_dec(void* page) -> uint32_t {
    using namespace object_fixture;
    check(lock_depth == 0 && irq_depth == 0);
    auto found = frames.find(page);
    check(found != frames.end() && found->second.references != 0);
    uint32_t const left = --found->second.references;
    if (left == 0) { std::free(page); frames.erase(found); }
    return left;
}
auto page_ref_get(void* page) -> uint32_t { return object_fixture::references(page); }
auto page_kind_get(void* page) -> PageKind {
    auto found = object_fixture::frames.find(page);
    return found == object_fixture::frames.end() ? PageKind::UNKNOWN : found->second.kind;
}
}
namespace vm_fixture {
auto allocate_table() -> ker::mod::mm::paging::PageTable* {
    require(object_fixture::irq_depth == 0 && object_fixture::lock_depth == 0 && object_fixture::pub_depth == 0,
            "table preparation outside root/PUB/IRQ");
    ++table_attempts;
    if (fail_table_after == 0) return nullptr;
    if (fail_table_after > 0) --fail_table_after;
    void* page = std::aligned_alloc(4096, 4096);
    if (!page) return nullptr;
    std::memset(page, 0, 4096);
    require(object_fixture::frames.emplace(page, object_fixture::Frame{1, ker::mod::mm::PageKind::PAGE_TABLE}).second,
            "finite table owner insertion");
    return static_cast<ker::mod::mm::paging::PageTable*>(page);
}
void release_table(ker::mod::mm::paging::PageTable* table) {
    require(object_fixture::pub_depth == 0, "unused table release outside PUB");
    require(ker::mod::mm::phys::page_kind_get(table) == ker::mod::mm::PageKind::PAGE_TABLE && object_fixture::references(table) == 1,
            "finite table storage has exact sole owner");
    (void)ker::mod::mm::phys::page_ref_dec(table);
}
auto table_pages() -> size_t { return object_fixture::frames.size() - object_fixture::live_pages(); }
}
namespace ker::mod::mm::huge::detail {
auto allocate_table() noexcept -> paging::PageTable* { return vm_fixture::allocate_table(); }
void release_table(paging::PageTable* table) noexcept { vm_fixture::release_table(table); }
}
namespace ker::mod::mm::file_mapping {
auto LifetimeRetirementExecutor::is_current_worker() noexcept -> bool { return vm_fixture::worker; }
RetirementCompletion::RetirementCompletion(RetirementCompletion&&) noexcept = default;
auto RetirementCompletion::operator=(RetirementCompletion&&) noexcept -> RetirementCompletion& = default;
RetirementCompletion::~RetirementCompletion() = default;
void RetirementCompletion::reset() noexcept { object_fixture::check(empty()); }
auto RetirementCompletion::query() const noexcept -> RetirementReturn { object_fixture::check(empty()); return {}; }
}
