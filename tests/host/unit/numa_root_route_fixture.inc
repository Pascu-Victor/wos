// Explicit Host endpoints for extracted actual pool/factory/return bodies.
// This is not a real physical allocator, CPU/CR3, scheduler or root registry.
#include <array>
#include <atomic>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <platform/mm/lifecycle_census.hpp>
#include <platform/mm/numa_root_reserve.hpp>
#include <platform/mm/page_alloc.hpp>
#include <platform/mm/paging.hpp>
#include <platform/sys/spinlock.hpp>
#include <stdexcept>
#include <string_view>

namespace test {
unsigned assertions{};
void check(bool value, const char* message) {
    ++assertions;
    if (!value) throw std::runtime_error(message);
}
struct Stop {};
void outside() { check(ker::mod::sys::host_spinlocks_held == 0, "physical/output preparation under Host lock"); }
using namespace ker::mod::mm;
struct alignas(4096) Frame {
    paging::PageTable table{};
    PhysicalPageOwner owner{};
    PageKind kind{};
    uint32_t refs{};
    bool live{}, member{};
    uint16_t physical_node{};
};
std::array<Frame, 256> frames{};
unsigned physical_calls{}, frees{}, reserve_takes{}, reserve_returns{}, fail_calls{};
paging::PageTable* current_root{};
bool reserve_available{};
unsigned allocation_node{}, locality_node{}, reserve_index{255}, topology_reads{}, kind_queries{}, ref_queries{};
bool numa_boot{};
void (*retag_probe)(void*, PhysicalPageOwner){};
auto frame(void* p) -> Frame& {
    for (auto& f : frames)
        if (&f.table == p) {
            check(f.live, "provider touched returned/freed frame");
            return f;
        }
    throw std::runtime_error("unowned Host physical pointer");
}
auto allocate(PhysicalPageOwner owner) -> void* {
    outside();
    ++physical_calls;
    if (fail_calls != 0) {
        --fail_calls;
        return nullptr;
    }
    for (auto& f : frames)
        if (!f.live && !f.member && (!numa_boot || f.physical_node == allocation_node)) {
            f.live = true;
            f.owner = owner;
            f.kind = PageKind::NORMAL;
            f.refs = 1;
            return &f.table;
        }
    throw std::runtime_error("Host endpoint frame limit");
}
}  // namespace test
namespace ker::mod::mm::addr {
using vaddr_t = uint64_t;
auto get_phys_pointer(uint64_t value) -> uint64_t* { return reinterpret_cast<uint64_t*>(value); }
}  // namespace ker::mod::mm::addr
namespace ker::mod::dbg {
[[noreturn]] void panic_handler(const char*) { throw test::Stop{}; }
template <class... T>
void emergency_log(const char*, T...) {}
}  // namespace ker::mod::dbg
namespace ker::mod::mm::phys {
// WOS_TEST_INSERT_ACTUAL_PHYSICAL_TYPES
auto page_kind_get(void* p) -> PageKind {
    test::outside();
    ++test::kind_queries;
    return test::frame(p).kind;
}
auto page_ref_get(void* p) -> uint32_t {
    test::outside();
    ++test::ref_queries;
    return test::frame(p).refs;
}
auto page_reassign_owner(void* p, PhysicalPageOwner owner) -> bool {
    test::outside();
    if (test::retag_probe != nullptr) test::retag_probe(p, owner);
    test::frame(p).owner = owner;
    return true;
}
auto page_mark_kind(void* p, PageKind kind) -> bool {
    test::outside();
    test::frame(p).kind = kind;
    return true;
}
auto page_alloc_full_overwrite_page_with_reclaim_may_fail(PhysicalPageOwner owner, std::string_view) -> void* {
    return test::allocate(owner);
}
auto page_alloc_full_overwrite_page_with_reclaim(PhysicalPageOwner owner, std::string_view) -> void* { return test::allocate(owner); }
void page_free(void* p) {
    test::outside();
    auto& f = test::frame(p);
    test::check(!f.member && f.refs == 1, "free requires original sole ordinary backing");
    f.live = false;
    f.refs = 0;
    f.kind = PageKind::FREE;
    f.owner = PhysicalPageOwner::INVALID;
    ++test::frees;
}
}  // namespace ker::mod::mm::phys
namespace ker::mod::mm::numa::root_reserve {
auto try_take(NodeId requested) noexcept -> TakeResult {
    ++test::reserve_takes;
    if (requested != test::frames[test::reserve_index].physical_node) return {nullptr, Error::INVALID_NODE};
    if (!test::reserve_available) return {nullptr, Error::EMPTY};
    auto& f = test::frames[test::reserve_index];
    f.owner = PhysicalPageOwner::PAGE_TABLE;
    test::reserve_available = false;
    return {&f.table, Error::NONE};
}
auto try_return(paging::PageTable* p, ZeroTable zero) noexcept -> ReturnResult {
    auto& f = test::frames[test::reserve_index];
    if (p != &f.table || !f.member) return ReturnResult::NOT_MEMBER;
    if (test::reserve_available || f.refs != 1 || f.owner != PhysicalPageOwner::PAGE_TABLE) return ReturnResult::CORRUPT;
    zero(p);
    f.owner = PhysicalPageOwner::PAGE_TABLE_POOL_RESERVE;
    test::reserve_available = true;
    ++test::reserve_returns;
    return ReturnResult::RETURNED;
}
}  // namespace ker::mod::mm::numa::root_reserve
namespace ker::mod::mm::virt {
using namespace paging;
using namespace addr;
struct log {
    template <class... T>
    static void critical(const char*, T...) {}
};
[[noreturn]] void hcf() { throw test::Stop{}; }
// WOS_TEST_INSERT_ACTUAL_OUTPUT_TYPES
namespace reserve = numa::root_reserve;
// WOS_TEST_INSERT_ACTUAL_DIAGNOSTIC_TYPES
static_assert(sizeof(RootRouteDiagnostics) <= 8192);
// WOS_TEST_INSERT_ACTUAL_POOL_TYPES
PageTable* kernel_pagemap{};
auto rdcr3() -> uint64_t { return reinterpret_cast<uint64_t>(kernel_pagemap); }
void wrcr3(uint64_t) {}
void selftest_require_mutable_pagemap_root(PageTable*, const char*) {}
auto selftest_kernel_page_table_frame(PageTable* p) -> bool { return p == kernel_pagemap; }
// Explicit immutable topology primitive. Each binary invocation publishes
// exactly one boot context, independently of physical allocation locality.
}  // namespace ker::mod::mm::virt
namespace ker::mod::acpi::numa {
struct View {
    uint16_t node_count{1};
};
inline View value{};
inline bool published{true};
auto boot_topology() -> const View* {
    test::outside();
    ++test::topology_reads;
    return published ? &value : nullptr;
}
}  // namespace ker::mod::acpi::numa
namespace ker::mod::mm::virt {
// WOS_TEST_INSERT_ACTUAL_POOL_INITIALIZER
// WOS_TEST_INSERT_ACTUAL_POOL_FUNCTIONS
struct RootReserveLocality {
    uint64_t cpu{};
    numa::NodeId node{};
};
auto capture_root_reserve_locality() -> RootReserveLocality { return {0, static_cast<numa::NodeId>(test::locality_node)}; }
struct UserRootControlChunk {};
struct RootControl {
    uint64_t serial{1};
};
mod::sys::Spinlock user_mapping_pin_lock{};
lifecycle_census::Epoch selftest_root_epoch{};
// This route fixture never arms current-census mode. Actual epoch writer and
// return-tail sensitivity belongs to the separate semantic runtime fixture.
auto selftest_current_root_change_begin() noexcept -> bool { return false; }
void selftest_current_root_change_end(bool owned) noexcept { test::check(!owned, "unarmed route fixture owns no current epoch unit"); }
void selftest_current_root_detached_binding_returned() noexcept {}
enum class UserRootAttachResult { ATTACHED, DUPLICATE, SERIAL_EXHAUSTED, NEED_CHUNK };
std::array<PageTable*, 256> controls{};
bool serial_failure{}, need_chunk{}, chunk_failure{};
unsigned metadata_live{}, root_readers{};
RootControl control{};
namespace huge {
struct RootKey {
    uint64_t address_bits{}, incarnation{};
};
}  // namespace huge
struct ManagedIndex {
    auto root_has_published_owners(huge::RootKey) -> bool { return false; }
} managed_huge_registry;
struct FileIndex {
    auto root_empty_locked(PageTable*) -> bool { return true; }
} file_mapping_index;
auto attach_user_root_control_locked(PageTable* root, UserRootControlChunk*) -> UserRootAttachResult {
    if (serial_failure) return UserRootAttachResult::SERIAL_EXHAUSTED;
    if (need_chunk) return UserRootAttachResult::NEED_CHUNK;
    for (auto* p : controls)
        if (p == root) return UserRootAttachResult::DUPLICATE;
    for (auto& p : controls)
        if (p == nullptr) {
            p = root;
            ++metadata_live;
            return UserRootAttachResult::ATTACHED;
        }
    throw std::runtime_error("Host control capacity");
}
auto allocate_user_root_control_chunk() -> UserRootControlChunk* {
    test::outside();
    test::check(chunk_failure, "only declared control-failure endpoint is used");
    return nullptr;
}
auto find_user_root_control_locked(PageTable* root) -> RootControl* {
    for (auto* p : controls)
        if (p == root) return &control;
    return nullptr;
}
auto retire_user_root_control_locked(PageTable* root, UserRootControlChunk*&) -> bool {
    for (auto& p : controls)
        if (p == root) {
            p = nullptr;
            --metadata_live;
            return true;
        }
    return false;
}
void owned_frame_purge_pagemap(PageTable*) {}
auto is_current_pagemap(PageTable* root) -> bool { return root == test::current_root; }
struct UserPagemapExclusiveScope {
    explicit UserPagemapExclusiveScope(PageTable*) {
        test::check(root_readers == 0, "exclusive scope overlap");
        ++root_readers;
    }
    ~UserPagemapExclusiveScope() { --root_readers; }
};
// WOS_TEST_INSERT_ACTUAL_ROOT_FUNCTIONS
namespace mm = ker::mod::mm;
// WOS_TEST_INSERT_ACTUAL_CONSERVATION_PREDICATES
}  // namespace ker::mod::mm::virt

namespace test {
namespace virt = ker::mod::mm::virt;
namespace mm = ker::mod::mm;
using S = virt::SelftestRootAllocationSource;
using R = virt::SelftestRootReturnDisposition;
void reset() {
    check(virt::metadata_live == 0 && virt::root_readers == 0, "old owner/control scope still live");
    while (virt::page_table_pool.count != 0) {
        auto* p = virt::try_alloc_page_table_from_pool();
        phys::page_free(p);
    }
    for (auto& f : frames) {
        if (f.member) {
            check(reserve_available && f.owner == PhysicalPageOwner::PAGE_TABLE_POOL_RESERVE && f.refs == 1, "emergency member loan lost");
            f.member = false;
            f.live = false;
            f.refs = 0;
        }
        check(!f.live, "ordinary frame leaked outside cache");
    }
    reserve_available = false;
    fail_calls = 0;
    virt::serial_failure = virt::need_chunk = virt::chunk_failure = false;
}
void reserve_seed(unsigned node = 0) {
    reserve_index = numa_boot ? 254 + node : 255;
    auto& f = frames[reserve_index];
    check(!f.live, "seed already owned");
    f.live = f.member = true;
    f.refs = 1;
    f.kind = PageKind::PAGE_TABLE;
    f.owner = PhysicalPageOwner::PAGE_TABLE_POOL_RESERVE;
    reserve_available = true;
}
auto create(virt::SelftestRootFactoryOutput& out, virt::SelftestRootFactoryOptions options = {}) -> paging::PageTable* {
    return virt::selftest_create_pagemap(options, out);
}
void expect_return(paging::PageTable* p, R expected) {
    auto const pa = reinterpret_cast<uint64_t>(p);
    virt::SelftestRootReleaseOutput out{};
    virt::selftest_release_pagemap(p, out, 67);
    check(out.owned_facts_valid && out.historical_pa == pa && out.references == 1 && out.kind == PageKind::PAGE_TABLE,
          "release facts must come from still-owned exact root");
    check(out.control_retired && out.disposition == expected, "actual registered return disposition");
    auto const history = out;
    auto cleared = [&] {
        check(out.historical_pa == 0 && out.references == 0 && out.kind == PageKind::UNKNOWN && out.disposition == R::UNKNOWN &&
                  !out.owned_facts_valid && !out.control_retired,
              "reused release output is reset before admission refusal");
        check(out.historical_root_bits == 0 && !out.insertion.inserted && out.insertion.completed_sequence == 0,
              "reused insertion authority is reset before refusal");
    };
    virt::selftest_release_pagemap(nullptr, out);
    cleared();
    out = history;
    auto* original_kernel = virt::kernel_pagemap;
    virt::kernel_pagemap = p;
    virt::selftest_release_pagemap(p, out);
    cleared();
    virt::kernel_pagemap = original_kernel;
    out = history;
    current_root = p;
    virt::selftest_release_pagemap(p, out);
    cleared();
    current_root = nullptr;
}
void physical_and_pool() {
    virt::SelftestRootFactoryOutput a{}, b{};
    auto* first = create(a);
    check(first && a.source == S::PHYSICAL_ALLOCATOR && a.historical_pa_valid && a.historical_pa == reinterpret_cast<uint64_t>(first),
          "physical source and exact address");
    check(a.ordinary_allocator_called && !a.ordinary_failure_injected && !a.reserve_attempted && a.control_registered,
          "default genuine ordinary-first path");
    std::memset(first, 0x63, sizeof(*first));
    expect_return(first, R::LEGACY_CACHED);
    auto* second = create(b);
    check(second == first && b.source == S::LEGACY_POOL && b.historical_pa_valid, "actual pool hit labelled separately");
    auto* bytes = reinterpret_cast<const uint8_t*>(second);
    for (size_t i = 0; i < sizeof(*second); ++i) check(bytes[i] == 0, "real zero-on-return cache semantics");
    expect_return(second, R::LEGACY_CACHED);
}
void emergency() {
    reserve_seed();
    fail_calls = 1;
    virt::SelftestRootFactoryOutput out{};
    auto* root = create(out);
    check(root == &frames[reserve_index].table && out.source == S::EMERGENCY_RESERVE && out.reserve_attempted && !reserve_available,
          "actual ordinary failure precedes emergency source");
    expect_return(root, R::EMERGENCY_RETURNED);
    check(reserve_available && virt::page_table_pool.count == 0, "member returned before ordinary adoption");
}
void exhaustion() {
    fail_calls = 1;
    virt::SelftestRootFactoryOutput out{};
    check(create(out) == nullptr && out.source == S::UNKNOWN && !out.historical_pa_valid && !out.control_registered,
          "failed allocation cannot invent source/PA");
}
void full_pool() {
    virt::SelftestRootFactoryOutput out{};
    auto* root = create(out);
    for (size_t i = 0; i < virt::PAGE_TABLE_POOL_CAPACITY; ++i) {
        auto* page = static_cast<paging::PageTable*>(allocate(PhysicalPageOwner::PAGE_TABLE));
        phys::page_mark_kind(page, PageKind::PAGE_TABLE);
        virt::release_unregistered_user_root(page);
    }
    auto const before = frees;
    expect_return(root, R::PHYSICAL_RELEASED);
    check(frees == before + 1 && virt::page_table_pool.count == 64, "full pool frees exactly original owner once");
}
void rollback(bool from_reserve, bool no_chunk, bool injected) {
    if (from_reserve) reserve_seed();
    virt::serial_failure = !no_chunk;
    virt::need_chunk = no_chunk;
    virt::chunk_failure = no_chunk;
    virt::SelftestRootFactoryOptions options{};
    options.force_ordinary_failure = from_reserve;
    options.fail_needed_control_chunk = injected;
    virt::SelftestRootFactoryOutput out{};
    check(create(out, options) == nullptr && out.historical_pa_valid && !out.control_registered,
          "real admission failure retains actual history");
    check(out.rollback.owned_facts_valid && !out.rollback.control_retired && out.rollback.references == 1 &&
              out.rollback.historical_pa == out.historical_pa,
          "unregistered return facts precede actual return");
    check(out.rollback.disposition == (from_reserve ? R::EMERGENCY_RETURNED : (numa_boot ? R::PHYSICAL_RELEASED : R::LEGACY_CACHED)),
          "actual unregistered disposition");
    check(out.rollback_returned == from_reserve, "old reserved rollback flag preserved");
}
void bad_owner(bool kind) {
    virt::SelftestRootFactoryOutput out{};
    auto* root = create(out);
    auto& f = frame(root);
    if (kind)
        f.kind = PageKind::NORMAL;
    else
        f.refs = 2;
    virt::SelftestRootReleaseOutput returned{};
    bool refused = false;
    try {
        virt::selftest_release_pagemap(root, returned);
    } catch (Stop) {
        refused = true;
    }
    check(refused && !returned.owned_facts_valid && virt::metadata_live == 1, "old guard before metadata/output retirement");
    f.kind = PageKind::PAGE_TABLE;
    f.refs = 1;
    expect_return(root, numa_boot ? R::PHYSICAL_RELEASED : R::LEGACY_CACHED);
}
void old_wrapper() {
    auto* root = virt::create_pagemap_may_fail();
    check(root != nullptr, "old wrapper still allocates");
    virt::release_pagemap(root, 3);
    check(virt::page_table_pool.count == 1, "old wrapper retains original cache behavior");
}
void invalid_override() {
    auto const calls = physical_calls;
    virt::SelftestRootFactoryOptions options{};
    options.use_node_override = true;
    options.node_override = 2;
    virt::SelftestRootFactoryOutput out{};
    check(create(out, options) == nullptr && physical_calls == calls && out.source == S::UNKNOWN && !out.historical_pa_valid,
          "old invalid override refuses without diagnostics inventing allocation");
}

// These are explicit physical endpoint observations, not copies of the cache
// policy expression. A retained frame changes its node's free/owner/kind row.
struct PhysicalRows {
    std::array<unsigned, 2> free{};
    std::array<std::array<unsigned, mm::PHYSICAL_PAGE_OWNER_COUNT>, 2> owners{};
    std::array<std::array<unsigned, 8>, 2> kinds{};
    auto operator==(const PhysicalRows&) const -> bool = default;
};
auto physical_rows() -> PhysicalRows {
    PhysicalRows out{};
    for (auto const& f : frames) {
        check(f.physical_node < 2, "physical endpoint node is bounded");
        if (!f.live) {
            ++out.free[f.physical_node];
            ++out.kinds[f.physical_node][static_cast<size_t>(PageKind::FREE)];
        } else {
            auto const owner = static_cast<size_t>(f.owner);
            check(owner < mm::PHYSICAL_PAGE_OWNER_COUNT && f.refs == 1, "each modeled physical owner is exclusive");
            ++out.owners[f.physical_node][owner];
            ++out.kinds[f.physical_node][static_cast<size_t>(f.kind)];
        }
    }
    return out;
}
void initialize_boot(std::string_view mode) {
    numa_boot = mode == "numa";
    for (size_t i = 0; i < frames.size(); ++i) {
        frames[i].physical_node = numa_boot ? i % 2 : 0;
        frames[i].kind = PageKind::FREE;
    }
    auto& topology = ker::mod::acpi::numa::value;
    topology.node_count = numa_boot ? 2 : 1;
    // A pre-publication miss is real bootstrap history, not a resettable counter.
    auto const misses = virt::page_table_pool_alloc_misses.load();
    check(virt::try_alloc_page_table_from_pool() == nullptr, "empty bootstrap cache miss");
    check(virt::initialize_page_table_pool_after_topology(), "actual topology initializer accepts empty one-shot boot");
    check(topology_reads == 1 && virt::page_table_pool_alloc_misses.load() == misses + 1,
          "initializer reads topology once outside lock and preserves bootstrap misses");
    auto const observed = topology_reads;
    check(!virt::initialize_page_table_pool_after_topology(), "repeated initialization refuses without resetting cache");
    check(topology_reads == observed + 1, "one immutable topology lookup per initializer call");
}
void rejected_boot(std::string_view mode) {
    auto& topology = ker::mod::acpi::numa::value;
    if (mode == "unpublished") ker::mod::acpi::numa::published = false;
    if (mode == "zero-nodes") topology.node_count = 0;
    if (mode == "too-many-nodes") topology.node_count = mm::numa::MAX_NODES + 1;
    paging::PageTable* owned = nullptr;
    if (mode == "nonempty") {
        owned = static_cast<paging::PageTable*>(allocate(PhysicalPageOwner::PAGE_TABLE));
        frame(owned).kind = PageKind::PAGE_TABLE;
        frame(owned).owner = PhysicalPageOwner::PAGE_TABLE_POOL_RESERVE;
        std::memset(owned, 0x73, sizeof(*owned));
        virt::page_table_pool.pages[0] = owned;
        virt::page_table_pool.count = 1;
    }
    if (mode == "inflight") virt::page_table_pool.proof_inflight = 1;
    if (mode == "invalid-proof") virt::page_table_pool.proof_invalid = true;
    auto const enabled = virt::page_table_pool_enabled.load();
    auto const count = virt::page_table_pool.count;
    auto const sequence = virt::page_table_pool.proof_sequence;
    auto const inflight = virt::page_table_pool.proof_inflight;
    auto const invalid = virt::page_table_pool.proof_invalid;
    check(!virt::initialize_page_table_pool_after_topology(), "invalid boot initializer refuses");
    check(!virt::page_table_pool_topology_initialized && virt::page_table_pool_enabled.load() == enabled &&
              virt::page_table_pool.count == count && virt::page_table_pool.proof_sequence == sequence &&
              virt::page_table_pool.proof_inflight == inflight && virt::page_table_pool.proof_invalid == invalid,
          "failed initializer preserves every policy/pool/proof field");
    check(topology_reads == 1 && physical_calls == (owned ? 1U : 0U) && frees == 0,
          "failed initializer neither allocates nor frees nor repeats provider lookup");
    if (owned) {
        check(virt::page_table_pool.pages[0] == owned && frame(owned).owner == PhysicalPageOwner::PAGE_TABLE_POOL_RESERVE &&
                  reinterpret_cast<uint8_t*>(owned)[0] == 0x73,
              "failed nonempty initialization preserves exact owned bytes");
        // Cleanup only the deliberately injected invalid Host state. There is
        // no production reset, and this process never becomes a valid boot.
        virt::page_table_pool.count = 0;
        virt::page_table_pool.pages[0] = nullptr;
        phys::page_free(owned);
    }
}
void numa_lifecycle() {
    auto const baseline = physical_rows();
    auto const topology_before = topology_reads;
    auto const hits = virt::page_table_pool_alloc_hits.load();
    auto const releases = virt::page_table_pool_releases.load();
    auto const sequence = virt::page_table_pool.proof_sequence;
    for (unsigned node = 0; node < 2; ++node) {
        allocation_node = node;
        locality_node = 1 - node;  // deliberately unrelated to physical backing
        for (unsigned cycle = 0; cycle < 1000; ++cycle) {
            virt::SelftestRootFactoryOutput out{};
            auto* root = create(out);
            check(root && out.source == S::PHYSICAL_ALLOCATOR && frame(root).physical_node == node && out.captured_local == 1 - node &&
                      out.control_registered,
                  "NUMA factory follows explicit physical endpoint, not requested-locality fiction");
            auto const* bytes = reinterpret_cast<const uint8_t*>(root);
            for (size_t i = 0; i < sizeof(*root); ++i) check(bytes[i] == 0, "ordinary fallback zeros root before publication");
            std::memset(root, 0x5b, sizeof(*root));
            virt::SelftestRootReleaseOutput returned{};
            virt::selftest_release_pagemap(root, returned);
            check(physical_rows() == baseline && virt::metadata_live == 0,
                  "NUMA per-node free/kind/all-owner rows close after each root lifecycle");
            check(returned.owned_facts_valid && returned.control_retired && returned.references == 1 &&
                      returned.kind == PageKind::PAGE_TABLE && returned.disposition == R::PHYSICAL_RELEASED &&
                      !returned.insertion.inserted && returned.insertion.completed_sequence == 0,
                  "actual registered NUMA return reports physical release without cache authority");
            auto* table = virt::alloc_zeroed_page_table(true);
            check(table && frame(table).physical_node == node, "actual hierarchy/spare allocation uses explicit physical backing");
            std::memset(table, 0xa5, sizeof(*table));
            virt::SelftestRootPoolInsertion insertion{99, true};
            auto const rejects = virt::page_table_pool_rejects.load();
            check(!virt::try_release_page_table_to_pool(table, {}, &insertion) && !insertion.inserted &&
                      insertion.completed_sequence == 0 && virt::page_table_pool_rejects.load() == rejects + 1,
                  "disabled adoption leaves no insertion receipt");
            check(frame(table).owner == PhysicalPageOwner::PAGE_TABLE && frame(table).kind == PageKind::PAGE_TABLE &&
                      frame(table).refs == 1 && reinterpret_cast<uint8_t*>(table)[0] == 0xa5,
                  "disabled adoption preserves caller ownership and bytes before physical return");
            for (size_t i = 0; i < sizeof(*table); ++i)
                check(reinterpret_cast<uint8_t*>(table)[i] == 0xa5, "disabled adoption zeroes no caller-owned byte");
            phys::page_free(table);
            check(physical_rows() == baseline && virt::page_table_pool.count == 0 && virt::page_table_pool.proof_inflight == 0,
                  "NUMA hierarchy return restores exact physical baseline without retained cache");
        }
    }
    check(virt::page_table_pool_alloc_hits.load() == hits && virt::page_table_pool_releases.load() == releases &&
              virt::page_table_pool.proof_sequence == sequence && topology_reads == topology_before,
          "disabled fastpaths neither transfer cache ownership nor repeat topology lookup");
    virt::SelftestRootPoolInsertion insertion{77, true};
    auto const kind_before_null = kind_queries, ref_before_null = ref_queries;
    check(virt::try_release_page_table_to_pool(nullptr, {}, &insertion) && !insertion.inserted && insertion.completed_sequence == 0 &&
              kind_queries == kind_before_null && ref_queries == ref_before_null,
          "disabled policy retains null-release success without querying a nonexistent frame");
    auto* invalid = static_cast<paging::PageTable*>(allocate(PhysicalPageOwner::PAGE_TABLE));
    frame(invalid).kind = PageKind::PAGE_TABLE;
    frame(invalid).refs = 2;
    auto const kind_before_ref = kind_queries, ref_before_ref = ref_queries;
    check(!virt::try_release_page_table_to_pool(invalid, {}) && kind_queries == kind_before_ref + 1 && ref_queries == ref_before_ref + 1 &&
              frame(invalid).refs == 2,
          "disabled adoption still checks kind and exclusive ref before policy refusal");
    frame(invalid).kind = PageKind::NORMAL;
    frame(invalid).refs = 1;
    auto const kind_before_bad = kind_queries, ref_before_bad = ref_queries;
    check(!virt::try_release_page_table_to_pool(invalid, {}) && kind_queries == kind_before_bad + 1 && ref_queries == ref_before_bad,
          "invalid kind short-circuits ref query exactly as original guard");
    frame(invalid).kind = PageKind::PAGE_TABLE;
    phys::page_free(invalid);
    check(physical_rows() == baseline, "invalid adoption never transfers physical ownership");
    for (unsigned node = 0; node < 2; ++node) {
        allocation_node = locality_node = node;
        bad_owner(false);
        reset();
        bad_owner(true);
        reset();
        for (bool reserve : {false, true}) {
            if (reserve) reserve_seed(node);
            virt::serial_failure = true;
            virt::SelftestRootFactoryOptions options{};
            options.force_ordinary_failure = reserve;
            options.use_node_override = reserve;
            options.node_override = node;
            virt::SelftestRootFactoryOutput out{};
            check(create(out, options) == nullptr && !out.control_registered && out.rollback.owned_facts_valid &&
                      out.rollback.disposition == (reserve ? R::EMERGENCY_RETURNED : R::PHYSICAL_RELEASED),
                  "NUMA failed metadata publication rolls back exact ordinary or emergency origin");
            reset();
            check(physical_rows() == baseline, "rollback preserves both domain memberships");
        }
        rollback(false, true, false);
        reset();
        rollback(false, true, true);
        reset();
        check(physical_rows() == baseline, "metadata allocation failure preserves private physical conservation");
    }
    std::printf("PASS numa_node_lifecycle cycles_per_node=1000 nodes=2 all_owner_rows=%zu\n", mm::PHYSICAL_PAGE_OWNER_COUNT);
}

// WOS_TEST_INSERT_CONSERVATION_CASES
}  // namespace test
int main(int argc, char** argv) {
    try {
        std::string_view const mode = argc == 2 ? argv[1] : "uma";
        if (mode != "uma" && mode != "numa") {
            test::rejected_boot(mode);
            std::printf("PASS rejected_boot=%s assertions=%u\n", argv[1], test::assertions);
            return 0;
        }
        test::initialize_boot(mode);
        if (mode == "numa") {
            test::numa_lifecycle();
            test::reset();
            return 0;
        }
        test::physical_and_pool();
        test::reset();
        test::emergency();
        test::reset();
        test::exhaustion();
        test::reset();
        test::full_pool();
        test::reset();
        test::rollback(false, false, false);
        test::reset();
        test::rollback(true, false, false);
        test::reset();
        test::rollback(false, true, false);
        test::reset();
        test::rollback(false, true, true);
        test::reset();
        test::bad_owner(false);
        test::reset();
        test::bad_owner(true);
        test::reset();
        test::old_wrapper();
        test::reset();
        test::invalid_override();
        test::reset();
        test::conservation_cases();
        test::reset();
        std::printf("PASS actual_root_route_cases=12 assertions=%u diagnostic_bytes=%zu endpoints_disclosed=1\n", test::assertions,
                    sizeof(ker::mod::mm::virt::RootRouteDiagnostics));
        return 0;
    } catch (const std::exception& e) {
        std::fprintf(stderr, "FAIL %s\n", e.what());
    } catch (...) {
        std::fprintf(stderr, "FAIL unexpected invariant\n");
    }
    return 1;
}
