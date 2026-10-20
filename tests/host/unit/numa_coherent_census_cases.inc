#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <memory>
#include <new>
#include <platform/acpi/numa_boot.hpp>
#include <platform/mm/numa_balance_scan.hpp>
#include <span>
#include <test/numa_lifecycle_census.hpp>

namespace mm = ker::mod::mm;
namespace nb = mm::numa::balance;
namespace nl = ker::test::numa_lifecycle;
namespace debt = nl::mm_debt;
namespace topology = ker::mod::acpi::numa;

namespace fixture {
size_t checks{}, cases{}, capture_calls{}, preliminary_calls{}, preflights{}, locked_points{};
mm::paging::PageZone* zones{};
mm::paging::PageZone* huge{};
topology::Topology* domains{};
uint64_t hhdm{};
bool have_topology{true};
enum class Mutation { NONE, RETURNED_PIN, ACTIVE_INTENT, RETAINED_DEBT, REGISTRY_ABA };
Mutation mutation{};
nl::MmDebtSnapshot* output_address{};
nl::MmDebtCaptureScratch* scratch_address{};
void check(bool value, const char* message) {
    ++checks;
    if (!value) {
        std::fprintf(stderr, "FAIL: %s\n", message);
        std::abort();
    }
}
auto held_count() -> size_t {
    size_t result{};
    for (auto* zone = zones; zone; zone = zone->next) result += zone->allocator->lock_held.load();
    return result;
}
void capture_spy(nl::MmDebtSnapshot& out, nl::MmDebtCaptureScratch& scratch, bool call_provider = true) noexcept;
}  // namespace fixture

namespace ker::mod::mm::addr {
auto get_hhdm_offset() -> uint64_t { return fixture::hhdm; }
}  // namespace ker::mod::mm::addr
namespace ker::mod::acpi::numa {
auto boot_topology() noexcept -> const Topology* { return fixture::have_topology ? fixture::domains : nullptr; }
}  // namespace ker::mod::acpi::numa
namespace ker::mod::mm::phys {
auto get_zones() -> paging::PageZone* { return fixture::zones; }
auto get_huge_page_zone() -> paging::PageZone* { return fixture::huge; }
void get_physical_balance_snapshot(PhysicalBalanceSnapshot& out) {
    fixture::check(fixture::held_count() == 0, "preliminary immutable reserves outside allocator locks");
    ++fixture::preliminary_calls;
    out = {};
    out.per_cpu_page_cache_reserve_pages = 7;
    out.per_cpu_page_cache_reserve_objects = 1;
    out.firmware_reserves[0] = {3, 1};
}
}  // namespace ker::mod::mm::phys
namespace ker::mod::mm::virt {
// Direct-map reachability only; the real Host backing is supplied below.
auto selftest_direct_map_contains(const void*) -> bool { return true; }
}  // namespace ker::mod::mm::virt

/* @COUNTERS@ */
/* @COLLECTORS@ */
/* @PREDICATES@ */

namespace fixture {
void capture_spy(nl::MmDebtSnapshot& out, nl::MmDebtCaptureScratch& scratch, bool call_provider) noexcept {
    bool const PREFLIGHT = capture_calls++ % 2 == 0;
    if (PREFLIGHT) {
        check(held_count() == 0, "ignored preflight must run before every physical lock");
        output_address = &out;
        scratch_address = &scratch;
        ++preflights;
    } else {
        check(held_count() == 2, "authoritative capture must hold every allocator lock");
        check(&out == output_address && &scratch == scratch_address, "same preaddressed provider input/output storage");
        ++locked_points;
    }
    if (call_provider) debt::snapshot_addressable(out, scratch);
    if (PREFLIGHT) {
        switch (mutation) {
            case Mutation::RETURNED_PIN:
                debt::add(nl::CounterField::USER_PAGE_PINS);
                debt::drop(nl::CounterField::USER_PAGE_PINS);
                break;
            case Mutation::ACTIVE_INTENT:
                debt::begin_change();
                break;
            case Mutation::RETAINED_DEBT:
                debt::add(nl::CounterField::PIN_RETURNS);
                break;
            case Mutation::REGISTRY_ABA:
                debt::begin_change();
                debt::add(nl::CounterField::LEGACY_FILE_REGISTRY);
                debt::drop(nl::CounterField::LEGACY_FILE_REGISTRY);
                debt::end_change();
                break;
            case Mutation::NONE:
                break;
        }
    }
}

constexpr uint64_t PAGE = mm::paging::PAGE_SIZE;
constexpr uint64_t ZONE_BYTES = PAGE * 256;
constexpr uint64_t STRIDE = ZONE_BYTES + PAGE;
constexpr uint64_t PHYSICAL = 0x40000000;
struct Release {
    void operator()(void* memory) const { std::free(memory); }
};
struct Arena {
    std::unique_ptr<void, Release> memory{std::aligned_alloc(PAGE, 2 * STRIDE)};
    std::unique_ptr<topology::Topology> topology{std::make_unique<topology::Topology>()};
    mm::paging::PageZone* zone[2]{};
    Arena() {
        check(memory != nullptr, "owned real allocator backing");
        std::memset(memory.get(), 0, 2 * STRIDE);
        hhdm = reinterpret_cast<uint64_t>(memory.get()) - PHYSICAL;
        for (size_t i = 0; i < 2; ++i) {
            auto* base = static_cast<std::byte*>(memory.get()) + i * STRIDE;
            zone[i] = ::new (base) mm::paging::PageZone{};
            auto* allocator = ::new (base + PAGE) mm::PageAllocator{};
            allocator->init(reinterpret_cast<uint64_t>(allocator), ZONE_BYTES);
            zone[i]->allocator = allocator;
            zone[i]->start = allocator->base;
            zone[i]->len = static_cast<uint64_t>(allocator->total_pages) * PAGE;
            zone[i]->zone_num = i;
            topology->memory[i] = {PHYSICAL + i * STRIDE, STRIDE, static_cast<uint16_t>(i), 0};
            topology->nodes[i].usable_bytes = STRIDE;
        }
        zone[0]->next = zone[1];
        topology->node_count = 2;
        topology->memory_range_count = 2;
        topology->provenance = topology::Provenance::SRAT_SLIT;
        zones = zone[0];
        huge = zone[1];  // Existing regular zone must not be locked/scanned twice.
        domains = topology.get();
    }
    ~Arena() { check(held_count() == 0, "all actual locks released before backing retirement"); }
};
struct Point {
    std::unique_ptr<nl::RegistrySnapshot> registry{std::make_unique<nl::RegistrySnapshot>()};
    std::unique_ptr<nb::Snapshot> physical{std::make_unique<nb::Snapshot>()};
    std::unique_ptr<nb::Workspace> work{std::make_unique<nb::Workspace>()};
    nl::MmDebtSnapshot mm{};
    nl::MmDebtCaptureScratch capture{};
    void registry_copy() {
        // Supplied registry copy endpoint. Actual registry copying/mutex logic
        // remains covered by the existing complete handler/registry fixture.
        nl::MmDebtSnapshot value{};
        debt::snapshot(value);
        registry->stamp = {value.version_end, value.inflight, value.valid != 0 && value.version_begin == value.version_end};
        registry->flags = nl::REGISTRY_COMPLETE;
        registry->legacy_rows = value.legacy_file_registry_rows;
        registry->native_rows = value.native_file_registry_rows;
        registry->row_count = registry->legacy_rows + registry->native_rows;
    }
    void scan(bool prepare_registry = true) {
        if (prepare_registry) registry_copy();
        nl::prepare_mm_capture(mm, capture);
        capture_calls = 0;
        auto const result = nb::selftest_snapshot_with_mm(*physical, *work, mm, capture, 2 * (256 + 1));
        check(result.valid() && physical->complete, "both unchanged full physical scans complete");
        check(physical->scanned_frames == 2 * (256 + 1), "every actual managed frame visited");
        check(physical->zone_count == 2 && held_count() == 0, "duplicate huge-zone elision and reverse unlock");
        check(capture_calls == 2, "exactly ignored plus authoritative MM capture");
    }
    auto coherent() const -> bool { return nl::coherent_registry_mm(*registry, mm); }
};

void physical_ownership_mutations(const nb::Snapshot& baseline) {
    static_assert(mm::PHYSICAL_PAGE_OWNER_COUNT == 41);
    auto changed = std::make_unique<nb::Snapshot>();
    auto refuse = [&] {
        check(!nl::same_physical_ownership(baseline, *changed) && !nl::same_physical_ownership(*changed, baseline),
              "every exact ownership quantity refuses in both operand orders");
        check(!nl::same_physical(baseline, *changed), "adjacent pair retains every ownership refusal");
    };
    for (size_t owner = 0; owner < mm::PHYSICAL_PAGE_OWNER_COUNT; ++owner) {
        for (size_t scope = 0; scope < 2 + baseline.node_count; ++scope) {
            for (bool objects : {false, true}) {
                *changed = baseline;
                auto& row = scope == 0   ? changed->global.owners[owner]
                            : scope == 1 ? changed->scanned.owners[owner]
                                         : changed->nodes[scope - 2].owners[owner];
                if (objects)
                    ++row.objects;
                else
                    ++row.pages;
                refuse();  // Includes INVALID; neither total nor one owner may hide another.
            }
        }
    }
    for (size_t scope = 0; scope <= baseline.node_count; ++scope) {
        auto counts = [&](nb::Snapshot& value) -> nb::Counts& { return scope == 0 ? value.scanned : value.nodes[scope - 1]; };
        for (auto member : {&nb::Counts::managed_pages, &nb::Counts::free_pages, &nb::Counts::allocator_metadata_pages,
                            &nb::Counts::zone_descriptor_pages, &nb::Counts::owner_pages}) {
            *changed = baseline;
            ++(counts(*changed).*member);
            refuse();
        }
        for (size_t kind = 0; kind < nb::KIND_COUNT; ++kind) {
            *changed = baseline;
            ++counts(*changed).kind_pages[kind];
            refuse();
        }
        *changed = baseline;
        auto& invalid = counts(*changed);
        invalid.cached_free_pages = invalid.free_pages + 1;
        refuse();
        check(!nl::same_physical_ownership_counts(invalid, invalid) && !nl::same_physical_counts(invalid, invalid),
              "identical impossible cached subsets cannot qualify either comparator");
    }
    auto global_change = [&](auto member) {
        *changed = baseline;
        ++(changed->global.*member);
        refuse();
    };
    global_change(&mm::phys::PhysicalBalanceSnapshot::managed_pages);
    global_change(&mm::phys::PhysicalBalanceSnapshot::free_pages);
    global_change(&mm::phys::PhysicalBalanceSnapshot::allocator_metadata_pages);
    global_change(&mm::phys::PhysicalBalanceSnapshot::zone_descriptor_pages);
    global_change(&mm::phys::PhysicalBalanceSnapshot::per_cpu_page_cache_reserve_pages);
    global_change(&mm::phys::PhysicalBalanceSnapshot::per_cpu_page_cache_reserve_objects);
    global_change(&mm::phys::PhysicalBalanceSnapshot::owner_pages);
    global_change(&mm::phys::PhysicalBalanceSnapshot::identity_pages);
    global_change(&mm::phys::PhysicalBalanceSnapshot::identity_mismatch_pages);
    global_change(&mm::phys::PhysicalBalanceSnapshot::untracked_unreclaimable_pages);
    global_change(&mm::phys::PhysicalBalanceSnapshot::zone_count);
    for (size_t reserve = 0; reserve < mm::phys::PHYSICAL_RESERVE_KIND_COUNT; ++reserve) {
        *changed = baseline;
        ++changed->global.firmware_reserves[reserve].pages;
        refuse();
        *changed = baseline;
        ++changed->global.firmware_reserves[reserve].objects;
        refuse();
    }
    *changed = baseline;
    changed->complete = false;
    refuse();
    *changed = baseline;
    changed->result.error = nb::Error::COUNTERS;
    refuse();
    *changed = baseline;
    changed->node_count = 0;
    refuse();
    *changed = baseline;
    --changed->node_count;
    refuse();
    *changed = baseline;
    ++changed->zone_count;
    refuse();
    *changed = baseline;
    ++changed->scanned_frames;
    refuse();
    *changed = baseline;
    changed->provenance = topology::Provenance::UMA_MISSING_SRAT;
    refuse();
}

void physical_free_partition_cases(Arena& arena) {
    ++cases;
    Point original, baseline, moved, stable;
    original.scan();
    constexpr size_t CACHED = 28;
    std::array<std::array<void*, CACHED>, 2> guards{};
    std::array<std::array<uint32_t, CACHED>, 2> indices{};
    for (size_t node = 0; node < 2; ++node) {
        auto* allocator = arena.zone[node]->allocator;
        auto const flags = allocator->lock_irq();
        for (size_t i = 0; i < CACHED; ++i) {
            auto* pair = allocator->alloc(2 * PAGE, mm::PhysicalPageOwner::SELFTEST);
            check(pair != nullptr && allocator->split_allocated_block_to_order0(pair), "real two-page allocation split into owners");
            guards[node][i] = static_cast<std::byte*>(pair) + PAGE;
            check(allocator->cache_allocated_order0(pair, indices[node][i]), "real returned frame enters free cache");
        }
        allocator->unlock_irq(flags);
    }
    baseline.scan();
    stable.scan();
    check(nl::same_physical(*baseline.physical, *stable.physical), "real cached baseline has exact adjacent pair");
    check(baseline.physical->scanned.cached_free_pages == 2 * CACHED, "both actual node caches are scanned");
    for (size_t node = 0; node < 2; ++node) {
        check(baseline.physical->nodes[node].cached_free_pages == CACHED, "each active node begins with28 cached free frames");
        auto* allocator = arena.zone[node]->allocator;
        auto const flags = allocator->lock_irq();
        for (size_t i = 0; i < 3; ++i)
            check(allocator->release_cached_order0_at(indices[node][i]) == PAGE, "actual cache-to-buddy transfer");
        allocator->unlock_irq(flags);
    }
    moved.scan();
    stable.scan();
    check(nl::same_physical(*moved.physical, *stable.physical), "cache-to-buddy endpoint retains exact adjacent stability");
    check(nl::same_physical_ownership(*baseline.physical, *moved.physical) &&
              nl::same_physical_ownership(*moved.physical, *baseline.physical),
          "real free partition movement preserves exact baseline ownership");
    check(!nl::same_physical(*baseline.physical, *moved.physical), "adjacent samples still reject real cached partition churn");
    check(moved.physical->global.free_pages == baseline.physical->global.free_pages &&
              moved.physical->scanned.free_pages == baseline.physical->scanned.free_pages &&
              moved.physical->scanned.cached_free_pages + 6 == baseline.physical->scanned.cached_free_pages,
          "six actual transfers change no global or scanned inclusive free capacity");
    for (size_t node = 0; node < 2; ++node) {
        auto const& before = baseline.physical->nodes[node];
        auto const& after = moved.physical->nodes[node];
        check(before.cached_free_pages == 28 && after.cached_free_pages == 25 && before.free_pages == after.free_pages &&
                  after.free_pages - after.cached_free_pages == before.free_pages - before.cached_free_pages + 3,
              "actual node28-to25 cached change is exactly three already-free buddy pages");
        auto* allocator = arena.zone[node]->allocator;
        auto const flags = allocator->lock_irq();
        for (size_t i = 0; i < 3; ++i)
            check(allocator->claim_free_order0_for_cache(indices[node][i]) != nullptr, "actual buddy-to-cache transfer");
        allocator->unlock_irq(flags);
    }
    stable.scan();
    check(nl::same_physical_ownership(*moved.physical, *stable.physical) && !nl::same_physical(*moved.physical, *stable.physical) &&
              nl::same_physical(*baseline.physical, *stable.physical),
          "reverse real transfer restores exact cache partition without changing ownership");
    for (size_t scope = 0; scope < 2 + baseline.physical->node_count; ++scope) {
        auto const& before = scope == 0   ? baseline.physical->global.owners
                             : scope == 1 ? baseline.physical->scanned.owners
                                          : baseline.physical->nodes[scope - 2].owners;
        auto const& after = scope == 0   ? moved.physical->global.owners
                            : scope == 1 ? moved.physical->scanned.owners
                                         : moved.physical->nodes[scope - 2].owners;
        for (size_t owner = 0; owner < mm::PHYSICAL_PAGE_OWNER_COUNT; ++owner)
            check(before[owner].pages == after[owner].pages && before[owner].objects == after[owner].objects,
                  "all41 actual global/scanned/per-node owner pages and objects stay exact");
    }
    physical_ownership_mutations(*baseline.physical);

    auto* allocator = arena.zone[0]->allocator;
    auto flags = allocator->lock_irq();
    auto* leak = allocator->alloc(PAGE, mm::PhysicalPageOwner::SELFTEST);
    allocator->unlock_irq(flags);
    check(leak != nullptr, "real retained frame for ownership refusal");
    stable.scan();
    check(!nl::same_physical_ownership(*baseline.physical, *stable.physical), "one real retained page still refuses baseline closure");
    flags = allocator->lock_irq();
    check(allocator->free(leak) == PAGE, "real retained frame returned");
    allocator->unlock_irq(flags);

    Point corrupt;
    auto refuse_scan = [&](nb::Error expected) {
        auto const result = nb::snapshot(*corrupt.physical, *corrupt.work, 2 * (256 + 1));
        check(result.error == expected && !corrupt.physical->complete, "actual scanner rejects corrupt cache metadata or counter");
    };
    flags = allocator->lock_irq();
    ++allocator->cached_order0_count;
    allocator->unlock_irq(flags);
    refuse_scan(nb::Error::COUNTERS);
    flags = allocator->lock_irq();
    --allocator->cached_order0_count;
    auto const cached_index = indices[0][0];
    allocator->page_owners[cached_index] = static_cast<uint8_t>(mm::PhysicalPageOwner::SELFTEST);
    allocator->unlock_irq(flags);
    refuse_scan(nb::Error::METADATA);
    flags = allocator->lock_irq();
    allocator->page_owners[cached_index] = static_cast<uint8_t>(mm::PhysicalPageOwner::INVALID);
    allocator->unlock_irq(flags);
    stable.scan();
    check(nl::same_physical(*baseline.physical, *stable.physical), "cache corruption is restored without concealing ownership debt");

    for (size_t node = 0; node < 2; ++node) {
        allocator = arena.zone[node]->allocator;
        flags = allocator->lock_irq();
        for (size_t i = 0; i < CACHED; ++i)
            check(allocator->release_cached_order0_at(indices[node][i]) == PAGE, "all actual free-cache entries returned");
        for (auto* guard : guards[node]) check(allocator->free(guard) == PAGE, "every actual buddy guard owner returned");
        allocator->unlock_irq(flags);
    }
    stable.scan();
    check(nl::same_physical(*original.physical, *stable.physical), "all real test owners and free partitions return exactly");
}

void tests() {
    Arena arena;
    Point a, b;
    ++cases;
    a.scan();
    b.scan();
    check(a.coherent() && b.coherent(), "two individually coherent points");
    check(nl::same_mm_debt(a.mm, b.mm) && nl::same_registry_rows(*a.registry, *b.registry) && nl::same_physical(*a.physical, *b.physical),
          "all exact original pair quantities remain equal");
    auto legacy = std::make_unique<nb::Snapshot>();
    auto work = std::make_unique<nb::Workspace>();
    check(nb::snapshot(*legacy, *work, 2 * (256 + 1)).valid(), "legacy actual collector still works");
    check(nl::same_physical(*legacy, *a.physical), "typed pair retains full legacy physical semantics");

    ++cases;
    for (size_t i = 0; i < 19; ++i) {
        debt::add(nl::CounterField::USER_PAGE_PINS);
        debt::drop(nl::CounterField::USER_PAGE_PINS);
    }
    b.scan();
    check(a.mm.version_end != b.mm.version_begin && a.coherent() && b.coherent(),
          "returned inter-point lifetimes have separate valid joins");
    check(nl::same_mm_debt(a.mm, b.mm) && nl::same_registry_rows(*a.registry, *b.registry) && nl::same_physical(*a.physical, *b.physical),
          "fully returned events preserve every actual endpoint quantity");

    ++cases;
    mutation = Mutation::RETURNED_PIN;
    b.scan();
    check(!b.coherent(), "mutation after ignored preflight must invalidate the short join");
    check(nl::same_mm_debt(a.mm, b.mm), "short join catches zero-net event independently of endpoint quantities");

    ++cases;
    mutation = Mutation::ACTIVE_INTENT;
    b.scan();
    check(!b.coherent() && !nl::mm_complete(b.mm), "active writer must refuse the coherent point");
    check(b.mm.inflight != 0 && b.physical->complete, "intent blocked on physical locks never makes collector wait or skip scan");
    debt::end_change();

    ++cases;
    mutation = Mutation::REGISTRY_ABA;
    b.scan();
    check(!b.coherent() && b.mm.legacy_file_registry_rows == 0, "same-count registry ABA must refuse exact version join");
    mutation = Mutation::NONE;

    ++cases;
    debt::add(nl::CounterField::PIN_RETURNS);
    b.scan();
    check(b.coherent() && !nl::same_mm_debt(a.mm, b.mm), "retained debt must fail exact endpoint equality");
    check(!nl::mm_settled(b.mm, *b.registry), "unlinked return tail remains unsettled");
    debt::drop(nl::CounterField::PIN_RETURNS);

    ++cases;
    for (size_t i = 0; i < 23; ++i) {
        debt::add(static_cast<nl::CounterField>(i));
        b.scan();
        check(!nl::same_mm_debt(a.mm, b.mm), "every one of 23 quantities remains compared");
        debt::drop(static_cast<nl::CounterField>(i));
    }

    ++cases;
    auto changed_counts = a.physical->scanned;
    ++changed_counts.free_pages;
    check(!nl::same_physical_counts(a.physical->scanned, changed_counts), "actual changed physical counts must fail equality");
    auto const flags = arena.zone[0]->allocator->lock_irq();
    auto* allocation = arena.zone[0]->allocator->alloc(PAGE, mm::PhysicalPageOwner::SELFTEST);
    arena.zone[0]->allocator->unlock_irq(flags);
    check(allocation != nullptr, "real allocated page");
    b.scan();
    check(b.coherent() && !nl::same_physical(*a.physical, *b.physical), "one persistent physical page must fail exact pair comparison");
    check(!nl::same_physical_counts(a.physical->scanned, b.physical->scanned), "real allocator count/owner changes remain exact");
    auto const release = arena.zone[0]->allocator->lock_irq();
    check(arena.zone[0]->allocator->free(allocation) == PAGE, "actual physical owner returns exactly one page");
    arena.zone[0]->allocator->unlock_irq(release);
    b.scan();
    check(nl::same_physical(*a.physical, *b.physical), "real allocator return restores every physical field");

    ++cases;
    auto invalid = b.mm;
    invalid.valid = 0;
    check(!nl::coherent_registry_mm(*b.registry, invalid), "invalid arithmetic refuses");
    invalid = b.mm;
    ++invalid.version_end;
    check(!nl::coherent_registry_mm(*b.registry, invalid), "mismatched before/after versions refuse");
    for (auto version : {uint64_t{0}, UINT64_MAX}) {
        auto stamp = b.registry->stamp;
        b.registry->stamp.version = version;
        invalid = b.mm;
        invalid.version_begin = invalid.version_end = version;
        // Actual named provider refuses zero/sentinel; registry quiescence
        // also rejects those stamps before a supplied value can join.
        check(!nl::coherent_registry_mm(*b.registry, invalid), "zero and exhausted registry version refuse");
        b.registry->stamp = stamp;
    }

    ++cases;
    debt::CounterState state;
    nl::MmDebtSnapshot old{}, named{};
    nl::MmDebtCaptureScratch scratch{};
    for (size_t i = 0; i < 23; ++i) {
        debt::adjust(state, static_cast<nl::CounterField>(i), true);
        debt::collect(state, old);
        debt::collect_addressable(state, named, scratch);
        check(std::memcmp(&old, &named, sizeof(old)) == 0, "all actual named loads equal legacy provider");
        debt::adjust(state, static_cast<nl::CounterField>(i), false);
    }
    state.arithmetic_valid.store(false);
    debt::collect_addressable(state, named, scratch);
    check(!named.valid, "named provider retains sticky arithmetic refusal");

    ++cases;
    for (auto version : {uint64_t{0}, UINT64_MAX}) {
        debt::CounterState boundary;
        boundary.epoch.version_.store(version);
        debt::collect_addressable(boundary, named, scratch);
        check(!named.valid && named.version_begin == version && named.version_end == version,
              "actual named provider refuses zero and exhausted versions");
    }
    debt::CounterState active;
    active.epoch.begin();
    debt::collect_addressable(active, named, scratch);
    check(named.valid && named.inflight == 1 && !nl::mm_complete(named), "actual named provider exposes preannounced intent");
    active.epoch.end();
    debt::collect_addressable(active, named, scratch);
    check(nl::mm_complete(named), "actual intent tail retires to complete sample");

    ++cases;
    b.scan();
    b.registry->legacy_rows = b.registry->row_count = 1;
    a.registry->legacy_rows = a.registry->row_count = 1;
    a.registry->rows[0] = b.registry->rows[0] = {7, 9, 0, 0x10000, 0x12000, 0, nl::LEGACY_ROW, 0};
    check(nl::same_registry_rows(*a.registry, *b.registry), "equal supplied registry rows");
    ++b.registry->rows[0].file_offset;
    check(!nl::same_registry_rows(*a.registry, *b.registry), "same-count registry geometry remains exact");
    auto changed_physical = std::make_unique<nb::Snapshot>(*a.physical);
    ++changed_physical->nodes[0].cached_free_pages;
    check(!nl::same_physical(*a.physical, *changed_physical), "node cached-free quantity remains exact");
    *changed_physical = *a.physical;
    ++changed_physical->global.identity_mismatch_pages;
    check(!nl::same_physical(*a.physical, *changed_physical), "physical ownership identity mismatch remains fatal");
    *changed_physical = *a.physical;
    ++changed_physical->global.owners[static_cast<size_t>(mm::PhysicalPageOwner::SELFTEST)].pages;
    check(!nl::same_physical(*a.physical, *changed_physical), "global owner totals remain exact");
    physical_free_partition_cases(arena);
    check(preflights == locked_points && locked_points > 30, "every complete typed scan has exactly one all-lock point");
}
}  // namespace fixture

int main() {
    fixture::tests();
    std::printf("PASS actual paired collector/scanner/MM/predicates; cases=%zu checks=%zu scans=%zu\n", fixture::cases, fixture::checks,
                fixture::locked_points);
}
