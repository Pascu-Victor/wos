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
    check(preflights == locked_points && locked_points > 30, "every complete typed scan has exactly one all-lock point");
}
}  // namespace fixture

int main() {
    fixture::tests();
    std::printf("PASS actual paired collector/scanner/MM/predicates; cases=%zu checks=%zu scans=%zu\n", fixture::cases, fixture::checks,
                fixture::locked_points);
}
