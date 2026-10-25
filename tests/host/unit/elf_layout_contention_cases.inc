// Host transport/metadata seams around the inserted, unchanged production bodies.
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <type_traits>
#include <utility>
#include <vector>
#include <platform/mm/paging.hpp>

namespace host {
inline unsigned checks{}, guard_depth{}, own_readers{}, queries{}, reads{};
inline unsigned acquired{}, released{}, cooperations{}, entropy_calls{};
inline uint64_t entropy_index{};
inline bool entropy_ok{true};
inline void* watched_readers{};
enum class Injection { None, Race, RaceCapacity, LateTeardown, SecondQueryBusy };
inline Injection injection{};
void after_cas(void*, bool);
void after_sub(void*);
void after_load(void*);
void enter_query();
[[noreturn]] void fail(const char* name) {
    std::fprintf(stderr, "ELF_LAYOUT_INVARIANT:%s\n", name);
    std::fflush(stderr);
    std::abort();
}
void check(bool condition, const char* name) {
    ++checks;
    if (!condition) fail(name);
}
template <typename T> class Atomic {
  public:
    std::atomic<T> value;
    constexpr Atomic(T initial = {}) : value(initial) {}
    auto load(std::memory_order order = std::memory_order_seq_cst) const -> T {
        auto result = value.load(order);
        after_load(const_cast<Atomic*>(this));
        return result;
    }
    void store(T next, std::memory_order order = std::memory_order_seq_cst) { value.store(next, order); }
    auto compare_exchange_weak(T& expected, T next, std::memory_order success, std::memory_order failure) -> bool {
        bool result = value.compare_exchange_weak(expected, next, success, failure);
        after_cas(this, result);
        return result;
    }
    auto fetch_sub(T count, std::memory_order order) -> T {
        auto result = value.fetch_sub(count, order);
        after_sub(this);
        return result;
    }
};
}  // namespace host

namespace ker::mod::sched {
struct PreemptGuard {
    PreemptGuard() { ++host::guard_depth; host::enter_query(); }
    ~PreemptGuard() {
        host::check(host::own_readers == 0, "reader-release");
        --host::guard_depth;
    }
};
}  // namespace ker::mod::sched

namespace ker::mod::mm::virt {
using paging::PageTable;
using paging::PageTableEntry;
using paging::PAGE_SIZE;
using vaddr_t = uint64_t;
// ACTUAL_GATE
auto entry_at(PageTable* table, size_t index) -> PageTableEntry& {
    host::check(index < 512, "table-index");
    ++host::reads;
    return table->entries[index];
}
auto pte_raw(const PageTableEntry& entry) -> uint64_t {
    uint64_t result;
    std::memcpy(&result, &entry, sizeof(result));
    return result;
}
namespace addr {
auto get_virt_pointer(uint64_t address) -> void* { return reinterpret_cast<void*>(address); }
}
// ACTUAL_ADMISSION
// ACTUAL_QUERY
}  // namespace ker::mod::mm::virt

namespace host {
namespace virt = ker::mod::mm::virt;
using ker::mod::mm::paging::PageTable;
inline PageTable* root{};
inline size_t stripe{};
void after_cas(void* address, bool success) {
    if (address != watched_readers || !success) return;
    ++acquired;
    ++own_readers;
    if (injection == Injection::Race || injection == Injection::RaceCapacity) {
        virt::user_pagemap_access_gates.at(stripe).teardown.store(true, std::memory_order_release);
    }
}
void after_sub(void* address) {
    if (address != watched_readers) return;
    check(own_readers != 0, "unowned-reader-release");
    --own_readers;
    ++released;
    if (injection == Injection::RaceCapacity) {
        virt::user_pagemap_access_gates.at(stripe).readers.store(UINT32_MAX, std::memory_order_release);
    }
}
void after_load(void* address) {
    if (address == watched_readers && injection == Injection::LateTeardown) {
        virt::user_pagemap_access_gates.at(stripe).teardown.store(true, std::memory_order_release);
    }
}
void enter_query() {
    ++queries;
    if (injection == Injection::SecondQueryBusy && queries == 2) {
        virt::user_pagemap_access_gates.at(stripe).teardown.store(true, std::memory_order_release);
    }
}
}  // namespace host

namespace ker::mod::random::entropy {
auto uniform_u64(uint64_t bound, uint64_t& result) -> bool {
    ++host::entropy_calls;
    if (!host::entropy_ok || !bound) return false;
    result = host::entropy_index % bound;
    return true;
}
}  // namespace ker::mod::random::entropy

namespace ker::mod::mm::user_layout {
// ACTUAL_LAYOUT_TYPES
// ACTUAL_LAYOUT
}  // namespace ker::mod::mm::user_layout

namespace ker::loader::elf {
using Elf64_Half = uint16_t;
// ACTUAL_COOPERATION
// ACTUAL_PROBE
struct LoadImageExtent { uint64_t min_vaddr{}, max_vaddr{}; };
struct Header { uint16_t e_type{}, e_phnum{}; };
struct ProgramHeader { uint32_t p_type{}; uint64_t p_align{}; };
struct ElfFile {
    Header elf_head{};
    uint64_t load_base{};
    LoadImageExtent extent{0, 4096};
    bool valid_extent{true};
    std::array<ProgramHeader, 2> headers{};
};
struct ElfLoadOptions {
    uint64_t base_address{};
    mod::mm::user_layout::ImageRole image_role{mod::mm::user_layout::ImageRole::MAIN};
};
constexpr uint16_t ET_EXEC = 2, ET_DYN = 3;
constexpr uint32_t PT_LOAD = 1;
// ELF metadata decoding is an endpoint here, not an alternate range algorithm.
auto load_image_extent(const ElfFile& image, LoadImageExtent& extent) -> bool {
    extent = image.extent;
    return image.valid_extent;
}
auto program_header_at(const ElfFile& image, uint16_t index) -> const ProgramHeader* { return &image.headers.at(index); }
// ACTUAL_IMAGE_SELECTION
}  // namespace ker::loader::elf

namespace host {
namespace layout = ker::mod::mm::user_layout;
namespace elf = ker::loader::elf;
using Status = virt::UserVirtualRangeStatus;
inline std::vector<PageTable*> tables;
auto table() -> PageTable* {
    auto* result = static_cast<PageTable*>(std::aligned_alloc(4096, sizeof(PageTable)));
    check(result != nullptr, "host-table-allocation");
    std::memset(result, 0, sizeof(PageTable));
    tables.push_back(result);
    return result;
}
auto raw(uint64_t value) -> ker::mod::mm::paging::PageTableEntry {
    ker::mod::mm::paging::PageTableEntry result;
    std::memcpy(&result, &value, sizeof(result));
    return result;
}
void put(PageTable* target, uint64_t address, uint64_t flags) {
    for (unsigned level = 4; level > 1; --level) {
        size_t index = (address >> (12 + 9 * (level - 1))) & 511;
        auto& entry = target->entries[index];
        if (!entry.present) entry = raw(reinterpret_cast<uint64_t>(table()) | 7);
        target = reinterpret_cast<PageTable*>(entry.frame << 12);
    }
    target->entries[(address >> 12) & 511] = raw(flags);
}
void reset(PageTable* next) {
    check(!guard_depth && !own_readers, "reset-boundary");
    root = next;
    stripe = virt::user_pagemap_access_gate_index(root);
    auto& gate = virt::user_pagemap_access_gates.at(stripe);
    gate.teardown.store(false);
    gate.readers.store(0);
    watched_readers = &gate.readers;
    queries = reads = acquired = released = cooperations = entropy_calls = 0;
    entropy_index = 0;
    entropy_ok = true;
    injection = Injection::None;
}
void ordinary() { check(!guard_depth && !own_readers, "cooperation-owner-boundary"); }
struct Cooperation {
    enum class Action { Clear, Leave, Occupy, Decline, Recursive } action{Action::Clear};
    elf::ElfLayoutCooperation* token{};
    uint64_t occupy_address{layout::MAIN_IMAGE_WINDOW.begin};
    std::vector<PageTable*> roots;
};
auto cooperate(void* opaque, PageTable* target) -> bool {
    ordinary();
    auto& value = *static_cast<Cooperation*>(opaque);
    value.roots.push_back(target);
    check(target == root, "cooperation-original-root");
    ++cooperations;
    if (value.action == Cooperation::Action::Recursive) {
        check(cooperations <= 2, "token-decrement-before-call");
        (void)value.token->try_cooperate(target);
        return true;
    }
    if (value.action == Cooperation::Action::Decline) return false;
    if (value.action != Cooperation::Action::Leave) {
        virt::user_pagemap_access_gates.at(stripe).teardown.store(false);
    }
    if (value.action == Cooperation::Action::Occupy) put(root, value.occupy_address, ker::mod::mm::paging::PAGE_RESERVED);
    return true;
}
struct Script {
    std::vector<Status> statuses;
    std::vector<std::pair<uint64_t, uint64_t>> ranges;
    size_t index{};
};
auto scripted(PageTable* target, uint64_t start, uint64_t size, void* opaque) -> Status {
    auto& value = *static_cast<Script*>(opaque);
    check(target == root, "selector-root");
    value.ranges.emplace_back(start, size);
    size_t index = std::min(value.index++, value.statuses.size() - 1);
    return value.statuses.at(index);
}
auto busy() -> virt::UserPagemapAccessGate& {
    auto& gate = virt::user_pagemap_access_gates.at(stripe);
    gate.teardown.store(true);
    return gate;
}
void query_cases() {
    auto* empty = table();
    reset(empty);
    constexpr uint64_t BASE = layout::MAIN_IMAGE_WINDOW.begin;
    check(virt::query_user_virtual_range(root, BASE, BASE + 4096) == Status::Free, "empty-free");
    check(reads == 1 && acquired == 1 && released == 1, "actual-bounded-walk");
    // A distinct, never-dereferenced root hashes to this same real gate.
    auto* foreign = reinterpret_cast<PageTable*>(reinterpret_cast<uintptr_t>(root) + (4096ULL << 12));
    check(foreign != root && virt::user_pagemap_access_gate_index(foreign) == stripe, "foreign-stripe");
    reset(empty);
    virt::user_pagemap_access_gates.at(virt::user_pagemap_access_gate_index(foreign)).teardown.store(true);
    check(virt::query_user_virtual_range(root, BASE, BASE + 4096) == Status::GateBusy, "foreign-busy");
    check(!reads && !acquired && !own_readers && !guard_depth, "busy-no-walk");
    check(!virt::is_user_virtual_range_free(root, BASE, BASE + 4096), "boolean-stays-fail-closed");
    reset(empty);
    injection = Injection::Race;
    check(virt::query_user_virtual_range(root, BASE, BASE + 4096) == Status::GateBusy, "raced-busy");
    check(acquired == 1 && released == 1 && !reads && !own_readers, "raced-reader-rollback");
    reset(empty);
    busy().readers.store(UINT32_MAX);
    check(virt::query_user_virtual_range(root, BASE, BASE + 4096) == Status::ReaderUnavailable, "capacity-terminal");
    reset(empty);
    virt::user_pagemap_access_gates.at(stripe).readers.store(UINT32_MAX);
    injection = Injection::LateTeardown;
    check(virt::query_user_virtual_range(root, BASE, BASE + 4096) == Status::ReaderUnavailable, "capacity-not-later-busy");
    reset(empty);
    injection = Injection::RaceCapacity;
    check(virt::query_user_virtual_range(root, BASE, BASE + 4096) == Status::ReaderUnavailable, "raced-capacity-terminal");
    check(acquired == 1 && released == 1 && !own_readers && !reads, "raced-capacity-rollback");
    reset(empty);
    for (auto [start, end] : std::array<std::pair<uint64_t, uint64_t>, 5>{{{BASE, BASE}, {BASE + 1, BASE + 4096},
             {BASE, BASE + 4097}, {BASE + 4096, BASE}, {0, 0x800000001000ULL}}}) {
        check(virt::query_user_virtual_range(root, start, end) == Status::Invalid, "invalid-bounds");
    }
    check(virt::query_user_virtual_range(nullptr, 0, 4096) == Status::Invalid && !queries, "invalid-null-root");
    auto* malformed = table();
    reset(malformed);
    malformed->entries[0] = raw(ker::mod::mm::paging::PAGE_PRESENT | (1ULL << 7));
    check(virt::query_user_virtual_range(root, BASE, BASE + 4096) == Status::Invalid, "invalid-walk");
    auto* occupied = table();
    reset(occupied);
    put(root, BASE, ker::mod::mm::paging::PAGE_RESERVED);
    check(virt::query_user_virtual_range(root, BASE, BASE + 4096) == Status::Occupied, "reserved-occupied");
    put(root, BASE + 4096, ker::mod::mm::paging::PAGE_PRESENT);
    check(virt::query_user_virtual_range(root, BASE + 4096, BASE + 8192) == Status::Occupied, "present-occupied");
}
void selector_cases() {
    reset(table());
    uint64_t base = 123;
    Script script{{Status::Occupied, Status::Free}};
    check(layout::choose_image_base_with_query(root, layout::ImageRole::MAIN, 0, 4096, base, scripted, &script), "selector-success");
    check(entropy_calls == 1, "entropy-once");
    check(script.ranges.size() == 2 && script.ranges[0].first == layout::MAIN_IMAGE_WINDOW.begin &&
              script.ranges[1].first == layout::MAIN_IMAGE_WINDOW.begin + layout::IMAGE_ALIGNMENT &&
              base == script.ranges[1].first, "occupied-only-cursor");
    for (auto status : {Status::GateBusy, Status::ReaderUnavailable, Status::Invalid}) {
        Script terminal{{status}};
        entropy_calls = 0;
        base = 123;
        check(!layout::choose_image_base_with_query(root, layout::ImageRole::MAIN, 0, 4096, base, scripted, &terminal), "selector-refuses-terminal");
        check(terminal.ranges.size() == 1 && entropy_calls == 1 && base == 123, "selector-terminal");
    }
    Script occupied{{Status::Occupied}};
    check(!layout::choose_image_base_with_query(root, layout::ImageRole::INTERPRETER, 0, 4096, base, scripted, &occupied), "occupied-exhausted");
    check(occupied.ranges.size() == 256, "genuine-collision-limit");
    Script bounded{{Status::Free}};
    unsigned previous = entropy_calls;
    check(!layout::choose_image_base_with_query(root, layout::ImageRole::MAIN, 1, 4096, base, scripted, &bounded) &&
              !bounded.index && entropy_calls == previous, "selector-invalid-before-entropy");
    entropy_ok = false;
    check(!layout::choose_image_base_with_query(root, layout::ImageRole::MAIN, 0, 4096, base, scripted, &bounded) && !bounded.index,
          "entropy-failure");
}
void probe_cases() {
    constexpr uint64_t BASE = layout::MAIN_IMAGE_WINDOW.begin;
    reset(table());
    busy();
    elf::ElfRangeProbe raw_probe;
    check(!raw_probe.range_is_free(root, BASE, 4096) && !cooperations, "default-no-cooperation");
    check(raw_probe.error() == -EAGAIN, "busy-eagain");
    reset(table());
    busy();
    Cooperation allowed;
    elf::ElfLayoutCooperation token(cooperate, &allowed);
    elf::ElfRangeProbe probe{&token};
    check(probe.range_is_free(root, BASE, 4096), "cooperation-resolves-foreign");
    check(queries == 2 && cooperations == 1 && allowed.roots.size() == 1, "same-range-rerun");
    ordinary();
    reset(table());
    busy();
    Cooperation occupation{Cooperation::Action::Occupy};
    elf::ElfLayoutCooperation occupy_token(cooperate, &occupation);
    elf::ElfRangeProbe occupied{&occupy_token};
    check(!occupied.range_is_free(root, BASE, 4096) && occupied.status == Status::Occupied && occupied.error() == 0,
          "cooperation-not-authority");
    reset(table());
    busy();
    Cooperation collision{Cooperation::Action::Occupy};
    elf::ElfLayoutCooperation collision_token(cooperate, &collision);
    elf::ElfRangeProbe collision_probe{&collision_token};
    uint64_t next_base = 0;
    check(layout::choose_image_base_with_query(root, layout::ImageRole::MAIN, 0, 4096, next_base,
              elf::ElfRangeProbe::query, &collision_probe), "cooperation-then-collision");
    check(next_base == BASE + layout::IMAGE_ALIGNMENT && entropy_calls == 1 && cooperations == 1 && queries == 3,
          "busy-same-candidate-occupied-advances");
    reset(table());
    busy();
    Cooperation permanent{Cooperation::Action::Leave};
    elf::ElfLayoutCooperation shared(cooperate, &permanent);
    elf::ElfRangeProbe main{&shared};
    check(!main.range_is_free(root, BASE, 4096), "permanent-busy-refusal");
    check(cooperations == 2 && queries == 3 && main.error() == -EAGAIN, "shared-budget");
    elf::ElfRangeProbe interpreter{&shared};
    check(!interpreter.range_is_free(root, layout::INTERPRETER_WINDOW.begin, 4096) && cooperations == 2 && queries == 4,
          "budget-shared-main-interpreter");
    reset(table());
    Cooperation two_images;
    elf::ElfLayoutCooperation image_token(cooperate, &two_images);
    elf::ElfRangeProbe main_image{&image_token}, interpreter_image{&image_token}, fixed_metadata{&image_token};
    busy();
    check(main_image.range_is_free(root, BASE, 4096), "main-shared-cooperation");
    busy();
    check(interpreter_image.range_is_free(root, layout::INTERPRETER_WINDOW.begin, 4096), "interpreter-shared-cooperation");
    busy();
    check(!fixed_metadata.range_is_free(root, 0x700000000000ULL, 4096) && fixed_metadata.error() == -EAGAIN &&
              cooperations == 2 && queries == 5, "successful-images-exhaust-shared-budget");
    reset(table());
    busy().readers.store(UINT32_MAX);
    Cooperation capacity;
    elf::ElfLayoutCooperation capacity_token(cooperate, &capacity);
    elf::ElfRangeProbe unavailable{&capacity_token};
    check(!unavailable.range_is_free(root, BASE, 4096) && !cooperations && unavailable.status == Status::ReaderUnavailable &&
              unavailable.error() == 0, "capacity-no-cooperation");
    reset(table());
    busy();
    Cooperation decline{Cooperation::Action::Decline};
    elf::ElfLayoutCooperation decline_token(cooperate, &decline);
    elf::ElfRangeProbe refused{&decline_token};
    check(!refused.range_is_free(root, BASE, 4096) && queries == 1 && cooperations == 1 && refused.error() == -EAGAIN,
          "declined-context");
    reset(table());
    Cooperation recursive{Cooperation::Action::Recursive};
    elf::ElfLayoutCooperation recursive_token(cooperate, &recursive);
    recursive.token = &recursive_token;
    check(recursive_token.try_cooperate(root) && cooperations == 2 && !recursive_token.try_cooperate(root), "reentrant-budget");
    static_assert(!std::is_copy_constructible_v<elf::ElfLayoutCooperation> && !std::is_move_constructible_v<elf::ElfLayoutCooperation>);
    elf::ElfLayoutCooperation absent(nullptr, nullptr);
    check(!absent.try_cooperate(root), "missing-authority");
}
void image_selection_cases() {
    reset(table());
    injection = Injection::SecondQueryBusy;
    Cooperation context;
    elf::ElfLayoutCooperation token(cooperate, &context);
    elf::ElfRangeProbe probe{&token};
    elf::ElfFile image;
    image.elf_head.e_type = elf::ET_DYN;
    elf::ElfLoadOptions options;
    check(elf::select_image_load_base(image, root, options, probe), "second-selected-validation");
    check(queries == 3 && entropy_calls == 1 && cooperations == 1 && image.load_base == layout::MAIN_IMAGE_WINDOW.begin,
          "selected-rerun-same-base");
    reset(table());
    busy();
    elf::ElfFile fixed;
    fixed.elf_head.e_type = elf::ET_EXEC;
    fixed.extent = {0x400000, 0x401000};
    elf::ElfRangeProbe no_authority;
    check(!elf::select_image_load_base(fixed, root, options, no_authority) && no_authority.error() == -EAGAIN && !entropy_calls,
          "fixed-exec-busy");
    reset(table());
    put(root, 0x400000, ker::mod::mm::paging::PAGE_RESERVED);
    elf::ElfRangeProbe reserved;
    check(!elf::select_image_load_base(fixed, root, options, reserved) && reserved.error() == 0, "fixed-exec-occupied");
    reset(table());
    elf::ElfRangeProbe invalid;
    check(!invalid.range_is_free(root, UINT64_MAX - 4095, 8192) && invalid.status == Status::Invalid && !queries,
          "probe-overflow");
}
}  // namespace host

int main() {
    host::query_cases();
    host::selector_cases();
    host::probe_cases();
    host::image_selection_cases();
    host::ordinary();
    for (auto* table : host::tables) std::free(table);
    std::printf("ELF_LAYOUT_CONTENTION_PASS checks=%u\n", host::checks);
}
