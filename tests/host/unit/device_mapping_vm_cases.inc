#include <platform/mm/device_mapping_revoke.hpp>

namespace vm_fixture {
namespace vm = ker::mod::mm::device_vm;
namespace fm = ker::mod::mm::file_mapping;
namespace virt = ker::mod::mm::virt;
namespace phys = ker::mod::mm::phys;
namespace paging = ker::mod::mm::paging;
namespace mapping = ker::dev::mapping;
using Pub = ker::syscall::vmem::SharedVmemPublicationGuard;
using Prep = ker::mod::sys::SyscallPreparationScope;
using Task = ker::mod::sched::task::Task;
using Root = paging::PageTable;
constexpr uint64_t PAGE = 4096, BASE = 0x1ff000, RW = 3;

auto leaf(Root* root, uint64_t address) -> paging::PageTableEntry* {
    for (int level = 4; level > 1; --level) {
        auto const& e = root->entries.at(static_cast<size_t>(virt::index_of(address, level)));
        if (!e.present || e.pagesize) return nullptr;
        root = reinterpret_cast<Root*>(static_cast<uint64_t>(e.frame) << 12);
    }
    return &root->entries.at(static_cast<size_t>(virt::index_of(address, 1)));
}
auto raw(Root* root, uint64_t address) -> uint64_t {
    auto* entry = leaf(root, address);
    return entry != nullptr ? virt::pte_raw(*entry) : 0;
}
void free_empty_tables(Root* root, unsigned level = 4) {
    for (size_t i = 0; i < (level == 4 ? 256U : 512U); ++i) {
        auto& e = root->entries[i];
        if (!e.present) continue;
        require(level > 1 && !e.pagesize, "Host storage teardown sees no surviving leaf");
        auto* child = reinterpret_cast<Root*>(static_cast<uint64_t>(e.frame) << 12);
        free_empty_tables(child, level - 1);
        release_table(child);
        e = {};
    }
}
struct World {
    ker::vfs::File file;
    mapping::QuotaAccountRef quota;
    mapping::ObjectRef object;
    Task actor, second;
    World(uint64_t pages = 140, uint64_t rights = RW) {
        require(table_pages() == 0 && object_fixture::live_pages() == 0 && object_fixture::reserved() == 0,
                "previous finite physical/callback owners drained");
        require(mapping::QuotaAccount::create({pages, 1, pages}, quota) == 0, "actual resident quota");
        require(mapping::Object::create(quota, pages * PAGE, rights, object) == 0, "actual initialized WB Object");
        actor.pagemap = allocate_table();
        second.pagemap = allocate_table();
        require(actor.pagemap && second.pagemap, "finite root storage allocation");
        virt::controls[0] = {actor.pagemap, 17, 0};
        virt::controls[1] = {second.pagemap, 23, 0};
        current = &actor;
    }
    ~World() {
        fail_table_after = -1;
        object_fixture::fail_pin = false;
        actor.lazy_vmem_ranges.clear();
        current = &actor;
        int result = -EAGAIN;
        for (unsigned i = 0; i < 16 && result == -EAGAIN; ++i) {
            result = vm::revoke_cpu_mappings(object);
            object_fixture::drain();
        }
        require(result == 0, "actual revoke facade completes finite owner cleanup");
        for (auto* root : {actor.pagemap, second.pagemap}) {
            require(virt::file_mapping_index.root_empty_locked(root), "actual index empty after CPU revoke");
            free_empty_tables(root);
            release_table(root);
        }
        virt::controls = {};
        actor.pagemap = second.pagemap = nullptr;
        object.reset();
        object_fixture::drain();
        require(quota.get()->usage().resident_pages == 0 && quota.get()->usage().objects == 0, "actual Object callbacks return quota");
        quota.reset();
        object_fixture::drain();
        ker::vfs::vfs_put_file(&file);
        expect(file.refcount.load() == 0, "exact File owners balanced");
        expect(table_pages() == 0 && object_fixture::live_pages() == 0 && object_fixture::reserved() == 0,
               "all Host physical and actual retirement reservations return to baseline");
        expect(actor.usercopy_pagemap_accesses.load() == 0 && second.usercopy_pagemap_accesses.load() == 0,
               "actual VM helpers return all finite Task-local leases");
        current = nullptr;
    }
    auto map(uint64_t start, uint64_t offset, uint64_t pages, uint64_t protections = RW, vm::Limits limits = {}) -> int {
        int error = 0;
        auto* family = fm::FileMappingLifetime::prepare_device_shared_wb(file, object, 0, object.get()->bytes(),
                                                                        object.get()->max_protections(), error);
        if (family == nullptr) return error;
        vm::PreparedMap plan;
        error = plan.prepare(*family, start, offset, pages * PAGE, protections, limits);
        if (error == 0) {
            Prep preparation(*current);
            Pub publication;
            error = plan.publish_current(*current, preparation, publication);
        }
        plan.reset();
        family->release(fm::FamilyOwner::PREPARED);
        object_fixture::drain();
        return error;
    }
    auto change(uint64_t start, uint64_t end, vm::Change operation, uint64_t protections = 0) -> int {
        vm::PreparedChange plan;
        int error = plan.prepare_current(*current, start, end, operation, protections);
        if (error == 0) {
            Prep preparation(*current);
            Pub publication;
            error = plan.commit_current(*current, preparation, publication);
        }
        plan.reset();
        object_fixture::drain();
        return error;
    }
    auto refs(uint64_t offset) -> uint32_t { return object_fixture::references(object.get()->cpu_page(offset)); }
};

void dynamic_view_and_visibility() {
    ++cases;
    World world;
    unsigned const before = flushes;
    require(world.map(BASE, 0, 130) == 0, "actual map exceeds old64-page cap and crosses table boundary");
    expect(flushes == before + 1, "one actual prepared publication CPU barrier");
    for (uint64_t page = 0; page < 130; ++page) {
        uint64_t const value = raw(world.actor.pagemap, BASE + page * PAGE);
        expect((value & (paging::PAGE_PRESENT | paging::PAGE_USER | paging::PAGE_WRITE | paging::PAGE_SHARED | paging::PAGE_NX)) ==
                   (paging::PAGE_PRESENT | paging::PAGE_USER | paging::PAGE_WRITE | paging::PAGE_SHARED | paging::PAGE_NX),
               "actual 4K leaf rights and ordinary shared ownership");
        expect((value & (paging::PAGE_COW | paging::PAGE_PWT | paging::PAGE_PCD | paging::PAGE_PAT_4K)) == 0,
               "actual leaf remains NX/WB/no-COW");
        expect(world.refs(page * PAGE) == 2, "Object and actual PTE each own one real frame reference");
    }
    auto* cpu = static_cast<uint64_t*>(world.object.get()->cpu_page(65 * PAGE));
    auto* entry = leaf(world.actor.pagemap, BASE + 65 * PAGE);
    auto* mapped = reinterpret_cast<uint64_t*>(static_cast<uint64_t>(entry->frame) << 12);
    *cpu = 0xabcdef1234ULL;
    expect(*mapped == 0xabcdef1234ULL, "mapped leaf observes actual Object backing");
    *mapped = 0x542137ULL;
    expect(*cpu == 0x542137ULL, "actual Object backing observes mapped writes");
    require(world.change(BASE + 63 * PAGE, BASE + 67 * PAGE, vm::Change::Protect, 0) == 0,
            "partial NONE crosses actual64-page commit boundary");
    for (uint64_t p = 63; p < 67; ++p)
        expect((raw(world.actor.pagemap, BASE + p * PAGE) & (paging::PAGE_USER | paging::PAGE_WRITE)) == 0 && world.refs(p * PAGE) == 2,
               "PROT_NONE retains PMM/PTE coverage while withdrawing user rights");
    require(world.change(BASE + 63 * PAGE, BASE + 67 * PAGE, vm::Change::Protect, RW) == 0, "fragment rights can restore within grant");
    require(world.change(BASE + 64 * PAGE, BASE + 66 * PAGE, vm::Change::Remove) == 0, "middle partial unmap across split fragments");
    expect(raw(world.actor.pagemap, BASE + 64 * PAGE) == 0 && raw(world.actor.pagemap, BASE + 66 * PAGE) != 0,
           "partial remove preserves exact side fragments");
    expect(world.refs(64 * PAGE) == 1 && world.refs(65 * PAGE) == 1 && world.refs(66 * PAGE) == 2,
           "old leaf refs drop only after completed withdrawal reset");
}

void reversed_adjacent_and_sparse() {
    ++cases;
    World world(12);
    require(world.map(BASE, 0, 2) == 0 && world.map(BASE + 2 * PAGE, 2 * PAGE, 2) == 0, "two reverse-index adjacent views");
    require(world.change(BASE + PAGE, BASE + 3 * PAGE, vm::Change::Protect, 1) == 0, "actual sorted multi-binding partial protect");
    expect((raw(world.actor.pagemap, BASE) & paging::PAGE_WRITE) != 0 &&
               (raw(world.actor.pagemap, BASE + PAGE) & paging::PAGE_WRITE) == 0 &&
               (raw(world.actor.pagemap, BASE + 2 * PAGE) & paging::PAGE_WRITE) == 0 &&
               (raw(world.actor.pagemap, BASE + 3 * PAGE) & paging::PAGE_WRITE) != 0, "exact multi-binding rights changed");
    require(world.change(BASE + PAGE, BASE + 3 * PAGE, vm::Change::Remove) == 0, "multi-binding middle remove");
    uint64_t const left = raw(world.actor.pagemap, BASE), right = raw(world.actor.pagemap, BASE + 3 * PAGE);
    world.actor.lazy_vmem_ranges.push_back({BASE + PAGE, BASE + 2 * PAGE});
    expect(world.change(BASE, BASE + 4 * PAGE, vm::Change::Remove) == -EOPNOTSUPP, "sparse device+lazy remove refuses before effects");
    expect(raw(world.actor.pagemap, BASE) == left && raw(world.actor.pagemap, BASE + 3 * PAGE) == right,
           "mixed lazy refusal leaves both device endpoints unchanged");
    world.actor.lazy_vmem_ranges.clear();
    expect(world.change(BASE, BASE + 4 * PAGE, vm::Change::Protect, 1) == -ENOMEM, "mprotect actual holes returns ENOMEM");
    expect(raw(world.actor.pagemap, BASE) == left && raw(world.actor.pagemap, BASE + 3 * PAGE) == right,
           "mprotect hole refusal has no partial effect");
    require(world.change(BASE, BASE + 4 * PAGE, vm::Change::Remove) == 0, "munmap actual holes remains supported");
}

void admission_and_failures() {
    ++cases;
    World world(8);
    virt::controls[0].ps_admission_suspensions = 1;
    expect(world.map(BASE, 0, 4) == -EAGAIN && raw(world.actor.pagemap, BASE) == 0,
           "actual mremap suspension excludes late device admission");
    virt::controls[0].ps_admission_suspensions = 0;
    expect(world.map(BASE, 0, 4, RW, {.max_pages = 3}) == -E2BIG, "configured view capacity refuses before effects");
    fail_table_after = 1;
    expect(world.map(BASE, 0, 4) == -ENOMEM, "prepared table allocation failure is reversible");
    fail_table_after = -1;
    expect(raw(world.actor.pagemap, BASE) == 0 && world.refs(0) == 1, "failed table preparation returns all loans and leaves root cold");
    object_fixture::fail_pin = true;
    expect(world.map(BASE, 0, 4) == -ENODEV, "actual PMM loan refusal is explicit");
    object_fixture::fail_pin = false;
    expect(raw(world.actor.pagemap, BASE) == 0 && world.refs(0) == 1, "failed loan preparation changes no PTE/frame owner");
    require(world.map(BASE, 0, 4) == 0, "admission recovers after failures");
    uint64_t const old = raw(world.actor.pagemap, BASE);
    expect(world.map(BASE, 4 * PAGE, 2) == -EAGAIN && raw(world.actor.pagemap, BASE) == old,
           "occupied range refusal preserves exact old mapping");
    bool overlap = false;
    { Pub pub; expect(vm::current_device_overlap(world.actor, BASE, BASE + PAGE, pub, overlap) == 0 && overlap,
                      "actual current routing sees binding coverage"); }
}

void read_only_and_stale_plan() {
    ++cases;
    World world(8, 1);
    require(world.map(BASE, 0, 4, 1) == 0, "read-only family mapping");
    uint64_t const old = raw(world.actor.pagemap, BASE);
    expect(world.change(BASE, BASE + PAGE, vm::Change::Protect, RW) == -EACCES && raw(world.actor.pagemap, BASE) == old,
           "immutable grant ceiling rejects new write rights");
    vm::PreparedChange stale;
    require(stale.prepare_current(world.actor, BASE, BASE + PAGE, vm::Change::Remove) == 0, "real stale snapshot capture");
    require(world.change(BASE, BASE + PAGE, vm::Change::Protect, 0) == 0, "replace exact old binding at same root/VA/frame");
    { Prep prep(world.actor); Pub pub; expect(stale.commit_current(world.actor, prep, pub) == -EAGAIN, "old incarnation cannot mutate replacement"); }
    stale.reset();
    object_fixture::drain();
    expect((raw(world.actor.pagemap, BASE) & paging::PAGE_USER) == 0 && world.refs(0) == 2,
           "stale refusal preserves replacement and PMM owner");
}

void multiroot_export_and_fork_plan() {
    ++cases;
    World world(12);
    require(world.map(BASE, 0, 4) == 0, "first root mapping");
    Prep prep(world.actor);
    {
        vm::PreparedFork fork;
        require(fork.prepare_current(world.actor, prep) == 0 && !fork.empty(), "actual eager binding-only fork preparation");
        { Pub pub; expect(fork.matches_current(world.actor, pub), "actual prepared fork exact match"); }
        require(world.map(BASE + 8 * PAGE, 8 * PAGE, 1) == 0, "later sibling-style device admission");
        { Pub pub; expect(!fork.matches_current(world.actor, pub), "new actual device binding invalidates complete fork plan"); }
    }
    object_fixture::drain();
    current = &world.second;
    require(world.map(BASE + PAGE, PAGE, 2) == 0, "second root shares actual Object pages");
    current = &world.actor;
    mapping::ObjectExportRef exported;
    uint64_t physical = 0;
    require(world.object.get()->frame_at(PAGE, physical) && world.object.get()->export_page(PAGE, physical, exported),
            "actual counted independent Object export");
    expect(vm::revoke_cpu_mappings(world.object) == -EAGAIN, "export owner prevents false complete revoke");
    object_fixture::drain();
    expect(raw(world.actor.pagemap, BASE) == 0 && raw(world.second.pagemap, BASE + PAGE) == 0,
           "actual reverse membership withdraws every root alias");
    expect(world.object.get()->state() == mapping::ObjectState::revoking && world.refs(PAGE) == 1,
           "completed CPU withdrawal differs from exported-owner drain");
    expect(world.map(BASE, 0, 1) != 0, "closed generation cannot be republished");
    exported.reset();
    object_fixture::drain();
    expect(vm::revoke_cpu_mappings(world.object) == 0 && world.object.get()->state() == mapping::ObjectState::revoked,
           "real export drop and callbacks allow final revoked state");
}
}

int main() {
    using namespace vm_fixture;
    dynamic_view_and_visibility();
    reversed_adjacent_and_sparse();
    admission_and_failures();
    read_only_and_stale_plan();
    multiroot_export_and_fork_plan();
    expect(object_fixture::irq_depth == 0 && object_fixture::lock_depth == 0 && object_fixture::mutex_depth == 0 &&
               object_fixture::pub_depth == 0 && object_fixture::reserved() == 0,
           "terminal finite context/queue baseline");
    std::printf("%s device_mapping_vm cases=%u assertions=%u failures=%u host_metadata_ports=1\n",
                failures == 0 ? "PASS" : "FAIL", cases, assertions, failures);
    return failures == 0 ? 0 : 3;
}
