#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/lifetime_retirement_executor.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/security/credentials.hpp>
#include <platform/sys/usercopy.hpp>
#include <string_view>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <test/binding_root_runtime_ktest.hpp>
#include <test/device_mapping_client_ktest.hpp>
#include <test/device_mapping_object_ktest.hpp>
#include <vfs/fs/tmpfs.hpp>
#include <vfs/vfs.hpp>

namespace ker::test {
namespace {
namespace mm = mod::mm;
namespace fm = mm::file_mapping;
namespace virt = mm::virt;
namespace sched = mod::sched;
namespace task = sched::task;
namespace uc = mod::sys::usercopy;
namespace vfs = ker::vfs;
namespace tmpfs = vfs::tmpfs;
using log = mod::dbg::logger<"binding-root-test">;
using Admission = virt::BindingRootAdmission;
constexpr uint64_t PAGE = mm::paging::PAGE_SIZE;
constexpr uint64_t ADDRESS = 0x5E000000ULL;
constexpr uint64_t WAIT_US = 10'000'000;
std::atomic<bool> started{false};

struct Checks {
    uint64_t assertions{}, cases{};
    void require(bool condition, const char* reason) {
        ++assertions;
        if (condition) return;
        // No unwind may free a root/provider behind an unjoined reader or a
        // pending real callback. Preserve the owned fixture for diagnosis.
        log::error("BINDING_ROOT_RUNTIME FAIL checks=%lu reason=%s", assertions, reason);
        mod::dbg::panic_handler("scheduled binding root selftest failed");
    }
    void pass(const char* name) {
        ++cases;
        log::info("BINDING_ROOT_RUNTIME CASE_PASS %s", name);
    }
};

auto physical(void* page) -> uint64_t { return reinterpret_cast<uint64_t>(mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(page))); }

struct Fixture {
    Checks& checks;
    std::unique_ptr<task::Task> subject{new (std::nothrow) task::Task{}};
    tmpfs::TmpNode* tree{};
    tmpfs::TmpNode* node{};
    vfs::File* file{};
    fm::FileMappingLifetime* family{};  // PREPARED, released before waiting.
    void* frame{};                      // Independent observer reference.
    fm::RetirementCompletion returned;

    explicit Fixture(Checks& value) : checks(value) {}
    ~Fixture() {
        checks.require(subject != nullptr && subject->pagemap == nullptr && family == nullptr && file == nullptr,
                       "all root and File owners explicitly retired");
        checks.require(node != nullptr && node->open_count.load(std::memory_order_acquire) == 0, "actual backend close returned");
        checks.require(mm::phys::page_ref_get(frame) == 1, "observer is final frame owner after all PTEs");
        static_cast<void>(mm::phys::page_ref_dec(frame));
        returned.reset();
        tmpfs::tmpfs_lock_tree();
        tmpfs::tmpfs_free_node(tree);
        tmpfs::tmpfs_unlock_tree();
    }
    void init() {
        checks.require(subject != nullptr, "allocate original private Task");
        subject->type = task::TaskType::PROCESS;
        subject->pid = task::get_next_pid();
        subject->heap_index = -1;  // Original private construction; never posted.
        subject->pagemap = virt::create_pagemap_may_fail();
        checks.require(subject->pagemap != nullptr, "allocate actual registered root");
        checks.require(mm::phys::page_ref_get(subject->pagemap) == 1, "one original root allocator owner");
        tree = tmpfs::create_root_node();
        checks.require(tree != nullptr, "create detached actual TMPFS tree");
        int error = 0;
        file = tmpfs::tmpfs_open_path(mod::security::Snapshot::bootstrap_root(), tree, "binding-root", vfs::O_CREAT | 2, 0600, &error);
        checks.require(file != nullptr && error == 0 && file->fops == nullptr, "actual private backend open awaits VFS table");
        file->fops = tmpfs::get_tmpfs_fops();  // Same actual table installed by VFS.
        node = static_cast<tmpfs::TmpNode*>(file->private_data);
        checks.require(node != nullptr && node->open_count.load(std::memory_order_acquire) == 1, "actual inode open owner");
        family = fm::FileMappingLifetime::prepare(*file, returned);
        checks.require(family != nullptr && !returned.empty(), "real permanent executor reservation and observer");
        frame = mm::phys::page_alloc_may_fail(mm::PhysicalPageOwner::SELFTEST, PAGE, "binding-root-observer");
        checks.require(frame != nullptr, "allocate independent ordinary frame");
        std::memset(frame, 0x5C, PAGE);
        map_frame();
    }
    void map_frame() {
        checks.require(mm::phys::page_ref_try_inc(frame), "acquire PTE frame owner before publication");
        virt::map_page(subject->pagemap, ADDRESS, physical(frame), mm::paging::page_types::USER_READONLY | mm::paging::PAGE_NX);
        checks.require(virt::translate(subject->pagemap, ADDRESS) == physical(frame), "actual PTE adopted frame owner");
    }
    void bind(fm::FileBindingSnapshot& empty) {
        auto* prepared = fm::FileMappingBinding::prepare_unbound(*family);
        checks.require(prepared != nullptr, "prepare real binding and callback ticket outside PUB");
        checks.require(virt::attach_unpublished_file_binding(prepared, subject->pagemap, ADDRESS, ADDRESS + PAGE, 0) && prepared == nullptr,
                       "actual index adopts prepared binding");
        bool exact = false;
        {
            syscall::vmem::SharedVmemPublicationGuard publication;
            checks.require(virt::selftest_capture_binding_snapshot(*subject, ADDRESS, ADDRESS + PAGE, publication, empty, exact) && exact,
                           "real registered control/index issues sealed exact snapshot");
        }
        checks.require(empty.retirement_state() == fm::BindingRetirementState::INDEXED, "snapshot observes actual indexed state");
    }
    void destroy_tree() {
        checks.require(subject->usercopy_pagemap_accesses.load(std::memory_order_acquire) == 0, "all actual Task reader leases joined");
        checks.require(mm::phys::page_ref_get(subject->pagemap) == 1, "lease never retained root allocator frame");
        virt::destroy_user_space(subject->pagemap, 0, "binding-root-test", "private-tree");
        checks.require(mm::phys::page_ref_get(frame) == 1, "actual tree teardown returned PTE frame ref");
    }
    void release_root() {
        auto* const ROOT = subject->detach_pagemap_after_usercopy_quiescence();
        checks.require(ROOT != nullptr && mm::phys::page_ref_get(ROOT) == 1, "retire original sole root owner");
        virt::release_pagemap(ROOT);
        // ROOT is historical identity only from this point onward.
    }
    void close_owners() {
        auto* const FILE = file;
        file = nullptr;
        vfs::vfs_put_file(FILE);
        auto* const FAMILY = family;
        family = nullptr;
        FAMILY->release(fm::FamilyOwner::PREPARED);
    }
    void await_return() {
        uint64_t const DEADLINE = sched::saturating_deadline_us(mod::time::get_us(), WAIT_US);
        for (;;) {
            auto const VALUE = returned.query();
            if (VALUE.state == fm::RetirementReturnState::RETURNED) {
                checks.require(VALUE.callback_result == 0, "actual final File callback returned success");
                return;
            }
            checks.require(VALUE.state == fm::RetirementReturnState::PENDING && mod::time::get_us() < DEADLINE,
                           "bounded independent callback observation preserves pending owners");
            sched::kern_sleep_us(1000);
        }
    }
};

struct Reader {
    task::Task* target{};  // Fixture retains its original unpublished Task/root.
    task::Task* worker{};  // Real extra coordinator join reference.
    std::atomic<bool> ready{false}, release{false};
    uint64_t deadline{};
};

void reader_entry() {
    auto* const CURRENT = sched::get_current_task();
    auto* const BOX = static_cast<Reader*>(CURRENT->kernel_thread_context);
    Checks checks{};
    checks.require(BOX != nullptr, "posted reader has actual retained context");
    {
        auto const HOLD = sched::hold_kernel_shutdown();
        uc::StableUserPage pin;
        checks.require(uc::pin_task_user_page(*BOX->target, ADDRESS, false, false, pin), "actual stable user page holds stripe reader");
        BOX->ready.store(true, std::memory_order_release);
        // Do not voluntarily schedule while a foreign root/frame pin is held.
        // Timer preemption remains enabled so this also works on one CPU.
        while (!BOX->release.load(std::memory_order_acquire)) {
            checks.require(mod::time::get_us() < BOX->deadline, "reader command deadline preserves live pin/context");
            asm volatile("pause" ::: "memory");
        }
    }
    CURRENT->kernel_thread_context = nullptr;
    sched::exit_current_kernel_thread();
}

void begin_reader(Checks& checks, Reader& box, task::Task& subject) {
    box.target = &subject;
    box.deadline = sched::saturating_deadline_us(mod::time::get_us(), WAIT_US);
    box.worker = task::Task::create_kernel_thread("binding_root_reader", reader_entry);
    checks.require(box.worker != nullptr, "allocate actual reader worker");
    uint32_t refs = 1;
    checks.require(box.worker->ref_count.compare_exchange_strong(refs, 2, std::memory_order_acq_rel, std::memory_order_acquire),
                   "reserve exact unpublished reader join reference");
    box.worker->kernel_thread_context = &box;
    if (!sched::post_task_balanced(box.worker)) {
        box.worker->kernel_thread_context = nullptr;
        box.worker->release();
        task::destroy_unpublished_user_thread(box.worker);
        box.worker = nullptr;
        checks.require(false, "reader post failed with owned rollback");
    }
    while (!box.ready.load(std::memory_order_acquire)) {
        checks.require(mod::time::get_us() < box.deadline, "actual reader starts before bounded deadline");
        sched::kern_sleep_us(1000);
    }
}

void finish_reader(Checks& checks, Reader& box) {
    box.release.store(true, std::memory_order_release);
    while (box.worker->state.load(std::memory_order_acquire) != task::TaskState::DEAD) {
        checks.require(mod::time::get_us() < box.deadline, "reader actually reaches DEAD before context release");
        sched::kern_sleep_us(1000);
    }
    checks.require(box.worker->kernel_thread_context == nullptr, "reader stopped using borrowed context");
    box.worker->release();
    box.worker = nullptr;
}

void lease_lifecycle(Checks& checks) {
    Fixture fixture(checks);
    fixture.init();
    fm::FileBindingSnapshot original;
    fixture.bind(original);
    auto* const WALK = virt::prepare_destroy_user_space_budget_state();
    checks.require(WALK != nullptr, "prepare actual try-teardown continuation before PUB");
    {
        syscall::vmem::SharedVmemPublicationGuard publication;
        virt::BindingRootMutationLease lease;
        checks.require(virt::try_acquire_binding_root(original, publication, lease) == Admission::Acquired && lease.acquired(),
                       "exact indexed snapshot acquires actual exclusive root gate");
        checks.require(mm::phys::page_ref_get(fixture.subject->pagemap) == 1 && mm::phys::page_ref_get(fixture.frame) == 2,
                       "root lease acquires no root/data allocator references");
        checks.require(!virt::begin_prepared_destroy_user_space(*WALK, fixture.subject->pagemap, 0, "binding-root-test", "held", false),
                       "actual try-only teardown refuses held root lease");
        checks.require(virt::try_acquire_binding_root(original, publication, lease) == Admission::OwnerUsed && lease.acquired(),
                       "one-shot reuse cannot alter existing lease");
        virt::BindingRootMutationLease second;
        checks.require(virt::try_acquire_binding_root(original, publication, second) == Admission::Invalid && lease.acquired(),
                       "borrowed snapshot cannot acquire another lease");
        lease.finish();
        lease.finish();
        checks.require(!lease.acquired() && !original.empty(), "balanced finish keeps snapshot owner");
    }
    virt::destroy_user_space_budget_state_destroy(WALK);
    checks.pass("exact_snapshot_gate_and_try_teardown");

    Reader reader;
    begin_reader(checks, reader, *fixture.subject);
    {
        syscall::vmem::SharedVmemPublicationGuard publication;
        virt::BindingRootMutationLease lease;
        checks.require(virt::try_acquire_binding_root(original, publication, lease) == Admission::Busy && !lease.acquired(),
                       "independent real StableUserPage reader refuses exclusive admission");
    }
    finish_reader(checks, reader);
    checks.require(mm::phys::page_ref_get(fixture.frame) == 2 && fixture.subject->usercopy_pagemap_accesses.load() == 0,
                   "joined real reader released every frame and local/root lease");
    checks.pass("stable_reader_busy_then_returns");

    {
        syscall::vmem::SharedVmemPublicationGuard publication;
        sched::PreemptGuard preempt;
        virt::BindingRootMutationLease lease;
        checks.require(virt::try_acquire_binding_root(original, publication, lease) == Admission::Context && !lease.acquired(),
                       "preemption-disabled context refuses before gate effect");
    }
    checks.pass("excluded_context_refuses");

    fixture.destroy_tree();
    checks.require(original.retirement_state() == fm::BindingRetirementState::TLB_COMPLETE, "actual teardown marks ACK-complete binding");
    fixture.map_frame();  // Same root, VA and physical frame; a new binding.
    fm::FileBindingSnapshot replacement;
    fixture.bind(replacement);
    {
        syscall::vmem::SharedVmemPublicationGuard publication;
        virt::BindingRootMutationLease stale, current;
        checks.require(virt::try_acquire_binding_root(original, publication, stale) == Admission::NotIndexed && !stale.acquired(),
                       "replaced binding cannot impersonate equal root VA and frame");
        checks.require(virt::try_acquire_binding_root(replacement, publication, current) == Admission::Acquired,
                       "actual replacement binding acquires its own authority");
    }
    fixture.destroy_tree();
    fixture.release_root();
    {
        syscall::vmem::SharedVmemPublicationGuard publication;
        virt::BindingRootMutationLease stale;
        checks.require(virt::try_acquire_binding_root(original, publication, stale) == Admission::StaleRoot && !stale.acquired(),
                       "snapshot survives actual root recycling but conveys no stale root authority");
    }
    checks.pass("replacement_and_recycled_root_refuse");
    fixture.close_owners();
    checks.require(fixture.returned.query().state == fm::RetirementReturnState::PENDING,
                   "held snapshot prevents final callback without self-wait");
    original.reset();
    replacement.reset();
    fixture.await_return();
    checks.pass("snapshots_release_before_actual_callback_return");
}

void collision_lifecycle(Checks& checks) {
    // At most 4097 live roots force some pair to share the actual 4096 stripes.
    // Keep every candidate owned until the pair is selected; no forced PMM
    // address, registry entry or gate state is installed by the fixture.
    constexpr size_t STRIPES = 4096;
    struct Roots {
        std::array<mm::paging::PageTable*, STRIPES + 1> owned{};
        std::array<size_t, STRIPES> first{};
        size_t count{};
        Roots() { first.fill(SIZE_MAX); }
        ~Roots() {
            for (auto* root : owned)
                if (root != nullptr) virt::release_pagemap(root);
        }
    };
    auto roots = std::unique_ptr<Roots>(new (std::nothrow) Roots{});
    checks.require(roots != nullptr, "allocate bounded collision candidate ledger outside guards");
    size_t left = SIZE_MAX, right = SIZE_MAX;
    for (size_t i = 0; i < roots->owned.size(); ++i) {
        auto* const ROOT = virt::create_pagemap_may_fail();
        checks.require(ROOT != nullptr && mm::phys::page_ref_get(ROOT) == 1, "collision candidate has real sole root owner");
        roots->owned.at(i) = ROOT;
        ++roots->count;
        size_t const STRIPE = (reinterpret_cast<uintptr_t>(ROOT) >> 12U) & (STRIPES - 1);
        if (roots->first.at(STRIPE) != SIZE_MAX) {
            left = roots->first.at(STRIPE);
            right = i;
            break;
        }
        roots->first.at(STRIPE) = i;
    }
    checks.require(left != SIZE_MAX && right != SIZE_MAX && left != right, "bounded actual roots contain a distinct colliding pair");
    Fixture fixture(checks);
    fixture.init();
    fixture.destroy_tree();
    fixture.release_root();
    auto* const TARGET_ROOT = roots->owned.at(left);
    roots->owned.at(left) = nullptr;  // Transfer original allocator ownership.
    checks.require(fixture.subject->replace_pagemap_after_usercopy_quiescence(TARGET_ROOT) == nullptr,
                   "original private Task adopts exact selected root and reopens local gate");
    fixture.map_frame();
    fm::FileBindingSnapshot snapshot;
    fixture.bind(snapshot);
    auto other = std::unique_ptr<task::Task>(new (std::nothrow) task::Task{});
    checks.require(other != nullptr, "allocate original private reader Task");
    other->type = task::TaskType::PROCESS;
    other->pid = task::get_next_pid();
    other->heap_index = -1;
    other->pagemap = roots->owned.at(right);
    roots->owned.at(right) = nullptr;  // The reader Task now owns this root.
    checks.require(other->pagemap != TARGET_ROOT && mm::phys::page_ref_try_inc(fixture.frame), "acquire distinct-root PTE frame owner");
    virt::map_page(other->pagemap, ADDRESS, physical(fixture.frame), mm::paging::page_types::USER_READONLY | mm::paging::PAGE_NX);
    Reader reader;
    begin_reader(checks, reader, *other);
    {
        syscall::vmem::SharedVmemPublicationGuard publication;
        virt::BindingRootMutationLease lease;
        checks.require(virt::try_acquire_binding_root(snapshot, publication, lease) == Admission::Busy && !lease.acquired(),
                       "actual foreign-root reader collision refuses exclusive admission");
        checks.require(mm::phys::page_ref_get(TARGET_ROOT) == 1 && mm::phys::page_ref_get(other->pagemap) == 1,
                       "colliding admission retained neither root allocator frame");
    }
    finish_reader(checks, reader);
    {
        syscall::vmem::SharedVmemPublicationGuard publication;
        virt::BindingRootMutationLease lease;
        checks.require(virt::try_acquire_binding_root(snapshot, publication, lease) == Admission::Acquired && lease.acquired(),
                       "same exact root admits after unrelated reader returns");
    }
    auto* const OTHER_ROOT = other->detach_pagemap_after_usercopy_quiescence();
    virt::destroy_user_space(OTHER_ROOT, 0, "binding-root-test", "collision-reader");
    virt::release_pagemap(OTHER_ROOT);
    fixture.destroy_tree();
    fixture.release_root();
    fixture.close_owners();
    snapshot.reset();
    fixture.await_return();
    log::info("BINDING_ROOT_RUNTIME COLLISION candidates=%lu candidate_root_byte_limit=%lu", roots->count, (STRIPES + 1) * PAGE);
    checks.pass("unrelated_actual_root_stripe_collision");
}

void wait_for_init_mounts(Checks& checks) {
    uint64_t const DEADLINE = sched::saturating_deadline_us(mod::time::get_us(), 30'000'000);
    for (;;) {
        auto* init = sched::find_task_by_pid_safe(1);
        if (init != nullptr) {
            task::Task::FilesystemContext context;
            bool const READY = init->capture_filesystem_context(context) && context.root_len == 7 &&
                               std::string_view(context.root.data(), context.root_len) == "/rootfs";
            init->release();
            if (READY) {
                log::info("BINDING_ROOT_RUNTIME INIT_READY root=/rootfs");
                return;
            }
        }
        checks.require(mod::time::get_us() < DEADLINE, "actual PID1 root pivot precedes normal shutdown request");
        sched::kern_sleep_us(10000);
    }
}

void runtime_entry() {
    {
        auto const HOLD = sched::hold_kernel_shutdown();
        Checks checks{};
        auto const EXECUTOR = fm::LifetimeRetirementExecutor::snapshot();
        checks.require(mm::phys::can_wait_for_reclaim() && EXECUTOR.initialized && EXECUTOR.available && EXECUTOR.admission_open,
                       "actual scheduled context and permanent retirement executor");
        log::info("BINDING_ROOT_RUNTIME BEGIN");
        checks.require(run_device_mapping_client_selftest(), "actual retained device endpoint/client lifecycle cases");
        checks.require(run_device_mapping_object_selftest(), "actual backing quota, grants and IRQ export retirement cases");
        lease_lifecycle(checks);
        collision_lifecycle(checks);
        checks.require(checks.cases == 6, "all actual lease lifecycle cases returned");
        log::info("BINDING_ROOT_RUNTIME PASS cases=%lu assertions=%lu device_map_exercised=0", checks.cases, checks.assertions);
        wait_for_init_mounts(checks);
    }
    {
        auto const HOLD = sched::hold_kernel_shutdown();
        auto* init = sched::find_task_by_pid_safe(1);
        if (init == nullptr) mod::dbg::panic_handler("binding root fixture lost PID1");
        constexpr int WOS_SIGUSR2 = 12;
        init->signal_generate_pending_mask(uint64_t{1} << (WOS_SIGUSR2 - 1));
        sched::wake_task_for_signal(init);
        init->release();
        log::info("BINDING_ROOT_RUNTIME POWEROFF_REQUESTED via=init-sigusr2");
    }
    sched::exit_current_kernel_thread();
}
}  // namespace

auto start_binding_root_runtime_selftest() -> bool {
    bool expected = false;
    if (!started.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) return false;
    auto* worker = task::Task::create_kernel_thread("binding_root_runtime_selftest", runtime_entry);
    if (worker == nullptr || !sched::post_task_balanced(worker)) {
        if (worker != nullptr) task::destroy_unpublished_user_thread(worker);
        started.store(false, std::memory_order_release);
        return false;
    }
    return true;
}
}  // namespace ker::test
