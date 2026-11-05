namespace lease_fixture {
namespace actual = ker::mod::mm::virt;
namespace fm = ker::mod::mm::file_mapping;
namespace sched = ker::mod::sched;
using Root = ker::mod::mm::paging::PageTable;
using Snapshot = fm::FileBindingSnapshot;
using Lease = actual::BindingRootMutationLease;
using Admission = actual::BindingRootAdmission;
using Pub = ker::syscall::vmem::SharedVmemPublicationGuard;
using Task = sched::task::Task;
using TaskState = sched::task::TaskState;
using TaskType = sched::task::TaskType;
constexpr uint64_t SERIAL = 17, START = 0x400000, PAGE = 4096;

// Same seam lock protects both the actual lease's lookup and all actual index
// operations. IndexGuard's extra depth diagnoses callbacks under that seam.
using PinGuard = snapshot_fixture::IndexGuard;

void ordinary(Task& task) {
    run_queues = queryable = interrupts = true;
    panic = access_window = retirement_worker = false;
    preempt = 0;
    current = &task;
    task.state.store(TaskState::ACTIVE);
    task.scheduler_transition_active.store(false);
    task.exit_in_progress.store(false);
    task.usercopy_pagemap_closing.store(false);
    task.deferred_task_switch = task.wants_block = false;
    task.type = TaskType::PROCESS;
    task.syscall_account_start_us = 1;
    require(!task.kernel_shutdown_ownership.held(), "ordinary fixture starts without abandoned shutdown hold");
}

struct World {
    ker::vfs::File file;
    Root* root{};
    void* protected_storage{};
    fm::FileMappingLifetime* family{};
    fm::FileMappingBinding* indexed{};
    // A finite leaf identity oracle only: the Host never reads a PTE or owns a
    // physical frame. Native tests cover actual same-frame translation reuse.
    uint64_t leaf_frame{0x3a000};
    unsigned initial_puts{snapshot_fixture::puts}, initial_closes{snapshot_fixture::closes};
    explicit World(Root* identity = nullptr) {
        require(publication_depth == 0 && snapshot_fixture::lock_depth == 0, "family preparation outside guards");
        if (identity == nullptr) {
            protected_storage = mmap(nullptr, PAGE, PROT_NONE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
            require(protected_storage != MAP_FAILED, "allocate inaccessible stale-root key");
            root = static_cast<Root*>(protected_storage);
        } else {
            root = identity;
        }
        require(actual::host_root_controls[0].root == nullptr, "isolated root-control port slot");
        actual::host_root_controls[0] = {root, SERIAL};
        family = fm::FileMappingLifetime::prepare(file);
        require(family != nullptr && file.refcount.load() == 2, "actual family retains exact File");
        add();
    }
    ~World() {
        require(publication_depth == 0 && snapshot_fixture::lock_depth == 0, "all cleanup outside PUB/gate/pin guards");
        detach();
        actual::host_root_controls[0] = {};
        family->release(fm::FamilyOwner::PREPARED);
        ker::vfs::vfs_put_file(&file);
        snapshot_fixture::drain();
        check(file.refcount.load() == 0 && snapshot_fixture::closes == initial_closes + 1,
              "actual binding/family callbacks close exact File once");
        check(snapshot_fixture::puts == initial_puts + 2 && snapshot_fixture::queue_empty(),
              "all actual queued family/binding owners drain");
        check(snapshot_fixture::lock_depth == 0 && snapshot_fixture::index_depth == 0, "all seam locks released");
        if (protected_storage != nullptr) check(munmap(protected_storage, PAGE) == 0, "free protected host identity storage");
    }
    void add() {
        require(indexed == nullptr, "only one active binding per finite fixture interval");
        auto* prepared = fm::FileMappingBinding::prepare(*family, root, SERIAL, START, START + PAGE, 0);
        require(prepared != nullptr, "actual binding preparation");
        indexed = prepared;
        {
            PinGuard guard;
            require(actual::file_mapping_index.attach_locked(prepared, SERIAL), "actual binding index attach");
        }
        require(prepared == nullptr, "actual index consumes prepared owner");
    }
    void detach() {
        fm::FileMappingTlbRetirement retired;
        {
            PinGuard guard;
            require(actual::file_mapping_index.detach_root_locked(root, SERIAL, retired), "actual binding index detach");
            require(actual::file_mapping_index.root_empty_locked(root), "actual root index empty after detach");
        }
        indexed = nullptr;
        // Finite protocol endpoint only. This is never hardware TLB evidence.
        retired.release_after_completed_tlb();
    }
    void capture(Snapshot& output) {
        bool exact = false;
        PinGuard guard;
        require(actual::file_mapping_index.overlap_locked(root, SERIAL, START, START + PAGE, output, exact),
                "snapshot authority originates in actual typed index capture");
        require(exact && !output.empty(), "actual typed capture owns exact interval");
    }
    void gate_free() const {
        auto const& gate = actual::user_pagemap_access_gates.at(actual::user_pagemap_access_gate_index(root));
        check(gate.readers.load() == 0 && !gate.teardown.load(), "refusal/finish leaves no reader or exclusive owner");
    }
};

void success_and_one_shot(Task& task) {
    ++cases;
    ordinary(task);
    World world;
    Snapshot snapshot;
    world.capture(snapshot);
    auto const before = world.indexed->owner_counts();
    int const refs = world.file.refcount.load();
    unsigned const closes = snapshot_fixture::closes, callbacks = snapshot_fixture::callbacks;
    {
        Pub pub;
        check(pub.acquired() && pub.acquired_by_current_executor(), "actual blocking PUB identifies its scheduled owner");
        check(ker::mod::mm::phys::can_wait_for_reclaim(), "actual reclaim predicate admits ordinary context");
        Lease lease;
        check(!lease.acquired(), "virgin lease owns no gate");
        check(actual::try_acquire_binding_root(snapshot, pub, lease) == Admission::Acquired, "actual root lease acquired");
        check(lease.acquired(), "actual lease holds bidirectional borrow and PUB");
        auto& gate = actual::user_pagemap_access_gates.at(actual::user_pagemap_access_gate_index(world.root));
        check(gate.teardown.load() && gate.readers.load() == 0, "actual permanent exclusive gate held");
        size_t reader = actual::USER_PAGEMAP_ACCESS_GATE_NONE;
        check(!actual::acquire_user_pagemap_access(world.root, false, reader), "actual readers refuse leased stripe");
        check(reader == actual::USER_PAGEMAP_ACCESS_GATE_NONE, "refused reader owns no slot");
        check(actual::try_acquire_binding_root(snapshot, pub, lease) == Admission::OwnerUsed && lease.acquired(),
              "second attempt does not consume a held owner");
        Lease borrowed;
        check(actual::try_acquire_binding_root(snapshot, pub, borrowed) == Admission::Invalid,
              "one snapshot cannot support two simultaneous borrows");
        lease.finish();
        lease.finish();
        check(!lease.acquired(), "finish is idempotent and clears exact borrow");
        check(actual::try_acquire_binding_root(snapshot, pub, lease) == Admission::OwnerUsed,
              "successful owner remains one-shot after finish");
        Lease scoped;
        check(actual::try_acquire_binding_root(snapshot, pub, scoped) == Admission::Acquired,
              "finished snapshot can support a fresh scoped owner");
    }
    world.gate_free();
    auto const after = world.indexed->owner_counts();
    check(before.total == after.total && before.by_role == after.by_role && world.file.refcount.load() == refs,
          "lease acquires no binding/family/File/root allocator reference");
    check(snapshot_fixture::closes == closes && snapshot_fixture::callbacks == callbacks,
          "lease never executes backend retirement");
    snapshot.reset();
}

void stale_roots_and_replacement(Task& task) {
    ordinary(task);
    World world;
    Snapshot old;
    world.capture(old);
    for (bool missing : {true, false}) {
        ++cases;
        actual::host_root_controls[0] = missing ? actual::HostRootControl{} : actual::HostRootControl{world.root, SERIAL + 1};
        unsigned const lookups = control_lookups;
        {
            Pub pub;
            Lease lease;
            check(actual::try_acquire_binding_root(old, pub, lease) == Admission::StaleRoot,
                  missing ? "inaccessible missing root refused before root probe" : "inaccessible wrong-serial root refused before root probe");
            check(!lease.acquired() && !old.empty(), "stale root refusal preserves snapshot authority");
            check(actual::try_acquire_binding_root(old, pub, lease) == Admission::OwnerUsed, "stale refusal consumes exact attempt");
        }
        check(control_lookups == lookups + 1, "stale admission uses one pointer-key control lookup");
        world.gate_free();
    }
    actual::host_root_controls[0] = {world.root, SERIAL};
    uint64_t const frame = world.leaf_frame;
    auto* const former = world.indexed;
    world.detach();
    ++cases;
    {
        Pub pub;
        Lease lease;
        check(actual::try_acquire_binding_root(old, pub, lease) == Admission::NotIndexed,
              "detached actual snapshot is not current binding authority");
    }
    world.gate_free();
    world.add();
    Snapshot replacement;
    world.capture(replacement);
    ++cases;
    check(former != world.indexed && frame == world.leaf_frame && former->root() == world.indexed->root() &&
              former->start() == world.indexed->start(), "same finite root/VA/frame identity has a different actual binding");
    {
        Pub pub;
        Lease old_lease, new_lease;
        check(actual::try_acquire_binding_root(old, pub, old_lease) == Admission::NotIndexed,
              "actual binding identity rejects same-VA/root/frame replacement");
        check(actual::try_acquire_binding_root(replacement, pub, new_lease) == Admission::Acquired,
              "new indexed incarnation remains admissible after old refusal");
    }
    world.gate_free();
    replacement.reset();
    old.reset();
}

void real_gate_contention(Task& task) {
    ordinary(task);
    World world;
    Snapshot snapshot;
    world.capture(snapshot);
    auto* collision = reinterpret_cast<Root*>(reinterpret_cast<uintptr_t>(world.root) + 16U * 1024U * 1024U);
    check(collision != world.root && actual::user_pagemap_access_gate_index(collision) == actual::user_pagemap_access_gate_index(world.root),
          "distinct roots sixteen MiB apart collide in actual permanent gate");
    ++cases;
    size_t reader = actual::USER_PAGEMAP_ACCESS_GATE_NONE;
    require(actual::acquire_user_pagemap_access(collision, false, reader), "actual colliding reader admission");
    unsigned const lookups = control_lookups;
    {
        Pub pub;
        Lease busy;
        check(actual::try_acquire_binding_root(snapshot, pub, busy) == Admission::Busy, "actual readers cause try-only Busy");
        check(actual::user_pagemap_access_gates.at(reader).readers.load() == 1 &&
                  !actual::user_pagemap_access_gates.at(reader).teardown.load(), "Busy rolls back only attempt's exclusive flag");
    }
    check(control_lookups == lookups, "reader Busy never reaches root lookup");
    actual::release_user_pagemap_access(reader);
    world.gate_free();
    ++cases;
    size_t const exclusive = actual::try_begin_user_pagemap_exclusive(collision);
    require(exclusive != actual::USER_PAGEMAP_ACCESS_GATE_NONE, "actual colliding exclusive admission");
    {
        Pub pub;
        Lease busy;
        check(actual::try_acquire_binding_root(snapshot, pub, busy) == Admission::Busy, "actual colliding exclusive causes Busy");
        check(actual::user_pagemap_access_gates.at(exclusive).teardown.load(), "Busy never releases another exact exclusive owner");
    }
    actual::end_user_pagemap_exclusive(exclusive);
    world.gate_free();
    snapshot.reset();
}

template <typename Change>
void context_refusal(Task& task, World& world, Snapshot& snapshot, const char* message, Change change) {
    ++cases;
    ordinary(task);
    auto const owners = world.indexed->owner_counts();
    unsigned const lookups = control_lookups;
    {
        Pub pub;
        change();
        Lease lease;
        check(actual::try_acquire_binding_root(snapshot, pub, lease) == Admission::Context, message);
        check(!lease.acquired() && !snapshot.empty(), "context refusal keeps snapshot and no gate");
        check(actual::try_acquire_binding_root(snapshot, pub, lease) == Admission::OwnerUsed, "context refusal remains one-shot");
        ordinary(task);  // Restore owning executor before actual PUB destruction.
    }
    check(control_lookups == lookups, "context refusal happens before root lookup");
    auto const after = world.indexed->owner_counts();
    check(owners.total == after.total && owners.by_role == after.by_role, "context refusal preserves actual owner counts");
    world.gate_free();
}

void contexts(Task& task) {
    ordinary(task);
    World world;
    Snapshot snapshot;
    world.capture(snapshot);
    context_refusal(task, world, snapshot, "no run queues refused", [] { run_queues = false; });
    context_refusal(task, world, snapshot, "unqueryable current refused", [] { queryable = false; });
    context_refusal(task, world, snapshot, "IF off refused", [] { interrupts = false; });
    context_refusal(task, world, snapshot, "preemption nesting refused", [] { preempt = 1; });
    context_refusal(task, world, snapshot, "panic refused", [] { panic = true; });
    context_refusal(task, world, snapshot, "null current refused", [] { current = nullptr; });
    context_refusal(task, world, snapshot, "scheduler transition refused", [&] { task.scheduler_transition_active.store(true); });
    context_refusal(task, world, snapshot, "deferred switch refused", [&] { task.deferred_task_switch = true; });
    context_refusal(task, world, snapshot, "pending block refused", [&] { task.wants_block = true; });
    for (TaskState state : {TaskState::NEW, TaskState::EXITING, TaskState::DEAD})
        context_refusal(task, world, snapshot, "non-ACTIVE Task refused", [&] { task.state.store(state); });
    context_refusal(task, world, snapshot, "exiting Task refused", [&] { task.exit_in_progress.store(true); });
    context_refusal(task, world, snapshot, "closing root accessor refused", [&] { task.usercopy_pagemap_closing.store(true); });
    context_refusal(task, world, snapshot, "nonprocess/nondaemon executor refused", [&] { task.type = TaskType::OTHER; });
    context_refusal(task, world, snapshot, "process outside syscall refused", [&] { task.syscall_account_start_us = 0; });
    context_refusal(task, world, snapshot, "daemon without actual shutdown ownership refused", [&] { task.type = TaskType::DAEMON; });
    context_refusal(task, world, snapshot, "usercopy access window refused", [] { access_window = true; });
    context_refusal(task, world, snapshot, "retirement executor worker refused", [] { retirement_worker = true; });
    ++cases;
    task.type = TaskType::DAEMON;
    task.syscall_account_start_us = 0;
    {
        sched::KernelShutdownHold hold(&task.kernel_shutdown_ownership);
        Pub pub;
        Lease lease;
        check(task.kernel_shutdown_ownership.held() && actual::try_acquire_binding_root(snapshot, pub, lease) == Admission::Acquired,
              "daemon admits only while genuine ShutdownOwnership is held");
        lease.finish();
        retirement_worker = true;
        Lease worker;
        check(actual::try_acquire_binding_root(snapshot, pub, worker) == Admission::Context,
              "worker exclusion still wins over actual daemon shutdown hold");
        retirement_worker = false;
    }
    ordinary(task);
    world.gate_free();
    snapshot.reset();
}

void publication_and_invalid(Task& task) {
    ordinary(task);
    World world;
    Snapshot snapshot;
    world.capture(snapshot);
    Task foreign;
    ++cases;
    {
        Pub pub;
        Lease lease;
        require(actual::try_acquire_binding_root(snapshot, pub, lease) == Admission::Acquired, "owner established before executor substitution");
        current = &foreign;
        check(pub.acquired() && !pub.acquired_by_current_executor(), "actual PUB singleton identity alone does not authorize foreign executor");
        check(!lease.acquired(), "actual lease acquired oracle rejects cross-task PUB borrowing");
        Lease refused;
        check(actual::try_acquire_binding_root(snapshot, pub, refused) == Admission::Context, "actual factory rejects foreign PUB executor");
        current = &task;
        check(pub.acquired_by_current_executor() && lease.acquired(), "restoring exact owner restores held lease oracle");
    }
    world.gate_free();
    ++cases;
    current = nullptr;
    {
        Pub boot;
        current = &task;
        Lease lease;
        check(boot.acquired() && !boot.acquired_by_current_executor(), "boot-null publication cannot become later Task authority");
        check(actual::try_acquire_binding_root(snapshot, boot, lease) == Admission::Context, "early boot guard rejected by actual factory");
    }
    ++cases;
    {
        Snapshot empty;
        Pub pub;
        Lease lease, finished;
        check(actual::try_acquire_binding_root(empty, pub, lease) == Admission::Invalid, "empty sealed snapshot refused");
        finished.finish();
        check(actual::try_acquire_binding_root(snapshot, pub, finished) == Admission::OwnerUsed, "finish of virgin consumes that owner");
    }
    snapshot.reset();
}

void kernel_root_refusal(Task& task) {
    ++cases;
    ordinary(task);
    World world(actual::kernel_pagemap);
    Snapshot snapshot;
    world.capture(snapshot);
    unsigned const lookups = control_lookups;
    {
        Pub pub;
        Lease lease;
        check(actual::try_acquire_binding_root(snapshot, pub, lease) == Admission::Invalid, "actual sealed binding cannot authorize kernel root");
    }
    check(control_lookups == lookups, "kernel root refusal precedes metadata lookup");
    world.gate_free();
    snapshot.reset();
}

template <typename Misuse>
void expect_protocol_death(const char* message, Misuse misuse) {
    ++cases;
    ++deaths;
    std::fflush(nullptr);
    pid_t const child = fork();
    require(child >= 0, "fork bounded misuse probe");
    if (child == 0) {
        signal(SIGILL, SIG_DFL);
        signal(SIGABRT, SIG_DFL);
        alarm(3);
        Task task;
        ordinary(task);
        World world;
        Snapshot snapshot;
        world.capture(snapshot);
        misuse(task, world, snapshot);
        _exit(97);  // Returning or timing out is never protocol-failure proof.
    }
    int status{};
    pid_t waited;
    do { waited = waitpid(child, &status, 0); } while (waited < 0 && errno == EINTR);
    check(waited == child && WIFSIGNALED(status) && (WTERMSIG(status) == SIGILL || WTERMSIG(status) == SIGABRT), message);
}

void protocol_deaths() {
    expect_protocol_death("actual snapshot reset traps before borrowed owner loss", [](Task&, World&, Snapshot& snapshot) {
        Pub pub;
        Lease lease;
        if (actual::try_acquire_binding_root(snapshot, pub, lease) != Admission::Acquired) _exit(98);
        snapshot.reset();
    });
    expect_protocol_death("actual snapshot move traps before borrowed owner transfer", [](Task&, World&, Snapshot& snapshot) {
        Pub pub;
        Lease lease;
        if (actual::try_acquire_binding_root(snapshot, pub, lease) != Admission::Acquired) _exit(98);
        Snapshot stolen(std::move(snapshot));
    });
    expect_protocol_death("actual lease finish traps under foreign scheduled PUB executor", [](Task&, World&, Snapshot& snapshot) {
        Pub pub;
        Lease lease;
        if (actual::try_acquire_binding_root(snapshot, pub, lease) != Admission::Acquired) _exit(98);
        Task foreign;
        current = &foreign;
        lease.finish();
    });
    expect_protocol_death("actual snapshot assignment traps before replacing a borrowed destination", [](Task&, World& world, Snapshot& snapshot) {
        Snapshot source;
        world.capture(source);
        Pub pub;
        Lease lease;
        if (actual::try_acquire_binding_root(snapshot, pub, lease) != Admission::Acquired) _exit(98);
        snapshot = std::move(source);
    });
}
}

int main() {
    using namespace lease_fixture;
    Task task;
    ordinary(task);
    success_and_one_shot(task);
    stale_roots_and_replacement(task);
    real_gate_contention(task);
    contexts(task);
    publication_and_invalid(task);
    kernel_root_refusal(task);
    protocol_deaths();
    check(publication_depth == 0 && snapshot_fixture::lock_depth == 0 && snapshot_fixture::index_depth == 0,
          "terminal PUB/pin/owner seams empty");
    check(snapshot_fixture::queue_empty() && shootdown_services == 0, "no retirement callbacks or blocking gate service left pending");
    for (const auto& gate : actual::user_pagemap_access_gates)
        check(gate.readers.load() == 0 && !gate.teardown.load(), "all permanent gate stripes returned to baseline");
    std::printf("%s binding_root_lease cases=%u assertions=%u deaths=%u failures=%u host_metadata_control_ports=1\n",
                failures == 0 ? "PASS" : "FAIL", cases, assertions, deaths, failures);
    return failures == 0 ? 0 : 3;
}
