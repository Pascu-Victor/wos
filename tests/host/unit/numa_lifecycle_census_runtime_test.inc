#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <type_traits>
#include <vector>
namespace fixture {
unsigned checks{}, lock_depth{}, copy_pins{}, provider_calls{}, copies{}, progress{}, tail_events{};
const void* tail_target{};
void observe_tail(unsigned phase);
void after_native_unlock();
void native_operation_probe();
inline bool native_probe{}, native_gc{true}, native_wait{}, native_root_busy{}, native_exclusive{true};
inline unsigned native_adoptions{}, native_backends{};
void check(bool value, int line = __builtin_LINE()) {
    ++checks;
    if (!value) {
        std::fprintf(stderr, "handler oracle failed line=%d checks=%u\n", line, checks);
        std::abort();
    }
}
void outside() {
    check(lock_depth == 0 && copy_pins == 0);
    ++provider_calls;
}
}  // namespace fixture
#include <platform/mm/lifecycle_census.hpp>
#include <test/numa_lifecycle_census_wire.hpp>
namespace ker::mod::acpi::numa {
constexpr size_t MAX_NODES = 64;
}
namespace ker::mod::mm {
constexpr size_t PHYSICAL_PAGE_OWNER_COUNT = 41;
constexpr size_t KERNEL_STACK_SIZE = 8192;
}  // namespace ker::mod::mm
namespace ker::mod::mm::phys {
constexpr size_t PHYSICAL_RESERVE_KIND_COUNT = 8;
}
namespace ker::mod::mm::numa::balance {
constexpr size_t KIND_COUNT = 8;
}
/* @DECLARATIONS@ */
namespace ker::vfs {
struct File {
    std::atomic<int> refcount{1};
};
class FdReservation;
}  // namespace ker::vfs
namespace ker::mod::sched::resource {
class ProcessReservation;
}
namespace ker::mod::sched::task {
struct Task;
}
namespace ker::mod::mm::file_mapping {
struct Completion {
    RetirementReturnState state{RetirementReturnState::RETURNED};
    int callback_result{};
    auto empty() const { return false; }
    struct Return {
        RetirementReturnState state;
        int callback_result;
    };
    auto query() const {
        fixture::native_operation_probe();
        return Return{state, callback_result};
    }
};
struct UnpublishedMappingRetirement {
    struct Entry {
        Completion completion;
    };
    std::array<Entry, 8> entries{};
    size_t count{};
    int* root_walk{};
    void* retired_root{};
    bool root_walk_started{};
    ker::mod::sched::task::Task* controller{};
    UnpublishedRetirementState state{UnpublishedRetirementState::PREPARED};
    UnpublishedRetirementMode mode{UnpublishedRetirementMode::OWNED_SLOT};
    int first_close_result{};
    ker::mod::sched::task::Task* deferred_subject{};
    UnpublishedMappingRetirement* deferred_next{};
    bool deferred_claimed{};
    UnpublishedMappingRetirement* active_next{};
    ker::mod::sched::task::Task* active_subject{};
    ~UnpublishedMappingRetirement() {
        if (fixture::tail_target != nullptr) fixture::observe_tail(2);
    }
};
}  // namespace ker::mod::mm::file_mapping
namespace ker::mod::dbg {
[[noreturn]] void panic_handler(const char*) { std::abort(); }
}  // namespace ker::mod::dbg
namespace ker::mod::mm::phys {
void page_free(void*) { fixture::observe_tail(1); }
}  // namespace ker::mod::mm::phys
namespace fixture {
ker::mod::sched::task::Task* current{};
bool can_query{true}, irq{true}, read_ok{true}, write_ok{true}, privileged{true};
uint32_t ids[6]{};
uint64_t preempt{}, root_serial{7}, now{100}, ready_mask{255}, cores{8}, vmap_debt{}, active_tasks{8};
unsigned physical_calls{};
bool physical_churn{}, owner_churn{};
ker::test::numa_lifecycle::MmDebtSnapshot mm{};
}  // namespace fixture
namespace ker::mod::security {
enum class Authority { KERNEL_DEBUG };
struct Ids {
    uint32_t real{}, effective{}, saved{};
};
struct Credentials {
    Ids user() const { return {fixture::ids[0], fixture::ids[1], fixture::ids[2]}; }
    Ids group() const { return {fixture::ids[3], fixture::ids[4], fixture::ids[5]}; }
    bool allows(Authority) const { return fixture::privileged; }
};
}  // namespace ker::mod::security
namespace ker::mod::sched::task {
enum class TaskType { PROCESS, DAEMON };
enum class TaskState { ACTIVE, DEAD };
struct Task {
    uint64_t pid{50}, syscall_account_start_us{1};
    char* name{};
    struct Context {
        uint64_t syscall_kernel_stack{};
    } context;
    void release() { fixture::observe_tail(3); }
    ~Task() {
        if (fixture::tail_target == this) {
            fixture::check(name == nullptr && context.syscall_kernel_stack == 0);
            fixture::observe_tail(4);
        }
    }
    TaskType type{TaskType::PROCESS};
    bool is_thread{}, wki_proxy_task{}, deferred_task_switch{}, wants_block{};
    std::atomic<TaskState> state{TaskState::ACTIVE};
    std::atomic<bool> scheduler_transition_active{}, shares_user_pagemap{};
    std::atomic<uint64_t> resource_thread_owners{}, resource_cpu_threads{};
    int root{};
    int* pagemap{&root};
    static constexpr unsigned FD_TABLE_SIZE = 256;
    struct Table {
        std::array<void*, 256> files{};
        std::array<bool, 256> reservations{};
        size_t size() const {
            size_t n = 0;
            for (auto* p : files) n += p != nullptr;
            return n;
        }
        size_t reserved_count() const {
            size_t n = 0;
            for (bool b : reservations) n += b;
            return n;
        }
        bool reserved(unsigned i) const { return reservations[i]; }
        void* lookup(unsigned i) const { return files[i]; }
    } fd_table;
    ker::mod::sys::Spinlock fd_table_lock;
    std::array<bool, 256> cloexec{};
    bool get_fd_cloexec(unsigned i) const { return cloexec[i]; }
    ker::vfs::FdReservation* pending_fd_reservations{};
    ker::mod::sched::resource::ProcessReservation* pending_process_reservations{};
    void* pending_wki_fd_snapshots{};
    void* pending_wki_submissions{};
    void* pending_wki_submit_buffers{};
    void* pending_wki_filesystems{};
    void* pending_signal_targets{};
    std::atomic<Task*> owned_unpublished_process{};
    std::atomic<void*> owned_exec_preparation{}, owned_exec_arguments{}, pending_exec_handoff{}, active_exec_handoff{};
    std::atomic<ker::mod::mm::file_mapping::UnpublishedMappingRetirement*> native_file_mapping_retirement{};
    std::atomic<bool> unpublished_teardown_in_progress{}, exec_preparation_teardown_in_progress{}, exit_in_progress{},
        process_exit_requested{};
    ker::mod::sys::Spinlock resource_lock;
    bool resource_cpu_admission_closed{}, resource_exec_admission_closed{}, resource_exec_safe_wait{};
    std::atomic<uint64_t> resource_exec_quiescence_epoch{};
    uint64_t resource_exec_quiescent_threads{};
    std::atomic<uint32_t> ref_count{2};
    auto credential_owner() { return this; }
    auto credentials() const { return ker::mod::security::Credentials{}; }
};
}  // namespace ker::mod::sched::task
/* @SCOPE@ */
namespace fixture {
ker::mod::mm::lifecycle_storage::Snapshot storage{};
ker::vfs::SelftestMountCensus mounts{};
ker::mod::sched::resource::SelftestPopulationCensus population{};
ker::mod::sched::resource::SelftestServiceCensus services{};
ker::syscall::process::child_events::SelftestLifecycleCensus children{};
ker::syscall::process::child_events::SelftestOwnerLifecycleCensus owner_children{};
ker::syscall::shm::SelftestShmCensus ipc{};
ker::mod::mm::virt::SelftestRootCensus roots{};
ker::vfs::SelftestFdCensus descriptors{};
ker::mod::mm::file_mapping::LifetimeRetirementSnapshot executor{};
ker::test::numa_lifecycle::NativeDebtSnapshot native{};
ker::test::numa_lifecycle::RegistrySnapshot registry{};
}  // namespace fixture
namespace ker::test::lifecycle60 {
constexpr size_t CENSUS_CPUS = 8;
class CensusLease {
   public:
    explicit operator bool() const { return false; }
    auto subject() const -> ker::mod::sched::task::Task* { return nullptr; }
    uint64_t cookie() const { return 0; }
};
enum class GateResult { WAIT, ACCEPT, REFUSE };
GateResult settlement{GateResult::ACCEPT};
auto ntp_boot_settlement() noexcept {
    fixture::outside();
    return settlement;
}
/* @SCALARS@ */
}  // namespace ker::test::lifecycle60
namespace ker::mod::mm::lifecycle_storage {
auto snapshot() noexcept {
    fixture::outside();
    return fixture::storage;
}
}  // namespace ker::mod::mm::lifecycle_storage
namespace ker::vfs {
auto selftest_mount_census() {
    fixture::outside();
    return fixture::mounts;
}
class FdReservation {
   public:
    FdReservation* m_next{};

   private:
    static void collect_selftest(mod::sched::task::Task&, uint64_t, SelftestFdCensus&);
    friend auto selftest_fd_census_parked(const ker::test::lifecycle60::CensusLease&) -> SelftestFdCensus;
    friend void selftest_fd_census_current(const ker::test::numa_lifecycle::CurrentCensusScope&, SelftestFdCensus&);
};
/* @FD_READERS@ */
enum class SelftestWritebackPreparation { WAIT, READY, FAILED };
SelftestWritebackPreparation preparation{SelftestWritebackPreparation::READY};
auto selftest_prepare_dirty_writeback_worker() noexcept {
    fixture::outside();
    return preparation;
}
}  // namespace ker::vfs
namespace ker::mod::sched::resource {
auto selftest_service_census() {
    fixture::outside();
    return fixture::services;
}
class ProcessReservation {
   public:
    ProcessReservation* m_next{};

   private:
    static void collect_selftest(task::Task&, uint64_t, SelftestPopulationCensus&);
    friend auto selftest_population_census(const ker::test::lifecycle60::CensusLease&) -> SelftestPopulationCensus;
    friend void selftest_population_census_current(const ker::test::numa_lifecycle::CurrentCensusScope&, SelftestPopulationCensus&);
};
struct Charge {
    Charge* next{};
    task::Task* subject{};
    uint32_t uid{};
};
Charge* population{};
mm::lifecycle_census::Epoch selftest_population_epoch;
mm::lifecycle_census::OwnerLock population_lock{selftest_population_epoch};
/* @POP_READERS@ */
}  // namespace ker::mod::sched::resource
namespace ker::syscall::process::child_events {
// Existing handler fixture supplies controlled provider snapshots. The new
// semantic-provider runtime fixture executes the actual child/root writers.
void selftest_enable_current_lifecycle_census() noexcept { fixture::outside(); }
void selftest_current_lifecycle_census_into(SelftestLifecycleCensus& out, ker::mod::mm::lifecycle_census::Stamp& before) noexcept {
    fixture::outside();
    out = fixture::children;
    before = out.stamp;
}
void selftest_current_owner_lifecycle_census_into(ker::mod::sched::task::Task&, SelftestOwnerLifecycleCensus& out,
                                                  ker::mod::mm::lifecycle_census::Stamp& before) noexcept {
    fixture::outside();
    out = fixture::owner_children;
    before = out.stamp;
}
auto selftest_lifecycle_census() {
    fixture::outside();
    return fixture::children;
}
auto selftest_owner_lifecycle_census(ker::mod::sched::task::Task&) {
    fixture::outside();
    return fixture::owner_children;
}
}  // namespace ker::syscall::process::child_events
namespace ker::syscall::shm {
auto selftest_shm_census() {
    fixture::outside();
    return fixture::ipc;
}
}  // namespace ker::syscall::shm
namespace ker::mod::mm::virt {
auto begin_prepared_destroy_user_space(int&, void*, uint64_t, const char*, const char*, bool) {
    fixture::native_operation_probe();
    return !fixture::native_root_busy;
}
auto destroy_user_space_budgeted(void*, int) {
    fixture::native_operation_probe();
    return true;
}
auto finish_destroy_user_space_and_release_pagemap(void*) {
    fixture::native_operation_probe();
    return true;
}
void destroy_user_space_budget_state_destroy(void*) { fixture::native_operation_probe(); }
void selftest_enable_current_root_census() noexcept { fixture::outside(); }
void selftest_current_root_census_into(SelftestRootCensus& out, ker::mod::mm::lifecycle_census::Stamp& before) noexcept {
    fixture::outside();
    out = fixture::roots;
    before = out.stamp;
}
auto selftest_root_census() {
    fixture::outside();
    return fixture::roots;
}
auto capture_user_root_incarnation(int*, uint64_t& empty) noexcept {
    fixture::outside();
    fixture::check(empty == 0);
    empty = fixture::root_serial;
    return empty != 0;
}
auto kernel_vmap_pending_free_pages() {
    fixture::outside();
    return fixture::vmap_debt;
}
auto drain_kernel_vmap_frees_bounded(size_t n) {
    fixture::outside();
    fixture::check(n == 128);
    ++fixture::progress;
    return 0;
}
}  // namespace ker::mod::mm::virt
/* @OWNER@ */
namespace ker::mod::mm::file_mapping {
class LifetimeRetirementExecutor {
   public:
    static auto is_current_worker() { return false; }
    static auto snapshot() {
        fixture::outside();
        return fixture::executor;
    }
};
}  // namespace ker::mod::mm::file_mapping
namespace ker::mod::sched {
auto can_query_current_task() { return fixture::can_query; }
auto get_current_task() { return fixture::current; }
auto preempt_count() { return fixture::preempt; }
auto interrupts_enabled() { return fixture::irq; }
auto selftest_task_context_ready_mask() { return fixture::ready_mask; }
auto get_active_task_count() {
    fixture::outside();
    return fixture::active_tasks;
}
auto get_scoped_pid_task_count() {
    fixture::outside();
    return 8;
}
namespace threading {
auto get_active_thread_count() {
    fixture::outside();
    return 8;
}
}  // namespace threading
namespace pid_ns {
auto live_namespace_count() {
    fixture::outside();
    return 0;
}
}  // namespace pid_ns
namespace ipc {
auto live_namespace_count() {
    fixture::outside();
    return 0;
}
}  // namespace ipc
namespace compartment {
struct Status {
    uint64_t pending{}, completed{}, exit_requests{};
};
auto status() {
    fixture::outside();
    return Status{};
}
}  // namespace compartment
void request_gc();
auto is_current_gc_worker() { return fixture::native_gc; }
struct NativeShutdownHold {};
auto hold_kernel_shutdown() { return NativeShutdownHold{}; }
struct PreemptGuard {};
namespace task {
mm::lifecycle_census::Epoch native_debt_epoch;
mm::lifecycle_census::OwnerLock native_mapping_admission_lock{native_debt_epoch}, native_retirement_queue_lock{native_debt_epoch};
std::atomic<uint64_t> native_return_tails{};
std::atomic<bool> native_debt_arithmetic_valid{true};
size_t native_mapping_constructions{}, native_retirement_queue_outstanding{};
mm::file_mapping::UnpublishedMappingRetirement* native_mapping_active_records{};
mm::file_mapping::UnpublishedMappingRetirement* native_retirement_queue_head{};
mm::file_mapping::UnpublishedMappingRetirement* native_retirement_queue_tail{};
bool native_mapping_construction_closed{};
auto unpublished_process_is_exclusively_owned(Task*) { return fixture::native_exclusive; }
void teardown_unpublished_process_resources(Task*, mm::file_mapping::UnpublishedMappingRetirement*) { fixture::native_operation_probe(); }
/* @NATIVE_READERS@ */
}  // namespace task
struct Trace {
    uint64_t gc_deferred_queued{}, gc_deferred_completed{}, gc_deferred_depth{};
};
auto get_scheduler_trace_stats(int) {
    fixture::outside();
    return Trace{};
}
struct Queue {
    uint64_t active_task_count{}, expired_task_count{}, wait_queue_count{}, timer_queue_depth{};
};
auto get_run_queue_stats(size_t cpu) {
    fixture::outside();
    fixture::check(cpu < 8);
    return Queue{};
}
auto get_expired_task_refcounts(size_t cpu, uint64_t* pid, uint32_t* refs, size_t count) {
    fixture::outside();
    fixture::check(cpu < 8 && count == 1);
    *pid = 0;
    *refs = 0;
    return 0;
}
void request_gc() {
    fixture::outside();
    ++fixture::progress;
}
}  // namespace ker::mod::sched

namespace ker::mod::mm::phys {
void get_alloc_stats_snapshot(AllocStatsSnapshot& out) {
    fixture::outside();
    out = {};
}
}  // namespace ker::mod::mm::phys
namespace ker::vfs::mount_ns {
auto live_namespace_count() {
    fixture::outside();
    return 0;
}
}  // namespace ker::vfs::mount_ns
namespace ker::mod::smt {
auto get_core_count() { return fixture::cores; }
}  // namespace ker::mod::smt
namespace ker::mod::time {
auto get_us() { return fixture::now; }
}  // namespace ker::mod::time
namespace ker::mod::sys {
class SyscallPreparationScope {
    bool ready_{};

   public:
    explicit SyscallPreparationScope(ker::mod::sched::task::Task& a)
        : ready_(fixture::can_query && fixture::current == &a && a.syscall_account_start_us && fixture::preempt == 0) {
        if (ready_) fixture::irq = true;
    }
    auto ready() const { return ready_; }
};
namespace usercopy {
constexpr uint64_t USER_ADDR_LIMIT = 0x800000000000ULL;
auto range_valid(uint64_t start, size_t size) { return start != 0 && start < USER_ADDR_LIMIT && size <= USER_ADDR_LIMIT - start; }
auto copy_from_task_mapped(ker::mod::sched::task::Task&, uint64_t address, void* out, size_t bytes) {
    fixture::outside();
    ++fixture::copies;
    if (!fixture::read_ok) return false;
    ++fixture::copy_pins;
    std::memcpy(out, reinterpret_cast<void*>(address), bytes);
    --fixture::copy_pins;
    return true;
}
auto copy_to_task_mapped(ker::mod::sched::task::Task&, uint64_t address, const void* in, size_t bytes) {
    fixture::outside();
    ++fixture::copies;
    if (!fixture::write_ok) return false;
    ++fixture::copy_pins;
    std::memcpy(reinterpret_cast<void*>(address), in, bytes);
    --fixture::copy_pins;
    return true;
}
auto copy_to_task(ker::mod::sched::task::Task&, uint64_t, const void*, size_t) {
    fixture::check(false);
    return false;
}
}  // namespace usercopy
}  // namespace ker::mod::sys
/* @ACTIVITY_HELPERS@ */
namespace ker::test::numa_lifecycle::mm_debt {
// Controlled diagnostic endpoint: no live hook stream in this handler fixture.
// Actual checked interpreter is extracted above; UNKNOWN grants no authority.
void activity_snapshot(MmActivitySnapshot& out) noexcept {
    fixture::outside();
    out = {};
}
void snapshot(MmDebtSnapshot& out) noexcept {
    fixture::outside();
    out = fixture::mm;
}
}  // namespace ker::test::numa_lifecycle::mm_debt
namespace ker::syscall::vmem {
struct Row {
    void* pagemap{};
    void* file{};
    uint64_t start{}, length{}, file_offset{};
    bool writeback{};
};
std::vector<Row> g_file_mmap_ranges;
struct Entry {
    Entry* registry_next{};
    bool registered{};
    void* root{};
    void* lifetime{};
    void* binding{};
    uint64_t start{}, end{}, file_offset{};
};
Entry* g_native_file_mmap_ranges{};
struct Mutex {
    bool busy{};
    bool try_lock() {
        if (busy) return false;
        fixture::outside();
        ++fixture::lock_depth;
        return true;
    }
    void unlock() {
        fixture::check(fixture::lock_depth == 1);
        --fixture::lock_depth;
    }
} g_file_mmap_ranges_lock;
/* @REGISTRY_READER@ */
}  // namespace ker::syscall::vmem
namespace ker::mod::mm::numa::balance {
struct Workspace {};
auto snapshot(Snapshot& out, Workspace&, uint64_t max) noexcept -> Result {
    fixture::outside();
    fixture::check(max == 8 * 1024 * 1024);
    ++fixture::physical_calls;
    std::memset(&out, 0, sizeof(out));
    out.complete = true;
    out.result.error = Error::NONE;
    out.node_count = 1;
    out.zone_count = 1;
    out.scanned_frames = 1000;
    out.global.managed_pages = 1000;
    out.global.free_pages = 998;
    out.global.allocator_metadata_pages = 1;
    out.global.zone_descriptor_pages = 1;
    out.global.identity_pages = 1000;
    out.global.zone_count = 1;
    out.scanned.managed_pages = 1000;
    out.scanned.free_pages = 998;
    out.scanned.allocator_metadata_pages = 1;
    out.scanned.zone_descriptor_pages = 1;
    out.nodes[0] = out.scanned;
    if (fixture::physical_churn && fixture::physical_calls % 2 == 0) {
        --out.scanned.free_pages;
        --out.nodes[0].free_pages;
        --out.global.free_pages;
    }
    return out.result;
}
// Disclosed physical/MM endpoint for the complete handler fixture. Actual
// all-allocator-lock scanning and named MM capture have their separate actual-
// body coherent-census test; this wrapper preserves this fixture's old vectors.
auto selftest_snapshot_with_mm(Snapshot& out, Workspace& work, ker::test::numa_lifecycle::MmDebtSnapshot& mm,
                               ker::test::numa_lifecycle::MmDebtCaptureScratch& capture, uint64_t max) noexcept -> Result {
    fixture::outside();
    mm = fixture::mm;
    capture.before = capture.after = {mm.version_end, mm.inflight, mm.valid != 0};
    return snapshot(out, work, max);
}
}  // namespace ker::mod::mm::numa::balance
/* @HANDLER@ */
namespace fixture {
void native_operation_probe() {
    if (!native_probe) return;
    outside();
    auto const stamp = ker::mod::sched::task::current_native_semantic_epoch.snapshot();
    check(stamp.valid && stamp.inflight != 0);
    ++native_backends;
}
void after_native_unlock() {
    if (!native_probe) return;
    using namespace ker::mod::sched::task;
    auto const stamp = current_native_semantic_epoch.snapshot();
    check(stamp.valid && stamp.inflight != 0);
    ++native_adoptions;
}
void observe_tail(unsigned phase) {
    outside();
    check(ker::mod::sched::task::native_return_tails.load() == 1);
    check(phase >= 1 && phase <= 4);
    ++tail_events;
}
using namespace ker::test::numa_lifecycle;
ker::mod::sched::task::Task actor;
Report output;
Request request;
void reset() {
    for (auto& id : ids) id = 0;
    current = &actor;
    can_query = true;
    irq = true;
    read_ok = write_ok = privileged = true;
    preempt = 0;
    root_serial = 7;
    now = 100;
    ready_mask = 255;
    cores = 8;
    vmap_debt = 0;
    active_tasks = 8;
    physical_calls = provider_calls = copies = progress = 0;
    physical_churn = owner_churn = false;
    actor.fd_table = {};
    actor.cloexec = {};
    actor.pending_fd_reservations = nullptr;
    actor.pending_process_reservations = nullptr;
    ker::mod::sched::resource::population = nullptr;
    ker::mod::sched::task::native_return_tails = 0;
    ker::mod::sched::task::native_debt_arithmetic_valid = true;
    ker::mod::sched::task::native_mapping_constructions = ker::mod::sched::task::native_retirement_queue_outstanding = 0;
    ker::mod::sched::task::native_mapping_active_records = nullptr;
    ker::mod::sched::task::native_retirement_queue_head = ker::mod::sched::task::native_retirement_queue_tail = nullptr;
    ker::mod::sched::task::native_mapping_construction_closed = false;
    ker::syscall::vmem::g_file_mmap_ranges.clear();
    ker::syscall::vmem::g_native_file_mmap_ranges = nullptr;
    ker::syscall::vmem::g_file_mmap_ranges_lock.busy = false;
    actor.state.store(ker::mod::sched::task::TaskState::ACTIVE);
    actor.type = ker::mod::sched::task::TaskType::PROCESS;
    actor.is_thread = false;
    actor.syscall_account_start_us = 1;
    actor.shares_user_pagemap.store(false);
    actor.resource_thread_owners.store(0);
    actor.resource_cpu_threads.store(0);
    actor.resource_exec_admission_closed = false;
    actor.owned_unpublished_process.store(nullptr);
    actor.scheduler_transition_active.store(false);
    actor.pending_wki_submissions = nullptr;
    storage = {};
    storage.sequence = 1;
    storage.valid = true;
    storage.task.live_blocks = 8;
    storage.file.live_blocks = 5;
    mounts = {};
    mounts.stamp = {1, 0, true};
    mounts.complete = true;
    population = {};
    population.stamp = {1, 0, true};
    population.complete = true;
    services = {};
    services.stamp = {1, 0, true};
    services.complete = true;
    children = {};
    children.stamp = {1, 0, true};
    children.complete = true;
    owner_children = {};
    owner_children.stamp = {1, 0, true};
    owner_children.complete = true;
    owner_children.parent_link = true;
    ipc = {};
    ipc.stamp = {1, 0, true};
    ipc.complete = true;
    roots = {};
    roots.stamp = {1, 0, true};
    roots.complete = true;
    roots.roots = {3, 1, 7};
    descriptors = {};
    descriptors.complete = true;
    executor = {};
    executor.initialized = executor.available = executor.admission_open = true;
    native = {};
    native.stamp = {1, 0, true};
    native.flags = NATIVE_COMPLETE | NATIVE_ARITHMETIC_VALID;
    registry = {};
    registry.stamp = {1, 0, true};
    registry.flags = REGISTRY_COMPLETE;
    mm = {};
    mm.version_begin = mm.version_end = 1;
    mm.valid = 1;
    ker::test::lifecycle60::settlement = ker::test::lifecycle60::GateResult::ACCEPT;
    ker::vfs::preparation = ker::vfs::SelftestWritebackPreparation::READY;
    session = {};
    boot_enabled.store(true);
    prefault_plain();
    output = {};
    request = {1, sizeof(Request), 0, 0, 0, 19, reinterpret_cast<uint64_t>(&output), sizeof(Report), {0, 0}};
}
auto call(Phase phase) -> int64_t {
    request.phase = static_cast<uint32_t>(phase);
    auto result = static_cast<int64_t>(dispatch_current_census(reinterpret_cast<uint64_t>(&request), sizeof(Request), 0, 0));
    if (result == 0) request.cookie = output.header.cookie;
    return result;
}
void accept(Phase p) {
    check(call(p) == 0);
    check(output.header.outcome == 1 && output.header.coverage == 255);
}
void begin() {
    accept(Phase::PREPARE);
    ++now;
    accept(Phase::BEGIN);
    ++now;
}
}  // namespace fixture
int main() {
    using namespace fixture;
    using namespace ker::test::numa_lifecycle;
    reset();
    begin();
    accept(Phase::END);
    check(session.flags & SESSION_ENDED);
    check(physical_calls == 6);
    check(lock_depth == 0 && copy_pins == 0);
    reset();
    check(!std::is_constructible_v<CurrentCensusScope, ker::mod::sched::task::Task&, uint64_t, uint64_t>);
    for (unsigned condition = 0; condition < 10; ++condition) {
        reset();
        switch (condition) {
            case 0:
                privileged = false;
                break;
            case 1:
                preempt = 1;
                break;
            case 2:
                actor.is_thread = true;
                break;
            case 3:
                actor.shares_user_pagemap = true;
                break;
            case 4:
                actor.resource_thread_owners = 1;
                break;
            case 5:
                actor.owned_unpublished_process = reinterpret_cast<ker::mod::sched::task::Task*>(1);
                break;
            case 6:
                ready_mask = 127;
                break;
            case 7:
                actor.resource_exec_admission_closed = true;
                break;
            case 8:
                actor.pending_wki_submissions = reinterpret_cast<void*>(1);
                break;
            case 9:
                actor.syscall_account_start_us = 0;
                break;
        }
        check(call(Phase::PREPARE) == -EPERM);
        check(session.cookie == 0);
    }
    for (unsigned id = 0; id < 6; ++id) {
        reset();
        ids[id] = 100;
        check(call(Phase::PREPARE) == -EPERM);
        check(session.cookie == 0);
    }
    reset();
    request.bytes = 0;
    check(call(Phase::PREPARE) == -EINVAL);
    check(session.cookie == 0);
    reset();
    read_ok = false;
    check(call(Phase::PREPARE) == -EFAULT);
    check(session.cookie == 0);
    reset();
    begin();
    ++root_serial;
    check(call(Phase::END) == -EPERM);
    check(output.header.outcome == 2 && output.header.reason == static_cast<uint64_t>(Reason::ROOT_CHANGED));
    reset();
    begin();
    physical_churn = true;
    check(call(Phase::END) == 0 && output.header.outcome == 0);
    check(progress == 2);
    reset();
    begin();
    active_tasks = 9;
    check(call(Phase::END) == 0 && output.header.outcome == 0 && output.header.reason == static_cast<uint64_t>(Reason::BASELINE_DEBT));
    reset();
    begin();
    storage.file.live_blocks++;
    check(call(Phase::END) == 0 && output.header.outcome == 0);
    reset();
    begin();
    ker::vfs::File f;
    actor.fd_table.files[17] = &f;
    check(call(Phase::END) == 0 && output.header.outcome == 0);
    reset();
    begin();
    mm.swap_objects = 1;
    check(call(Phase::END) == 0 && output.header.outcome == 0);
    reset();
    begin();
    mm.pin_returns = 1;
    check(call(Phase::END) == 0 && output.header.outcome == 0);
    reset();
    begin();
    mm.valid = 0;
    check(call(Phase::END) == 0 && output.header.outcome == 0);
    reset();
    begin();
    ker::mod::sched::task::adjust_native_return_tail(true);
    check(call(Phase::END) == 0 && output.header.outcome == 0);
    reset();
    begin();
    ker::mod::sched::task::native_debt_arithmetic_valid = false;
    check(call(Phase::END) == 0 && output.header.outcome == 2);
    reset();
    begin();
    ker::syscall::vmem::g_file_mmap_ranges_lock.busy = true;
    check(call(Phase::END) == 0 && output.header.outcome == 0);
    reset();
    begin();
    ker::syscall::vmem::g_file_mmap_ranges.resize(4097);
    check(call(Phase::END) == 0 && output.header.outcome == 2);
    reset();
    begin();
    executor.observer_handles = 1;
    check(call(Phase::END) == 0 && output.header.outcome == 0);
    reset();
    begin();
    vmap_debt = 1;
    check(call(Phase::END) == 0 && output.header.outcome == 0);
    reset();
    begin();
    owner_children.zombie_children = 1;
    check(call(Phase::END) == 0 && output.header.outcome == 0);
    reset();
    begin();
    write_ok = false;
    check(call(Phase::END) == -EFAULT);
    check(session.flags & SESSION_POISONED);
    write_ok = true;
    check(call(Phase::END) == 0 && output.header.outcome == 2);
    reset();
    begin();
    now = session.deadline;
    check(call(Phase::END) == 0 && output.header.outcome == 2 && output.header.reason == static_cast<uint64_t>(Reason::DEADLINE));
    reset();
    accept(Phase::PREPARE);
    check(call(Phase::END) == 0 && output.header.outcome == 2);
    reset();
    ker::vfs::preparation = ker::vfs::SelftestWritebackPreparation::WAIT;
    check(call(Phase::PREPARE) == 0 && output.header.outcome == 0 && physical_calls == 0);
    reset();
    ker::vfs::preparation = ker::vfs::SelftestWritebackPreparation::FAILED;
    check(call(Phase::PREPARE) == 0 && output.header.outcome == 2);
    reset();
    for (bool detached : {false, true}) {
        auto* child = new ker::mod::sched::task::Task;
        child->name = new char[8];
        child->context.syscall_kernel_stack = ker::mod::mm::KERNEL_STACK_SIZE + 4096;
        auto* record = new ker::mod::mm::file_mapping::UnpublishedMappingRetirement;
        record->active_subject = child;
        ker::mod::sched::task::native_mapping_active_records = record;
        child->native_file_mapping_retirement = record;
        actor.unpublished_teardown_in_progress = true;
        tail_target = child;
        auto before = tail_events;
        ker::mod::sched::task::actual_return_tail(&actor, child, record, detached);
        tail_target = nullptr;
        check(tail_events == before + 4 && ker::mod::sched::task::native_mapping_active_records == nullptr);
        check(actor.unpublished_teardown_in_progress.load() == detached);
        actor.unpublished_teardown_in_progress = false;
        ker::mod::sched::task::selftest_native_debt_snapshot(native);
        check(native.return_tails == 0 && (native.flags & NATIVE_ARITHMETIC_VALID));
    }
    // These are the actual new typed/native/registry reader bodies, not provider replacements.
    reset();
    ker::vfs::File shared;
    shared.refcount = 2;
    actor.fd_table.files[3] = actor.fd_table.files[7] = &shared;
    accept(Phase::PREPARE);
    check(output.totals.installed_fds == 2 && output.totals.distinct_files == 1);
    ++now;
    accept(Phase::BEGIN);
    actor.cloexec[7] = true;
    ++now;
    check(call(Phase::END) == 0 && output.header.outcome == 0);
    reset();
    shared.refcount = 1;
    actor.fd_table.files[3] = actor.fd_table.files[7] = &shared;
    check(call(Phase::PREPARE) == 0 && output.header.outcome == 0);  // Actual multiplicity exceeds real refs.
    reset();
    ker::vfs::FdReservation pending;
    pending.m_next = &pending;
    actor.pending_fd_reservations = &pending;
    check(call(Phase::PREPARE) == 0 && output.header.outcome == 0);  // Actual bounded256 list refuses cyclic prefix.
    reset();
    ker::mod::sched::resource::ProcessReservation pending_process;
    pending_process.m_next = &pending_process;
    actor.pending_process_reservations = &pending_process;
    check(call(Phase::PREPARE) == 0 && output.header.outcome == 0);  // Actual bounded65536 process list refuses prefix.
    reset();
    ker::mod::sched::resource::Charge charge;
    charge.uid = 100;
    ker::mod::sched::resource::population = &charge;
    check(call(Phase::PREPARE) == 0 && output.header.outcome == 0);  // Real reservation debt, no charge publication.
    reset();
    charge.subject = &actor;
    ker::mod::sched::resource::population = &charge;
    accept(Phase::PREPARE);
    check(output.totals.population_attached == 1);
    reset();
    ker::mod::sched::task::native_mapping_constructions = 1;
    check(call(Phase::PREPARE) == 0 && output.header.outcome == 0);
    reset();
    ker::mod::sched::task::native_retirement_queue_outstanding = 1;
    check(call(Phase::PREPARE) == 0 && output.header.outcome == 0);  // Claimed prefix needs no list head.
    reset();
    ker::mod::sched::task::native_retirement_queue_tail =
        reinterpret_cast<ker::mod::mm::file_mapping::UnpublishedMappingRetirement*>(&actor);
    check(call(Phase::PREPARE) == 0 && output.header.outcome == 2);  // Inconsistent head/tail.
    reset();
    ker::mod::sched::task::native_mapping_active_records =
        reinterpret_cast<ker::mod::mm::file_mapping::UnpublishedMappingRetirement*>(&actor);
    check(call(Phase::PREPARE) == 0 && output.header.outcome == 0);
    reset();
    ker::mod::sched::task::native_mapping_construction_closed = true;
    check(call(Phase::PREPARE) == 0 && output.header.outcome == 0);
    reset();
    ker::mod::sched::task::adjust_native_return_tail(true);
    ker::mod::sched::task::selftest_native_debt_snapshot(native);
    check(native.return_tails == 1 && (native.flags & NATIVE_ARITHMETIC_VALID));
    ker::mod::sched::task::adjust_native_return_tail(false);
    ker::mod::sched::task::selftest_native_debt_snapshot(native);
    check(native.return_tails == 0);
    ker::mod::sched::task::adjust_native_return_tail(false);
    ker::mod::sched::task::selftest_native_debt_snapshot(native);
    check(!(native.flags & NATIVE_ARITHMETIC_VALID));
    reset();
    ker::syscall::vmem::g_file_mmap_ranges.push_back({&actor, &shared, 0x10000, 4096, 9, true});
    mm.legacy_file_registry_rows = 1;
    begin();
    ker::syscall::vmem::g_file_mmap_ranges[0].file_offset = 10;
    check(call(Phase::END) == 0 && output.header.outcome == 0);  // Full geometry, not counts/hash.
    reset();
    for (size_t i = 0; i < 4096; ++i)
        ker::syscall::vmem::g_file_mmap_ranges.push_back({&actor, &shared, 0x10000 + i * 4096, 4096, i * 4096, false});
    ker::syscall::vmem::selftest_file_registry_snapshot(registry);
    check(registry.flags == REGISTRY_COMPLETE && registry.row_count == 4096 && registry.rows[4095].file_offset == 4095 * 4096);
    ker::syscall::vmem::g_file_mmap_ranges.push_back({&actor, &shared, 0x2000000, 4096, 0, false});
    ker::syscall::vmem::selftest_file_registry_snapshot(registry);
    check(registry.flags & REGISTRY_OVERFLOW);
    reset();
    ker::syscall::vmem::g_file_mmap_ranges.push_back({&actor, &shared, UINT64_MAX, 4096, 0, false});
    ker::syscall::vmem::selftest_file_registry_snapshot(registry);
    check(registry.flags & REGISTRY_INVALID);
    reset();
    ker::syscall::vmem::Entry entry{nullptr, true, &actor, &shared, &entry, 0x10000, 0x12000, 17};
    ker::syscall::vmem::g_native_file_mmap_ranges = &entry;
    mm.native_file_registry_rows = 1;
    begin();
    entry.end += 4096;
    check(call(Phase::END) == 0 && output.header.outcome == 0);
    entry.registry_next = &entry;
    ker::syscall::vmem::selftest_file_registry_snapshot(registry);
    check(registry.flags & REGISTRY_OVERFLOW);
    reset();
    mm.legacy_file_registry_rows = 1;
    check(call(Phase::PREPARE) == 0 && output.header.outcome == 0);  // Retained but not published prefix.
    reset();
    {
        using namespace ker::mod::sched::task;
        namespace fm = ker::mod::mm::file_mapping;
        auto const clean = current_native_semantic_epoch.snapshot();
        auto const legacy = native_debt_epoch.snapshot();
        NativeDebtSnapshot before_poll{}, after_poll{};
        selftest_current_native_debt_snapshot(before_poll);
        bool root_active = true;
        for (unsigned i = 0; i < 3; ++i) {
            check(!native_unpublished_retirements_pending());
            check(!process_native_unpublished_retirements_slice(root_active) && !root_active);
        }
        auto const observed = current_native_semantic_epoch.snapshot();
        selftest_current_native_debt_snapshot(after_poll);
        check(ker::mod::mm::lifecycle_census::unchanged(before_poll.stamp, after_poll.stamp));
        check(ker::mod::mm::lifecycle_census::unchanged(clean, observed));
        check(native_debt_epoch.snapshot().version > legacy.version);
        check(begin_native_file_mapping_construction());
        end_native_file_mapping_construction();
        check(!ker::mod::mm::lifecycle_census::unchanged(clean, current_native_semantic_epoch.snapshot()));
        auto const before_close = current_native_semantic_epoch.snapshot();
        check(close_native_file_mapping_admission());
        auto const closed = current_native_semantic_epoch.snapshot();
        check(closed.version > before_close.version && close_native_file_mapping_admission());
        check(ker::mod::mm::lifecycle_census::unchanged(closed, current_native_semantic_epoch.snapshot()));
        native_mapping_construction_closed = false;  // Explicit isolated Host endpoint reset.
        auto* child = new Task;
        child->name = new char[8];
        child->context.syscall_kernel_stack = ker::mod::mm::KERNEL_STACK_SIZE + 4096;
        auto* record = new fm::UnpublishedMappingRetirement;
        record->controller = &actor;
        record->active_subject = child;
        record->count = 1;
        record->entries[0].completion.state = fm::RetirementReturnState::PENDING;
        native_mapping_active_records = record;
        child->native_file_mapping_retirement = record;
        native_probe = true;
        defer_transferred_native_unpublished_process(actor, *child);
        check(native_retirement_queue_outstanding == 1 && native_retirement_queue_head == record);
        auto const queued = current_native_semantic_epoch.snapshot();
        // Claim, off-lock pending completion and real requeue all share the
        // outer operation; unlock/backend callbacks probe actual code windows.
        check(process_native_unpublished_retirements_slice(root_active));
        check(!root_active && record->state == fm::UnpublishedRetirementState::WAITING_RETURN);
        check(native_retirement_queue_head == record && !record->deferred_claimed);
        check(current_native_semantic_epoch.snapshot().inflight == 0);
        check(current_native_semantic_epoch.snapshot().version > queued.version);
        record->entries[0].completion.state = fm::RetirementReturnState::RETURNED;
        tail_target = child;
        check(!process_native_unpublished_retirements_slice(root_active));
        tail_target = nullptr;
        native_probe = false;
        check(native_retirement_queue_outstanding == 0 && native_retirement_queue_head == nullptr);
        check(native_mapping_active_records == nullptr && native_return_tails == 0);
        check(native_adoptions >= 6 && native_backends >= 3);
        ker::test::numa_lifecycle::NativeDebtSnapshot after{};
        selftest_current_native_debt_snapshot(after);
        check(after.stamp.inflight == 0 && after.flags == (NATIVE_COMPLETE | NATIVE_ARITHMETIC_VALID));
    }
    {
        // Test-only field visibility exposes actual Epoch arithmetic. Neither
        // this seed nor a reset API exists in the kernel.
        using namespace ker::mod::sched::task;
        current_native_semantic_epoch.version_.store(UINT64_MAX - 2);
        {
            CurrentNativeSemanticOperation op;
            op.begin();
            check(current_native_semantic_epoch.snapshot().inflight == 1);
        }
        NativeDebtSnapshot exhausted{};
        selftest_current_native_debt_snapshot(exhausted);
        check(exhausted.stamp.version == UINT64_MAX && !exhausted.stamp.valid && !(exhausted.flags & NATIVE_COMPLETE));
        current_native_semantic_epoch.end();
        selftest_current_native_debt_snapshot(exhausted);
        check(!exhausted.stamp.valid && !(exhausted.flags & NATIVE_COMPLETE));
        ker::mod::mm::lifecycle_census::BoundaryEpoch underflow;
        underflow.end();
        check(!underflow.snapshot().valid);
        ker::mod::mm::lifecycle_census::BoundaryEpoch inflight_overflow;
        inflight_overflow.inflight_.store(UINT64_MAX);
        inflight_overflow.begin();
        check(!inflight_overflow.snapshot().valid);
    }
    std::printf(
        "PASS actual complete current census handler checks=%u; explicit provider/Task/transport endpoints; no native/runtime "
        "qualification\n",
        checks);
}
