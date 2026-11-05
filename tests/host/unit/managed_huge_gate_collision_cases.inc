// Separate Host TU: production gate storage/hash/CAS bodies are inserted whole.
// PMM/control/current-root/pin-lock facts below are explicit Host endpoints.
// Actual922 factory/token/consume bodies use finite allocation/Task/control ports.
// No native allocator, publication, copier, IRQ/TLB, WOS progress or C92 cause is proved.
#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <new>
#include <tuple>
#include <type_traits>
// ACTUAL_921_PAGING_HEADER
#ifdef WOS_SELFTEST
#error This focused TU checks the actual Normal WOS_SELFTEST-off gate branch.
#endif

namespace host_gate921 {
[[noreturn]] void forbidden(const char* operation) {
    ADD_FAILURE() << "unexpected wait/scheduler/TLB endpoint: " << operation;
    std::abort();
}
}  // namespace host_gate921
namespace ker::mod::sched::task {
enum class TaskType { PROCESS, DAEMON };
enum class TaskState { ACTIVE, EXITING, DEAD };
enum class ChildMembershipState { UNLINKED, PUBLISHING, LIVE };
struct Task {
    static constexpr uint32_t ACTIVE_REGISTRY_INDEX_INVALID = UINT32_MAX;
    enum class sched_queue { NONE, RUNNABLE };
    TaskType type{TaskType::PROCESS};
    std::atomic<TaskState> state{TaskState::ACTIVE};
    uint64_t pid{1};
    mm::paging::PageTable* pagemap{};
    bool is_thread{}, has_run{}, has_exited{};
    void* thread{};
    std::atomic<bool> exit_in_progress{}, exit_notify_ready{}, scheduler_transition_active{}, scheduler_published{}, shares_user_pagemap{},
        usercopy_pagemap_closing{}, gc_queued{};
    std::atomic<uint32_t> usercopy_pagemap_accesses{}, ref_count{1};
    uint32_t active_registry_index{ACTIVE_REGISTRY_INDEX_INVALID};
    enum sched_queue sched_queue { sched_queue::NONE };
    int heap_index{-1};
    Task* sched_next{};
    std::atomic<ChildMembershipState> child_membership_state{ChildMembershipState::UNLINKED};
    bool child_publication_established{};
    Task* child_parent{};
    Task* child_sibling_prev{};
    Task* child_sibling_next{};
    uint64_t syscall_account_start_us{1};
    auto try_acquire_usercopy_pagemap(mm::paging::PageTable*&) -> bool;
    void release_usercopy_pagemap();
};
}  // namespace ker::mod::sched::task
namespace host_gate923 {
inline bool interrupts = true, lease_refused = false, require_gate_clear_on_release = false, wait_sampling = false;
inline unsigned release_after{};
inline size_t foreign_gate = static_cast<size_t>(-1);
inline unsigned preempt{}, preempt_enters{}, preempt_exits{}, lease_acquires{}, lease_releases{}, yields{};
inline void (*after_lease)() = nullptr;
inline void (*yield_hook)() = nullptr;
}  // namespace host_gate923
namespace ker::mod::sys {
struct SyscallPreparationScope {
    bool prepared{true};
    auto ready() const noexcept -> bool { return prepared; }
};
}  // namespace ker::mod::sys
namespace ker::syscall::vmem {
struct SharedVmemPublicationGuard {
    bool held{true};
    auto acquired() const noexcept -> bool { return held; }
};
}  // namespace ker::syscall::vmem
namespace ker::mod::sched {
task::Task* current{};
bool queryable{true};
auto can_query_current_task() -> bool { return queryable; }
auto get_current_task() -> task::Task* { return current; }
void kern_yield() {
    if (!host_gate923::yield_hook) host_gate921::forbidden("scheduler yield");
    EXPECT_TRUE(host_gate923::interrupts);
    EXPECT_EQ(host_gate923::preempt, 0U);
    ASSERT_NE(current, nullptr);
    EXPECT_EQ(current->usercopy_pagemap_accesses.load(), 0U);
    ++host_gate923::yields;
    host_gate923::yield_hook();
}
auto interrupts_enabled() -> bool { return host_gate923::interrupts; }
auto preempt_count() -> uint32_t { return host_gate923::preempt; }
void preempt_disable() {
    ++host_gate923::preempt;
    ++host_gate923::preempt_enters;
}
void preempt_enable() {
    EXPECT_GT(host_gate923::preempt, 0U);
    --host_gate923::preempt;
    ++host_gate923::preempt_exits;
}
// ACTUAL_923_PREEMPT_GUARD
}  // namespace ker::mod::sched
namespace ker::mod::mm {
// ACTUAL_921_PAGE_KIND
namespace phys {
auto page_kind_get(void*) -> PageKind;
auto page_ref_get(void*) -> uint32_t;
}  // namespace phys
}  // namespace ker::mod::mm
namespace ker::mod::mm::virt {
using paging::PageTable;
using paging::PageTableEntry;
// ACTUAL_921_REFUSAL_TYPES
// ACTUAL_922_PRIVATE_TYPE
namespace detail {
struct ForkSourceAccess;
}
// ACTUAL_923_SOURCE_TYPE
struct UserRootControlChunk;
// ACTUAL_921_ROOT_CONTROL
struct RootFact {
    PageTable* root{};
    PageKind kind{PageKind::PAGE_TABLE};
    uint32_t references{1};
    UserRootControl control{};
    bool index_empty{true};
    bool control_present{true};
};
std::array<RootFact, 3> facts{};
PageTable* kernel_pagemap{};
unsigned kind_queries{}, reference_queries{};
unsigned expected_readers{1}, allocation_calls{}, kernel_copy_calls{};
PageTable* next_factory_root{};
PageTable* active_root{};
bool allocation_fails{};
struct PinLock {
    bool held{};
    unsigned acquisitions{};
    auto lock_irqsave() -> uint64_t {
        EXPECT_FALSE(held);
        held = true;
        ++acquisitions;
        return 0;
    }
    void unlock_irqrestore(uint64_t flags) {
        EXPECT_TRUE(held);
        EXPECT_EQ(flags, 0U);
        held = false;
    }
} user_mapping_pin_lock;
auto fact(PageTable* root) -> RootFact* {
    for (auto& value : facts)
        if (value.root == root) return &value;
    ADD_FAILURE() << "unknown Host root";
    return nullptr;
}
auto is_current_pagemap(PageTable* root) -> bool { return root == active_root; }  // Explicit current-root endpoint.
auto find_user_root_control_locked(PageTable* root) -> UserRootControl* {
    EXPECT_TRUE(user_mapping_pin_lock.held);
    auto* value = fact(root);
    return value && value->control_present ? &value->control : nullptr;
}
struct EmptyIndex {
    auto root_empty_locked(PageTable* root) -> bool {
        EXPECT_TRUE(user_mapping_pin_lock.held);
        auto* value = fact(root);
        return value && value->index_empty;
    }
} file_mapping_index;
// The actual raw acquire body references these only in its waiting branch.
// They are refusing endpoints, not replacement gate algorithms.
void service_user_pagemap_gate_wait(uint64_t&) { host_gate921::forbidden("gate wait service"); }
void service_pending_tlb_shootdowns() { host_gate921::forbidden("TLB service"); }
// ACTUAL_921_GATE_STORAGE
// ACTUAL_921_GATE_FUNCTIONS
// Whole typed pin admission, with mapping/fault resolution as finite ports.
// The actual shared reader CAS above decides Busy versus terminal failure.
using vaddr_t = uint64_t;
// ACTUAL_98_PIN_ATTEMPT_TYPE
struct UserPagePin {
    PageTable* pagemap{};
    void* hhdm_page{};
    size_t access_gate_index{USER_PAGEMAP_ACCESS_GATE_NONE};
};
bool host_pin_mapping_ready = true;
unsigned host_pin_mapping_calls{};
auto ensure_user_page_writable(sched::task::Task*, vaddr_t) -> bool { return host_pin_mapping_ready; }
auto ensure_user_page_mapped(sched::task::Task*, vaddr_t) -> bool { return host_pin_mapping_ready; }
auto try_pin_current_user_page_mapping(PageTable* root, vaddr_t, bool, size_t gate, UserPagePin& out) -> bool {
    ++host_pin_mapping_calls;
    EXPECT_EQ(user_pagemap_access_gates.at(gate).readers.load(), 1U);
    if (!host_pin_mapping_ready) return false;
    out = {root, root, gate};
    return true;
}
// ACTUAL_98_TYPED_PIN_FUNCTIONS
// ACTUAL_921_CHILD_FUNCTIONS
// Finite allocation and kernel-half-copy endpoints. These do not exercise PMM.
auto create_user_root_common(bool may_fail) -> PageTable* {
    EXPECT_FALSE(may_fail);
    ++allocation_calls;
    return allocation_fails ? nullptr : next_factory_root;
}
void copy_kernel_mappings(sched::task::Task* child) {
    EXPECT_NE(child, nullptr);
    EXPECT_EQ(child->pagemap, next_factory_root);
    ++kernel_copy_calls;
}
// ACTUAL_922_PRIVATE_FUNCTIONS
namespace detail {
// ACTUAL_923_SOURCE_ACCESS
}
}  // namespace ker::mod::mm::virt
namespace ker::mod::sched::task {
auto Task::try_acquire_usercopy_pagemap(mm::paging::PageTable*& out) -> bool {
    out = nullptr;
    if (host_gate923::wait_sampling) EXPECT_EQ(host_gate923::preempt, 1U);
    if (host_gate923::lease_refused || state.load() != TaskState::ACTIVE || usercopy_pagemap_closing.load() || !pagemap) return false;
    ++usercopy_pagemap_accesses;
    ++host_gate923::lease_acquires;
    out = pagemap;
    if (host_gate923::after_lease) host_gate923::after_lease();
    return true;
}
void Task::release_usercopy_pagemap() {
    if (host_gate923::wait_sampling) EXPECT_EQ(host_gate923::preempt, 1U);
    EXPECT_GT(usercopy_pagemap_accesses.load(), 0U);
    if (host_gate923::require_gate_clear_on_release) {
        namespace v = mm::virt;
        EXPECT_FALSE(v::user_pagemap_access_gates.at(v::user_pagemap_access_gate_index(pagemap)).teardown.load());
    }
    --usercopy_pagemap_accesses;
    ++host_gate923::lease_releases;
}
}  // namespace ker::mod::sched::task
namespace ker::mod::mm::huge {
// Actual context predicates; only the finite Task/IRQ/PUB observations are ports.
namespace detail {
auto current_fork_context_valid(sched::task::Task&, const sys::SyscallPreparationScope&,
                                const ker::syscall::vmem::SharedVmemPublicationGuard&) noexcept -> bool;
}
// ACTUAL_923_CONTEXT_FUNCTIONS
}  // namespace ker::mod::mm::huge
namespace ker::mod::mm::virt {
// ACTUAL_923_EXCLUSIVE_SCOPE
// ACTUAL_923_SOURCE_FUNCTIONS
}  // namespace ker::mod::mm::virt
namespace ker::mod::mm::phys {
auto page_kind_get(void* pointer) -> PageKind {
    namespace v = virt;
    ++v::kind_queries;
    auto* root = static_cast<paging::PageTable*>(pointer);
    EXPECT_EQ(v::user_pagemap_access_gates.at(v::user_pagemap_access_gate_index(root)).readers.load(), v::expected_readers);
    auto* value = v::fact(root);
    return value ? value->kind : PageKind::UNKNOWN;
}
auto page_ref_get(void* pointer) -> uint32_t {
    namespace v = virt;
    ++v::reference_queries;
    auto* root = static_cast<paging::PageTable*>(pointer);
    EXPECT_EQ(v::user_pagemap_access_gates.at(v::user_pagemap_access_gate_index(root)).readers.load(), v::expected_readers);
    auto* value = v::fact(root);
    return value ? value->references : 0;
}
}  // namespace ker::mod::mm::phys

namespace {
namespace v = ker::mod::mm::virt;
using PageTable = ker::mod::mm::paging::PageTable;
constexpr size_t PAGE = ker::mod::mm::paging::PAGE_SIZE;
class GateCollision921 : public ::testing::Test {
   protected:
    // Count comes from the actual source, not a duplicate hash implementation.
    static constexpr size_t BACKING_BYTES = (v::USER_PAGEMAP_ACCESS_GATE_COUNT + 1) * PAGE;
    static_assert(BACKING_BYTES <= 17 * 1024 * 1024);
    void* backing{};
    std::array<PageTable*, 3> roots{};
    std::array<std::array<unsigned char, sizeof(PageTable)>, 3> before{};
    std::array<v::RootFact, 3> original_facts{};
    struct Exclusive {
        size_t gate{v::USER_PAGEMAP_ACCESS_GATE_NONE};
        explicit Exclusive(PageTable* root) : gate(v::try_begin_user_pagemap_exclusive(root)) {}
        ~Exclusive() { end(); }
        void end() {
            if (gate != v::USER_PAGEMAP_ACCESS_GATE_NONE) {
                v::end_user_pagemap_exclusive(gate);
                gate = v::USER_PAGEMAP_ACCESS_GATE_NONE;
            }
        }
        Exclusive(const Exclusive&) = delete;
        auto operator=(const Exclusive&) -> Exclusive& = delete;
    };
    void SetUp() override {
        ASSERT_EQ(v::USER_PAGEMAP_ACCESS_GATE_COUNT, 4096U);
        for (auto& gate : v::user_pagemap_access_gates) {
            ASSERT_EQ(gate.readers.load(), 0U);
            ASSERT_FALSE(gate.teardown.load());
        }
        backing = std::aligned_alloc(PAGE, BACKING_BYTES);
        ASSERT_NE(backing, nullptr);
        auto* bytes = static_cast<unsigned char*>(backing);
        roots = {::new (bytes) PageTable{}, ::new (bytes + v::USER_PAGEMAP_ACCESS_GATE_COUNT * PAGE) PageTable{},
                 ::new (bytes + PAGE) PageTable{}};
        ASSERT_NE(roots[0], roots[1]);
        ASSERT_EQ(v::user_pagemap_access_gate_index(roots[0]), v::user_pagemap_access_gate_index(roots[1]));
        ASSERT_NE(v::user_pagemap_access_gate_index(roots[0]), v::user_pagemap_access_gate_index(roots[2]));
        for (size_t i = 0; i < roots.size(); ++i) {
            v::facts[i] = {};
            v::facts[i].root = roots[i];
            v::facts[i].control.root = roots[i];
            v::facts[i].control.serial = 100 + i;
            v::facts[i].control.ps_geometry_epoch = 1;
            std::memcpy(before[i].data(), roots[i], sizeof(PageTable));
        }
        original_facts = v::facts;
        v::kind_queries = v::reference_queries = 0;
        v::user_mapping_pin_lock.acquisitions = 0;
        v::expected_readers = 1;
        v::allocation_calls = v::kernel_copy_calls = 0;
        v::next_factory_root = roots[0];
        v::active_root = nullptr;
        v::allocation_fails = false;
        v::host_pin_mapping_ready = true;
        v::host_pin_mapping_calls = 0;
        ker::mod::sched::queryable = true;
        ker::mod::sched::current = nullptr;
        host_gate923::interrupts = true;
        host_gate923::lease_refused = false;
        host_gate923::require_gate_clear_on_release = false;
        host_gate923::wait_sampling = false;
        host_gate923::release_after = 0;
        host_gate923::foreign_gate = v::USER_PAGEMAP_ACCESS_GATE_NONE;
        host_gate923::preempt = host_gate923::preempt_enters = host_gate923::preempt_exits = 0;
        host_gate923::lease_acquires = host_gate923::lease_releases = host_gate923::yields = 0;
        host_gate923::after_lease = host_gate923::yield_hook = nullptr;
        ASSERT_FALSE(v::user_mapping_pin_lock.held);
    }
    void TearDown() override {
        EXPECT_FALSE(v::user_mapping_pin_lock.held);
        for (auto& gate : v::user_pagemap_access_gates) {
            EXPECT_EQ(gate.readers.load(), 0U);
            EXPECT_FALSE(gate.teardown.load());
        }
        if (!backing) return;
        for (size_t i = 0; i < roots.size(); ++i) {
            if (!roots[i]) continue;
            EXPECT_EQ(std::memcmp(before[i].data(), roots[i], sizeof(PageTable)), 0);
            auto const& a = v::facts[i];
            auto const& b = original_facts[i];
            EXPECT_EQ(std::tie(a.root, a.kind, a.references, a.index_empty, a.control.root, a.control.serial, a.control.hash_next,
                               a.control.free_next, a.control.chunk, a.control.ps_geometry_epoch, a.control.ps_admission_suspensions,
                               a.control_present),
                      std::tie(b.root, b.kind, b.references, b.index_empty, b.control.root, b.control.serial, b.control.hash_next,
                               b.control.free_next, b.control.chunk, b.control.ps_geometry_epoch, b.control.ps_admission_suspensions,
                               b.control_present));
            roots[i]->~PageTable();
        }
        std::free(backing);
        v::facts = {};
        ker::mod::sched::current = nullptr;
    }
};
TEST_F(GateCollision921, RawReaderCollisionRefusesThenRecovers) {
    Exclusive foreign(roots[1]);
    ASSERT_NE(foreign.gate, v::USER_PAGEMAP_ACCESS_GATE_NONE);
    ASSERT_TRUE(v::user_pagemap_access_gates.at(foreign.gate).teardown.load());
    size_t reader = v::USER_PAGEMAP_ACCESS_GATE_NONE;
    EXPECT_FALSE(v::acquire_user_pagemap_access(roots[0], false, reader));
    EXPECT_EQ(reader, v::USER_PAGEMAP_ACCESS_GATE_NONE);
    v::release_user_pagemap_access(reader);
    EXPECT_EQ(v::user_pagemap_access_gates.at(foreign.gate).readers.load(), 0U);
    EXPECT_TRUE(v::user_pagemap_access_gates.at(foreign.gate).teardown.load());
    foreign.end();
    EXPECT_TRUE(v::acquire_user_pagemap_access(roots[0], false, reader));
    EXPECT_EQ(reader, v::user_pagemap_access_gate_index(roots[0]));
    if (reader != v::USER_PAGEMAP_ACCESS_GATE_NONE) EXPECT_EQ(v::user_pagemap_access_gates.at(reader).readers.load(), 1U);
    v::release_user_pagemap_access(reader);
}
TEST_F(GateCollision921, TypedFaultingPinReportsForeignBusyThenRecovers) {
    ker::mod::sched::task::Task actor;
    actor.pagemap = roots[0];
    v::UserPagePin pin;
    Exclusive foreign(roots[1]);
    ASSERT_NE(foreign.gate, v::USER_PAGEMAP_ACCESS_GATE_NONE);
    EXPECT_EQ(v::try_pin_user_page_for_task(&actor, roots[0], 0x4000, false, true, pin), v::UserPagePinAttempt::Busy);
    EXPECT_EQ(pin.pagemap, nullptr);
    EXPECT_EQ(pin.hhdm_page, nullptr);
    EXPECT_EQ(pin.access_gate_index, v::USER_PAGEMAP_ACCESS_GATE_NONE);
    EXPECT_EQ(v::host_pin_mapping_calls, 0U);
    EXPECT_EQ(v::user_pagemap_access_gates.at(foreign.gate).readers.load(), 0U);
    EXPECT_TRUE(v::user_pagemap_access_gates.at(foreign.gate).teardown.load());
    foreign.end();
    ASSERT_EQ(v::try_pin_user_page_for_task(&actor, roots[0], 0x4000, false, true, pin), v::UserPagePinAttempt::Pinned);
    EXPECT_EQ(pin.pagemap, roots[0]);
    EXPECT_EQ(v::host_pin_mapping_calls, 1U);
    v::release_user_pagemap_access(pin.access_gate_index);
    pin = {};
}
TEST_F(GateCollision921, TypedPinSaturationStaysFailedWithAndWithoutTeardown) {
    ker::mod::sched::task::Task actor;
    actor.pagemap = roots[0];
    auto& gate = v::user_pagemap_access_gates.at(v::user_pagemap_access_gate_index(roots[0]));
    for (bool teardown : {false, true}) {
        gate.readers.store(UINT32_MAX);
        gate.teardown.store(teardown);
        v::UserPagePin pin;
        EXPECT_EQ(v::try_pin_user_page_for_task(&actor, roots[0], 0x4000, false, true, pin), v::UserPagePinAttempt::Failed);
        EXPECT_EQ(pin.pagemap, nullptr);
        EXPECT_EQ(pin.hhdm_page, nullptr);
        EXPECT_EQ(pin.access_gate_index, v::USER_PAGEMAP_ACCESS_GATE_NONE);
        EXPECT_EQ(gate.readers.load(), UINT32_MAX);
        EXPECT_EQ(gate.teardown.load(), teardown);
        EXPECT_EQ(v::host_pin_mapping_calls, 0U);
        gate.readers.store(0);
        gate.teardown.store(false);
    }
}
TEST_F(GateCollision921, TypedPinInvalidOrMappingFailureIsNeverBusy) {
    ker::mod::sched::task::Task actor;
    actor.pagemap = roots[0];
    v::UserPagePin pin;
    Exclusive foreign(roots[1]);
    ASSERT_NE(foreign.gate, v::USER_PAGEMAP_ACCESS_GATE_NONE);
    EXPECT_EQ(v::try_pin_user_page_for_task(nullptr, roots[0], 0x4000, false, true, pin), v::UserPagePinAttempt::Failed);
    EXPECT_EQ(v::try_pin_user_page_for_task(&actor, roots[1], 0x4000, false, true, pin), v::UserPagePinAttempt::Failed);
    EXPECT_EQ(v::host_pin_mapping_calls, 0U);
    foreign.end();
    actor.pagemap = nullptr;
    EXPECT_EQ(v::try_pin_user_page_for_task(&actor, nullptr, 0x4000, false, true, pin), v::UserPagePinAttempt::Failed);
    actor.pagemap = roots[0];
    v::host_pin_mapping_ready = false;
    EXPECT_EQ(v::try_pin_user_page_for_task(&actor, roots[0], 0x4000, false, true, pin), v::UserPagePinAttempt::Failed);
    EXPECT_EQ(v::user_pagemap_access_gates.at(v::user_pagemap_access_gate_index(roots[0])).readers.load(), 0U);
    EXPECT_EQ(pin.pagemap, nullptr);
}
TEST_F(GateCollision921, ObservedChildCollisionRefusesWithoutStructuralChangeAndRecovers) {
    v::UnpublishedRootRefusal witness;
    ASSERT_TRUE(v::unpublished_user_root_empty_observed(roots[0], &witness));
    ASSERT_FALSE(witness.valid);
    auto const kinds = v::kind_queries, references = v::reference_queries;
    auto const pins = v::user_mapping_pin_lock.acquisitions;
    Exclusive foreign(roots[1]);
    ASSERT_NE(foreign.gate, v::USER_PAGEMAP_ACCESS_GATE_NONE);
    ASSERT_TRUE(v::user_pagemap_access_gates.at(foreign.gate).teardown.load());
    EXPECT_FALSE(v::unpublished_user_root_empty_observed(roots[0], &witness));
    EXPECT_TRUE(witness.valid);
    EXPECT_EQ(witness.site, v::UnpublishedRootRefusalSite::ReaderAdmissionRefused);
    EXPECT_EQ(witness.user_slot, UINT16_MAX);
    EXPECT_EQ(v::kind_queries, kinds);
    EXPECT_EQ(v::reference_queries, references);
    EXPECT_EQ(v::user_mapping_pin_lock.acquisitions, pins);
    EXPECT_EQ(v::user_pagemap_access_gates.at(foreign.gate).readers.load(), 0U);
    EXPECT_TRUE(v::user_pagemap_access_gates.at(foreign.gate).teardown.load());
    foreign.end();
    EXPECT_TRUE(v::unpublished_user_root_empty_observed(roots[0], &witness));
    EXPECT_FALSE(witness.valid);
    EXPECT_EQ(witness.site, v::UnpublishedRootRefusalSite::None);
    EXPECT_EQ(witness.user_slot, UINT16_MAX);
}
TEST_F(GateCollision921, NoncollidingForeignExclusiveDoesNotRefuseChild) {
    Exclusive foreign(roots[2]);
    ASSERT_NE(foreign.gate, v::USER_PAGEMAP_ACCESS_GATE_NONE);
    ASSERT_TRUE(v::user_pagemap_access_gates.at(foreign.gate).teardown.load());
    v::UnpublishedRootRefusal witness;
    EXPECT_TRUE(v::unpublished_user_root_empty_observed(roots[0], &witness));
    EXPECT_FALSE(witness.valid);
    EXPECT_TRUE(v::user_pagemap_access_gates.at(foreign.gate).teardown.load());
    EXPECT_EQ(v::user_pagemap_access_gates.at(foreign.gate).readers.load(), 0U);
}
TEST_F(GateCollision921, TryExclusiveRollsBackWithoutStealingExistingReader) {
    size_t reader = v::USER_PAGEMAP_ACCESS_GATE_NONE;
    ASSERT_TRUE(v::acquire_user_pagemap_access(roots[0], false, reader));
    Exclusive refused(roots[1]);
    EXPECT_EQ(refused.gate, v::USER_PAGEMAP_ACCESS_GATE_NONE);
    EXPECT_FALSE(v::user_pagemap_access_gates.at(reader).teardown.load());
    EXPECT_EQ(v::user_pagemap_access_gates.at(reader).readers.load(), 1U);
    v::release_user_pagemap_access(reader);
    Exclusive acquired(roots[1]);
    EXPECT_NE(acquired.gate, v::USER_PAGEMAP_ACCESS_GATE_NONE);
}

// Actual class layout forbids copying, moving and reconstruction from raw facts.
static_assert(!std::is_aggregate_v<v::ForkPrivateChildRoot>);
static_assert(std::is_default_constructible_v<v::ForkPrivateChildRoot>);
static_assert(!std::is_copy_constructible_v<v::ForkPrivateChildRoot> && !std::is_copy_assignable_v<v::ForkPrivateChildRoot>);
static_assert(!std::is_move_constructible_v<v::ForkPrivateChildRoot> && !std::is_move_assignable_v<v::ForkPrivateChildRoot>);
static_assert(!std::is_constructible_v<v::ForkPrivateChildRoot, ker::mod::sched::task::Task&, ker::mod::sched::task::Task&, PageTable*>);

class ForkPrivateRoot922 : public GateCollision921 {
   protected:
    ker::mod::sched::task::Task creator, child;
    void SetUp() override {
        GateCollision921::SetUp();
        creator.pid = 10;
        child.pid = 11;
        creator.pagemap = roots[2];
        ker::mod::sched::current = &creator;
        v::expected_readers = 0;
    }
    auto issue(v::ForkPrivateChildRoot& authority) -> PageTable* { return v::create_fork_private_child_root(creator, child, authority); }
    auto consume(v::ForkPrivateChildRoot& authority, v::UnpublishedRootRefusal& witness) -> bool {
        return v::consume_fork_private_child_root(creator, child, roots[0], authority, &witness);
    }
};
TEST_F(ForkPrivateRoot922, FactoryBornAuthorityConsumesOnceAndCannotReissue) {
    v::ForkPrivateChildRoot authority;
    ASSERT_EQ(issue(authority), roots[0]);
    EXPECT_EQ(child.pagemap, roots[0]);
    EXPECT_EQ(v::allocation_calls, 1U);
    EXPECT_EQ(v::kernel_copy_calls, 1U);
    v::UnpublishedRootRefusal witness{true, v::UnpublishedRootRefusalSite::UserEntryNonzero, 7};
    EXPECT_TRUE(consume(authority, witness));
    EXPECT_FALSE(witness.valid);
    EXPECT_FALSE(consume(authority, witness));
    EXPECT_EQ(witness.site, v::UnpublishedRootRefusalSite::PrivateAuthority);
    EXPECT_EQ(issue(authority), nullptr);
    EXPECT_EQ(v::allocation_calls, 1U);
}
TEST_F(ForkPrivateRoot922, DefaultInvalidatedAndWrongFactoryOwnersCannotMintAuthority) {
    v::ForkPrivateChildRoot empty;
    v::UnpublishedRootRefusal witness;
    EXPECT_FALSE(consume(empty, witness));
    EXPECT_EQ(witness.site, v::UnpublishedRootRefusalSite::PrivateAuthority);
    EXPECT_EQ(issue(empty), nullptr);
    v::ForkPrivateChildRoot invalidated;
    invalidated.invalidate();
    EXPECT_EQ(issue(invalidated), nullptr);
    v::ForkPrivateChildRoot wrong;
    ker::mod::sched::current = &child;
    EXPECT_EQ(issue(wrong), nullptr);
    EXPECT_EQ(v::allocation_calls, 0U);
    ker::mod::sched::current = &creator;
}
TEST_F(ForkPrivateRoot922, AllocationFailureHasNoKernelCopyOrUsableAuthority) {
    v::ForkPrivateChildRoot authority;
    v::allocation_fails = true;
    EXPECT_EQ(issue(authority), nullptr);
    EXPECT_EQ(child.pagemap, nullptr);
    EXPECT_EQ(v::allocation_calls, 1U);
    EXPECT_EQ(v::kernel_copy_calls, 0U);
    v::UnpublishedRootRefusal witness;
    EXPECT_FALSE(consume(authority, witness));
    v::allocation_fails = false;
}
TEST_F(ForkPrivateRoot922, ValidTypedRootIgnoresForeignStripeWhileGenericStillRefuses) {
    v::ForkPrivateChildRoot authority;
    ASSERT_EQ(issue(authority), roots[0]);
    Exclusive foreign(roots[1]);
    ASSERT_NE(foreign.gate, v::USER_PAGEMAP_ACCESS_GATE_NONE);
    v::UnpublishedRootRefusal witness;
    EXPECT_FALSE(v::unpublished_user_root_empty_observed(roots[0], &witness));
    EXPECT_EQ(witness.site, v::UnpublishedRootRefusalSite::ReaderAdmissionRefused);
    EXPECT_TRUE(consume(authority, witness));
    EXPECT_FALSE(witness.valid);
    EXPECT_TRUE(v::user_pagemap_access_gates.at(foreign.gate).teardown.load());
    EXPECT_EQ(v::user_pagemap_access_gates.at(foreign.gate).readers.load(), 0U);
}
TEST_F(ForkPrivateRoot922, TypedValidationLeavesNoChildReaderBeforeAliasedParentExclusive) {
    v::ForkPrivateChildRoot authority;
    ASSERT_EQ(issue(authority), roots[0]);
    v::UnpublishedRootRefusal witness;
    ASSERT_TRUE(consume(authority, witness));
    Exclusive parent(roots[1]);
    EXPECT_NE(parent.gate, v::USER_PAGEMAP_ACCESS_GATE_NONE);
    EXPECT_EQ(v::user_pagemap_access_gates.at(parent.gate).readers.load(), 0U);
}
TEST_F(ForkPrivateRoot922, WrongCreatorChildRootAndIdentityAreTerminalRefusals) {
    for (unsigned which = 0; which < 5; ++which) {
        v::ForkPrivateChildRoot authority;
        child.pagemap = nullptr;
        ASSERT_EQ(issue(authority), roots[0]);
        ker::mod::sched::task::Task other;
        other.pid = 19;
        v::UnpublishedRootRefusal witness;
        bool okay{};
        if (which == 0) okay = v::consume_fork_private_child_root(other, child, roots[0], authority, &witness);
        if (which == 1) okay = v::consume_fork_private_child_root(creator, other, roots[0], authority, &witness);
        if (which == 2) okay = v::consume_fork_private_child_root(creator, child, roots[2], authority, &witness);
        if (which == 3) {
            ++creator.pid;
            okay = consume(authority, witness);
            --creator.pid;
        }
        if (which == 4) {
            ++child.pid;
            okay = consume(authority, witness);
            --child.pid;
        }
        EXPECT_FALSE(okay) << which;
        EXPECT_EQ(witness.site, v::UnpublishedRootRefusalSite::PrivateAuthority) << which;
        EXPECT_FALSE(consume(authority, witness)) << "failed consume must remain consumed";
    }
}
TEST_F(ForkPrivateRoot922, EveryPrivateTaskGuardRejectsWithoutStructuralMutation) {
    using Task = ker::mod::sched::task::Task;
    using State = ker::mod::sched::task::TaskState;
    for (unsigned which = 0; which < 32; ++which) {
        v::ForkPrivateChildRoot authority;
        child.pagemap = nullptr;
        ASSERT_EQ(issue(authority), roots[0]);
        switch (which) {
            case 0:
                child.scheduler_published = true;
                break;
            case 1:
                child.shares_user_pagemap = true;
                break;
            case 2:
                child.usercopy_pagemap_closing = true;
                break;
            case 3:
                child.usercopy_pagemap_accesses = 1;
                break;
            case 4:
                child.ref_count = 2;
                break;
            case 5:
                child.has_run = true;
                break;
            case 6:
                child.has_exited = true;
                break;
            case 7:
                child.exit_in_progress = true;
                break;
            case 8:
                child.gc_queued = true;
                break;
            case 9:
                child.active_registry_index = 0;
                break;
            case 10:
                child.sched_queue = Task::sched_queue::RUNNABLE;
                break;
            case 11:
                child.heap_index = 0;
                break;
            case 12:
                child.sched_next = &creator;
                break;
            case 13:
                child.scheduler_transition_active = true;
                break;
            case 14:
                child.is_thread = true;
                break;
            case 15:
                child.thread = &creator;
                break;
            case 16:
                child.state = State::EXITING;
                break;
            case 17:
                ker::mod::sched::queryable = false;
                break;
            case 18:
                ker::mod::sched::current = &child;
                break;
            case 19:
                child.type = ker::mod::sched::task::TaskType::DAEMON;
                break;
            case 20:
                child.pid = 0;
                break;
            case 21:
                child.pid = creator.pid;
                break;
            case 22:
                creator.state = State::EXITING;
                break;
            case 23:
                creator.pid = 0;
                break;
            case 24:
                creator.scheduler_transition_active = true;
                break;
            case 25:
                creator.exit_in_progress = true;
                break;
            case 26:
                creator.has_exited = true;
                break;
            case 27:
                child.child_membership_state = ker::mod::sched::task::ChildMembershipState::PUBLISHING;
                break;
            case 28:
                child.child_publication_established = true;
                break;
            case 29:
                child.child_parent = &creator;
                break;
            case 30:
                child.child_sibling_prev = &creator;
                break;
            case 31:
                child.child_sibling_next = &creator;
                break;
        }
        v::UnpublishedRootRefusal witness;
        EXPECT_FALSE(consume(authority, witness)) << which;
        EXPECT_EQ(witness.site, v::UnpublishedRootRefusalSite::PrivateAuthority) << which;
        child.scheduler_published = child.shares_user_pagemap = child.usercopy_pagemap_closing = false;
        child.usercopy_pagemap_accesses = 0;
        child.ref_count = 1;
        child.has_run = child.has_exited = false;
        child.exit_in_progress = child.gc_queued = false;
        child.active_registry_index = Task::ACTIVE_REGISTRY_INDEX_INVALID;
        child.sched_queue = Task::sched_queue::NONE;
        child.heap_index = -1;
        child.sched_next = nullptr;
        child.scheduler_transition_active = false;
        child.is_thread = false;
        child.thread = nullptr;
        child.state = State::ACTIVE;
        child.type = ker::mod::sched::task::TaskType::PROCESS;
        child.pid = 11;
        creator.state = State::ACTIVE;
        creator.pid = 10;
        creator.scheduler_transition_active = false;
        creator.exit_in_progress = false;
        creator.has_exited = false;
        child.child_membership_state = ker::mod::sched::task::ChildMembershipState::UNLINKED;
        child.child_publication_established = false;
        child.child_parent = child.child_sibling_prev = child.child_sibling_next = nullptr;
        ker::mod::sched::queryable = true;
        ker::mod::sched::current = &creator;
        EXPECT_FALSE(consume(authority, witness));
    }
}
TEST_F(ForkPrivateRoot922, KindRefControlSerialIndexAndAllUserSlotsStayMandatory) {
    using Site = v::UnpublishedRootRefusalSite;
    for (unsigned which = 0; which < 7; ++which) {
        v::ForkPrivateChildRoot authority;
        child.pagemap = nullptr;
        ASSERT_EQ(issue(authority), roots[0]);
        switch (which) {
            case 0:
                v::facts[0].kind = ker::mod::mm::PageKind::UNKNOWN;
                break;
            case 1:
                v::facts[0].references = 2;
                break;
            case 2:
                v::facts[0].control_present = false;
                break;
            case 3:
                v::facts[0].control.serial = 0;
                break;
            case 4:
                ++v::facts[0].control.serial;
                break;
            case 5:
                v::facts[0].index_empty = false;
                break;
            case 6:
                roots[0]->entries[255].present = 1;
                break;
        }
        v::UnpublishedRootRefusal witness;
        EXPECT_FALSE(consume(authority, witness)) << which;
        EXPECT_TRUE(witness.valid);
        if (which == 0) EXPECT_EQ(witness.site, Site::PageKind);
        if (which == 1) EXPECT_EQ(witness.site, Site::ReferenceCount);
        if (which == 2) EXPECT_EQ(witness.site, Site::MissingControl);
        if (which == 3) EXPECT_EQ(witness.site, Site::ZeroSerial);
        if (which == 4) EXPECT_EQ(witness.site, Site::PrivateAuthority);
        if (which == 5) EXPECT_EQ(witness.site, Site::FileIndexNonempty);
        if (which == 6) {
            EXPECT_EQ(witness.site, Site::UserEntryNonzero);
            EXPECT_EQ(witness.user_slot, 255U);
        }
        v::facts[0] = original_facts[0];
        roots[0]->entries[255] = {};
        EXPECT_FALSE(consume(authority, witness));
    }
}

TEST_F(ForkPrivateRoot922, MissingFactoryRegistrationLeavesBoundRootForOriginalRollback) {
    v::ForkPrivateChildRoot authority;
    v::facts[0].control.serial = 0;
    EXPECT_EQ(issue(authority), roots[0]);
    EXPECT_EQ(child.pagemap, roots[0]);
    v::facts[0] = original_facts[0];
    v::UnpublishedRootRefusal witness;
    EXPECT_FALSE(consume(authority, witness));
    EXPECT_EQ(witness.site, v::UnpublishedRootRefusalSite::PrivateAuthority);
}
TEST_F(ForkPrivateRoot922, EveryOneOf256UserEntriesIsCheckedAndFailureIsTerminal) {
    for (size_t slot = 0; slot < 256; ++slot) {
        v::ForkPrivateChildRoot authority;
        child.pagemap = nullptr;
        ASSERT_EQ(issue(authority), roots[0]);
        roots[0]->entries[slot].present = 1;
        v::UnpublishedRootRefusal witness;
        EXPECT_FALSE(consume(authority, witness)) << slot;
        EXPECT_EQ(witness.site, v::UnpublishedRootRefusalSite::UserEntryNonzero) << slot;
        EXPECT_EQ(witness.user_slot, slot);
        roots[0]->entries[slot] = {};
        EXPECT_FALSE(consume(authority, witness));
    }
}
TEST_F(ForkPrivateRoot922, BoundRootCannotBecomeKernelCurrentOrBeReplaced) {
    for (unsigned which = 0; which < 3; ++which) {
        v::ForkPrivateChildRoot authority;
        child.pagemap = nullptr;
        ASSERT_EQ(issue(authority), roots[0]);
        if (which == 0) v::kernel_pagemap = roots[0];
        if (which == 1) v::active_root = roots[0];
        if (which == 2) child.pagemap = roots[2];
        v::UnpublishedRootRefusal witness;
        EXPECT_FALSE(consume(authority, witness));
        EXPECT_EQ(witness.site, which == 2 ? v::UnpublishedRootRefusalSite::PrivateAuthority : v::UnpublishedRootRefusalSite::InputRoot);
        v::kernel_pagemap = nullptr;
        v::active_root = nullptr;
        child.pagemap = roots[0];
        EXPECT_FALSE(consume(authority, witness));
    }
}

TEST_F(ForkPrivateRoot922, ThreadedProcessCreatorRemainsSupportedWithSharedSource) {
    creator.is_thread = true;
    creator.shares_user_pagemap = true;
    v::ForkPrivateChildRoot authority;
    ASSERT_EQ(issue(authority), roots[0]);
    v::UnpublishedRootRefusal witness;
    EXPECT_TRUE(consume(authority, witness));
    EXPECT_FALSE(witness.valid);
}

static_assert(!std::is_copy_constructible_v<v::ForkSourceOwner> && !std::is_move_constructible_v<v::ForkSourceOwner>);
static_assert(!std::is_constructible_v<v::ForkSourceOwner, PageTable*, size_t>);
class ForkSourceGate923 : public GateCollision921 {
   protected:
    ker::mod::sched::task::Task actor;
    ker::mod::sys::SyscallPreparationScope preparation;
    ker::syscall::vmem::SharedVmemPublicationGuard publication;
    void SetUp() override {
        GateCollision921::SetUp();
        actor.pid = 19;
        actor.pagemap = roots[0];
        ker::mod::sched::current = &actor;
        host_gate923::interrupts = false;
    }
    auto acquire(v::ForkSourceOwner& owner) -> v::ForkSourceAdmission {
        return v::try_acquire_fork_source(actor, preparation, publication, owner);
    }
};
TEST_F(ForkSourceGate923, TryOwnsExactlyOneLeaseAndOwnerUsedDoesNotDiscardIt) {
    v::ForkSourceOwner owner;
    ASSERT_EQ(acquire(owner), v::ForkSourceAdmission::Acquired);
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 1U);
    EXPECT_EQ(host_gate923::lease_acquires, 1U);
    auto const gate = v::user_pagemap_access_gate_index(roots[0]);
    EXPECT_TRUE(v::user_pagemap_access_gates[gate].teardown.load());
    EXPECT_EQ(acquire(owner), v::ForkSourceAdmission::OwnerUsed);
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 1U);
    EXPECT_TRUE(v::user_pagemap_access_gates[gate].teardown.load());
    host_gate923::require_gate_clear_on_release = true;
    owner.finish();
    owner.finish();
    EXPECT_FALSE(v::user_pagemap_access_gates[gate].teardown.load());
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
    EXPECT_EQ(host_gate923::lease_releases, 1U);
}
TEST_F(ForkSourceGate923, PrivateAdoptionTransfersLeaseWithoutNestedAcquireAndReleasesGateFirst) {
    v::ForkSourceOwner owner;
    ASSERT_EQ(acquire(owner), v::ForkSourceAdmission::Acquired);
    host_gate923::require_gate_clear_on_release = true;
    {
        v::UserPagemapExclusiveScope owned(roots[0], &owner);
        EXPECT_TRUE(owned.owns(roots[0]));
        EXPECT_FALSE(owned.owns(roots[2]));
        EXPECT_EQ(host_gate923::lease_acquires, 1U);
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 1U);
        owner.finish();  // Transferred owner cannot release the adopted lease.
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 1U);
        EXPECT_TRUE(v::user_pagemap_access_gates[v::user_pagemap_access_gate_index(roots[0])].teardown.load());
        owned.finish();
        owned.finish();
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
        EXPECT_EQ(host_gate923::lease_releases, 1U);
    }
}
TEST_F(ForkSourceGate923, TransientSpeculativeReaderDoesNotInvalidateAnAlreadyIssuedOwner) {
    v::ForkSourceOwner owner;
    ASSERT_EQ(acquire(owner), v::ForkSourceAdmission::Acquired);
    auto& gate = v::user_pagemap_access_gates[v::user_pagemap_access_gate_index(roots[0])];
    // Explicit speculative CAS endpoint: it accesses no root/PTE and will
    // roll back after observing teardown. It is not an admitted reader.
    gate.readers.store(1);
    EXPECT_EQ(v::detail::fork_source_owner_root(actor, preparation, publication, owner), roots[0]);
    {
        v::UserPagemapExclusiveScope owned(roots[0], &owner);
        EXPECT_TRUE(owned.owns(roots[0]));
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 1U);
        gate.readers.store(0);  // Before any actual copier/PTE operation.
    }
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
}
TEST_F(ForkSourceGate923, InvalidProvidedOwnerAndNullExpectedNeverAdoptOrFallBack) {
    v::ForkSourceOwner virgin;
    size_t gate = v::USER_PAGEMAP_ACCESS_GATE_NONE;
    ker::mod::sched::task::Task* lease = nullptr;
    EXPECT_FALSE(v::detail::ForkSourceAccess::transfer(virgin, nullptr, gate, lease));
    EXPECT_EQ(gate, v::USER_PAGEMAP_ACCESS_GATE_NONE);
    EXPECT_EQ(lease, nullptr);
    {
        v::UserPagemapExclusiveScope invalid(roots[0], &virgin);
        EXPECT_FALSE(invalid.owns(roots[0]));
    }
    v::ForkSourceOwner owner;
    ASSERT_EQ(acquire(owner), v::ForkSourceAdmission::Acquired);
    {
        v::UserPagemapExclusiveScope wrong(roots[2], &owner);
        EXPECT_FALSE(wrong.owns(roots[2]));
    }
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 1U);
    host_gate923::require_gate_clear_on_release = true;
    owner.finish();
}
TEST_F(ForkSourceGate923, ActualReaderAndForeignExclusiveRefuseWithoutStealingTheirOwners) {
    size_t reader = v::USER_PAGEMAP_ACCESS_GATE_NONE;
    ASSERT_TRUE(v::acquire_user_pagemap_access(roots[1], false, reader));
    v::ForkSourceOwner busy;
    EXPECT_EQ(acquire(busy), v::ForkSourceAdmission::Busy);
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
    EXPECT_FALSE(v::user_pagemap_access_gates[reader].teardown.load());
    EXPECT_EQ(v::user_pagemap_access_gates[reader].readers.load(), 1U);
    v::release_user_pagemap_access(reader);
    Exclusive foreign(roots[1]);
    ASSERT_NE(foreign.gate, v::USER_PAGEMAP_ACCESS_GATE_NONE);
    v::ForkSourceOwner second;
    EXPECT_EQ(acquire(second), v::ForkSourceAdmission::Busy);
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
    EXPECT_TRUE(v::user_pagemap_access_gates[foreign.gate].teardown.load());
}
TEST_F(ForkSourceGate923, ContextLeaseAndPostLeaseIdentityFailuresReleaseEveryPartialOwner) {
    v::ForkSourceOwner context;
    preparation.prepared = false;
    EXPECT_EQ(acquire(context), v::ForkSourceAdmission::Context);
    preparation.prepared = true;
    v::ForkSourceOwner denied;
    host_gate923::lease_refused = true;
    EXPECT_EQ(acquire(denied), v::ForkSourceAdmission::TaskLease);
    host_gate923::lease_refused = false;
    v::ForkSourceOwner changed;
    host_gate923::after_lease = +[] { ker::mod::sched::queryable = false; };
    EXPECT_EQ(acquire(changed), v::ForkSourceAdmission::Context);
    host_gate923::after_lease = nullptr;
    ker::mod::sched::queryable = true;
    v::ForkSourceOwner stale;
    host_gate923::after_lease = +[] { ker::mod::sched::current->usercopy_pagemap_closing = true; };
    EXPECT_EQ(acquire(stale), v::ForkSourceAdmission::RootStale);
    host_gate923::after_lease = nullptr;
    actor.usercopy_pagemap_closing = false;
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
    EXPECT_EQ(host_gate923::lease_acquires, host_gate923::lease_releases);
}
TEST_F(ForkSourceGate923, GuardedBorrowRejectsOtherPreparationPublicationRootAndContext) {
    v::ForkSourceOwner owner;
    ASSERT_EQ(acquire(owner), v::ForkSourceAdmission::Acquired);
    ker::mod::sys::SyscallPreparationScope other_preparation;
    ker::syscall::vmem::SharedVmemPublicationGuard other_publication;
    EXPECT_EQ(v::detail::fork_source_owner_root(actor, other_preparation, publication, owner), nullptr);
    EXPECT_EQ(v::detail::fork_source_owner_root(actor, preparation, other_publication, owner), nullptr);
    actor.pagemap = roots[2];
    EXPECT_EQ(v::detail::fork_source_owner_root(actor, preparation, publication, owner), nullptr);
    actor.pagemap = roots[0];
    publication.held = false;
    EXPECT_EQ(v::detail::fork_source_owner_root(actor, preparation, publication, owner), nullptr);
    publication.held = true;
    owner.finish();
}
TEST_F(ForkSourceGate923, AdvisoryWaitIsOwnerFreeUntilActualForeignProgressAt3_7_17) {
    publication.held = false;  // Finite context port observes no PUB across advisory waits.
    for (unsigned release : {3U, 7U, 17U}) {
        host_gate923::yields = 0;
        host_gate923::lease_acquires = host_gate923::lease_releases = 0;
        host_gate923::preempt_enters = host_gate923::preempt_exits = 0;
        Exclusive foreign(roots[1]);
        ASSERT_NE(foreign.gate, v::USER_PAGEMAP_ACCESS_GATE_NONE);
        host_gate923::foreign_gate = foreign.gate;
        host_gate923::release_after = release;
        host_gate923::interrupts = true;
        host_gate923::wait_sampling = true;
        host_gate923::yield_hook = +[] {
            EXPECT_FALSE(v::user_mapping_pin_lock.held);
            EXPECT_EQ(host_gate923::preempt, 0U);
            EXPECT_EQ(ker::mod::sched::current->usercopy_pagemap_accesses.load(), 0U);
            if (host_gate923::yields == host_gate923::release_after) v::end_user_pagemap_exclusive(host_gate923::foreign_gate);
        };
        EXPECT_EQ(v::wait_for_fork_source(actor, preparation), 0);
        if (v::user_pagemap_access_gates[foreign.gate].teardown.load())
            foreign.end();
        else
            foreign.gate = v::USER_PAGEMAP_ACCESS_GATE_NONE;
        EXPECT_EQ(host_gate923::yields, release);
        EXPECT_EQ(host_gate923::lease_acquires, release + 1);
        EXPECT_EQ(host_gate923::lease_acquires, host_gate923::lease_releases);
        EXPECT_EQ(host_gate923::preempt_enters, host_gate923::preempt_exits);
        EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
        host_gate923::yield_hook = nullptr;
        host_gate923::wait_sampling = false;
    }
}
TEST_F(ForkSourceGate923, AdvisoryWaitRefusesContextOrLeaseLossWithoutLeakingAndRevalidatesAfterYield) {
    host_gate923::interrupts = true;
    host_gate923::wait_sampling = true;
    publication.held = false;
    preparation.prepared = false;
    EXPECT_EQ(v::wait_for_fork_source(actor, preparation), -EWOULDBLOCK);
    preparation.prepared = true;
    host_gate923::lease_refused = true;
    EXPECT_EQ(v::wait_for_fork_source(actor, preparation), -EAGAIN);
    host_gate923::lease_refused = false;
    v::kernel_pagemap = roots[0];
    EXPECT_EQ(v::wait_for_fork_source(actor, preparation), -ESTALE);
    v::kernel_pagemap = nullptr;
    Exclusive foreign(roots[1]);
    ASSERT_NE(foreign.gate, v::USER_PAGEMAP_ACCESS_GATE_NONE);
    host_gate923::yield_hook = +[] { ker::mod::sched::queryable = false; };
    EXPECT_EQ(v::wait_for_fork_source(actor, preparation), -EWOULDBLOCK);
    EXPECT_EQ(host_gate923::yields, 1U);
    EXPECT_EQ(actor.usercopy_pagemap_accesses.load(), 0U);
    ker::mod::sched::queryable = true;
    host_gate923::yield_hook = nullptr;
    host_gate923::wait_sampling = false;
}
}  // namespace
