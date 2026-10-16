// Actual body regression; CPU/IRQ/Task/namespace/PMM/callback endpoints below
// are explicit Host seams. No native scheduler, physical allocator or SMP claim.
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <functional>
#include <limits>
#include <map>
#include <mutex>
#include <new>
#include <optional>
#include <set>
#include <string_view>
#include <utility>
#include <vector>

namespace fixture {
unsigned checks{}, cases{}, locks{}, irq_depth{};
std::function<void()> on_task_release;
std::function<void(const void*)> on_unlock;
void check(bool value, const char* message) {
    ++checks;
    if (!value) {
        std::fprintf(stderr, "FAIL: %s\n", message);
        std::abort();
    }
}
}  // namespace fixture

#define private public  // Visibility only for owned Epoch saturation controls.
#include <platform/mm/lifecycle_census.hpp>
#undef private
#include <platform/sched/session.hpp>

namespace ker::mod::sched::pid_ns {
struct NamespaceRef {
    bool host{true}, closing{};
    auto is_host() const -> bool { return host; }
    auto closed() const -> bool { return closing; }
    auto operator==(const NamespaceRef&) const -> bool = default;
};
class PublicationGuard {
   public:
    explicit PublicationGuard(const NamespaceRef& space) : open_(!space.closed()) {}
    auto open() const -> bool { return open_; }

   private:
    bool open_;
};
inline NamespaceRef host_scope;
}  // namespace ker::mod::sched::pid_ns

namespace ker::mod::sched::task {
struct Task;
/* @CHILD_TYPES@ */
enum class TaskType { PROCESS, DAEMON };
enum class TaskState { ACTIVE, EXITING, DEAD };
struct Task {
    uint64_t pid{}, parent_pid{}, child_queue_version{1};
    bool is_thread{}, child_publication_established{}, ptrace_traced{}, ptrace_stopped{}, ptrace_stop_pending{};
    uint64_t ptrace_tracer_pid{};
    std::atomic<TaskState> state{TaskState::ACTIVE};
    std::atomic<unsigned> refs{1};
    bool acquire_refused{};
    TaskType type{TaskType::PROCESS};
    std::atomic<ChildMembershipState> child_membership_state{ChildMembershipState::UNLINKED};
    Task *child_parent{}, *child_sibling_prev{}, *child_sibling_next{}, *child_list_head{};
    Task *child_tracer{}, *child_tracee_prev{}, *child_tracee_next{}, *child_tracee_head{};
    ChildEvent *child_event_head{}, *child_event_tail{}, *child_parent_claimed_event{}, *child_tracer_claimed_event{};
    ChildWaitRegistration* child_waiter_head{};
    ChildWaitRegistration child_wait_registration{};
    ChildEventClaim child_claim{this};
    ChildWaitView child_wait_view{ChildWaitView::PHYSICAL};
    ChildEvent child_parent_exit_event{}, child_tracer_exit_event{}, child_ptrace_stop_event{}, child_job_stop_event{},
        child_continued_event{};
    std::array<uint64_t, 5> child_event_local_groups{};
    std::atomic<bool> exit_in_progress{}, process_exit_requested{}, exit_notify_ready{}, jobctl_stopped{}, jobctl_stop_pending{};
    bool has_exited{};
    int32_t exit_status{};
    uint64_t child_user_time_us{}, child_system_time_us{}, imported_child_user_time_us{}, imported_child_system_time_us{};
    ker::mod::sys::Spinlock resource_lock;
    struct {
        uint64_t cpu_user_time_us{}, cpu_system_time_us{};
    } resources;
    session::Identity job{};
    session::Names names{};
    pid_ns::NamespaceRef* scope{&pid_ns::host_scope};
    auto credential_owner() -> Task* { return this; }
    auto credential_owner() const -> const Task* { return this; }
    auto pid_namespace() const -> const pid_ns::NamespaceRef* { return scope; }
    auto namespace_tid() const -> uint64_t { return pid; }
    auto try_acquire_lifetime_ref() -> bool {
        if (acquire_refused || refs.load() == 0) return false;
        refs.fetch_add(1);
        return true;
    }
    void release() {
        if (fixture::on_task_release) fixture::on_task_release();
        fixture::check(refs.fetch_sub(1) > 1, "actual child ref has an owner to return");
    }
    auto job_identity() const -> session::Identity { return job; }
    auto job_identity_snapshot() const -> session::IdentitySnapshot { return {job, names}; }
    void publish_job_identity(const session::Identity& value, std::optional<session::Names> visible = std::nullopt) {
        job = value;
        if (visible) names = *visible;
    }
    auto local_group_slot(const ChildEvent& event) -> uint64_t* {
        std::array<const ChildEvent*, 5> events{&child_parent_exit_event, &child_tracer_exit_event, &child_job_stop_event,
                                                &child_continued_event, &child_ptrace_stop_event};
        for (size_t i = 0; i < events.size(); ++i)
            if (events[i] == &event) return &child_event_local_groups[i];
        return nullptr;
    }
};
auto process_pid(const Task& task) -> uint64_t { return task.pid; }
auto same_pid_namespace(const Task& a, const Task& b) -> bool { return a.scope == b.scope; }
auto task_rusage_user_time_us(const Task&) -> uint64_t { return 0; }
auto task_rusage_system_time_us(const Task&) -> uint64_t { return 0; }
}  // namespace ker::mod::sched::task
namespace ker::mod::sched::resource {
auto saturating_cpu_add(uint64_t a, uint64_t b) -> uint64_t { return b > UINT64_MAX - a ? UINT64_MAX : a + b; }
}  // namespace ker::mod::sched::resource
namespace ker::mod::sched {
unsigned wakes{}, gc_requests{};
void wake_task_from_event(task::Task*) { ++wakes; }
void wake_task_for_signal(task::Task*) { ++wakes; }
void request_gc() { ++gc_requests; }
task::Task* reaper{};
auto find_task_by_pid_safe(uint64_t) -> task::Task* {
    if (reaper != nullptr && reaper->try_acquire_lifetime_ref()) return reaper;
    return nullptr;
}
auto find_task_by_pid_namespace_safe(const pid_ns::NamespaceRef&, uint64_t id) -> task::Task* { return find_task_by_pid_safe(id); }
}  // namespace ker::mod::sched
namespace ker::net::wki {
constexpr uint8_t WKI_CHILD_SIGNAL_HOPS = 1, WKI_PARENT_DEATH_HOPS = 1;
}
namespace ker::syscall::process::child_events {
namespace sched_task = ker::mod::sched::task;
using Task = sched_task::Task;
using ChildEvent = sched_task::ChildEvent;
using ChildEventKind = sched_task::ChildEventKind;
using ChildEventAudience = sched_task::ChildEventAudience;
using ChildMembershipState = sched_task::ChildMembershipState;
using ChildWaitRegistration = sched_task::ChildWaitRegistration;
using Selector = sched_task::ChildWaitSelector;
using Claim = sched_task::ChildEventClaim;
using View = sched_task::ChildWaitView;
/* @CHILD_DECLARATIONS@ */
auto selector_matches(Selector, uint64_t, uint64_t, ker::mod::sched::session::Origin = {}) -> bool;
void signal_child_event(Task&, uint8_t) {}
void signal_parent_death(Task& owner, uint8_t) { owner.release(); }
void cancel_wait(Task&);
/* @CHILD_SOURCE@ */
}  // namespace ker::syscall::process::child_events

namespace ker::vfs {
struct File;
}
namespace ker::mod::mm {
enum class PhysicalPageOwner { PAGE_TABLE, USER_ROOT_CONTROL };
/* @PAGE_KIND@ */
namespace addr {
/* @ADDRESS_ALIASES@ */
// Explicit identity translation for owned Host pointers, not real HHDM/PA.
auto get_phys_pointer(paddr_t value) -> paddr_t* { return reinterpret_cast<paddr_t*>(value); }
}  // namespace addr
namespace paging {
constexpr uint64_t PAGE_SIZE = 4096, PAGE_SHIFT = 12;
struct alignas(4096) PageTable {
    std::array<uint64_t, 512> entries{};
};
}  // namespace paging
namespace numa {
using NodeId = uint16_t;
constexpr NodeId INVALID_NODE = UINT16_MAX;
namespace root_reserve {
enum class Error : uint8_t { NONE, NOT_READY, NO_TOPOLOGY, ALREADY_INITIALIZED, INVALID_NODE, EMPTY, CORRUPT };
enum class ReturnResult : uint8_t { NOT_MEMBER, RETURNED, CORRUPT };
struct TakeResult {
    paging::PageTable* table{};
    Error error{Error::EMPTY};
};
auto try_take(NodeId) -> TakeResult { return {}; }
auto try_return(paging::PageTable*, void (*)(paging::PageTable*)) -> ReturnResult { return ReturnResult::NOT_MEMBER; }
}  // namespace root_reserve
}  // namespace numa
namespace huge {
struct RootKey {
    uint64_t address_bits{}, incarnation{};
};
}  // namespace huge
namespace file_mapping {
struct LifetimeRetirementTicket {};
struct LifetimeRetirementPort {
    void* context{};
    void (*submit_ready)(void*, LifetimeRetirementTicket&) noexcept {};
};
struct FileMappingLifetime {
    const LifetimeRetirementPort* port_{};
    auto file() const -> ker::vfs::File* { return nullptr; }
};
/* @INDEX_DECLARATIONS@ */
}  // namespace file_mapping
}  // namespace ker::mod::mm
namespace ker::mod::sys::usercopy {
constexpr uint64_t USER_ADDR_LIMIT = uint64_t{1} << 47;
}
namespace ker::mod::mm::virt {
auto selftest_current_root_change_begin() noexcept -> bool;
void selftest_current_root_change_end(bool) noexcept;
void selftest_current_root_detached_binding_returned() noexcept;
}  // namespace ker::mod::mm::virt
namespace ker::mod::mm::file_mapping {
/* @INDEX_SOURCE@ */
}
namespace ker::mod::acpi::numa {
struct Topology {
    uint16_t cpu_count{2}, node_count{2};
    std::array<uint16_t, 256> cpu_nodes{0, 1};
};
Topology topology;
auto boot_topology() -> const Topology* { return &topology; }
}  // namespace ker::mod::acpi::numa
namespace ker::mod::cpu {
auto get_current_cpu_id_safe() -> uint64_t { return 0; }
}  // namespace ker::mod::cpu
namespace ker::mod::sched {
class PreemptGuard {
   public:
    PreemptGuard() { fixture::check(fixture::locks == 0, "actual root captures locality before locks"); }
};
}  // namespace ker::mod::sched
namespace ker::mod::dbg {
template <typename... Args>
void emergency_log(const char*, Args...) {}
[[noreturn]] void panic_handler(const char*) { std::abort(); }
}  // namespace ker::mod::dbg
namespace ker::mod::mm::virt {
using namespace paging;
using addr::vaddr_t;
namespace lifecycle_census = ker::mod::mm::lifecycle_census;
namespace phys {
void* page_alloc_full_overwrite_page_may_fail(PhysicalPageOwner, const char*);
void page_free(void*);
auto page_kind_get(const void*) -> PageKind;
auto page_ref_get(const void*) -> unsigned;
}  // namespace phys
/* @ROOT_DECLARATIONS@ */
void selftest_enable_current_root_census() noexcept;
auto selftest_current_root_change_begin() noexcept -> bool;
void selftest_current_root_change_end(bool) noexcept;
void selftest_current_root_detached_binding_returned() noexcept;
void selftest_current_root_census_into(SelftestRootCensus&, lifecycle_census::Stamp&) noexcept;
PageTable kernel_root;
PageTable* kernel_pagemap = &kernel_root;
file_mapping::FileMappingIndex file_mapping_index;
lifecycle_census::Epoch selftest_root_epoch;
lifecycle_census::Epoch selftest_current_root_epoch;
bool selftest_current_root_enabled{};
lifecycle_census::OwnerLock user_mapping_pin_lock{selftest_root_epoch};
struct {
    auto root_has_published_owners(huge::RootKey) const -> bool { return false; }
} managed_huge_registry;
struct HostLog {
    template <typename... Args>
    static void critical(const char*, Args...) {
        std::abort();
    }
};
using log = HostLog;
[[noreturn]] void hcf() { std::abort(); }
unsigned physical_allocations{}, physical_returns{};
int fail_root_after{-1}, fail_control_after{-1};
std::set<void*> active_pages;
std::vector<void*> free_pages;
bool check_physical_epoch{};
auto current_root_stamp() -> lifecycle_census::Stamp { return selftest_current_root_epoch.snapshot(); }
void check_root_intent() {
    if (check_physical_epoch) fixture::check(current_root_stamp().inflight != 0, "root private allocation/return remains inflight");
}
auto allocate_page() -> void* {
    fixture::check(fixture::locks == 0, "physical seam outside actual root/index locks");
    check_root_intent();
    void* result = nullptr;
    if (free_pages.empty())
        result = std::aligned_alloc(PAGE_SIZE, PAGE_SIZE);
    else {
        result = free_pages.back();
        free_pages.pop_back();
    }
    fixture::check(result != nullptr && active_pages.insert(result).second, "unique owned physical endpoint");
    ++physical_allocations;
    std::memset(result, 0, PAGE_SIZE);
    return result;
}
namespace phys {
void* page_alloc_full_overwrite_page_may_fail(PhysicalPageOwner, const char*) {
    if (fail_control_after == 0) return nullptr;
    if (fail_control_after > 0) --fail_control_after;
    return allocate_page();
}
void page_free(void* page) {
    fixture::check(fixture::locks == 0, "actual root return occurs after pin unlock");
    check_root_intent();
    fixture::check(active_pages.erase(page) == 1, "owned physical return exactly once");
    free_pages.push_back(page);
    ++physical_returns;
}
auto page_kind_get(const void* page) -> PageKind {
    return active_pages.contains(const_cast<void*>(page)) ? PageKind::PAGE_TABLE : PageKind::NORMAL;
}
auto page_ref_get(const void* page) -> unsigned { return active_pages.contains(const_cast<void*>(page)) ? 1 : 0; }
}  // namespace phys
auto alloc_zeroed_page_table(bool may_fail = false, SelftestRootAllocationSource* source = nullptr) -> PageTable* {
    if (fail_root_after == 0) {
        if (!may_fail) std::abort();
        return nullptr;
    }
    if (fail_root_after > 0) --fail_root_after;
    auto* result = static_cast<PageTable*>(allocate_page());
    // This existing endpoint has only physical backing, including its reuse
    // vector; it does not implement a legacy pool or emergency membership.
    if (source != nullptr) *source = SelftestRootAllocationSource::PHYSICAL_ALLOCATOR;
    return result;
}
void zero_page_table_for_pool(PageTable* page) { std::memset(page, 0, PAGE_SIZE); }
struct ReturnCallsite {
    void* caller{};
    uint64_t owner_pid{};
};
auto try_release_page_table_to_pool(PageTable*, ReturnCallsite, SelftestRootPoolInsertion* insertion = nullptr) -> bool {
    // Existing endpoint never retains a root. Clear optional authority only;
    // actual physical return/census behavior and all old vectors stay exact.
    if (insertion != nullptr) {
        insertion->completed_sequence = 0;
        insertion->inserted = false;
    }
    return false;
}
auto selftest_kernel_page_table_frame(PageTable* page) -> bool { return page == kernel_pagemap; }
void owned_frame_purge_pagemap(PageTable*) { check_root_intent(); }
/* @ROOT_SOURCE@ */
}  // namespace ker::mod::mm::virt

/* @CASES@ */
