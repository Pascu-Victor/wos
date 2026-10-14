#include <abi/callnums/vmem.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <memory>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/lifetime_retirement_executor.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/unpublished_mapping_retirement.hpp>
#include <platform/mm/virt.hpp>
#include <platform/power/power.hpp>
#include <platform/sched/pid_namespace.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/security/credentials.hpp>
#include <platform/sys/mutex.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <test/file_mapping_lifetime_runtime_ktest.hpp>
#include <vfs/fd_reservation.hpp>
#include <vfs/file.hpp>
#include <vfs/fs/procfs.hpp>
#include <vfs/fs/tmpfs.hpp>
#include <vfs/vfs.hpp>

namespace ker::test {
namespace {
namespace mm = ker::mod::mm;
namespace fm = mm::file_mapping;
namespace virt = mm::virt;
namespace sched = ker::mod::sched;
namespace task = sched::task;
namespace tmpfs = ker::vfs::tmpfs;
namespace vfs = ker::vfs;
namespace vmem = ker::syscall::vmem;
using log = ker::mod::dbg::logger<"file-mapping-test">;
using Result = fm::UnpublishedRetirementResult;

constexpr uint64_t PAGE = mm::paging::PAGE_SIZE;
constexpr uint64_t ADDRESS = 0x400000;
constexpr uint64_t WAIT_US = 5'000'000;
std::atomic<bool> started{false};

struct Checks {
    uint64_t assertions{};
    uint64_t cases{};
    uint64_t returned{};
    void require(bool condition, const char* what) {
        ++assertions;
        if (condition) return;
        // A failure never unwinds a live native transaction or frees providers
        // behind a pending callback. The still-owned state survives this panic.
        log::error("FILE_MAPPING_RUNTIME FAIL assertion=%s checks=%lu", what, assertions);
        ker::mod::dbg::panic_handler("scheduled file mapping lifetime selftest failed");
    }
    void pass(const char* name) {
        ++cases;
        log::info("FILE_MAPPING_RUNTIME CASE_PASS %s", name);
    }
};

auto content() -> std::array<uint8_t, PAGE> {
    std::array<uint8_t, PAGE> out{};
    for (size_t i = 0; i < out.size(); ++i) out.at(i) = static_cast<uint8_t>((i * 37 + 19) % 251);
    return out;
}

// Real backend File and private mount; no global FD/mount registration or fake
// fops. The linked inode/mount stay alive until every family callback returns.
struct Fixture {
    Checks& checks;
    task::Task& owner;
    task::Task* child{};
    tmpfs::TmpNode* root{};
    tmpfs::TmpNode* node{};
    tmpfs::TmpfsMount* mount{};
    vfs::File* file{};
    std::array<fm::RetirementCompletion, fm::UnpublishedMappingRetirement::CAPACITY> observers{};
    size_t observer_count{};

    Fixture(Checks& check, task::Task& controller) : checks(check), owner(controller) {}
    Fixture(const Fixture&) = delete;
    auto operator=(const Fixture&) -> Fixture& = delete;
    ~Fixture() {
        checks.require(child == nullptr, "fixture owns no child at destruction");
        for (auto& observer : observers) observer.reset();
        checks.require(file == nullptr, "fixture File reference released before retirement");
        checks.require(node != nullptr && node->open_count.load(std::memory_order_acquire) == 0,
                       "real final File close ended TMPFS node open ownership");
        tmpfs::tmpfs_lock_tree();
        if (mount != nullptr) {
            tmpfs::destroy_mount_context(mount);
        } else if (root != nullptr) {
            tmpfs::tmpfs_free_node(root);
        }
        tmpfs::tmpfs_unlock_tree();
    }

    void init() {
        checks.require(owner.owned_unpublished_process.load(std::memory_order_acquire) == nullptr, "controller slot starts empty");
        child = new (std::nothrow) task::Task{};
        checks.require(child != nullptr, "fresh child allocated");
        child->type = task::TaskType::PROCESS;
        child->state.store(task::TaskState::ACTIVE, std::memory_order_release);
        child->pid = task::get_next_pid();
        checks.require(task::claim_unpublished_process(&owner, child), "claim exact child slot");
        child->pagemap = virt::create_pagemap();
        checks.require(child->pagemap != nullptr && child->pagemap != virt::get_kernel_pagemap(), "private registered root");
        checks.require(mm::phys::page_ref_get(child->pagemap) == 1, "root has sole allocator owner");
        checks.require(child->ref_count.load(std::memory_order_acquire) == 2, "creator plus exact owner slot");
        root = tmpfs::create_root_node();
        checks.require(root != nullptr, "private TMPFS root");
        int error = 0;
        mount = tmpfs::create_mount_context(root, "size=8K", false, &error);
        checks.require(mount != nullptr && error == 0, "private TMPFS mount");
        file =
            tmpfs::tmpfs_open_path(ker::mod::security::Snapshot::bootstrap_root(), root, "runtime-owned", vfs::O_CREAT | 2, 0600, &error);
        checks.require(file != nullptr && error == 0, "real TMPFS open");
        // Backend open supplies real node/open ownership. Ordinary VFS
        // dispatch installs this exact table too; no operation is substituted.
        checks.require(file->fops == nullptr, "backend open awaits operation table installation");
        file->fops = tmpfs::get_tmpfs_fops();
        checks.require(file->mount_owner == nullptr && file->fs_type == vfs::FSType::TMPFS && file->fops == tmpfs::get_tmpfs_fops(),
                       "supported actual detached backend operations");
        node = static_cast<tmpfs::TmpNode*>(file->private_data);
        checks.require(node != nullptr && node->open_count.load(std::memory_order_acquire) == 1, "one actual backend File open");
        auto const BYTES = content();
        checks.require(vfs::vfs_pwrite_file(file, BYTES.data(), BYTES.size(), 0) == static_cast<ssize_t>(BYTES.size()),
                       "write actual backend bytes");
        checks.require(file->refcount.load(std::memory_order_acquire) == 1, "fixture initially owns one File ref");
    }

    void close_file() {
        auto* const RETIRED = file;
        file = nullptr;
        if (RETIRED != nullptr) vfs::vfs_put_file(RETIRED);
    }

    auto record() -> fm::UnpublishedMappingRetirement& {
        checks.require(child != nullptr, "record access retains child");
        auto* record = child->native_file_mapping_retirement.load(std::memory_order_acquire);
        checks.require(record != nullptr, "actual stable native record exists");
        return *record;
    }

    auto map(uint64_t address = ADDRESS, uint64_t prot = ker::abi::vmem::PROT_READ | ker::abi::vmem::PROT_WRITE) -> int {
        checks.require(file != nullptr, "producer borrows owned real File");
        return vmem::prepare_unpublished_file_mapping(owner, *child, *file, address, PAGE, 0, prot);
    }

    void observe(size_t entry_index) {
        auto& entry = record().entries.at(entry_index);
        checks.require(entry.lifetime != nullptr && observer_count < observers.size(), "live family available for observation");
        auto& observer = observers.at(observer_count++);
        checks.require(entry.lifetime->observe_return(observer), "capture independent exact-generation return observer");
        checks.require(observer.generation() != 0 && observer.query().state == fm::RetirementReturnState::PENDING,
                       "active family callback has not run");
    }

    void require_owners(size_t entry_index, uint64_t pins = 0) {
        auto& entry = record().entries.at(entry_index);
        checks.require(entry.lifetime != nullptr && entry.binding != nullptr && entry.registered, "actual holders published");
        auto const FAMILY = entry.lifetime->owner_counts();
        for (size_t i = 0; i < FAMILY.by_role.size(); ++i) {
            auto const role = static_cast<fm::FamilyOwner>(i);
            uint64_t const EXPECTED =
                role == fm::FamilyOwner::LAZY || role == fm::FamilyOwner::REGISTRY || role == fm::FamilyOwner::BINDING ? 1 : 0;
            checks.require(FAMILY.by_role.at(i) == EXPECTED, "family has exact actual container owners");
        }
        checks.require(FAMILY.total == 3, "one LAZY plus REGISTRY plus BINDING");
        auto const BINDING = entry.binding->owner_counts();
        for (size_t i = 0; i < BINDING.by_role.size(); ++i) {
            auto const role = static_cast<fm::BindingOwner>(i);
            uint64_t const EXPECTED = role == fm::BindingOwner::INDEX ? 1 : role == fm::BindingOwner::PIN ? pins : 0;
            checks.require(BINDING.by_role.at(i) == EXPECTED, "binding has exact index and real pin owners");
        }
        checks.require(BINDING.total == 1 + pins, "exact binding total");
    }

    void retire() {
        auto* const RECORD = &record();
        size_t const COUNT = RECORD->count;
        int close_result = 12345;
        uint64_t const START = ker::mod::time::get_us();
        for (;;) {
            auto const RESULT =
                task::retire_native_unpublished_process(&owner, child, fm::UnpublishedRetirementMode::OWNED_SLOT, &close_result);
            if (RESULT == Result::COMPLETE) {
                // COMPLETE consumed/deleted Task, record and root. No read of
                // any of them, including a diagnostic counter, follows here.
                child = nullptr;
                break;
            }
            checks.require(RESULT == Result::PENDING, "retirement progresses through actual worker");
            checks.require(owner.owned_unpublished_process.load(std::memory_order_acquire) == child &&
                               owner.unpublished_teardown_in_progress.load(std::memory_order_acquire),
                           "pending preserves exact slot and controller teardown hold");
            checks.require(child->native_file_mapping_retirement.load(std::memory_order_acquire) == RECORD && RECORD->count == COUNT,
                           "pending preserves stable record and observers");
            checks.require(ker::mod::time::get_us() - START < WAIT_US, "retirement deadline preserves pending owned state");
            sched::kern_sleep_us(1000);
        }
        checks.require(close_result == 0, "all actual File callbacks returned success");
        checks.require(owner.owned_unpublished_process.load(std::memory_order_acquire) == nullptr &&
                           !owner.unpublished_teardown_in_progress.load(std::memory_order_acquire),
                       "complete consumes slot and clears controller hold");
        for (size_t i = 0; i < observer_count; ++i) {
            auto const RETURN = observers.at(i).query();
            checks.require(RETURN.state == fm::RetirementReturnState::RETURNED && RETURN.callback_result == 0,
                           "independent observer proves callback RETURNED after Task deletion");
            ++checks.returned;
            observers.at(i).reset();
        }
    }
};

void materialized_pin_refuses(Checks& checks, task::Task& owner) {
    Fixture fixture(checks, owner);
    fixture.init();
    checks.require(fixture.map() == 0, "admit real private native mapping");
    fixture.observe(0);
    fixture.require_owners(0);
    checks.require(vmem::validate_native_unpublished_mapping_owners(*fixture.child), "actual containers validate before fault");
    auto* const ROOT = fixture.child->pagemap;
    auto* const RECORD = &fixture.record();
    checks.require(virt::translate(ROOT, ADDRESS) == virt::PADDR_INVALID, "native interval starts cold");
    checks.require(virt::ensure_user_page_mapped(fixture.child, ADDRESS), "real software lazy materializer installs PTE");
    auto const PHYSICAL = virt::translate(ROOT, ADDRESS);
    checks.require(PHYSICAL != virt::PADDR_INVALID, "materializer exposed real PTE");
    auto* const FRAME = reinterpret_cast<void*>(PHYSICAL + mm::addr::get_hhdm_offset());
    uint32_t const FRAME_REFS = mm::phys::page_ref_get(FRAME);
    checks.require(FRAME_REFS != 0, "mapped frame has allocator owner");
    virt::UserPagePin pin{};
    checks.require(virt::pin_user_page_for_task(fixture.child, ROOT, ADDRESS, false, true, pin), "actual user-page pin");
    checks.require(pin.file_binding == RECORD->entries.at(0).binding && pin.root_incarnation != 0 && pin.hhdm_page == FRAME,
                   "pin captures exact native binding and root incarnation");
    checks.require(mm::phys::page_ref_get(FRAME) == FRAME_REFS + 1, "pin adds exactly one frame ref");
    auto const BYTES = content();
    checks.require(std::memcmp(pin.hhdm_page, BYTES.data(), BYTES.size()) == 0, "pinned bytes came from actual TMPFS");
    fixture.require_owners(0, 1);
    auto const REFUSED = task::retire_native_unpublished_process(&owner, fixture.child, fm::UnpublishedRetirementMode::OWNED_SLOT);
    checks.require(REFUSED == Result::REFUSED, "external PIN refuses before mutation");
    checks.require(owner.owned_unpublished_process.load(std::memory_order_acquire) == fixture.child &&
                       !owner.unpublished_teardown_in_progress.load(std::memory_order_acquire),
                   "refusal preserves exact owner slot and clears admission hold");
    checks.require(fixture.child->pagemap == ROOT &&
                       fixture.child->native_file_mapping_retirement.load(std::memory_order_acquire) == RECORD &&
                       RECORD->state == fm::UnpublishedRetirementState::PREPARED && RECORD->count == 1,
                   "refusal preserves root and PREPARED record");
    checks.require(virt::user_page_pin_still_mapped(pin) && virt::translate(ROOT, ADDRESS) == PHYSICAL &&
                       std::memcmp(pin.hhdm_page, BYTES.data(), BYTES.size()) == 0,
                   "refusal preserves real pin PTE and bytes");
    fixture.require_owners(0, 1);
    virt::unpin_user_page(pin);
    checks.require(pin.file_binding == nullptr && pin.pagemap == nullptr, "last unpin retires association and root access");
    checks.require(mm::phys::page_ref_get(FRAME) == FRAME_REFS, "unpin drops exactly its frame ref");
    fixture.require_owners(0);
    checks.require(vmem::validate_native_unpublished_mapping_owners(*fixture.child), "actual holders validate after unpin");
    fixture.close_file();  // Family now owns the final actual File reference.
    fixture.retire();
    checks.pass("materialized_pin_refuses_then_returns");
}

void cold_shared_file_families(Checks& checks, task::Task& owner) {
    Fixture fixture(checks, owner);
    fixture.init();
    checks.require(fixture.map(ADDRESS, ker::abi::vmem::PROT_READ) == 0, "cold read-only native family");
    checks.require(fixture.map(ADDRESS + 2 * PAGE, 0) == 0, "cold PROT_NONE native family");
    checks.require(fixture.record().count == 2 && fixture.file->refcount.load(std::memory_order_acquire) == 3,
                   "distinct families each own one real File ref");
    checks.require(fixture.record().entries.at(0).lifetime != fixture.record().entries.at(1).lifetime &&
                       fixture.record().entries.at(0).lifetime->file() == fixture.record().entries.at(1).lifetime->file(),
                   "two families share exact backend File");
    for (size_t i = 0; i < 2; ++i) {
        fixture.observe(i);
        fixture.require_owners(i);
    }
    checks.require(virt::translate(fixture.child->pagemap, ADDRESS) == virt::PADDR_INVALID &&
                       virt::translate(fixture.child->pagemap, ADDRESS + 2 * PAGE) == virt::PADDR_INVALID,
                   "cold and protection-none intervals remain without PTEs");
    checks.require(vmem::validate_native_unpublished_mapping_owners(*fixture.child), "index includes cold and PROT_NONE intervals");
    fixture.close_file();
    fixture.retire();
    checks.pass("cold_and_prot_none_shared_file_return");
}

void collision_observer_drains(Checks& checks, task::Task& owner) {
    Fixture fixture(checks, owner);
    fixture.init();
    checks.require(fixture.map() == 0, "original mapping before staging collision");
    fixture.observe(0);
    auto* const ROOT = fixture.child->pagemap;
    auto* const RECORD = &fixture.record();
    auto* const LIFETIME = RECORD->entries.at(0).lifetime;
    auto* const BINDING = RECORD->entries.at(0).binding;
    checks.require(fixture.map() == -EBUSY, "actual overlapping producer fails staging");
    checks.require(&fixture.record() == RECORD && RECORD->count == 2 && RECORD->state == fm::UnpublishedRetirementState::PREPARED,
                   "failed staging retains stable observed continuation");
    auto& failed = RECORD->entries.at(1);
    checks.require(failed.lifetime == nullptr && failed.binding == nullptr && !failed.registered && !failed.completion.empty(),
                   "failed stage owns observer without published family binding");
    uint64_t const GENERATION = failed.completion.generation();
    uint64_t const START = ker::mod::time::get_us();
    for (;;) {
        auto const RETURN = failed.completion.query();
        if (RETURN.state == fm::RetirementReturnState::RETURNED) {
            checks.require(RETURN.callback_result == 0, "failed-stage actual File callback returned success");
            ++checks.returned;
            break;
        }
        checks.require(RETURN.state == fm::RetirementReturnState::PENDING && failed.completion.generation() == GENERATION,
                       "failed-stage observer generation stays pending");
        checks.require(ker::mod::time::get_us() - START < WAIT_US, "staging callback deadline preserves continuation");
        sched::kern_sleep_us(1000);
    }
    checks.require(fixture.child->pagemap == ROOT && RECORD->entries.at(0).lifetime == LIFETIME &&
                       RECORD->entries.at(0).binding == BINDING && virt::translate(ROOT, ADDRESS) == virt::PADDR_INVALID,
                   "failed preparation leaves original cold mapping unchanged");
    checks.require(fixture.file->refcount.load(std::memory_order_acquire) == 2, "failed family dropped only its own File ref");
    fixture.require_owners(0);
    checks.require(vmem::validate_native_unpublished_mapping_owners(*fixture.child), "actual graph includes failed observer-only entry");
    fixture.close_file();
    fixture.retire();
    checks.pass("staging_collision_observer_drains");
}

void transferred_pin_refusal_recovers(Checks& checks, task::Task& owner) {
    checks.require(!task::native_unpublished_retirements_pending(), "deferred native queue starts empty");
    Fixture fixture(checks, owner);
    fixture.init();
    checks.require(fixture.map() == 0, "native mapping before actual transferred cleanup");
    fixture.observe(0);
    auto* const SUBJECT = fixture.child;
    auto* const ROOT = SUBJECT->pagemap;
    auto* const RECORD = &fixture.record();
    auto* const LIFETIME = RECORD->entries.at(0).lifetime;
    auto* const BINDING = RECORD->entries.at(0).binding;
    virt::UserPagePin pin{};
    checks.require(virt::pin_user_page_for_task(SUBJECT, ROOT, ADDRESS, false, true, pin),
                   "actual prefault and pin before transferred cleanup");
    checks.require(pin.file_binding == BINDING && virt::user_page_pin_still_mapped(pin), "transferred refusal owns exact live PIN");
    auto const BYTES = content();
    checks.require(std::memcmp(pin.hhdm_page, BYTES.data(), BYTES.size()) == 0, "transferred pin retains actual TMPFS bytes");
    fixture.require_owners(0, 1);
    fixture.close_file();
    auto* const TAKEN = task::take_unpublished_process(&owner);
    checks.require(TAKEN == SUBJECT, "legacy caller transfers exact existing slot reference");
    bool const DESTROYED = task::destroy_unpublished_process(TAKEN);
    // The actual legacy caller consumes the transferred reference even on
    // refusal, placing it in the permanent queue. This fixture never destroys
    // that child again. Its PIN still prevents destructive GC admission here.
    fixture.child = nullptr;
    checks.require(!DESTROYED, "real external PIN forces transferred destruction refusal");
    checks.require(owner.owned_unpublished_process.load(std::memory_order_acquire) == nullptr &&
                       !owner.unpublished_teardown_in_progress.load(std::memory_order_acquire),
                   "deferred handoff consumes slot and ends borrowed controller hold");
    checks.require(task::native_unpublished_retirements_pending(), "real queue includes queued or GC-claimed transferred owner");
    checks.require(SUBJECT->native_file_mapping_retirement.load(std::memory_order_acquire) == RECORD && SUBJECT->pagemap == ROOT &&
                       SUBJECT->ref_count.load(std::memory_order_acquire) == 2 &&
                       RECORD->state == fm::UnpublishedRetirementState::PREPARED && RECORD->count == 1 && RECORD->controller == nullptr &&
                       RECORD->deferred_subject == SUBJECT,
                   "PIN refusal preserves exact subject record and transferred ref without controller borrow");
    checks.require(RECORD->entries.at(0).lifetime == LIFETIME && RECORD->entries.at(0).binding == BINDING &&
                       RECORD->entries.at(0).registered && LIFETIME->owner_counts().total == 3,
                   "deferred refusal preserves actual lazy registry and binding holders");
    auto const BINDING_OWNERS = BINDING->owner_counts();
    // The genuine GC validator can briefly own its actual binding SNAPSHOT
    // while refusing the held PIN. Its temporary owner is allowed here.
    uint64_t const SNAPSHOTS = BINDING_OWNERS.count(fm::BindingOwner::SNAPSHOT);
    checks.require(BINDING_OWNERS.count(fm::BindingOwner::INDEX) == 1 && BINDING_OWNERS.count(fm::BindingOwner::PIN) == 1 &&
                       SNAPSHOTS <= 1 && BINDING_OWNERS.total == 2 + SNAPSHOTS,
                   "deferred interval keeps real INDEX and PIN through any GC snapshot");
    checks.require(virt::user_page_pin_still_mapped(pin) && std::memcmp(pin.hhdm_page, BYTES.data(), BYTES.size()) == 0 &&
                       fixture.observers.at(0).query().state == fm::RetirementReturnState::PENDING,
                   "queued refusal has not changed PTE bytes or run active family callback");
    virt::unpin_user_page(pin);
    // No subject, root, record, binding, family, or frame access after unpin:
    // the real scheduled GC worker may delete the Task immediately. Poll only
    // permanent queue accounting and the separately owned return observer.
    uint64_t const START = ker::mod::time::get_us();
    for (;;) {
        bool const PENDING = task::native_unpublished_retirements_pending();
        auto const RETURN = fixture.observers.at(0).query();
        checks.require(RETURN.state == fm::RetirementReturnState::PENDING || RETURN.state == fm::RetirementReturnState::RETURNED,
                       "transferred observer remains valid while real GC advances");
        if (!PENDING && RETURN.state == fm::RetirementReturnState::RETURNED) {
            checks.require(RETURN.callback_result == 0, "GC-owned actual family File callback returned success");
            ++checks.returned;
            break;
        }
        checks.require(ker::mod::time::get_us() - START < WAIT_US, "GC retirement deadline preserves queued ownership");
        sched::kern_sleep_us(1000);
    }
    fixture.observers.at(0).reset();
    checks.pass("transferred_pin_refusal_recovers_via_real_gc");
}

void procfs_self_target_returns_before_delete(Checks& checks, task::Task& owner) {
    namespace pid_ns = sched::pid_ns;
    namespace procfs = vfs::procfs;
    size_t const NAMESPACES_BEFORE = pid_ns::live_namespace_count();
    uint32_t const ACTIVE_BEFORE = sched::get_active_task_count();
    uint32_t const SCOPED_BEFORE = sched::get_scoped_pid_task_count();
    {
        pid_ns::NamespaceRef scope;
        checks.require(pid_ns::create_namespace(scope) == 0 && !scope.is_host(), "create actual private PID namespace");
        checks.require(owner.owned_unpublished_process.load(std::memory_order_acquire) == nullptr, "procfs controller slot empty");
        auto* child = new (std::nothrow) task::Task{};
        checks.require(child != nullptr, "allocate real procfs target child");
        child->type = task::TaskType::PROCESS;
        child->state.store(task::TaskState::ACTIVE, std::memory_order_release);
        child->pid = task::get_next_pid();
        checks.require(child->initialize_pid_namespace(scope), "child owns exact private namespace identity");
        checks.require(task::claim_unpublished_process(&owner, child), "claim actual procfs target child slot");
        child->pagemap = virt::create_pagemap();
        checks.require(
            child->pagemap != nullptr && child->pagemap != virt::get_kernel_pagemap() && mm::phys::page_ref_get(child->pagemap) == 1,
            "procfs child owns actual registered root");
        auto requester = std::unique_ptr<task::Task>(new (std::nothrow) task::Task{});
        checks.require(requester != nullptr, "allocate separate real lookup requester");
        requester->type = task::TaskType::PROCESS;
        requester->pid = task::get_next_pid();
        checks.require(requester->initialize_pid_namespace(scope) && requester->namespace_tid() != 0 &&
                           requester->namespace_tid() != child->namespace_tid(),
                       "numeric lookup cannot use requester self shortcut");
        checks.require(child->native_file_mapping_retirement.load(std::memory_order_acquire) == nullptr &&
                           sched::scheduler_selftest_register_unpublished_task(*child),
                       "actual production registry and scoped index publish before native selection");
        checks.require(!child->scheduler_published.load(std::memory_order_acquire) &&
                           child->active_registry_index != task::Task::ACTIVE_REGISTRY_INDEX_INVALID,
                       "registry fixture does not admit child to a runqueue");
        std::array<char, 64> path{};
        int const PATH_SIZE =
            std::snprintf(path.data(), path.size(), "%llu/status", static_cast<unsigned long long>(child->namespace_tid()));
        checks.require(PATH_SIZE > 0 && static_cast<size_t>(PATH_SIZE) < path.size(), "bounded numeric namespace path");
        int error = 0;
        auto* opened =
            procfs::procfs_open_path_as(requester->credentials(), requester->pid, path.data(), 0, 0, &error, false, requester.get());
        checks.require(opened != nullptr && error == 0 && opened->fops == nullptr, "actual scoped procfs open awaits dispatch table");
        opened->fops = procfs::get_procfs_fops();
        auto* const DATA = static_cast<procfs::ProcFileData*>(opened->private_data);
        checks.require(opened->fs_type == vfs::FSType::PROCFS && opened->mount_owner == nullptr && !opened->is_directory &&
                           DATA != nullptr && DATA->target == child && DATA->node.pid == child->pid &&
                           DATA->node.type == procfs::ProcNodeType::STATUS_FILE && DATA->pid_view == scope,
                       "actual scoped selector retained exact self target and namespace view");
        checks.require(child->ref_count.load(std::memory_order_acquire) == 3 && opened->refcount.load(std::memory_order_acquire) == 1,
                       "actual ProcFileData owns one child ref independent of one File owner");
        uint64_t const PHYSICAL_PID = child->pid;
        uint64_t const LOCAL_PID = child->namespace_tid();
        sched::scheduler_selftest_unregister_unpublished_task(*child);
        checks.require(child->active_registry_index == task::Task::ACTIVE_REGISTRY_INDEX_INVALID &&
                           sched::find_task_by_pid_namespace_safe(scope, LOCAL_PID) == nullptr &&
                           sched::find_task_by_pid_safe(PHYSICAL_PID) == nullptr,
                       "exact child removed from actual active physical and scoped registries");
        int fd = -1;
        {
            vfs::FdReservation reservation(child);
            checks.require(reservation.result() >= 0, "reserve actual child descriptor");
            fd = reservation.install(opened);
            checks.require(fd >= 0, "transfer opened File owner into actual descriptor");
            opened = nullptr;
        }
        auto* const BORROWED = vfs::vfs_get_file(child, fd);
        checks.require(BORROWED != nullptr && BORROWED->private_data == DATA && BORROWED->refcount.load(std::memory_order_acquire) == 1 &&
                           child->ref_count.load(std::memory_order_acquire) == 3 && child->fd_table.reserved_count() == 0,
                       "descriptor borrow adds no File or leftover reservation owner");
        vfs::Stat st{};
        checks.require(vfs::vfs_fstat_file(BORROWED, &st) == 0 && (st.st_mode & vfs::S_IFMT) == vfs::S_IFREG && st.st_size == 0,
                       "actual procfs stat admits cold zero-size regular ownership fixture");
        checks.require(vmem::prepare_unpublished_file_mapping(owner, *child, *BORROWED, ADDRESS, PAGE, 0, ker::abi::vmem::PROT_NONE) == 0,
                       "actual native producer accepts owned procfs self graph");
        auto* const ROOT = child->pagemap;
        auto* const RECORD = child->native_file_mapping_retirement.load(std::memory_order_acquire);
        checks.require(RECORD != nullptr && RECORD->count == 1 && RECORD->entries.at(0).lifetime != nullptr &&
                           RECORD->entries.at(0).lifetime->file() == BORROWED,
                       "native family owns exact procfs description");
        fm::RetirementCompletion completion;
        checks.require(
            RECORD->entries.at(0).lifetime->observe_return(completion) && completion.query().state == fm::RetirementReturnState::PENDING,
            "independent procfs callback return observer before teardown");
        checks.require(BORROWED->refcount.load(std::memory_order_acquire) == 2 && vfs::vfs_close_for_task(child, fd) == 0,
                       "close child FD after family admission");
        checks.require(BORROWED->refcount.load(std::memory_order_acquire) == 1 && child->ref_count.load(std::memory_order_acquire) == 3 &&
                           child->fd_table.size() == 0 && child->fd_table.reserved_count() == 0 && DATA->target == child &&
                           DATA->pid_view == scope,
                       "sole family File keeps actual child self ref and exact namespace view");
        checks.require(virt::translate(ROOT, ADDRESS) == virt::PADDR_INVALID && vmem::validate_native_unpublished_mapping_owners(*child),
                       "cold PROT_NONE procfs family has actual lazy registry and index holders");
        uint32_t self_refs = 0;
        checks.require(vfs::vfs_count_exclusive_unpublished_self_refs(*child, self_refs) == 0 && self_refs == 1,
                       "real exclusive graph enumerates one ProcFileData self ref at Task RC3");
        int close_result = 12345;
        uint64_t const START = ker::mod::time::get_us();
        for (;;) {
            auto const RESULT =
                task::retire_native_unpublished_process(&owner, child, fm::UnpublishedRetirementMode::OWNED_SLOT, &close_result);
            if (RESULT == Result::COMPLETE) {
                child = nullptr;  // Task, record and ProcFileData are deleted.
                break;
            }
            checks.require(RESULT == Result::PENDING && owner.owned_unpublished_process.load(std::memory_order_acquire) == child &&
                               owner.unpublished_teardown_in_progress.load(std::memory_order_acquire),
                           "procfs pending preserves exact owned child continuation");
            checks.require(child->native_file_mapping_retirement.load(std::memory_order_acquire) == RECORD && RECORD->count == 1,
                           "procfs pending preserves counted real callback observer");
            checks.require(ker::mod::time::get_us() - START < WAIT_US, "procfs return deadline preserves owned graph");
            sched::kern_sleep_us(1000);
        }
        // No Task/root/record/File/ProcFileData access follows COMPLETE.
        auto const RETURN = completion.query();
        checks.require(RETURN.state == fm::RetirementReturnState::RETURNED && RETURN.callback_result == 0 && close_result == 0,
                       "actual procfs close returned before completion consumed Task self lifetime");
        ++checks.returned;
        completion.reset();
        checks.require(owner.owned_unpublished_process.load(std::memory_order_acquire) == nullptr &&
                           !owner.unpublished_teardown_in_progress.load(std::memory_order_acquire),
                       "procfs COMPLETE consumes slot and controller hold");
        checks.require(requester->ref_count.load(std::memory_order_acquire) == 1 && scope.operations_drained(),
                       "remaining private scope owners are only requester and explicit scope");
        requester.reset();
        checks.require(scope.begin_retirement() && scope.closed() && scope.retiring() && scope.operations_drained(),
                       "retire exact owned private namespace identity without live operations");
        scope.reset();
        checks.require(scope.is_host(), "all explicit private scope handles reset");
    }
    // Normal PID1/providers can publish unrelated Tasks/namespaces concurrently.
    // These global observations are context; exact owned identities above gate.
    log::info(
        "FILE_MAPPING_RUNTIME PROCFS_CENSUS active_before=%u active_after=%u scoped_before=%u scoped_after=%u namespaces_before=%zu "
        "namespaces_after=%zu",
        ACTIVE_BEFORE, sched::get_active_task_count(), SCOPED_BEFORE, sched::get_scoped_pid_task_count(), NAMESPACES_BEFORE,
        pid_ns::live_namespace_count());
    checks.pass("procfs_self_target_returns_before_delete");
}

void maximum_native_shape_retires_without_census_growth(Checks& checks, task::Task& owner) {
    constexpr size_t FAMILIES = 8;
    constexpr size_t PAGES_PER_FAMILY = 64;
    constexpr uint64_t LENGTH = PAGES_PER_FAMILY * PAGE;
    static_assert(fm::UnpublishedMappingRetirement::CAPACITY == FAMILIES);
    Fixture fixture(checks, owner);
    fixture.init();
    checks.require(fixture.file->fops->vfs_truncate(fixture.file, static_cast<off_t>(LENGTH)) == 0,
                   "actual sparse backend truncate prepares full admitted file length");
    bool sparse = false;
    {
        ker::mod::sys::MutexGuard guard(fixture.node->io_lock);
        sparse = fixture.node->size == LENGTH && fixture.node->page_count == PAGES_PER_FAMILY && fixture.node->charged_pages == 1 &&
                 fixture.node->resident_pages == 1 && fixture.node->pages != nullptr &&
                 fixture.node->pages[0].state == tmpfs::TmpPageState::RESIDENT;
        for (size_t i = 1; i < PAGES_PER_FAMILY && sparse; ++i)
            sparse = fixture.node->pages[i].state == tmpfs::TmpPageState::HOLE && fixture.node->pages[i].data == nullptr;
    }
    checks.require(sparse, "64-page file has one real frame and 63 hole descriptors");
    auto const BYTES = content();
    std::array<uint8_t, PAGE> const ZERO{};
    std::array<uint8_t, PAGE> read_buffer{};
    for (size_t page = 0; page < PAGES_PER_FAMILY; ++page) {
        read_buffer.fill(0xA5);
        checks.require(vfs::vfs_pread_file(fixture.file, read_buffer.data(), read_buffer.size(), static_cast<off_t>(page * PAGE)) ==
                           static_cast<ssize_t>(read_buffer.size()),
                       "actual backend reads every sparse page at admitted length");
        auto const& EXPECTED = page == 0 ? BYTES : ZERO;
        checks.require(read_buffer == EXPECTED, "actual sparse backend bytes and holes match owned fixture");
    }
    auto* const ROOT = fixture.child->pagemap;
    uint64_t const SERIAL = virt::selftest_user_root_incarnation(ROOT);
    checks.require(SERIAL != 0 && virt::unpublished_user_root_empty(ROOT), "capacity fixture starts with exact empty native root");
    constexpr uint64_t FIRST_ADDRESS = 2 * (1ULL << 39) - 32 * PAGE;
    checks.require(vmem::prepare_unpublished_file_mapping(owner, *fixture.child, *fixture.file, FIRST_ADDRESS, LENGTH + PAGE, 0,
                                                          ker::abi::vmem::PROT_READ | ker::abi::vmem::PROT_WRITE) == -E2BIG,
                   "65-page native interval refuses before record admission");
    checks.require(fixture.child->native_file_mapping_retirement.load(std::memory_order_acquire) == nullptr &&
                       fixture.child->pagemap == ROOT && virt::selftest_user_root_incarnation(ROOT) == SERIAL &&
                       virt::unpublished_user_root_empty(ROOT) && fixture.child->lazy_vmem_ranges.empty() &&
                       fixture.child->ref_count.load(std::memory_order_acquire) == 2 &&
                       owner.owned_unpublished_process.load(std::memory_order_acquire) == fixture.child &&
                       !owner.unpublished_teardown_in_progress.load(std::memory_order_acquire) &&
                       fixture.file->refcount.load(std::memory_order_acquire) == 1,
                   "oversized refusal preserves exact Task root File slot and absent record");
    for (size_t family = 0; family < FAMILIES; ++family) {
        uint64_t const BASE = (2 * family + 2) * (1ULL << 39) - 32 * PAGE;
        checks.require(vmem::prepare_unpublished_file_mapping(owner, *fixture.child, *fixture.file, BASE, LENGTH, 0,
                                                              ker::abi::vmem::PROT_READ | ker::abi::vmem::PROT_WRITE) == 0,
                       "admit actual 64-page family across distinct PML4 boundary pair");
        fixture.observe(family);
        fixture.require_owners(family);
    }
    checks.require(fixture.record().count == FAMILIES && fixture.observer_count == FAMILIES &&
                       fixture.file->refcount.load(std::memory_order_acquire) == static_cast<int32_t>(FAMILIES + 1),
                   "eight families own one File ref each and eight observers own return storage");
    auto* const RECORD = &fixture.record();
    auto const REGISTRY_INDEX = fixture.child->active_registry_index;
    checks.require(!sched::post_task_balanced(fixture.child), "actual scheduler rejects native Task publication");
    checks.require(
        !fixture.child->scheduler_published.load(std::memory_order_acquire) && !fixture.child->gc_queued.load(std::memory_order_acquire) &&
            fixture.child->state.load(std::memory_order_acquire) == task::TaskState::ACTIVE &&
            fixture.child->active_registry_index == REGISTRY_INDEX && REGISTRY_INDEX == task::Task::ACTIVE_REGISTRY_INDEX_INVALID &&
            fixture.child->pagemap == ROOT && fixture.child->native_file_mapping_retirement.load(std::memory_order_acquire) == RECORD &&
            owner.owned_unpublished_process.load(std::memory_order_acquire) == fixture.child,
        "publication refusal preserves exclusively unpublished native ownership");
    auto destination = std::unique_ptr<task::Task>(new (std::nothrow) task::Task{});
    checks.require(destination != nullptr, "allocate fresh real clone destination");
    auto const* const DESTINATION_ROWS = destination->lazy_vmem_ranges.data();
    checks.require(!task::clone_lazy_vmem_ranges(*destination, *fixture.child), "actual lazy clone rejects native source");
    checks.require(destination->lazy_vmem_ranges.empty() && destination->lazy_vmem_ranges.data() == DESTINATION_ROWS &&
                       destination->pagemap == nullptr && destination->ref_count.load(std::memory_order_acquire) == 1 &&
                       destination->native_file_mapping_retirement.load(std::memory_order_acquire) == nullptr,
                   "native clone refusal leaves fresh destination unchanged");
    destination.reset();
    for (size_t family = 0; family < FAMILIES; ++family) {
        uint64_t const BASE = (2 * family + 2) * (1ULL << 39) - 32 * PAGE;
        for (size_t page = 0; page < PAGES_PER_FAMILY; ++page) {
            virt::UserPagePin pin{};
            checks.require(virt::pin_user_page_for_task(fixture.child, ROOT, BASE + page * PAGE, false, true, pin),
                           "real pin materializer installs every admitted private page");
            checks.require(pin.file_binding == RECORD->entries.at(family).binding && pin.root_incarnation == SERIAL &&
                               mm::phys::page_ref_get(pin.hhdm_page) == 2,
                           "real private page has exact mapping and pin frame owners");
            auto const& EXPECTED = page == 0 ? BYTES : ZERO;
            checks.require(std::memcmp(pin.hhdm_page, EXPECTED.data(), EXPECTED.size()) == 0,
                           "every actual native leaf contains backend bytes or zero hole");
            virt::unpin_user_page(pin);
            checks.require(pin.pagemap == nullptr && pin.file_binding == nullptr, "each pin drops exact root and binding access");
        }
        fixture.require_owners(family);
    }
    auto const STATS = virt::collect_user_memory_stats(ROOT);
    checks.require(STATS.virtual_pages == FAMILIES * PAGES_PER_FAMILY && STATS.resident_pages == FAMILIES * PAGES_PER_FAMILY &&
                       STATS.shared_pages == 0 && STATS.page_table_pages == 49,
                   "actual 512 private leaves require root plus 48 boundary-crossing table frames");
    checks.require(vmem::validate_native_unpublished_mapping_owners(*fixture.child),
                   "maximal actual container graph validates after all unpins");
    size_t used = 0;
    {
        ker::mod::sys::MutexGuard guard(fixture.mount->accounting_lock);
        used = fixture.mount->used_bytes;
    }
    {
        ker::mod::sys::MutexGuard guard(fixture.node->io_lock);
        sparse = fixture.node->size == LENGTH && fixture.node->page_count == PAGES_PER_FAMILY && fixture.node->charged_pages == 1 &&
                 fixture.node->resident_pages == 1 && fixture.node->pages != nullptr &&
                 fixture.node->pages[0].state == tmpfs::TmpPageState::RESIDENT && fixture.node->pages[0].data != nullptr &&
                 std::memcmp(fixture.node->pages[0].data, BYTES.data(), BYTES.size()) == 0;
        for (size_t i = 1; i < PAGES_PER_FAMILY && sparse; ++i)
            sparse = fixture.node->pages[i].state == tmpfs::TmpPageState::HOLE && fixture.node->pages[i].data == nullptr;
    }
    checks.require(sparse && used == PAGE, "512 private mapping copies preserve backend bytes holes and one-page quota");
    fixture.close_file();
    // Actual native collector forbids growing its inline FrameSet. The full
    // 49-table walk must complete through the real bounded retirement path.
    fixture.retire();
    for (auto const& observer : fixture.observers) checks.require(observer.empty(), "all eight owned return handles retired");
    checks.pass("maximum_native_shape_retires_without_census_growth");
}

void runtime_entry() {
    {
        auto const HOLD = sched::hold_kernel_shutdown();
        Checks checks{};
        auto* owner = sched::get_current_task();
        checks.require(owner != nullptr && owner->type == task::TaskType::DAEMON && sched::interrupts_enabled() &&
                           sched::preempt_count() == 0 && !fm::LifetimeRetirementExecutor::is_current_worker(),
                       "ordinary real scheduled diagnostic context");
        auto const EXECUTOR = fm::LifetimeRetirementExecutor::snapshot();
        checks.require(EXECUTOR.initialized && EXECUTOR.available && EXECUTOR.admission_open, "permanent executor is available");
        log::info("FILE_MAPPING_RUNTIME BEGIN");
        materialized_pin_refuses(checks, *owner);
        cold_shared_file_families(checks, *owner);
        collision_observer_drains(checks, *owner);
        transferred_pin_refusal_recovers(checks, *owner);
        procfs_self_target_returns_before_delete(checks, *owner);
        maximum_native_shape_retires_without_census_growth(checks, *owner);
        checks.require(owner->owned_unpublished_process.load(std::memory_order_acquire) == nullptr, "all child transactions retired");
        checks.require(checks.cases == 6 && checks.returned == 15, "all scheduled cases and actual callbacks completed");
        log::info("FILE_MAPPING_RUNTIME PASS cases=%lu assertions=%lu callback_returned=%lu", checks.cases, checks.assertions,
                  checks.returned);
    }
    // The ordinary finalizer retires PID1/providers and the real executor. No
    // shutdown bypass and no generic worker stop while a native fixture lives.
    ker::mod::power::selftest_shutdown();
}
}  // namespace

auto start_file_mapping_runtime_selftest() -> bool {
    bool expected = false;
    if (!started.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) return false;
    auto* worker = task::Task::create_kernel_thread("file_mapping_runtime_selftest", runtime_entry);
    if (worker == nullptr) {
        started.store(false, std::memory_order_release);
        return false;
    }
    if (!sched::post_task_balanced(worker)) {
        task::destroy_unpublished_user_thread(worker);
        started.store(false, std::memory_order_release);
        return false;
    }
    return true;
}

}  // namespace ker::test
