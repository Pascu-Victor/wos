#include "anonymous_usercopy_runtime.hpp"
#ifdef WOS_SELFTEST
#include <abi/callnums/futex.h>
#include <abi/callnums/vfs.h>
#include <abi/callnums/vmem.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <dev/block_device.hpp>
#include <dev/block_request.hpp>
#include <memory>
#include <new>
#include <platform/mm/numa_alloc.hpp>
#include <platform/mm/swap.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/task.hpp>
#include <platform/sched/threading.hpp>
#include <platform/sys/mutex.hpp>
#include <platform/sys/syscall_preparation.hpp>
#include <platform/sys/usercopy.hpp>
#include <syscalls_impl/futex/futex.hpp>
#include <syscalls_impl/vfs/sys_vfs.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <test/fault_block_device.hpp>
#include <vfs/file.hpp>
#include <vfs/fs/tmpfs.hpp>
#include <vfs/vfs.hpp>

namespace ker::test::anonymous_usercopy {
namespace {
namespace mm = mod::mm;
namespace swap = mm::swap;
namespace uc = mod::sys::usercopy;
using Task = mod::sched::task::Task;
constexpr uint64_t PAGE_BYTES = 4096;
constexpr uint64_t DEVICE_PAGES = 16;
std::atomic<bool> boot_enabled{false};
std::atomic<bool> busy{false};
std::atomic<bool> poisoned{false};
static_assert(std::atomic<bool>::is_always_lock_free);

struct DeviceHistory {
    uint64_t writes{}, reads{}, flushes{}, events{}, overflow{}, overlap{};
    bool exact{};
};

// The sole published reference to outer_ is one swap extent. No device registry,
// devfs, block queue, partition, remote binding or direct caller receives it.
class SerializedFaultStorage final {
   public:
    SerializedFaultStorage() {
        outer_.block_size = PAGE_BYTES;
        outer_.total_blocks = DEVICE_PAGES;
        outer_.read_blocks = read_callback;
        outer_.write_blocks = write_callback;
        outer_.flush = flush_callback;
        outer_.private_data = this;
    }
    SerializedFaultStorage(const SerializedFaultStorage&) = delete;
    auto operator=(const SerializedFaultStorage&) -> SerializedFaultStorage& = delete;

    auto activate(const char* name) -> int {
        if (active_ || detached_ || !inner_.valid() || !provider_is_private()) return -EINVAL;
        expected_ = {.name = name, .device = &outer_, .start_block = 0, .page_count = DEVICE_PAGES};
        swap::SwapExtent const EXTENT{&outer_, 0, DEVICE_PAGES};
        int const RESULT = swap::activate_extents(name, &EXTENT, 1);
        if (RESULT == 0) active_ = true;
        return RESULT;
    }

    // Exactly one call after the handler joined every owned operation and
    // consumed its mapping. A refusal keeps both outer and inner storage live.
    auto detach() -> int {
        if (!active_ || detached_ || !provider_is_private()) return -ESTALE;
        int const RESULT = swap::selftest_try_detach_empty_area(expected_);
        if (RESULT == 0) {
            active_ = false;
            detached_ = true;
        }
        return RESULT;
    }

    auto live_counts(DeviceHistory& out) -> bool {
        if (!active_ || detached_ || !enter()) return false;
        DeviceHistory copied{};
        copied.writes = inner_.operation_count(FaultBlockOperation::WRITE);
        copied.reads = inner_.operation_count(FaultBlockOperation::READ);
        copied.flushes = inner_.operation_count(FaultBlockOperation::FLUSH);
        copied.events = inner_.event_count();
        copied.overflow = inner_.history_overflowed();
        copied.overlap = overlap_rejected_.load(std::memory_order_acquire);
        out = copied;
        gate_.unlock();
        return true;
    }

    auto final_history(DeviceHistory& out) -> bool {
        // Successful detach includes descriptor disposal, and excludes all
        // slot-backed callbacks. No inner history is inspected before this cut.
        if (active_ || !detached_ || !provider_is_private() || !gate_.try_lock()) return false;
        DeviceHistory copied{};
        copied.writes = inner_.operation_count(FaultBlockOperation::WRITE);
        copied.reads = inner_.operation_count(FaultBlockOperation::READ);
        copied.flushes = inner_.operation_count(FaultBlockOperation::FLUSH);
        copied.events = inner_.event_count();
        copied.overflow = inner_.history_overflowed();
        copied.overlap = overlap_rejected_.load(std::memory_order_acquire);
        auto const* first = inner_.event(0);
        auto const* second = inner_.event(1);
        copied.exact = copied.writes == 1 && copied.reads == 1 && copied.flushes == 0 && copied.events == 2 && copied.overflow == 0 &&
                       copied.overlap == 0 && first != nullptr && second != nullptr && first->operation == FaultBlockOperation::WRITE &&
                       second->operation == FaultBlockOperation::READ && first->global_index == 1 && second->global_index == 2 &&
                       first->operation_index == 1 && second->operation_index == 1 && first->count == 1 && second->count == 1 &&
                       first->block == second->block && first->block < DEVICE_PAGES && first->result == 0 && second->result == 0 &&
                       first->transferred_bytes == PAGE_BYTES && second->transferred_bytes == PAGE_BYTES;
        out = copied;
        gate_.unlock();
        return true;
    }

    auto active() const noexcept -> bool { return active_; }

   private:
    auto provider_is_private() const -> bool {
        return !dev::block_device_is_registered(&outer_) && !dev::block_queue_configured(&outer_) && outer_.major == 0 &&
               outer_.minor == 0 && outer_.block_size == PAGE_BYTES && outer_.total_blocks == DEVICE_PAGES && outer_.capabilities == 0 &&
               !outer_.read_only && outer_.remotable == nullptr && !outer_.is_partition && outer_.parent_disk == nullptr &&
               outer_.partition_start_lba == 0 && outer_.partition_end_lba == 0 && outer_.private_data == this &&
               outer_.read_blocks == read_callback && outer_.write_blocks == write_callback && outer_.flush == flush_callback;
    }
    auto enter() -> bool {
        if (gate_.try_lock()) return true;
        // A contending attempt creates no inner event, so history alone cannot
        // establish absence. Publish the sticky refusal before returning.
        overlap_rejected_.store(true, std::memory_order_release);
        return false;
    }
    static auto read_callback(dev::BlockDevice* device, uint64_t block, size_t count, void* buffer) -> int {
        auto* self = static_cast<SerializedFaultStorage*>(device->private_data);
        if (!self->enter()) return -EBUSY;
        auto* inner = self->inner_.device();
        int const RESULT = inner->read_blocks(inner, block, count, buffer);
        self->gate_.unlock();
        return RESULT;
    }
    static auto write_callback(dev::BlockDevice* device, uint64_t block, size_t count, const void* buffer) -> int {
        auto* self = static_cast<SerializedFaultStorage*>(device->private_data);
        if (!self->enter()) return -EBUSY;
        auto* inner = self->inner_.device();
        int const RESULT = inner->write_blocks(inner, block, count, buffer);
        self->gate_.unlock();
        return RESULT;
    }
    static auto flush_callback(dev::BlockDevice* device) -> int {
        auto* self = static_cast<SerializedFaultStorage*>(device->private_data);
        if (!self->enter()) return -EBUSY;
        auto* inner = self->inner_.device();
        int const RESULT = inner->flush(inner);
        self->gate_.unlock();
        return RESULT;
    }
    FaultBlockDevice inner_{PAGE_BYTES, DEVICE_PAGES};
    dev::BlockDevice outer_{};
    mod::sys::Mutex gate_{};
    std::atomic<bool> overlap_rejected_{false};
    swap::SelftestEmptyAreaExpectation expected_{};
    bool active_{}, detached_{};
};

// Instrumented heap storage is constructed and touched before activation. It
// contains no Task pointer/reference that could survive the genuine syscall.
struct CaseStorage {
    SerializedFaultStorage storage;
    std::array<uint8_t, PAGE_BYTES> bytes{};
    std::array<char, 96> area_name{};
    Report report{};
};
static_assert(sizeof(CaseStorage) < 1024 * 1024);
// Exactly one bounded exceptional owner; never reused as an operation session.
CaseStorage* quarantine{};

struct BusyOwner {
    ~BusyOwner() {
        if (!poisoned.load(std::memory_order_acquire)) busy.store(false, std::memory_order_release);
    }
};
struct RetainedFile {
    vfs::File* file{};
    ~RetainedFile() {
        if (file != nullptr) vfs::vfs_put_file(file);
    }
};

auto overlaps(uint64_t first, uint64_t size, uint64_t second, uint64_t other_size) -> bool {
    uint64_t end = 0, other_end = 0;
    return __builtin_add_overflow(first, size, &end) || __builtin_add_overflow(second, other_size, &other_end) ||
           (first < other_end && second < end);
}
auto pattern(size_t index) -> uint8_t { return static_cast<uint8_t>((index * 131 + 0x53) & 255); }
auto replacement(size_t index) -> uint8_t { return static_cast<uint8_t>((index * 29 + 0xa7) & 255); }

auto actor_admitted(Task& actor, uint64_t& serial) -> bool {
    if (!boot_enabled.load(std::memory_order_acquire) || !mod::sched::can_query_current_task() ||
        mod::sched::get_current_task() != &actor || actor.pid == 0 || actor.type != mod::sched::task::TaskType::PROCESS ||
        actor.is_thread || actor.credential_owner() != &actor ||
        actor.state.load(std::memory_order_acquire) != mod::sched::task::TaskState::ACTIVE || actor.syscall_account_start_us == 0 ||
        mod::sched::preempt_count() != 0 || !mod::sched::interrupts_enabled() || actor.has_exited ||
        actor.scheduler_transition_active.load(std::memory_order_acquire) || actor.deferred_task_switch || actor.wants_block ||
        actor.shares_user_pagemap.load(std::memory_order_acquire) || actor.resource_thread_owners.load(std::memory_order_acquire) != 0 ||
        actor.resource_cpu_threads.load(std::memory_order_acquire) != 0 || actor.wki_proxy_task ||
        actor.pending_wki_fd_snapshots != nullptr || actor.pending_wki_submissions != nullptr ||
        actor.pending_wki_submit_buffers != nullptr || actor.pending_wki_filesystems != nullptr ||
        actor.pending_signal_targets != nullptr || actor.pending_ptrace_operations != nullptr || actor.ptrace_traced ||
        actor.ptrace_stopped.load(std::memory_order_acquire) || actor.ptrace_stop_pending ||
        actor.owned_unpublished_process.load(std::memory_order_acquire) != nullptr ||
        actor.unpublished_teardown_in_progress.load(std::memory_order_acquire) ||
        actor.owned_exec_preparation.load(std::memory_order_acquire) != nullptr ||
        actor.owned_exec_arguments.load(std::memory_order_acquire) != nullptr ||
        actor.exec_preparation_teardown_in_progress.load(std::memory_order_acquire) ||
        actor.pending_exec_handoff.load(std::memory_order_acquire) != nullptr ||
        actor.active_exec_handoff.load(std::memory_order_acquire) != nullptr ||
        actor.native_file_mapping_retirement.load(std::memory_order_acquire) != nullptr ||
        actor.exit_in_progress.load(std::memory_order_acquire) || actor.process_exit_requested.load(std::memory_order_acquire) ||
        actor.usercopy_pagemap_closing.load(std::memory_order_acquire) || actor.futex_waiter.load(std::memory_order_acquire) != nullptr)
        return false;
    auto const* pid = actor.pid_namespace();
    auto const* mount = actor.mount_namespace();
    auto const* ipc = actor.ipc_namespace();
    if (pid == nullptr || mount == nullptr || ipc == nullptr || !pid->is_host() || !mount->is_host() || !ipc->is_host()) return false;
    auto const credentials = actor.credentials();
    auto const USER = credentials.user(), GROUP = credentials.group();
    if (USER.real != 0 || USER.effective != 0 || USER.saved != 0 || GROUP.real != 0 || GROUP.effective != 0 || GROUP.saved != 0 ||
        !credentials.allows(mod::security::Authority::KERNEL_DEBUG))
        return false;
    uint64_t const FLAGS = actor.resource_lock.lock_irqsave();
    bool const resources = !actor.resource_cpu_admission_closed && !actor.resource_exec_admission_closed &&
                           actor.resource_exec_quiescence_epoch.load(std::memory_order_acquire) == 0 &&
                           actor.resource_exec_quiescent_threads == 0 && !actor.resource_exec_safe_wait;
    actor.resource_lock.unlock_irqrestore(FLAGS);
    if (!resources || actor.memory_policy_snapshot() != mm::numa::Policy{}) return false;
    serial = mm::virt::current_user_root_incarnation(actor);
    return serial != 0;
}

auto request_valid(const Request& request, uint64_t request_address) -> bool {
    if (request.version != VERSION || request.bytes != sizeof(Request) ||
        request.case_id > static_cast<uint32_t>(Case::FUTEX_ZERO_TIMEOUT) || request.flags != 0 || request.reserved != 0 ||
        request.nonce == 0 || request.output_bytes != sizeof(Report) || request.address == 0 || request.address % PAGE_BYTES != 0 ||
        request.output_address % alignof(Report) != 0 ||
        request.length != (request.case_id == static_cast<uint32_t>(Case::WRITE_PREFIX) ? 2 * PAGE_BYTES : PAGE_BYTES) ||
        !uc::range_valid(request.address, request.length) || !uc::range_valid(request.output_address, sizeof(Report)) ||
        overlaps(request_address, sizeof(Request), request.address, request.length) ||
        overlaps(request.output_address, sizeof(Report), request.address, request.length) ||
        overlaps(request_address, sizeof(Request), request.output_address, sizeof(Report)))
        return false;
    if (request.case_id != static_cast<uint32_t>(Case::FUTEX_ZERO_TIMEOUT))
        return request.fd >= 0 && request.fd <= INT_MAX && request.timeout_address == 0;
    return request.fd == -1 && request.timeout_address != 0 && uc::range_valid(request.timeout_address, sizeof(timespec)) &&
           !overlaps(request.timeout_address, sizeof(timespec), request.address, request.length) &&
           !overlaps(request.timeout_address, sizeof(timespec), request_address, sizeof(Request)) &&
           !overlaps(request.timeout_address, sizeof(timespec), request.output_address, sizeof(Report));
}

auto mapping_valid(Task& actor, const Request& request, uint64_t serial, mm::virt::SelftestCurrentAnonymousPage& first) -> bool {
    if (actor.thread == nullptr || mod::sched::threading::range_overlaps_initial_layout(actor.thread, request.address, request.length) ||
        overlaps(request.address, request.length, actor.image_vaddr_start, actor.image_vaddr_end - actor.image_vaddr_start) ||
        overlaps(request.address, request.length, actor.interp_vaddr_start, actor.interp_vaddr_end - actor.interp_vaddr_start) ||
        (actor.sigaltstack_size != 0 && overlaps(request.address, request.length, actor.sigaltstack_sp, actor.sigaltstack_size)))
        return false;
    syscall::vmem::SharedVmemPublicationGuard publication;
    if (!mm::virt::selftest_observe_current_anonymous_page(actor, request.address, serial, first)) return false;
    auto row = [](const mm::virt::SelftestCurrentAnonymousPage& page, uint64_t start, uint64_t end, uint64_t prot) {
        return page.state == mm::virt::SelftestCurrentAnonymousState::NONE && page.resident == 1 && page.normal == 1 &&
               page.frame_refs == 1 && page.task_accesses == 0 && page.root_gate_readers == 0 && page.lazy_matches == 1 &&
               page.lazy_start == start && page.lazy_end == end && page.lazy_prot == prot &&
               page.lazy_kind == static_cast<uint64_t>(mod::sched::task::LazyVmemKind::ANONYMOUS) &&
               (page.lazy_flags & (abi::vmem::MAP_PRIVATE | abi::vmem::MAP_ANONYMOUS | abi::vmem::MAP_SHARED)) ==
                   (abi::vmem::MAP_PRIVATE | abi::vmem::MAP_ANONYMOUS);
    };
    if (!row(first, request.address, request.address + PAGE_BYTES, abi::vmem::PROT_READ | abi::vmem::PROT_WRITE) || first.user != 1)
        return false;
    if (request.case_id == static_cast<uint32_t>(Case::WRITE_PREFIX)) {
        mm::virt::SelftestCurrentAnonymousPage second{};
        if (!mm::virt::selftest_observe_current_anonymous_page(actor, request.address + PAGE_BYTES, serial, second) ||
            !row(second, request.address + PAGE_BYTES, request.address + 2 * PAGE_BYTES, abi::vmem::PROT_NONE) || second.user != 0)
            return false;
    }
    return true;
}

auto check_pattern(Task& actor, const Request& request, CaseStorage& storage, bool after_copy) -> bool {
    if (!uc::copy_from_task_mapped(actor, request.address, storage.bytes.data(), storage.bytes.size())) return false;
    for (size_t i = 0; i < storage.bytes.size(); ++i) {
        uint8_t const EXPECTED = after_copy && request.case_id == static_cast<uint32_t>(Case::READ_SUBRANGE) && i >= 64 && i < 192
                                     ? replacement(i - 64)
                                     : pattern(i);
        ++storage.report.pattern_bytes_checked;
        if (storage.bytes[i] != EXPECTED) return false;
    }
    return true;
}

auto perform_case(Task& actor, const Request& request, uint64_t serial, const mod::sys::SyscallPreparationScope& preparation,
                  const mm::numa::AllocationContext& placement, CaseStorage& storage) -> int {
    mm::virt::SelftestCurrentAnonymousPage before{};
    if (!mapping_valid(actor, request, serial, before) || !check_pattern(actor, request, storage, false)) return -EAGAIN;
    storage.report.source_ref1 = before.frame_refs;
    ++storage.report.checks;
    if (!mm::virt::selftest_pageout_current_anonymous_page(actor, request.address, serial, placement)) return -EIO;
    mm::virt::SelftestCurrentAnonymousPage swapped{};
    if (!mm::virt::selftest_observe_current_anonymous_page(actor, request.address, serial, swapped) ||
        swapped.state != mm::virt::SelftestCurrentAnonymousState::SWAPPED || swapped.resident != 0 || swapped.object_refs != 1 ||
        swapped.queued != 0 || swapped.cancelled != 0 || swapped.frame_present != 0 || swapped.waiter_present != 0 ||
        swapped.gate_present != 0 || swapped.task_accesses != 0 || swapped.root_gate_readers != 0)
        return -EIO;
    storage.report.selected_pages = 1;
    swap::SwapStats stats{};
    swap::get_stats(&stats);
    storage.report.used_swapped = stats.used_bytes / PAGE_BYTES;
    if (stats.active_areas != 1 || stats.total_bytes != DEVICE_PAGES * PAGE_BYTES || stats.used_bytes != PAGE_BYTES) return -EIO;
    DeviceHistory counts_before{}, counts_after{};
    if (!storage.storage.live_counts(counts_before)) return -EBUSY;
    storage.bytes.fill(0x6d);
    if (uc::copy_from_task_mapped(actor, request.address, storage.bytes.data(), storage.bytes.size())) return -EIO;
    for (auto byte : storage.bytes) {
        ++storage.report.guard_bytes_checked;
        if (byte != 0x6d) return -EIO;
    }
    if (!storage.storage.live_counts(counts_after) || counts_before.reads != counts_after.reads || counts_after.reads != 0) return -EIO;
    uint64_t fresh_serial = 0;
    if (!actor_admitted(actor, fresh_serial) || fresh_serial != serial) return -EPERM;
    {
        mod::sys::SyscallPreparationScope::LegacyInterval const LEGACY{preparation};
        if (request.case_id == static_cast<uint32_t>(Case::FUTEX_ZERO_TIMEOUT)) {
            uint32_t expected = 0;
            for (size_t i = 0; i < sizeof(expected); ++i) expected |= static_cast<uint32_t>(pattern(i)) << (8 * i);
            storage.report.operation_result = static_cast<int64_t>(syscall::futex::sys_futex(
                static_cast<uint64_t>(abi::futex::futex_ops::FUTEX_WAIT), request.address, expected, request.timeout_address));
            storage.report.expected_result = -ETIMEDOUT;
        } else {
            bool const READ = request.case_id == static_cast<uint32_t>(Case::READ_SUBRANGE);
            storage.report.operation_result = syscall::vfs::sys_vfs(
                static_cast<uint64_t>(READ ? abi::vfs::ops::READ : abi::vfs::ops::WRITE), static_cast<uint64_t>(request.fd),
                request.address + (READ ? 64 : 0), READ ? 128 : request.length, 0, 0);
            storage.report.expected_result = READ ? 128 : PAGE_BYTES;
        }
    }
    if (storage.report.operation_result != storage.report.expected_result || !check_pattern(actor, request, storage, true)) return -EIO;
    mm::virt::SelftestCurrentAnonymousPage resident{};
    if (!mm::virt::selftest_observe_current_anonymous_page(actor, request.address, serial, resident) ||
        resident.state != mm::virt::SelftestCurrentAnonymousState::NONE || resident.resident != 1 || resident.normal != 1 ||
        resident.frame_refs != 1 || resident.user != 1 || resident.task_accesses != 0 || resident.root_gate_readers != 0 ||
        actor.futex_waiter.load(std::memory_order_acquire) != nullptr || actor.deferred_task_switch)
        return -EIO;
    storage.report.resident_ref1 = resident.frame_refs;
    swap::get_stats(&stats);
    storage.report.used_after_copy = stats.used_bytes / PAGE_BYTES;
    ++storage.report.checks;
    return stats.active_areas == 1 && stats.total_bytes == DEVICE_PAGES * PAGE_BYTES && stats.used_bytes == 0 &&
                   stats.free_bytes == DEVICE_PAGES * PAGE_BYTES
               ? 0
               : -EIO;
}

auto consume_mapping(Task& actor, const Request& request, uint64_t serial, const mod::sys::SyscallPreparationScope& preparation,
                     Report& report) -> bool {
    // Cleanup is still owed with a pending fatal request. Require actual current
    // root identity, not renewed admission which would abandon that obligation.
    if (!mod::sched::can_query_current_task() || mod::sched::get_current_task() != &actor ||
        mm::virt::current_user_root_incarnation(actor) != serial)
        return false;
    uint64_t result = 0;
    {
        mod::sys::SyscallPreparationScope::LegacyInterval const LEGACY{preparation};
        result = syscall::vmem::sys_vmem(static_cast<uint64_t>(abi::vmem::ops::ANON_FREE), request.address, request.length, 0, 0);
    }
    if (result != 0) return false;
    for (uint64_t offset = 0; offset < request.length; offset += PAGE_BYTES) {
        mm::virt::SelftestCurrentAnonymousPage empty{};
        if (!mm::virt::selftest_observe_current_anonymous_page(actor, request.address + offset, serial, empty) ||
            empty.state != mm::virt::SelftestCurrentAnonymousState::NONE || empty.resident != 0 || empty.lazy_matches != 0 ||
            empty.task_accesses != 0 || empty.root_gate_readers != 0)
            return false;
        report.task_access_after = empty.task_accesses;
        report.root_readers_after = empty.root_gate_readers;
    }
    report.futex_waiter_after = actor.futex_waiter.load(std::memory_order_acquire) != nullptr;
    report.mapping_consumed = report.futex_waiter_after == 0;
    return report.mapping_consumed != 0;
}

}  // namespace

void enable_boot() noexcept { boot_enabled.store(true, std::memory_order_release); }

auto dispatch(uint64_t request_address, uint64_t bytes, uint64_t unused3, uint64_t unused4) -> uint64_t {
    if (!boot_enabled.load(std::memory_order_acquire) || !mod::sched::can_query_current_task()) return static_cast<uint64_t>(-ENOSYS);
    auto* actor = mod::sched::get_current_task();
    if (actor == nullptr || bytes != sizeof(Request) || unused3 != 0 || unused4 != 0 || !uc::range_valid(request_address, sizeof(Request)))
        return static_cast<uint64_t>(-EINVAL);
    mod::sys::SyscallPreparationScope preparation{*actor};
    uint64_t serial = 0;
    if (!preparation.ready() || !actor_admitted(*actor, serial)) return static_cast<uint64_t>(-EPERM);
    bool expected = false;
    if (poisoned.load(std::memory_order_acquire) || !busy.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
        return static_cast<uint64_t>(-EBUSY);
    BusyOwner const BUSY;
    Request request{};
    if (!uc::copy_from_task_mapped(*actor, request_address, &request, sizeof(request))) return static_cast<uint64_t>(-EFAULT);
    if (!request_valid(request, request_address)) return static_cast<uint64_t>(-EINVAL);
    // Real initiating policy/CPU capture before descriptor/resource/MM owners.
    mm::numa::AllocationContext placement{};
    if (mm::numa::capture_allocation_context(actor->memory_policy_snapshot(), placement) != mm::numa::AllocationError::NONE)
        return static_cast<uint64_t>(-ENODEV);
    placement.virtual_page_index = request.address / PAGE_BYTES;
    RetainedFile file;
    if (request.case_id != static_cast<uint32_t>(Case::FUTEX_ZERO_TIMEOUT)) {
        file.file = vfs::vfs_get_file_retain(actor, static_cast<int>(request.fd));
        if (file.file == nullptr || file.file->is_directory || file.file->fs_type != vfs::FSType::TMPFS ||
            file.file->fops != vfs::tmpfs::get_tmpfs_fops() || (file.file->open_flags & 3) != 2 || file.file->pos != 0 ||
            file.file->refcount.load(std::memory_order_acquire) != 2)
            return static_cast<uint64_t>(-EBADF);
        vfs::Stat stat{};
        if (vfs::vfs_fstat_file_native(file.file, &stat) != 0 || (stat.st_mode & vfs::S_IFMT) != vfs::S_IFREG ||
            stat.st_size != (request.case_id == static_cast<uint32_t>(Case::READ_SUBRANGE) ? 128 : 0))
            return static_cast<uint64_t>(-EINVAL);
    } else {
        timespec timeout{};
        if (!uc::copy_from_task_mapped(*actor, request.timeout_address, &timeout, sizeof(timeout))) return static_cast<uint64_t>(-EFAULT);
        if (timeout.tv_sec != 0 || timeout.tv_nsec != 0) return static_cast<uint64_t>(-EINVAL);
    }
    mm::virt::SelftestCurrentAnonymousPage first{};
    if (!mapping_valid(*actor, request, serial, first)) return static_cast<uint64_t>(-EINVAL);
    // All output bytes must already be writable resident4K control storage.
    {
        uc::StableUserPage control;
        for (uint64_t offset = 0; offset < sizeof(Report);) {
            uint64_t const ADDRESS = request.output_address + offset;
            if (!uc::pin_task_user_page(*actor, ADDRESS, true, false, control) || !control.still_mapped())
                return static_cast<uint64_t>(-EFAULT);
            uint64_t const REMAIN = PAGE_BYTES - (ADDRESS % PAGE_BYTES);
            offset += std::min<uint64_t>(sizeof(Report) - offset, REMAIN);
        }
    }
    swap::SwapStats stats{};
    swap::get_stats(&stats);
    if (stats.active_areas != 0 || stats.total_bytes != 0 || stats.used_bytes != 0) return static_cast<uint64_t>(-EBUSY);
    auto storage = std::unique_ptr<CaseStorage>(new (std::nothrow) CaseStorage{});
    if (storage == nullptr) return static_cast<uint64_t>(-ENOMEM);
    auto volatile* touched = reinterpret_cast<const volatile uint8_t*>(storage.get());
    for (size_t offset = 0; offset < sizeof(CaseStorage); offset += PAGE_BYTES) (void)touched[offset];
    (void)touched[sizeof(CaseStorage) - 1];
    auto& report = storage->report;
    report.version = VERSION;
    report.bytes = sizeof(Report);
    report.nonce = request.nonce;
    report.case_id = request.case_id;
    report.actor_pid = actor->pid;
    report.root_serial = serial;
    report.used_before = stats.used_bytes / PAGE_BYTES;
    report.total_before = stats.total_bytes / PAGE_BYTES;
    int const NAME = std::snprintf(storage->area_name.data(), storage->area_name.size(), "anonymous-usercopy-%llu-%llu",
                                   static_cast<unsigned long long>(actor->pid), static_cast<unsigned long long>(request.nonce));
    if (NAME <= 0 || static_cast<size_t>(NAME) >= storage->area_name.size()) return static_cast<uint64_t>(-EINVAL);
    uint64_t fresh_serial = 0;
    if (!actor_admitted(*actor, fresh_serial) || fresh_serial != serial) return static_cast<uint64_t>(-EPERM);
    int const ACTIVATE = storage->storage.activate(storage->area_name.data());
    if (ACTIVATE != 0) return static_cast<uint64_t>(ACTIVATE);
    // From this point every result converges on actual consumed-range cleanup
    // and one atomic empty detach. Never return through an active-device dtor.
    swap::get_stats(&stats);
    report.total_active = stats.total_bytes / PAGE_BYTES;
    report.free_active = stats.free_bytes / PAGE_BYTES;
    int status = stats.active_areas == 1 && stats.total_bytes == DEVICE_PAGES * PAGE_BYTES && stats.used_bytes == 0 &&
                         stats.free_bytes == DEVICE_PAGES * PAGE_BYTES
                     ? perform_case(*actor, request, serial, preparation, placement, *storage)
                     : -EBUSY;
    bool const CONSUMED = consume_mapping(*actor, request, serial, preparation, report);
    int const DETACH = CONSUMED ? storage->storage.detach() : -EIO;
    if (!CONSUMED || DETACH != 0) {
        poisoned.store(true, std::memory_order_release);
        quarantine = storage.release();
        return static_cast<uint64_t>(status != 0 ? status : (DETACH != 0 ? DETACH : -EIO));
    }
    DeviceHistory history{};
    bool const HISTORY = storage->storage.final_history(history);
    if (!HISTORY) {
        poisoned.store(true, std::memory_order_release);
        quarantine = storage.release();
        return static_cast<uint64_t>(-EIO);
    }
    report.block_writes = history.writes;
    report.block_reads = history.reads;
    report.history_overflow = history.overflow;
    swap::get_stats(&stats);
    report.total_after_cleanup = stats.total_bytes / PAGE_BYTES;
    report.used_after_cleanup = stats.used_bytes / PAGE_BYTES;
    report.cleanup_complete = stats.active_areas == 0 && stats.total_bytes == 0 && stats.used_bytes == 0;
    if (!HISTORY || !history.exact || !report.cleanup_complete) status = -EIO;
    report.valid = status == 0;
    Report const copied = report;
    storage.reset();
    if (!uc::copy_to_task_mapped(*actor, request.output_address, &copied, sizeof(copied))) return static_cast<uint64_t>(-EFAULT);
    return static_cast<uint64_t>(status);
}

}  // namespace ker::test::anonymous_usercopy
#endif
