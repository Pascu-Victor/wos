#include "device_mapping_vm_ktest.hpp"

#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <dev/device_mapping_client.hpp>
#include <memory>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/device_mapping_revoke.hpp>
#include <platform/mm/lifetime_retirement_executor.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sys/syscall_preparation.hpp>
#include <platform/sys/usercopy.hpp>
#include <string_view>
#include <vfs/fs/devfs.hpp>

namespace ker::test {
namespace {
namespace mapping = ker::dev::mapping;
namespace fm = ker::mod::mm::file_mapping;
namespace sched = ker::mod::sched;
namespace usercopy = ker::mod::sys::usercopy;
namespace devfs = ker::vfs::devfs;
using log = ker::mod::dbg::logger<"device-vm-test">;
constexpr char NAME[] = "ktest_device_mapping_wb";
constexpr uint64_t MAGIC = 0x574f534442574231ULL, VERSION = 1, PAGE = 4096, BYTES = 160 * PAGE;
constexpr uint64_t READ = 1, WRITE = 2;
enum Command : uint64_t { CHECK = 1, STORE, REMOVE_GRANT, HOLD, DROP, REVOKE, ASSERT_READS, DONE, RAW_COPY_REFUSAL };
struct Record {
    uint64_t magic, version, bytes, base, end, grant_generation, object_generation, page_size;
};
struct Request {
    uint64_t magic, command, offset, value, extra;
    std::array<uint64_t, 3> reserved;
};
static_assert(sizeof(Record) == 64 && sizeof(Request) == 64);
std::atomic<bool> started{};

struct Context final : mapping::EndpointContext {
    mapping::QuotaAccountRef account;
    mapping::ObjectRef object;
    usercopy::UserPageFrameRef retained_frame;
    std::atomic<bool> busy{}, retiring{}, done{}, failed{}, revoke_logged{};
    std::atomic<unsigned> reads{}, closes{}, commands{};
};

class CallbackClaim {
   public:
    explicit CallbackClaim(Context& context) : context_(context), held_(!context.busy.exchange(true, std::memory_order_acquire)) {}
    ~CallbackClaim() {
        if (held_) context_.busy.store(false, std::memory_order_release);
    }
    auto valid() const -> bool { return held_ && !context_.retiring.load(std::memory_order_acquire); }

   private:
    Context& context_;
    bool held_;
};

auto context(ker::vfs::File* file) -> Context* {
    return file != nullptr && file->device_client ? &static_cast<Context&>(file->device_client.get()->endpoint().context()) : nullptr;
}
auto read_record(ker::vfs::File* file, void* output, size_t count) -> ssize_t {
    auto* value = context(file);
    auto* actor = sched::can_query_current_task() ? sched::get_current_task() : nullptr;
    if (value == nullptr || actor == nullptr) return -EBADF;
    ker::mod::sys::SyscallPreparationScope preparation(*actor);
    if (!preparation.ready()) return -EWOULDBLOCK;
    CallbackClaim claim(*value);
    if (!claim.valid()) return -EBUSY;
    ++value->reads;
    if (count != sizeof(Record) || output == nullptr || file->pos != 0 || !value->object) return -EINVAL;
    int const configured = file->device_client.get()->configure_grants(1);
    if (configured != 0 && configured != -EALREADY) return configured;
    mapping::Grant grant;
    int const result = file->device_client.get()->grant(value->object, 0, BYTES, READ | WRITE, grant);
    if (result != 0) return result;
    Record const record{MAGIC, VERSION, BYTES, grant.base, grant.end, grant.generation, value->object.get()->generation(), PAGE};
    std::memcpy(output, &record, sizeof(record));
    log::info("DEVICE_VM_RUNTIME RECORD pid=%lu base=%lx bytes=%lu", actor->pid, grant.base, BYTES);
    return sizeof(record);
}
auto command(ker::vfs::File* file, const void* input, size_t count) -> ssize_t {
    auto* value = context(file);
    auto* actor = sched::can_query_current_task() ? sched::get_current_task() : nullptr;
    if (value == nullptr || actor == nullptr) return -EBADF;
    ker::mod::sys::SyscallPreparationScope preparation(*actor);
    if (!preparation.ready()) return -EWOULDBLOCK;
    CallbackClaim claim(*value);
    if (!claim.valid()) return -EBUSY;
    if (input == nullptr || count != sizeof(Request) || !value->object) return -EINVAL;
    Request request{};
    std::memcpy(&request, input, sizeof(request));
    if (request.magic != MAGIC || request.reserved != std::array<uint64_t, 3>{}) return -EINVAL;
    ++value->commands;
    switch (request.command) {
        case CHECK:
        case STORE: {
            if (request.offset % sizeof(uint64_t) != 0 || request.offset > BYTES - sizeof(uint64_t)) return -EINVAL;
            auto* page = static_cast<std::byte*>(value->object.get()->cpu_page(request.offset & ~(PAGE - 1)));
            auto* word = reinterpret_cast<uint64_t*>(page + (request.offset & (PAGE - 1)));
            std::atomic_ref<uint64_t> cell(*word);
            if (request.command == STORE)
                cell.store(request.value, std::memory_order_release);
            else if (cell.load(std::memory_order_acquire) != request.value)
                return -EIO;
            break;
        }
        case REMOVE_GRANT: {
            int const result =
                file->device_client.get()->remove_grant({.generation = request.extra, .base = request.offset, .end = request.value});
            if (result != 0) return result;
            break;
        }
        case HOLD: {
            if (value->retained_frame.valid() || (request.value & (PAGE - 1)) != 0) return -EINVAL;
            uint64_t physical{};
            usercopy::StableUserPage pin;
            if (!value->object.get()->frame_at(request.offset, physical) ||
                !usercopy::pin_task_user_page(*actor, request.value, false, false, pin) || pin.physical_address() != physical ||
                !pin.still_mapped() || !pin.retain_frame(value->retained_frame))
                return -EFAULT;
            if (value->object.get()->owners().exports != 1) {
                value->retained_frame.reset();
                return -EIO;
            }
            break;
        }
        case DROP: {
            if (!value->retained_frame.valid()) return -EINVAL;
            ker::mod::sys::Spinlock irq_lock;
            auto const flags = irq_lock.lock_irqsave();
            value->retained_frame.reset();
            irq_lock.unlock_irqrestore(flags);
            break;
        }
        case REVOKE: {
            if (!value->revoke_logged.exchange(true))
                log::info("DEVICE_VM_RUNTIME REVOKE caller_pid=%lu expected_child_pid=%lu expected_va=%lx", actor->pid, request.offset,
                          request.value);
            int const result = ker::mod::mm::device_vm::revoke_cpu_mappings(value->object);
            if (result != 0) return result;
            break;
        }
        case RAW_COPY_REFUSAL: {
            namespace virt = ker::mod::mm::virt;
            auto* private_root = virt::create_pagemap_may_fail();
            if (private_root == nullptr) return -ENOMEM;
            bool const initially_empty = virt::unpublished_user_root_empty(private_root);
            bool const copied = initially_empty && virt::deep_copy_user_pagemap_cow(actor->pagemap, private_root);
            bool const stayed_empty = virt::unpublished_user_root_empty(private_root);
            virt::destroy_user_space(private_root, 0, "device-raw-copy-test", "private-refusal-target");
            virt::release_pagemap(private_root);
            if (!initially_empty || copied || !stayed_empty) return -EIO;
            log::info("DEVICE_VM_RUNTIME RAW_COPY_REFUSED pid=%lu empty_target=1", actor->pid);
            break;
        }
        case ASSERT_READS:
            if (value->reads.load(std::memory_order_acquire) != request.value) return -EIO;
            break;
        case DONE:
            value->failed.store(request.value != 0, std::memory_order_release);
            value->done.store(true, std::memory_order_release);
            break;
        default:
            return -EINVAL;
    }
    return sizeof(request);
}
auto close(ker::vfs::File* file) -> int {
    auto* value = context(file);
    if (value == nullptr) return -EBADF;
    ++value->closes;
    return 0;
}

void require(bool valid, const char* reason) {
    if (valid) return;
    log::error("DEVICE_VM_RUNTIME FAIL %s", reason);
    ker::mod::dbg::panic_handler("public device mapping fixture failed");
}
void returned(fm::RetirementCompletion& completion) {
    uint64_t const deadline = sched::saturating_deadline_us(ker::mod::time::get_us(), 10'000'000);
    for (;;) {
        auto const status = completion.query();
        if (status.state == fm::RetirementReturnState::RETURNED) {
            require(status.callback_result == 0, "actual retirement callback returned success");
            return;
        }
        require(status.state == fm::RetirementReturnState::PENDING && ker::mod::time::get_us() < deadline,
                "bounded exact Object/account return");
        sched::kern_sleep_us(1000);
    }
}
void wait_init() {
    uint64_t const deadline = sched::saturating_deadline_us(ker::mod::time::get_us(), 30'000'000);
    for (;;) {
        auto* init = sched::find_task_by_pid_safe(1);
        if (init != nullptr) {
            sched::task::Task::FilesystemContext path;
            bool const ready = init->capture_filesystem_context(path) && std::string_view(path.root.data(), path.root_len) == "/rootfs";
            init->release();
            if (ready) return;
        }
        require(ker::mod::time::get_us() < deadline, "PID1 root pivot before user fixture readiness");
        sched::kern_sleep_us(10000);
    }
}
void runtime_entry() {
    {
        auto const hold = sched::hold_kernel_shutdown();
        require(ker::mod::mm::phys::can_wait_for_reclaim(), "ordinary scheduled provider controller");
        std::unique_ptr<Context> owned(new (std::nothrow) Context);
        require(owned != nullptr, "owned provider context");
        auto* value = owned.get();
        require(mapping::QuotaAccount::create({.resident_pages = 160, .objects = 1}, value->account) == 0 &&
                    mapping::Object::create(value->account, BYTES, READ | WRITE, value->object) == 0,
                "real WB Object and independent quota");
        fm::RetirementCompletion object_done, account_done;
        require(value->object.get()->observe_return(object_done) && value->account.get()->observe_return(account_done),
                "actual exact-generation cleanup observers");
        mapping::EndpointRef endpoint;
        require(mapping::Endpoint::create(240, 4, NAME, {.close = close, .read = read_record, .write = command}, std::move(owned),
                                          endpoint) == 0,
                "retained endpoint without legacy open hooks");
        auto const root = ker::mod::security::Snapshot::bootstrap_root();
        require(devfs::devfs_add_mapping_endpoint(NAME, endpoint, &root) != nullptr, "exclusive test-only devfs publication");
        wait_init();
        log::info("DEVICE_VM_RUNTIME READY path=/dev/%s bytes=%lu protocol=1 genuine_user_process_required=1", NAME, BYTES);
        uint64_t const deadline = sched::saturating_deadline_us(ker::mod::time::get_us(), 300'000'000);
        while (!value->done.load(std::memory_order_acquire)) {
            require(ker::mod::time::get_us() < deadline, "bounded wait for external public user fixture");
            sched::kern_sleep_us(10000);
        }
        value->retiring.store(true, std::memory_order_release);
        endpoint.get()->close_admission();
        require(devfs::devfs_remove_node(NAME), "unlink exact-owned test endpoint");
        while (value->busy.load(std::memory_order_acquire)) {
            require(ker::mod::time::get_us() < deadline, "in-flight actual user callback returns");
            sched::kern_sleep_us(1000);
        }
        bool const user_pass = !value->failed.load(std::memory_order_acquire);
        if (user_pass) {
            require(!value->retained_frame.valid() && value->object.get()->state() == mapping::ObjectState::revoked,
                    "user completed actual export return and CPU revoke");
        } else {
            value->retained_frame.reset();
            for (;;) {
                int const result = ker::mod::mm::device_vm::revoke_cpu_mappings(value->object);
                if (result == 0) break;
                require(result == -EAGAIN && ker::mod::time::get_us() < deadline, "failed user fixture CPU cleanup completes");
                sched::kern_sleep_us(1000);
            }
        }
        for (;;) {
            auto const counts = value->object.get()->owners();
            if (counts.total == 1 && counts.by_role[static_cast<size_t>(mapping::ObjectRole::owner)] == 1) break;
            require(ker::mod::time::get_us() < deadline, "user Files/families/grants/exports drain");
            sched::kern_sleep_us(1000);
        }
        unsigned const reads = value->reads.load(), closes = value->closes.load();
        if (user_pass) require(reads == 3 && closes == 3, "no mmap read fallback and all real Files closed");
        value->object.reset();
        returned(object_done);
        require(value->account.get()->usage().resident_pages == 0 && value->account.get()->usage().objects == 0,
                "actual backing and quota returned");
        value->account.reset();
        returned(account_done);
        log::info(
            "DEVICE_VM_RUNTIME %s user_report=%u reads=%u closes=%u quota_pages=0 quota_objects=0 object_returned=1 account_returned=1",
            user_pass ? "PASS" : "FAIL", user_pass ? 1U : 0U, reads, closes);
        endpoint.reset();
    }
    {
        auto const hold = sched::hold_kernel_shutdown();
        auto* init = sched::find_task_by_pid_safe(1);
        require(init != nullptr, "retain PID1 for normal shutdown");
        init->signal_generate_pending_mask(uint64_t{1} << 11);
        sched::wake_task_for_signal(init);
        init->release();
        log::info("DEVICE_VM_RUNTIME POWEROFF_REQUESTED via=init-sigusr2");
    }
    sched::exit_current_kernel_thread();
}
}  // namespace

auto start_device_mapping_vm_selftest() -> bool {
    bool expected = false;
    if (!started.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) return false;
    auto* worker = sched::task::Task::create_kernel_thread("device_mapping_vm_fixture", runtime_entry);
    if (worker == nullptr || !sched::post_task_balanced(worker)) {
        if (worker != nullptr) sched::task::destroy_unpublished_user_thread(worker);
        started.store(false, std::memory_order_release);
        return false;
    }
    return true;
}
}  // namespace ker::test
