#include <dev/firmware/boot_source.hpp>
#include <dev/firmware/cache.hpp>
#include <dev/firmware/firmware.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/mutex.hpp>
#include <util/hcf.hpp>

namespace ker::dev::firmware {
namespace {
namespace sched = ker::mod::sched;
ker::mod::sys::Mutex mutex;
Cache cache;
bool bound = false;
uint64_t next_scope = 0;

auto ordinary_task() -> sched::task::Task* {
    if (!ker::mod::mm::phys::can_wait_for_reclaim()) return nullptr;
    return sched::get_current_task();
}

auto bind_source() -> Error {
    auto const STATUS = source_status();
    if (STATUS != Error::none) return STATUS;
    if (!bound) {
        auto const RESULT = cache.bind(source_index());
        if (RESULT != Error::none) return RESULT;
        bound = true;
    }
    return Error::none;
}

[[noreturn]] void lifetime_violation() {
    ker::mod::dbg::panic_handler("firmware Scope destroyed outside its owning ordinary task");
    hcf();
}
}  // namespace

Scope::Scope() : owner_(ordinary_task()), hold_(owner_ == nullptr ? nullptr : &owner_->kernel_shutdown_ownership) {
    if (owner_ == nullptr) return;
    ker::mod::sys::MutexGuard const GUARD(mutex);
    if (next_scope == UINT64_MAX) {
        error_ = Error::generation_exhausted;
        return;
    }
    identity_ = ++next_scope;
    error_ = Error::none;
}

auto Scope::valid_context() const -> bool { return owner_ != nullptr && ordinary_task() == owner_; }

Scope::~Scope() {
    if (owner_ == nullptr) return;
    if (!valid_context()) lifetime_violation();
    ker::mod::sys::MutexGuard const GUARD(mutex);
    for (auto& lease : leases_) {
        if (lease.sequence == 0) continue;
        if (cache.release(lease.object) != Error::none) lifetime_violation();
        lease = {};
    }
    // Mutex is released, then hold_ drops the task's shutdown ownership.
}

auto Scope::request(const Policy& policy, uint32_t required_revision, View& out) -> Error {
    if (!valid_context()) return Error::context;
    if (error_ != Error::none) return error_;
    Lease* available = nullptr;
    for (auto& lease : leases_) {
        if (lease.sequence == 0) {
            available = &lease;
            break;
        }
    }
    if (available == nullptr) return Error::scope_full;
    if (next_lease_ == UINT64_MAX) return Error::generation_exhausted;
    ker::mod::sys::MutexGuard const GUARD(mutex);
    auto const SOURCE = bind_source();
    if (SOURCE != Error::none) return SOURCE;
    auto const RESULT = cache.acquire(policy, required_revision);
    if (!RESULT.valid()) return RESULT.error;
    available->object = RESULT.handle;
    available->sequence = ++next_lease_;
    out = {RESULT.bytes, RESULT.handle, identity_, available->sequence};
    return Error::none;
}

auto Scope::release(const View& view) -> Error {
    if (!valid_context()) return Error::context;
    if (error_ != Error::none) return error_;
    if (view.scope != identity_ || view.lease == 0) return Error::stale;
    for (auto& lease : leases_) {
        if (lease.sequence != view.lease || lease.object != view.object) continue;
        ker::mod::sys::MutexGuard const GUARD(mutex);
        auto const RESULT = cache.release(lease.object);
        if (RESULT == Error::none) lease = {};
        return RESULT;
    }
    return Error::stale;
}

auto snapshot(Stats& out) -> Error {
    if (ordinary_task() == nullptr) return Error::context;
    auto const HOLD = sched::hold_kernel_shutdown();
    ker::mod::sys::MutexGuard const GUARD(mutex);
    out = cache.stats();
    return Error::none;
}
}  // namespace ker::dev::firmware
