#include "device_mapping_object.hpp"

#include <abi/callnums/vmem.h>

#include <atomic>
#include <cerrno>
#include <cstring>
#include <limits>
#include <new>
#include <platform/mm/addr.hpp>
#include <platform/mm/cache_policy.hpp>
#include <platform/mm/lifetime_retirement_executor.hpp>
#include <platform/mm/phys.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <utility>

namespace ker::dev::mapping {
namespace {
namespace fm = ker::mod::mm::file_mapping;
namespace phys = ker::mod::mm::phys;
constinit detail::ObjectGenerations generations;
auto valid_port(const fm::LifetimeRetirementPort* port) noexcept -> bool {
    return port != nullptr && port->reserve != nullptr && port->submit_ready != nullptr && port->cancel_reserved != nullptr &&
           port->observe_return != nullptr;
}
auto physical(void* page) noexcept -> uint64_t {
    return reinterpret_cast<uint64_t>(ker::mod::mm::addr::get_phys_pointer(reinterpret_cast<uint64_t>(page)));
}
auto valid_role(ObjectRole role) noexcept -> bool { return role < ObjectRole::count; }
}  // namespace

auto detail::ObjectGenerations::issue() noexcept -> uint64_t {
    uint64_t value = last_.load(std::memory_order_relaxed);
    while (value != UINT64_MAX) {
        if (last_.compare_exchange_weak(value, value + 1, std::memory_order_relaxed)) return value + 1;
    }
    return 0;
}

QuotaAccountRef::~QuotaAccountRef() { reset(); }
QuotaAccountRef::QuotaAccountRef(QuotaAccountRef&& other) noexcept : account_(std::exchange(other.account_, nullptr)) {}
auto QuotaAccountRef::operator=(QuotaAccountRef&& other) noexcept -> QuotaAccountRef& {
    if (this != &other) {
        reset();
        account_ = std::exchange(other.account_, nullptr);
    }
    return *this;
}
auto QuotaAccountRef::clone_into(QuotaAccountRef& empty) const noexcept -> bool {
    if (empty || account_ == nullptr || !account_->retain()) return false;
    empty.account_ = account_;
    return true;
}
void QuotaAccountRef::reset() noexcept {
    auto* account = std::exchange(account_, nullptr);
    if (account != nullptr) account->release();
}

auto QuotaAccount::create(QuotaLimits limits, QuotaAccountRef& empty) -> int {
    if (empty || limits.resident_pages == 0 || limits.objects == 0 || limits.max_object_pages == 0) return -EINVAL;
    if (!phys::can_wait_for_reclaim()) return -EWOULDBLOCK;
    auto const* port = fm::LifetimeRetirementExecutor::port();
    if (!valid_port(port)) return -ENODEV;
    auto* account = new (std::nothrow) QuotaAccount(limits);
    if (account == nullptr) return -ENOMEM;
    account->port_ = port;
    account->ticket_ = port->reserve(port->context, finish, account);
    if (account->ticket_ == nullptr) {
        delete account;
        return -ENOSPC;
    }
    empty = QuotaAccountRef(account);
    return 0;
}
auto QuotaAccount::usage() const noexcept -> QuotaUsage {
    auto const flags = lock_.lock_irqsave();
    auto const usage = usage_;
    lock_.unlock_irqrestore(flags);
    return usage;
}
auto QuotaAccount::observe_return(fm::RetirementCompletion& empty) noexcept -> bool {
    return port_->observe_return(port_->context, *ticket_, finish, this, empty);
}
auto QuotaAccount::charge(uint64_t pages) noexcept -> bool {
    auto const flags = lock_.lock_irqsave();
    bool const valid = references_ != 0 && pages != 0 && pages <= limits_.max_object_pages &&
                       usage_.resident_pages <= limits_.resident_pages && pages <= limits_.resident_pages - usage_.resident_pages &&
                       usage_.objects < limits_.objects;
    if (valid) {
        usage_.resident_pages += pages;
        ++usage_.objects;
    }
    lock_.unlock_irqrestore(flags);
    return valid;
}
void QuotaAccount::uncharge(uint64_t pages) noexcept {
    auto const flags = lock_.lock_irqsave();
    if (pages == 0 || usage_.resident_pages < pages || usage_.objects == 0) __builtin_trap();
    usage_.resident_pages -= pages;
    --usage_.objects;
    lock_.unlock_irqrestore(flags);
}
auto QuotaAccount::retain() noexcept -> bool {
    auto const flags = lock_.lock_irqsave();
    bool const valid = references_ != 0 && references_ != UINT64_MAX;
    if (valid) ++references_;
    lock_.unlock_irqrestore(flags);
    return valid;
}
void QuotaAccount::release() noexcept {
    auto const flags = lock_.lock_irqsave();
    if (references_ == 0) __builtin_trap();
    bool const last = --references_ == 0;
    auto const* port = port_;
    auto* ticket = ticket_;
    lock_.unlock_irqrestore(flags);
    if (last) port->submit_ready(port->context, *ticket);
    // The real worker may have deleted the account already.
}
auto QuotaAccount::finish(void* argument) noexcept -> int {
    auto* account = static_cast<QuotaAccount*>(argument);
    if (!phys::can_wait_for_reclaim() || account->references_ != 0 || account->usage_.resident_pages != 0 || account->usage_.objects != 0)
        __builtin_trap();
    delete account;
    return 0;
}

ObjectRef::~ObjectRef() { reset(); }
ObjectRef::ObjectRef(ObjectRef&& other) noexcept : object_(std::exchange(other.object_, nullptr)), role_(other.role_) {}
auto ObjectRef::operator=(ObjectRef&& other) noexcept -> ObjectRef& {
    if (this != &other) {
        reset();
        object_ = std::exchange(other.object_, nullptr);
        role_ = other.role_;
    }
    return *this;
}
auto ObjectRef::clone_into(ObjectRef& empty, ObjectRole role) const noexcept -> bool {
    if (empty || object_ == nullptr || !object_->retain(role)) return false;
    empty.object_ = object_;
    empty.role_ = role;
    return true;
}
void ObjectRef::reset() noexcept {
    auto* object = std::exchange(object_, nullptr);
    if (object != nullptr) object->release(role_);
}
ObjectExportRef::~ObjectExportRef() { reset(); }
ObjectExportRef::ObjectExportRef(ObjectExportRef&& other) noexcept : object_(std::exchange(other.object_, nullptr)) {}
auto ObjectExportRef::operator=(ObjectExportRef&& other) noexcept -> ObjectExportRef& {
    if (this != &other) {
        reset();
        object_ = std::exchange(other.object_, nullptr);
    }
    return *this;
}
auto ObjectExportRef::clone_into(ObjectExportRef& empty) const noexcept -> bool {
    if (empty || object_ == nullptr || !object_->retain(ObjectRole::owner, true)) return false;
    empty.object_ = object_;
    return true;
}
void ObjectExportRef::reset() noexcept {
    auto* object = std::exchange(object_, nullptr);
    if (object != nullptr) object->release(ObjectRole::owner, true);
}
PageLoan::~PageLoan() { reset(); }
PageLoan::PageLoan(PageLoan&& other) noexcept
    : object_(std::exchange(other.object_, nullptr)),
      page_(std::exchange(other.page_, nullptr)),
      physical_(std::exchange(other.physical_, 0)) {}
auto PageLoan::operator=(PageLoan&& other) noexcept -> PageLoan& {
    if (this != &other) {
        reset();
        object_ = std::exchange(other.object_, nullptr);
        page_ = std::exchange(other.page_, nullptr);
        physical_ = std::exchange(other.physical_, 0);
    }
    return *this;
}
void PageLoan::reset() noexcept {
    auto* object = std::exchange(object_, nullptr);
    auto* page = std::exchange(page_, nullptr);
    physical_ = 0;
    if (object != nullptr) {
        static_cast<void>(phys::page_ref_dec(page));
        object->release(ObjectRole::loan);
    }
}

Object::~Object() {
    if (pages_ != nullptr) {
        for (size_t index = 0; index < page_count_; ++index)
            if (pages_[index] != nullptr) static_cast<void>(phys::page_ref_dec(pages_[index]));
        delete[] pages_;
    }
    if (account_) account_.get()->uncharge(page_count_);
}
auto Object::create(const QuotaAccountRef& account, uint64_t bytes, uint64_t max_protections, ObjectRef& empty) -> int {
    constexpr uint64_t RIGHTS = ker::abi::vmem::PROT_READ | ker::abi::vmem::PROT_WRITE;
    if (empty || !account || bytes == 0 || bytes % OBJECT_PAGE_SIZE != 0 || (max_protections & ~RIGHTS) != 0 ||
        (max_protections & ker::abi::vmem::PROT_READ) == 0)
        return -EINVAL;
    uint64_t const pages = bytes / OBJECT_PAGE_SIZE;
    if (pages > std::numeric_limits<size_t>::max() / sizeof(void*)) return -EOVERFLOW;
    if (!phys::can_wait_for_reclaim()) return -EWOULDBLOCK;
    auto const cache = ker::mod::mm::cache::qualification();
    if (!cache.complete || !cache.wb_encoding) return -EOPNOTSUPP;
    auto const* port = fm::LifetimeRetirementExecutor::port();
    if (!valid_port(port)) return -ENODEV;
    QuotaAccountRef retained;
    if (!account.clone_into(retained)) return -EOVERFLOW;
    if (!account.get()->charge(pages)) return -EDQUOT;
    auto* object = new (std::nothrow) Object;
    if (object == nullptr) {
        account.get()->uncharge(pages);
        return -ENOMEM;
    }
    object->account_ = std::move(retained);
    object->page_count_ = static_cast<size_t>(pages);
    object->bytes_ = bytes;
    object->max_protections_ = max_protections;
    object->port_ = port;
    auto fail = [object, port](int error) {
        if (object->ticket_ != nullptr && !port->cancel_reserved(port->context, *object->ticket_, finish, object)) __builtin_trap();
        delete object;
        return error;
    };
    object->generation_ = generations.issue();
    if (object->generation_ == 0) return fail(-ENOSPC);
    object->pages_ = new (std::nothrow) void*[object->page_count_]{};
    if (object->pages_ == nullptr) return fail(-ENOMEM);
    object->ticket_ = port->reserve(port->context, finish, object);
    if (object->ticket_ == nullptr) return fail(-ENOSPC);
    for (size_t index = 0; index < object->page_count_; ++index) {
        auto* page = phys::page_alloc_may_fail(ker::mod::mm::PhysicalPageOwner::USER_SHARED_MEMORY, OBJECT_PAGE_SIZE, "device-object");
        if (page == nullptr) return fail(-ENOMEM);
        object->pages_[index] = page;
        std::memset(page, 0, OBJECT_PAGE_SIZE);
    }
    object->owners_.by_role.at(static_cast<size_t>(ObjectRole::owner)) = 1;
    object->owners_.total = 1;
    empty = ObjectRef(object, ObjectRole::owner);
    return 0;
}
auto Object::owners() const noexcept -> ObjectOwners {
    auto const flags = lock_.lock_irqsave();
    auto const owners = owners_;
    lock_.unlock_irqrestore(flags);
    return owners;
}
auto Object::state() const noexcept -> ObjectState {
    auto const flags = lock_.lock_irqsave();
    auto const state = state_;
    lock_.unlock_irqrestore(flags);
    return state;
}
auto Object::frame_at(uint64_t offset, uint64_t& out) const noexcept -> bool {
    auto* page = cpu_page(offset);
    if (page == nullptr) return false;
    out = physical(page);
    return true;
}
auto Object::cpu_page(uint64_t offset) const noexcept -> void* {
    if (offset % OBJECT_PAGE_SIZE != 0 || offset >= bytes_) return nullptr;
    return pages_[static_cast<size_t>(offset / OBJECT_PAGE_SIZE)];
}
auto Object::retain(ObjectRole role, bool exporting) noexcept -> bool {
    if (!valid_role(role)) return false;
    auto const flags = lock_.lock_irqsave();
    auto& count = exporting ? owners_.exports : owners_.by_role.at(static_cast<size_t>(role));
    bool const live_required = exporting || role != ObjectRole::owner;
    bool const valid =
        owners_.total != 0 && owners_.total != UINT64_MAX && count != UINT64_MAX && (!live_required || state_ == ObjectState::live);
    if (valid) {
        ++count;
        ++owners_.total;
    }
    lock_.unlock_irqrestore(flags);
    return valid;
}
void Object::release(ObjectRole role, bool exporting) noexcept {
    if (!valid_role(role)) __builtin_trap();
    auto const flags = lock_.lock_irqsave();
    auto& count = exporting ? owners_.exports : owners_.by_role.at(static_cast<size_t>(role));
    if (owners_.total == 0 || count == 0) __builtin_trap();
    --count;
    bool const last = --owners_.total == 0;
    auto const* port = port_;
    auto* ticket = ticket_;
    lock_.unlock_irqrestore(flags);
    if (last) port->submit_ready(port->context, *ticket);
    // No Object/member guard access follows publication to the actual worker.
}
auto Object::prepare_page(uint64_t offset, PageLoan& empty) noexcept -> bool {
    auto* page = cpu_page(offset);
    if (empty || page == nullptr || !retain(ObjectRole::loan)) return false;
    if (!phys::page_ref_try_inc(page)) {
        release(ObjectRole::loan);
        return false;
    }
    empty.object_ = this;
    empty.page_ = page;
    empty.physical_ = physical(page);
    return true;
}
auto Object::export_page(uint64_t offset, uint64_t expected, ObjectExportRef& empty) noexcept -> bool {
    auto* page = cpu_page(offset);
    if (empty || page == nullptr || physical(page) != expected || !retain(ObjectRole::owner, true)) return false;
    empty.object_ = this;
    return true;
}
auto Object::begin_revoke(const ker::syscall::vmem::SharedVmemPublicationGuard& pub) noexcept -> bool {
    if (!pub.acquired_by_current_executor()) return false;
    auto const flags = lock_.lock_irqsave();
    if (state_ == ObjectState::live) state_ = ObjectState::revoking;
    bool const pending = state_ == ObjectState::revoking;
    lock_.unlock_irqrestore(flags);
    return pending;
}
auto Object::try_finish_revoke(const ker::syscall::vmem::SharedVmemPublicationGuard& pub) noexcept -> bool {
    if (!pub.acquired_by_current_executor()) return false;
    auto const flags = lock_.lock_irqsave();
    bool const drained = owners_.exports == 0 && owners_.by_role.at(static_cast<size_t>(ObjectRole::binding)) == 0 &&
                         owners_.by_role.at(static_cast<size_t>(ObjectRole::loan)) == 0;
    if (state_ == ObjectState::revoking && drained) state_ = ObjectState::revoked;
    bool const complete = state_ == ObjectState::revoked;
    lock_.unlock_irqrestore(flags);
    return complete;
}
auto Object::observe_return(fm::RetirementCompletion& empty) noexcept -> bool {
    return port_->observe_return(port_->context, *ticket_, finish, this, empty);
}
auto Object::finish(void* argument) noexcept -> int {
    auto* object = static_cast<Object*>(argument);
    if (!phys::can_wait_for_reclaim() || object->owners_.total != 0) __builtin_trap();
    delete object;
    return 0;
}

ObjectCommitGuard::~ObjectCommitGuard() { finish(); }
auto ObjectCommitGuard::try_acquire(const ObjectRef& binding, uint64_t generation,
                                    const ker::syscall::vmem::SharedVmemPublicationGuard& pub) noexcept -> bool {
    if (object_ != nullptr || !binding || binding.role_ != ObjectRole::binding || !pub.acquired_by_current_executor()) return false;
    auto* object = binding.object_;
    auto const flags = object->lock_.lock_irqsave();
    if (object->state_ != ObjectState::live || object->generation_ != generation) {
        object->lock_.unlock_irqrestore(flags);
        return false;
    }
    object_ = object;
    flags_ = flags;
    return true;
}
auto ObjectCommitGuard::commit_loan(PageLoan& loan, const ObjectRef& binding) noexcept -> bool {
    if (object_ == nullptr || !loan || loan.object_ != object_ || binding.object_ != object_ || binding.role_ != ObjectRole::binding ||
        object_->owners_.by_role.at(static_cast<size_t>(ObjectRole::binding)) == 0)
        return false;
    auto& loans = object_->owners_.by_role.at(static_cast<size_t>(ObjectRole::loan));
    if (loans == 0 || object_->owners_.total < 2) __builtin_trap();
    --loans;
    --object_->owners_.total;
    loan.object_ = nullptr;
    loan.page_ = nullptr;
    loan.physical_ = 0;
    return true;
}
void ObjectCommitGuard::finish() noexcept {
    auto* object = std::exchange(object_, nullptr);
    if (object != nullptr) object->lock_.unlock_irqrestore(flags_);
}

}  // namespace ker::dev::mapping
