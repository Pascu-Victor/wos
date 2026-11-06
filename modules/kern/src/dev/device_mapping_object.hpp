#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <platform/sys/spinlock.hpp>

namespace ker::mod::mm::file_mapping {
struct LifetimeRetirementPort;
struct LifetimeRetirementTicket;
class RetirementCompletion;
}  // namespace ker::mod::mm::file_mapping

namespace ker::syscall::vmem {
class SharedVmemPublicationGuard;
}

namespace ker::dev::mapping {

namespace detail {
class ObjectGenerations {
   public:
    explicit constexpr ObjectGenerations(uint64_t last = 0) : last_(last) {}
    [[nodiscard]] auto issue() noexcept -> uint64_t;

   private:
    std::atomic<uint64_t> last_;
};
}  // namespace detail

inline constexpr uint64_t OBJECT_PAGE_SIZE = 4096;
struct QuotaLimits {
    uint64_t resident_pages{};
    uint64_t objects{};
    uint64_t max_object_pages = 262144;
};
struct QuotaUsage {
    uint64_t resident_pages{};
    uint64_t objects{};
};
class QuotaAccount;
class Object;
class ObjectCommitGuard;

class QuotaAccountRef {
   public:
    QuotaAccountRef() = default;
    ~QuotaAccountRef();
    QuotaAccountRef(const QuotaAccountRef&) = delete;
    auto operator=(const QuotaAccountRef&) -> QuotaAccountRef& = delete;
    QuotaAccountRef(QuotaAccountRef&&) noexcept;
    auto operator=(QuotaAccountRef&&) noexcept -> QuotaAccountRef&;
    [[nodiscard]] auto clone_into(QuotaAccountRef& empty) const noexcept -> bool;
    void reset() noexcept;
    [[nodiscard]] auto get() const noexcept -> QuotaAccount* { return account_; }
    explicit operator bool() const noexcept { return account_ != nullptr; }

   private:
    friend class QuotaAccount;
    explicit QuotaAccountRef(QuotaAccount* account) : account_(account) {}
    QuotaAccount* account_{};
};

class QuotaAccount {
   public:
    [[nodiscard]] static auto create(QuotaLimits, QuotaAccountRef& empty) -> int;
    [[nodiscard]] auto limits() const noexcept -> QuotaLimits { return limits_; }
    [[nodiscard]] auto usage() const noexcept -> QuotaUsage;
    [[nodiscard]] auto observe_return(ker::mod::mm::file_mapping::RetirementCompletion& empty) noexcept -> bool;

   private:
    friend class QuotaAccountRef;
    friend class Object;
    explicit QuotaAccount(QuotaLimits limits) : limits_(limits) {}
    ~QuotaAccount() = default;
    [[nodiscard]] auto charge(uint64_t pages) noexcept -> bool;
    void uncharge(uint64_t pages) noexcept;
    [[nodiscard]] auto retain() noexcept -> bool;
    void release() noexcept;
    static auto finish(void*) noexcept -> int;
    mutable ker::mod::sys::Spinlock lock_;
    QuotaLimits limits_;
    QuotaUsage usage_{};
    uint64_t references_ = 1;
    const ker::mod::mm::file_mapping::LifetimeRetirementPort* port_{};
    ker::mod::mm::file_mapping::LifetimeRetirementTicket* ticket_{};
};

enum class ObjectRole : uint8_t { owner, grant, binding, loan, count };
enum class ObjectState : uint8_t { live, revoking, revoked };
struct ObjectOwners {
    std::array<uint64_t, static_cast<size_t>(ObjectRole::count)> by_role{};
    uint64_t exports{};
    uint64_t total{};
};

class ObjectRef {
   public:
    ObjectRef() = default;
    ~ObjectRef();
    ObjectRef(const ObjectRef&) = delete;
    auto operator=(const ObjectRef&) -> ObjectRef& = delete;
    ObjectRef(ObjectRef&&) noexcept;
    auto operator=(ObjectRef&&) noexcept -> ObjectRef&;
    [[nodiscard]] auto clone_into(ObjectRef& empty, ObjectRole role = ObjectRole::owner) const noexcept -> bool;
    void reset() noexcept;
    [[nodiscard]] auto get() const noexcept -> Object* { return object_; }
    [[nodiscard]] auto role() const noexcept -> ObjectRole { return role_; }
    explicit operator bool() const noexcept { return object_ != nullptr; }

   private:
    friend class Object;
    friend class ObjectCommitGuard;
    ObjectRef(Object* object, ObjectRole role) : object_(object), role_(role) {}
    Object* object_{};
    ObjectRole role_{ObjectRole::owner};
};

class ObjectExportRef {
   public:
    ObjectExportRef() = default;
    ~ObjectExportRef();
    ObjectExportRef(const ObjectExportRef&) = delete;
    auto operator=(const ObjectExportRef&) -> ObjectExportRef& = delete;
    ObjectExportRef(ObjectExportRef&&) noexcept;
    auto operator=(ObjectExportRef&&) noexcept -> ObjectExportRef&;
    [[nodiscard]] auto clone_into(ObjectExportRef& empty) const noexcept -> bool;
    void reset() noexcept;
    explicit operator bool() const noexcept { return object_ != nullptr; }

   private:
    friend class Object;
    Object* object_{};
};

class PageLoan {
   public:
    PageLoan() = default;
    ~PageLoan();
    PageLoan(const PageLoan&) = delete;
    auto operator=(const PageLoan&) -> PageLoan& = delete;
    PageLoan(PageLoan&&) noexcept;
    auto operator=(PageLoan&&) noexcept -> PageLoan&;
    [[nodiscard]] auto physical() const noexcept -> uint64_t { return physical_; }
    void reset() noexcept;
    explicit operator bool() const noexcept { return object_ != nullptr; }

   private:
    friend class Object;
    friend class ObjectCommitGuard;
    Object* object_{};
    void* page_{};
    uint64_t physical_{};
};

class Object {
   public:
    [[nodiscard]] static auto create(const QuotaAccountRef&, uint64_t bytes, uint64_t max_protections, ObjectRef& empty) -> int;
    [[nodiscard]] auto bytes() const noexcept -> uint64_t { return bytes_; }
    [[nodiscard]] auto pages() const noexcept -> size_t { return page_count_; }
    [[nodiscard]] auto generation() const noexcept -> uint64_t { return generation_; }
    [[nodiscard]] auto max_protections() const noexcept -> uint64_t { return max_protections_; }
    [[nodiscard]] auto owners() const noexcept -> ObjectOwners;
    [[nodiscard]] auto state() const noexcept -> ObjectState;
    [[nodiscard]] auto frame_at(uint64_t byte_offset, uint64_t& physical) const noexcept -> bool;
    [[nodiscard]] auto cpu_page(uint64_t byte_offset) const noexcept -> void*;
    [[nodiscard]] auto prepare_page(uint64_t byte_offset, PageLoan& empty) noexcept -> bool;
    [[nodiscard]] auto export_page(uint64_t byte_offset, uint64_t expected_physical, ObjectExportRef& empty) noexcept -> bool;
    [[nodiscard]] auto begin_revoke(const ker::syscall::vmem::SharedVmemPublicationGuard&) noexcept -> bool;
    [[nodiscard]] auto try_finish_revoke(const ker::syscall::vmem::SharedVmemPublicationGuard&) noexcept -> bool;
    [[nodiscard]] auto observe_return(ker::mod::mm::file_mapping::RetirementCompletion& empty) noexcept -> bool;

   private:
    friend class ObjectRef;
    friend class ObjectExportRef;
    friend class PageLoan;
    friend class ObjectCommitGuard;
    Object() = default;
    ~Object();
    [[nodiscard]] auto retain(ObjectRole, bool exporting = false) noexcept -> bool;
    void release(ObjectRole, bool exporting = false) noexcept;
    static auto finish(void*) noexcept -> int;
    mutable ker::mod::sys::Spinlock lock_;
    ObjectOwners owners_{};
    ObjectState state_{ObjectState::live};
    QuotaAccountRef account_;
    void** pages_{};
    size_t page_count_{};
    uint64_t bytes_{}, generation_{}, max_protections_{};
    const ker::mod::mm::file_mapping::LifetimeRetirementPort* port_{};
    ker::mod::mm::file_mapping::LifetimeRetirementTicket* ticket_{};
};

// Borrow an actual prepared BINDING owner under current-executor PUB. Caller
// already owns the actual MM pin/root protocol; lock order is PUB->root->COW->
// MM pin->Object lifecycle. No allocation/PMM/usercopy/callback/put under guard.
class ObjectCommitGuard {
   public:
    ObjectCommitGuard() = default;
    ~ObjectCommitGuard();
    ObjectCommitGuard(const ObjectCommitGuard&) = delete;
    auto operator=(const ObjectCommitGuard&) -> ObjectCommitGuard& = delete;
    ObjectCommitGuard(ObjectCommitGuard&&) = delete;
    auto operator=(ObjectCommitGuard&&) -> ObjectCommitGuard& = delete;
    [[nodiscard]] auto try_acquire(const ObjectRef& binding_owner, uint64_t generation,
                                   const ker::syscall::vmem::SharedVmemPublicationGuard&) noexcept -> bool;
    [[nodiscard]] auto commit_loan(PageLoan&, const ObjectRef& binding_owner) noexcept -> bool;
    void finish() noexcept;
    [[nodiscard]] auto acquired() const noexcept -> bool { return object_ != nullptr; }

   private:
    Object* object_{};
    uint64_t flags_{};
};

}  // namespace ker::dev::mapping
