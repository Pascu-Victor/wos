#include <cerrno>
#include <climits>
#include <new>
#include <platform/mm/file_mapping_lifetime.hpp>
#include <platform/mm/lifetime_retirement_executor.hpp>
#ifdef WOS_SELFTEST
#include <platform/mm/lifecycle_storage.hpp>
#include <platform/mm/virt.hpp>
#endif
#include <platform/mm/paging.hpp>
#include <platform/sys/spinlock.hpp>
#include <platform/sys/usercopy.hpp>
#include <utility>
#include <vfs/file.hpp>
#include <vfs/fs/tmpfs.hpp>
#include <vfs/vfs.hpp>

namespace ker::mod::mm::file_mapping {
namespace {

// Permanent storage: a callback may delete its object after submit_ready, even
// before the last releaser unlocks. No retiring object owns this lock/guard.
constinit sys::Spinlock owner_count_lock{};

class OwnerCountGuard {
   public:
    OwnerCountGuard() : flags_(owner_count_lock.lock_irqsave()) {}
    ~OwnerCountGuard() { owner_count_lock.unlock_irqrestore(flags_); }
    OwnerCountGuard(const OwnerCountGuard&) = delete;
    auto operator=(const OwnerCountGuard&) -> OwnerCountGuard& = delete;

   private:
    uint64_t flags_;
};

template <typename Counts>
auto counts_valid(const Counts& counts) noexcept -> bool {
    uint64_t sum = 0;
    for (uint64_t const VALUE : counts.by_role) {
        if (VALUE > UINT64_MAX - sum) return false;
        sum += VALUE;
    }
    return sum == counts.total;
}

template <typename Counts, typename Role>
auto acquire_owner(Counts& counts, Role role) noexcept -> bool {
    size_t const INDEX = static_cast<size_t>(role);
    if (INDEX >= counts.by_role.size() || counts.total == 0 || counts.total == UINT64_MAX || counts.by_role[INDEX] == UINT64_MAX)
        return false;
    ++counts.total;
    ++counts.by_role[INDEX];
    return true;
}

template <typename Counts, typename Role>
auto release_owner(Counts& counts, Role role) noexcept -> bool {
    size_t const INDEX = static_cast<size_t>(role);
    if (INDEX >= counts.by_role.size() || counts.total == 0 || counts.by_role[INDEX] == 0) __builtin_trap();
    --counts.by_role[INDEX];
    return --counts.total == 0;
}

template <typename Counts, typename Role>
auto reclassify_owner(Counts& counts, Role from, Role to) noexcept -> bool {
    size_t const FROM = static_cast<size_t>(from);
    size_t const TO = static_cast<size_t>(to);
    if (FROM >= counts.by_role.size() || TO >= counts.by_role.size() || counts.total == 0 || counts.by_role[FROM] == 0) return false;
    if (FROM == TO) return true;
    if (counts.by_role[TO] == UINT64_MAX) return false;
    --counts.by_role[FROM];
    ++counts.by_role[TO];
    return true;
}

template <typename Counts>
auto same_owners(const Counts& current, const Counts& expected) noexcept -> bool {
    return current.total != 0 && counts_valid(expected) && current.total == expected.total && current.by_role == expected.by_role;
}

auto port_valid(const LifetimeRetirementPort& port) noexcept -> bool {
    return port.reserve != nullptr && port.submit_ready != nullptr && port.observe_return != nullptr;
}

auto retain_file_checked(ker::vfs::File& file) noexcept -> bool {
    int refs = file.refcount.load(std::memory_order_relaxed);
    while (refs > 0 && refs < INT_MAX) {
        if (file.refcount.compare_exchange_weak(refs, refs + 1, std::memory_order_acq_rel, std::memory_order_relaxed)) return true;
    }
    return false;
}

}  // namespace

FileMappingLifetime::FileMappingLifetime(const LifetimeRetirementPort& port) noexcept : port_(&port) {
    owners_.by_role[static_cast<size_t>(FamilyOwner::PREPARED)] = 1;
    owners_.total = 1;
}

FileMappingLifetime::~FileMappingLifetime() = default;

auto FileMappingLifetime::prepare(ker::vfs::File& borrowed_file) -> FileMappingLifetime* {
    auto const* port = LifetimeRetirementExecutor::port();
    if (port == nullptr || !port_valid(*port)) return nullptr;
#ifdef WOS_SELFTEST
    lifecycle_storage::begin_allocation(lifecycle_storage::Kind::FAMILY);
#endif
    auto* owner = new (std::nothrow) FileMappingLifetime(*port);
#ifdef WOS_SELFTEST
    lifecycle_storage::finish_allocation(lifecycle_storage::Kind::FAMILY, owner != nullptr);
#endif
    if (owner == nullptr) return nullptr;
    if (!retain_file_checked(borrowed_file)) {
        delete owner;
#ifdef WOS_SELFTEST
        lifecycle_storage::finish_deallocation(lifecycle_storage::Kind::FAMILY);
#endif
        return nullptr;
    }
    owner->file_ = &borrowed_file;
    owner->ticket_ = port->reserve(port->context, finish, owner);
    if (owner->ticket_ == nullptr) {
        // Caller still owns borrowed_file. No reservation can invoke this owner.
        owner->file_ = nullptr;
        delete owner;
#ifdef WOS_SELFTEST
        lifecycle_storage::finish_deallocation(lifecycle_storage::Kind::FAMILY);
#endif
        ker::vfs::vfs_put_file(&borrowed_file);
        return nullptr;
    }
    return owner;
}

auto FileMappingLifetime::prepare(ker::vfs::File& borrowed_file, RetirementCompletion& empty) -> FileMappingLifetime* {
    if (!empty.empty()) return nullptr;
    auto const* port = LifetimeRetirementExecutor::port();
    if (port == nullptr || port->cancel_reserved == nullptr) return nullptr;
    auto* owner = prepare(borrowed_file);
    if (owner == nullptr) return nullptr;
    if (owner->observe_return(empty)) return owner;
    auto const* const PORT = owner->port_;
    if (PORT->cancel_reserved == nullptr || !PORT->cancel_reserved(PORT->context, *owner->ticket_, finish, owner)) {
        // Never free the argument or queue an unobserved self-target callback
        // when exact RESERVED cancellation is unproved. Preserve ownership.
        __builtin_trap();
    }
    owner->ticket_ = nullptr;
    owner->file_ = nullptr;
    delete owner;
#ifdef WOS_SELFTEST
    lifecycle_storage::finish_deallocation(lifecycle_storage::Kind::FAMILY);
#endif
    ker::vfs::vfs_put_file(&borrowed_file);  // Caller still owns its borrowed ref.
    return nullptr;
}

auto FileMappingLifetime::prepare_read_shared_tmpfs(ker::vfs::File& borrowed_file, uint64_t file_offset, uint64_t length,
                                                    RetirementCompletion& empty, int& error) -> FileMappingLifetime* {
    if (!empty.empty()) {
        error = -EINVAL;
        return nullptr;
    }
    std::unique_ptr<ker::vfs::tmpfs::TmpfsReadSharedBacking> backing;
    error = ker::vfs::tmpfs::TmpfsReadSharedBacking::prepare(borrowed_file, file_offset, length, backing);
    if (error != 0) return nullptr;
    auto* owner = prepare(borrowed_file, empty);
    if (owner == nullptr) {
        error = -ENOMEM;
        return nullptr;
    }
    // The sole PREPARED owner is still unpublished. No fallible operation or
    // cleanup occurs after observer capture and before adoption.
    owner->read_shared_backing_ = std::move(backing);
    owner->backing_mode_ = FileMappingBackingMode::TMPFS_READ_SHARED;
    error = 0;
    return owner;
}

auto FileMappingLifetime::prepare_demand_read_shared_tmpfs(ker::vfs::File& borrowed_file, uint64_t file_offset, uint64_t length, int& error)
    -> FileMappingLifetime* {
    return prepare_demand_shared_tmpfs(borrowed_file, file_offset, length, false, error);
}

auto FileMappingLifetime::prepare_demand_shared_tmpfs(ker::vfs::File& borrowed_file, uint64_t file_offset, uint64_t length,
                                                      bool write_capable, int& error) -> FileMappingLifetime* {
    std::unique_ptr<ker::vfs::tmpfs::TmpfsDemandSharedBacking> backing;
    error = ker::vfs::tmpfs::TmpfsDemandSharedBacking::prepare(borrowed_file, file_offset, length, write_capable, backing);
    if (error != 0) return nullptr;
    auto* owner = prepare(borrowed_file);
    if (owner == nullptr) {
        error = -ENOMEM;
        return nullptr;
    }
    // The strict backend graph owns no Task. Only the actual permanent
    // executor supplies this unobserved callback and accounts through return.
    owner->demand_read_shared_backing_ = std::move(backing);
    owner->backing_mode_ =
        write_capable ? FileMappingBackingMode::TMPFS_DEMAND_WRITE_SHARED : FileMappingBackingMode::TMPFS_DEMAND_READ_SHARED;
    error = 0;
    return owner;
}

auto FileMappingLifetime::try_acquire(FamilyOwner role) noexcept -> bool {
    OwnerCountGuard const GUARD;
    return acquire_owner(owners_, role);
}

void FileMappingLifetime::release(FamilyOwner role) noexcept {
    OwnerCountGuard const GUARD;
    if (release_owner(owners_, role)) {
        auto const* const PORT = port_;
        auto* const TICKET = ticket_;
        PORT->submit_ready(PORT->context, *TICKET);
        // Nothing belonging to this object is accessed after publication.
    }
}

auto FileMappingLifetime::reclassify(FamilyOwner from, FamilyOwner to) noexcept -> bool {
    OwnerCountGuard const GUARD;
    return reclassify_owner(owners_, from, to);
}

auto FileMappingLifetime::owner_counts() const noexcept -> FamilyOwnerCounts {
    OwnerCountGuard const GUARD;
    return owners_;
}

auto FileMappingLifetime::owners_match(const FamilyOwnerCounts& expected) const noexcept -> bool {
    OwnerCountGuard const GUARD;
    return same_owners(owners_, expected);
}

auto FileMappingLifetime::observe_return(RetirementCompletion& empty) noexcept -> bool {
    // Caller keeps a real active/protected owner through the acquisition. The
    // returned handle supplies only exact-generation storage, not active refs.
    return port_->observe_return(port_->context, *ticket_, finish, this, empty);
}

auto FileMappingLifetime::finish(void* argument) noexcept -> int {
    auto* owner = static_cast<FileMappingLifetime*>(argument);
    ker::vfs::File* file = nullptr;
    {
        OwnerCountGuard const GUARD;
        if (owner->owners_.total != 0 || !counts_valid(owner->owners_)) __builtin_trap();
        file = std::exchange(owner->file_, nullptr);
        if (file == nullptr) __builtin_trap();
    }
    // All MM/owner/queue guards have ended. Close may destroy File even on error;
    // its result is recorded once after return, never retried with a freed arg.
    int const RESULT = ker::vfs::vfs_put_file_result(file);
    // The actual close has returned. Release canonical frames, EOF/node owners
    // and finally mount busy ownership before the executor publishes RETURNED.
    owner->read_shared_backing_.reset();
    owner->demand_read_shared_backing_.reset();
    delete owner;
#ifdef WOS_SELFTEST
    lifecycle_storage::finish_deallocation(lifecycle_storage::Kind::FAMILY);
#endif
    return RESULT;
}

FileMappingBinding::FileMappingBinding(FileMappingLifetime& lifetime) noexcept : lifetime_(&lifetime) {
    owners_.by_role[static_cast<size_t>(BindingOwner::PREPARED)] = 1;
    owners_.total = 1;
}

auto FileMappingBinding::prepare_unbound(FileMappingLifetime& lifetime) -> FileMappingBinding* {
    if (!lifetime.try_acquire(FamilyOwner::BINDING)) return nullptr;
#ifdef WOS_SELFTEST
    lifecycle_storage::begin_allocation(lifecycle_storage::Kind::BINDING);
#endif
    auto* binding = new (std::nothrow) FileMappingBinding(lifetime);
#ifdef WOS_SELFTEST
    lifecycle_storage::finish_allocation(lifecycle_storage::Kind::BINDING, binding != nullptr);
#endif
    if (binding == nullptr) {
        lifetime.release(FamilyOwner::BINDING);
        return nullptr;
    }
    auto const* const PORT = lifetime.port_;
    binding->ticket_ = PORT->reserve(PORT->context, finish, binding);
    if (binding->ticket_ == nullptr) {
        delete binding;
#ifdef WOS_SELFTEST
        lifecycle_storage::finish_deallocation(lifecycle_storage::Kind::BINDING);
#endif
        lifetime.release(FamilyOwner::BINDING);
        return nullptr;
    }
    return binding;
}

auto FileMappingBinding::configure_unpublished(paging::PageTable* root, uint64_t root_incarnation, uint64_t start, uint64_t end,
                                               uint64_t file_offset) noexcept -> bool {
    constexpr uint64_t PAGE_MASK = paging::PAGE_SIZE - 1;
    if (root == nullptr || root_incarnation == 0 || start >= end || end > sys::usercopy::USER_ADDR_LIMIT || (start & PAGE_MASK) != 0 ||
        (end & PAGE_MASK) != 0 || (file_offset & PAGE_MASK) != 0 || file_offset > INT64_MAX ||
        end - start - 1 > static_cast<uint64_t>(INT64_MAX) - file_offset)
        return false;
    OwnerCountGuard const GUARD;
    if (root_ != nullptr || index_state_ != IndexState::PREPARED || owners_.total != 1 || owners_.count(BindingOwner::PREPARED) != 1)
        return false;
    root_ = root;
    root_incarnation_ = root_incarnation;
    start_ = start;
    end_ = end;
    file_offset_ = file_offset;
    return true;
}

auto FileMappingBinding::prepare(FileMappingLifetime& lifetime, paging::PageTable* root, uint64_t root_incarnation, uint64_t start,
                                 uint64_t end, uint64_t file_offset) -> FileMappingBinding* {
    auto* const BINDING = prepare_unbound(lifetime);
    if (BINDING != nullptr && !BINDING->configure_unpublished(root, root_incarnation, start, end, file_offset)) {
        BINDING->release(BindingOwner::PREPARED);
        return nullptr;
    }
    return BINDING;
}

auto FileMappingBinding::try_acquire(BindingOwner role) noexcept -> bool {
    OwnerCountGuard const GUARD;
    return acquire_owner(owners_, role);
}

void FileMappingBinding::release(BindingOwner role) noexcept {
    OwnerCountGuard const GUARD;
    if (release_owner(owners_, role)) {
        auto const* const PORT = lifetime_->port_;
        auto* const TICKET = ticket_;
        PORT->submit_ready(PORT->context, *TICKET);
    }
}

auto FileMappingBinding::reclassify(BindingOwner from, BindingOwner to) noexcept -> bool {
    OwnerCountGuard const GUARD;
    return reclassify_owner(owners_, from, to);
}

auto FileMappingBinding::owner_counts() const noexcept -> BindingOwnerCounts {
    OwnerCountGuard const GUARD;
    return owners_;
}

auto FileMappingBinding::owners_match(const BindingOwnerCounts& expected) const noexcept -> bool {
    OwnerCountGuard const GUARD;
    return same_owners(owners_, expected);
}

auto FileMappingBinding::observe_return(RetirementCompletion& empty) noexcept -> bool {
    auto const* const PORT = lifetime_->port_;
    return PORT->observe_return(PORT->context, *ticket_, finish, this, empty);
}

auto FileMappingBinding::finish(void* argument) noexcept -> int {
    auto* binding = static_cast<FileMappingBinding*>(argument);
    FileMappingLifetime* lifetime = nullptr;
    {
        OwnerCountGuard const GUARD;
        if (binding->owners_.total != 0 || !counts_valid(binding->owners_) || binding->index_state_ == IndexState::INDEXED ||
            binding->index_next_ != nullptr || binding->retire_next_ != nullptr)
            __builtin_trap();
        lifetime = std::exchange(binding->lifetime_, nullptr);
        if (lifetime == nullptr) __builtin_trap();
    }
    delete binding;
#ifdef WOS_SELFTEST
    lifecycle_storage::finish_deallocation(lifecycle_storage::Kind::BINDING);
#endif
    lifetime->release(FamilyOwner::BINDING);  // Separate pre-reserved callback.
    return 0;
}

FileMappingTlbRetirement::~FileMappingTlbRetirement() {
    if (head_ != nullptr || tail_ != nullptr) __builtin_trap();
}

void FileMappingTlbRetirement::append(FileMappingBinding& binding) noexcept {
    if (binding.index_state_ != FileMappingBinding::IndexState::DETACHED || binding.retire_next_ != nullptr) __builtin_trap();
    if (tail_ != nullptr)
        tail_->retire_next_ = &binding;
    else
        head_ = &binding;
    tail_ = &binding;
}

void FileMappingTlbRetirement::release_after_completed_tlb() noexcept {
    while (head_ != nullptr) {
        auto* const CURRENT = head_;
        head_ = CURRENT->retire_next_;
        CURRENT->retire_next_ = nullptr;
        if (head_ == nullptr) tail_ = nullptr;
        CURRENT->release(BindingOwner::TLB);
#ifdef WOS_SELFTEST
        virt::selftest_current_root_detached_binding_returned();
#endif
        // Callback may have deleted CURRENT; advance uses only saved batch state.
    }
}

auto FileMappingIndex::bucket(paging::PageTable* root) noexcept -> size_t {
    return static_cast<size_t>((reinterpret_cast<uintptr_t>(root) >> paging::PAGE_SHIFT) & (BUCKET_COUNT - 1));
}

auto FileMappingIndex::root_matches_locked(paging::PageTable* root, uint64_t root_incarnation) const noexcept -> bool {
    if (root == nullptr) return false;
    for (auto* current = heads_[bucket(root)]; current != nullptr; current = current->index_next_) {
        if (current->root_ == root && (root_incarnation == 0 || current->root_incarnation_ != root_incarnation)) return false;
    }
    return true;
}

auto FileMappingIndex::find_locked(paging::PageTable* root, uint64_t address) const noexcept -> FileMappingBinding* {
    for (auto* current = heads_[bucket(root)]; current != nullptr; current = current->index_next_) {
        if (current->root_ == root && address >= current->start_ && address < current->end_) return current;
    }
    return nullptr;
}

auto FileMappingIndex::attach_locked(FileMappingBinding*& prepared, uint64_t root_incarnation) noexcept -> bool {
    if (prepared == nullptr || prepared->root_ == nullptr || root_incarnation == 0 || prepared->root_incarnation_ != root_incarnation ||
        prepared->index_next_ != nullptr || prepared->retire_next_ != nullptr ||
        prepared->index_state_ != FileMappingBinding::IndexState::PREPARED)
        return false;
    size_t const SLOT = bucket(prepared->root_);
    for (auto* current = heads_[SLOT]; current != nullptr; current = current->index_next_) {
        if (current->root_ == prepared->root_ &&
            (current->root_incarnation_ != root_incarnation || (prepared->start_ < current->end_ && current->start_ < prepared->end_)))
            return false;
    }
#ifdef WOS_SELFTEST
    bool const semantic_owned = virt::selftest_current_root_change_begin();
    if (!prepared->reclassify(BindingOwner::PREPARED, BindingOwner::INDEX)) {
        virt::selftest_current_root_change_end(semantic_owned);
        return false;
    }
#else
    if (!prepared->reclassify(BindingOwner::PREPARED, BindingOwner::INDEX)) return false;
#endif
    auto* const BINDING = std::exchange(prepared, nullptr);
    BINDING->index_next_ = heads_[SLOT];
    BINDING->index_state_ = FileMappingBinding::IndexState::INDEXED;
    heads_[SLOT] = BINDING;
#ifdef WOS_SELFTEST
    virt::selftest_current_root_change_end(semantic_owned);
#endif
    return true;
}

auto FileMappingIndex::detach_locked(FileMappingBinding& binding, FileMappingTlbRetirement& retirement) noexcept -> bool {
    if (binding.root_ == nullptr || binding.index_state_ != FileMappingBinding::IndexState::INDEXED || binding.retire_next_ != nullptr)
        return false;
    auto** link = &heads_[bucket(binding.root_)];
    while (*link != nullptr && *link != &binding) link = &(*link)->index_next_;
#ifdef WOS_SELFTEST
    if (*link == nullptr) return false;
    bool const semantic_owned = virt::selftest_current_root_change_begin();
    if (!binding.reclassify(BindingOwner::INDEX, BindingOwner::TLB)) {
        virt::selftest_current_root_change_end(semantic_owned);
        return false;
    }
#else
    if (*link == nullptr || !binding.reclassify(BindingOwner::INDEX, BindingOwner::TLB)) return false;
#endif
    // Success transfers the current epoch obligation with this DETACHED row.
    // The exclusive retirement batch consumes it after actual TLB-ref return.
    *link = binding.index_next_;
    binding.index_next_ = nullptr;
    binding.index_state_ = FileMappingBinding::IndexState::DETACHED;
    retirement.append(binding);
    return true;
}

auto FileMappingIndex::detach_root_locked(paging::PageTable* root, uint64_t root_incarnation, FileMappingTlbRetirement& retirement) noexcept
    -> bool {
    if (!root_matches_locked(root, root_incarnation)) return false;
    auto** link = &heads_[bucket(root)];
    while (*link != nullptr) {
        auto* const CURRENT = *link;
        if (CURRENT->root_ != root) {
            link = &CURRENT->index_next_;
            continue;
        }
#ifdef WOS_SELFTEST
        static_cast<void>(virt::selftest_current_root_change_begin());
#endif
        if (!CURRENT->reclassify(BindingOwner::INDEX, BindingOwner::TLB)) __builtin_trap();
        *link = CURRENT->index_next_;
        CURRENT->index_next_ = nullptr;
        CURRENT->index_state_ = FileMappingBinding::IndexState::DETACHED;
        retirement.append(*CURRENT);
    }
    return true;
}

auto FileMappingIndex::root_empty_locked(paging::PageTable* root) const noexcept -> bool {
    for (auto* current = heads_[bucket(root)]; current != nullptr; current = current->index_next_) {
        if (current->root_ == root) return false;
    }
    return true;
}

auto FileMappingIndex::retain_for_pin_locked(paging::PageTable* root, uint64_t root_incarnation, uint64_t address,
                                             FileMappingBinding*& owned) noexcept -> FileMappingPinResult {
    if (owned != nullptr || !root_matches_locked(root, root_incarnation)) return FileMappingPinResult::REFUSE;
    auto* const CURRENT = find_locked(root, address);
    if (CURRENT == nullptr) return FileMappingPinResult::NOT_FILE_MAPPING;
    if (!CURRENT->try_acquire(BindingOwner::PIN)) return FileMappingPinResult::REFUSE;
    owned = CURRENT;
    return FileMappingPinResult::ACQUIRED;
}

auto FileMappingIndex::matches_pin_locked(paging::PageTable* root, uint64_t root_incarnation, uint64_t address,
                                          const FileMappingBinding* binding) const noexcept -> bool {
    return root_matches_locked(root, root_incarnation) && find_locked(root, address) == binding;
}

auto FileMappingIndex::count_root_locked(paging::PageTable* root, uint64_t root_incarnation, size_t& count) const noexcept -> bool {
    if (!root_matches_locked(root, root_incarnation)) return false;
    size_t found = 0;
    for (auto* current = heads_[bucket(root)]; current != nullptr; current = current->index_next_) {
        if (current->root_ != root) continue;
        if (found == SIZE_MAX) return false;
        ++found;
    }
    count = found;
    return true;
}

auto FileMappingIndex::overlap_locked(paging::PageTable* root, uint64_t root_incarnation, uint64_t start, uint64_t end,
                                      FileMappingBinding*& owned, bool& exact) noexcept -> bool {
    constexpr uint64_t PAGE_MASK = paging::PAGE_SIZE - 1;
    if (owned != nullptr || root_incarnation == 0 || start >= end || end > sys::usercopy::USER_ADDR_LIMIT ||
        ((start | end) & PAGE_MASK) != 0 || !root_matches_locked(root, root_incarnation))
        return false;
    FileMappingBinding* first = nullptr;
    bool one_exact = true;
    for (auto* current = heads_[bucket(root)]; current != nullptr; current = current->index_next_) {
        if (current->root_ != root || current->start_ >= end || start >= current->end_) continue;
        if (first == nullptr) {
            first = current;
            one_exact = current->start_ == start && current->end_ == end;
        } else {
            one_exact = false;
        }
    }
    // Finish validation before acquiring the sole result owner. Failure never
    // needs a terminal release under the caller's actual pin lock.
    if (first != nullptr && !first->try_acquire(BindingOwner::SNAPSHOT)) return false;
    owned = first;
    exact = one_exact;
    return true;
}

auto FileMappingIndex::snapshot_root_locked(paging::PageTable* root, uint64_t root_incarnation, FileMappingBinding** output,
                                            size_t capacity, size_t& owned_count) noexcept -> bool {
    if (owned_count != 0) __builtin_trap();
    if ((output == nullptr && capacity != 0) || !root_matches_locked(root, root_incarnation)) return false;
    for (auto* current = heads_[bucket(root)]; current != nullptr; current = current->index_next_) {
        if (current->root_ != root) continue;
        if (owned_count == capacity || !current->try_acquire(BindingOwner::SNAPSHOT)) return false;
        output[owned_count++] = current;
    }
    return true;
}

#ifdef WOS_SELFTEST
auto FileMappingIndex::selftest_count_all_locked(uint64_t& empty_count) const noexcept -> bool {
    if (empty_count != 0) return false;
    uint64_t count = 0;
    for (auto* head : heads_) {
        for (auto* binding = head; binding != nullptr; binding = binding->index_next_) {
            if (count == 65536 || binding->index_state_ != FileMappingBinding::IndexState::INDEXED) return false;
            ++count;
        }
    }
    empty_count = count;
    return true;
}
#endif

}  // namespace ker::mod::mm::file_mapping
