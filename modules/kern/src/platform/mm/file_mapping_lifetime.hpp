#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>

namespace ker::vfs {
struct File;
}
namespace ker::vfs::tmpfs {
class TmpfsReadSharedBacking;
class TmpfsDemandSharedBacking;
}  // namespace ker::vfs::tmpfs
namespace ker::mod::mm::paging {
struct PageTable;
}

namespace ker::mod::mm::file_mapping {

struct LifetimeRetirementTicket;
using LifetimeRetirementRun = auto (*)(void*) noexcept -> int;
class LifetimeRetirementExecutor;

enum class RetirementReturnState : uint8_t { EMPTY, PENDING, RETURNED, INVALID };
struct RetirementReturn {
    RetirementReturnState state{RetirementReturnState::EMPTY};
    int callback_result{};  // RETURNED includes a terminal destructive-close error.
};

// Exact-generation observation storage only: no active family, File or Task ref.
// Capture while owned; drop all active/snapshot refs before waiting. A single
// handle's move/query/reset is externally serialized. Separate handles may race.
class RetirementCompletion {
   public:
    RetirementCompletion() noexcept = default;
    RetirementCompletion(RetirementCompletion&& other) noexcept;
    auto operator=(RetirementCompletion&& other) noexcept -> RetirementCompletion&;
    RetirementCompletion(const RetirementCompletion&) = delete;
    auto operator=(const RetirementCompletion&) -> RetirementCompletion& = delete;
    ~RetirementCompletion();
    [[nodiscard]] auto empty() const noexcept -> bool { return executor_ == nullptr; }
    [[nodiscard]] auto generation() const noexcept -> uint64_t { return generation_; }
    [[nodiscard]] auto query() const noexcept -> RetirementReturn;
    void reset() noexcept;

   private:
    friend class LifetimeRetirementExecutor;
    LifetimeRetirementExecutor* executor_{};
    LifetimeRetirementTicket* ticket_{};
    uint64_t generation_{};
};

// The permanent real executor supplies this port. A reserved callback cannot run
// before submit_ready. Submission is IRQ-safe, allocation-free, nonrejecting after
// admission closes, and never wakes or synchronously executes backend work.
// The executor drops its queue lock before callback invocation, retains accounting
// through actual callback return, and never retries a destructive-close error.
struct LifetimeRetirementPort {
    void* context{};
    auto (*reserve)(void*, LifetimeRetirementRun, void*) noexcept -> LifetimeRetirementTicket* {};
    void (*submit_ready)(void*, LifetimeRetirementTicket&) noexcept {};
    auto (*observe_return)(void*, LifetimeRetirementTicket&, LifetimeRetirementRun, void*, RetirementCompletion&) noexcept -> bool{};
    // Optional cancellation of the exact RESERVED run/argument with no observers.
    // Failure preserves ownership; success is not callback-return evidence.
    auto (*cancel_reserved)(void*, LifetimeRetirementTicket&, LifetimeRetirementRun, void*) noexcept -> bool{};
};

enum class FamilyOwner : uint8_t { PREPARED, LAZY, REGISTRY, SYNC, BINDING, SNAPSHOT, COUNT };
enum class BindingOwner : uint8_t { PREPARED, INDEX, PIN, INSTALLER, SNAPSHOT, TLB, COUNT };

struct FamilyOwnerCounts {
    std::array<uint64_t, static_cast<size_t>(FamilyOwner::COUNT)> by_role{};
    uint64_t total{};
    [[nodiscard]] auto count(FamilyOwner role) const noexcept -> uint64_t {
        size_t const INDEX = static_cast<size_t>(role);
        return INDEX < by_role.size() ? by_role[INDEX] : 0;
    }
};

struct BindingOwnerCounts {
    std::array<uint64_t, static_cast<size_t>(BindingOwner::COUNT)> by_role{};
    uint64_t total{};
    [[nodiscard]] auto count(BindingOwner role) const noexcept -> uint64_t {
        size_t const INDEX = static_cast<size_t>(role);
        return INDEX < by_role.size() ? by_role[INDEX] : 0;
    }
};

class FileMappingBinding;
enum class FileMappingBackingMode : uint8_t { PRIVATE_COPY, TMPFS_READ_SHARED, TMPFS_DEMAND_READ_SHARED, TMPFS_DEMAND_WRITE_SHARED };

// One family owns exactly one File reference, independent of its typed consumers.
// The explicit readonly TMPFS mode also owns its fixed-EOF canonical backing.
class FileMappingLifetime {
   public:
    // Ordinary allocation context, outside MM/root/publication/IRQ guards. Caller
    // owns borrowed_file. Uses only the actual permanent executor; no fake port.
    // Success returns one PREPARED owner. Failure retains no additional File ref.
    // Unobserved callers need an independent lifetime boundary; Task self-target
    // staging uses the transactional overload below.
    [[nodiscard]] static auto prepare(ker::vfs::File& borrowed_file) -> FileMappingLifetime*;
    // Transactional self-target staging: success also captures the exact return
    // observer. Refusal cancels the unused reservation before synchronous rollback;
    // it never queues an unobserved File callback. The output starts empty.
    [[nodiscard]] static auto prepare(ker::vfs::File& borrowed_file, RetirementCompletion& empty) -> FileMappingLifetime*;
    // Backend preparation may allocate/block before any MM/root guard or return
    // observer exists. Failure leaves no queued family and preserves empty.
    [[nodiscard]] static auto prepare_read_shared_tmpfs(ker::vfs::File& borrowed_file, uint64_t file_offset, uint64_t length,
                                                        RetirementCompletion& empty, int& error) -> FileMappingLifetime*;
    // Strict mounted TMPFS graph has no Task backedge. Ordinary demand backing
    // preparation uses unobserved permanent tickets; callback return remains
    // accounted by the real executor even after the publishing Task is gone.
    [[nodiscard]] static auto prepare_demand_read_shared_tmpfs(ker::vfs::File& borrowed_file, uint64_t file_offset, uint64_t length,
                                                               int& error) -> FileMappingLifetime*;
    [[nodiscard]] static auto prepare_demand_shared_tmpfs(ker::vfs::File& borrowed_file, uint64_t file_offset, uint64_t length,
                                                          bool write_capable, int& error) -> FileMappingLifetime*;
    // Caller already owns/protects a live owner; a bare pointer cannot resurrect.
    [[nodiscard]] auto try_acquire(FamilyOwner role) noexcept -> bool;
    void release(FamilyOwner role) noexcept;
    [[nodiscard]] auto reclassify(FamilyOwner from, FamilyOwner to) noexcept -> bool;
    [[nodiscard]] auto owner_counts() const noexcept -> FamilyOwnerCounts;
    // Exact diagnostic counts, not an authorization to delete a Task graph.
    // Integration must enumerate all holders under its actual graph guards.
    [[nodiscard]] auto owners_match(const FamilyOwnerCounts& expected) const noexcept -> bool;
    [[nodiscard]] auto observe_return(RetirementCompletion& empty) noexcept -> bool;
    [[nodiscard]] auto file() const noexcept -> ker::vfs::File* { return file_; }
    [[nodiscard]] auto backing_mode() const noexcept -> FileMappingBackingMode { return backing_mode_; }
    [[nodiscard]] auto is_demand_shared() const noexcept -> bool {
        return backing_mode_ == FileMappingBackingMode::TMPFS_DEMAND_READ_SHARED ||
               backing_mode_ == FileMappingBackingMode::TMPFS_DEMAND_WRITE_SHARED;
    }
    [[nodiscard]] auto shared_writable() const noexcept -> bool {
        return backing_mode_ == FileMappingBackingMode::TMPFS_DEMAND_WRITE_SHARED;
    }
    // Borrowed only while a real family/binding/installer owner is held.
    [[nodiscard]] auto read_shared_backing() const noexcept -> const ker::vfs::tmpfs::TmpfsReadSharedBacking* {
        return read_shared_backing_.get();
    }
    [[nodiscard]] auto demand_read_shared_backing() const noexcept -> const ker::vfs::tmpfs::TmpfsDemandSharedBacking* {
        return demand_read_shared_backing_.get();
    }

    FileMappingLifetime(const FileMappingLifetime&) = delete;
    auto operator=(const FileMappingLifetime&) -> FileMappingLifetime& = delete;

   private:
    friend class FileMappingBinding;
    explicit FileMappingLifetime(const LifetimeRetirementPort& port) noexcept;
    ~FileMappingLifetime();
    static auto finish(void* argument) noexcept -> int;
    FamilyOwnerCounts owners_{};  // Permanent owner-count IRQ lock, never a member lock.
    const LifetimeRetirementPort* port_{};
    LifetimeRetirementTicket* ticket_{};
    ker::vfs::File* file_{};
    FileMappingBackingMode backing_mode_{FileMappingBackingMode::PRIVATE_COPY};
    std::unique_ptr<ker::vfs::tmpfs::TmpfsReadSharedBacking> read_shared_backing_{};
    std::unique_ptr<ker::vfs::tmpfs::TmpfsDemandSharedBacking> demand_read_shared_backing_{};
};

class FileMappingIndex;
class FileMappingTlbRetirement;

// A stable immutable interval incarnation. A retained binding prevents pointer
// ABA even when another mapping uses the same root, VA and physical frame.
// Root fields are identity only; this object never owns/dereferences a root.
class FileMappingBinding {
   public:
    [[nodiscard]] static auto prepare_unbound(FileMappingLifetime& lifetime) -> FileMappingBinding*;
    // One-time initialization, before publication. Root serial must be obtained
    // from the actual registered root. No allocation or mutation on refusal.
    [[nodiscard]] auto configure_unpublished(paging::PageTable* root, uint64_t root_incarnation, uint64_t start, uint64_t end,
                                             uint64_t file_offset) noexcept -> bool;
    [[nodiscard]] static auto prepare(FileMappingLifetime& lifetime, paging::PageTable* root, uint64_t root_incarnation, uint64_t start,
                                      uint64_t end, uint64_t file_offset) -> FileMappingBinding*;
    [[nodiscard]] auto try_acquire(BindingOwner role) noexcept -> bool;
    void release(BindingOwner role) noexcept;
    [[nodiscard]] auto reclassify(BindingOwner from, BindingOwner to) noexcept -> bool;
    [[nodiscard]] auto owner_counts() const noexcept -> BindingOwnerCounts;
    [[nodiscard]] auto owners_match(const BindingOwnerCounts& expected) const noexcept -> bool;
    [[nodiscard]] auto observe_return(RetirementCompletion& empty) noexcept -> bool;
    [[nodiscard]] auto file() const noexcept -> ker::vfs::File* { return lifetime_->file(); }
    [[nodiscard]] auto lifetime() const noexcept -> FileMappingLifetime* { return lifetime_; }
    [[nodiscard]] auto root() const noexcept -> paging::PageTable* { return root_; }
    [[nodiscard]] auto root_incarnation() const noexcept -> uint64_t { return root_incarnation_; }
    [[nodiscard]] auto start() const noexcept -> uint64_t { return start_; }
    [[nodiscard]] auto end() const noexcept -> uint64_t { return end_; }
    [[nodiscard]] auto file_offset() const noexcept -> uint64_t { return file_offset_; }

    FileMappingBinding(const FileMappingBinding&) = delete;
    auto operator=(const FileMappingBinding&) -> FileMappingBinding& = delete;

   private:
    friend class FileMappingIndex;
    friend class FileMappingTlbRetirement;
    enum class IndexState : uint8_t { PREPARED, INDEXED, DETACHED };
    explicit FileMappingBinding(FileMappingLifetime& lifetime) noexcept;
    ~FileMappingBinding() = default;
    static auto finish(void* argument) noexcept -> int;
    BindingOwnerCounts owners_{};
    FileMappingLifetime* lifetime_{};  // One family BINDING owner through callback return.
    LifetimeRetirementTicket* ticket_{};
    paging::PageTable* root_{};
    uint64_t root_incarnation_{};
    uint64_t start_{};
    uint64_t end_{};
    uint64_t file_offset_{};
    FileMappingBinding* index_next_{};   // Actual user_mapping_pin_lock only.
    FileMappingBinding* retire_next_{};  // Exclusive retirement batch after detach.
    IndexState index_state_{IndexState::PREPARED};
};

// Owns exact INDEX refs transferred as TLB refs, without a new acquisition.
// No automatic flush/release: caller must finish the real TLB/root ACK first.
class FileMappingTlbRetirement {
   public:
    FileMappingTlbRetirement() = default;
    ~FileMappingTlbRetirement();  // A nonempty abandoned batch is a caller bug.
    FileMappingTlbRetirement(const FileMappingTlbRetirement&) = delete;
    auto operator=(const FileMappingTlbRetirement&) -> FileMappingTlbRetirement& = delete;
    [[nodiscard]] auto empty() const noexcept -> bool { return head_ == nullptr; }
    void release_after_completed_tlb() noexcept;

   private:
    friend class FileMappingIndex;
    void append(FileMappingBinding& binding) noexcept;
    FileMappingBinding* head_{};
    FileMappingBinding* tail_{};
};

enum class FileMappingPinResult : uint8_t { NOT_FILE_MAPPING, ACQUIRED, REFUSE };

// One instance beside the real user_mapping_pin_lock. All *_locked operations
// require that existing lock. No allocation, File put, wait or backend runs here.
// Coverage persists across absent/PROT_NONE PTEs until actual mapping retirement.
class FileMappingIndex {
   public:
    FileMappingIndex() = default;
    FileMappingIndex(const FileMappingIndex&) = delete;
    auto operator=(const FileMappingIndex&) -> FileMappingIndex& = delete;
    [[nodiscard]] auto attach_locked(FileMappingBinding*& prepared, uint64_t root_incarnation) noexcept -> bool;
    [[nodiscard]] auto detach_locked(FileMappingBinding& binding, FileMappingTlbRetirement& retirement) noexcept -> bool;
    [[nodiscard]] auto detach_root_locked(paging::PageTable* root, uint64_t root_incarnation, FileMappingTlbRetirement& retirement) noexcept
        -> bool;
    [[nodiscard]] auto root_empty_locked(paging::PageTable* root) const noexcept -> bool;
    [[nodiscard]] auto retain_for_pin_locked(paging::PageTable* root, uint64_t root_incarnation, uint64_t address,
                                             FileMappingBinding*& owned) noexcept -> FileMappingPinResult;
    // Also require original PTE permissions/frame equality under the same lock.
    // Null matches only no association at this address and no stale root serial.
    [[nodiscard]] auto matches_pin_locked(paging::PageTable* root, uint64_t root_incarnation, uint64_t address,
                                          const FileMappingBinding* binding) const noexcept -> bool;
    [[nodiscard]] auto count_root_locked(paging::PageTable* root, uint64_t root_incarnation, size_t& count) const noexcept -> bool;
#ifdef WOS_SELFTEST
    // Same real user_mapping_pin_lock as attach/detach; refusal preserves out.
    [[nodiscard]] auto selftest_count_all_locked(uint64_t& empty_count) const noexcept -> bool;
#endif
    // Empty owned required. Retain the first overlapping binding as SNAPSHOT;
    // exact means one equal interval, or no overlap (owned remains null).
    // Refusal changes neither output and acquires no owner. Drop success's
    // SNAPSHOT outside the actual pin lock, including when exact is false.
    [[nodiscard]] auto overlap_locked(paging::PageTable* root, uint64_t root_incarnation, uint64_t start, uint64_t end,
                                      FileMappingBinding*& owned, bool& exact) noexcept -> bool;
    // Preallocate outside lock. Refusal preserves the actual acquired prefix;
    // caller releases its SNAPSHOT owners after unlocking, including on failure.
    [[nodiscard]] auto snapshot_root_locked(paging::PageTable* root, uint64_t root_incarnation, FileMappingBinding** output,
                                            size_t capacity, size_t& owned_count) noexcept -> bool;

   private:
    static constexpr size_t BUCKET_COUNT = 256;
    [[nodiscard]] static auto bucket(paging::PageTable* root) noexcept -> size_t;
    [[nodiscard]] auto root_matches_locked(paging::PageTable* root, uint64_t root_incarnation) const noexcept -> bool;
    [[nodiscard]] auto find_locked(paging::PageTable* root, uint64_t address) const noexcept -> FileMappingBinding*;
    std::array<FileMappingBinding*, BUCKET_COUNT> heads_{};
};

}  // namespace ker::mod::mm::file_mapping
