#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <net/wki/rdma_registry.hpp>
#include <span>

namespace ker::net::wki::rdma_operations {

constexpr size_t OPERATION_SLOTS = rdma_protocol::MAX_OPERATIONS;
constexpr size_t SCRATCH_BYTES = OPERATION_SLOTS * rdma_protocol::MAX_TRANSFER_BYTES;

enum class Role : uint8_t { NONE, RECEIVE_WRITE, SERVE_READ, RECEIVE_NOTIFY, SEND_WRITE, RECEIVE_READ };
enum class State : uint8_t { FREE, PREPARING, ARMED, RECEIVING, READY, CLAIMED, WAIT_END, TERMINAL, ERASING };
enum class Outcome : uint8_t { NO_EFFECT, EXACT, UNCERTAIN };

struct Result {
    int32_t status = 0;
    uint32_t bytes = 0;
    Outcome outcome = Outcome::NO_EFFECT;
};

struct Spec {
    Role role = Role::NONE;
    // Current local private identity for incoming records, including responses.
    rdma_registry::Binding binding{};
    rdma_protocol::RegionDescriptor grant{};
    uint64_t operation_id = 0;
    uint64_t receive_ticket = 0;
    uint64_t offset = 0;
    uint32_t requested = 0;
    uint16_t flags = 0;
    uint64_t deadline_us = 0;
};

struct Statistics {
    uint32_t live = 0;
    uint32_t armed = 0;
    uint32_t claimed = 0;
    uint32_t terminal = 0;
    uint32_t erasing = 0;
    uint32_t handles = 0;
    uint32_t work = 0;
};

class Operations;

class Handle final {
   public:
    Handle() = default;
    ~Handle();
    Handle(const Handle&) = delete;
    auto operator=(const Handle&) -> Handle& = delete;
    Handle(Handle&& other) noexcept;
    auto operator=(Handle&& other) noexcept -> Handle&;
    explicit operator bool() const { return owner_ != nullptr; }
    void reset();  // atomic release; never needs peer/session/table admission

   private:
    friend class Operations;
    Operations* owner_ = nullptr;
    size_t slot_ = OPERATION_SLOTS;
    uint64_t generation_ = 0;
};

enum class WorkKind : uint8_t { NONE, PREPARE, SEND, EFFECT, RESULT };

// A task-only pin. Buffers and Spec remain immutable until reset/finish, except
// PREPARE's explicitly mutable staging. Native asynchronous I/O must retain
// EFFECT until its actual completion; submission alone cannot release it.
class Work final {
   public:
    Work() = default;
    ~Work();
    Work(const Work&) = delete;
    auto operator=(const Work&) -> Work& = delete;
    Work(Work&& other) noexcept;
    auto operator=(Work&& other) noexcept -> Work&;
    explicit operator bool() const { return owner_ != nullptr; }
    [[nodiscard]] auto spec() const -> Spec { return spec_; }
    [[nodiscard]] auto staging() const -> std::span<const uint8_t> { return staging_; }
    [[nodiscard]] auto prepare_buffer() -> std::span<uint8_t>;
    // PREPARE, or SERVE_READ EFFECT, may fill private scratch. Every other
    // kind returns an empty mutable span; RX cannot borrow a writable pointer.
    [[nodiscard]] auto backing() const -> rdma_registry::ReleasedRegion { return backing_; }
    void reset();  // abandoned EFFECT remains uncertain, never exact success

   private:
    friend class Operations;
    Operations* owner_ = nullptr;
    size_t slot_ = OPERATION_SLOTS;
    uint64_t generation_ = 0;
    WorkKind kind_ = WorkKind::NONE;
    Spec spec_{};
    std::span<const uint8_t> staging_{};
    rdma_registry::ReleasedRegion backing_{};
};

class EraseWork final {
   public:
    EraseWork() = default;
    ~EraseWork();
    EraseWork(const EraseWork&) = delete;
    auto operator=(const EraseWork&) -> EraseWork& = delete;
    explicit operator bool() const { return owner_ != nullptr; }
    [[nodiscard]] auto buffer() -> std::span<uint8_t> { return staging_; }
    // Only after owner has erased every scratch byte. A contended finish may
    // be retried without erasing/calling native I/O again. Destruction before
    // finish quarantines the ERASING slot rather than permitting unsafe reuse.

   private:
    friend class Operations;
    Operations* owner_ = nullptr;
    size_t slot_ = OPERATION_SLOTS;
    uint64_t generation_ = 0;
    std::span<uint8_t> staging_{};
    rdma_registry::Lease lease_{};
};

class Operations final {
   public:
    Operations() = default;
    ~Operations();
    Operations(const Operations&) = delete;
    auto operator=(const Operations&) -> Operations& = delete;
    // Task owner supplies and clears32MiB private scratch before this one-shot
    // initialization, and retains it through final shutdown/drain.
    [[nodiscard]] auto initialize(std::span<uint8_t> scratch) -> rdma_protocol::Drop;
    // Incoming roles require exact unclaimed registry lease; outgoing roles
    // require an empty lease. IDs/tickets come from nonwrapping allocators and
    // authenticated consumer control. prepare never authorizes a native effect.
    [[nodiscard]] auto prepare(const Spec& spec, rdma_registry::Lease&& lease, Handle& output) -> rdma_protocol::Drop;
    [[nodiscard]] auto arm(const Handle& handle, uint64_t consumer_generation) -> rdma_protocol::Drop;
    [[nodiscard]] auto begin_prepare(const Handle& handle, Work& output) -> rdma_protocol::Drop;
    // Trusted task fills requested bytes before seal. No writable loan remains
    // afterwards; RX/TX never exposes uncleared or unfinished staging.
    [[nodiscard]] auto seal(Work& work) -> rdma_protocol::Drop;
    [[nodiscard]] auto begin_send(const Handle& handle, Work& output) -> rdma_protocol::Drop;
    [[nodiscard]] auto finish_send(Work& work, int32_t status) -> rdma_protocol::Drop;
    // Header already authenticated/replay-admitted from the same immutable
    // private payload. Current Binding was revalidated under short peer lease.
    // Bounded8-slot lookup/8192-byte copy; never creates an unarmed operation.
    [[nodiscard]] auto admit(const rdma_registry::Binding& current, const rdma_protocol::Header& header, std::span<const uint8_t> plaintext)
        -> rdma_protocol::Drop;
    // Takes registry claim under table guard, then returns stable task buffers
    // with guard released. Caller revalidated current peer/channel before call.
    [[nodiscard]] auto take_effect(const Handle& handle, Work& output) -> rdma_protocol::Drop;
    [[nodiscard]] auto finish_effect(Work& work, const Result& result) -> rdma_protocol::Drop;
    // Terminal observation waits for actual TX/effect Work retirement. READ
    // result staging is immutable; caller copies it outside all guards.
    [[nodiscard]] auto take_result(const Handle& handle, Work& output, Result& result) -> rdma_protocol::Drop;
    [[nodiscard]] auto observe(const Handle& handle, Result& output) -> rdma_protocol::Drop;
    [[nodiscard]] auto cancel(const Handle& handle, int32_t status) -> rdma_protocol::Drop;
    [[nodiscard]] auto close_binding(const rdma_registry::Binding& binding, int32_t status) -> rdma_protocol::Drop;
    [[nodiscard]] auto expire(uint64_t now_us, int32_t status) -> rdma_protocol::Drop;
    [[nodiscard]] auto begin_erase(EraseWork& output) -> rdma_protocol::Drop;
    [[nodiscard]] auto finish_erase(EraseWork& work) -> rdma_protocol::Drop;
    [[nodiscard]] auto statistics(Statistics& output) -> rdma_protocol::Drop;
    [[nodiscard]] auto shutdown() -> rdma_protocol::Drop;

   private:
    friend class Handle;
    friend class Work;
    struct Slot {
        State state = State::FREE;
        bool closing = false;
        bool end_seen = false;
        bool returned_length_set = false;
        bool send_attempted = false;
        uint64_t generation = 0;
        uint64_t consumer_generation = 0;
        Spec spec{};
        rdma_protocol::Coverage coverage{};
        Result result{};
        rdma_registry::Lease lease{};
        std::atomic<uint32_t> handles{0};
        std::atomic<uint32_t> work{0};
        std::atomic<bool> abandoned{false};
    };
    void release(Handle& handle);
    void release(Work& work);
    void close_locked(Slot& slot, int32_t status);
    void settle_abandoned_locked(Slot& slot);
    [[nodiscard]] auto find(const Handle& handle) -> Slot*;
    [[nodiscard]] auto find(const Work& work) -> Slot*;
    void publish_work(size_t slot, WorkKind kind, Work& output);
    std::atomic_flag guard_ = ATOMIC_FLAG_INIT;
    std::array<Slot, OPERATION_SLOTS> slots_{};
    std::span<uint8_t> scratch_{};
    uint64_t next_generation_ = 1;
    bool initialized_ = false;
    bool initialized_once_ = false;
    bool stopping_ = false;
};

}  // namespace ker::net::wki::rdma_operations
