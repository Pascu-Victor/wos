#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <net/wki/rdma_protocol.hpp>

namespace ker::net::wki::rdma_registry {

constexpr size_t REGION_SLOTS = 256;
constexpr uint64_t REGISTERED_BYTES = 64ULL * 1024 * 1024;

// Private immutable authority. Wire channel numbers never replace these exact
// local allocations. Production callers revalidate their liveness under short
// peer admission before claim; no registry method calls into core/consumer code.
struct Binding {
    uint16_t owner_node = 0;
    uint16_t peer_node = 0;
    rdma_protocol::SessionId session_id{};
    uint64_t secure_incarnation = 0;
    uintptr_t transport_cookie = 0;
    uint64_t transport_generation = 0;
    uintptr_t channel_cookie = 0;
    uint32_t channel_generation = 0;
    uint16_t channel_id = 0;
    rdma_protocol::Transport transport = rdma_protocol::Transport::NONE;
    rdma_protocol::Service service = rdma_protocol::Service::NONE;
};

struct Registration {
    Binding binding{};
    void* backing = nullptr;  // private kernel memory, never a peer-mapped BAR
    uint32_t size = 0;
    uint64_t binding_generation = 0;
    uint8_t permissions = 0;
};

struct ReleasedRegion {
    void* backing = nullptr;
    uint32_t size = 0;
};

struct Statistics {
    uint32_t live = 0;
    uint32_t closing = 0;
    uint32_t references = 0;
    uint32_t claims = 0;
    uint64_t bytes = 0;
};

class Registry;

// A pin is not authorization. memory() returns a pointer only after claim()
// linearizes before revoke. Such preexisting claimed work may finish while
// retirement waits for the pin. A lease never holds a lock or peer admission.
class Lease final {
   public:
    Lease() = default;
    ~Lease();
    Lease(const Lease&) = delete;
    auto operator=(const Lease&) -> Lease& = delete;
    Lease(Lease&& other) noexcept;
    auto operator=(Lease&& other) noexcept -> Lease&;
    explicit operator bool() const { return registry_ != nullptr; }
    [[nodiscard]] auto claim() -> rdma_protocol::Drop;
    [[nodiscard]] auto memory() const -> ReleasedRegion;
    [[nodiscard]] auto descriptor() const -> rdma_protocol::RegionDescriptor { return descriptor_; }
    void reset();

   private:
    friend class Registry;
    Registry* registry_ = nullptr;
    size_t slot_ = REGION_SLOTS;
    uint64_t generation_ = 0;
    bool claimed_ = false;
    rdma_protocol::RegionDescriptor descriptor_{};
};

class Registry final {
   public:
    Registry() = default;
    ~Registry();
    Registry(const Registry&) = delete;
    auto operator=(const Registry&) -> Registry& = delete;
    // One-shot per boot/instance. Root is local private entropy; caller wipes
    // its source copy. Shutdown cannot reset generations for reinitialization.
    [[nodiscard]] auto initialize(uint16_t owner, const rdma_crypto::Key& root) -> rdma_protocol::Drop;
    [[nodiscard]] auto register_region(const Registration& registration, rdma_protocol::RegionDescriptor& output) -> rdma_protocol::Drop;
    // Successful crypto verification is a caller precondition, not grant
    // authority. Incoming READ/WRITE/NOTIFY must match this private binding.
    [[nodiscard]] auto admit(const Binding& expected, const rdma_protocol::Header& header, Lease& output) -> rdma_protocol::Drop;
    [[nodiscard]] auto revoke(uint32_t rkey, uint64_t generation) -> rdma_protocol::Drop;
    [[nodiscard]] auto close_peer(uint16_t peer, const rdma_protocol::SessionId& session) -> rdma_protocol::Drop;
    [[nodiscard]] auto close_transport(uintptr_t cookie, uint64_t generation) -> rdma_protocol::Drop;
    [[nodiscard]] auto reclaim(uint32_t rkey, uint64_t generation, ReleasedRegion& output) -> rdma_protocol::Drop;
    [[nodiscard]] auto statistics(Statistics& output) -> rdma_protocol::Drop;
    // Close every region; succeed/erase root only once each exact allocation
    // has been reclaimed by its owner. Never silently discard backing storage.
    [[nodiscard]] auto shutdown() -> rdma_protocol::Drop;

   private:
    friend class Lease;
    struct Slot {
        bool active = false;
        bool closing = false;
        Binding binding{};
        rdma_protocol::RegionDescriptor descriptor{};
        void* backing = nullptr;
        std::atomic<uint32_t> references{0};
        std::atomic<uint32_t> claims{0};
    };
    [[nodiscard]] auto claim(Lease& lease) -> rdma_protocol::Drop;
    [[nodiscard]] auto memory(const Lease& lease) const -> ReleasedRegion;
    void release(Lease& lease);
    std::atomic_flag guard_ = ATOMIC_FLAG_INIT;
    std::array<Slot, REGION_SLOTS> slots_{};
    rdma_crypto::Key capability_root_{};
    uint16_t owner_ = 0;
    bool initialized_ = false;
    bool initialized_once_ = false;
    bool stopping_ = false;
    uint32_t next_rkey_ = 1;
    uint64_t next_generation_ = 1;
    uint64_t bytes_ = 0;
};

}  // namespace ker::net::wki::rdma_registry
