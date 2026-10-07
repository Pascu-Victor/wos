#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <net/wki/rdma_protocol.hpp>

namespace ker::net::wki::rdma_session {

constexpr size_t DOMAIN_COUNT = 8;  // two transports, four services

struct Statistics {
    bool begun = false;
    bool negotiated = false;
    uint8_t transports = 0;
    uint8_t services = 0;
    uint32_t live_keys = 0;
    uint32_t replay_windows = 0;
    uint64_t incarnation = 0;
};

// A private session model shared by the two adapters. The kernel caller holds
// a short peer lifecycle admission around begin/install/send/receive, verifies
// the exact transport registration and channel identity separately, and never
// treats successful authentication as consumer or memory authority.
//
// Every public operation attempts the fixed guard once. No operation allocates,
// waits, invokes a callback or exposes a key. A record is at most8336 bytes.
class Session final {
   public:
    Session() = default;
    ~Session();
    Session(const Session&) = delete;
    auto operator=(const Session&) -> Session& = delete;

    // Incarnations are supplied by a nonwrapping local allocator. A retired
    // incarnation cannot be restarted on this object, even with identical
    // control-session identity. No keys are retained until both offers agree.
    // The first8 local offer nonce bytes encode incarnation in little endian;
    // this forces a distinct key transcript when counters restart. The owner
    // supplies the remaining8 bytes and a live control-session identity.
    [[nodiscard]] auto begin(uint16_t local, uint16_t peer, uint64_t incarnation, const rdma_protocol::Offer& offer) -> rdma_protocol::Drop;
    // Exact duplicate offers are idempotent and do not reset counters/replay.
    // A changed installed offer retires this incarnation and fails closed.
    [[nodiscard]] auto install(const rdma_protocol::Offer& remote, const rdma_crypto::Key& control_tx, const rdma_crypto::Key& control_rx)
        -> rdma_protocol::Drop;
    [[nodiscard]] auto available(rdma_protocol::Transport transport, rdma_protocol::Service service) -> bool;

    // Caller supplies a header with counter0. The model assigns the counter,
    // burns it even if encoding fails, and preserves record output on failure.
    [[nodiscard]] auto send(const rdma_protocol::Header& header, const void* plaintext, void* record, size_t capacity)
        -> rdma_protocol::Drop;
    // The record is a bounded immutable private snapshot owned exclusively by
    // the caller. On success its payload is plaintext and header is published;
    // on any failure the supplied record bytes are erased. Decrypt and replay
    // admission share the same short guard, with no plaintext effect on backing.
    [[nodiscard]] auto receive(rdma_protocol::Transport ingress, void* private_record, size_t size, rdma_protocol::Header& header)
        -> rdma_protocol::Drop;
    // Retirement wipes keys, counters, replay and offer state before return.
    // Caller must retry PRESSURE in task context; it must not report retirement
    // or free associated region/operation state while that result is pending.
    [[nodiscard]] auto retire() -> rdma_protocol::Drop;
    [[nodiscard]] auto statistics(Statistics& output) -> rdma_protocol::Drop;

   private:
    struct Domain {
        bool active = false;
        rdma_protocol::KeyMaterial tx{};
        rdma_protocol::KeyMaterial rx{};
        uint64_t next_tx = 0;
        rdma_protocol::ReplayWindow replay{};
    };

    void erase_locked();
    [[nodiscard]] auto context(rdma_protocol::Transport transport, rdma_protocol::Service service, bool outgoing) const
        -> rdma_protocol::KeyContext;
    std::atomic_flag guard_ = ATOMIC_FLAG_INIT;
    std::array<Domain, DOMAIN_COUNT> domains_{};
    rdma_protocol::Offer local_offer_{};
    rdma_protocol::Offer remote_offer_{};
    auth_crypto::Digest negotiation_{};
    uint16_t local_ = 0;
    uint16_t peer_ = 0;
    uint64_t incarnation_ = 0;
    uint64_t last_incarnation_ = 0;
    uint8_t transports_ = 0;
    uint8_t services_ = 0;
    bool begun_ = false;
    bool negotiated_ = false;
};

}  // namespace ker::net::wki::rdma_session
