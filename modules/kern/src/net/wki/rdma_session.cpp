#include "rdma_session.hpp"

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace ker::net::wki::rdma_session {

namespace {

using rdma_protocol::Drop;
using rdma_protocol::Service;
using rdma_protocol::Transport;

class TryGuard final {
   public:
    explicit TryGuard(std::atomic_flag& flag) : flag_(flag), held_(!flag.test_and_set(std::memory_order_acquire)) {}
    ~TryGuard() {
        if (held_) flag_.clear(std::memory_order_release);
    }
    TryGuard(const TryGuard&) = delete;
    auto operator=(const TryGuard&) -> TryGuard& = delete;
    explicit operator bool() const { return held_; }

   private:
    std::atomic_flag& flag_;
    bool held_;
};

class RecordWipe final {
   public:
    RecordWipe(void* data, size_t size) : data_(data), size_(size) {}
    ~RecordWipe() {
        if (armed_) auth_crypto::secure_erase(data_, size_);
    }
    RecordWipe(const RecordWipe&) = delete;
    auto operator=(const RecordWipe&) -> RecordWipe& = delete;
    void accept() { armed_ = false; }

   private:
    void* data_;
    size_t size_;
    bool armed_ = true;
};

auto overlaps(const void* lhs, size_t lhs_size, const void* rhs, size_t rhs_size) -> bool {
    if (lhs_size == 0 || rhs_size == 0) return false;
    uintptr_t const LEFT = reinterpret_cast<uintptr_t>(lhs);
    uintptr_t const RIGHT = reinterpret_cast<uintptr_t>(rhs);
    // Difference comparisons do not wrap even for a rejected hostile range.
    return LEFT <= RIGHT ? lhs_size > RIGHT - LEFT : rhs_size > LEFT - RIGHT;
}

auto valid_range(const void* data, size_t size) -> bool {
    return (data != nullptr || size == 0) && size <= UINTPTR_MAX - reinterpret_cast<uintptr_t>(data);
}

auto valid_domain(Transport transport, Service service) -> bool {
    return rdma_protocol::transport_bit(transport) != 0 && rdma_protocol::service_bit(service) != 0;
}

auto domain_index(Transport transport, Service service) -> size_t {
    return (static_cast<size_t>(transport) - 1) * 4 + static_cast<size_t>(service) - 1;
}

}  // namespace

Session::~Session() {
    // External quiescence is mandatory; destruction cannot retry a busy guard.
    erase_locked();
    last_incarnation_ = 0;
}

void Session::erase_locked() {
    auth_crypto::secure_erase(domains_.data(), sizeof(domains_));
    auth_crypto::secure_erase(&local_offer_, sizeof(local_offer_));
    auth_crypto::secure_erase(&remote_offer_, sizeof(remote_offer_));
    auth_crypto::secure_erase(negotiation_.data(), negotiation_.size());
    local_ = peer_ = 0;
    incarnation_ = 0;
    transports_ = services_ = 0;
    begun_ = negotiated_ = false;
    // last_incarnation_ deliberately survives explicit retirement.
}

auto Session::context(Transport transport, Service service, bool outgoing) const -> rdma_protocol::KeyContext {
    return {.session_id = local_offer_.session_id,
            .negotiation = negotiation_,
            .src_node = outgoing ? local_ : peer_,
            .dst_node = outgoing ? peer_ : local_,
            .transport = transport,
            .service = service};
}

auto Session::begin(uint16_t local, uint16_t peer, uint64_t incarnation, const rdma_protocol::Offer& offer) -> Drop {
    if (!valid_range(&offer, sizeof(offer)) || overlaps(this, sizeof(*this), &offer, sizeof(offer))) return Drop::LENGTH;
    TryGuard const GUARD(guard_);
    if (!GUARD) return Drop::PRESSURE;
    if (begun_ || incarnation == 0 || incarnation <= last_incarnation_) return Drop::RETIRED;
    if (local == 0 || peer == 0 || local == UINT16_MAX || peer == UINT16_MAX || local == peer) return Drop::IDENTITY;
    if (!rdma_protocol::valid_offer(offer)) return Drop::FIELD;
    uint64_t offer_incarnation = 0;
    for (size_t index = 0; index < sizeof(incarnation); ++index) {
        offer_incarnation |= uint64_t{offer.nonce[index]} << (index * 8U);
    }
    if (offer_incarnation != incarnation) return Drop::FIELD;
    local_offer_ = offer;
    local_ = local;
    peer_ = peer;
    incarnation_ = last_incarnation_ = incarnation;
    begun_ = true;
    return Drop::NONE;
}

auto Session::install(const rdma_protocol::Offer& remote, const rdma_crypto::Key& control_tx, const rdma_crypto::Key& control_rx) -> Drop {
    if (!valid_range(&remote, sizeof(remote)) || !valid_range(&control_tx, sizeof(control_tx)) ||
        !valid_range(&control_rx, sizeof(control_rx)) || overlaps(this, sizeof(*this), &remote, sizeof(remote)) ||
        overlaps(this, sizeof(*this), &control_tx, sizeof(control_tx)) || overlaps(this, sizeof(*this), &control_rx, sizeof(control_rx))) {
        return Drop::LENGTH;
    }
    TryGuard const GUARD(guard_);
    if (!GUARD) return Drop::PRESSURE;
    if (!begun_) return Drop::RETIRED;
    if (negotiated_) {
        if (std::memcmp(&remote_offer_, &remote, sizeof(remote)) == 0) return Drop::NONE;
        erase_locked();
        return Drop::RETIRED;
    }
    if (!rdma_protocol::valid_offer(remote)) return Drop::FIELD;
    if (remote.session_id != local_offer_.session_id) return Drop::SESSION;
    if (!rdma_protocol::negotiation_digest(local_, peer_, local_offer_, remote, negotiation_)) return Drop::SESSION;
    remote_offer_ = remote;
    transports_ = local_offer_.transports & remote.transports;
    services_ = local_offer_.services & remote.services;
    for (size_t index = 0; index < domains_.size(); ++index) {
        auto const TRANSPORT = static_cast<Transport>(index / 4 + 1);
        auto const SERVICE = static_cast<Service>(index % 4 + 1);
        if ((transports_ & rdma_protocol::transport_bit(TRANSPORT)) == 0 || (services_ & rdma_protocol::service_bit(SERVICE)) == 0)
            continue;
        auto& domain = domains_[index];
        if (!rdma_protocol::derive_key(control_tx, context(TRANSPORT, SERVICE, true), domain.tx) ||
            !rdma_protocol::derive_key(control_rx, context(TRANSPORT, SERVICE, false), domain.rx)) {
            erase_locked();
            return Drop::RETIRED;
        }
        domain.next_tx = 1;
        domain.active = true;
    }
    negotiated_ = true;
    return Drop::NONE;
}

auto Session::available(Transport transport, Service service) -> bool {
    TryGuard const GUARD(guard_);
    return GUARD && begun_ && negotiated_ && valid_domain(transport, service) && domains_[domain_index(transport, service)].active;
}

auto Session::send(const rdma_protocol::Header& header, const void* plaintext, void* record, size_t capacity) -> Drop {
    if (!valid_range(&header, sizeof(header)) || overlaps(this, sizeof(*this), &header, sizeof(header))) return Drop::LENGTH;
    size_t const SIZE = rdma_protocol::HEADER_BYTES + header.payload_length;
    if (overlaps(this, sizeof(*this), plaintext, header.payload_length) || overlaps(this, sizeof(*this), record, SIZE)) return Drop::LENGTH;
    TryGuard const GUARD(guard_);
    if (!GUARD) return Drop::PRESSURE;
    if (!begun_ || !negotiated_) return Drop::RETIRED;
    auto const TRANSPORT = static_cast<Transport>(header.transport);
    auto const SERVICE = static_cast<Service>(header.service);
    if (!valid_domain(TRANSPORT, SERVICE) || !domains_[domain_index(TRANSPORT, SERVICE)].active) return Drop::DOMAIN;
    if (header.src_node != local_ || header.dst_node != peer_) return Drop::IDENTITY;
    if (header.session_id != local_offer_.session_id) return Drop::SESSION;
    if (header.counter != 0) return Drop::COUNTER;
    auto& domain = domains_[domain_index(TRANSPORT, SERVICE)];
    uint64_t const COUNTER = rdma_protocol::reserve(domain.next_tx);
    if (COUNTER == 0) return Drop::COUNTER;
    if (overlaps(&header, sizeof(header), record, SIZE) && static_cast<const void*>(&header) != record) return Drop::FIELD;
    rdma_protocol::Header assigned = header;
    assigned.counter = COUNTER;
    return rdma_protocol::encode(domain.tx, assigned, plaintext, record, capacity) ? Drop::NONE : Drop::FIELD;
}

auto Session::receive(Transport ingress, void* private_record, size_t size, rdma_protocol::Header& header) -> Drop {
    // Invalid ranges and aliases are not safe storage to consume or wipe.
    if (size < rdma_protocol::HEADER_BYTES || size > rdma_protocol::RECORD_BYTES || !valid_range(private_record, size) ||
        !valid_range(&header, sizeof(header)) || overlaps(this, sizeof(*this), private_record, size) ||
        overlaps(this, sizeof(*this), &header, sizeof(header)) || overlaps(private_record, size, &header, sizeof(header))) {
        return Drop::LENGTH;
    }
    RecordWipe wipe(private_record, size);
    TryGuard const GUARD(guard_);
    if (!GUARD) return Drop::PRESSURE;
    if (!begun_ || !negotiated_) return Drop::RETIRED;
    rdma_protocol::Header candidate{};
    std::memcpy(&candidate, private_record, sizeof(candidate));
    auto const SERVICE = static_cast<Service>(candidate.service);
    if (!valid_domain(ingress, SERVICE) || candidate.transport != static_cast<uint8_t>(ingress) ||
        !domains_[domain_index(ingress, SERVICE)].active) {
        return Drop::DOMAIN;
    }
    auto& domain = domains_[domain_index(ingress, SERVICE)];
    auto* plaintext = static_cast<uint8_t*>(private_record) + rdma_protocol::HEADER_BYTES;
    Drop const DECODE = rdma_protocol::decode(domain.rx, context(ingress, SERVICE, false), private_record, size, candidate, plaintext,
                                              size - rdma_protocol::HEADER_BYTES);
    if (DECODE != Drop::NONE) return DECODE;
    Drop const REPLAY = rdma_protocol::replay_admit(domain.replay, candidate.counter);
    if (REPLAY != Drop::NONE) return REPLAY;
    header = candidate;
    wipe.accept();
    return Drop::NONE;
}

auto Session::retire() -> Drop {
    TryGuard const GUARD(guard_);
    if (!GUARD) return Drop::PRESSURE;
    erase_locked();
    return Drop::NONE;
}

auto Session::statistics(Statistics& output) -> Drop {
    if (!valid_range(&output, sizeof(output)) || overlaps(this, sizeof(*this), &output, sizeof(output))) return Drop::LENGTH;
    TryGuard const GUARD(guard_);
    if (!GUARD) return Drop::PRESSURE;
    Statistics snapshot{
        .begun = begun_, .negotiated = negotiated_, .transports = transports_, .services = services_, .incarnation = incarnation_};
    for (const auto& domain : domains_) {
        if (domain.active) {
            snapshot.live_keys += 2;
            ++snapshot.replay_windows;
        }
    }
    output = snapshot;
    return Drop::NONE;
}

}  // namespace ker::net::wki::rdma_session
