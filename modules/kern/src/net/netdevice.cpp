#include "netdevice.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <net/backlog.hpp>
#include <net/net_trace.hpp>
#include <net/proto/ethernet.hpp>
#include <net/proto/ipv4.hpp>
#include <net/proto/ipv6.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/smt/smt.hpp>
#include <string_view>

#include "net/packet.hpp"
#include "platform/sys/spinlock.hpp"

namespace ker::net {

using log = ker::mod::dbg::logger<"netdev">;

namespace {
std::array<NetDevice*, MAX_NET_DEVICES> devices = {};
size_t device_count = 0;
uint32_t next_ifindex = 1;
uint32_t next_eth_index = 0;
uint64_t next_lifetime_generation = 1;
mod::sys::Spinlock devices_lock;

constexpr uint32_t NETDEV_LIFETIME_RETIRING = uint32_t{1} << 31U;
constexpr uint32_t NETDEV_LIFETIME_READER_MASK = NETDEV_LIFETIME_RETIRING - 1U;

constexpr uint32_t IFF_BROADCAST = 0x0002;
constexpr uint32_t IFF_LOOPBACK = 0x0008;
constexpr uint32_t IFF_MULTICAST = 0x1000;

auto is_loopback_name(const std::array<char, NETDEV_NAME_LEN>& name) -> bool { return std::string_view(name.data()) == "lo"; }

auto next_registration_generation_locked() -> uint64_t {
    uint64_t const GENERATION = next_lifetime_generation++;
    if (next_lifetime_generation == 0) {
        next_lifetime_generation = 1;
    }
    return GENERATION;
}

auto registered_identity_locked(NetDevice* dev) -> NetDeviceIdentity {
    if (dev == nullptr) {
        return {};
    }
    for (size_t i = 0; i < device_count; ++i) {
        if (devices.at(i) == dev) {
            return {.device = dev, .generation = dev->lifetime_generation.load(std::memory_order_acquire)};
        }
    }
    return {};
}
}  // namespace

NetDeviceRef::~NetDeviceRef() { reset(); }

NetDeviceRef::NetDeviceRef(NetDeviceRef&& other) noexcept : identity_(other.take_identity()) {}

auto NetDeviceRef::operator=(NetDeviceRef&& other) noexcept -> NetDeviceRef& {
    if (this != &other) {
        reset();
        identity_ = other.take_identity();
    }
    return *this;
}

auto NetDeviceRef::take_identity() -> NetDeviceIdentity {
    NetDeviceIdentity const IDENTITY = identity_;
    identity_ = {};
    return IDENTITY;
}

void NetDeviceRef::reset() {
    NetDeviceIdentity const IDENTITY = take_identity();
    netdev_release_identity(IDENTITY);
}

auto netdev_register(NetDevice* dev) -> int {
    devices_lock.lock();
    if (dev == nullptr || device_count >= MAX_NET_DEVICES) {
        devices_lock.unlock();
        return -1;
    }

    if (registered_identity_locked(dev).valid() || dev->lifetime_readers.load(std::memory_order_acquire) != NETDEV_LIFETIME_RETIRING) {
        devices_lock.unlock();
        return -1;
    }

    dev->ifindex = next_ifindex++;

    // Auto-name if the device name is empty
    if (dev->name.front() == '\0') {
        // Name format: "ethN"
        std::array<char, NETDEV_NAME_LEN> buf = {};
        buf.at(0) = 'e';
        buf.at(1) = 't';
        buf.at(2) = 'h';
        uint32_t const IDX = next_eth_index++;
        // Simple integer-to-string for index
        if (IDX < 10) {
            buf.at(3) = static_cast<char>('0' + static_cast<char>(IDX));
            buf.at(4) = '\0';
        } else {
            buf.at(3) = static_cast<char>('0' + static_cast<char>(IDX / 10));
            buf.at(4) = static_cast<char>('0' + static_cast<char>(IDX % 10));
            buf.at(5) = '\0';
        }
        dev->name = buf;
    }

    if (dev->tx_queue_len == 0) {
        dev->tx_queue_len = 1000;
    }
    if (dev->link_flags == 0) {
        if (is_loopback_name(dev->name)) {
            dev->link_flags = IFF_LOOPBACK;
        } else {
            dev->link_flags = IFF_BROADCAST | IFF_MULTICAST;
        }
    }

    uint64_t const GENERATION = next_registration_generation_locked();
    dev->lifetime_generation.store(GENERATION, std::memory_order_relaxed);
    dev->lifetime_readers.store(0, std::memory_order_release);
    devices.at(device_count) = dev;
    device_count++;
    devices_lock.unlock();

#ifdef DEBUG_NETDEV
    log::debug("registered device %s (ifindex=%d, MAC=%x:%x:%x:%x:%x:%x)", dev->name.data(), dev->ifindex, dev->mac[0], dev->mac[1],
               dev->mac[2], dev->mac[3], dev->mac[4], dev->mac[5]);
#endif

    return 0;
}

auto netdev_unregister(NetDevice* dev) -> int {
    NetDeviceRetireToken token{};
    int const RESULT = netdev_unregister_begin(dev, token);
    if (RESULT != 0) {
        return RESULT;
    }
    netdev_unregister_wait(token);
    return 0;
}

auto netdev_unregister_begin(NetDevice* dev, NetDeviceRetireToken& token) -> int {
    token = {};
    if (dev == nullptr) {
        return -1;
    }

    devices_lock.lock();
    for (size_t i = 0; i < device_count; i++) {
        if (devices.at(i) != dev) {
            continue;
        }

        uint64_t const GENERATION = dev->lifetime_generation.load(std::memory_order_acquire);
        uint32_t const PREVIOUS = dev->lifetime_readers.fetch_or(NETDEV_LIFETIME_RETIRING, std::memory_order_acq_rel);
        if ((PREVIOUS & NETDEV_LIFETIME_RETIRING) != 0U || GENERATION == 0) {
            devices_lock.unlock();
            return -1;
        }
        token.identity = {.device = dev, .generation = GENERATION};

        for (size_t j = i + 1; j < device_count; j++) {
            devices.at(j - 1) = devices.at(j);
        }
        device_count--;
        devices.at(device_count) = nullptr;
        devices_lock.unlock();
        return 0;
    }
    devices_lock.unlock();
    return -1;
}

void netdev_unregister_wait(const NetDeviceRetireToken& token) {
    if (!token.valid()) {
        return;
    }

    NetDevice* const DEV = token.identity.device;
    if (DEV->lifetime_generation.load(std::memory_order_acquire) != token.identity.generation ||
        (DEV->lifetime_readers.load(std::memory_order_acquire) & NETDEV_LIFETIME_RETIRING) == 0U) {
        log::error("netdev_unregister_wait: stale generation");
        return;
    }

    while ((DEV->lifetime_readers.load(std::memory_order_acquire) & NETDEV_LIFETIME_READER_MASK) != 0U) {
        ker::mod::sched::kern_yield();
    }
}

auto netdev_registered_identity(NetDevice* dev) -> NetDeviceIdentity {
    devices_lock.lock();
    NetDeviceIdentity const IDENTITY = registered_identity_locked(dev);
    devices_lock.unlock();
    return IDENTITY;
}

auto netdev_try_retain(NetDeviceIdentity identity) -> NetDeviceRef {
    if (!identity.valid() || identity.device->lifetime_generation.load(std::memory_order_acquire) != identity.generation) {
        return {};
    }

    auto& readers = identity.device->lifetime_readers;
    uint32_t state = readers.load(std::memory_order_acquire);
    for (;;) {
        if ((state & NETDEV_LIFETIME_RETIRING) != 0U || (state & NETDEV_LIFETIME_READER_MASK) == NETDEV_LIFETIME_READER_MASK) {
            return {};
        }
        if (readers.compare_exchange_weak(state, state + 1U, std::memory_order_acq_rel, std::memory_order_acquire)) {
            if (identity.device->lifetime_generation.load(std::memory_order_acquire) == identity.generation) {
                return NetDeviceRef(identity);
            }
            readers.fetch_sub(1U, std::memory_order_release);
            return {};
        }
    }
}

auto netdev_retain_registered(NetDevice* dev) -> NetDeviceRef { return netdev_try_retain(netdev_registered_identity(dev)); }

void netdev_release_identity(NetDeviceIdentity identity) {
    if (!identity.valid()) {
        return;
    }
    if (identity.device->lifetime_generation.load(std::memory_order_acquire) != identity.generation) {
        log::error("netdev_release_identity: stale generation");
        return;
    }

    auto& readers = identity.device->lifetime_readers;
    uint32_t state = readers.load(std::memory_order_relaxed);
    for (;;) {
        if ((state & NETDEV_LIFETIME_READER_MASK) == 0U) {
            log::error("netdev_release_identity: unbalanced release");
            return;
        }
        if (readers.compare_exchange_weak(state, state - 1U, std::memory_order_release, std::memory_order_relaxed)) {
            return;
        }
    }
}

auto netdev_find_by_name(const std::string_view NAME) -> NetDevice* {
    if (NAME.empty()) {
        return nullptr;
    }
    devices_lock.lock();
    for (size_t i = 0; i < device_count; i++) {
        if (std::string_view(devices.at(i)->name.data()) == NAME) {
            NetDevice* result = devices.at(i);
            devices_lock.unlock();
            return result;
        }
    }
    devices_lock.unlock();
    return nullptr;
}

auto netdev_find_by_name_ref(const std::string_view NAME) -> NetDeviceRef {
    if (NAME.empty()) {
        return {};
    }
    devices_lock.lock();
    for (size_t i = 0; i < device_count; ++i) {
        NetDevice* const DEV = devices.at(i);
        if (std::string_view(DEV->name.data()) == NAME) {
            NetDeviceRef result =
                netdev_try_retain({.device = DEV, .generation = DEV->lifetime_generation.load(std::memory_order_acquire)});
            devices_lock.unlock();
            return result;
        }
    }
    devices_lock.unlock();
    return {};
}

NetDeviceRegistryLease::NetDeviceRegistryLease() : irq_flags_(devices_lock.lock_irqsave()) {}

NetDeviceRegistryLease::~NetDeviceRegistryLease() { devices_lock.unlock_irqrestore(irq_flags_); }

// Membership is intentionally a member operation: the lease object proves the
// registry lock is held for the entire check/publication transaction.
auto NetDeviceRegistryLease::contains(const NetDevice* dev) const -> bool {  // NOLINT(readability-convert-member-functions-to-static)
    if (dev == nullptr) {
        return false;
    }
    for (size_t i = 0; i < device_count; ++i) {
        if (devices.at(i) == dev) {
            return true;
        }
    }
    return false;
}

auto netdev_is_registered(const NetDevice* dev) -> bool {
    NetDeviceRegistryLease const REGISTRATION;
    return REGISTRATION.contains(dev);
}

auto netdev_count() -> size_t {
    devices_lock.lock();
    size_t const COUNT = device_count;
    devices_lock.unlock();
    return COUNT;
}

auto netdev_at(size_t i) -> NetDevice* {
    devices_lock.lock();
    if (i >= device_count) {
        devices_lock.unlock();
        return nullptr;
    }
    NetDevice* dev = devices.at(i);
    devices_lock.unlock();
    return dev;
}

auto netdev_at_ref(size_t i) -> NetDeviceRef {
    devices_lock.lock();
    if (i >= device_count) {
        devices_lock.unlock();
        return {};
    }
    NetDevice* const DEV = devices.at(i);
    NetDeviceRef result = netdev_try_retain({.device = DEV, .generation = DEV->lifetime_generation.load(std::memory_order_acquire)});
    devices_lock.unlock();
    return result;
}

auto netdev_snapshot(NetDeviceSnapshot* out, size_t max) -> size_t {
    if (out == nullptr || max == 0) {
        return 0;
    }

    devices_lock.lock();
    size_t const COUNT = std::min(device_count, max);
    for (size_t i = 0; i < COUNT; ++i) {
        NetDevice const* dev = devices.at(i);
        if (dev == nullptr) {
            continue;
        }
        auto& row = out[i];
        row.name = dev->name;
        row.mac = dev->mac;
        row.mtu = dev->mtu;
        row.tx_queue_len = dev->tx_queue_len;
        row.link_flags = dev->link_flags;
        row.ifindex = dev->ifindex;
        row.state = dev->state;
        row.remotable = dev->remotable != nullptr;
        row.wki_rx_forward = dev->wki_rx_forward.load(std::memory_order_acquire) != nullptr;
        row.wki_transport = dev->wki_transport;
        row.rx_packets = dev->rx_packets;
        row.tx_packets = dev->tx_packets;
        row.rx_bytes = dev->rx_bytes;
        row.tx_bytes = dev->tx_bytes;
        row.rx_dropped = dev->rx_dropped;
        row.tx_dropped = dev->tx_dropped;
    }
    devices_lock.unlock();
    return COUNT;
}

// Forward declaration - will be implemented in ethernet.cpp
void netdev_rx(NetDevice* dev, PacketBuffer* pkt) {
    NET_TRACE_SPAN(SPAN_NETDEV_RX);
    if (dev == nullptr || pkt == nullptr) {
        return;
    }

#ifdef DEBUG_NETDEV
    log::debug("netdev_rx: received packet len=%zu on device %s", pkt->len, dev->name.data());
#endif

    pkt->dev = dev;
    dev->rx_packets++;
    dev->rx_bytes += pkt->len;

    // Loopback device sends raw IP packets (no Ethernet header)
    // Check if this is loopback by name
    if (is_loopback_name(dev->name)) {
#ifdef DEBUG_NETDEV
        log::debug("netdev_rx: loopback device detected, bypassing Ethernet");
#endif
        // Determine protocol from IP version in first byte
        if (pkt->len > 0) {
            uint8_t const VERSION = (pkt->data[0] >> 4) & 0xF;
#ifdef DEBUG_NETDEV
            log::debug("netdev_rx: IP version = %u", VERSION);
#endif
            if (VERSION == 4) {
                proto::ipv4_rx(dev, pkt);
                return;
            }
            if (VERSION == 6) {
                proto::ipv6_rx(dev, pkt);
                return;
            }
        }
        // Unknown or malformed
        pkt_free(pkt);
        return;
    }

    // Steer real NIC packets to per-CPU handler threads for protocol processing.
    // Keep NAPI workers focused on RX-ring draining; WKI/protocol handlers can
    // transmit, allocate, or wake tasks, and should not run on the netpoll stack.
    if (backlog_ready()) {
        uint64_t const TARGET = backlog_flow_hash(pkt, ker::mod::smt::get_core_count());
        backlog_enqueue(TARGET, pkt);
        return;
    }

    // Inline processing: same-CPU fast path, early boot, or single CPU.
    WkiRxForwardHook const RX_FORWARD = dev->wki_rx_forward.load(std::memory_order_acquire);
    if (RX_FORWARD != nullptr) {
        RX_FORWARD(dev, pkt);
    }
    NET_TRACE_TICK();
    proto::eth_rx(dev, pkt);
}

}  // namespace ker::net
