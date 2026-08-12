#pragma once

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <net/address.hpp>
#include <net/packet.hpp>
#include <platform/sys/spinlock.hpp>
#include <string_view>

namespace ker::net::wki {
struct RemotableOps;
}

namespace ker::net {

constexpr size_t MAX_NET_DEVICES = 16;
constexpr size_t NETDEV_NAME_LEN = 16;

struct NetDevice;
using WkiRxForwardHook = void (*)(NetDevice* dev, PacketBuffer* pkt);

static_assert(std::atomic<WkiRxForwardHook>::is_always_lock_free, "WKI RX forward hook must stay lock-free");

struct NetDeviceOps {
    int (*open)(NetDevice* dev);
    void (*close)(NetDevice* dev);
    int (*start_xmit)(NetDevice* dev, PacketBuffer* pkt);
    void (*set_mac)(NetDevice* dev, const uint8_t* mac);
    // Optional: pin NAPI worker for queue pair `pair_idx` to `cpu` and steer its MSI-X entry.
    // Returns 0 on success, -EINVAL if pair_idx is out of range, -ENOSYS if unsupported.
    int (*set_queue_cpu)(NetDevice* dev, uint32_t pair_idx, uint64_t cpu);
};

struct NetDevice {
    std::array<char, NETDEV_NAME_LEN> name{};
    proto::MacAddress mac;
    uint32_t mtu = 1500;
    uint32_t tx_queue_len = 1000;
    uint32_t link_flags = 0;
    uint8_t state = 0;  // 0=down, 1=up
    uint32_t ifindex = 0;
    NetDeviceOps const* ops = nullptr;
    void* private_data = nullptr;

    // WKI remotable trait - set by drivers that support remote access
    wki::RemotableOps const* remotable = nullptr;

    // D11: WKI RX forward hook - set by dev_server when a remote consumer is attached.
    // Called from netdev_rx() to forward received packets to remote consumers.
    std::atomic<WkiRxForwardHook> wki_rx_forward{nullptr};

    // Set by wki_eth_transport_init() when this NIC is claimed as the WKI transport.
    // Prevents the NIC from being advertised as a remotable NET resource to peers.
    bool wki_transport = false;

    // Statistics
    uint64_t rx_packets = 0;
    uint64_t tx_packets = 0;
    uint64_t rx_bytes = 0;
    uint64_t tx_bytes = 0;
    uint64_t rx_dropped = 0;
    uint64_t tx_dropped = 0;

    // Internal registration lifetime. Admission is lock-free so packets and
    // other users can retain a device without taking the registry lock.
    std::atomic<uint64_t> lifetime_generation{0};
    std::atomic<uint32_t> lifetime_readers{uint32_t{1} << 31U};
};

static_assert(std::atomic<uint64_t>::is_always_lock_free, "NetDevice generations must stay lock-free");
static_assert(std::atomic<uint32_t>::is_always_lock_free, "NetDevice retention must stay lock-free");

class NetDeviceRef {
   public:
    NetDeviceRef() = default;
    ~NetDeviceRef();

    NetDeviceRef(const NetDeviceRef&) = delete;
    auto operator=(const NetDeviceRef&) -> NetDeviceRef& = delete;
    NetDeviceRef(NetDeviceRef&& other) noexcept;
    auto operator=(NetDeviceRef&& other) noexcept -> NetDeviceRef&;

    [[nodiscard]] auto get() const -> NetDevice* { return identity_.device; }
    [[nodiscard]] auto operator->() const -> NetDevice* { return get(); }
    [[nodiscard]] explicit operator bool() const { return identity_.valid(); }
    [[nodiscard]] auto identity() const -> NetDeviceIdentity { return identity_; }
    [[nodiscard]] auto take_identity() -> NetDeviceIdentity;
    void reset();

   private:
    explicit NetDeviceRef(NetDeviceIdentity identity) : identity_(identity) {}

    NetDeviceIdentity identity_{};

    friend auto netdev_try_retain(NetDeviceIdentity identity) -> NetDeviceRef;
};

struct NetDeviceRetireToken {
    NetDeviceIdentity identity{};

    [[nodiscard]] auto valid() const -> bool { return identity.valid(); }
};

struct NetDeviceSnapshot {
    std::array<char, NETDEV_NAME_LEN> name{};
    proto::MacAddress mac{};
    uint32_t mtu = 0;
    uint32_t tx_queue_len = 0;
    uint32_t link_flags = 0;
    uint32_t ifindex = 0;
    uint8_t state = 0;
    bool remotable = false;
    bool wki_rx_forward = false;
    bool wki_transport = false;
    uint64_t rx_packets = 0;
    uint64_t tx_packets = 0;
    uint64_t rx_bytes = 0;
    uint64_t tx_bytes = 0;
    uint64_t rx_dropped = 0;
    uint64_t tx_dropped = 0;
};

// Coordinates registry membership with publication into dependent registries.
// Callers may acquire other registry locks while this lease is held, but the
// global order must remain netdevice registry before the dependent registry.
class NetDeviceRegistryLease {
   public:
    NetDeviceRegistryLease();
    ~NetDeviceRegistryLease();

    NetDeviceRegistryLease(const NetDeviceRegistryLease&) = delete;
    auto operator=(const NetDeviceRegistryLease&) -> NetDeviceRegistryLease& = delete;
    NetDeviceRegistryLease(NetDeviceRegistryLease&&) = delete;
    auto operator=(NetDeviceRegistryLease&&) -> NetDeviceRegistryLease& = delete;

    [[nodiscard]] auto contains(const NetDevice* dev) const -> bool;

   private:
    uint64_t irq_flags_ = 0;
};

// Register a new network device (assigns ifindex, auto-names "ethN" if name is empty)
auto netdev_register(NetDevice* dev) -> int;

// Unregister and drain retained users. This is a task-context operation.
auto netdev_unregister(NetDevice* dev) -> int;

// Two-phase retirement is available to teardown paths that must first unpublish
// the device, release their own retained objects, and only then wait. Neither
// phase waits with the registry lock held; netdev_unregister_wait() must run in
// task context.
auto netdev_unregister_begin(NetDevice* dev, NetDeviceRetireToken& token) -> int;
void netdev_unregister_wait(const NetDeviceRetireToken& token);

// Generation-qualified registration retention. A stale identity never admits
// a reader after storage at the same address has been registered again.
auto netdev_registered_identity(NetDevice* dev) -> NetDeviceIdentity;
auto netdev_try_retain(NetDeviceIdentity identity) -> NetDeviceRef;
auto netdev_retain_registered(NetDevice* dev) -> NetDeviceRef;
void netdev_release_identity(NetDeviceIdentity identity);

// Lookup
auto netdev_find_by_name(std::string_view name) -> NetDevice*;
auto netdev_find_by_name_ref(std::string_view name) -> NetDeviceRef;
auto netdev_is_registered(const NetDevice* dev) -> bool;
auto netdev_count() -> size_t;
auto netdev_at(size_t i) -> NetDevice*;
auto netdev_at_ref(size_t i) -> NetDeviceRef;
auto netdev_snapshot(NetDeviceSnapshot* out, size_t max) -> size_t;

// Called by drivers when a packet is received
void netdev_rx(NetDevice* dev, PacketBuffer* pkt);

}  // namespace ker::net
