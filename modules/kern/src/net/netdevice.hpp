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

namespace ker::mod::security {
class Snapshot;
}

namespace ker::net {

constexpr size_t MAX_NET_DEVICES = 16;
constexpr size_t NETDEV_NAME_LEN = 16;

struct NetDevice;
using WkiRxForwardHook = void (*)(NetDevice* dev, PacketBuffer* pkt);

static_assert(std::atomic<WkiRxForwardHook>::is_always_lock_free, "WKI RX forward hook must stay lock-free");

struct NetDeviceLinkControl {
    bool change_state = false;
    bool up = false;
    bool change_mac = false;
    std::array<uint8_t, 6> mac{};
};

// Owned link configuration; statistics and L3 address tables have separate
// lifetimes. Readers see the pre-control record until a request finishes.
struct NetDeviceConfigSnapshot {
    std::array<char, NETDEV_NAME_LEN> name{};
    proto::MacAddress mac{};
    uint32_t mtu = 0;
    uint32_t tx_queue_len = 0;
    uint32_t link_flags = 0;
    uint32_t ifindex = 0;
    uint8_t state = 0;
    auto operator==(const NetDeviceConfigSnapshot&) const -> bool = default;
};

enum class NetDeviceControlNotify : uint8_t { OWNER_STATE = 1, RESOURCE_ADVERT = 2 };

// Pure admission. Zero guarantees the native set_mac commit cannot refuse
// while this retained device and its immutable driver configuration remain live.
auto netdev_check_mac(NetDevice* dev, const uint8_t* mac) -> int;
auto netdev_check_fixed_mac(NetDevice* dev, const uint8_t* mac) -> int;

struct NetDeviceOps {
    // Return zero on success. On failure, return a negative errno and leave state unchanged.
    int (*open)(NetDevice* dev);
    void (*close)(NetDevice* dev);
    int (*start_xmit)(NetDevice* dev, PacketBuffer* pkt);
    void (*set_mac)(NetDevice* dev, const uint8_t* mac);
    // Optional: pin NAPI worker for queue pair `pair_idx` to `cpu` and steer its MSI-X entry.
    // Returns 0 on success, -EINVAL if pair_idx is out of range, -ENOSYS if unsupported.
    int (*set_queue_cpu)(NetDevice* dev, uint32_t pair_idx, uint64_t cpu);
    // Optional remote admission using the syscall's already captured identity.
    int (*open_as)(NetDevice* dev, const ker::mod::security::Snapshot& caller) = nullptr;
    // Same caller contract; failure leaves device state unchanged.
    int (*close_as)(NetDevice* dev, const ker::mod::security::Snapshot& caller) = nullptr;
    int (*check_mac)(NetDevice* dev, const uint8_t* mac) = nullptr;
    // One remote decision/commit for the driver effects in a combined request.
    int (*apply_link_as)(NetDevice* dev, const NetDeviceLinkControl& control, const ker::mod::security::Snapshot& caller) = nullptr;
    // Bounded metadata publication under control_publication_lock, before
    // releasing control_busy. Must not allocate, wait, send or invoke hardware.
    void (*finish_control)(NetDevice* dev) = nullptr;
    // Whole-request admission: errors must leave all queues and IRQs unchanged.
    int (*set_queue_cpu_mask)(NetDevice* dev, uint64_t cpu_mask) = nullptr;
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

    // The short lock coordinates admission/release with asynchronous metadata
    // publication. Never hold it across a driver callback or remote wait.
    mutable mod::sys::Spinlock control_publication_lock;
    std::atomic<bool> control_busy{false};
    bool control_retiring = false;  // Protected by control_publication_lock.
    NetDeviceConfigSnapshot control_before{};
    uint8_t control_notifications = 0;  // Requested notification kinds, under publication lock.
};

// Caller retains the registration or otherwise owns this device's storage.
auto netdev_config_snapshot(const NetDevice* dev) -> NetDeviceConfigSnapshot;
// Fence future controls and publish DOWN without racing an admitted writer.
// Does not unregister or wait; successful fresh registration resets the fence.
void netdev_mark_control_retiring(NetDevice* dev);
// Preserve each caller's notification scope while delaying publication until
// its complete configuration is visible. False means publish normally now.
auto netdev_defer_control_notify(NetDevice* dev, NetDeviceControlNotify kind) -> bool;

static_assert(std::atomic<uint64_t>::is_always_lock_free, "NetDevice generations must stay lock-free");
static_assert(std::atomic<uint32_t>::is_always_lock_free, "NetDevice retention must stay lock-free");
static_assert(std::atomic<bool>::is_always_lock_free, "NetDevice control admission must stay lock-free");

// Caller retains the device (or its owner binding) until after destruction.
// One ownership try only: an active request returns EBUSY. The publication
// lock covers only admission/release, never the lifetime of this guard.
// Keep this guard across admission, callbacks, commit and control notifications.
class NetDeviceControlGuard {
   public:
    NetDeviceControlGuard() = default;
    ~NetDeviceControlGuard();
    NetDeviceControlGuard(const NetDeviceControlGuard&) = delete;
    auto operator=(const NetDeviceControlGuard&) -> NetDeviceControlGuard& = delete;

    auto try_acquire(NetDevice* dev) -> int;
    // Finish and flush before an owner response; destruction is idempotent.
    void finish();

   private:
    NetDevice* device_ = nullptr;
};

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

// Bounded namespace admission held across a potentially failing driver call.
// The old name stays visible until commit. Destruction releases an uncommitted
// claim; the retained registration keeps storage alive through retirement.
class NetDeviceNameChange {
   public:
    NetDeviceNameChange() = default;
    ~NetDeviceNameChange();
    NetDeviceNameChange(const NetDeviceNameChange&) = delete;
    auto operator=(const NetDeviceNameChange&) -> NetDeviceNameChange& = delete;

    auto prepare(NetDeviceIdentity identity, std::string_view name) -> int;
    void commit();
    void cancel();

   private:
    void finish(bool publish);
    NetDeviceRef retained_{};
    size_t slot_ = MAX_NET_DEVICES;
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

// Caller retains the registration. Collision admission and publication share
// the registry lock with register/lookup/retirement; callbacks run elsewhere.
auto netdev_rename(NetDeviceIdentity identity, std::string_view name) -> int;

// Unregister and drain retained users. This is a task-context operation.
auto netdev_unregister(NetDevice* dev) -> int;

// Two-phase retirement is available to teardown paths that must first unpublish
// the device, release their own retained objects, and only then wait. begin()
// also retires route/interface and ARP/NDP state so queued packets cannot pin
// the retiring registration. Neither phase waits with the registry lock held;
// netdev_unregister_wait() must run in task context.
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
