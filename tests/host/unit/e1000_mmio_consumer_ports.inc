#pragma once

// Finite network/packet/IRQ endpoints for the complete actual e1000 consumer.
// The shared fixture supplies real PCI/MM/DMA/controller implementations. None
// of these endpoints creates a claim, MM owner, Borrow or quiescence witness.
#include <array>
#include <atomic>
#include <cassert>
#include <cstdint>
#include <new>
#include <platform/sys/spinlock.hpp>
#include <unordered_set>
#include <vector>

namespace ker::net {
struct NetDevice;
struct PacketBuffer;
namespace wki {
struct RemotableOps {
    bool (*can_remote)();
    bool (*can_share)();
    bool (*can_passthrough)();
    int (*on_remote_attach)(uint16_t);
    void (*on_remote_detach)(uint16_t);
    void (*on_remote_fault)(uint16_t);
};
}  // namespace wki
struct NetDeviceOps {
    int (*open)(NetDevice*);
    void (*close)(NetDevice*);
    int (*start_xmit)(NetDevice*, PacketBuffer*);
    void (*set_mac)(NetDevice*, const uint8_t*);
    int (*set_queue_cpu)(NetDevice*, uint32_t, uint64_t);
    int (*check_mac)(NetDevice*, const uint8_t*);
};
struct NetDevice {
    std::array<uint8_t, 6> mac{};
    uint32_t mtu{};
    uint8_t state{};
    const NetDeviceOps* ops{};
    void* private_data{};
    const wki::RemotableOps* remotable{};
    uint64_t rx_dropped{}, tx_packets{}, tx_bytes{};
};
constexpr size_t PKT_BUF_SIZE = 10240;
struct PacketBuffer {
    std::array<uint8_t, PKT_BUF_SIZE> storage{};
    uint8_t* data{storage.data()};
    size_t len{};
    NetDevice* dev{};
};
enum class NapiState : uint8_t { IDLE, SCHEDULED, POLLING, DISABLED };
enum class NapiScheduleSource : uint8_t { UNSPECIFIED, IRQ, REARM, STARTUP, COUNT };
enum class NapiPollSource : uint8_t { WORKER, INLINE, RESCUE, COUNT };
struct NapiStruct;
using NapiPollFn = int (*)(NapiStruct*, int, NapiPollSource);
struct NapiStruct {
    NetDevice* dev{};
    NapiPollFn poll{};
    std::atomic<NapiState> state{NapiState::DISABLED};
    int weight{};
    ker::mod::sched::task::Task* worker{};
    std::atomic<bool> has_work{};
};
struct NetDeviceRetireToken {};
}  // namespace ker::net

namespace e1000_host {
// Explicit finite host geometry, not native BAR observations or authority.
inline constexpr uint64_t OBSERVED_PROFILE_BAR3 = 0x810a0000;
inline constexpr size_t OBSERVED_PROFILE_BAR3_BYTES = 16 * 1024;
struct RegisterEffect {
    bool write;
    size_t offset;
    uint32_t value;
};
using IrqFn = void (*)(uint8_t, void*);
struct Irq {
    IrqFn callback{};
    void* context{};
};
inline std::array<Irq, 256> irqs{};
inline std::unordered_set<ker::net::PacketBuffer*> packets;
inline std::vector<RegisterEffect> register_effects;
inline ker::net::NetDevice* published{};
inline ker::net::NapiStruct* napi{};
inline ker::mod::sched::task::Task napi_worker;
inline size_t packet_budget{SIZE_MAX}, packet_frees{}, rx_delivered{}, unregisters{}, irq_drains{}, vector_frees{};
inline bool register_failed{}, irq_request_failed{}, reset_stuck{}, master_stuck{}, interrupt_mask_stuck{};
inline bool network_register_failed{}, napi_start_failed{};
inline void (*yield_hook)(){};
inline uint64_t milliseconds{};

// Called only by the shared MM hardware port after its actual Runtime has
// checked a genuine Borrow, generation, phase, bounds and canonical target VA.
inline auto read_register(size_t offset) -> uint32_t {
    assert((offset & 3U) == 0 && offset / 4 < pci_cold_host::registers.size());
    auto value = pci_cold_host::registers[offset / 4];
    if (offset == 8 && master_stuck) value |= 1U << 19;
    register_effects.push_back({false, offset, value});
    if (offset == 0xc0) pci_cold_host::registers[offset / 4] = 0;  // Actual ICR read-to-clear hardware semantics.
    return value;
}
inline void write_register(size_t offset, uint32_t value) {
    assert((offset & 3U) == 0 && offset / 4 < pci_cold_host::registers.size());
    register_effects.push_back({true, offset, value});
    auto& word = pci_cold_host::registers[offset / 4];
    if (offset == 0xd8) {
        if (!interrupt_mask_stuck) pci_cold_host::registers[0xd0 / 4] &= ~value;
        word = value;
    } else if (offset == 0xd0) {
        word |= value;
    } else {
        word = offset == 0 && !reset_stuck ? value & ~(1U << 26) : value;
    }
}

inline auto packet() -> ker::net::PacketBuffer* {
    if (packet_budget == 0) return nullptr;
    if (packet_budget != SIZE_MAX) --packet_budget;
    auto* result = new (std::nothrow) ker::net::PacketBuffer;
    if (result != nullptr) assert(packets.insert(result).second);
    return result;
}
inline void invoke_irq(uint8_t vector) {
    assert(irqs[vector].callback != nullptr);
    irqs[vector].callback(vector, irqs[vector].context);
}
}  // namespace e1000_host

namespace ker::net {
inline auto pkt_alloc_rx() -> PacketBuffer* { return e1000_host::packet(); }
inline void pkt_free(PacketBuffer* packet) {
    if (packet == nullptr) return;
    assert(e1000_host::packets.erase(packet) == 1);
    ++e1000_host::packet_frees;
    delete packet;
}
inline void pkt_pool_reserve_for_rx_descriptors(size_t count) { assert(count == 256); }
inline void netdev_rx(NetDevice*, PacketBuffer* packet) {
    ++e1000_host::rx_delivered;
    pkt_free(packet);
}
inline auto netdev_check_fixed_mac(NetDevice*, const uint8_t*) -> int { return -1; }
inline auto netdev_register(NetDevice* device) -> int {
    if (e1000_host::network_register_failed) return -1;
    assert(e1000_host::published == nullptr);
    e1000_host::published = device;
    return 0;
}
inline void netdev_mark_control_retiring(NetDevice*) {}
inline auto netdev_unregister_begin(NetDevice* device, NetDeviceRetireToken&) -> int {
    assert(e1000_host::published == device);
    e1000_host::published = nullptr;
    ++e1000_host::unregisters;
    return 0;
}
inline void napi_init(NapiStruct* napi, NetDevice* device, NapiPollFn poll, int weight) {
    napi->dev = device;
    napi->poll = poll;
    napi->weight = weight;
    e1000_host::napi = napi;
}
inline void napi_enable(NapiStruct* napi) {
    if (e1000_host::napi_start_failed) return;
    // Finite external NAPI endpoint only. This actual port object is never
    // installed as current, scheduled, or used to issue a Controller.
    napi->worker = &e1000_host::napi_worker;
    napi->state.store(NapiState::IDLE);
}
inline auto napi_schedule(NapiStruct* napi, NapiScheduleSource) -> bool {
    napi->state.store(NapiState::SCHEDULED);
    napi->has_work.store(true);
    return true;
}
inline void napi_complete(NapiStruct* napi) { napi->state.store(NapiState::IDLE); }
}  // namespace ker::net

namespace ker::mod::ioapic {
inline void route_irq(uint8_t, uint8_t, uint32_t) {}
}  // namespace ker::mod::ioapic
namespace ker::mod::gates {
inline auto allocate_vector() -> uint8_t { return 64; }
inline auto request_irq(uint8_t vector, e1000_host::IrqFn callback, void* context, const char*) -> int {
    if (e1000_host::irq_request_failed) return -1;
    assert(e1000_host::irqs[vector].callback == nullptr);
    e1000_host::irqs[vector] = {callback, context};
    return 0;
}
inline void free_irq(uint8_t vector) {
    if (e1000_host::irqs[vector].callback != nullptr) ++e1000_host::irq_drains;
    e1000_host::irqs[vector] = {};
    ++e1000_host::vector_frees;
}
}  // namespace ker::mod::gates
namespace ker::mod::time {
inline auto get_ms() -> uint64_t { return ++e1000_host::milliseconds; }
inline void sleep_us(uint64_t us) { e1000_host::milliseconds += (us + 999) / 1000; }
}  // namespace ker::mod::time
namespace ker::mod::sched {
inline void kern_yield() {
    if (e1000_host::yield_hook != nullptr) e1000_host::yield_hook();
}
}  // namespace ker::mod::sched
