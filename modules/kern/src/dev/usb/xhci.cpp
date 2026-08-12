#include "xhci.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <dev/usb/xhci_lifecycle.hpp>
#include <dev/usb/xhci_request.hpp>
#include <limits>
#include <new>  // IWYU pragma: keep
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/page_alloc.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <span>

#include "dev/pci.hpp"
#include "util/hcf.hpp"

namespace ker::dev::usb {

using log = ker::mod::dbg::logger<"xhci">;
namespace lifecycle = xhci_lifecycle;
namespace request = xhci_request;

constexpr size_t MAX_XHCI_CONTROLLERS = 2;
// NOLINTBEGIN(misc-use-internal-linkage)
std::array<XhciController*, MAX_XHCI_CONTROLLERS> controllers{};
std::atomic<size_t> controller_count{0};
// NOLINTEND(misc-use-internal-linkage)

namespace {

constexpr size_t PAGE_SIZE = 4096;
constexpr size_t REQUEST_CAPACITY = 128;
constexpr uint64_t COMMAND_TIMEOUT_US = 2'000'000;
constexpr uint64_t CONTROL_TIMEOUT_US = 2'000'000;
constexpr uint64_t BULK_TIMEOUT_US = 5'000'000;
constexpr uint32_t MAX_TRANSFER_LENGTH = 0x1FFFF;
constexpr int REQUEST_TIMEOUT_RESULT = -ETIMEDOUT;

using RequestTable = request::FixedRequestTable<REQUEST_CAPACITY, 4>;

UsbClassDriver* class_drivers = nullptr;
mod::sched::task::Task* usb_worker_task = nullptr;
std::atomic<bool> usb_worker_pending{false};

struct Alloc {
    void* virt{};
    uint64_t phys{};
};

struct CommandResult {
    int status{-EIO};
    uint8_t slot_id{};
};

enum class EnumerationResult : uint8_t {
    SUCCESS,
    REUSABLE_FAILURE,
    QUARANTINED,
};

auto read32(const volatile uint8_t* base, uint32_t offset) -> uint32_t {
    return *reinterpret_cast<const volatile uint32_t*>(base + offset);
}

void write32(volatile uint8_t* base, uint32_t offset, uint32_t value) { *reinterpret_cast<volatile uint32_t*>(base + offset) = value; }

void write64(volatile uint8_t* base, uint32_t offset, uint64_t value) { *reinterpret_cast<volatile uint64_t*>(base + offset) = value; }

auto virt_to_phys(void* value) -> uint64_t {
    auto const ADDRESS = reinterpret_cast<uint64_t>(value);
    if (ADDRESS >= 0xffffffff80000000ULL) {
        uint64_t const PHYS = mod::mm::virt::translate(mod::mm::virt::get_kernel_pagemap(), ADDRESS);
        if (PHYS == mod::mm::virt::PADDR_INVALID) {
            log::error("virt_to_phys failed for kernel address 0x%lx", ADDRESS);
            hcf();
        }
        return PHYS;
    }
    return reinterpret_cast<uint64_t>(mod::mm::addr::get_phys_pointer(ADDRESS));
}

auto alloc_pages(size_t bytes, const char* name = "xhci_dma") -> Alloc {
    size_t const ALLOC_BYTES = ((std::max(bytes, size_t{1}) + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE;
    void* storage = mod::mm::phys::page_alloc(mod::mm::PhysicalPageOwner::DEVICE_USB, ALLOC_BYTES, name);
    if (storage == nullptr) {
        return {};
    }
    std::memset(storage, 0, ALLOC_BYTES);
    return {.virt = storage, .phys = virt_to_phys(storage)};
}

auto alloc_page(const char* name = "xhci_page") -> Alloc { return alloc_pages(PAGE_SIZE, name); }

void free_page(void*& storage) {
    if (storage == nullptr) {
        return;
    }
    mod::mm::phys::page_free(storage);
    storage = nullptr;
}

auto request_table(XhciController* hc) -> RequestTable& { return *static_cast<RequestTable*>(hc->request_table); }

auto context_stride(const XhciController* hc) -> size_t { return hc->ctx64 ? 64U : 32U; }

auto input_drop_flags(UsbDevice& dev) -> uint32_t& { return *static_cast<uint32_t*>(dev.input_ctx); }

auto input_add_flags(UsbDevice& dev) -> uint32_t& { return *(static_cast<uint32_t*>(dev.input_ctx) + 1); }

auto input_slot_context(UsbDevice& dev) -> SlotContext* {
    auto* bytes = static_cast<uint8_t*>(dev.input_ctx);
    return reinterpret_cast<SlotContext*>(bytes + context_stride(dev.controller));
}

auto input_endpoint_context(UsbDevice& dev, uint8_t dci) -> EndpointContext* {
    auto* bytes = static_cast<uint8_t*>(dev.input_ctx);
    return reinterpret_cast<EndpointContext*>(bytes + (context_stride(dev.controller) * (static_cast<size_t>(dci) + 1U)));
}

auto output_slot_context(UsbDevice& dev) -> SlotContext* { return static_cast<SlotContext*>(dev.dev_ctx); }

void clear_input_context(UsbDevice& dev) { std::memset(dev.input_ctx, 0, PAGE_SIZE); }

auto ep_dci(uint8_t endpoint_address) -> uint8_t {
    uint8_t const NUMBER = endpoint_address & USB_EP_ADDR_MASK;
    if (NUMBER == 0) {
        return 1;
    }
    return (endpoint_address & USB_EP_DIR_IN) != 0 ? static_cast<uint8_t>((2U * NUMBER) + 1U) : static_cast<uint8_t>(2U * NUMBER);
}

auto max_packet_for_speed(uint8_t speed) -> uint16_t {
    switch (speed) {
        case USB_SPEED_LOW:
            return 8;
        case USB_SPEED_FULL:
        case USB_SPEED_HIGH:
            return 64;
        case USB_SPEED_SUPER:
            return 512;
        default:
            return 64;
    }
}

auto ring_entry_phys(uint64_t ring_phys, size_t index) -> uint64_t { return ring_phys + (index * sizeof(Trb)); }

auto next_ring_data_index(size_t index, size_t ring_size) -> size_t {
    ++index;
    return index >= ring_size - 1 ? 0 : index;
}

template <size_t Count>
auto plan_ring_entries(uint64_t ring_phys, size_t enqueue, size_t ring_size) -> std::array<uint64_t, Count> {
    std::array<uint64_t, Count> identities{};
    for (size_t i = 0; i < Count; ++i) {
        identities.at(i) = ring_entry_phys(ring_phys, enqueue);
        enqueue = next_ring_data_index(enqueue, ring_size);
    }
    return identities;
}

auto ring_enqueue(Trb* ring, uint64_t ring_phys, size_t* enqueue, bool* cycle, size_t ring_size, uint64_t parameter, uint32_t status,
                  uint32_t control) -> uint64_t {
    size_t index = *enqueue;
    uint64_t const IDENTITY = ring_entry_phys(ring_phys, index);
    ring[index].param = parameter;
    ring[index].status = status;
    std::atomic_thread_fence(std::memory_order_release);
    ring[index].control = (control & ~TRB_CYCLE) | (*cycle ? TRB_CYCLE : 0);

    ++index;
    if (index >= ring_size - 1) {
        ring[index].param = ring_phys;
        ring[index].status = 0;
        std::atomic_thread_fence(std::memory_order_release);
        ring[index].control = TRB_LINK | TRB_TOGGLE_CYCLE | (*cycle ? TRB_CYCLE : 0);
        *cycle = !*cycle;
        index = 0;
    }
    *enqueue = index;
    return IDENTITY;
}

void ring_doorbell(volatile uint32_t* doorbells, uint32_t slot, uint32_t target) {
    std::atomic_thread_fence(std::memory_order_release);
    doorbells[slot] = target;
}

void clear_command_handle(XhciController& hc) {
    hc.command_request_generation.store(0, std::memory_order_relaxed);
    hc.command_request_index.store(INVALID_REQUEST_INDEX, std::memory_order_release);
}

auto load_endpoint_handle(const UsbEndpoint& endpoint) -> request::RequestHandle {
    return {
        .index = endpoint.request_index.load(std::memory_order_acquire),
        .generation = endpoint.request_generation.load(std::memory_order_acquire),
    };
}

void clear_endpoint_handle(UsbEndpoint& endpoint) {
    endpoint.request_generation.store(0, std::memory_order_relaxed);
    endpoint.request_index.store(INVALID_REQUEST_INDEX, std::memory_order_release);
}

auto wait_for_request(RequestTable& requests, request::RequestHandle handle, uint64_t deadline_us, request::RequestSnapshot& snapshot)
    -> int {
    for (;;) {
        if (!requests.snapshot(handle, snapshot)) {
            return -EIO;
        }
        if (snapshot.state == request::RequestState::COMPLETED) {
            return snapshot.result;
        }
        if (snapshot.state == request::RequestState::TIMED_OUT || snapshot.state == request::RequestState::RETIRED) {
            return snapshot.result;
        }

        uint64_t const NOW = mod::time::get_us();
        if (NOW >= deadline_us) {
            auto const RESULT = requests.try_timeout(handle, NOW, REQUEST_TIMEOUT_RESULT);
            if (RESULT == request::TimeoutDisposition::TIMED_OUT || RESULT == request::TimeoutDisposition::ALREADY_TIMED_OUT) {
                static_cast<void>(requests.snapshot(handle, snapshot));
                return REQUEST_TIMEOUT_RESULT;
            }
        }
        mod::sched::kern_yield();
    }
}

auto acquire_command(XhciController& hc) -> bool {
    while (!hc.command_quarantined.load(std::memory_order_acquire)) {
        bool expected = false;
        if (hc.command_busy.compare_exchange_weak(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
            return true;
        }
        mod::sched::kern_yield();
    }
    return false;
}

auto send_command_result(XhciController* hc, uint64_t parameter, uint32_t status, uint32_t control) -> CommandResult {
    if (hc == nullptr || hc->request_table == nullptr || !acquire_command(*hc)) {
        return {.status = -EIO};
    }

    uint64_t const DEADLINE = mod::time::get_us() + COMMAND_TIMEOUT_US;
    request::RequestHandle handle{};
    {
        uint64_t const FLAGS = hc->cmd_lock.lock_irqsave();
        std::array<uint64_t, 1> const IDENTITIES = {ring_entry_phys(hc->cmd_ring_phys, hc->cmd_enqueue)};
        handle = request_table(hc).reserve({
            .kind = request::RequestKind::COMMAND,
            .trb_phys = IDENTITIES,
            .deadline_us = DEADLINE,
        });
        if (!handle.valid()) {
            hc->cmd_lock.unlock_irqrestore(FLAGS);
            hc->command_busy.store(false, std::memory_order_release);
            return {.status = -ENOMEM};
        }

        hc->command_request_generation.store(handle.generation, std::memory_order_relaxed);
        hc->command_request_index.store(handle.index, std::memory_order_release);
        static_cast<void>(
            ring_enqueue(hc->cmd_ring, hc->cmd_ring_phys, &hc->cmd_enqueue, &hc->cmd_cycle, CMD_RING_SIZE, parameter, status, control));
        ring_doorbell(hc->db, 0, 0);
        hc->cmd_lock.unlock_irqrestore(FLAGS);
    }

    request::RequestSnapshot snapshot{};
    int const RESULT = wait_for_request(request_table(hc), handle, DEADLINE, snapshot);
    if (snapshot.state == request::RequestState::TIMED_OUT) {
        // Command-ring recovery is controller-wide. Until a future reset path
        // proves stale command pointers retired, no command address may be
        // reused and all slot DMA remains quarantined.
        hc->command_quarantined.store(true, std::memory_order_release);
        return {.status = RESULT};
    }

    static_cast<void>(request_table(hc).release(handle));
    clear_command_handle(*hc);
    hc->command_busy.store(false, std::memory_order_release);
    return {.status = RESULT, .slot_id = snapshot.event_slot_id};
}

auto send_command(XhciController* hc, uint64_t parameter, uint32_t status, uint32_t control) -> int {
    return send_command_result(hc, parameter, status, control).status;
}

auto enable_slot(XhciController* hc) -> int {
    CommandResult const RESULT = send_command_result(hc, 0, 0, TRB_ENABLE_SLOT);
    return RESULT.status == 0 ? static_cast<int>(RESULT.slot_id) : RESULT.status;
}

auto disable_slot(XhciController* hc, uint8_t slot_id) -> int {
    return send_command(hc, 0, 0, TRB_DISABLE_SLOT | (static_cast<uint32_t>(slot_id) << 24U));
}

auto address_device(XhciController* hc, uint8_t slot_id, uint64_t input_context_phys) -> int {
    return send_command(hc, input_context_phys, 0, TRB_ADDRESS_DEVICE | (static_cast<uint32_t>(slot_id) << 24U));
}

auto stop_endpoint(XhciController* hc, uint8_t slot_id, uint8_t dci) -> int {
    return send_command(hc, 0, 0, TRB_STOP_ENDPOINT | (static_cast<uint32_t>(dci) << 16U) | (static_cast<uint32_t>(slot_id) << 24U));
}

void setup_ep0_context(UsbDevice& dev, uint16_t max_packet, uint64_t ring_phys) {
    SlotContext* slot = input_slot_context(dev);
    slot->data[0] = (1U << 27U) | (static_cast<uint32_t>(dev.speed) << 20U);
    slot->data[1] = static_cast<uint32_t>(dev.port) << 16U;

    EndpointContext* endpoint = input_endpoint_context(dev, 1);
    endpoint->data[1] = (3U << 1U) | (4U << 3U) | (static_cast<uint32_t>(max_packet) << 16U);
    endpoint->data[2] = static_cast<uint32_t>(ring_phys) | 1U;
    endpoint->data[3] = static_cast<uint32_t>(ring_phys >> 32U);
    endpoint->data[4] = 8;
}

void notify_usb_worker() {
    usb_worker_pending.store(true, std::memory_order_release);
    if (usb_worker_task != nullptr) {
        mod::sched::wake_task_from_event(usb_worker_task);
    }
}

void queue_port_snapshot(XhciController& hc, uint8_t port_id, uint32_t portsc) {
    if (port_id == 0 || port_id > hc.max_ports) {
        return;
    }
    auto& port = hc.ports.at(port_id);
    uint64_t const FLAGS = port.event_lock.lock_irqsave();
    port.latest_portsc.store(portsc, std::memory_order_release);
    port.pending_changes.fetch_or(portsc & lifecycle::PORTSC_CHANGE_MASK, std::memory_order_acq_rel);
    port.event_sequence.fetch_add(1, std::memory_order_acq_rel);
    if ((portsc & XHCI_PORTSC_CSC) != 0U) {
        port.event_generation.fetch_add(1, std::memory_order_acq_rel);
    }
    port.event_lock.unlock_irqrestore(FLAGS);
    hc.pending_ports.fetch_or(uint32_t{1} << (port_id - 1U), std::memory_order_acq_rel);
    notify_usb_worker();
}

void process_event(void* opaque, const Trb& event) {
    auto* hc = static_cast<XhciController*>(opaque);
    if (hc == nullptr) {
        return;
    }
    uint32_t const TYPE = event.control & TRB_TYPE_MASK;
    if (TYPE == TRB_CMD_COMPLETION || TYPE == TRB_TRANSFER_EVENT) {
        static_cast<void>(request_table(hc).complete_event(event));
        return;
    }
    if (TYPE != TRB_PORT_STATUS_CHG) {
        return;
    }

    uint8_t const PORT_ID = static_cast<uint8_t>((event.param >> 24U) & 0xFFU);
    if (PORT_ID == 0 || PORT_ID > hc->max_ports) {
        return;
    }
    uint32_t const OFFSET = XHCI_OP_PORTSC + ((PORT_ID - 1U) * 0x10U);
    uint32_t const PORTSC = read32(hc->op, OFFSET);
    write32(hc->op, OFFSET, lifecycle::compose_portsc_ack_write(PORTSC));
    queue_port_snapshot(*hc, PORT_ID, PORTSC);
}

void process_events(XhciController& hc) {
    request::EventRingCursor cursor{.dequeue = hc.evt_dequeue, .cycle = hc.evt_cycle};
    auto const DRAIN = request::consume_event_ring(hc.evt_ring, EVENT_RING_SIZE, cursor, EVENT_RING_SIZE, process_event, &hc);
    hc.evt_dequeue = cursor.dequeue;
    hc.evt_cycle = cursor.cycle;
    uint64_t const ERDP = hc.evt_ring_phys + (hc.evt_dequeue * sizeof(Trb));
    write64(hc.rt, XHCI_RT_ERDP, ERDP | (uint64_t{1} << 3U));
    (void)DRAIN;
}

void xhci_irq(uint8_t /*vector*/, void* opaque) {
    auto* hc = static_cast<XhciController*>(opaque);
    if (hc == nullptr) {
        return;
    }

    uint32_t const STATUS = read32(hc->op, XHCI_OP_USBSTS);
    uint32_t const ACK = STATUS & (XHCI_STS_EINT | XHCI_STS_PCD);
    if (ACK == 0) {
        return;
    }
    write32(hc->op, XHCI_OP_USBSTS, ACK);
    uint32_t const IMAN = read32(hc->rt, XHCI_RT_IMAN);
    write32(hc->rt, XHCI_RT_IMAN, IMAN | XHCI_IMAN_IP);
    if ((STATUS & XHCI_STS_EINT) != 0U) {
        process_events(*hc);
    }
}

auto generation_still_connected(XhciController& hc, uint8_t port_id, uint64_t generation) -> bool {
    if (hc.ports.at(port_id).event_generation.load(std::memory_order_acquire) != generation) {
        return false;
    }
    uint32_t const PORTSC = read32(hc.op, XHCI_OP_PORTSC + ((port_id - 1U) * 0x10U));
    return (PORTSC & XHCI_PORTSC_CCS) != 0U;
}

auto configuration_has_class_driver(UsbDevice& dev, uint8_t* config_data, size_t config_length) -> bool {
    size_t offset = 0;
    while (offset + 2 <= config_length) {
        uint8_t const LENGTH = config_data[offset];
        uint8_t const TYPE = config_data[offset + 1];
        if (LENGTH < 2 || offset + LENGTH > config_length) {
            return false;
        }
        if (TYPE == USB_DESC_INTERFACE && LENGTH >= sizeof(UsbInterfaceDescriptor)) {
            auto* interface = reinterpret_cast<UsbInterfaceDescriptor*>(config_data + offset);
            for (auto* driver = class_drivers; driver != nullptr; driver = driver->next) {
                if (driver->probe(&dev, interface)) {
                    return true;
                }
            }
        }
        offset += LENGTH;
    }
    return false;
}

auto probe_class_driver(UsbDevice& dev, uint8_t* config_data, size_t config_length) -> int {
    size_t offset = 0;
    while (offset + 2 <= config_length) {
        uint8_t const LENGTH = config_data[offset];
        uint8_t const TYPE = config_data[offset + 1];
        if (LENGTH < 2 || offset + LENGTH > config_length) {
            return -EINVAL;
        }
        if (TYPE == USB_DESC_INTERFACE && LENGTH >= sizeof(UsbInterfaceDescriptor)) {
            auto* interface = reinterpret_cast<UsbInterfaceDescriptor*>(config_data + offset);
            for (auto* driver = class_drivers; driver != nullptr; driver = driver->next) {
                if (!driver->probe(&dev, interface)) {
                    continue;
                }
                int const RESULT = driver->attach(&dev, interface, config_data, config_length);
                if (RESULT != 0) {
                    return RESULT;
                }
                dev.bound_driver = driver;
                dev.enumeration_stage = lifecycle::EnumerationStage::DRIVER_BOUND;
                return 0;
            }
        }
        offset += LENGTH;
    }
    return 0;
}

auto read_configuration(XhciController& hc, UsbDevice& dev, uint8_t config_index, std::array<uint8_t, 256>& buffer, size_t& config_length)
    -> int {
    buffer.fill(0);
    config_length = 0;
    UsbSetupPacket setup = {
        .bm_request_type = 0x80,
        .b_request = USB_REQ_GET_DESCRIPTOR,
        .w_value = static_cast<uint16_t>((USB_DESC_CONFIG << 8U) | config_index),
        .w_index = 0,
        .w_length = sizeof(UsbConfigDescriptor),
    };
    int result = xhci_control_transfer(&hc, dev.slot_id, &setup, buffer.data(), sizeof(UsbConfigDescriptor), true);
    if (result != 0) {
        return result;
    }

    auto const* header = reinterpret_cast<const UsbConfigDescriptor*>(buffer.data());
    size_t const TOTAL_LENGTH = header->w_total_length;
    if (header->b_length < sizeof(UsbConfigDescriptor) || header->b_descriptor_type != USB_DESC_CONFIG ||
        TOTAL_LENGTH < sizeof(UsbConfigDescriptor) || TOTAL_LENGTH > buffer.size()) {
        return -EINVAL;
    }

    setup.w_length = static_cast<uint16_t>(TOTAL_LENGTH);
    result = xhci_control_transfer(&hc, dev.slot_id, &setup, buffer.data(), TOTAL_LENGTH, true);
    if (result == 0) {
        config_length = TOTAL_LENGTH;
    }
    return result;
}

void mark_endpoint_request_cancelled(XhciController& hc, UsbEndpoint& endpoint) {
    if (!endpoint.request_busy.load(std::memory_order_acquire)) {
        return;
    }
    request::RequestHandle const HANDLE = load_endpoint_handle(endpoint);
    if (!HANDLE.valid()) {
        return;
    }
    static_cast<void>(request_table(&hc).cancel(HANDLE, -ECANCELED));
}

void retire_endpoint_request_after_barrier(XhciController& hc, UsbEndpoint& endpoint) {
    if (!endpoint.request_busy.load(std::memory_order_acquire)) {
        return;
    }
    request::RequestHandle const HANDLE = load_endpoint_handle(endpoint);
    request::RequestSnapshot snapshot{};
    if (HANDLE.valid() && request_table(&hc).snapshot(HANDLE, snapshot)) {
        if (snapshot.state == request::RequestState::TIMED_OUT) {
            static_cast<void>(request_table(&hc).mark_timed_out_retired(HANDLE));
        }
        static_cast<void>(request_table(&hc).release(HANDLE));
    }
    clear_endpoint_handle(endpoint);
    if (endpoint.request_dma != nullptr) {
        mod::mm::phys::page_free(endpoint.request_dma);
        endpoint.request_dma = nullptr;
    }
    endpoint.request_dma_len = 0;
    endpoint.request_dir_in = false;
    endpoint.request_busy.store(false, std::memory_order_release);
}

auto drop_non_control_endpoints(XhciController& hc, UsbDevice& dev) -> int {
    uint32_t const DROP = dev.configured_dcis & ~uint32_t{1U << 1U};
    if (DROP == 0) {
        return 0;
    }
    clear_input_context(dev);
    input_drop_flags(dev) = DROP;
    input_add_flags(dev) = 1U;
    *input_slot_context(dev) = *output_slot_context(dev);
    input_slot_context(dev)->data[0] &= ~(0x1FU << 27U);
    input_slot_context(dev)->data[0] |= 1U << 27U;
    return configure_endpoint(&hc, dev.slot_id, dev.input_ctx_phys);
}

auto teardown_device(XhciController& hc, XhciPort& port, UsbDevice& dev) -> bool {
    dev.state.store(UsbDeviceState::DISCONNECTING, std::memory_order_release);
    for (auto& endpoint : dev.endpoints) {
        endpoint.accepting.store(false, std::memory_order_release);
    }
    if (dev.bound_driver != nullptr && dev.bound_driver->quiesce != nullptr && !dev.bound_driver->quiesce(&dev)) {
        dev.state.store(UsbDeviceState::FAILED, std::memory_order_release);
        return false;
    }

    for (auto& endpoint : dev.endpoints) {
        mark_endpoint_request_cancelled(hc, endpoint);
    }

    if (hc.command_quarantined.load(std::memory_order_acquire)) {
        dev.state.store(UsbDeviceState::FAILED, std::memory_order_release);
        return false;
    }

    uint32_t const CONTROLLER_ENDPOINTS = dev.configured_dcis;
    for (uint8_t dci = 1; dci < 32; ++dci) {
        if ((CONTROLLER_ENDPOINTS & (uint32_t{1} << dci)) == 0) {
            continue;
        }
        UsbEndpoint& endpoint = dev.endpoints.at(dci);
        if (endpoint.request_busy.load(std::memory_order_acquire) || dci != 1) {
            static_cast<void>(stop_endpoint(&hc, dev.slot_id, dci));
        }
    }
    static_cast<void>(drop_non_control_endpoints(hc, dev));

    if (disable_slot(&hc, dev.slot_id) != 0) {
        dev.state.store(UsbDeviceState::FAILED, std::memory_order_release);
        return false;
    }

    // Disable Slot is the final controller-ownership barrier for all endpoint
    // requests, contexts, and rings of this slot.  Keep canceled request
    // records and bounce storage intact until class callers have observed the
    // cancellation and dropped their I/O references.
    if (dev.bound_driver != nullptr && dev.bound_driver->detach != nullptr) {
        dev.bound_driver->detach(&dev);
    }
    for (auto& endpoint : dev.endpoints) {
        retire_endpoint_request_after_barrier(hc, endpoint);
    }

    hc.dcbaap[dev.slot_id] = 0;
    std::atomic_thread_fence(std::memory_order_release);
    for (auto& endpoint : dev.endpoints) {
        if (endpoint.ring != nullptr) {
            void* ring = endpoint.ring;
            mod::mm::phys::page_free(ring);
            endpoint.ring = nullptr;
        }
    }
    free_page(dev.input_ctx);
    free_page(dev.dev_ctx);

    uint8_t const SLOT_ID = dev.slot_id;
    dev.~UsbDevice();
    new (&dev) UsbDevice{};
    port.slot_id = 0;
    (void)SLOT_ID;
    return true;
}

auto enumerate_device(XhciController& hc, XhciPort& port, uint8_t port_id, uint8_t speed, uint64_t generation) -> EnumerationResult {
    int const SLOT = enable_slot(&hc);
    if (SLOT <= 0 || SLOT > static_cast<int>(hc.max_slots) || SLOT > static_cast<int>(MAX_XHCI_SLOTS)) {
        log::warn("enable slot failed for port %u", port_id);
        return hc.command_quarantined.load(std::memory_order_acquire) ? EnumerationResult::QUARANTINED
                                                                      : EnumerationResult::REUSABLE_FAILURE;
    }

    auto& dev = hc.devices.at(static_cast<size_t>(SLOT));
    dev.~UsbDevice();
    new (&dev) UsbDevice{};
    dev.controller = &hc;
    dev.slot_id = static_cast<uint8_t>(SLOT);
    dev.port = port_id;
    dev.speed = speed;
    dev.max_packet0 = max_packet_for_speed(speed);
    dev.port_generation = generation;
    dev.enumeration_stage = lifecycle::EnumerationStage::SLOT_ENABLED;
    dev.state.store(UsbDeviceState::ENUMERATING, std::memory_order_release);
    port.slot_id = dev.slot_id;

    auto fail = [&]() -> EnumerationResult {
        if (!teardown_device(hc, port, dev)) {
            log::warn("port %u rollback quarantined at stage %u", port_id, static_cast<unsigned>(dev.enumeration_stage));
            return EnumerationResult::QUARANTINED;
        }
        return EnumerationResult::REUSABLE_FAILURE;
    };

    Alloc device_context = alloc_page("xhci_device_context");
    if (device_context.virt == nullptr) {
        return fail();
    }
    dev.dev_ctx = device_context.virt;
    dev.dev_ctx_phys = device_context.phys;
    dev.enumeration_stage = lifecycle::EnumerationStage::DEVICE_CONTEXT_ALLOCATED;

    hc.dcbaap[dev.slot_id] = device_context.phys;
    std::atomic_thread_fence(std::memory_order_release);
    dev.enumeration_stage = lifecycle::EnumerationStage::DCBAA_PUBLISHED;

    Alloc input_context = alloc_page("xhci_input_context");
    if (input_context.virt == nullptr) {
        return fail();
    }
    dev.input_ctx = input_context.virt;
    dev.input_ctx_phys = input_context.phys;
    dev.enumeration_stage = lifecycle::EnumerationStage::INPUT_CONTEXT_ALLOCATED;

    Alloc ep0_ring = alloc_pages(XFER_RING_SIZE * sizeof(Trb), "xhci_ep0_ring");
    if (ep0_ring.virt == nullptr) {
        return fail();
    }
    UsbEndpoint& ep0 = dev.endpoints.at(1);
    ep0.address = 0;
    ep0.type = USB_EP_TYPE_CONTROL;
    ep0.max_packet = dev.max_packet0;
    ep0.dci = 1;
    ep0.ring = static_cast<Trb*>(ep0_ring.virt);
    ep0.ring_phys = ep0_ring.phys;
    ep0.ring_cycle = true;
    dev.num_endpoints = 1;
    dev.enumeration_stage = lifecycle::EnumerationStage::EP0_RING_ALLOCATED;

    clear_input_context(dev);
    input_add_flags(dev) = (1U << 0U) | (1U << 1U);
    setup_ep0_context(dev, dev.max_packet0, ep0.ring_phys);
    if (address_device(&hc, dev.slot_id, dev.input_ctx_phys) != 0) {
        return fail();
    }
    ep0.configured = true;
    ep0.accepting.store(true, std::memory_order_release);
    dev.configured_dcis = uint32_t{1} << 1U;
    dev.enumeration_stage = lifecycle::EnumerationStage::ADDRESSED;

    if (!generation_still_connected(hc, port_id, generation)) {
        return fail();
    }

    UsbDeviceDescriptor descriptor{};
    UsbSetupPacket setup = {
        .bm_request_type = 0x80,
        .b_request = USB_REQ_GET_DESCRIPTOR,
        .w_value = static_cast<uint16_t>(USB_DESC_DEVICE << 8U),
        .w_index = 0,
        .w_length = sizeof(UsbDeviceDescriptor),
    };
    if (xhci_control_transfer(&hc, dev.slot_id, &setup, &descriptor, sizeof(descriptor), true) != 0 ||
        descriptor.b_length < sizeof(UsbDeviceDescriptor) || descriptor.b_num_configurations == 0) {
        return fail();
    }
    dev.vendor_id = descriptor.id_vendor;
    dev.product_id = descriptor.id_product;
    dev.device_class = descriptor.b_device_class;
    dev.device_subclass = descriptor.b_device_sub_class;
    dev.device_protocol = descriptor.b_device_protocol;

    std::array<uint8_t, 256> config_buffer{};
    size_t config_length = 0;
    bool have_configuration = false;
    for (size_t index = 0; index < descriptor.b_num_configurations; ++index) {
        if (!generation_still_connected(hc, port_id, generation)) {
            return fail();
        }
        std::array<uint8_t, 256> candidate{};
        size_t candidate_length = 0;
        if (read_configuration(hc, dev, static_cast<uint8_t>(index), candidate, candidate_length) != 0) {
            continue;
        }
        if (!have_configuration) {
            config_buffer = candidate;
            config_length = candidate_length;
            have_configuration = true;
        }
        if (configuration_has_class_driver(dev, candidate.data(), candidate_length)) {
            config_buffer = candidate;
            config_length = candidate_length;
            break;
        }
    }
    if (!have_configuration) {
        return fail();
    }
    dev.enumeration_stage = lifecycle::EnumerationStage::DESCRIPTORS_READ;

    if (!generation_still_connected(hc, port_id, generation) || probe_class_driver(dev, config_buffer.data(), config_length) != 0) {
        return fail();
    }

    auto const* full_config = reinterpret_cast<const UsbConfigDescriptor*>(config_buffer.data());
    UsbSetupPacket set_config = {
        .bm_request_type = 0,
        .b_request = USB_REQ_SET_CONFIG,
        .w_value = full_config->b_configuration_value,
        .w_index = 0,
        .w_length = 0,
    };
    if (xhci_control_transfer(&hc, dev.slot_id, &set_config, nullptr, 0, false) != 0) {
        return fail();
    }
    dev.enumeration_stage = lifecycle::EnumerationStage::USB_CONFIGURED;

    if (!generation_still_connected(hc, port_id, generation)) {
        return fail();
    }
    if (dev.bound_driver != nullptr) {
        dev.enumeration_stage = lifecycle::EnumerationStage::DRIVER_BOUND;
        if (dev.bound_driver->publish != nullptr && dev.bound_driver->publish(&dev) != 0) {
            return fail();
        }
    }
    dev.enumeration_stage = lifecycle::EnumerationStage::PUBLISHED;
    dev.state.store(UsbDeviceState::CONFIGURED, std::memory_order_release);
    log::info("port %u slot %u device %04x:%04x configured", port_id, dev.slot_id, dev.vendor_id, dev.product_id);
    return EnumerationResult::SUCCESS;
}

auto consume_port_batch(XhciPort& port) -> lifecycle::PortChangeBatch {
    lifecycle::PortChangeBatch batch{};
    uint64_t const FLAGS = port.event_lock.lock_irqsave();
    batch.sequence = port.event_sequence.load(std::memory_order_acquire);
    batch.generation = port.event_generation.load(std::memory_order_acquire);
    batch.observed_changes = port.pending_changes.exchange(0, std::memory_order_acq_rel);
    batch.latest_portsc = port.latest_portsc.load(std::memory_order_acquire);
    uint8_t const SPEED = static_cast<uint8_t>((batch.latest_portsc & XHCI_PORTSC_SPEED_MASK) >> XHCI_PORTSC_SPEED_SHIFT);
    batch.protocol = SPEED >= XHCI_SPEED_SUPER ? lifecycle::PortProtocol::USB3 : lifecycle::PortProtocol::USB2;
    batch.pending = true;
    port.event_lock.unlock_irqrestore(FLAGS);
    return batch;
}

void handle_port(XhciController& hc, uint8_t port_id) {
    auto& port = hc.ports.at(port_id);
    lifecycle::PortTransition transition{};
    bool recovery_transition = false;
    if (port.recovery_requested.exchange(false, std::memory_order_acq_rel) && port.slot_id != 0) {
        auto& dev = hc.devices.at(port.slot_id);
        port.lifecycle.state = lifecycle::PortState::DISCONNECTING;
        port.lifecycle.reconnect_pending = false;
        auto const DISPOSITION =
            teardown_device(hc, port, dev) ? lifecycle::RollbackDisposition::REUSABLE : lifecycle::RollbackDisposition::QUARANTINED;
        transition = lifecycle::finish_port_rollback(port.lifecycle, DISPOSITION);
        port.lifecycle = transition.port;
        if (DISPOSITION == lifecycle::RollbackDisposition::QUARANTINED) {
            return;
        }
        transition = lifecycle::resume_reusable_port(port.lifecycle);
        port.lifecycle = transition.port;
        recovery_transition = true;
    }

    if (!recovery_transition) {
        lifecycle::PortChangeBatch const BATCH = consume_port_batch(port);
        transition = lifecycle::reconcile_port(port.lifecycle, BATCH);
        port.lifecycle = transition.port;
        port.speed = static_cast<uint8_t>((BATCH.latest_portsc & XHCI_PORTSC_SPEED_MASK) >> XHCI_PORTSC_SPEED_SHIFT);
    }

    for (unsigned step = 0; step < 3; ++step) {
        if (transition.action == lifecycle::PortAction::NONE) {
            break;
        }
        if (transition.action == lifecycle::PortAction::RESET_PORT) {
            uint32_t const OFFSET = XHCI_OP_PORTSC + ((port_id - 1U) * 0x10U);
            uint32_t const CURRENT = read32(hc.op, OFFSET);
            write32(hc.op, OFFSET, lifecycle::compose_portsc_reset_write(CURRENT));
            break;
        }
        if (transition.action == lifecycle::PortAction::ENUMERATE) {
            EnumerationResult const RESULT = enumerate_device(hc, port, port_id, port.speed, port.lifecycle.active_generation);
            if (RESULT == EnumerationResult::SUCCESS) {
                transition = lifecycle::complete_enumeration(port.lifecycle, port.lifecycle.active_generation);
                port.lifecycle = transition.port;
                continue;
            }
            port.lifecycle.state =
                RESULT == EnumerationResult::QUARANTINED ? lifecycle::PortState::FAILED : lifecycle::PortState::DISCONNECTED;
            port.lifecycle.active_generation = 0;
            port.lifecycle.reconnect_pending = false;
            port.lifecycle.quarantined = RESULT == EnumerationResult::QUARANTINED;
            break;
        }

        lifecycle::RollbackDisposition disposition = lifecycle::RollbackDisposition::REUSABLE;
        if (port.slot_id != 0) {
            auto& dev = hc.devices.at(port.slot_id);
            disposition =
                teardown_device(hc, port, dev) ? lifecycle::RollbackDisposition::REUSABLE : lifecycle::RollbackDisposition::QUARANTINED;
        }
        transition = lifecycle::finish_port_rollback(port.lifecycle, disposition);
        port.lifecycle = transition.port;
        if (disposition == lifecycle::RollbackDisposition::REUSABLE) {
            transition = lifecycle::resume_reusable_port(port.lifecycle);
            port.lifecycle = transition.port;
        }
    }
}

void process_pending_ports() {
    size_t const CONTROLLER_COUNT = controller_count.load(std::memory_order_acquire);
    for (size_t controller_index = 0; controller_index < CONTROLLER_COUNT; ++controller_index) {
        XhciController* hc = controllers.at(controller_index);
        if (hc == nullptr) {
            continue;
        }
        uint32_t pending = hc->pending_ports.exchange(0, std::memory_order_acq_rel);
        for (uint8_t port_id = 1; port_id <= hc->max_ports; ++port_id) {
            if ((pending & (uint32_t{1} << (port_id - 1U))) != 0U) {
                handle_port(*hc, port_id);
            }
        }
    }
}

[[noreturn]] void usb_worker() {
    for (;;) {
        if (!usb_worker_pending.exchange(false, std::memory_order_acq_rel)) {
            mod::sched::kern_block();
            continue;
        }
        process_pending_ports();
    }
}

auto ensure_usb_worker() -> bool {
    if (usb_worker_task != nullptr) {
        return true;
    }
    usb_worker_task = mod::sched::task::Task::create_kernel_thread("usb_lifecycle", usb_worker);
    if (usb_worker_task == nullptr) {
        return false;
    }
    return mod::sched::post_task_balanced(usb_worker_task);
}

void scan_ports(XhciController& hc) {
    for (uint8_t port_id = 1; port_id <= hc.max_ports; ++port_id) {
        uint32_t const PORTSC = read32(hc.op, XHCI_OP_PORTSC + ((port_id - 1U) * 0x10U));
        if ((PORTSC & XHCI_PORTSC_CCS) != 0U) {
            queue_port_snapshot(hc, port_id, PORTSC | XHCI_PORTSC_CSC);
        }
    }
}

void cleanup_controller(XhciController* hc, bool irq_registered) {
    if (hc == nullptr) {
        return;
    }
    if (irq_registered) {
        uint32_t command = read32(hc->op, XHCI_OP_USBCMD);
        command &= ~(XHCI_CMD_INTE | XHCI_CMD_RUN);
        write32(hc->op, XHCI_OP_USBCMD, command);
        write32(hc->rt, XHCI_RT_IMAN, 0);
        mod::gates::free_irq(hc->irq_vector);
    }
    delete static_cast<RequestTable*>(hc->request_table);
    hc->request_table = nullptr;
    for (size_t i = 0; i < hc->scratchpad_count; ++i) {
        free_page(hc->scratchpad_buffers.at(i));
    }
    void* erst = hc->erst;
    free_page(erst);
    hc->erst = nullptr;
    void* event_ring = hc->evt_ring;
    free_page(event_ring);
    hc->evt_ring = nullptr;
    void* command_ring = hc->cmd_ring;
    free_page(command_ring);
    hc->cmd_ring = nullptr;
    void* scratchpad_array = hc->scratchpad_array;
    free_page(scratchpad_array);
    hc->scratchpad_array = nullptr;
    void* dcbaa = hc->dcbaap;
    free_page(dcbaa);
    hc->dcbaap = nullptr;
    hc->~XhciController();
    mod::mm::phys::page_free(hc);
}

auto init_controller(pci::PCIDevice* pci_device) -> int {
    size_t const CONTROLLER_INDEX = controller_count.load(std::memory_order_acquire);
    if (CONTROLLER_INDEX >= controllers.size() || !ensure_usb_worker()) {
        return -1;
    }
    pci::pci_enable_bus_master(pci_device);
    pci::pci_enable_memory_space(pci_device);
    auto* bar = pci::pci_map_bar(pci_device, 0);
    if (bar == nullptr) {
        return -1;
    }
    auto* base = reinterpret_cast<volatile uint8_t*>(bar);
    uint8_t const CAP_LENGTH = *base;
    uint32_t const HCSPARAMS1 = read32(base, XHCI_CAP_HCSPARAMS1);
    uint32_t const HCSPARAMS2 = read32(base, XHCI_CAP_HCSPARAMS2);
    uint32_t const HCCPARAMS1 = read32(base, XHCI_CAP_HCCPARAMS1);
    auto* op = const_cast<volatile uint8_t*>(base + CAP_LENGTH);
    auto* rt = const_cast<volatile uint8_t*>(base + read32(base, XHCI_CAP_RTSOFF));
    auto* doorbells = reinterpret_cast<volatile uint32_t*>(const_cast<volatile uint8_t*>(base + read32(base, XHCI_CAP_DBOFF)));

    uint32_t command = read32(op, XHCI_OP_USBCMD) & ~XHCI_CMD_RUN;
    write32(op, XHCI_OP_USBCMD, command);
    for (unsigned i = 0; i < 100'000 && (read32(op, XHCI_OP_USBSTS) & XHCI_STS_HCH) == 0U; ++i) {
        asm volatile("pause");
    }
    if ((read32(op, XHCI_OP_USBSTS) & XHCI_STS_HCH) == 0U) {
        log::error("controller did not halt");
        return -1;
    }
    write32(op, XHCI_OP_USBCMD, XHCI_CMD_HCRST);
    for (unsigned i = 0; i < 100'000; ++i) {
        if ((read32(op, XHCI_OP_USBCMD) & XHCI_CMD_HCRST) == 0U && (read32(op, XHCI_OP_USBSTS) & XHCI_STS_CNR) == 0U) {
            break;
        }
        asm volatile("pause");
    }
    if ((read32(op, XHCI_OP_USBCMD) & XHCI_CMD_HCRST) != 0U || (read32(op, XHCI_OP_USBSTS) & XHCI_STS_CNR) != 0U) {
        log::error("controller reset timed out");
        return -1;
    }

    void* storage = mod::mm::phys::page_alloc(mod::mm::PhysicalPageOwner::DEVICE_USB, sizeof(XhciController), "xhci_controller");
    if (storage == nullptr) {
        return -1;
    }
    auto* hc = new (storage) XhciController{};
    hc->base = base;
    hc->op = op;
    hc->rt = rt;
    hc->db = doorbells;
    hc->pci = pci_device;
    hc->max_slots = static_cast<uint8_t>(std::min<size_t>(HCSPARAMS1 & 0xFFU, MAX_XHCI_SLOTS));
    hc->max_intrs = static_cast<uint16_t>((HCSPARAMS1 >> 8U) & 0x7FFU);
    hc->max_ports = static_cast<uint8_t>(std::min<size_t>((HCSPARAMS1 >> 24U) & 0xFFU, MAX_XHCI_PORTS));
    hc->ctx64 = (HCCPARAMS1 & (1U << 2U)) != 0U;
    hc->request_table = new (std::nothrow) RequestTable{};
    if (hc->max_slots == 0 || hc->request_table == nullptr) {
        cleanup_controller(hc, false);
        return -1;
    }

    write32(op, XHCI_OP_CONFIG, hc->max_slots);
    Alloc dcbaa = alloc_page("xhci_dcbaa");
    if (dcbaa.virt == nullptr) {
        cleanup_controller(hc, false);
        return -1;
    }
    hc->dcbaap = static_cast<uint64_t*>(dcbaa.virt);
    hc->dcbaap_phys = dcbaa.phys;
    write64(op, XHCI_OP_DCBAAP, dcbaa.phys);

    uint32_t const SCRATCHPAD_COUNT = (((HCSPARAMS2 >> 27U) & 0x1FU) << 5U) | ((HCSPARAMS2 >> 21U) & 0x1FU);
    if (SCRATCHPAD_COUNT > hc->scratchpad_buffers.size()) {
        cleanup_controller(hc, false);
        return -1;
    }
    if (SCRATCHPAD_COUNT != 0) {
        Alloc scratchpad_array = alloc_pages(static_cast<size_t>(SCRATCHPAD_COUNT) * sizeof(uint64_t), "xhci_scratchpad_array");
        if (scratchpad_array.virt == nullptr) {
            cleanup_controller(hc, false);
            return -1;
        }
        hc->scratchpad_array = static_cast<uint64_t*>(scratchpad_array.virt);
        hc->scratchpad_array_phys = scratchpad_array.phys;
        for (uint32_t i = 0; i < SCRATCHPAD_COUNT; ++i) {
            Alloc scratchpad = alloc_page("xhci_scratchpad");
            if (scratchpad.virt == nullptr) {
                cleanup_controller(hc, false);
                return -1;
            }
            hc->scratchpad_buffers.at(i) = scratchpad.virt;
            hc->scratchpad_array[i] = scratchpad.phys;
            ++hc->scratchpad_count;
        }
        hc->dcbaap[0] = scratchpad_array.phys;
    }

    Alloc command_ring = alloc_pages(CMD_RING_SIZE * sizeof(Trb), "xhci_command_ring");
    Alloc event_ring = alloc_pages(EVENT_RING_SIZE * sizeof(Trb), "xhci_event_ring");
    Alloc erst = alloc_page("xhci_erst");
    if (command_ring.virt == nullptr || event_ring.virt == nullptr || erst.virt == nullptr) {
        if (command_ring.virt != nullptr) {
            mod::mm::phys::page_free(command_ring.virt);
        }
        if (event_ring.virt != nullptr) {
            mod::mm::phys::page_free(event_ring.virt);
        }
        if (erst.virt != nullptr) {
            mod::mm::phys::page_free(erst.virt);
        }
        cleanup_controller(hc, false);
        return -1;
    }
    hc->cmd_ring = static_cast<Trb*>(command_ring.virt);
    hc->cmd_ring_phys = command_ring.phys;
    hc->cmd_cycle = true;
    hc->evt_ring = static_cast<Trb*>(event_ring.virt);
    hc->evt_ring_phys = event_ring.phys;
    hc->evt_cycle = true;
    hc->erst = new (erst.virt) ErstEntry{};
    hc->erst_phys = erst.phys;
    hc->erst[0] = {.ring_base = event_ring.phys, .ring_size = EVENT_RING_SIZE, .reserved = 0};
    write64(op, XHCI_OP_CRCR, command_ring.phys | 1U);
    write32(rt, XHCI_RT_ERSTSZ, 1);
    write64(rt, XHCI_RT_ERDP, event_ring.phys);
    write64(rt, XHCI_RT_ERSTBA, erst.phys);
    write32(rt, XHCI_RT_IMOD, 0);
    write32(rt, XHCI_RT_IMAN, XHCI_IMAN_IE);

    uint8_t vector = mod::gates::allocate_vector();
    if (vector == 0) {
        cleanup_controller(hc, false);
        return -1;
    }
    hc->irq_vector = vector;
    if (pci::pci_enable_msix(pci_device, vector) != 0 && pci::pci_enable_msi(pci_device, vector) != 0) {
        mod::gates::free_irq(vector);
        vector = static_cast<uint8_t>(pci_device->interrupt_line + 32U);
        hc->irq_vector = vector;
    }
    if (mod::gates::request_irq(vector, xhci_irq, hc, "xhci") != 0) {
        mod::gates::free_irq(vector);
        cleanup_controller(hc, false);
        return -1;
    }

    command = read32(op, XHCI_OP_USBCMD) | XHCI_CMD_RUN | XHCI_CMD_INTE;
    write32(op, XHCI_OP_USBCMD, command);
    for (unsigned i = 0; i < 100'000 && (read32(op, XHCI_OP_USBSTS) & XHCI_STS_HCH) != 0U; ++i) {
        asm volatile("pause");
    }
    if ((read32(op, XHCI_OP_USBSTS) & XHCI_STS_HCH) != 0U) {
        log::error("controller did not start");
        cleanup_controller(hc, true);
        return -1;
    }

    controllers.at(CONTROLLER_INDEX) = hc;
    controller_count.store(CONTROLLER_INDEX + 1U, std::memory_order_release);
    log::info("controller ready slots=%u ports=%u ctx64=%u vec=0x%02x", hc->max_slots, hc->max_ports, hc->ctx64 ? 1U : 0U, hc->irq_vector);
    scan_ports(*hc);
    return 0;
}

auto submit_transfer(XhciController& hc, UsbDevice& dev, UsbEndpoint& endpoint, std::span<const uint64_t> identities, uint64_t deadline,
                     void* dma, size_t dma_length, bool direction_in) -> request::RequestHandle {
    request::RequestHandle const HANDLE = request_table(&hc).reserve({
        .kind = request::RequestKind::TRANSFER,
        .slot_id = dev.slot_id,
        .dci = endpoint.dci,
        .trb_phys = identities,
        .requested_length = static_cast<uint32_t>(dma_length),
        .deadline_us = deadline,
    });
    if (!HANDLE.valid()) {
        return {};
    }
    endpoint.request_dma = dma;
    endpoint.request_dma_len = dma_length;
    endpoint.request_dir_in = direction_in;
    endpoint.request_generation.store(HANDLE.generation, std::memory_order_relaxed);
    endpoint.request_index.store(HANDLE.index, std::memory_order_release);
    return HANDLE;
}

void request_device_recovery(UsbDevice& dev) {
    XhciController& hc = *dev.controller;
    XhciPort& port = hc.ports.at(dev.port);
    port.recovery_requested.store(true, std::memory_order_release);
    hc.pending_ports.fetch_or(uint32_t{1} << (dev.port - 1U), std::memory_order_acq_rel);
    notify_usb_worker();
}

}  // namespace

auto configure_endpoint(XhciController* hc, uint8_t slot_id, uint64_t input_context_phys) -> int {
    return send_command(hc, input_context_phys, 0, TRB_CONFIG_ENDPOINT | (static_cast<uint32_t>(slot_id) << 24U));
}

void usb_register_class_driver(UsbClassDriver* driver) {
    if (driver == nullptr) {
        return;
    }
    driver->next = class_drivers;
    class_drivers = driver;
}

auto xhci_control_transfer(XhciController* hc, uint8_t slot_id, UsbSetupPacket* setup, void* data, size_t length, bool direction_in)
    -> int {
    if (hc == nullptr || setup == nullptr || slot_id == 0 || slot_id > hc->max_slots || length > MAX_TRANSFER_LENGTH) {
        return -EINVAL;
    }
    UsbDevice& dev = hc->devices.at(slot_id);
    UsbEndpoint& endpoint = dev.endpoints.at(1);
    if (!endpoint.accepting.load(std::memory_order_acquire)) {
        return -ENODEV;
    }
    bool expected = false;
    if (!endpoint.request_busy.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return -EBUSY;
    }
    if (!endpoint.accepting.load(std::memory_order_acquire)) {
        endpoint.request_busy.store(false, std::memory_order_release);
        return -ENODEV;
    }

    Alloc bounce{};
    if (length != 0) {
        bounce = alloc_pages(length, "xhci_control_bounce");
        if (bounce.virt == nullptr) {
            endpoint.request_busy.store(false, std::memory_order_release);
            return -ENOMEM;
        }
        if (!direction_in) {
            std::memcpy(bounce.virt, data, length);
        }
    }

    uint64_t setup_parameter = 0;
    std::memcpy(&setup_parameter, setup, sizeof(*setup));
    uint64_t const DEADLINE = mod::time::get_us() + CONTROL_TIMEOUT_US;
    request::RequestHandle handle{};
    {
        uint64_t const FLAGS = endpoint.ring_lock.lock_irqsave();
        if (length == 0) {
            auto const IDENTITIES = plan_ring_entries<2>(endpoint.ring_phys, endpoint.ring_enqueue, XFER_RING_SIZE);
            handle = submit_transfer(*hc, dev, endpoint, IDENTITIES, DEADLINE, nullptr, 0, direction_in);
            if (handle.valid()) {
                static_cast<void>(ring_enqueue(endpoint.ring, endpoint.ring_phys, &endpoint.ring_enqueue, &endpoint.ring_cycle,
                                               XFER_RING_SIZE, setup_parameter, 8, TRB_SETUP | TRB_IDT | TRB_CHAIN));
                static_cast<void>(ring_enqueue(endpoint.ring, endpoint.ring_phys, &endpoint.ring_enqueue, &endpoint.ring_cycle,
                                               XFER_RING_SIZE, 0, 0, TRB_STATUS | TRB_DIR_IN | TRB_IOC));
            }
        } else {
            auto const IDENTITIES = plan_ring_entries<3>(endpoint.ring_phys, endpoint.ring_enqueue, XFER_RING_SIZE);
            handle = submit_transfer(*hc, dev, endpoint, IDENTITIES, DEADLINE, bounce.virt, length, direction_in);
            if (handle.valid()) {
                uint32_t const TRT = direction_in ? (3U << 16U) : (2U << 16U);
                static_cast<void>(ring_enqueue(endpoint.ring, endpoint.ring_phys, &endpoint.ring_enqueue, &endpoint.ring_cycle,
                                               XFER_RING_SIZE, setup_parameter, 8, TRB_SETUP | TRB_IDT | TRB_CHAIN | TRT));
                static_cast<void>(ring_enqueue(endpoint.ring, endpoint.ring_phys, &endpoint.ring_enqueue, &endpoint.ring_cycle,
                                               XFER_RING_SIZE, bounce.phys, static_cast<uint32_t>(length),
                                               TRB_DATA | TRB_CHAIN | (direction_in ? (TRB_DIR_IN | TRB_ENT) : 0U)));
                static_cast<void>(ring_enqueue(endpoint.ring, endpoint.ring_phys, &endpoint.ring_enqueue, &endpoint.ring_cycle,
                                               XFER_RING_SIZE, 0, 0, TRB_STATUS | (direction_in ? 0U : TRB_DIR_IN) | TRB_IOC));
            }
        }
        if (handle.valid()) {
            ring_doorbell(hc->db, slot_id, 1);
        }
        endpoint.ring_lock.unlock_irqrestore(FLAGS);
    }
    if (!handle.valid()) {
        if (bounce.virt != nullptr) {
            mod::mm::phys::page_free(bounce.virt);
        }
        endpoint.request_busy.store(false, std::memory_order_release);
        return -ENOMEM;
    }

    request::RequestSnapshot snapshot{};
    int const RESULT = wait_for_request(request_table(hc), handle, DEADLINE, snapshot);
    if (snapshot.state == request::RequestState::TIMED_OUT) {
        if (RESULT == REQUEST_TIMEOUT_RESULT) {
            request_device_recovery(dev);
        }
        return RESULT;
    }

    size_t const ACTUAL = snapshot.residual <= length ? length - snapshot.residual : 0;
    if (RESULT == 0 && direction_in && data != nullptr && ACTUAL != 0) {
        std::memcpy(data, bounce.virt, ACTUAL);
    }
    static_cast<void>(request_table(hc).release(handle));
    clear_endpoint_handle(endpoint);
    if (bounce.virt != nullptr) {
        mod::mm::phys::page_free(bounce.virt);
    }
    endpoint.request_dma = nullptr;
    endpoint.request_dma_len = 0;
    endpoint.request_busy.store(false, std::memory_order_release);
    return RESULT;
}

auto xhci_bulk_transfer(XhciController* hc, uint8_t slot_id, UsbEndpoint* endpoint, void* data, size_t length, size_t* actual,
                        bool allow_idle_wait) -> int {
    if (actual != nullptr) {
        *actual = 0;
    }
    if (hc == nullptr || endpoint == nullptr || data == nullptr || length == 0 || length > MAX_TRANSFER_LENGTH || slot_id == 0 ||
        slot_id > hc->max_slots) {
        return -EINVAL;
    }
    if (!endpoint->accepting.load(std::memory_order_acquire)) {
        return -ENODEV;
    }
    bool expected = false;
    if (!endpoint->request_busy.compare_exchange_strong(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
        return -EBUSY;
    }
    if (!endpoint->accepting.load(std::memory_order_acquire)) {
        endpoint->request_busy.store(false, std::memory_order_release);
        return -ENODEV;
    }

    UsbDevice& dev = hc->devices.at(slot_id);
    bool const DIRECTION_IN = (endpoint->address & USB_EP_DIR_IN) != 0;
    Alloc bounce = alloc_pages(length, "xhci_bulk_bounce");
    if (bounce.virt == nullptr) {
        endpoint->request_busy.store(false, std::memory_order_release);
        return -ENOMEM;
    }
    if (!DIRECTION_IN) {
        std::memcpy(bounce.virt, data, length);
    }

    uint64_t const DEADLINE = allow_idle_wait ? std::numeric_limits<uint64_t>::max() : mod::time::get_us() + BULK_TIMEOUT_US;
    request::RequestHandle handle{};
    {
        uint64_t const FLAGS = endpoint->ring_lock.lock_irqsave();
        auto const IDENTITIES = plan_ring_entries<1>(endpoint->ring_phys, endpoint->ring_enqueue, XFER_RING_SIZE);
        handle = submit_transfer(*hc, dev, *endpoint, IDENTITIES, DEADLINE, bounce.virt, length, DIRECTION_IN);
        if (handle.valid()) {
            static_cast<void>(ring_enqueue(endpoint->ring, endpoint->ring_phys, &endpoint->ring_enqueue, &endpoint->ring_cycle,
                                           XFER_RING_SIZE, bounce.phys, static_cast<uint32_t>(length),
                                           TRB_NORMAL | TRB_IOC | (DIRECTION_IN ? TRB_ISP : 0U)));
            ring_doorbell(hc->db, slot_id, endpoint->dci);
        }
        endpoint->ring_lock.unlock_irqrestore(FLAGS);
    }
    if (!handle.valid()) {
        mod::mm::phys::page_free(bounce.virt);
        endpoint->request_busy.store(false, std::memory_order_release);
        return -ENOMEM;
    }

    request::RequestSnapshot snapshot{};
    int const RESULT = wait_for_request(request_table(hc), handle, DEADLINE, snapshot);
    if (snapshot.state == request::RequestState::TIMED_OUT) {
        if (RESULT == REQUEST_TIMEOUT_RESULT) {
            request_device_recovery(dev);
        }
        return RESULT;
    }

    size_t const ACTUAL = snapshot.residual <= length ? length - snapshot.residual : 0;
    if (RESULT == 0 && DIRECTION_IN && ACTUAL != 0) {
        std::memcpy(data, bounce.virt, ACTUAL);
    }
    if (actual != nullptr) {
        *actual = ACTUAL;
    }
    static_cast<void>(request_table(hc).release(handle));
    clear_endpoint_handle(*endpoint);
    mod::mm::phys::page_free(bounce.virt);
    endpoint->request_dma = nullptr;
    endpoint->request_dma_len = 0;
    endpoint->request_busy.store(false, std::memory_order_release);
    return RESULT;
}

auto xhci_configure_bulk_endpoints(UsbDevice* dev, const UsbEndpointDescriptor& endpoint_in, const UsbEndpointDescriptor& endpoint_out,
                                   UsbEndpoint** bulk_in, UsbEndpoint** bulk_out) -> int {
    if (dev == nullptr || dev->controller == nullptr || dev->input_ctx == nullptr || dev->dev_ctx == nullptr || bulk_in == nullptr ||
        bulk_out == nullptr) {
        return -EINVAL;
    }
    uint8_t const DCI_IN = ep_dci(endpoint_in.b_endpoint_address);
    uint8_t const DCI_OUT = ep_dci(endpoint_out.b_endpoint_address);
    if (DCI_IN <= 1 || DCI_OUT <= 1 || DCI_IN == DCI_OUT || DCI_IN >= dev->endpoints.size() || DCI_OUT >= dev->endpoints.size()) {
        return -EINVAL;
    }

    Alloc ring_in = alloc_pages(XFER_RING_SIZE * sizeof(Trb), "xhci_bulk_in_ring");
    Alloc ring_out = alloc_pages(XFER_RING_SIZE * sizeof(Trb), "xhci_bulk_out_ring");
    if (ring_in.virt == nullptr || ring_out.virt == nullptr) {
        if (ring_in.virt != nullptr) {
            mod::mm::phys::page_free(ring_in.virt);
        }
        if (ring_out.virt != nullptr) {
            mod::mm::phys::page_free(ring_out.virt);
        }
        return -ENOMEM;
    }

    auto initialize_endpoint = [](UsbEndpoint& endpoint, const UsbEndpointDescriptor& descriptor, uint8_t dci, const Alloc& ring) {
        endpoint.address = descriptor.b_endpoint_address;
        endpoint.type = USB_EP_TYPE_BULK;
        endpoint.max_packet = descriptor.w_max_packet_size;
        endpoint.interval = descriptor.b_interval;
        endpoint.dci = dci;
        endpoint.ring = static_cast<Trb*>(ring.virt);
        endpoint.ring_phys = ring.phys;
        endpoint.ring_cycle = true;
    };
    UsbEndpoint& in = dev->endpoints.at(DCI_IN);
    UsbEndpoint& out = dev->endpoints.at(DCI_OUT);
    initialize_endpoint(in, endpoint_in, DCI_IN, ring_in);
    initialize_endpoint(out, endpoint_out, DCI_OUT, ring_out);
    dev->enumeration_stage = lifecycle::EnumerationStage::ENDPOINT_RINGS_ALLOCATED;

    clear_input_context(*dev);
    input_add_flags(*dev) = 1U | (uint32_t{1} << DCI_IN) | (uint32_t{1} << DCI_OUT);
    *input_slot_context(*dev) = *output_slot_context(*dev);
    uint8_t const HIGHEST_DCI = std::max(DCI_IN, DCI_OUT);
    input_slot_context(*dev)->data[0] &= ~(0x1FU << 27U);
    input_slot_context(*dev)->data[0] |= static_cast<uint32_t>(HIGHEST_DCI) << 27U;

    auto fill_context = [](EndpointContext& context, const UsbEndpoint& endpoint) {
        uint32_t const TYPE = (endpoint.address & USB_EP_DIR_IN) != 0 ? 6U : 2U;
        context.data[1] = (3U << 1U) | (TYPE << 3U) | (static_cast<uint32_t>(endpoint.max_packet) << 16U);
        context.data[2] = static_cast<uint32_t>(endpoint.ring_phys) | 1U;
        context.data[3] = static_cast<uint32_t>(endpoint.ring_phys >> 32U);
        context.data[4] = endpoint.max_packet;
    };
    fill_context(*input_endpoint_context(*dev, DCI_IN), in);
    fill_context(*input_endpoint_context(*dev, DCI_OUT), out);
    int const RESULT = configure_endpoint(dev->controller, dev->slot_id, dev->input_ctx_phys);
    if (RESULT != 0) {
        if (!dev->controller->command_quarantined.load(std::memory_order_acquire)) {
            void* in_ring = in.ring;
            void* out_ring = out.ring;
            mod::mm::phys::page_free(in_ring);
            mod::mm::phys::page_free(out_ring);
            in.ring = nullptr;
            out.ring = nullptr;
        }
        return RESULT;
    }

    in.configured = true;
    out.configured = true;
    in.accepting.store(true, std::memory_order_release);
    out.accepting.store(true, std::memory_order_release);
    dev->configured_dcis |= (uint32_t{1} << DCI_IN) | (uint32_t{1} << DCI_OUT);
    dev->num_endpoints = static_cast<uint8_t>(dev->num_endpoints + 2U);
    dev->enumeration_stage = lifecycle::EnumerationStage::ENDPOINTS_CONFIGURED;
    *bulk_in = &in;
    *bulk_out = &out;
    return 0;
}

auto xhci_default_controller() -> XhciController* {
    return controller_count.load(std::memory_order_acquire) == 0 ? nullptr : controllers.front();
}

auto xhci_init() -> int {
    int found = 0;
    size_t const COUNT = pci::pci_device_count();
    for (size_t index = 0; index < COUNT; ++index) {
        auto* device = pci::pci_get_device(index);
        if (device == nullptr || device->class_code != pci::PCI_CLASS_SERIAL_BUS || device->subclass_code != pci::PCI_SUBCLASS_USB ||
            device->prog_if != pci::PCI_PROG_IF_XHCI) {
            continue;
        }
        if (init_controller(device) == 0) {
            ++found;
        }
    }
    if (found == 0) {
        log::info("no controllers found");
    }
    return found;
}

}  // namespace ker::dev::usb
