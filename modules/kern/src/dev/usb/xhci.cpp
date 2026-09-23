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
#include <platform/init/limine_requests.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/page_alloc.hpp>
#include <platform/mm/phys.hpp>
#ifdef WOS_SELFTEST
#include <platform/power/power.hpp>
#endif
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <span>
#include <string_view>

#include "dev/pci.hpp"

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
constexpr size_t MAX_TRANSFER_LENGTH = xhci_dma::MAX_TRANSFER;
constexpr int REQUEST_TIMEOUT_RESULT = -ETIMEDOUT;

using RequestTable = request::FixedRequestTable<REQUEST_CAPACITY, 6>;

UsbClassDriver* class_drivers = nullptr;
mod::sched::task::Task* usb_worker_task = nullptr;
std::atomic<bool> usb_worker_pending{false};
std::atomic<bool> shutdown_started{false};

class CallbackGuard {
    mod::sched::KernelShutdownHold shutdown_{mod::sched::hold_kernel_shutdown()};
    dma::lifecycle::Guard admission_;

   public:
    explicit CallbackGuard(dma::lifecycle::Admission& admission) : admission_(admission) {}
    [[nodiscard]] explicit operator bool() const { return static_cast<bool>(admission_); }
};

auto drain_admission(dma::lifecycle::Admission& admission) -> bool {
    auto const START = mod::time::get_ms();
    for (size_t n = 0; n < 10'000'000 && mod::time::get_ms() - START < 5000; ++n) {
        if (admission.drained()) return true;
        mod::sched::kern_yield();
    }
    return admission.drained();
}

struct Alloc {
    void* virt{};
    uint64_t phys{};
};

xhci_dma::Api dma_api;
#ifdef WOS_SELFTEST
bool dma_test_probing = false, dma_test_withhold_halt = false;
size_t dma_test_allocations_left = SIZE_MAX;
size_t dma_test_slot_allocations_left = SIZE_MAX;
XhciController* dma_test_controller = nullptr;
std::atomic<size_t> dma_test_packet_echoes{0}, dma_test_protected_yields{0};
std::atomic<bool> dma_test_submit_held{false};
auto dma_test_flag(std::string_view token) -> bool {
    auto const* cmd = ker::init::get_kernel_cmdline();
    if (cmd == nullptr) return false;
    std::string_view text(cmd);
    for (size_t pos = text.find(token); pos != std::string_view::npos; pos = text.find(token, pos + 1)) {
        auto const END = pos + token.size();
        if ((pos == 0 || text[pos - 1] == ' ') && (END == text.size() || text[END] == ' ')) return true;
    }
    return false;
}
#endif

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

void write_dma_address(XhciController* hc, volatile uint8_t* base, uint32_t offset, uint64_t value) {
    if (hc->dma_mask == UINT32_MAX)
        write32(base, offset, static_cast<uint32_t>(value));
    else
        *reinterpret_cast<volatile uint64_t*>(base + offset) = value;
}

auto allocate_slot_dma(XhciController* hc, xhci_dma::Coherent& region, size_t bytes) -> Alloc {
    if (region.allocation.handle.generation != 0) return {};
#ifdef WOS_SELFTEST
    if (dma_test_flag("--xhci-test-fail-slot-allocate")) {
        if (dma_test_slot_allocations_left == SIZE_MAX) dma_test_slot_allocations_left = 2;
        if (dma_test_slot_allocations_left == 0) return {};
        --dma_test_slot_allocations_left;
    }
#endif
    auto const R = dma::allocate_coherent(hc->pci, bytes, PAGE_SIZE, hc->dma_mask);
    region.allocation = R.allocation;
    if (R.error != dma::Error::none || !region.prepare(dma_api)) return {};
    return {.virt = region.allocation.cpu, .phys = region.submission.address};
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
        ring[index].control = TRB_LINK | TRB_TOGGLE_CYCLE | (control & TRB_CHAIN) | (*cycle ? TRB_CYCLE : 0);
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
    return request::await_result(
        requests, handle, deadline_us, snapshot, [] { return mod::time::get_us(); }, [] { mod::sched::kern_yield(); },
        REQUEST_TIMEOUT_RESULT, -EIO);
}

auto acquire_command(XhciController& hc) -> bool {
    while (!hc.command_quarantined.load(std::memory_order_acquire) && !hc.io_admission.closed()) {
        bool expected = false;
        if (hc.command_busy.compare_exchange_weak(expected, true, std::memory_order_acq_rel, std::memory_order_acquire)) {
            return true;
        }
        mod::sched::kern_yield();
    }
    return false;
}

auto send_command_result(XhciController* hc, uint64_t parameter, uint32_t status, uint32_t control) -> CommandResult {
    if (hc == nullptr) return {.status = -EINVAL};
    CallbackGuard const GUARD(hc->io_admission);
    if (!GUARD || hc->request_table == nullptr || !acquire_command(*hc)) {
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
    if (hc.lifecycle_admission.closed() || port_id == 0 || port_id > hc.max_ports) {
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
    write_dma_address(&hc, hc.rt, XHCI_RT_ERDP, ERDP | (uint64_t{1} << 3U));
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
    if (!endpoint.request_busy.load(std::memory_order_acquire)) return;
    request::RequestHandle const HANDLE = load_endpoint_handle(endpoint);
    if (!HANDLE.valid()) return;
    for (;;) {
        auto const RESULT = request_table(&hc).cancel(HANDLE, -ECANCELED);
        if (RESULT != request::TimeoutDisposition::COMPLETION_WON) return;
        request::RequestSnapshot snapshot{};
        if (request_table(&hc).snapshot(HANDLE, snapshot) && snapshot.state != request::RequestState::SUBMITTED) return;
        mod::sched::kern_yield();
    }
}

auto retire_endpoint_request_after_barrier(XhciController& hc, UsbEndpoint& endpoint) -> bool {
    if (!endpoint.request_busy.load(std::memory_order_acquire)) return true;
    if (!endpoint.transfer_dma.retire(dma_api, true)) return false;
    request::RequestHandle const HANDLE = load_endpoint_handle(endpoint);
    request::RequestSnapshot snapshot{};
    if (HANDLE.valid()) {
        if (!request_table(&hc).snapshot(HANDLE, snapshot)) return false;
        if (snapshot.state == request::RequestState::TIMED_OUT && !request_table(&hc).mark_timed_out_retired(HANDLE)) return false;
        if (!request_table(&hc).release(HANDLE)) return false;
    }
    clear_endpoint_handle(endpoint);
    endpoint.request_busy.store(false, std::memory_order_release);
    return true;
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

auto quarantine_slot(UsbDevice& dev) -> bool {
    auto pin = [](xhci_dma::Coherent& region) {
        if (region.allocation.handle.generation != 0) (void)dma::quarantine(region.allocation.handle);
    };
    pin(dev.dev_ctx_dma);
    pin(dev.input_ctx_dma);
    for (auto& endpoint : dev.endpoints) pin(endpoint.ring_dma);
    dev.state.store(UsbDeviceState::FAILED, std::memory_order_release);
    auto const S = dma::stats(dev.controller->pci);
    log::warn("slot %u DMA retained regions=%zu pages=%zu q=%zu", dev.slot_id, S.regions, S.backing_pages, S.quarantined);
    return false;
}

auto teardown_device(XhciController& hc, XhciPort& port, UsbDevice& dev) -> bool {
    dev.state.store(UsbDeviceState::DISCONNECTING, std::memory_order_release);
    for (auto& endpoint : dev.endpoints) {
        // A caller that already owns request_busy may still be allocating.
        // Serialize closure with its final eligibility check and doorbell.
        auto const FLAGS = endpoint.ring_lock.lock_irqsave();
        endpoint.accepting.store(false, std::memory_order_release);
        endpoint.ring_lock.unlock_irqrestore(FLAGS);
    }
    if (dev.bound_driver != nullptr && dev.bound_driver->quiesce != nullptr && !dev.bound_driver->quiesce(&dev)) {
        return quarantine_slot(dev);
    }

    for (auto& endpoint : dev.endpoints) {
        mark_endpoint_request_cancelled(hc, endpoint);
    }
    if (hc.command_quarantined.load(std::memory_order_acquire)) return quarantine_slot(dev);

    uint32_t const CONTROLLER_ENDPOINTS = dev.configured_dcis;
    for (uint8_t dci = 1; dci < 32; ++dci) {
        if ((CONTROLLER_ENDPOINTS & (uint32_t{1} << dci)) == 0) continue;
        auto const* state = reinterpret_cast<const volatile uint32_t*>(static_cast<uint8_t*>(dev.dev_ctx) + context_stride(&hc) * dci);
        auto const ACTION = xhci_dma::endpoint_stop_action(*state);
        if (ACTION == xhci_dma::StopAction::unproven ||
            (ACTION == xhci_dma::StopAction::command && stop_endpoint(&hc, dev.slot_id, dci) != 0)) {
            return quarantine_slot(dev);
        }
    }
#ifdef WOS_SELFTEST
    if (dma_test_flag("--xhci-test-withhold-slot-stop")) return quarantine_slot(dev);
#endif
    if (drop_non_control_endpoints(hc, dev) != 0) return quarantine_slot(dev);
    if (disable_slot(&hc, dev.slot_id) != 0) return quarantine_slot(dev);

    // Every running endpoint stopped successfully, including its prior transfer
    // events on our single event ring. Disable Slot retires all slot addresses.
    // Keep request records/storage until the class's CPU readers have drained.
    if (dev.bound_driver != nullptr && dev.bound_driver->detach != nullptr) {
        dev.bound_driver->detach(&dev);
    }
    bool released = true;
    for (auto& endpoint : dev.endpoints) {
        released = retire_endpoint_request_after_barrier(hc, endpoint) && released;
    }

    hc.dcbaap[dev.slot_id] = 0;
    std::atomic_thread_fence(std::memory_order_release);
    for (auto& endpoint : dev.endpoints) {
        bool const R = endpoint.ring_dma.retire(dma_api, true);
        if (R) endpoint.ring = nullptr;
        released = R && released;
    }
    bool const INPUT_RELEASED = dev.input_ctx_dma.retire(dma_api, true);
    if (INPUT_RELEASED) dev.input_ctx = nullptr;
    bool const DEVICE_RELEASED = dev.dev_ctx_dma.retire(dma_api, true);
    if (DEVICE_RELEASED) dev.dev_ctx = nullptr;
    if (!released || !INPUT_RELEASED || !DEVICE_RELEASED) return quarantine_slot(dev);

    auto const AFTER = dma::stats(hc.pci);
    log::info("slot %u DMA released regions=%zu pages=%zu q=%zu", dev.slot_id, AFTER.regions, AFTER.backing_pages, AFTER.quarantined);
    dev.~UsbDevice();
    new (&dev) UsbDevice{};
    port.slot_id = 0;
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
    if (dev.controller != nullptr) {
        // A completed Disable Slot may still have pinned mappings after an
        // invalidation failure. Never overwrite that retained ownership record.
        (void)disable_slot(&hc, static_cast<uint8_t>(SLOT));
        return EnumerationResult::QUARANTINED;
    }
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

    Alloc device_context = allocate_slot_dma(&hc, dev.dev_ctx_dma, PAGE_SIZE);
    if (device_context.virt == nullptr) {
        return fail();
    }
    dev.dev_ctx = device_context.virt;
    dev.dev_ctx_phys = device_context.phys;
    dev.enumeration_stage = lifecycle::EnumerationStage::DEVICE_CONTEXT_ALLOCATED;

    dev.dev_ctx_dma.expose();
    hc.dcbaap[dev.slot_id] = device_context.phys;
    std::atomic_thread_fence(std::memory_order_release);
    dev.enumeration_stage = lifecycle::EnumerationStage::DCBAA_PUBLISHED;

    Alloc input_context = allocate_slot_dma(&hc, dev.input_ctx_dma, PAGE_SIZE);
    if (input_context.virt == nullptr) {
        return fail();
    }
    dev.input_ctx = input_context.virt;
    dev.input_ctx_phys = input_context.phys;
    dev.enumeration_stage = lifecycle::EnumerationStage::INPUT_CONTEXT_ALLOCATED;

    Alloc ep0_ring = allocate_slot_dma(&hc, dev.endpoints.at(1).ring_dma, XFER_RING_SIZE * sizeof(Trb));
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
    dev.input_ctx_dma.expose();
    ep0.ring_dma.expose();
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
        CallbackGuard const GUARD(hc->lifecycle_admission);
        if (!GUARD) continue;
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
    // IRQ wakeups retain this pointer after cooperative worker exit.
    if (!usb_worker_task->try_acquire()) return false;
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

auto allocate_controller_dma(XhciController* hc, size_t index, size_t bytes) -> Alloc {
#ifdef WOS_SELFTEST
    if (dma_test_probing && dma_test_allocations_left != SIZE_MAX) {
        if (dma_test_allocations_left == 0) return {};
        --dma_test_allocations_left;
    }
#endif
    auto& region = hc->controller_dma.at(index);
    auto const R = dma::allocate_coherent(hc->pci, bytes, PAGE_SIZE, hc->dma_mask);
    region.allocation = R.allocation;
    if (R.error != dma::Error::none || !region.prepare(dma_api)) return {};
    return {.virt = region.allocation.cpu, .phys = region.submission.address};
}

auto configure_coherent_dma(pci::PCIDevice* p) -> bool {
    uint8_t const CAP = pci::pci_find_capability(p, 0x10);
    if (CAP == 0) return true;  // Conventional snooped PCI.
    if (CAP > 0xf4) return false;
    auto const OFFSET = static_cast<uint8_t>(CAP + 8);
    auto value = pci::pci_config_read16(p->bus, p->slot, p->function, OFFSET);
    value = static_cast<uint16_t>(value & ~0x0810U);  // No Snoop and Relaxed Ordering.
    pci::pci_config_write16(p->bus, p->slot, p->function, OFFSET, value);
    return (pci::pci_config_read16(p->bus, p->slot, p->function, OFFSET) & 0x0810U) == 0;
}

auto halt_controller(XhciController* hc) -> bool {
    auto const START = mod::time::get_ms();
    for (size_t n = 0; n < 10'000'000 && mod::time::get_ms() - START < 100; ++n) {
        auto const STATUS = read32(hc->op, XHCI_OP_USBSTS);
        if (STATUS != UINT32_MAX && (STATUS & XHCI_STS_HCH) != 0) return true;
        asm volatile("pause" ::: "memory");
    }
    return false;
}

// Probe-owned controllers only: no registry/worker/class callers exist yet.
// Published-controller terminal retirement needs a separate software drain.
auto cleanup_controller(XhciController* hc, bool irq_registered) -> bool {
    if (hc == nullptr) return true;
    write32(hc->op, XHCI_OP_USBCMD, read32(hc->op, XHCI_OP_USBCMD) & ~(XHCI_CMD_INTE | XHCI_CMD_RUN));
    write32(hc->rt, XHCI_RT_IMAN, 0);
    bool const MASKED = (read32(hc->op, XHCI_OP_USBCMD) & XHCI_CMD_INTE) == 0 && (read32(hc->rt, XHCI_RT_IMAN) & XHCI_IMAN_IE) == 0;
    bool halted = halt_controller(hc);
#ifdef WOS_SELFTEST
    if (dma_test_withhold_halt) halted = false;
#endif
    bool const BME_OFF = pci::pci_set_bus_master(hc->pci, false);
    if (irq_registered && MASKED) {
        mod::gates::free_irq(hc->irq_vector);
        hc->irq_owned = false;
    }
    bool const PROOF = halted && BME_OFF && (!irq_registered || MASKED);
    bool released = true;
    for (auto& region : hc->controller_dma) released = region.retire(dma_api, PROOF) && released;
    auto const AFTER = dma::stats(hc->pci);
    log::info("controller DMA cleanup halted=%u masked=%u bme_off=%u released=%u regions=%zu pages=%zu q=%zu", halted, MASKED, BME_OFF,
              released, AFTER.regions, AFTER.backing_pages, AFTER.quarantined);
    if (!released || (irq_registered && !MASKED)) return false;
    delete static_cast<RequestTable*>(hc->request_table);
#ifdef WOS_SELFTEST
    if (dma_test_controller == hc) dma_test_controller = nullptr;
#endif
    hc->~XhciController();
    mod::mm::phys::page_free(hc);
    return true;
}

struct TerminalBackend {
    XhciController& hc;
    bool masked = false, halted = false, bme_off = false;
    void close_lifecycle() { hc.lifecycle_admission.close(); }
    void close_io() { hc.io_admission.close(); }
    auto drain_lifecycle() -> bool { return drain_admission(hc.lifecycle_admission); }
    auto close_io_and_slots() -> bool {
        bool closed = true;
        for (auto& dev : hc.devices) {
            if (dev.slot_id == 0) continue;
            dev.state.store(UsbDeviceState::DISCONNECTING, std::memory_order_release);
            for (auto& endpoint : dev.endpoints) {
                auto const FLAGS = endpoint.ring_lock.lock_irqsave();
                endpoint.accepting.store(false, std::memory_order_release);
                endpoint.ring_lock.unlock_irqrestore(FLAGS);
            }
            if (dev.bound_driver != nullptr) {
                closed = dev.bound_driver->quiesce != nullptr && dev.bound_driver->quiesce(&dev) && closed;
            }
            for (auto& endpoint : dev.endpoints) mark_endpoint_request_cancelled(hc, endpoint);
        }
        return closed;
    }
    auto drain_io() -> bool { return drain_admission(hc.io_admission); }
    auto drain_classes() -> bool {
        bool drained = true;
        for (auto& dev : hc.devices) {
            if (dev.slot_id != 0 && dev.bound_driver != nullptr) {
                drained = dev.bound_driver->terminal_drain != nullptr && dev.bound_driver->terminal_drain(&dev) && drained;
            }
        }
        return drained;
    }
    auto mask_interrupts() -> bool {
        write32(hc.op, XHCI_OP_USBCMD, read32(hc.op, XHCI_OP_USBCMD) & ~XHCI_CMD_INTE);
        write32(hc.rt, XHCI_RT_IMAN, 0);
        auto const COMMAND = read32(hc.op, XHCI_OP_USBCMD);
        auto const IMAN = read32(hc.rt, XHCI_RT_IMAN);
        return masked = COMMAND != UINT32_MAX && IMAN != UINT32_MAX && (COMMAND & XHCI_CMD_INTE) == 0 && (IMAN & XHCI_IMAN_IE) == 0;
    }
    auto halt() -> bool {
        write32(hc.op, XHCI_OP_USBCMD, read32(hc.op, XHCI_OP_USBCMD) & ~(XHCI_CMD_RUN | XHCI_CMD_INTE));
        halted = halt_controller(&hc);
#ifdef WOS_SELFTEST
        if (dma_test_flag("--xhci-test-withhold-terminal-halt")) return false;
#endif
        return halted;
    }
    auto disable_bus_master() -> bool { return bme_off = pci::pci_set_bus_master(hc.pci, false); }
    void drain_interrupts() {
        if (hc.irq_owned) {
            mod::gates::free_irq(hc.irq_vector);
            hc.irq_owned = false;
        }
    }
    auto retire_regions(bool proof) -> bool {
        bool released = true;
        for (auto& dev : hc.devices) {
            for (auto& endpoint : dev.endpoints) {
                bool const STREAM =
                    proof ? retire_endpoint_request_after_barrier(hc, endpoint) : endpoint.transfer_dma.retire(dma_api, false);
                released = STREAM && released;
                released = endpoint.ring_dma.retire(dma_api, proof) && released;
            }
            released = dev.input_ctx_dma.retire(dma_api, proof) && released;
            released = dev.dev_ctx_dma.retire(dma_api, proof) && released;
            if (dev.slot_id != 0) dev.state.store(UsbDeviceState::FAILED, std::memory_order_release);
        }
        for (auto& region : hc.controller_dma) released = region.retire(dma_api, proof) && released;
        return released;
    }
    void quarantine() { (void)retire_regions(false); }
    auto release() -> bool { return retire_regions(true); }
};

auto retire_controller(XhciController& hc) -> int {
    auto const HOLD = mod::sched::hold_kernel_shutdown();
    auto expected = xhci_terminal::State::active;
    if (!hc.terminal_state.compare_exchange_strong(expected, xhci_terminal::State::retiring, std::memory_order_acq_rel)) {
        return expected == xhci_terminal::State::released ? 0 : expected == xhci_terminal::State::retiring ? -EBUSY : -EIO;
    }
    TerminalBackend backend{hc};
    auto const RESULT = xhci_terminal::retire(backend);
    bool const RELEASED = RESULT == xhci_terminal::Result::released;
    hc.terminal_state.store(RELEASED ? xhci_terminal::State::released : xhci_terminal::State::retained, std::memory_order_release);
    auto const AFTER = dma::stats(hc.pci);
    log::info("terminal DMA result=%u halted=%u masked=%u bme_off=%u irq_owned=%u regions=%zu pages=%zu q=%zu",
              static_cast<unsigned>(RESULT), backend.halted, backend.masked, backend.bme_off, hc.irq_owned, AFTER.regions,
              AFTER.backing_pages, AFTER.quarantined);
    // Controller, request, USB/class and retained worker metadata stay stable.
    // Public callers must pass the permanently closed admission gate first.
    return RELEASED ? 0 : -EIO;
}

auto init_controller(pci::PCIDevice* pci_device) -> int {
    if (shutdown_started.load(std::memory_order_acquire)) return -ENODEV;
    size_t const CONTROLLER_INDEX = controller_count.load(std::memory_order_acquire);
    bool need_worker = true;
#ifdef WOS_SELFTEST
    need_worker = !dma_test_probing;
#endif
    if (CONTROLLER_INDEX >= controllers.size() || (need_worker && !ensure_usb_worker())) {
        return -1;
    }
    if (!pci::pci_set_bus_master(pci_device, false) || !configure_coherent_dma(pci_device)) return -1;
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
    hc->dma_mask = (HCCPARAMS1 & 1U) != 0 ? UINT64_MAX : UINT32_MAX;
#ifdef WOS_SELFTEST
    if (dma_test_probing) dma_test_controller = hc;
#endif
    if ((read32(op, XHCI_OP_PAGESIZE) & 1U) == 0) {
        (void)cleanup_controller(hc, false);
        return -1;
    }
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
    Alloc dcbaa = allocate_controller_dma(hc, xhci_dma::DCBAA, PAGE_SIZE);
    if (dcbaa.virt == nullptr) {
        cleanup_controller(hc, false);
        return -1;
    }
    hc->dcbaap = static_cast<uint64_t*>(dcbaa.virt);
    hc->dcbaap_phys = dcbaa.phys;

    size_t const SCRATCHPAD_COUNT = xhci_dma::scratchpad_count(HCSPARAMS2);
    if (SCRATCHPAD_COUNT > hc->scratchpad_buffers.size()) {
        cleanup_controller(hc, false);
        return -1;
    }
    if (SCRATCHPAD_COUNT != 0) {
        Alloc scratchpad_array =
            allocate_controller_dma(hc, xhci_dma::SCRATCH_ARRAY, static_cast<size_t>(SCRATCHPAD_COUNT) * sizeof(uint64_t));
        if (scratchpad_array.virt == nullptr) {
            cleanup_controller(hc, false);
            return -1;
        }
        hc->scratchpad_array = static_cast<uint64_t*>(scratchpad_array.virt);
        hc->scratchpad_array_phys = scratchpad_array.phys;
        for (uint32_t i = 0; i < SCRATCHPAD_COUNT; ++i) {
            Alloc scratchpad = allocate_controller_dma(hc, xhci_dma::SCRATCH_FIRST + i, PAGE_SIZE);
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

    Alloc command_ring = allocate_controller_dma(hc, xhci_dma::COMMAND, CMD_RING_SIZE * sizeof(Trb));
    Alloc event_ring = allocate_controller_dma(hc, xhci_dma::EVENT, EVENT_RING_SIZE * sizeof(Trb));
    Alloc erst = allocate_controller_dma(hc, xhci_dma::ERST, PAGE_SIZE);
    if (command_ring.virt == nullptr || event_ring.virt == nullptr || erst.virt == nullptr) {
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

    uint8_t vector = mod::gates::allocate_vector();
    if (vector == 0) {
        cleanup_controller(hc, false);
        return -1;
    }
    bool vector_reserved = true;
    hc->irq_vector = vector;
    if (pci::pci_enable_msix(pci_device, vector) != 0 && pci::pci_enable_msi(pci_device, vector) != 0) {
        mod::gates::free_irq(vector);
        vector_reserved = false;
        if (pci_device->interrupt_line == 0 || pci_device->interrupt_line >= 16) {
            (void)cleanup_controller(hc, false);
            return -1;
        }
        vector = static_cast<uint8_t>(pci_device->interrupt_line + 32U);
        hc->irq_vector = vector;
    }
    if (mod::gates::request_irq(vector, xhci_irq, hc, "xhci") != 0) {
        if (vector_reserved) mod::gates::free_irq(vector);
        cleanup_controller(hc, false);
        return -1;
    }

    hc->irq_owned = true;

    // Conservatively expose before attempted BME enable (failed readback may
    // still mean hardware observed it). All mappings already exist.
    for (auto& region : hc->controller_dma)
        if (region.submission.sequence != 0) region.expose();
    asm volatile("mfence" ::: "memory");
    if (!pci::pci_set_bus_master(pci_device, true)) {
        (void)cleanup_controller(hc, true);
        return -1;
    }
    // Address-register publication can fetch tables even before RUN. BME must
    // already be enabled, with every referenced allocation mapped/prepared.
    write_dma_address(hc, op, XHCI_OP_DCBAAP, dcbaa.phys);
    write_dma_address(hc, op, XHCI_OP_CRCR, command_ring.phys | 1U);
    write32(rt, XHCI_RT_ERSTSZ, 1);
    write_dma_address(hc, rt, XHCI_RT_ERDP, event_ring.phys);
    write_dma_address(hc, rt, XHCI_RT_ERSTBA, erst.phys);
    write32(rt, XHCI_RT_IMOD, 0);
    write32(rt, XHCI_RT_IMAN, XHCI_IMAN_IE);
    command = read32(op, XHCI_OP_USBCMD) | XHCI_CMD_RUN | XHCI_CMD_INTE;
    write32(op, XHCI_OP_USBCMD, command);
    for (unsigned i = 0; i < 100'000 && (read32(op, XHCI_OP_USBSTS) & XHCI_STS_HCH) != 0U; ++i) {
        asm volatile("pause");
    }
    if ((read32(op, XHCI_OP_USBSTS) & (XHCI_STS_HCH | XHCI_STS_HSE | XHCI_STS_HCE)) != 0U) {
        log::error("controller did not start cleanly");
        cleanup_controller(hc, true);
        return -1;
    }

    auto const MAPPED = dma::stats(pci_device);
    log::info("controller DMA prepared regions=%zu pages=%zu mask=0x%lx", MAPPED.regions, MAPPED.backing_pages, hc->dma_mask);
#ifdef WOS_SELFTEST
    if (dma_test_probing) return 0;
#endif
    controllers.at(CONTROLLER_INDEX) = hc;
    controller_count.store(CONTROLLER_INDEX + 1U, std::memory_order_release);
    log::info("controller ready slots=%u ports=%u ctx64=%u vec=0x%02x", hc->max_slots, hc->max_ports, hc->ctx64 ? 1U : 0U, hc->irq_vector);
    scan_ports(*hc);
    return 0;
}

auto submit_transfer(XhciController& hc, UsbDevice& dev, UsbEndpoint& endpoint, std::span<const uint64_t> identities, uint64_t deadline,
                     size_t dma_length, uint64_t terminal_trb, uint64_t cookie = 0) -> request::RequestHandle {
    if (!endpoint.accepting.load(std::memory_order_acquire)) return {};
    request::RequestHandle const HANDLE = request_table(&hc).reserve({
        .kind = request::RequestKind::TRANSFER,
        .slot_id = dev.slot_id,
        .dci = endpoint.dci,
        .trb_phys = identities,
        .requested_length = static_cast<uint32_t>(dma_length),
        .deadline_us = deadline,
        .event_data_valid = cookie != 0,
        .event_data = cookie,
        .terminal_trb = terminal_trb,
        .terminal_event_data = cookie != 0,
    });
    if (!HANDLE.valid()) return {};
    endpoint.transfer_dma.expose();
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

auto prepare_transfer_dma(XhciController& hc, UsbEndpoint& endpoint, void* data, size_t length, bool direction_in) -> bool {
    if (length == 0) return true;
    auto& region = endpoint.transfer_dma;
    if (region.allocation.handle.generation != 0) return false;
    auto const DIRECTION = direction_in ? dma::Direction::from_device : dma::Direction::to_device;
    auto const R = dma::allocate_streaming(hc.pci, length, DIRECTION, hc.dma_mask);
    region.allocation = R.allocation;
    if (R.error != dma::Error::none) return false;
    if (!direction_in) std::memcpy(region.allocation.cpu, data, length);
    return region.prepare(dma_api, DIRECTION);
}

auto abort_unpublished_transfer(UsbDevice& dev, UsbEndpoint& endpoint) -> int {
    if (endpoint.transfer_dma.retire(dma_api, false))
        endpoint.request_busy.store(false, std::memory_order_release);
    else
        request_device_recovery(dev);
    return -ENOMEM;
}

auto finish_transfer(UsbDevice& dev, UsbEndpoint& endpoint, request::RequestHandle handle, uint64_t deadline, void* data, size_t length,
                     bool direction_in, size_t* actual, const xhci_dma::TransferPlan& plan, std::span<const uint64_t> identities,
                     bool control) -> int {
    auto* hc = dev.controller;
    request::RequestSnapshot snapshot{};
    int const RESULT = wait_for_request(request_table(hc), handle, deadline, snapshot);
    if (snapshot.state == request::RequestState::TIMED_OUT) {
        if (RESULT != -ECANCELED) request_device_recovery(dev);
        return RESULT;
    }
    size_t transferred = length;
    bool valid = RESULT == 0 && snapshot.state == request::RequestState::COMPLETED;
    if (control) {
        valid = valid && !snapshot.event_uses_data && snapshot.residual == 0 && snapshot.completion_code == TRB_CC_SUCCESS;
        if (snapshot.short_seen) {
            bool found = false;
            for (size_t i = 0; i < plan.count; ++i) {
                auto const& SEGMENT = plan.segments.at(i);
                if (identities[i + 1] != snapshot.short_identity || snapshot.short_residual > SEGMENT.length) continue;
                transferred = SEGMENT.offset + SEGMENT.length - snapshot.short_residual;
                found = true;
                break;
            }
            valid = valid && found && direction_in;
        }
    } else {
        transferred = snapshot.residual;  // Event Data reports bytes transferred, not residue.
        valid = valid && snapshot.event_uses_data && transferred <= length && (direction_in || transferred == length);
    }
    if (!valid || !endpoint.transfer_dma.complete(dma_api, transferred, dma::Retirement::command_completed)) {
        request_device_recovery(dev);
        return RESULT != 0 ? RESULT : -EIO;
    }
    if (direction_in && transferred != 0) std::memcpy(data, endpoint.transfer_dma.allocation.cpu, transferred);
    if (!endpoint.transfer_dma.release(dma_api) || !request_table(hc).release(handle)) {
        request_device_recovery(dev);
        return -EIO;
    }
    clear_endpoint_handle(endpoint);
    endpoint.request_busy.store(false, std::memory_order_release);
    if (actual != nullptr) *actual = transferred;
#ifdef WOS_SELFTEST
    if (!control && transferred >= 28 && dma_test_flag("--xhci-test-packets")) {
        auto* bytes = static_cast<uint8_t*>(data);
        constexpr std::array<uint8_t, 10> TAG{0x88, 0xb5, 'W', 'O', 'S', 'D', 'M', 'A', '2', '3'};
        if (std::memcmp(bytes + 12, TAG.data(), TAG.size()) == 0) {
            if (!direction_in) {
                log::info("DMA test bulk TX bytes=%zu segments=%zu first=%zu", transferred, plan.count, plan.segments.front().length);
            } else if (bytes[22] == 1) {
                uint32_t expected_length = 0;
                std::memcpy(&expected_length, bytes + 24, sizeof(expected_length));
                bool matched = bytes[22] == 1 && expected_length == transferred;
                for (size_t i = 28; i < transferred; ++i)
                    matched = matched && bytes[i] == static_cast<uint8_t>(i * 17 + transferred + bytes[23]);
                int echoed = -ENODEV;
                if (matched) {
                    for (auto& out : dev.endpoints) {
                        if (!out.configured || out.type != USB_EP_TYPE_BULK || (out.address & USB_EP_DIR_IN) != 0) continue;
                        bytes[22] = 2;
                        echoed = xhci_bulk_transfer(hc, dev.slot_id, &out, data, transferred);
                        bytes[22] = 1;
                        break;
                    }
                }
                log::info("DMA test bulk RX bytes=%zu segments=%zu matched=%u echo=%d low=0x%lx first=%zu", transferred, plan.count,
                          matched, echoed, plan.segments.front().address & 65535U, plan.segments.front().length);
                if (matched && echoed == 0) dma_test_packet_echoes.fetch_add(1, std::memory_order_release);
            }
        }
    }
#endif
    return 0;
}

auto next_transfer_cookie(XhciController& hc) -> uint64_t {
    auto previous = hc.transfer_cookie.load(std::memory_order_relaxed);
    while (previous != UINT64_MAX) {
        if (hc.transfer_cookie.compare_exchange_weak(previous, previous + 1, std::memory_order_relaxed)) return previous + 1;
    }
    return 0;
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
    if (hc == nullptr || setup == nullptr || slot_id == 0 || slot_id > hc->max_slots || length > MAX_TRANSFER_LENGTH ||
        (length != 0 && data == nullptr)) {
        return -EINVAL;
    }
    CallbackGuard const GUARD(hc->io_admission);
    if (!GUARD) return -ENODEV;
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

    if (!prepare_transfer_dma(*hc, endpoint, data, length, direction_in)) return abort_unpublished_transfer(dev, endpoint);
    auto const PLAN = xhci_dma::plan_transfer(endpoint.transfer_dma.submission.address, length, endpoint.max_packet);
    if (!PLAN.valid) return abort_unpublished_transfer(dev, endpoint);
    uint64_t setup_parameter = 0;
    std::memcpy(&setup_parameter, setup, sizeof(*setup));
    uint64_t const DEADLINE = mod::time::get_us() + CONTROL_TIMEOUT_US;
    request::RequestHandle handle{};
    std::array<uint64_t, 6> identities{};
    {
        uint64_t const FLAGS = endpoint.ring_lock.lock_irqsave();
        identities = plan_ring_entries<6>(endpoint.ring_phys, endpoint.ring_enqueue, XFER_RING_SIZE);
        handle = submit_transfer(*hc, dev, endpoint, {identities.data(), PLAN.count + 2}, DEADLINE, length, identities.at(PLAN.count + 1));
        if (handle.valid()) {
            uint32_t const TRT = length == 0 ? 0 : (direction_in ? (3U << 16U) : (2U << 16U));
            (void)ring_enqueue(endpoint.ring, endpoint.ring_phys, &endpoint.ring_enqueue, &endpoint.ring_cycle, XFER_RING_SIZE,
                               setup_parameter, 8, TRB_SETUP | TRB_IDT | TRT);
            for (size_t i = 0; i < PLAN.count; ++i) {
                auto const& SEGMENT = PLAN.segments.at(i);
                uint32_t const TYPE = i == 0 ? TRB_DATA | (direction_in ? TRB_DIR_IN : 0U) : TRB_NORMAL;
                uint32_t const FLAGS = TYPE | (i + 1 < PLAN.count ? TRB_CHAIN : TRB_ENT) | (direction_in ? TRB_ISP : 0U);
                (void)ring_enqueue(endpoint.ring, endpoint.ring_phys, &endpoint.ring_enqueue, &endpoint.ring_cycle, XFER_RING_SIZE,
                                   SEGMENT.address, static_cast<uint32_t>(SEGMENT.length) | (SEGMENT.td_size << 17U), FLAGS);
            }
            (void)ring_enqueue(endpoint.ring, endpoint.ring_phys, &endpoint.ring_enqueue, &endpoint.ring_cycle, XFER_RING_SIZE, 0, 0,
                               TRB_STATUS | (length == 0 || !direction_in ? TRB_DIR_IN : 0U) | TRB_IOC);
            ring_doorbell(hc->db, slot_id, 1);
        }
        endpoint.ring_lock.unlock_irqrestore(FLAGS);
    }
    if (!handle.valid()) return abort_unpublished_transfer(dev, endpoint);
    return finish_transfer(dev, endpoint, handle, DEADLINE, data, length, direction_in, nullptr, PLAN, identities, true);
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
    CallbackGuard const GUARD(hc->io_admission);
    if (!GUARD) return -ENODEV;
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
    if (!prepare_transfer_dma(*hc, *endpoint, data, length, DIRECTION_IN)) return abort_unpublished_transfer(dev, *endpoint);
    size_t first_segment_limit = SIZE_MAX;
#ifdef WOS_SELFTEST
    // Small QEMU USB payloads can exercise chained data and short completion
    // without changing the owned DMA span or the normal boundary planner.
    if (dma_test_flag("--xhci-test-split-first")) first_segment_limit = endpoint->max_packet;
#endif
    auto const PLAN = xhci_dma::plan_transfer(endpoint->transfer_dma.submission.address, length, endpoint->max_packet, first_segment_limit);
    uint64_t const COOKIE = next_transfer_cookie(*hc);
    if (!PLAN.valid || COOKIE == 0) return abort_unpublished_transfer(dev, *endpoint);
#ifdef WOS_SELFTEST
    bool const HELD_SUBMIT = DIRECTION_IN && dma_test_flag("--xhci-test-hold-slot-submit");
    if (HELD_SUBMIT) {
        log::info("slot %u bulk publication held", slot_id);
        dma_test_submit_held.store(true, std::memory_order_release);
        while (endpoint->accepting.load(std::memory_order_acquire)) {
            mod::sched::kern_yield();
            auto* current = mod::sched::get_current_task();
            if (current != nullptr && current->kernel_shutdown_requested.load(std::memory_order_acquire))
                dma_test_protected_yields.fetch_add(1, std::memory_order_release);
        }
        log::info("slot %u bulk publication resumed after admission closed", slot_id);
    }
#endif

    uint64_t const DEADLINE = allow_idle_wait ? std::numeric_limits<uint64_t>::max() : mod::time::get_us() + BULK_TIMEOUT_US;
    request::RequestHandle handle{};
    std::array<uint64_t, 4> identities{};
    {
        uint64_t const FLAGS = endpoint->ring_lock.lock_irqsave();
        identities = plan_ring_entries<4>(endpoint->ring_phys, endpoint->ring_enqueue, XFER_RING_SIZE);
        handle = submit_transfer(*hc, dev, *endpoint, {identities.data(), PLAN.count + 1}, DEADLINE, length, 0, COOKIE);
        if (handle.valid()) {
            for (size_t i = 0; i < PLAN.count; ++i) {
                auto const& SEGMENT = PLAN.segments.at(i);
                (void)ring_enqueue(endpoint->ring, endpoint->ring_phys, &endpoint->ring_enqueue, &endpoint->ring_cycle, XFER_RING_SIZE,
                                   SEGMENT.address, static_cast<uint32_t>(SEGMENT.length) | (SEGMENT.td_size << 17U),
                                   TRB_NORMAL | TRB_CHAIN);
            }
            (void)ring_enqueue(endpoint->ring, endpoint->ring_phys, &endpoint->ring_enqueue, &endpoint->ring_cycle, XFER_RING_SIZE, COOKIE,
                               0, TRB_EVENT_DATA | TRB_IOC);
            ring_doorbell(hc->db, slot_id, endpoint->dci);
        }
        endpoint->ring_lock.unlock_irqrestore(FLAGS);
    }
    if (!handle.valid()) {
#ifdef WOS_SELFTEST
        if (HELD_SUBMIT) log::info("slot %u held bulk publication rejected", slot_id);
#endif
        return abort_unpublished_transfer(dev, *endpoint);
    }
    return finish_transfer(dev, *endpoint, handle, DEADLINE, data, length, DIRECTION_IN, actual, PLAN, identities, false);
}

auto xhci_configure_bulk_endpoints(UsbDevice* dev, const UsbEndpointDescriptor& endpoint_in, const UsbEndpointDescriptor& endpoint_out,
                                   UsbEndpoint** bulk_in, UsbEndpoint** bulk_out) -> int {
    if (dev == nullptr || dev->controller == nullptr) return -EINVAL;
    CallbackGuard const GUARD(dev->controller->io_admission);
    if (!GUARD) return -ENODEV;
    if (dev->input_ctx == nullptr || dev->dev_ctx == nullptr || bulk_in == nullptr || bulk_out == nullptr) {
        return -EINVAL;
    }
    uint8_t const DCI_IN = ep_dci(endpoint_in.b_endpoint_address);
    uint8_t const DCI_OUT = ep_dci(endpoint_out.b_endpoint_address);
    if (DCI_IN <= 1 || DCI_OUT <= 1 || DCI_IN == DCI_OUT || DCI_IN >= dev->endpoints.size() || DCI_OUT >= dev->endpoints.size()) {
        return -EINVAL;
    }

    UsbEndpoint& in = dev->endpoints.at(DCI_IN);
    UsbEndpoint& out = dev->endpoints.at(DCI_OUT);
    if (in.ring_dma.allocation.handle.generation != 0 || out.ring_dma.allocation.handle.generation != 0) return -EBUSY;
    Alloc ring_in = allocate_slot_dma(dev->controller, in.ring_dma, XFER_RING_SIZE * sizeof(Trb));
    Alloc ring_out = allocate_slot_dma(dev->controller, out.ring_dma, XFER_RING_SIZE * sizeof(Trb));
    if (ring_in.virt == nullptr || ring_out.virt == nullptr) {
        // Enumeration rollback owns even a partial/faulted allocation.
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
    in.ring_dma.expose();
    out.ring_dma.expose();
    int const RESULT = configure_endpoint(dev->controller, dev->slot_id, dev->input_ctx_phys);
    if (RESULT != 0) {
        // Keep exposed rings until slot rollback proves all hardware readers
        // retired. Command errors alone do not grant mapping-release authority.
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

#ifdef WOS_SELFTEST
namespace {
void dma_terminal_runner() {
    UsbDevice* device = nullptr;
    XhciController* hc = nullptr;
    auto const START = mod::time::get_ms();
    while (mod::time::get_ms() - START < 90'000) {
        hc = xhci_default_controller();
        if (hc != nullptr) {
            for (auto& dev : hc->devices) {
                if (dev.state.load(std::memory_order_acquire) == UsbDeviceState::CONFIGURED) {
                    device = &dev;
                    break;
                }
            }
        }
        bool const PACKETS = !dma_test_flag("--xhci-test-packets") || dma_test_packet_echoes.load(std::memory_order_acquire) >= 64;
        bool const HELD = !dma_test_flag("--xhci-test-hold-slot-submit") || dma_test_submit_held.load(std::memory_order_acquire);
        if (device != nullptr && PACKETS && HELD) break;
        mod::sched::kern_sleep_us(1000);
    }
    bool ok = device != nullptr;
    mod::sched::kern_sleep_us(1'000'000);
    if (dma_test_flag("--xhci-test-terminal-power")) {
        log::info("terminal power fixture ready=%u", ok);
        mod::power::selftest_shutdown();
    }
    auto const REQUEST = mod::sched::request_kernel_threads_shutdown(1000);
    if (dma_test_flag("--xhci-test-hold-slot-submit")) {
        auto const WAIT = mod::time::get_ms();
        while (dma_test_protected_yields.load(std::memory_order_acquire) < 2 && mod::time::get_ms() - WAIT < 2000)
            mod::sched::kern_sleep_us(1000);
        ok = ok && dma_test_protected_yields.load(std::memory_order_acquire) >= 2;
    }
    auto const BEFORE = hc != nullptr ? dma::stats(hc->pci) : dma::Stats{};
    int const RESULT = xhci_shutdown();
    int const AGAIN = xhci_shutdown();
    auto const AFTER = hc != nullptr ? dma::stats(hc->pci) : dma::Stats{};
    bool const WITHHELD = dma_test_flag("--xhci-test-withhold-terminal-halt");
    bool const ACCOUNTED =
        WITHHELD ? AFTER.regions == BEFORE.regions && AFTER.backing_pages == BEFORE.backing_pages && AFTER.quarantined == AFTER.regions
                 : AFTER.regions == 0 && AFTER.backing_pages == 0;
    bool late = false;
    if (device != nullptr) {
        UsbSetupPacket setup{};
        std::array<uint8_t, 1> buffer{};
        late = xhci_control_transfer(hc, device->slot_id, &setup, nullptr, 0, false) == -ENODEV &&
               xhci_bulk_transfer(hc, device->slot_id, &device->endpoints.at(1), buffer.data(), 1) == -ENODEV &&
               configure_endpoint(hc, device->slot_id, device->input_ctx_phys) == -EIO;
    }
    ok = ok && ACCOUNTED && late && hc != nullptr && !hc->irq_owned && hc->lifecycle_admission.drained() && hc->io_admission.drained() &&
         RESULT == (WITHHELD ? -EIO : 0) && AGAIN == RESULT;
    log::info(
        "terminal live %s result=%d repeat=%d late=%u accounted=%u shutdown_targets=%zu protected_yields=%zu regions=%zu->%zu "
        "pages=%zu->%zu q=%zu",
        ok ? "PASS" : "FAIL", RESULT, AGAIN, late, ACCOUNTED, REQUEST.requested, dma_test_protected_yields.load(), BEFORE.regions,
        AFTER.regions, BEFORE.backing_pages, AFTER.backing_pages, AFTER.quarantined);
    mod::power::selftest_shutdown();
}
}  // namespace

auto xhci_dma_packet_test_enabled() -> bool { return dma_test_flag("--xhci-test-packets"); }

auto xhci_dma_selftest() -> XhciDmaTestResult {
    XhciDmaTestResult r;
    pci::PCIDevice* target = nullptr;
    for (size_t i = 0; i < pci::pci_device_count(); ++i) {
        auto* p = pci::pci_get_device(i);
        if (p != nullptr && p->class_code == pci::PCI_CLASS_SERIAL_BUS && p->subclass_code == pci::PCI_SUBCLASS_USB &&
            p->prog_if == pci::PCI_PROG_IF_XHCI) {
            target = p;
            break;
        }
    }
    if (target == nullptr) return r;
    r.found = true;
    r.fail_allocate = dma_test_flag("--xhci-test-fail-allocate");
    r.withheld = dma_test_flag("--xhci-test-withhold-halt");
    r.before = dma::stats(target);
    dma_test_probing = true;
    dma_test_withhold_halt = r.withheld;
    dma_test_allocations_left = r.fail_allocate ? 2 : SIZE_MAX;
    r.prepared = init_controller(target) == 0;
    r.mapped = dma::stats(target);
    if (dma_test_controller != nullptr)
        r.released = cleanup_controller(dma_test_controller, dma_test_controller->irq_owned);
    else
        r.released = r.mapped.regions == 0;
    r.after = dma::stats(target);
    r.bme_off = (pci::pci_config_read16(target->bus, target->slot, target->function, pci::PCI_COMMAND) & pci::PCI_COMMAND_BUS_MASTER) == 0;
    log::info("DMA test prepared=%u fail_allocate=%u withheld=%u released=%u bme_off=%u regions=%zu->%zu pages=%zu->%zu q=%zu", r.prepared,
              r.fail_allocate, r.withheld, r.released, r.bme_off, r.mapped.regions, r.after.regions, r.mapped.backing_pages,
              r.after.backing_pages, r.after.quarantined);
    dma_test_probing = false;
    return r;
}
#endif

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
#ifdef WOS_SELFTEST
    if (found != 0 && (dma_test_flag("--xhci-test-terminal") || dma_test_flag("--xhci-test-terminal-power"))) {
        auto* task = mod::sched::task::Task::create_kernel_thread("xhci_dma_terminal", dma_terminal_runner);
        if (task == nullptr || !mod::sched::post_task_balanced(task)) log::error("terminal fixture failed to start");
    }
#endif
    return found;
}

auto xhci_shutdown() -> int {
    if (mod::sched::can_query_current_task() && !mod::mm::phys::can_wait_for_reclaim()) return -EWOULDBLOCK;
    shutdown_started.store(true, std::memory_order_release);
    int result = 0;
    auto const COUNT = controller_count.load(std::memory_order_acquire);
    for (size_t i = 0; i < COUNT; ++i) {
        if (controllers.at(i) == nullptr) continue;
        int const RET = retire_controller(*controllers.at(i));
        if (RET != 0) result = RET;
    }
    return result;
}

}  // namespace ker::dev::usb
