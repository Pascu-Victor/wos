#include "xhci.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <cstring>
#include <new>  // IWYU pragma: keep
#include <platform/dbg/dbg.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <utility>

#include "dev/pci.hpp"
#include "util/hcf.hpp"

namespace ker::dev::usb {

using log = ker::mod::dbg::logger<"xhci">;

constexpr size_t MAX_XHCI_CONTROLLERS = 2;
// NOLINTBEGIN(misc-use-internal-linkage)
std::array<XhciController*, MAX_XHCI_CONTROLLERS> controllers{};
size_t controller_count = 0;
// NOLINTEND(misc-use-internal-linkage)

namespace {
UsbClassDriver* class_drivers = nullptr;
constexpr size_t PAGE_SIZE = 4096;

// -- MMIO helpers --
auto read32(const volatile uint8_t* base, uint32_t offset) -> uint32_t {
    return *reinterpret_cast<const volatile uint32_t*>(base + offset);
}

void write32(volatile uint8_t* base, uint32_t offset, uint32_t val) { *reinterpret_cast<volatile uint32_t*>(base + offset) = val; }

[[maybe_unused]]
auto read64(const volatile uint8_t* base, uint32_t offset) -> uint64_t {
    return *reinterpret_cast<const volatile uint64_t*>(base + offset);
}

void write64(volatile uint8_t* base, uint32_t offset, uint64_t val) { *reinterpret_cast<volatile uint64_t*>(base + offset) = val; }

auto virt_to_phys(void* v) -> uint64_t {
    auto addr = reinterpret_cast<uint64_t>(v);
    if (addr >= 0xffffffff80000000ULL) {
        uint64_t const PHYS = mod::mm::virt::translate(mod::mm::virt::get_kernel_pagemap(), addr);
        if (PHYS == mod::mm::virt::PADDR_INVALID) {
            log::error("virt_to_phys failed for kernel address 0x%lx", addr);
            hcf();
        }
        return PHYS;
    }
    return reinterpret_cast<uint64_t>(mod::mm::addr::get_phys_pointer(addr));
}

// Allocate a page-aligned zeroed buffer, return {virt, phys}
struct Alloc {
    void* virt;
    uint64_t phys;
};

auto alloc_page() -> Alloc {
    void* v = mod::mm::phys::page_alloc(PAGE_SIZE);
    if (v == nullptr) {
        return {.virt = nullptr, .phys = 0};
    }
    std::memset(v, 0, PAGE_SIZE);
    return {.virt = v, .phys = virt_to_phys(v)};
}

auto alloc_pages(size_t bytes) -> Alloc {
    // Round up to page
    size_t const ALLOC_BYTES = ((bytes + PAGE_SIZE - 1) / PAGE_SIZE) * PAGE_SIZE;
    void* v = mod::mm::phys::page_alloc(ALLOC_BYTES);
    if (v == nullptr) {
        return {.virt = nullptr, .phys = 0};
    }
    std::memset(v, 0, ALLOC_BYTES);
    return {.virt = v, .phys = virt_to_phys(v)};
}

// -- Ring operations --
void ring_enqueue(Trb* ring, size_t* enqueue, bool* cycle, size_t ring_size, uint64_t param, uint32_t status, uint32_t control) {
    size_t idx = *enqueue;
    ring[idx].param = param;
    ring[idx].status = status;
    ring[idx].control = (control & ~TRB_CYCLE) | (*cycle ? TRB_CYCLE : 0);

    idx++;
    if (idx >= ring_size - 1) {
        // Write link TRB
        ring[idx].param = virt_to_phys(&ring[0]);
        ring[idx].status = 0;
        ring[idx].control = TRB_LINK | TRB_TOGGLE_CYCLE | (*cycle ? TRB_CYCLE : 0);
        *cycle = !*cycle;
        idx = 0;
    }
    *enqueue = idx;
}

void ring_doorbell(volatile uint32_t* db, uint32_t slot, uint32_t target) { db[slot] = target; }

// -- Command helpers --
auto send_command(XhciController* hc, uint64_t param, uint32_t status, uint32_t control) -> int {
    hc->cmd_lock.lock();
    hc->cmd_done = false;
    hc->cmd_result = 0;
    hc->cmd_slot_id = 0;

    ring_enqueue(hc->cmd_ring, &hc->cmd_enqueue, &hc->cmd_cycle, CMD_RING_SIZE, param, status, control);

    // Ring command doorbell (slot 0, target 0)
    ring_doorbell(hc->db, 0, 0);

    // Poll for completion (with timeout)
    for (int i = 0; i < 1000000; i++) {
        if (hc->cmd_done) {
            uint32_t const CC = (hc->cmd_result >> 24) & 0xFF;
            hc->cmd_lock.unlock();
            return (CC == TRB_CC_SUCCESS) ? 0 : -1;
        }
        asm volatile("pause");
    }

    hc->cmd_lock.unlock();
    log::warn("command timeout");
    return -1;
}

auto enable_slot(XhciController* hc) -> int {
    hc->cmd_lock.lock();
    hc->cmd_done = false;
    hc->cmd_result = 0;
    hc->cmd_slot_id = 0;

    ring_enqueue(hc->cmd_ring, &hc->cmd_enqueue, &hc->cmd_cycle, CMD_RING_SIZE, 0, 0, TRB_ENABLE_SLOT);
    ring_doorbell(hc->db, 0, 0);

    for (int i = 0; i < 1000000; i++) {
        if (hc->cmd_done) {
            uint32_t const CC = (hc->cmd_result >> 24) & 0xFF;
            uint32_t const SLOT = hc->cmd_slot_id;
            hc->cmd_lock.unlock();
            if (CC != TRB_CC_SUCCESS) {
                return -1;
            }
            return static_cast<int>(SLOT);
        }
        asm volatile("pause");
    }
    hc->cmd_lock.unlock();
    return -1;
}

auto address_device(XhciController* hc, uint8_t slot_id, uint64_t input_ctx_phys) -> int {
    return send_command(hc, input_ctx_phys, 0, TRB_ADDRESS_DEVICE | (static_cast<uint32_t>(slot_id) << 24));
}

// -- Endpoint context helpers --
// EP context DCI: for EP0 = 1, for EPn OUT = 2*n, for EPn IN = 2*n+1
auto ep_dci(uint8_t ep_addr) -> uint8_t {
    uint8_t const NUM = ep_addr & 0x0F;
    if (NUM == 0) {
        return 1;  // EP0 (control, bidirectional)
    }
    return ((ep_addr & 0x80) != 0) ? ((2 * NUM) + 1) : (2 * NUM);
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

// Set up EP0 context in input context
void setup_ep0_context(InputContext* ictx, uint8_t speed, uint16_t max_packet, uint64_t ring_phys) {
    // Slot context
    auto* s = &ictx->slot;
    // Bits 31:27 = route string (0)
    // Bits 23:20 = speed
    // Bits 31:27 of data[0] = Context Entries (1 = only EP0)
    s->data[0] = (1U << 27) | (static_cast<uint32_t>(speed) << 20);
    // data[1] bits 23:16 = root hub port number (set by caller)

    // EP0 context (index 0 in ep[] = DCI 1)
    auto* ep = &ictx->ep[0];
    // data[0] = reserved
    // data[1]: bits 2:1 = EP Type (4 = Control Bidirectional)
    //          bits 15:3 = Max Packet Size
    //          bits 23:16 = Max Burst Size
    ep->data[1] = (4U << 1) | (static_cast<uint32_t>(max_packet) << 16);
    // data[2] = TR Dequeue Pointer low (with DCS bit 0 = 1)
    ep->data[2] = static_cast<uint32_t>(ring_phys) | 1;  // DCS=1
    ep->data[3] = static_cast<uint32_t>(ring_phys >> 32);
    // data[4]: bits 15:0 = Average TRB Length
    ep->data[4] = 8;  // For control EP, average = 8 (setup packet)
}

// Allocate a transfer ring for an endpoint
[[maybe_unused]]
auto alloc_transfer_ring() -> Trb* {
    auto a = alloc_pages(XFER_RING_SIZE * sizeof(Trb));
    return static_cast<Trb*>(a.virt);
}

// -- Device enumeration --
void enumerate_device(XhciController* hc, uint8_t port, uint8_t speed);
void probe_class_drivers(XhciController* hc, UsbDevice* dev, uint8_t* config_data, size_t config_len);

// -- Event processing --
void process_event(XhciController* hc, Trb* evt) {
    uint32_t const TYPE = evt->control & TRB_TYPE_MASK;

    if (TYPE == TRB_CMD_COMPLETION) {
        hc->cmd_result = evt->status;
        hc->cmd_slot_id = (evt->control >> 24) & 0xFF;
        hc->cmd_done = true;
    } else if (TYPE == TRB_PORT_STATUS_CHG) {
        uint8_t const PORT_ID = ((evt->param >> 24) & 0xFF);
        if (PORT_ID == 0 || PORT_ID > hc->max_ports) {
            return;
        }
        uint32_t const PORTSC_OFF = XHCI_OP_PORTSC + ((PORT_ID - 1) * 0x10);
        uint32_t const PORTSC = read32(hc->op, PORTSC_OFF);

        // Acknowledge status change bits
        uint32_t ack = PORTSC & ~XHCI_PORTSC_PED;  // Don't clear PED
        ack &= ~XHCI_PORTSC_W1C_MASK;
        ack |= (PORTSC & (XHCI_PORTSC_CSC | XHCI_PORTSC_PEC | XHCI_PORTSC_PRC));
        write32(hc->op, PORTSC_OFF, ack);

        if ((PORTSC & XHCI_PORTSC_CCS) != 0U) {
            uint8_t const SPD = (PORTSC & XHCI_PORTSC_SPEED_MASK) >> XHCI_PORTSC_SPEED_SHIFT;
            log::debug("port %u connect speed=%u", PORT_ID, SPD);

            // For USB2 ports, need to reset. USB3 ports auto-enable.
            if (SPD < XHCI_SPEED_SUPER) {
                // Issue port reset
                uint32_t val = read32(hc->op, PORTSC_OFF);
                val &= ~XHCI_PORTSC_W1C_MASK;
                val |= XHCI_PORTSC_PR;
                write32(hc->op, PORTSC_OFF, val);
            } else {
                enumerate_device(hc, PORT_ID, SPD);
            }
        }
    } else if (TYPE == TRB_TRANSFER_EVENT) {
        // Transfer completion - wake blocked task
        // For now, just mark the command as done
        // (used for synchronous control/bulk transfers)
        hc->cmd_result = evt->status;
        hc->cmd_done = true;
    }
}

void process_events(XhciController* hc) {
    while (true) {
        Trb* evt = &hc->evt_ring[hc->evt_dequeue];
        bool const CYCLE = (evt->control & TRB_CYCLE) != 0;
        if (CYCLE != hc->evt_cycle) {
            break;  // No more events
        }

        process_event(hc, evt);

        hc->evt_dequeue++;
        if (hc->evt_dequeue >= EVENT_RING_SIZE) {
            hc->evt_dequeue = 0;
            hc->evt_cycle = !hc->evt_cycle;
        }
    }

    // Update ERDP
    uint64_t const ERDP_PHYS = hc->evt_ring_phys + (hc->evt_dequeue * sizeof(Trb));
    write64(hc->rt, XHCI_RT_ERDP, ERDP_PHYS | (1U << 3));  // EHB bit
}

void xhci_irq(uint8_t /*unused*/, void* data) {
    auto* hc = static_cast<XhciController*>(data);
    if (hc == nullptr) {
        return;
    }

    uint32_t const STS = read32(hc->op, XHCI_OP_USBSTS);
    if ((STS & XHCI_STS_EINT) == 0U) {
        return;
    }

    // Acknowledge
    write32(hc->op, XHCI_OP_USBSTS, XHCI_STS_EINT);

    // Clear IMAN IP
    uint32_t const IMAN = read32(hc->rt, XHCI_RT_IMAN);
    write32(hc->rt, XHCI_RT_IMAN, IMAN | XHCI_IMAN_IP);

    process_events(hc);
}

// -- Device enumeration implementation --
void enumerate_device(XhciController* hc, uint8_t port, uint8_t speed) {
    // 1. Enable Slot
    int const SLOT = enable_slot(hc);
    if (SLOT <= 0 || std::cmp_greater_equal(SLOT, MAX_XHCI_SLOTS)) {
        log::warn("enable slot failed");
        return;
    }

    log::debug("slot %u for port %u", SLOT, port);

    auto& dev = hc->devices.at(static_cast<size_t>(SLOT));
    dev.slot_id = static_cast<uint8_t>(SLOT);
    dev.port = port;
    dev.speed = speed;
    dev.active = true;
    dev.max_packet0 = max_packet_for_speed(speed);

    // 2. Allocate device context
    auto dc_alloc = alloc_page();
    if (dc_alloc.virt == nullptr) {
        return;
    }
    dev.dev_ctx = new (dc_alloc.virt) DeviceContext{};
    hc->dcbaap[SLOT] = dc_alloc.phys;

    // 3. Allocate input context
    auto ic_alloc = alloc_page();
    if (ic_alloc.virt == nullptr) {
        return;
    }
    dev.input_ctx = new (ic_alloc.virt) InputContext{};
    dev.input_ctx_phys = ic_alloc.phys;

    // 4. Allocate EP0 transfer ring
    auto ep0_ring = alloc_pages(XFER_RING_SIZE * sizeof(Trb));
    if (ep0_ring.virt == nullptr) {
        return;
    }
    auto& ep0 = dev.endpoints.at(0);
    ep0.ring = static_cast<Trb*>(ep0_ring.virt);
    ep0.ring_phys = ep0_ring.phys;
    ep0.ring_enqueue = 0;
    ep0.ring_cycle = true;
    ep0.address = 0;
    ep0.type = USB_EP_TYPE_CONTROL;
    ep0.max_packet = dev.max_packet0;
    dev.num_endpoints = 1;

    // 5. Set up input context
    dev.input_ctx->add_flags = (1 << 0) | (1 << 1);  // Slot + EP0
    dev.input_ctx->drop_flags = 0;
    setup_ep0_context(dev.input_ctx, speed, dev.max_packet0, ep0_ring.phys);
    // Set root hub port in slot context
    dev.input_ctx->slot.data[1] = (static_cast<uint32_t>(port) << 16);

    // 6. Address Device
    if (address_device(hc, dev.slot_id, dev.input_ctx_phys) != 0) {
        log::warn("address device failed");
        dev.active = false;
        return;
    }

    // 7. Get Device Descriptor
    UsbDeviceDescriptor desc = {};
    UsbSetupPacket setup = {};
    setup.bm_request_type = 0x80;  // Device-to-host, standard, device
    setup.b_request = USB_REQ_GET_DESCRIPTOR;
    setup.w_value = (USB_DESC_DEVICE << 8);
    setup.w_index = 0;
    setup.w_length = sizeof(UsbDeviceDescriptor);

    if (xhci_control_transfer(hc, dev.slot_id, &setup, &desc, sizeof(desc), true) != 0) {
        log::warn("get device descriptor failed");
        return;
    }

    dev.vendor_id = desc.id_vendor;
    dev.product_id = desc.id_product;
    dev.device_class = desc.b_device_class;
    dev.device_subclass = desc.b_device_sub_class;
    dev.device_protocol = desc.b_device_protocol;

    log::info("Usb device %04x:%04x class=0x%02x", desc.id_vendor, desc.id_product, desc.b_device_class);

    // 8. Get Configuration Descriptor
    if (desc.b_num_configurations == 0) {
        return;
    }

    // First get just the config header to learn wTotalLength
    std::array<uint8_t, 256> config_buf{};
    setup.w_value = (USB_DESC_CONFIG << 8);
    setup.w_length = sizeof(UsbConfigDescriptor);
    if (xhci_control_transfer(hc, dev.slot_id, &setup, config_buf.data(), sizeof(UsbConfigDescriptor), true) != 0) {
        log::warn("get config descriptor failed");
        return;
    }

    auto* cfg = reinterpret_cast<UsbConfigDescriptor*>(config_buf.data());
    uint16_t total_len = cfg->w_total_length;
    total_len = std::min<uint16_t>(total_len, config_buf.size());

    // Now fetch full config
    setup.w_length = total_len;
    if (xhci_control_transfer(hc, dev.slot_id, &setup, config_buf.data(), total_len, true) != 0) {
        log::warn("get full config failed");
        return;
    }

    // 9. Set Configuration
    UsbSetupPacket set_cfg = {};
    set_cfg.bm_request_type = 0x00;
    set_cfg.b_request = USB_REQ_SET_CONFIG;
    set_cfg.w_value = cfg->b_configuration_value;
    set_cfg.w_index = 0;
    set_cfg.w_length = 0;
    xhci_control_transfer(hc, dev.slot_id, &set_cfg, nullptr, 0, false);

    // 10. Probe class drivers
    probe_class_drivers(hc, &dev, config_buf.data(), total_len);
}

void probe_class_drivers(XhciController* hc, UsbDevice* dev, uint8_t* config_data, size_t config_len) {
    (void)hc;
    // Walk configuration descriptor to find interfaces
    size_t offset = 0;
    while (offset + 2 <= config_len) {
        uint8_t const LEN = config_data[offset];
        uint8_t const TYPE = config_data[offset + 1];
        if (LEN == 0) {
            break;
        }
        if (offset + LEN > config_len) {
            break;
        }

        if (TYPE == USB_DESC_INTERFACE) {
            auto* iface = reinterpret_cast<UsbInterfaceDescriptor*>(config_data + offset);
            for (auto* drv = class_drivers; drv != nullptr; drv = drv->next) {
                if (drv->probe(dev, iface)) {
                    log::info("class driver '%s' matched", drv->name);
                    drv->attach(dev, iface, config_data, config_len);
                    return;
                }
            }
        }
        offset += LEN;
    }
}

// -- Scan ports on init --
void scan_ports(XhciController* hc) {
    for (uint8_t p = 1; p <= hc->max_ports; p++) {
        uint32_t const PORTSC = read32(hc->op, XHCI_OP_PORTSC + ((p - 1) * 0x10));
        if ((PORTSC & XHCI_PORTSC_CCS) != 0U) {
            uint8_t const SPD = (PORTSC & XHCI_PORTSC_SPEED_MASK) >> XHCI_PORTSC_SPEED_SHIFT;
            log::debug("port %u already connected speed=%u", p, SPD);

            if (SPD >= XHCI_SPEED_SUPER || (PORTSC & XHCI_PORTSC_PED) != 0U) {
                enumerate_device(hc, p, SPD);
            } else {
                // Issue port reset
                uint32_t val = PORTSC & ~XHCI_PORTSC_W1C_MASK;
                val |= XHCI_PORTSC_PR;
                write32(hc->op, XHCI_OP_PORTSC + ((p - 1) * 0x10), val);
                // Enumeration will happen on PRC event
            }
        }
    }
}

// -- Controller init --
auto init_controller(pci::PCIDevice* pci_dev) -> int {
    if (controller_count >= MAX_XHCI_CONTROLLERS) {
        return -1;
    }

    // Enable bus mastering + memory space
    pci::pci_enable_bus_master(pci_dev);
    pci::pci_enable_memory_space(pci_dev);

    // Map BAR0 (MMIO) into kernel page table
    auto* bar0_ptr = pci::pci_map_bar(pci_dev, 0);
    if (bar0_ptr == nullptr) {
        log::error("BAR0 is zero");
        return -1;
    }

    auto* base = reinterpret_cast<volatile uint8_t*>(bar0_ptr);

    // Read capability registers
    uint8_t const CAP_LENGTH = *base;
    uint32_t const HCSPARAMS1 = read32(base, XHCI_CAP_HCSPARAMS1);
    uint32_t const HCSPARAMS2 = read32(base, XHCI_CAP_HCSPARAMS2);
    uint32_t const HCCPARAMS1 = read32(base, XHCI_CAP_HCCPARAMS1);
    uint32_t const DBOFF = read32(base, XHCI_CAP_DBOFF);
    uint32_t const RTSOFF = read32(base, XHCI_CAP_RTSOFF);

    uint8_t max_slots = (HCSPARAMS1 >> 0) & 0xFF;
    uint16_t const MAX_INTRS = (HCSPARAMS1 >> 8) & 0x7FF;
    uint8_t max_ports = (HCSPARAMS1 >> 24) & 0xFF;
    bool const CTX64 = (HCCPARAMS1 & (1 << 2)) != 0;

    max_slots = std::min<size_t>(max_slots, MAX_XHCI_SLOTS);
    max_ports = std::min<size_t>(max_ports, MAX_XHCI_PORTS);

    log::info("slots=%u ports=%u intrs=%u ctx64=%u", max_slots, max_ports, MAX_INTRS, CTX64 ? 1U : 0U);

    auto* op = const_cast<volatile uint8_t*>(base + CAP_LENGTH);
    auto* rt = const_cast<volatile uint8_t*>(base + RTSOFF);
    auto* db = reinterpret_cast<volatile uint32_t*>(const_cast<volatile uint8_t*>(base + DBOFF));

    // 1. Stop controller
    uint32_t cmd = read32(op, XHCI_OP_USBCMD);
    cmd &= ~XHCI_CMD_RUN;
    write32(op, XHCI_OP_USBCMD, cmd);

    // Wait for halted
    for (int i = 0; i < 100000; i++) {
        if ((read32(op, XHCI_OP_USBSTS) & XHCI_STS_HCH) != 0U) {
            break;
        }
        asm volatile("pause");
    }

    // 2. Reset controller
    write32(op, XHCI_OP_USBCMD, XHCI_CMD_HCRST);
    for (int i = 0; i < 100000; i++) {
        uint32_t const C = read32(op, XHCI_OP_USBCMD);
        uint32_t const S = read32(op, XHCI_OP_USBSTS);
        if (((C & XHCI_CMD_HCRST) == 0U) && ((S & XHCI_STS_CNR) == 0U)) {
            break;
        }
        asm volatile("pause");
    }

    // Allocate controller state
    // NOLINTNEXTLINE(misc-const-correctness): placement-new constructs mutable controller storage.
    void* hc_storage = mod::mm::phys::page_alloc(sizeof(XhciController));
    if (hc_storage == nullptr) {
        return -1;
    }
    auto* hc = new (hc_storage) XhciController{};

    hc->base = base;
    hc->op = op;
    hc->rt = rt;
    hc->db = db;
    hc->pci = pci_dev;
    hc->max_slots = max_slots;
    hc->max_ports = max_ports;
    hc->max_intrs = MAX_INTRS;
    hc->ctx64 = CTX64;

    // 3. Set Max Device Slots Enabled
    write32(op, XHCI_OP_CONFIG, max_slots);

    // 4. Allocate DCBAA (64-byte aligned)
    auto dcbaa_alloc = alloc_page();
    if (dcbaa_alloc.virt == nullptr) {
        return -1;
    }
    hc->dcbaap = static_cast<uint64_t*>(dcbaa_alloc.virt);
    hc->dcbaap_phys = dcbaa_alloc.phys;
    write64(op, XHCI_OP_DCBAAP, hc->dcbaap_phys);

    // 5. Allocate scratchpad buffers (if needed)
    uint32_t const MAX_SCRATCH_HI = (HCSPARAMS2 >> 21) & 0x1F;
    uint32_t const MAX_SCRATCH_LO = (HCSPARAMS2 >> 27) & 0x1F;
    uint32_t const MAX_SCRATCH = (MAX_SCRATCH_HI << 5) | MAX_SCRATCH_LO;
    if (MAX_SCRATCH > 0) {
        auto sp_arr = alloc_page();
        if (sp_arr.virt == nullptr) {
            return -1;
        }
        hc->scratchpad_array = static_cast<uint64_t*>(sp_arr.virt);
        hc->scratchpad_array_phys = sp_arr.phys;

        for (uint32_t i = 0; i < MAX_SCRATCH; i++) {
            auto buf = alloc_page();
            if (buf.virt == nullptr) {
                return -1;
            }
            hc->scratchpad_array[i] = buf.phys;
        }
        hc->dcbaap[0] = hc->scratchpad_array_phys;
    }

    // 6. Allocate Command Ring
    auto cmd_alloc = alloc_pages(CMD_RING_SIZE * sizeof(Trb));
    if (cmd_alloc.virt == nullptr) {
        return -1;
    }
    hc->cmd_ring = static_cast<Trb*>(cmd_alloc.virt);
    hc->cmd_ring_phys = cmd_alloc.phys;
    hc->cmd_enqueue = 0;
    hc->cmd_cycle = true;
    // Set CRCR (with cycle bit)
    write64(op, XHCI_OP_CRCR, hc->cmd_ring_phys | 1);

    // 7. Allocate Event Ring
    auto evt_alloc = alloc_pages(EVENT_RING_SIZE * sizeof(Trb));
    if (evt_alloc.virt == nullptr) {
        return -1;
    }
    hc->evt_ring = static_cast<Trb*>(evt_alloc.virt);
    hc->evt_ring_phys = evt_alloc.phys;
    hc->evt_dequeue = 0;
    hc->evt_cycle = true;

    // ERST
    auto erst_alloc = alloc_page();
    if (erst_alloc.virt == nullptr) {
        return -1;
    }
    hc->erst = new (erst_alloc.virt) ErstEntry{};
    hc->erst_phys = erst_alloc.phys;
    hc->erst[0].ring_base = hc->evt_ring_phys;
    hc->erst[0].ring_size = EVENT_RING_SIZE;
    hc->erst[0].reserved = 0;

    // Configure interrupter 0
    write32(rt, XHCI_RT_ERSTSZ, 1);
    write64(rt, XHCI_RT_ERDP, hc->evt_ring_phys);
    write64(rt, XHCI_RT_ERSTBA, hc->erst_phys);

    // Set interrupt moderation (0 = no throttle)
    write32(rt, XHCI_RT_IMOD, 0);

    // Enable interrupter
    write32(rt, XHCI_RT_IMAN, XHCI_IMAN_IE);

    // 8. Set up IRQ
    uint8_t vector = mod::gates::allocate_vector();
    if (vector == 0) {
        log::error("no free IRQ vector");
        return -1;
    }
    hc->irq_vector = vector;

    int const MSI_RET = pci::pci_enable_msi(pci_dev, vector);
    if (MSI_RET != 0) {
        vector = pci_dev->interrupt_line + 32;
        hc->irq_vector = vector;
    }
    mod::gates::request_irq(vector, xhci_irq, hc, "xhci");

    // 9. Start controller
    cmd = read32(op, XHCI_OP_USBCMD);
    cmd |= XHCI_CMD_RUN | XHCI_CMD_INTE;
    write32(op, XHCI_OP_USBCMD, cmd);

    // Wait for not halted
    for (int i = 0; i < 100000; i++) {
        if ((read32(op, XHCI_OP_USBSTS) & XHCI_STS_HCH) == 0U) {
            break;
        }
        asm volatile("pause");
    }

    controllers.at(controller_count++) = hc;

    log::info("controller ready, vec=0x%02x", hc->irq_vector);

    // 10. Scan ports for already-connected devices
    scan_ports(hc);

    return 0;
}

}  // namespace

// -- Public API --

auto configure_endpoint(XhciController* hc, uint8_t slot_id, uint64_t input_ctx_phys) -> int {
    return send_command(hc, input_ctx_phys, 0, TRB_CONFIG_ENDPOINT | (static_cast<uint32_t>(slot_id) << 24));
}

void usb_register_class_driver(UsbClassDriver* drv) {
    drv->next = class_drivers;
    class_drivers = drv;
}

auto xhci_control_transfer(XhciController* hc, uint8_t slot_id, UsbSetupPacket* setup, void* data, size_t len, bool dir_in) -> int {
    auto& dev = hc->devices.at(slot_id);
    auto& ep0 = dev.endpoints.at(0);

    // Build Setup Stage TRB
    uint64_t setup_param = 0;
    std::memcpy(&setup_param, setup, sizeof(UsbSetupPacket));

    uint32_t const SETUP_STATUS = 8;  // TRB transfer length = 8
    uint32_t setup_dir = 0;
    if (len > 0) {
        setup_dir = dir_in ? (3U << 16) : (2U << 16);
    }
    uint32_t const SETUP_CTRL = TRB_SETUP | TRB_IDT | setup_dir;

    ring_enqueue(ep0.ring, &ep0.ring_enqueue, &ep0.ring_cycle, XFER_RING_SIZE, setup_param, SETUP_STATUS, SETUP_CTRL);

    // Build Data Stage TRB (if data)
    if (len > 0 && data != nullptr) {
        uint64_t const DATA_PHYS = virt_to_phys(data);
        auto const DATA_STATUS = static_cast<uint32_t>(len);
        uint32_t const DATA_CTRL = TRB_DATA | (dir_in ? TRB_DIR_IN : 0);

        ring_enqueue(ep0.ring, &ep0.ring_enqueue, &ep0.ring_cycle, XFER_RING_SIZE, DATA_PHYS, DATA_STATUS, DATA_CTRL);
    }

    // Build Status Stage TRB
    uint32_t status_ctrl = TRB_STATUS | TRB_IOC;
    if (len > 0 && !dir_in) {
        status_ctrl |= TRB_DIR_IN;  // Status stage is opposite direction
    }

    hc->cmd_done = false;
    hc->cmd_result = 0;

    ring_enqueue(ep0.ring, &ep0.ring_enqueue, &ep0.ring_cycle, XFER_RING_SIZE, 0, 0, status_ctrl);

    // Ring EP0 doorbell (target = 1 for EP0)
    ring_doorbell(hc->db, slot_id, 1);

    // Wait for completion
    for (int i = 0; i < 2000000; i++) {
        if (hc->cmd_done) {
            uint32_t const CC = (hc->cmd_result >> 24) & 0xFF;
            if (CC == TRB_CC_SUCCESS || CC == TRB_CC_SHORT_PKT) {
                return 0;
            }
            return -1;
        }
        asm volatile("pause");
    }

    log::warn("control transfer timeout");
    return -1;
}

auto xhci_bulk_transfer(XhciController* hc, uint8_t slot_id, UsbEndpoint* ep, void* data, size_t len) -> int {
    uint64_t const PHYS = virt_to_phys(data);
    auto const STATUS = static_cast<uint32_t>(len);
    uint32_t const CTRL = TRB_NORMAL | TRB_IOC;

    hc->cmd_done = false;
    hc->cmd_result = 0;

    ring_enqueue(ep->ring, &ep->ring_enqueue, &ep->ring_cycle, XFER_RING_SIZE, PHYS, STATUS, CTRL);

    // Ring doorbell: slot_id, target = DCI
    uint8_t const DCI = ep_dci(ep->address);
    ring_doorbell(hc->db, slot_id, DCI);

    // Wait
    for (int i = 0; i < 5000000; i++) {
        if (hc->cmd_done) {
            uint32_t const CC = (hc->cmd_result >> 24) & 0xFF;
            if (CC == TRB_CC_SUCCESS || CC == TRB_CC_SHORT_PKT) {
                return 0;
            }
            return -1;
        }
        asm volatile("pause");
    }

    log::warn("bulk transfer timeout");
    return -1;
}

auto xhci_default_controller() -> XhciController* {
    if (controller_count == 0) {
        return nullptr;
    }
    return controllers.at(0);
}

auto xhci_init() -> int {
    int found = 0;

    size_t const COUNT = pci::pci_device_count();
    for (size_t i = 0; i < COUNT; i++) {
        auto* dev = pci::pci_get_device(i);
        if (dev == nullptr) {
            continue;
        }

        // xHCI: class=0x0C, subclass=0x03, prog_if=0x30
        if (dev->class_code == pci::PCI_CLASS_SERIAL_BUS && dev->subclass_code == pci::PCI_SUBCLASS_USB &&
            dev->prog_if == pci::PCI_PROG_IF_XHCI) {
            log::info("found controller at PCI %02x:%02x.%x", dev->bus, dev->slot, dev->function);

            if (init_controller(dev) == 0) {
                found++;
            }
        }
    }

    if (found == 0) {
        log::info("no controllers found");
    }

    return found;
}

}  // namespace ker::dev::usb
