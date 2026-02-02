#include "xhci.hpp"

#include <cstring>
#include <mod/io/serial/serial.hpp>
#include <platform/interrupt/gates.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>

namespace ker::dev::usb {

constexpr size_t MAX_XHCI_CONTROLLERS = 2;
XhciController* controllers[MAX_XHCI_CONTROLLERS] = {};
size_t controller_count = 0;

namespace {
UsbClassDriver* class_drivers = nullptr;

// ── MMIO helpers ──
auto read32(volatile uint8_t* base, uint32_t offset) -> uint32_t { return *reinterpret_cast<volatile uint32_t*>(base + offset); }

void write32(volatile uint8_t* base, uint32_t offset, uint32_t val) { *reinterpret_cast<volatile uint32_t*>(base + offset) = val; }

auto read64(volatile uint8_t* base, uint32_t offset) -> uint64_t { return *reinterpret_cast<volatile uint64_t*>(base + offset); }

void write64(volatile uint8_t* base, uint32_t offset, uint64_t val) { *reinterpret_cast<volatile uint64_t*>(base + offset) = val; }

auto virt_to_phys(void* v) -> uint64_t {
    auto addr = reinterpret_cast<uint64_t>(v);
    if (addr >= 0xffffffff80000000ULL) {
        return mod::mm::virt::translate(mod::mm::virt::getKernelPagemap(), addr);
    }
    return reinterpret_cast<uint64_t>(mod::mm::addr::getPhysPointer(addr));
}

// Allocate a page-aligned zeroed buffer, return {virt, phys}
struct Alloc {
    void* virt;
    uint64_t phys;
};

auto alloc_page() -> Alloc {
    void* v = mod::mm::phys::pageAlloc(4096);
    if (v == nullptr) return {nullptr, 0};
    std::memset(v, 0, 4096);
    return {v, virt_to_phys(v)};
}

auto alloc_pages(size_t bytes) -> Alloc {
    // Round up to page
    size_t pages = (bytes + 4095) / 4096;
    void* v = mod::mm::phys::pageAlloc(pages * 4096);
    if (v == nullptr) return {nullptr, 0};
    std::memset(v, 0, pages * 4096);
    return {v, virt_to_phys(v)};
}

// ── Ring operations ──
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

// ── Command helpers ──
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
            uint32_t cc = (hc->cmd_result >> 24) & 0xFF;
            hc->cmd_lock.unlock();
            return (cc == TRB_CC_SUCCESS) ? 0 : -1;
        }
        asm volatile("pause");
    }

    hc->cmd_lock.unlock();
    mod::io::serial::write("xhci: command timeout\n");
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
            uint32_t cc = (hc->cmd_result >> 24) & 0xFF;
            uint32_t slot = hc->cmd_slot_id;
            hc->cmd_lock.unlock();
            if (cc != TRB_CC_SUCCESS) return -1;
            return static_cast<int>(slot);
        }
        asm volatile("pause");
    }
    hc->cmd_lock.unlock();
    return -1;
}

auto address_device(XhciController* hc, uint8_t slot_id, uint64_t input_ctx_phys) -> int {
    return send_command(hc, input_ctx_phys, 0, TRB_ADDRESS_DEVICE | (static_cast<uint32_t>(slot_id) << 24));
}

// ── Endpoint context helpers ──
// EP context DCI: for EP0 = 1, for EPn OUT = 2*n, for EPn IN = 2*n+1
auto ep_dci(uint8_t ep_addr) -> uint8_t {
    uint8_t num = ep_addr & 0x0F;
    if (num == 0) return 1;  // EP0 (control, bidirectional)
    return (ep_addr & 0x80) ? (2 * num + 1) : (2 * num);
}

auto max_packet_for_speed(uint8_t speed) -> uint16_t {
    switch (speed) {
        case USB_SPEED_LOW:
            return 8;
        case USB_SPEED_FULL:
            return 64;
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
    s->data[0] = (1u << 27) | (static_cast<uint32_t>(speed) << 20);
    // data[1] bits 23:16 = root hub port number (set by caller)

    // EP0 context (index 0 in ep[] = DCI 1)
    auto* ep = &ictx->ep[0];
    // data[0] = reserved
    // data[1]: bits 2:1 = EP Type (4 = Control Bidirectional)
    //          bits 15:3 = Max Packet Size
    //          bits 23:16 = Max Burst Size
    ep->data[1] = (4u << 1) | (static_cast<uint32_t>(max_packet) << 16);
    // data[2] = TR Dequeue Pointer low (with DCS bit 0 = 1)
    ep->data[2] = static_cast<uint32_t>(ring_phys) | 1;  // DCS=1
    ep->data[3] = static_cast<uint32_t>(ring_phys >> 32);
    // data[4]: bits 15:0 = Average TRB Length
    ep->data[4] = 8;  // For control EP, average = 8 (setup packet)
}

// Allocate a transfer ring for an endpoint
auto alloc_transfer_ring() -> Trb* {
    auto a = alloc_pages(XFER_RING_SIZE * sizeof(Trb));
    return static_cast<Trb*>(a.virt);
}

// ── Device enumeration ──
void enumerate_device(XhciController* hc, uint8_t port, uint8_t speed);
void probe_class_drivers(XhciController* hc, UsbDevice* dev, uint8_t* config_data, size_t config_len);

// ── Event processing ──
void process_event(XhciController* hc, Trb* evt) {
    uint32_t type = evt->control & TRB_TYPE_MASK;

    if (type == TRB_CMD_COMPLETION) {
        hc->cmd_result = evt->status;
        hc->cmd_slot_id = (evt->control >> 24) & 0xFF;
        hc->cmd_done = true;
    } else if (type == TRB_PORT_STATUS_CHG) {
        uint8_t port_id = ((evt->param >> 24) & 0xFF);
        if (port_id == 0 || port_id > hc->max_ports) return;
        uint32_t portsc_off = XHCI_OP_PORTSC + (port_id - 1) * 0x10;
        uint32_t portsc = read32(hc->op, portsc_off);

        // Acknowledge status change bits
        uint32_t ack = portsc & ~XHCI_PORTSC_PED;  // Don't clear PED
        ack &= ~XHCI_PORTSC_W1C_MASK;
        ack |= (portsc & (XHCI_PORTSC_CSC | XHCI_PORTSC_PEC | XHCI_PORTSC_PRC));
        write32(hc->op, portsc_off, ack);

        if (portsc & XHCI_PORTSC_CCS) {
            uint8_t spd = (portsc & XHCI_PORTSC_SPEED_MASK) >> XHCI_PORTSC_SPEED_SHIFT;
            mod::io::serial::write("xhci: port ");
            mod::io::serial::writeHex(port_id);
            mod::io::serial::write(" connect speed=");
            mod::io::serial::writeHex(spd);
            mod::io::serial::write("\n");

            // For USB2 ports, need to reset. USB3 ports auto-enable.
            if (spd < XHCI_SPEED_SUPER) {
                // Issue port reset
                uint32_t val = read32(hc->op, portsc_off);
                val &= ~XHCI_PORTSC_W1C_MASK;
                val |= XHCI_PORTSC_PR;
                write32(hc->op, portsc_off, val);
            } else {
                enumerate_device(hc, port_id, spd);
            }
        }
    } else if (type == TRB_TRANSFER_EVENT) {
        // Transfer completion — wake blocked task
        // For now, just mark the command as done
        // (used for synchronous control/bulk transfers)
        hc->cmd_result = evt->status;
        hc->cmd_done = true;
    }
}

void process_events(XhciController* hc) {
    while (true) {
        Trb* evt = &hc->evt_ring[hc->evt_dequeue];
        bool cycle = (evt->control & TRB_CYCLE) != 0;
        if (cycle != hc->evt_cycle) break;  // No more events

        process_event(hc, evt);

        hc->evt_dequeue++;
        if (hc->evt_dequeue >= EVENT_RING_SIZE) {
            hc->evt_dequeue = 0;
            hc->evt_cycle = !hc->evt_cycle;
        }
    }

    // Update ERDP
    uint64_t erdp_phys = hc->evt_ring_phys + hc->evt_dequeue * sizeof(Trb);
    write64(hc->rt, XHCI_RT_ERDP, erdp_phys | (1u << 3));  // EHB bit
}

void xhci_irq(uint8_t, void* data) {
    auto* hc = static_cast<XhciController*>(data);
    if (hc == nullptr) return;

    uint32_t sts = read32(hc->op, XHCI_OP_USBSTS);
    if (!(sts & XHCI_STS_EINT)) return;

    // Acknowledge
    write32(hc->op, XHCI_OP_USBSTS, XHCI_STS_EINT);

    // Clear IMAN IP
    uint32_t iman = read32(hc->rt, XHCI_RT_IMAN);
    write32(hc->rt, XHCI_RT_IMAN, iman | XHCI_IMAN_IP);

    process_events(hc);
}

// ── Device enumeration implementation ──
void enumerate_device(XhciController* hc, uint8_t port, uint8_t speed) {
    // 1. Enable Slot
    int slot = enable_slot(hc);
    if (slot <= 0 || slot >= static_cast<int>(MAX_XHCI_SLOTS)) {
        mod::io::serial::write("xhci: enable slot failed\n");
        return;
    }

    mod::io::serial::write("xhci: slot ");
    mod::io::serial::writeHex(slot);
    mod::io::serial::write(" for port ");
    mod::io::serial::writeHex(port);
    mod::io::serial::write("\n");

    auto& dev = hc->devices[slot];
    dev.slot_id = static_cast<uint8_t>(slot);
    dev.port = port;
    dev.speed = speed;
    dev.active = true;
    dev.max_packet0 = max_packet_for_speed(speed);

    // 2. Allocate device context
    auto dc_alloc = alloc_page();
    if (dc_alloc.virt == nullptr) return;
    dev.dev_ctx = static_cast<DeviceContext*>(dc_alloc.virt);
    hc->dcbaap[slot] = dc_alloc.phys;

    // 3. Allocate input context
    auto ic_alloc = alloc_page();
    if (ic_alloc.virt == nullptr) return;
    dev.input_ctx = static_cast<InputContext*>(ic_alloc.virt);
    dev.input_ctx_phys = ic_alloc.phys;

    // 4. Allocate EP0 transfer ring
    auto ep0_ring = alloc_pages(XFER_RING_SIZE * sizeof(Trb));
    if (ep0_ring.virt == nullptr) return;
    dev.endpoints[0].ring = static_cast<Trb*>(ep0_ring.virt);
    dev.endpoints[0].ring_phys = ep0_ring.phys;
    dev.endpoints[0].ring_enqueue = 0;
    dev.endpoints[0].ring_cycle = true;
    dev.endpoints[0].address = 0;
    dev.endpoints[0].type = USB_EP_TYPE_CONTROL;
    dev.endpoints[0].max_packet = dev.max_packet0;
    dev.num_endpoints = 1;

    // 5. Set up input context
    dev.input_ctx->add_flags = (1 << 0) | (1 << 1);  // Slot + EP0
    dev.input_ctx->drop_flags = 0;
    setup_ep0_context(dev.input_ctx, speed, dev.max_packet0, ep0_ring.phys);
    // Set root hub port in slot context
    dev.input_ctx->slot.data[1] = (static_cast<uint32_t>(port) << 16);

    // 6. Address Device
    if (address_device(hc, dev.slot_id, dev.input_ctx_phys) != 0) {
        mod::io::serial::write("xhci: address device failed\n");
        dev.active = false;
        return;
    }

    // 7. Get Device Descriptor
    UsbDeviceDescriptor desc = {};
    UsbSetupPacket setup = {};
    setup.bmRequestType = 0x80;  // Device-to-host, standard, device
    setup.bRequest = USB_REQ_GET_DESCRIPTOR;
    setup.wValue = (USB_DESC_DEVICE << 8);
    setup.wIndex = 0;
    setup.wLength = sizeof(UsbDeviceDescriptor);

    if (xhci_control_transfer(hc, dev.slot_id, &setup, &desc, sizeof(desc), true) != 0) {
        mod::io::serial::write("xhci: get device descriptor failed\n");
        return;
    }

    dev.vendor_id = desc.idVendor;
    dev.product_id = desc.idProduct;
    dev.device_class = desc.bDeviceClass;
    dev.device_subclass = desc.bDeviceSubClass;
    dev.device_protocol = desc.bDeviceProtocol;

    mod::io::serial::write("xhci: USB device ");
    mod::io::serial::writeHex(desc.idVendor);
    mod::io::serial::write(":");
    mod::io::serial::writeHex(desc.idProduct);
    mod::io::serial::write(" class=");
    mod::io::serial::writeHex(desc.bDeviceClass);
    mod::io::serial::write("\n");

    // 8. Get Configuration Descriptor
    if (desc.bNumConfigurations == 0) return;

    // First get just the config header to learn wTotalLength
    uint8_t config_buf[256] = {};
    setup.wValue = (USB_DESC_CONFIG << 8);
    setup.wLength = sizeof(UsbConfigDescriptor);
    if (xhci_control_transfer(hc, dev.slot_id, &setup, config_buf, sizeof(UsbConfigDescriptor), true) != 0) {
        mod::io::serial::write("xhci: get config descriptor failed\n");
        return;
    }

    auto* cfg = reinterpret_cast<UsbConfigDescriptor*>(config_buf);
    uint16_t total_len = cfg->wTotalLength;
    if (total_len > sizeof(config_buf)) total_len = sizeof(config_buf);

    // Now fetch full config
    setup.wLength = total_len;
    if (xhci_control_transfer(hc, dev.slot_id, &setup, config_buf, total_len, true) != 0) {
        mod::io::serial::write("xhci: get full config failed\n");
        return;
    }

    // 9. Set Configuration
    UsbSetupPacket set_cfg = {};
    set_cfg.bmRequestType = 0x00;
    set_cfg.bRequest = USB_REQ_SET_CONFIG;
    set_cfg.wValue = cfg->bConfigurationValue;
    set_cfg.wIndex = 0;
    set_cfg.wLength = 0;
    xhci_control_transfer(hc, dev.slot_id, &set_cfg, nullptr, 0, false);

    // 10. Probe class drivers
    probe_class_drivers(hc, &dev, config_buf, total_len);
}

void probe_class_drivers(XhciController* hc, UsbDevice* dev, uint8_t* config_data, size_t config_len) {
    (void)hc;
    // Walk configuration descriptor to find interfaces
    size_t offset = 0;
    while (offset + 2 <= config_len) {
        uint8_t len = config_data[offset];
        uint8_t type = config_data[offset + 1];
        if (len == 0) break;
        if (offset + len > config_len) break;

        if (type == USB_DESC_INTERFACE) {
            auto* iface = reinterpret_cast<UsbInterfaceDescriptor*>(config_data + offset);
            for (auto* drv = class_drivers; drv != nullptr; drv = drv->next) {
                if (drv->probe(dev, iface)) {
                    mod::io::serial::write("xhci: class driver '");
                    mod::io::serial::write(drv->name);
                    mod::io::serial::write("' matched\n");
                    drv->attach(dev, iface, config_data, config_len);
                    return;
                }
            }
        }
        offset += len;
    }
}

// ── Scan ports on init ──
void scan_ports(XhciController* hc) {
    for (uint8_t p = 1; p <= hc->max_ports; p++) {
        uint32_t portsc = read32(hc->op, XHCI_OP_PORTSC + (p - 1) * 0x10);
        if (portsc & XHCI_PORTSC_CCS) {
            uint8_t spd = (portsc & XHCI_PORTSC_SPEED_MASK) >> XHCI_PORTSC_SPEED_SHIFT;
            mod::io::serial::write("xhci: port ");
            mod::io::serial::writeHex(p);
            mod::io::serial::write(" already connected speed=");
            mod::io::serial::writeHex(spd);
            mod::io::serial::write("\n");

            if (spd >= XHCI_SPEED_SUPER) {
                enumerate_device(hc, p, spd);
            } else if (portsc & XHCI_PORTSC_PED) {
                // Already enabled (e.g. by BIOS)
                enumerate_device(hc, p, spd);
            } else {
                // Issue port reset
                uint32_t val = portsc & ~XHCI_PORTSC_W1C_MASK;
                val |= XHCI_PORTSC_PR;
                write32(hc->op, XHCI_OP_PORTSC + (p - 1) * 0x10, val);
                // Enumeration will happen on PRC event
            }
        }
    }
}

// ── Controller init ──
auto init_controller(pci::PCIDevice* pci_dev) -> int {
    if (controller_count >= MAX_XHCI_CONTROLLERS) return -1;

    // Enable bus mastering + memory space
    pci::pci_enable_bus_master(pci_dev);
    pci::pci_enable_memory_space(pci_dev);

    // Map BAR0 (MMIO) into kernel page table
    auto* bar0_ptr = pci::pci_map_bar(pci_dev, 0);
    if (bar0_ptr == nullptr) {
        mod::io::serial::write("xhci: BAR0 is zero\n");
        return -1;
    }

    auto* base = reinterpret_cast<volatile uint8_t*>(bar0_ptr);

    // Read capability registers
    uint8_t cap_length = *base;
    uint32_t hcsparams1 = read32(base, XHCI_CAP_HCSPARAMS1);
    uint32_t hcsparams2 = read32(base, XHCI_CAP_HCSPARAMS2);
    uint32_t hccparams1 = read32(base, XHCI_CAP_HCCPARAMS1);
    uint32_t dboff = read32(base, XHCI_CAP_DBOFF);
    uint32_t rtsoff = read32(base, XHCI_CAP_RTSOFF);

    uint8_t max_slots = (hcsparams1 >> 0) & 0xFF;
    uint16_t max_intrs = (hcsparams1 >> 8) & 0x7FF;
    uint8_t max_ports = (hcsparams1 >> 24) & 0xFF;
    bool ctx64 = (hccparams1 & (1 << 2)) != 0;

    if (max_slots > MAX_XHCI_SLOTS) max_slots = MAX_XHCI_SLOTS;
    if (max_ports > MAX_XHCI_PORTS) max_ports = MAX_XHCI_PORTS;

    mod::io::serial::write("xhci: slots=");
    mod::io::serial::writeHex(max_slots);
    mod::io::serial::write(" ports=");
    mod::io::serial::writeHex(max_ports);
    mod::io::serial::write(" intrs=");
    mod::io::serial::writeHex(max_intrs);
    mod::io::serial::write(" ctx64=");
    mod::io::serial::writeHex(ctx64 ? 1 : 0);
    mod::io::serial::write("\n");

    auto* op = const_cast<volatile uint8_t*>(base + cap_length);
    auto* rt = const_cast<volatile uint8_t*>(base + rtsoff);
    auto* db = reinterpret_cast<volatile uint32_t*>(const_cast<volatile uint8_t*>(base + dboff));

    // 1. Stop controller
    uint32_t cmd = read32(op, XHCI_OP_USBCMD);
    cmd &= ~XHCI_CMD_RUN;
    write32(op, XHCI_OP_USBCMD, cmd);

    // Wait for halted
    for (int i = 0; i < 100000; i++) {
        if (read32(op, XHCI_OP_USBSTS) & XHCI_STS_HCH) break;
        asm volatile("pause");
    }

    // 2. Reset controller
    write32(op, XHCI_OP_USBCMD, XHCI_CMD_HCRST);
    for (int i = 0; i < 100000; i++) {
        uint32_t c = read32(op, XHCI_OP_USBCMD);
        uint32_t s = read32(op, XHCI_OP_USBSTS);
        if (!(c & XHCI_CMD_HCRST) && !(s & XHCI_STS_CNR)) break;
        asm volatile("pause");
    }

    // Allocate controller state
    auto* hc = static_cast<XhciController*>(mod::mm::phys::pageAlloc(sizeof(XhciController)));
    if (hc == nullptr) return -1;
    std::memset(hc, 0, sizeof(XhciController));

    hc->base = base;
    hc->op = op;
    hc->rt = rt;
    hc->db = db;
    hc->pci = pci_dev;
    hc->max_slots = max_slots;
    hc->max_ports = max_ports;
    hc->max_intrs = max_intrs;
    hc->ctx64 = ctx64;

    // 3. Set Max Device Slots Enabled
    write32(op, XHCI_OP_CONFIG, max_slots);

    // 4. Allocate DCBAA (64-byte aligned)
    auto dcbaa_alloc = alloc_page();
    if (dcbaa_alloc.virt == nullptr) return -1;
    hc->dcbaap = static_cast<uint64_t*>(dcbaa_alloc.virt);
    hc->dcbaap_phys = dcbaa_alloc.phys;
    write64(op, XHCI_OP_DCBAAP, hc->dcbaap_phys);

    // 5. Allocate scratchpad buffers (if needed)
    uint32_t max_scratch_hi = (hcsparams2 >> 21) & 0x1F;
    uint32_t max_scratch_lo = (hcsparams2 >> 27) & 0x1F;
    uint32_t max_scratch = (max_scratch_hi << 5) | max_scratch_lo;
    if (max_scratch > 0) {
        auto sp_arr = alloc_page();
        if (sp_arr.virt == nullptr) return -1;
        hc->scratchpad_array = static_cast<uint64_t*>(sp_arr.virt);
        hc->scratchpad_array_phys = sp_arr.phys;

        for (uint32_t i = 0; i < max_scratch; i++) {
            auto buf = alloc_page();
            if (buf.virt == nullptr) return -1;
            hc->scratchpad_array[i] = buf.phys;
        }
        hc->dcbaap[0] = hc->scratchpad_array_phys;
    }

    // 6. Allocate Command Ring
    auto cmd_alloc = alloc_pages(CMD_RING_SIZE * sizeof(Trb));
    if (cmd_alloc.virt == nullptr) return -1;
    hc->cmd_ring = static_cast<Trb*>(cmd_alloc.virt);
    hc->cmd_ring_phys = cmd_alloc.phys;
    hc->cmd_enqueue = 0;
    hc->cmd_cycle = true;
    // Set CRCR (with cycle bit)
    write64(op, XHCI_OP_CRCR, hc->cmd_ring_phys | 1);

    // 7. Allocate Event Ring
    auto evt_alloc = alloc_pages(EVENT_RING_SIZE * sizeof(Trb));
    if (evt_alloc.virt == nullptr) return -1;
    hc->evt_ring = static_cast<Trb*>(evt_alloc.virt);
    hc->evt_ring_phys = evt_alloc.phys;
    hc->evt_dequeue = 0;
    hc->evt_cycle = true;

    // ERST
    auto erst_alloc = alloc_page();
    if (erst_alloc.virt == nullptr) return -1;
    hc->erst = static_cast<ErstEntry*>(erst_alloc.virt);
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
    uint8_t vector = mod::gates::allocateVector();
    if (vector == 0) {
        mod::io::serial::write("xhci: no free IRQ vector\n");
        return -1;
    }
    hc->irq_vector = vector;

    int msi_ret = pci::pci_enable_msi(pci_dev, vector);
    if (msi_ret != 0) {
        vector = pci_dev->interrupt_line + 32;
        hc->irq_vector = vector;
    }
    mod::gates::requestIrq(vector, xhci_irq, hc, "xhci");

    // 9. Start controller
    cmd = read32(op, XHCI_OP_USBCMD);
    cmd |= XHCI_CMD_RUN | XHCI_CMD_INTE;
    write32(op, XHCI_OP_USBCMD, cmd);

    // Wait for not halted
    for (int i = 0; i < 100000; i++) {
        if (!(read32(op, XHCI_OP_USBSTS) & XHCI_STS_HCH)) break;
        asm volatile("pause");
    }

    controllers[controller_count++] = hc;

    mod::io::serial::write("xhci: controller ready, vec=");
    mod::io::serial::writeHex(hc->irq_vector);
    mod::io::serial::write("\n");

    // 10. Scan ports for already-connected devices
    scan_ports(hc);

    return 0;
}

}  // namespace

// ── Public API ──

auto configure_endpoint(XhciController* hc, uint8_t slot_id, uint64_t input_ctx_phys) -> int {
    return send_command(hc, input_ctx_phys, 0, TRB_CONFIG_ENDPOINT | (static_cast<uint32_t>(slot_id) << 24));
}

void usb_register_class_driver(UsbClassDriver* drv) {
    drv->next = class_drivers;
    class_drivers = drv;
}

auto xhci_control_transfer(XhciController* hc, uint8_t slot_id, UsbSetupPacket* setup, void* data, size_t len, bool dir_in) -> int {
    auto& dev = hc->devices[slot_id];
    auto& ep0 = dev.endpoints[0];

    // Build Setup Stage TRB
    uint64_t setup_param;
    std::memcpy(&setup_param, setup, 8);

    uint32_t setup_status = 8;  // TRB transfer length = 8
    uint32_t setup_ctrl = TRB_SETUP | TRB_IDT | (len > 0 ? (dir_in ? (3u << 16) : (2u << 16)) : 0);

    ring_enqueue(ep0.ring, &ep0.ring_enqueue, &ep0.ring_cycle, XFER_RING_SIZE, setup_param, setup_status, setup_ctrl);

    // Build Data Stage TRB (if data)
    if (len > 0 && data != nullptr) {
        uint64_t data_phys = virt_to_phys(data);
        uint32_t data_status = static_cast<uint32_t>(len);
        uint32_t data_ctrl = TRB_DATA | (dir_in ? TRB_DIR_IN : 0);

        ring_enqueue(ep0.ring, &ep0.ring_enqueue, &ep0.ring_cycle, XFER_RING_SIZE, data_phys, data_status, data_ctrl);
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
            uint32_t cc = (hc->cmd_result >> 24) & 0xFF;
            if (cc == TRB_CC_SUCCESS || cc == TRB_CC_SHORT_PKT) return 0;
            return -1;
        }
        asm volatile("pause");
    }

    mod::io::serial::write("xhci: control transfer timeout\n");
    return -1;
}

auto xhci_bulk_transfer(XhciController* hc, uint8_t slot_id, UsbEndpoint* ep, void* data, size_t len) -> int {
    uint64_t phys = virt_to_phys(data);
    uint32_t status = static_cast<uint32_t>(len);
    uint32_t ctrl = TRB_NORMAL | TRB_IOC;

    hc->cmd_done = false;
    hc->cmd_result = 0;

    ring_enqueue(ep->ring, &ep->ring_enqueue, &ep->ring_cycle, XFER_RING_SIZE, phys, status, ctrl);

    // Ring doorbell: slot_id, target = DCI
    uint8_t dci = ep_dci(ep->address);
    ring_doorbell(hc->db, slot_id, dci);

    // Wait
    for (int i = 0; i < 5000000; i++) {
        if (hc->cmd_done) {
            uint32_t cc = (hc->cmd_result >> 24) & 0xFF;
            if (cc == TRB_CC_SUCCESS || cc == TRB_CC_SHORT_PKT) return 0;
            return -1;
        }
        asm volatile("pause");
    }

    mod::io::serial::write("xhci: bulk transfer timeout\n");
    return -1;
}

auto xhci_init() -> int {
    int found = 0;

    size_t count = pci::pci_device_count();
    for (size_t i = 0; i < count; i++) {
        auto* dev = pci::pci_get_device(i);
        if (dev == nullptr) continue;

        // xHCI: class=0x0C, subclass=0x03, prog_if=0x30
        if (dev->class_code == pci::PCI_CLASS_SERIAL_BUS && dev->subclass_code == pci::PCI_SUBCLASS_USB &&
            dev->prog_if == pci::PCI_PROG_IF_XHCI) {
            mod::io::serial::write("xhci: found controller at PCI ");
            mod::io::serial::writeHex(dev->bus);
            mod::io::serial::write(":");
            mod::io::serial::writeHex(dev->slot);
            mod::io::serial::write(".");
            mod::io::serial::writeHex(dev->function);
            mod::io::serial::write("\n");

            if (init_controller(dev) == 0) {
                found++;
            }
        }
    }

    if (found == 0) {
        mod::io::serial::write("xhci: no controllers found\n");
    }

    return found;
}

}  // namespace ker::dev::usb
