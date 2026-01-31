#pragma once

#include <cstddef>
#include <cstdint>
#include <dev/pci.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::dev::usb {

// ── xHCI Capability Register Offsets (from BAR0) ──
constexpr uint32_t XHCI_CAP_CAPLENGTH = 0x00;   // 1 byte
constexpr uint32_t XHCI_CAP_HCIVERSION = 0x02;  // 2 bytes
constexpr uint32_t XHCI_CAP_HCSPARAMS1 = 0x04;  // 4 bytes
constexpr uint32_t XHCI_CAP_HCSPARAMS2 = 0x08;
constexpr uint32_t XHCI_CAP_HCSPARAMS3 = 0x0C;
constexpr uint32_t XHCI_CAP_HCCPARAMS1 = 0x10;
constexpr uint32_t XHCI_CAP_DBOFF = 0x14;   // Doorbell offset
constexpr uint32_t XHCI_CAP_RTSOFF = 0x18;  // Runtime registers offset

// ── Operational Register Offsets (from BAR0 + CAPLENGTH) ──
constexpr uint32_t XHCI_OP_USBCMD = 0x00;
constexpr uint32_t XHCI_OP_USBSTS = 0x04;
constexpr uint32_t XHCI_OP_PAGESIZE = 0x08;
constexpr uint32_t XHCI_OP_DNCTRL = 0x14;
constexpr uint32_t XHCI_OP_CRCR = 0x18;    // 8 bytes
constexpr uint32_t XHCI_OP_DCBAAP = 0x30;  // 8 bytes
constexpr uint32_t XHCI_OP_CONFIG = 0x38;
constexpr uint32_t XHCI_OP_PORTSC = 0x400;  // Port Status/Control base

// USBCMD bits
constexpr uint32_t XHCI_CMD_RUN = (1 << 0);
constexpr uint32_t XHCI_CMD_HCRST = (1 << 1);
constexpr uint32_t XHCI_CMD_INTE = (1 << 2);
constexpr uint32_t XHCI_CMD_HSEE = (1 << 3);

// USBSTS bits
constexpr uint32_t XHCI_STS_HCH = (1 << 0);  // HC Halted
constexpr uint32_t XHCI_STS_HSE = (1 << 2);
constexpr uint32_t XHCI_STS_EINT = (1 << 3);  // Event Interrupt
constexpr uint32_t XHCI_STS_PCD = (1 << 4);   // Port Change Detect
constexpr uint32_t XHCI_STS_CNR = (1 << 11);  // Controller Not Ready

// PORTSC bits
constexpr uint32_t XHCI_PORTSC_CCS = (1 << 0);   // Current Connect Status
constexpr uint32_t XHCI_PORTSC_PED = (1 << 1);   // Port Enabled
constexpr uint32_t XHCI_PORTSC_OCA = (1 << 3);   // Over-current Active
constexpr uint32_t XHCI_PORTSC_PR = (1 << 4);    // Port Reset
constexpr uint32_t XHCI_PORTSC_PP = (1 << 9);    // Port Power
constexpr uint32_t XHCI_PORTSC_CSC = (1 << 17);  // Connect Status Change
constexpr uint32_t XHCI_PORTSC_PEC = (1 << 18);  // Port Enabled Change
constexpr uint32_t XHCI_PORTSC_PRC = (1 << 21);  // Port Reset Change
constexpr uint32_t XHCI_PORTSC_SPEED_MASK = (0xF << 10);
constexpr uint32_t XHCI_PORTSC_SPEED_SHIFT = 10;
// Port speed values
constexpr uint32_t XHCI_SPEED_FULL = 1;
constexpr uint32_t XHCI_SPEED_LOW = 2;
constexpr uint32_t XHCI_SPEED_HIGH = 3;
constexpr uint32_t XHCI_SPEED_SUPER = 4;
// Write-1-to-clear bits in PORTSC (must preserve when writing)
constexpr uint32_t XHCI_PORTSC_W1C_MASK =
    XHCI_PORTSC_CSC | XHCI_PORTSC_PEC | XHCI_PORTSC_PRC | (1 << 19) | (1 << 20) | (1 << 22) | (1 << 23);

// ── Runtime Register Offsets (from BAR0 + RTSOFF) ──
constexpr uint32_t XHCI_RT_IMAN = 0x20;    // Interrupter 0 Management
constexpr uint32_t XHCI_RT_IMOD = 0x24;    // Interrupter 0 Moderation
constexpr uint32_t XHCI_RT_ERSTSZ = 0x28;  // Event Ring Segment Table Size
constexpr uint32_t XHCI_RT_ERSTBA = 0x30;  // Event Ring Segment Table Base (8 bytes)
constexpr uint32_t XHCI_RT_ERDP = 0x38;    // Event Ring Dequeue Pointer (8 bytes)

// IMAN bits
constexpr uint32_t XHCI_IMAN_IP = (1 << 0);  // Interrupt Pending
constexpr uint32_t XHCI_IMAN_IE = (1 << 1);  // Interrupt Enable

// ── TRB (Transfer Request Block) ──
struct Trb {
    uint64_t param;
    uint32_t status;
    uint32_t control;
} __attribute__((packed));
static_assert(sizeof(Trb) == 16);

// TRB types (bits 15:10 of control)
constexpr uint32_t TRB_TYPE_SHIFT = 10;
constexpr uint32_t TRB_TYPE_MASK = (0x3F << TRB_TYPE_SHIFT);

// Transfer TRB types
constexpr uint32_t TRB_NORMAL = (1 << TRB_TYPE_SHIFT);
constexpr uint32_t TRB_SETUP = (2 << TRB_TYPE_SHIFT);
constexpr uint32_t TRB_DATA = (3 << TRB_TYPE_SHIFT);
constexpr uint32_t TRB_STATUS = (4 << TRB_TYPE_SHIFT);
constexpr uint32_t TRB_LINK = (6 << TRB_TYPE_SHIFT);
constexpr uint32_t TRB_EVENT_DATA = (7 << TRB_TYPE_SHIFT);
constexpr uint32_t TRB_NOOP = (8 << TRB_TYPE_SHIFT);

// Command TRB types
constexpr uint32_t TRB_ENABLE_SLOT = (9 << TRB_TYPE_SHIFT);
constexpr uint32_t TRB_DISABLE_SLOT = (10 << TRB_TYPE_SHIFT);
constexpr uint32_t TRB_ADDRESS_DEVICE = (11 << TRB_TYPE_SHIFT);
constexpr uint32_t TRB_CONFIG_ENDPOINT = (12 << TRB_TYPE_SHIFT);
constexpr uint32_t TRB_EVALUATE_CTX = (13 << TRB_TYPE_SHIFT);
constexpr uint32_t TRB_RESET_ENDPOINT = (14 << TRB_TYPE_SHIFT);

// Event TRB types
constexpr uint32_t TRB_TRANSFER_EVENT = (32 << TRB_TYPE_SHIFT);
constexpr uint32_t TRB_CMD_COMPLETION = (33 << TRB_TYPE_SHIFT);
constexpr uint32_t TRB_PORT_STATUS_CHG = (34 << TRB_TYPE_SHIFT);

// TRB control flags
constexpr uint32_t TRB_CYCLE = (1 << 0);
constexpr uint32_t TRB_TOGGLE_CYCLE = (1 << 1);
constexpr uint32_t TRB_IOC = (1 << 5);      // Interrupt on Completion
constexpr uint32_t TRB_IDT = (1 << 6);      // Immediate Data
constexpr uint32_t TRB_DIR_IN = (1 << 16);  // Data direction IN (for setup TRB)

// TRB completion codes (bits 31:24 of status in event TRB)
constexpr uint32_t TRB_CC_SUCCESS = 1;
constexpr uint32_t TRB_CC_SHORT_PKT = 13;

// ── Event Ring Segment Table Entry ──
struct ErstEntry {
    uint64_t ring_base;
    uint32_t ring_size;
    uint32_t reserved;
} __attribute__((packed));

// ── Device Context structures ──
// Slot context (32 bytes)
struct SlotContext {
    uint32_t data[8];
} __attribute__((packed));

// Endpoint context (32 bytes)
struct EndpointContext {
    uint32_t data[8];
} __attribute__((packed));

// Input context: Input Control Context + Slot + 31 EPs = 33 * 32 = 1056 bytes
struct InputContext {
    uint32_t drop_flags;
    uint32_t add_flags;
    uint32_t reserved[6];
    SlotContext slot;
    EndpointContext ep[31];
} __attribute__((packed));

// Device context: Slot + 31 EPs = 32 * 32 = 1024 bytes
struct DeviceContext {
    SlotContext slot;
    EndpointContext ep[31];
} __attribute__((packed));

// ── Ring sizes ──
constexpr size_t CMD_RING_SIZE = 256;  // TRBs
constexpr size_t EVENT_RING_SIZE = 256;
constexpr size_t XFER_RING_SIZE = 256;
constexpr size_t MAX_XHCI_SLOTS = 64;
constexpr size_t MAX_XHCI_PORTS = 16;

// ── USB device speed ──
constexpr uint8_t USB_SPEED_FULL = 1;
constexpr uint8_t USB_SPEED_LOW = 2;
constexpr uint8_t USB_SPEED_HIGH = 3;
constexpr uint8_t USB_SPEED_SUPER = 4;

// ── USB Standard Requests ──
constexpr uint8_t USB_REQ_GET_STATUS = 0x00;
constexpr uint8_t USB_REQ_CLEAR_FEATURE = 0x01;
constexpr uint8_t USB_REQ_SET_FEATURE = 0x03;
constexpr uint8_t USB_REQ_SET_ADDRESS = 0x05;
constexpr uint8_t USB_REQ_GET_DESCRIPTOR = 0x06;
constexpr uint8_t USB_REQ_SET_CONFIG = 0x09;

// USB descriptor types
constexpr uint8_t USB_DESC_DEVICE = 0x01;
constexpr uint8_t USB_DESC_CONFIG = 0x02;
constexpr uint8_t USB_DESC_INTERFACE = 0x04;
constexpr uint8_t USB_DESC_ENDPOINT = 0x05;

// ── USB Descriptors ──
struct UsbDeviceDescriptor {
    uint8_t bLength;
    uint8_t bDescriptorType;
    uint16_t bcdUSB;
    uint8_t bDeviceClass;
    uint8_t bDeviceSubClass;
    uint8_t bDeviceProtocol;
    uint8_t bMaxPacketSize0;
    uint16_t idVendor;
    uint16_t idProduct;
    uint16_t bcdDevice;
    uint8_t iManufacturer;
    uint8_t iProduct;
    uint8_t iSerialNumber;
    uint8_t bNumConfigurations;
} __attribute__((packed));

struct UsbConfigDescriptor {
    uint8_t bLength;
    uint8_t bDescriptorType;
    uint16_t wTotalLength;
    uint8_t bNumInterfaces;
    uint8_t bConfigurationValue;
    uint8_t iConfiguration;
    uint8_t bmAttributes;
    uint8_t bMaxPower;
} __attribute__((packed));

struct UsbInterfaceDescriptor {
    uint8_t bLength;
    uint8_t bDescriptorType;
    uint8_t bInterfaceNumber;
    uint8_t bAlternateSetting;
    uint8_t bNumEndpoints;
    uint8_t bInterfaceClass;
    uint8_t bInterfaceSubClass;
    uint8_t bInterfaceProtocol;
    uint8_t iInterface;
} __attribute__((packed));

struct UsbEndpointDescriptor {
    uint8_t bLength;
    uint8_t bDescriptorType;
    uint8_t bEndpointAddress;
    uint8_t bmAttributes;
    uint16_t wMaxPacketSize;
    uint8_t bInterval;
} __attribute__((packed));

// ── USB Setup Packet ──
struct UsbSetupPacket {
    uint8_t bmRequestType;
    uint8_t bRequest;
    uint16_t wValue;
    uint16_t wIndex;
    uint16_t wLength;
} __attribute__((packed));

// ── USB Endpoint descriptor helpers ──
constexpr uint8_t USB_EP_DIR_IN = 0x80;
constexpr uint8_t USB_EP_DIR_OUT = 0x00;
constexpr uint8_t USB_EP_ADDR_MASK = 0x0F;
constexpr uint8_t USB_EP_TYPE_MASK = 0x03;
constexpr uint8_t USB_EP_TYPE_CONTROL = 0;
constexpr uint8_t USB_EP_TYPE_ISOCH = 1;
constexpr uint8_t USB_EP_TYPE_BULK = 2;
constexpr uint8_t USB_EP_TYPE_INTR = 3;

// ── USB Class codes ──
constexpr uint8_t USB_CLASS_CDC_DATA = 0x0A;
constexpr uint8_t USB_CLASS_CDC = 0x02;
constexpr uint8_t USB_CLASS_VENDOR = 0xFF;

// ── USB Device (high-level) ──
struct UsbEndpoint {
    uint8_t address;  // endpoint address (with direction bit)
    uint8_t type;     // control/bulk/interrupt/isochronous
    uint16_t max_packet;
    uint8_t interval;
    Trb* ring;  // transfer ring (xHCI-specific)
    uint64_t ring_phys;
    size_t ring_enqueue;
    bool ring_cycle;
};

struct UsbDevice {
    uint8_t slot_id;
    uint8_t port;
    uint8_t speed;
    uint16_t vendor_id;
    uint16_t product_id;
    uint8_t device_class;
    uint8_t device_subclass;
    uint8_t device_protocol;
    uint8_t max_packet0;
    UsbEndpoint endpoints[16];
    uint8_t num_endpoints;
    DeviceContext* dev_ctx;
    InputContext* input_ctx;
    uint64_t input_ctx_phys;
    void* driver_data;
    bool active;
};

// ── Class Driver Registration ──
struct UsbClassDriver {
    const char* name;
    bool (*probe)(UsbDevice*, UsbInterfaceDescriptor*);
    int (*attach)(UsbDevice*, UsbInterfaceDescriptor*, uint8_t* config_desc, size_t config_len);
    void (*detach)(UsbDevice*);
    UsbClassDriver* next;
};

void usb_register_class_driver(UsbClassDriver* drv);

// ── xHCI Controller ──
struct XhciController {
    volatile uint8_t* base;  // MMIO base (BAR0)
    volatile uint8_t* op;    // Operational registers
    volatile uint8_t* rt;    // Runtime registers
    volatile uint32_t* db;   // Doorbell registers

    pci::PCIDevice* pci;
    uint8_t irq_vector;

    // Capability info
    uint8_t max_slots;
    uint8_t max_ports;
    uint16_t max_intrs;
    bool ctx64;  // 64-byte context structures

    // Device Context Base Address Array
    uint64_t* dcbaap;
    uint64_t dcbaap_phys;

    // Command ring
    Trb* cmd_ring;
    uint64_t cmd_ring_phys;
    size_t cmd_enqueue;
    bool cmd_cycle;
    ker::mod::sys::Spinlock cmd_lock;

    // Event ring
    Trb* evt_ring;
    uint64_t evt_ring_phys;
    ErstEntry* erst;
    uint64_t erst_phys;
    size_t evt_dequeue;
    bool evt_cycle;

    // Scratchpad
    uint64_t* scratchpad_array;
    uint64_t scratchpad_array_phys;

    // Devices
    UsbDevice devices[MAX_XHCI_SLOTS];

    // Command completion
    volatile bool cmd_done;
    volatile uint32_t cmd_result;
    volatile uint32_t cmd_slot_id;
};

auto xhci_init() -> int;

// Internal: used by USB core for transfers
auto xhci_control_transfer(XhciController* hc, uint8_t slot_id, UsbSetupPacket* setup, void* data, size_t len, bool dir_in) -> int;
auto xhci_bulk_transfer(XhciController* hc, uint8_t slot_id, UsbEndpoint* ep, void* data, size_t len) -> int;

auto configure_endpoint(XhciController* hc, uint8_t slot_id, uint64_t input_ctx_phys) -> int;

}  // namespace ker::dev::usb
