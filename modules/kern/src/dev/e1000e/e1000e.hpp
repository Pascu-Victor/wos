#pragma once

#include <cstdint>
#include <net/netdevice.hpp>
#include <net/netpoll.hpp>
#include <net/packet.hpp>
#include <dev/pci.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::dev::e1000e {

// ── MMIO Register Offsets ───────────────────────────────────────────────
constexpr uint32_t REG_CTRL   = 0x0000;  // Device Control
constexpr uint32_t REG_STATUS = 0x0008;  // Device Status
constexpr uint32_t REG_EECD   = 0x0010;  // EEPROM/Flash Control
constexpr uint32_t REG_EERD   = 0x0014;  // EEPROM Read
constexpr uint32_t REG_ICR    = 0x00C0;  // Interrupt Cause Read
constexpr uint32_t REG_ICS    = 0x00C8;  // Interrupt Cause Set
constexpr uint32_t REG_IMS    = 0x00D0;  // Interrupt Mask Set
constexpr uint32_t REG_IMC    = 0x00D8;  // Interrupt Mask Clear
constexpr uint32_t REG_RCTL   = 0x0100;  // Receive Control
constexpr uint32_t REG_TCTL   = 0x0400;  // Transmit Control
constexpr uint32_t REG_TIPG   = 0x0410;  // Transmit Inter-Packet Gap
constexpr uint32_t REG_RDBAL  = 0x2800;  // RX Descriptor Base Low
constexpr uint32_t REG_RDBAH  = 0x2804;  // RX Descriptor Base High
constexpr uint32_t REG_RDLEN  = 0x2808;  // RX Descriptor Length
constexpr uint32_t REG_RDH    = 0x2810;  // RX Descriptor Head
constexpr uint32_t REG_RDT    = 0x2818;  // RX Descriptor Tail
constexpr uint32_t REG_TDBAL  = 0x3800;  // TX Descriptor Base Low
constexpr uint32_t REG_TDBAH  = 0x3804;  // TX Descriptor Base High
constexpr uint32_t REG_TDLEN  = 0x3808;  // TX Descriptor Length
constexpr uint32_t REG_TDH    = 0x3810;  // TX Descriptor Head
constexpr uint32_t REG_TDT    = 0x3818;  // TX Descriptor Tail
constexpr uint32_t REG_RAL    = 0x5400;  // Receive Address Low
constexpr uint32_t REG_RAH    = 0x5404;  // Receive Address High
constexpr uint32_t REG_MTA    = 0x5200;  // Multicast Table Array (128 entries)

// ── CTRL Register Bits ──────────────────────────────────────────────────
constexpr uint32_t CTRL_SLU   = (1u << 6);   // Set Link Up
constexpr uint32_t CTRL_RST   = (1u << 26);  // Device Reset
constexpr uint32_t CTRL_PHY_RST = (1u << 31); // PHY Reset

// ── RCTL Register Bits ──────────────────────────────────────────────────
constexpr uint32_t RCTL_EN    = (1u << 1);   // Receiver Enable
constexpr uint32_t RCTL_SBP   = (1u << 2);   // Store Bad Packets
constexpr uint32_t RCTL_UPE   = (1u << 3);   // Unicast Promiscuous Enable
constexpr uint32_t RCTL_MPE   = (1u << 4);   // Multicast Promiscuous Enable
constexpr uint32_t RCTL_LPE   = (1u << 5);   // Long Packet Enable
constexpr uint32_t RCTL_BAM   = (1u << 15);  // Broadcast Accept Mode
constexpr uint32_t RCTL_BSIZE_2048 = 0;      // Buffer size 2048 (bits 16:17 = 00)
constexpr uint32_t RCTL_SECRC = (1u << 26);  // Strip Ethernet CRC

// ── TCTL Register Bits ──────────────────────────────────────────────────
constexpr uint32_t TCTL_EN    = (1u << 1);   // Transmitter Enable
constexpr uint32_t TCTL_PSP   = (1u << 3);   // Pad Short Packets
constexpr uint32_t TCTL_CT_SHIFT = 4;        // Collision Threshold
constexpr uint32_t TCTL_COLD_SHIFT = 12;     // Collision Distance

// ── Interrupt Cause Bits ────────────────────────────────────────────────
constexpr uint32_t ICR_TXDW   = (1u << 0);   // TX Descriptor Written Back
constexpr uint32_t ICR_TXQE   = (1u << 1);   // TX Queue Empty
constexpr uint32_t ICR_LSC    = (1u << 2);   // Link Status Change
constexpr uint32_t ICR_RXDMT0 = (1u << 4);   // RX Desc Min Threshold
constexpr uint32_t ICR_RXO    = (1u << 6);   // RX Overrun
constexpr uint32_t ICR_RXT0   = (1u << 7);   // RX Timer Interrupt

// ── RAH bits ────────────────────────────────────────────────────────────
constexpr uint32_t RAH_AV     = (1u << 31);  // Address Valid

// ── EEPROM ──────────────────────────────────────────────────────────────
constexpr uint32_t EERD_START = (1u << 0);
constexpr uint32_t EERD_DONE  = (1u << 4);

// ── Descriptor ring sizes ───────────────────────────────────────────────
constexpr size_t NUM_RX_DESC = 256;
constexpr size_t NUM_TX_DESC = 256;

// ── Legacy RX Descriptor ────────────────────────────────────────────────
struct E1000RxDesc {
    uint64_t addr;       // Buffer physical address
    uint16_t length;     // Bytes received
    uint16_t checksum;   // Packet checksum
    uint8_t  status;     // Descriptor status
    uint8_t  errors;     // Descriptor errors
    uint16_t special;
} __attribute__((packed));

constexpr uint8_t RX_STATUS_DD  = (1u << 0);  // Descriptor Done
constexpr uint8_t RX_STATUS_EOP = (1u << 1);  // End of Packet

// ── Legacy TX Descriptor ────────────────────────────────────────────────
struct E1000TxDesc {
    uint64_t addr;       // Buffer physical address
    uint16_t length;     // Bytes to send
    uint8_t  cso;        // Checksum offset
    uint8_t  cmd;        // Command field
    uint8_t  status;     // Descriptor status
    uint8_t  css;        // Checksum start
    uint16_t special;
} __attribute__((packed));

constexpr uint8_t TX_CMD_EOP  = (1u << 0);  // End of Packet
constexpr uint8_t TX_CMD_IFCS = (1u << 1);  // Insert FCS/CRC
constexpr uint8_t TX_CMD_RS   = (1u << 3);  // Report Status
constexpr uint8_t TX_STATUS_DD = (1u << 0); // Descriptor Done

// ── Device structure ────────────────────────────────────────────────────
struct E1000Device {
    ker::net::NetDevice netdev;     // embedded NetDevice (first member for casting)
    pci::PCIDevice* pci = nullptr;
    volatile uint32_t* mmio = nullptr;  // mapped BAR0

    // RX ring
    E1000RxDesc* rx_descs = nullptr;    // physically contiguous
    ker::net::PacketBuffer* rx_bufs[NUM_RX_DESC] = {};
    uint16_t rx_tail = 0;

    // TX ring
    E1000TxDesc* tx_descs = nullptr;    // physically contiguous
    ker::net::PacketBuffer* tx_bufs[NUM_TX_DESC] = {};
    uint16_t tx_tail = 0;

    uint8_t irq_vector = 0;
    ker::mod::sys::Spinlock tx_lock;

    // NAPI state for deferred packet processing
    ker::net::NapiStruct napi{};
};

// ── Driver entry point ──────────────────────────────────────────────────
void e1000e_init();

}  // namespace ker::dev::e1000e
