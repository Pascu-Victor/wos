#include "qemu_fw_cfg.hpp"

#include <array>
#include <cstdint>
#include <cstring>
#include <mod/io/port/port.hpp>

// QEMU fw_cfg I/O-port interface (x86)
// Selector register: 0x510 (16-bit write)
// Data register:     0x511 (8-bit read, auto-increments)
// File directory:    selector 0x0019

namespace ker::platform::fw {

namespace {

constexpr uint16_t FW_CFG_PORT_SEL = 0x510;
constexpr uint16_t FW_CFG_PORT_DATA = 0x511;
constexpr uint16_t FW_CFG_FILE_DIR = 0x0019;
constexpr uint16_t FW_CFG_SIGNATURE = 0x0000;
constexpr size_t FW_CFG_MAX_NAME = 56;
constexpr std::array<char, 4> FW_CFG_MAGIC = {'Q', 'E', 'M', 'U'};

struct FwCfgFile {
    uint32_t size;
    uint16_t select;
    uint16_t reserved;
    // NOLINTNEXTLINE(cppcoreguidelines-avoid-c-arrays,modernize-avoid-c-arrays): Firmware directory wire layout.
    char name[FW_CFG_MAX_NAME];
} __attribute__((packed));

auto read_be16() -> uint16_t {
    uint8_t const HI = inb(FW_CFG_PORT_DATA);
    uint8_t const LO = inb(FW_CFG_PORT_DATA);
    return static_cast<uint16_t>((HI << 8) | LO);
}

auto read_be32() -> uint32_t {
    uint8_t const B3 = inb(FW_CFG_PORT_DATA);
    uint8_t const B2 = inb(FW_CFG_PORT_DATA);
    uint8_t const B1 = inb(FW_CFG_PORT_DATA);
    uint8_t const B0 = inb(FW_CFG_PORT_DATA);
    return (static_cast<uint32_t>(B3) << 24) | (static_cast<uint32_t>(B2) << 16) | (static_cast<uint32_t>(B1) << 8) |
           static_cast<uint32_t>(B0);
}

void read_bytes(void* buf, size_t n) {
    auto* p = static_cast<uint8_t*>(buf);
    for (size_t i = 0; i < n; i++) {
        p[i] = inb(FW_CFG_PORT_DATA);
    }
}

void skip_bytes(size_t n) {
    for (size_t i = 0; i < n; i++) {
        inb(FW_CFG_PORT_DATA);
    }
}

}  // namespace

auto fw_cfg_read_file(const char* name, void* buf, size_t buf_size) -> int {
    if (name == nullptr || buf == nullptr || buf_size == 0) {
        return -1;
    }

    // Verify fw_cfg is present by reading signature
    outw(FW_CFG_PORT_SEL, FW_CFG_SIGNATURE);
    std::array<char, 4> sig{};
    read_bytes(sig.data(), sig.size());
    if (std::memcmp(sig.data(), FW_CFG_MAGIC.data(), sig.size()) != 0) {
        return -1;  // No QEMU fw_cfg device
    }

    // Read file directory
    outw(FW_CFG_PORT_SEL, FW_CFG_FILE_DIR);
    uint32_t const COUNT = read_be32();

    for (uint32_t i = 0; i < COUNT; i++) {
        uint32_t const FILE_SIZE = read_be32();
        uint16_t const FILE_SEL = read_be16();
        skip_bytes(2);  // reserved

        std::array<char, FW_CFG_MAX_NAME> file_name{};
        read_bytes(file_name.data(), file_name.size());

        if (std::strcmp(file_name.data(), name) == 0) {
            // Found it — select and read
            outw(FW_CFG_PORT_SEL, FILE_SEL);
            size_t const TO_READ = (FILE_SIZE < buf_size) ? FILE_SIZE : buf_size;
            read_bytes(buf, TO_READ);
            return static_cast<int>(TO_READ);
        }
    }

    return -1;  // Not found
}

}  // namespace ker::platform::fw
