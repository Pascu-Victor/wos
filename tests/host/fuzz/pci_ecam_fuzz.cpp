#include <algorithm>
#include <dev/pci_ecam.hpp>
#include <vector>

namespace ecam = ker::dev::pci::ecam;
struct Reader {
    std::span<const uint8_t> bytes;
    size_t reads = 0;
    auto read16(uint16_t offset) -> std::optional<uint16_t> {
        if (++reads > 1100 || offset % 2 != 0 || offset > 4094) __builtin_trap();
        if (offset + size_t{2} > bytes.size()) return std::nullopt;
        return static_cast<uint16_t>(ker::mod::acpi::bounded::detail::read(bytes, offset, 2));
    }
    auto read32(uint16_t offset) -> std::optional<uint32_t> {
        if (++reads > 1100 || offset % 4 != 0 || offset > 4092) __builtin_trap();
        if (offset + size_t{4} > bytes.size()) return std::nullopt;
        return static_cast<uint32_t>(ker::mod::acpi::bounded::detail::read(bytes, offset, 4));
    }
};
extern "C" auto LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) -> int {
    auto check = [](std::span<const uint8_t> bytes) {
        auto const TABLE = ecam::parse(bytes);
        if (!TABLE.valid()) {
            if (TABLE.count != 0) __builtin_trap();
            return;
        }
        if (TABLE.count == 0 || TABLE.count > ecam::MAX_WINDOWS) __builtin_trap();
        for (size_t i = 0; i < TABLE.count; ++i) {
            auto const& w = TABLE.windows[i];
            auto const FIRST = ecam::address(TABLE, w.segment, w.first_bus, 0, 0, 0, 4);
            auto const LAST = ecam::address(TABLE, w.segment, w.last_bus, 31, 7, 4092, 4);
            if (!FIRST || !LAST || *FIRST != w.start() || *LAST + 4 != w.end()) __builtin_trap();
        }
    };
    check({data, size});
    if (size <= ecam::MAX_WINDOWS * 16) {
        std::vector<uint8_t> bytes(44);
        bytes.insert(bytes.end(), data, data + size);
        std::copy_n("MCFG", 4, bytes.begin());
        for (size_t i = 0; i < 4; ++i) bytes[4 + i] = static_cast<uint8_t>(bytes.size() >> (8 * i));
        bytes[8] = 1;
        uint8_t sum = 0;
        for (auto b : bytes) sum = static_cast<uint8_t>(sum + b);
        bytes[9] = static_cast<uint8_t>(0U - sum);
        check(bytes);
    }
    // Preserve arbitrary capability-list bytes while bypassing unrelated ID
    // discovery so fuzzing reaches pointer/range/cycle/control parsing.
    std::vector<uint8_t> config(data, data + std::min(size, size_t{4096}));
    if (config.size() < 256) config.resize(256);
    config[0] = 0xf4;
    config[1] = 0x1a;
    config[2] = 0x41;
    config[3] = 0x10;
    config[14] = 0;
    config[6] |= 0x10;
    config[0x34] = 0x40;
    config[0x40] = 0x10;
    config[0x42] = 2;
    Reader reader{config};
    auto const CAPS = ecam::inspect(reader, 0x1af4, 0x1041, 0);
    if (!CAPS.valid() && (CAPS.pcie != 0 || CAPS.acs != 0 || CAPS.extended_count != 0)) __builtin_trap();
    return 0;
}
