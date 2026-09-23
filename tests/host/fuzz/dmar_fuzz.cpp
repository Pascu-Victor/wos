#include <dev/iommu/dmar.hpp>
#include <dev/iommu/scopes.hpp>
#include <vector>

extern "C" auto LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) -> int {
    namespace dmar = ker::dev::iommu::dmar;
    auto check = [](std::span<const uint8_t> bytes) {
        auto const RESULT = dmar::parse(bytes);
        if (RESULT.valid()) {
            if (RESULT.table.bytes.size() > bytes.size() || RESULT.table.units == 0 || RESULT.table.scopes > dmar::MAX_SCOPES) {
                __builtin_trap();
            }
        } else if (!RESULT.table.bytes.empty() || RESULT.table.units != 0) {
            __builtin_trap();
        }
        namespace scopes = ker::dev::iommu::scopes;
        constexpr std::array<scopes::Device, 4> DEVICES{{{.slot = 1},
                                                         {.slot = 2, .header = 1, .secondary = 3, .subordinate = 3},
                                                         {.bus = 3, .slot = 4, .parent = 1},
                                                         {.slot = 1, .function = 1}}};
        auto const OWNERS = scopes::qualify(bytes, DEVICES, true);
        if (OWNERS.valid()) {
            if (!RESULT.valid() || OWNERS.device_count != DEVICES.size() || OWNERS.region_count > scopes::MAX_REGIONS) __builtin_trap();
            for (size_t i = 0; i < DEVICES.size(); ++i) {
                auto const& binding = OWNERS.devices[i];
                if (binding.unit >= RESULT.table.units || (OWNERS.region_count < 64 && (binding.reserved >> OWNERS.region_count) != 0))
                    __builtin_trap();
            }
        } else if (OWNERS.device_count != 0 || OWNERS.region_count != 0) {
            __builtin_trap();
        }
    };
    check({data, size});
    // Also reach structural validation without waiting for random mutations to
    // discover both the ACPI signature and checksum. Keep raw-input coverage.
    if (size <= dmar::MAX_TABLE_SIZE - dmar::HEADER_SIZE) {
        std::vector<uint8_t> bytes(dmar::HEADER_SIZE);
        bytes.insert(bytes.end(), data, data + size);
        bytes[0] = 'D';
        bytes[1] = 'M';
        bytes[2] = 'A';
        bytes[3] = 'R';
        for (size_t i = 0; i < 4; ++i) bytes[4 + i] = static_cast<uint8_t>(bytes.size() >> (i * 8U));
        bytes[8] = 1;
        bytes[36] = 47;
        uint8_t sum = 0;
        for (auto byte : bytes) sum = static_cast<uint8_t>(sum + byte);
        bytes[9] = static_cast<uint8_t>(0U - sum);
        check(bytes);
    }
    return 0;
}
