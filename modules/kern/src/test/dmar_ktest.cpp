#include <array>
#include <cstddef>
#include <cstdint>
#include <dev/iommu/dmar.hpp>
#include <span>
#include <test/ktest.hpp>

KTEST(Dmar, BoundedFirmwareDecode) {
    namespace dmar = ker::dev::iommu::dmar;
    std::array<uint8_t, 64> bytes{};
    bytes[0] = 'D';
    bytes[1] = 'M';
    bytes[2] = 'A';
    bytes[3] = 'R';
    bytes[4] = 64;
    bytes[8] = 1;
    bytes[36] = 47;
    bytes[50] = 16;
    bytes[52] = 1;
    bytes[58] = 0xd9;
    bytes[59] = 0xfe;
    uint8_t sum = 0;
    for (auto byte : bytes) {
        sum = static_cast<uint8_t>(sum + byte);
    }
    bytes[9] = static_cast<uint8_t>(0U - sum);
    auto result = dmar::parse(bytes);
    KREQUIRE_TRUE(result.valid());
    KEXPECT_EQ(result.table.units, size_t{1});
    KEXPECT_EQ(result.table.address_bits, uint8_t{48});
    KEXPECT_FALSE(result.table.unsupported);
    for (size_t size = 0; size < bytes.size(); ++size) {
        result = dmar::parse(std::span(bytes).first(size));
        KEXPECT_FALSE(result.valid());
        KEXPECT_TRUE(result.table.bytes.empty());
    }
    bytes[50] = 0;
    bytes[9] = static_cast<uint8_t>(bytes[9] + 16U);
    result = dmar::parse(bytes);
    KEXPECT_EQ(result.error, dmar::Error::record_length);
    KEXPECT_TRUE(result.table.bytes.empty());
}
