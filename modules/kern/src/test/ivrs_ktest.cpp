#include <array>
#include <dev/iommu/ivrs.hpp>
#include <test/ktest.hpp>

KTEST(Ivrs, BoundedObservationNeverAuthorizesHardware) {
    namespace ivrs = ker::dev::iommu::ivrs;
    // A synthetic IVHD10 and inclusive alias range. This proves parsing only.
    std::array<uint8_t, 84> bytes{};
    bytes[0] = 'I';
    bytes[1] = 'V';
    bytes[2] = 'R';
    bytes[3] = 'S';
    bytes[4] = bytes.size();
    bytes[8] = 1;
    bytes[48] = 0x10;
    bytes[50] = 36;
    bytes[54] = 0x40;
    bytes[58] = 0xd9;
    bytes[59] = 0xfe;
    bytes[72] = 0x43;
    bytes[73] = 0x10;
    bytes[77] = 0x80;
    bytes[80] = 4;
    bytes[81] = 0x20;
    auto checksum = [&] {
        bytes[9] = 0;
        uint8_t sum = 0;
        for (auto byte : bytes) sum = static_cast<uint8_t>(sum + byte);
        bytes[9] = static_cast<uint8_t>(0U - sum);
    };
    checksum();
    auto result = ivrs::parse(bytes);
    KREQUIRE_TRUE(result.valid());
    KEXPECT_EQ(result.table.hardware_blocks, size_t{1});
    KEXPECT_EQ(result.table.device_entries, size_t{2});
    KEXPECT_EQ(result.table.bytes.data(), bytes.data());
    for (size_t size = 0; size < bytes.size(); ++size) {
        result = ivrs::parse(std::span(bytes).first(size));
        KEXPECT_FALSE(result.valid());
        KEXPECT_TRUE(result.table.bytes.empty());
    }
    bytes[81] = 0x0f;
    checksum();
    result = ivrs::parse(bytes);
    KEXPECT_EQ(result.error, ivrs::Error::device_range);
    KEXPECT_TRUE(result.table.bytes.empty());
    KEXPECT_EQ(result.table.hardware_blocks, size_t{0});
}
