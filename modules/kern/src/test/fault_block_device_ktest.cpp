#include <memory>
#include <new>
#include <test/fault_block_device_impl.hpp>
#include <test/ktest.hpp>

KTEST(FaultBlockDevice, ReusableFixtureStartsDeterministic) {
    auto device = std::unique_ptr<ker::test::FaultBlockDevice>(new (std::nothrow) ker::test::FaultBlockDevice(512, 4, 0x5a));
    KEXPECT_TRUE(device != nullptr);
    if (device == nullptr) {
        return;
    }
    KEXPECT_TRUE(device->valid());
    KEXPECT_EQ(device->media_bytes(), static_cast<size_t>(2048));
    KEXPECT_TRUE(device->volatile_matches_durable());
}
