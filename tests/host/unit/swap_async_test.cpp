#include <gtest/gtest.h>

#include "../../../modules/kern/src/test/swap_async_ktest.cpp"

// Platform/VFS/registry seams only; the production KTEST body, swap area/slot
// implementation, CRC routine, and block request runtime compile unchanged.
namespace ker::dev {
auto block_device_is_read_only(const BlockDevice* device) -> bool { return device != nullptr && device->read_only; }
auto block_device_find_by_name(const char*) -> BlockDevice* { return nullptr; }
auto block_read(BlockDevice* device, uint64_t lba, size_t count, void* buffer) -> int {
    return device->read_blocks(device, lba, count, buffer);
}
auto block_write(BlockDevice* device, uint64_t lba, size_t count, const void* buffer) -> int {
    return device->write_blocks(device, lba, count, buffer);
}
}  // namespace ker::dev

namespace ker::test {
int g_pass{};
int g_fail{};
void check_eq_log(const char* file, int line, uint64_t a, uint64_t b, const char* as, const char* bs) {
    ADD_FAILURE_AT(file, line) << as << "=" << a << ", " << bs << "=" << b;
}
void check_ne_log(const char* file, int line, uint64_t, uint64_t, const char* as, const char* bs) {
    ADD_FAILURE_AT(file, line) << as << " == " << bs;
}
void check_true_log(const char* file, int line, const char* expression) { ADD_FAILURE_AT(file, line) << expression; }
void check_null_log(const char* file, int line, const void*, const char* expression) { ADD_FAILURE_AT(file, line) << expression; }
}  // namespace ker::test

TEST(SwapAsync, ActualKernelTrace) {
    _kt_SwapAsync_DelayedIoPinsGenerationAndAreaUntilFinish();
    EXPECT_EQ(ker::test::g_fail, 0);
    EXPECT_GE(ker::test::g_pass, 40);
    RecordProperty("ktest_assertions", ker::test::g_pass);
}
