#include <gtest/gtest.h>

#include <array>
#include <cstring>
#include <limits>
#include <vfs/flock_abi.hpp>

using ker::vfs::VfsFlockAbi;

TEST(VfsFlockAbi, CopiesTheWholeSignedUserPid) {
    for (int64_t pid : std::array<int64_t, 5>{-1, 0, 1, 0x100000000, std::numeric_limits<int64_t>::max()}) {
        VfsFlockAbi kernel{.l_type = 1, .l_len = 16, .l_pid = pid};
        std::array<unsigned char, sizeof(kernel)> user{};
        std::memcpy(user.data(), &kernel, sizeof(kernel));
        int64_t observed = 0;
        std::memcpy(&observed, user.data() + 24, sizeof(observed));
        EXPECT_EQ(observed, pid);
    }
}

TEST(VfsFlockAbi, ReadsHighPidBitsBeforeAnOfdAdmissionDecision) {
    std::array<unsigned char, sizeof(VfsFlockAbi)> user{};
    int64_t const PID = 0x100000000;
    std::memcpy(user.data() + 24, &PID, sizeof(PID));
    VfsFlockAbi kernel{};
    std::memcpy(&kernel, user.data(), sizeof(kernel));
    EXPECT_EQ(kernel.l_pid, PID);
    EXPECT_NE(kernel.l_pid, 0);
}
