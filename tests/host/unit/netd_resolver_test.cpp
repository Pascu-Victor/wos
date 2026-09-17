#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>

#include "netd/resolver.hpp"

namespace {
class NetdResolver : public testing::Test {
   protected:
    std::filesystem::path base;
    int directory{-1};
    netd::DhcpLease lease{};
    void SetUp() override {
        std::array<char, 64> pattern{};
        std::strcpy(pattern.data(), "/tmp/wos-netd-resolver-XXXXXX");
        ASSERT_NE(::mkdtemp(pattern.data()), nullptr);
        base = pattern.data();
        directory = ::open(base.c_str(), O_RDONLY | O_DIRECTORY);
        ASSERT_GE(directory, 0);
        ASSERT_EQ(::fchmod(directory, 0755), 0);
        create("resolv.conf", 0644, "original");
        lease.dns_count = 3;
        lease.dns_servers = {0x01020304, 0x05060708, 0x090A0B0C};
        std::strcpy(lease.search_domains.data(), "example.test");
    }
    void TearDown() override {
        if (directory >= 0) ::close(directory);
        std::filesystem::remove_all(base);
    }
    void create(const char* name, mode_t mode, const char* content) {
        int const FD = ::openat(directory, name, O_CREAT | O_EXCL | O_WRONLY, mode);
        ASSERT_GE(FD, 0);
        ASSERT_EQ(::fchmod(FD, mode), 0);
        ASSERT_EQ(::write(FD, content, std::strlen(content)), static_cast<ssize_t>(std::strlen(content)));
        ASSERT_EQ(::close(FD), 0);
    }
    auto read(const char* name) -> std::string {
        std::ifstream file(base / name);
        return {std::istreambuf_iterator<char>(file), {}};
    }
};

TEST_F(NetdResolver, ReplacementKeepsOldReadersAndPublishesCompletePublicFile) {
    int const OLD = ::openat(directory, "resolv.conf", O_RDONLY);
    ASSERT_GE(OLD, 0);
    ASSERT_EQ(netd::write_resolv_conf_at(directory, lease), 0);
    EXPECT_EQ(read("resolv.conf"),
              "# Managed by netd via DHCP\noptions timeout:2 attempts:4\nnameserver 1.2.3.4\nnameserver 5.6.7.8\nnameserver "
              "9.10.11.12\nsearch example.test\n");
    std::array<char, 16> original{};
    EXPECT_EQ(::read(OLD, original.data(), original.size()), 8);
    EXPECT_STREQ(original.data(), "original");
    ::close(OLD);
    struct stat state{};
    ASSERT_EQ(::fstatat(directory, "resolv.conf", &state, AT_SYMLINK_NOFOLLOW), 0);
    EXPECT_EQ(state.st_mode, S_IFREG | 0644);
    EXPECT_EQ(state.st_nlink, 1U);
    EXPECT_EQ(state.st_uid, ::geteuid());
    EXPECT_EQ(state.st_gid, ::getegid());
    EXPECT_FALSE(std::filesystem::exists(base / "resolv.conf.tmp"));
}

TEST_F(NetdResolver, RejectsAliasesAndSpecialFilesWithoutTouchingSentinel) {
    create("sentinel", 0600, "untouched");
    for (unsigned variant = 0; variant < 4; ++variant) {
        if (variant == 0) ASSERT_EQ(::symlinkat("sentinel", directory, "resolv.conf.tmp"), 0);
        if (variant == 1) ASSERT_EQ(::linkat(directory, "sentinel", directory, "resolv.conf.tmp", 0), 0);
        if (variant == 2) ASSERT_EQ(::mkfifoat(directory, "resolv.conf.tmp", 0600), 0);
        if (variant == 3) create("resolv.conf.tmp", 0666, "untrusted");
        EXPECT_EQ(netd::write_resolv_conf_at(directory, lease), EPERM);
        EXPECT_EQ(read("resolv.conf"), "original");
        EXPECT_EQ(read("sentinel"), "untouched");
        ASSERT_EQ(::unlinkat(directory, "resolv.conf.tmp", 0), 0);
    }
    ASSERT_EQ(::unlinkat(directory, "resolv.conf", 0), 0);
    ASSERT_EQ(::symlinkat("sentinel", directory, "resolv.conf"), 0);
    EXPECT_EQ(netd::write_resolv_conf_at(directory, lease), EPERM);
    EXPECT_EQ(read("sentinel"), "untouched");
    EXPECT_FALSE(std::filesystem::exists(base / "resolv.conf.tmp"));
}

TEST_F(NetdResolver, RecoversOnlyOwnedSingleLinkTemporaryFiles) {
    for (mode_t mode : {0600, 0644}) {
        create("resolv.conf.tmp", mode, "interrupted");
        EXPECT_EQ(netd::write_resolv_conf_at(directory, lease), 0);
        EXPECT_NE(read("resolv.conf").find("nameserver 1.2.3.4"), std::string::npos);
        EXPECT_FALSE(std::filesystem::exists(base / "resolv.conf.tmp"));
    }
}

TEST_F(NetdResolver, RejectsInvalidLeaseAndWritableDirectoryBeforeMutation) {
    lease.dns_count = 4;
    EXPECT_EQ(netd::write_resolv_conf_at(directory, lease), EINVAL);
    lease.dns_count = 3;
    lease.domain_name.fill('x');
    EXPECT_EQ(netd::write_resolv_conf_at(directory, lease), EINVAL);
    lease.domain_name.fill(0);
    ASSERT_EQ(::fchmod(directory, 0777), 0);
    EXPECT_EQ(netd::write_resolv_conf_at(directory, lease), EPERM);
    EXPECT_EQ(read("resolv.conf"), "original");
    EXPECT_FALSE(std::filesystem::exists(base / "resolv.conf.tmp"));
}
}  // namespace
