#include <cerrno>
#include <cstring>
#include <test/ktest.hpp>
#include <util/hostname.hpp>

KTEST(Hostname, InvalidPublicationPreservesSnapshotAndCopiesDoNotAlias) {
    namespace hostname = ker::util::hostname;
    auto const ORIGINAL = hostname::snapshot();
    KEXPECT_EQ(hostname::set("hostname-check", 14), 0);
    auto const HELD = hostname::snapshot();
    KEXPECT_EQ(hostname::set(nullptr, 1), -EINVAL);
    KEXPECT_EQ(hostname::set("", 0), -EINVAL);
    KEXPECT_EQ(hostname::set("bad-name-", 9), -EINVAL);
    KEXPECT_EQ(hostname::set("-bad", 4), -EINVAL);
    KEXPECT_EQ(hostname::set("bad.name", 8), -EINVAL);
    KEXPECT_EQ(hostname::set("bad\0name", 8), -EINVAL);
    KEXPECT_EQ(hostname::set(HELD.data(), hostname::HOSTNAME_MAX), -EINVAL);
    KEXPECT_TRUE(hostname::snapshot() == HELD);
    KEXPECT_EQ(hostname::set("x", 1), 0);
    auto const SHORT = hostname::snapshot();
    KEXPECT_TRUE(std::strcmp(HELD.data(), "hostname-check") == 0);
    KEXPECT_EQ(SHORT.front(), 'x');
    for (size_t i = 1; i < SHORT.size(); ++i) KEXPECT_EQ(SHORT[i], '\0');
    KEXPECT_EQ(hostname::set(ORIGINAL.data(), std::strlen(ORIGINAL.data())), 0);
    KEXPECT_TRUE(hostname::snapshot() == ORIGINAL);
}
