#include <gtest/gtest.h>

#include <cerrno>
#include <cstdint>
#include <string>

#include "wkictl/peer_resolver.hpp"

namespace {

constexpr char HEADER[] = "hostname node_id connected cpus load_pct last_update_us local\n";

auto resolve(const std::string& snapshot, const char* hostname, uint16_t* node_id) -> bool {
    errno = 0;
    return wkictl::resolve_peer_hostname_snapshot(snapshot.data(), snapshot.size(), hostname, node_id);
}

TEST(WkiPeerResolver, ResolvesExactlyOneConnectedHostname) {
    std::string const SNAPSHOT = std::string(HEADER) + "wos-0 417 1 4 3 100 1\n" + "wos-1 9021 1 8 7 200 0\n";
    uint16_t node_id = 0;

    ASSERT_TRUE(resolve(SNAPSHOT, "wos-1", &node_id));
    EXPECT_EQ(node_id, 9021);
}

TEST(WkiPeerResolver, MissingAndDisconnectedNamesFailClosed) {
    std::string const SNAPSHOT = std::string(HEADER) + "wos-0 417 1 4 3 100 1\n" + "wos-1 9021 0 8 7 200 0\n";
    uint16_t node_id = 123;

    EXPECT_FALSE(resolve(SNAPSHOT, "wos-2", &node_id));
    EXPECT_EQ(errno, ENOENT);
    EXPECT_EQ(node_id, 123);

    EXPECT_FALSE(resolve(SNAPSHOT, "wos-1", &node_id));
    EXPECT_EQ(errno, ENOTCONN);
    EXPECT_EQ(node_id, 123);
}

TEST(WkiPeerResolver, DuplicateHostnameOrNodeIdentityFailsClosed) {
    std::string const DUPLICATE_HOST =
        std::string(HEADER) + "wos-0 417 1 4 3 100 1\n" + "wos-1 9021 1 8 7 200 0\n" + "wos-1 9022 1 8 7 201 0\n";
    std::string const DUPLICATE_NODE =
        std::string(HEADER) + "wos-0 417 1 4 3 100 1\n" + "wos-1 9021 1 8 7 200 0\n" + "wos-2 9021 1 8 7 201 0\n";
    uint16_t node_id = 0;

    EXPECT_FALSE(resolve(DUPLICATE_HOST, "wos-1", &node_id));
    EXPECT_EQ(errno, EEXIST);
    EXPECT_FALSE(resolve(DUPLICATE_NODE, "wos-1", &node_id));
    EXPECT_EQ(errno, EEXIST);
}

TEST(WkiPeerResolver, MalformedOrAmbiguousProducerOutputFailsClosed) {
    uint16_t node_id = 0;
    std::string const NO_LOCAL = std::string(HEADER) + "wos-1 9021 1 8 7 200 0\n";
    std::string const TWO_LOCALS = std::string(HEADER) + "wos-0 417 1 4 3 100 1\n" + "wos-x 418 1 4 3 101 1\n" + "wos-1 9021 1 8 7 200 0\n";
    std::string const BAD_COLUMNS = std::string(HEADER) + "wos-0 417 1 4 3 100 1\n" + "wos-1 9021 1 8 7 0\n";
    std::string const BAD_NODE = std::string(HEADER) + "wos-0 417 1 4 3 100 1\n" + "wos-1 65535 1 8 7 200 0\n";

    EXPECT_FALSE(resolve(NO_LOCAL, "wos-1", &node_id));
    EXPECT_EQ(errno, EBADMSG);
    EXPECT_FALSE(resolve(TWO_LOCALS, "wos-1", &node_id));
    EXPECT_EQ(errno, EBADMSG);
    EXPECT_FALSE(resolve(BAD_COLUMNS, "wos-1", &node_id));
    EXPECT_EQ(errno, EBADMSG);
    EXPECT_FALSE(resolve(BAD_NODE, "wos-1", &node_id));
    EXPECT_EQ(errno, EBADMSG);
}

TEST(WkiPeerResolver, MissingNewlineAndProducerCeilingRejectTruncation) {
    uint16_t node_id = 0;
    std::string const NO_NEWLINE = std::string(HEADER) + "wos-0 417 1 4 3 100 1\n" + "wos-1 9021 1 8 7 200 0";
    std::string at_ceiling(wkictl::WKI_PEER_PROC_SNAPSHOT_CAPACITY - 1, 'x');
    at_ceiling.replace(0, sizeof(HEADER) - 1, HEADER);
    at_ceiling.back() = '\n';

    EXPECT_FALSE(resolve(NO_NEWLINE, "wos-1", &node_id));
    EXPECT_EQ(errno, EBADMSG);
    EXPECT_FALSE(resolve(at_ceiling, "wos-1", &node_id));
    EXPECT_EQ(errno, EOVERFLOW);
}

}  // namespace
