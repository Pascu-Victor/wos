#include <gtest/gtest.h>

#include <array>
#include <cstdint>
#include <netd/ra.hpp>

namespace {

void store_be16(uint8_t* out, uint16_t value) {
    out[0] = static_cast<uint8_t>(value >> 8U);
    out[1] = static_cast<uint8_t>(value);
}

void store_be32(uint8_t* out, uint32_t value) {
    out[0] = static_cast<uint8_t>(value >> 24U);
    out[1] = static_cast<uint8_t>(value >> 16U);
    out[2] = static_cast<uint8_t>(value >> 8U);
    out[3] = static_cast<uint8_t>(value);
}

auto make_ra(uint8_t prefix_flags = 0xC0, uint16_t router_lifetime = 180, uint32_t valid = 600, uint32_t preferred = 300)
    -> std::array<uint8_t, 88> {
    std::array<uint8_t, 88> packet{};
    packet[0] = 0x60;
    store_be16(packet.data() + 4, 48);
    packet[6] = 58;
    packet[7] = 255;
    packet[8] = 0xFE;
    packet[9] = 0x80;
    packet[23] = 1;
    packet[24] = 0xFF;
    packet[25] = 0x02;
    packet[39] = 1;
    packet[40] = 134;
    store_be16(packet.data() + 46, router_lifetime);

    packet[56] = 3;
    packet[57] = 4;
    packet[58] = 64;
    packet[59] = prefix_flags;
    store_be32(packet.data() + 60, valid);
    store_be32(packet.data() + 64, preferred);
    packet[72] = 0x20;
    packet[73] = 0x01;
    packet[74] = 0x0D;
    packet[75] = 0xB8;
    packet[77] = 6;
    return packet;
}

auto make_ra_with_mtu(uint32_t mtu) -> std::array<uint8_t, 96> {
    std::array<uint8_t, 96> packet{};
    auto prefix = make_ra();
    std::copy(prefix.begin(), prefix.end(), packet.begin());
    store_be16(packet.data() + 4, 56);
    packet[88] = 5;
    packet[89] = 1;
    store_be32(packet.data() + 92, mtu);
    return packet;
}

constexpr std::array<uint8_t, 6> MAC = {0x52, 0x54, 0, 0, 0, 1};

}  // namespace

TEST(NetdRa, ParsesStrictValidAdvertisementAndSlaacPrefix) {
    auto packet = make_ra();
    netd::RouterAdvertisement advert{};
    EXPECT_EQ(netd::parse_router_advertisement(packet, advert), netd::RaParseError::NONE);
    ASSERT_EQ(advert.prefix_count, 1u);
    EXPECT_TRUE(advert.prefixes[0].on_link);
    EXPECT_TRUE(advert.prefixes[0].autonomous);

    std::array<uint8_t, 16> address{};
    ASSERT_TRUE(netd::make_slaac_address(advert.prefixes[0], MAC, address));
    EXPECT_EQ(address[0], 0x20);
    EXPECT_EQ(address[5], 6);
    EXPECT_EQ(address[8], static_cast<uint8_t>(MAC[0] ^ 2U));
    EXPECT_EQ(address[11], 0xFF);
    EXPECT_EQ(address[12], 0xFE);
}

TEST(NetdRa, RejectsHopSourceAndOptionFramingViolations) {
    auto packet = make_ra();
    netd::RouterAdvertisement advert{};
    packet[7] = 64;
    EXPECT_EQ(netd::parse_router_advertisement(packet, advert), netd::RaParseError::BAD_HOP_LIMIT);
    packet = make_ra();
    packet[8] = 0x20;
    EXPECT_EQ(netd::parse_router_advertisement(packet, advert), netd::RaParseError::BAD_SOURCE);
    packet = make_ra();
    packet[57] = 0;
    EXPECT_EQ(netd::parse_router_advertisement(packet, advert), netd::RaParseError::MALFORMED_OPTION);
    packet = make_ra();
    packet[59] = 0x40;
    EXPECT_EQ(netd::parse_router_advertisement(packet, advert), netd::RaParseError::NONE);
}

TEST(NetdRa, LinkLocalIdentityRequiresLinkScopeAndCompletedDad) {
    std::array<uint8_t, 16> link_local{};
    link_local[0] = 0xFE;
    link_local[1] = 0x80;
    link_local[15] = 1;
    EXPECT_TRUE(netd::ipv6_link_local_usable(link_local, 253, 0));
    EXPECT_TRUE(netd::ipv6_link_local_usable(link_local, 253, 0x20));  // deprecated remains usable
    EXPECT_FALSE(netd::ipv6_link_local_usable(link_local, 0, 0));
    EXPECT_FALSE(netd::ipv6_link_local_usable(link_local, 253, 0x40));
    EXPECT_FALSE(netd::ipv6_link_local_usable(link_local, 253, 0x08));
    link_local[0] = 0x20;
    EXPECT_FALSE(netd::ipv6_link_local_usable(link_local, 253, 0));
}

TEST(NetdRa, PlansAOnlyPrefixWithoutConnectedRouteAndBoundsMtu) {
    auto packet = make_ra(0x40);
    netd::RouterAdvertisement advert{};
    ASSERT_EQ(netd::parse_router_advertisement(packet, advert), netd::RaParseError::NONE);
    auto plan = netd::plan_ra_update({}, advert, MAC, 9000);
    EXPECT_TRUE(plan.set_address);
    EXPECT_TRUE(plan.no_prefix_route);

    auto mtu_packet = make_ra_with_mtu(1500);
    ASSERT_EQ(netd::parse_router_advertisement(mtu_packet, advert), netd::RaParseError::NONE);
    plan = netd::plan_ra_update({}, advert, MAC, 9000);
    EXPECT_TRUE(plan.set_mtu);
    EXPECT_EQ(plan.mtu, 1500u);
    plan = netd::plan_ra_update({}, advert, MAC, 1400, 1400);
    EXPECT_FALSE(plan.set_mtu);

    plan = netd::plan_ra_update({}, advert, MAC, 9000, 1500);
    EXPECT_FALSE(plan.set_mtu);
    plan = netd::plan_ra_update({}, advert, MAC, 9000, 1400);
    EXPECT_TRUE(plan.set_mtu);
    EXPECT_EQ(plan.mtu, 1500u);

    mtu_packet = make_ra_with_mtu(1279);
    EXPECT_EQ(netd::parse_router_advertisement(mtu_packet, advert), netd::RaParseError::MALFORMED_OPTION);
}

TEST(NetdRa, PlansExplicitRouteForLinkOnlyPrefix) {
    auto packet = make_ra(0x80);
    netd::RouterAdvertisement advert{};
    ASSERT_EQ(netd::parse_router_advertisement(packet, advert), netd::RaParseError::NONE);
    auto plan = netd::plan_ra_update({}, advert, MAC);
    EXPECT_FALSE(plan.set_address);
    EXPECT_TRUE(plan.set_onlink_route);
    EXPECT_EQ(plan.onlink_prefix_len, 64);
    EXPECT_EQ(plan.onlink_prefix[0], 0x20);
    EXPECT_EQ(plan.onlink_prefix[5], 6);
    EXPECT_EQ(plan.onlink_lifetime_s, 600u);

    netd::RaManagedState state{};
    state.onlink_route_installed = true;
    state.onlink_prefix = plan.onlink_prefix;
    state.onlink_prefix_len = plan.onlink_prefix_len;
    packet = make_ra(0x80, 180, 0, 0);
    ASSERT_EQ(netd::parse_router_advertisement(packet, advert), netd::RaParseError::NONE);
    plan = netd::plan_ra_update(state, advert, MAC);
    EXPECT_TRUE(plan.delete_onlink_route);
    EXPECT_FALSE(plan.set_onlink_route);

    packet = make_ra(0x00);
    ASSERT_EQ(netd::parse_router_advertisement(packet, advert), netd::RaParseError::NONE);
    plan = netd::plan_ra_update(state, advert, MAC);
    EXPECT_TRUE(plan.delete_onlink_route);
    EXPECT_FALSE(plan.set_onlink_route);
}

TEST(NetdRa, WithdrawalDeletesOnlyManagedMatchingState) {
    auto packet = make_ra(0xC0, 0, 0, 0);
    netd::RouterAdvertisement advert{};
    ASSERT_EQ(netd::parse_router_advertisement(packet, advert), netd::RaParseError::NONE);

    netd::RaManagedState state{};
    state.address_installed = true;
    ASSERT_TRUE(netd::make_slaac_address(advert.prefixes[0], MAC, state.address));
    state.prefix_len = 64;
    state.route_installed = true;
    state.router = advert.source;
    auto const plan = netd::plan_ra_update(state, advert, MAC);
    EXPECT_TRUE(plan.delete_address);
    EXPECT_TRUE(plan.delete_route);
    EXPECT_FALSE(plan.set_address);
    EXPECT_FALSE(plan.set_route);
}
