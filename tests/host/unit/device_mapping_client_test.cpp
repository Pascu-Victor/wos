#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <cerrno>
#include <dev/device_mapping_client.hpp>
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <type_traits>
#include <utility>
#include <vfs/file.hpp>

namespace {
namespace mapping = ker::dev::mapping;
namespace security = ker::mod::security;

struct Context final : mapping::EndpointContext {
    explicit Context(std::atomic<unsigned>& destroyed) : destroyed(destroyed) {}
    ~Context() override { ++destroyed; }
    std::atomic<unsigned>& destroyed;
};

auto make_endpoint(std::atomic<unsigned>& destroyed) -> mapping::EndpointRef {
    mapping::EndpointRef endpoint;
    EXPECT_EQ(mapping::Endpoint::create(240, 1, "owned-endpoint", {}, std::make_unique<Context>(destroyed), endpoint), 0);
    return endpoint;
}

static_assert(!std::is_copy_constructible_v<mapping::EndpointRef>);
static_assert(!std::is_copy_constructible_v<mapping::ClientRef>);
static_assert(std::is_nothrow_move_constructible_v<mapping::EndpointRef>);
static_assert(std::is_nothrow_move_constructible_v<mapping::ClientRef>);
static_assert(!std::is_copy_constructible_v<mapping::Endpoint>);
static_assert(!std::is_copy_constructible_v<mapping::Client>);

TEST(DeviceMappingClient, CheckedReferenceEngineRefusesDeadAndSaturatedCounts) {
    mapping::detail::ReferenceCount dead(0);
    EXPECT_FALSE(dead.retain());
    EXPECT_DEATH(static_cast<void>(dead.drop_last()), "");
    mapping::detail::ReferenceCount saturated(std::numeric_limits<uint32_t>::max());
    EXPECT_FALSE(saturated.retain());
    EXPECT_FALSE(saturated.drop_last());
    EXPECT_TRUE(saturated.retain());
    EXPECT_FALSE(saturated.retain());
    mapping::detail::ReferenceCount live;
    EXPECT_TRUE(live.retain());
    EXPECT_FALSE(live.drop_last());
    EXPECT_TRUE(live.drop_last());
    EXPECT_FALSE(live.retain());
}

TEST(DeviceMappingClient, EmptyAndMoveOwnersReleaseTheCompleteContextExactlyOnce) {
    std::atomic<unsigned> destroyed{};
    mapping::EndpointRef empty;
    EXPECT_FALSE(empty.clone_into(empty));
    empty.reset();
    auto endpoint = make_endpoint(destroyed);
    ASSERT_TRUE(endpoint);
    auto* const identity = endpoint.get();
    mapping::EndpointRef second;
    ASSERT_TRUE(endpoint.clone_into(second));
    EXPECT_FALSE(endpoint.clone_into(second));
    EXPECT_EQ(second.get(), identity);
    auto moved = std::move(endpoint);
    EXPECT_FALSE(endpoint);
    moved = std::move(moved);
    EXPECT_EQ(moved.get(), identity);
    second.reset();
    EXPECT_EQ(destroyed.load(), 0U);
    moved.reset();
    EXPECT_EQ(destroyed.load(), 1U);
    moved.reset();
    EXPECT_EQ(destroyed.load(), 1U);
}

TEST(DeviceMappingClient, FactoryRejectsBorrowedOpenHooksAndInvalidNamesWithoutPublishing) {
    std::atomic<unsigned> destroyed{};
    for (std::string_view const name : std::array<std::string_view, 6>{"", ".", "..", "a/b", std::string_view("a\0b", 3), "bad\nname"}) {
        mapping::EndpointRef endpoint;
        EXPECT_EQ(mapping::Endpoint::create(1, 2, name, {}, std::make_unique<Context>(destroyed), endpoint), -EINVAL);
        EXPECT_FALSE(endpoint);
    }
    mapping::EndpointRef endpoint;
    std::string oversized(mapping::Endpoint::NAME_CAPACITY, 'x');
    EXPECT_EQ(mapping::Endpoint::create(1, 2, oversized, {}, std::make_unique<Context>(destroyed), endpoint), -EINVAL);
    ker::dev::CharDeviceOps operations{};
    operations.open = [](ker::vfs::File*) { return 0; };
    EXPECT_EQ(mapping::Endpoint::create(1, 2, "open", operations, std::make_unique<Context>(destroyed), endpoint), -EINVAL);
    operations.open = nullptr;
    operations.open_with_credentials = [](ker::vfs::File*, const security::Snapshot&, ker::mod::sched::task::Task*) { return 0; };
    EXPECT_EQ(mapping::Endpoint::create(1, 2, "credentials", operations, std::make_unique<Context>(destroyed), endpoint), -EINVAL);
    EXPECT_EQ(mapping::Endpoint::create(1, 2, "null", {}, nullptr, endpoint), -EINVAL);
    EXPECT_FALSE(endpoint);
    EXPECT_EQ(destroyed.load(), 9U);
}

TEST(DeviceMappingClient, CapturesOwnedCredentialsAndImmutableAccessCeilings) {
    std::atomic<unsigned> destroyed{};
    auto endpoint = make_endpoint(destroyed);
    std::array<uint32_t, 2> groups{101, 102};
    security::Snapshot credentials;
    ASSERT_EQ(security::Snapshot::restore_fields({.user = {42, 43, 44}, .group = {51, 52, 53}, .groups = groups}, credentials), 0);
    mapping::ClientRef client;
    ASSERT_EQ(mapping::Client::open(endpoint, credentials, {.open_flags = ker::vfs::O_CLOEXEC, .read_only = true, .nosuid = true}, client),
              0);
    groups.fill(900);
    credentials = security::Snapshot::bootstrap_root();
    endpoint.reset();
    ASSERT_TRUE(client);
    EXPECT_EQ(client.get()->credentials().user().effective, 43U);
    EXPECT_EQ(client.get()->credentials().group().effective, 52U);
    ASSERT_EQ(client.get()->credentials().groups().size(), 2U);
    EXPECT_EQ(client.get()->credentials().groups()[0], 101U);
    EXPECT_EQ(client.get()->credentials().groups()[1], 102U);
    EXPECT_EQ(client.get()->access().open_flags, static_cast<unsigned>(ker::vfs::O_CLOEXEC));
    EXPECT_TRUE(client.get()->access().read_only);
    EXPECT_TRUE(client.get()->access().nosuid);
    EXPECT_EQ(destroyed.load(), 0U);
    client.reset();
    EXPECT_EQ(destroyed.load(), 1U);
}

TEST(DeviceMappingClient, ClientSharesExactEndpointAndSurvivesIndependentOwnerRelease) {
    std::atomic<unsigned> destroyed{};
    auto endpoint = make_endpoint(destroyed);
    auto* const identity = endpoint.get();
    mapping::ClientRef client, retained;
    ASSERT_EQ(mapping::Client::open(endpoint, {}, {}, client), 0);
    ASSERT_TRUE(client.clone_into(retained));
    EXPECT_FALSE(client.clone_into(retained));
    auto* const client_identity = client.get();
    auto moved = std::move(client);
    EXPECT_FALSE(client);
    EXPECT_EQ(moved.get(), client_identity);
    EXPECT_EQ(&moved.get()->endpoint(), identity);
    endpoint.reset();
    moved.reset();
    EXPECT_EQ(retained.get(), client_identity);
    EXPECT_EQ(destroyed.load(), 0U);
    retained.reset();
    EXPECT_EQ(destroyed.load(), 1U);
}

TEST(DeviceMappingClient, InvalidAdmissionLeavesExistingOutputsAndEndpointUntouched) {
    std::atomic<unsigned> destroyed{};
    auto endpoint = make_endpoint(destroyed);
    mapping::ClientRef client;
    EXPECT_EQ(mapping::Client::open({}, {}, {}, client), -EINVAL);
    EXPECT_EQ(mapping::Client::open(endpoint, {}, {.open_flags = 3}, client), -EINVAL);
    EXPECT_EQ(mapping::Client::open(endpoint, {}, {.open_flags = 1, .read_only = true}, client), -EROFS);
    EXPECT_EQ(mapping::Client::open(endpoint, {}, {.open_flags = 2, .read_only = true}, client), -EROFS);
    EXPECT_EQ(mapping::Client::open(endpoint, {}, {.open_flags = ker::vfs::O_TRUNC, .read_only = true}, client), -EROFS);
    EXPECT_FALSE(client);
    ASSERT_EQ(mapping::Client::open(endpoint, {}, {}, client), 0);
    auto* const identity = client.get();
    EXPECT_EQ(mapping::Client::open(endpoint, {}, {}, client), -EINVAL);
    EXPECT_EQ(client.get(), identity);
    EXPECT_EQ(destroyed.load(), 0U);
}

TEST(DeviceMappingClient, ClosingAdmissionIsPermanentWithoutRevokingRetainedClients) {
    std::atomic<unsigned> destroyed{};
    auto endpoint = make_endpoint(destroyed);
    mapping::ClientRef existing, denied;
    ASSERT_EQ(mapping::Client::open(endpoint, {}, {}, existing), 0);
    endpoint.get()->close_admission();
    endpoint.get()->close_admission();
    EXPECT_FALSE(endpoint.get()->accepting_clients());
    EXPECT_EQ(mapping::Client::open(endpoint, {}, {}, denied), -ENODEV);
    EXPECT_FALSE(denied);
    EXPECT_EQ(&existing.get()->endpoint(), endpoint.get());
    endpoint.reset();
    EXPECT_EQ(destroyed.load(), 0U);
    existing.reset();
    EXPECT_EQ(destroyed.load(), 1U);
}

TEST(DeviceMappingClient, ConcurrentRetainedReadersDoNotReopenOrReplaceTheClient) {
    std::atomic<unsigned> destroyed{}, failures{};
    auto endpoint = make_endpoint(destroyed);
    mapping::ClientRef client;
    ASSERT_EQ(mapping::Client::open(endpoint, security::Snapshot::bootstrap_root(), {.open_flags = 2, .nosuid = true}, client), 0);
    auto* const identity = client.get();
    std::array<std::thread, 4> readers;
    for (auto& reader : readers) {
        reader = std::thread([&] {
            for (unsigned iteration = 0; iteration < 1000; ++iteration) {
                mapping::ClientRef retained;
                if (!client.clone_into(retained) || retained.get() != identity || retained.get()->access().open_flags != 2 ||
                    !retained.get()->access().nosuid || retained.get()->credentials().user().effective != 0)
                    ++failures;
            }
        });
    }
    endpoint.get()->close_admission();
    for (auto& reader : readers) reader.join();
    EXPECT_EQ(failures.load(), 0U);
    EXPECT_EQ(client.get(), identity);
    EXPECT_EQ(destroyed.load(), 0U);
    endpoint.reset();
    client.reset();
    EXPECT_EQ(destroyed.load(), 1U);
}
}  // namespace
