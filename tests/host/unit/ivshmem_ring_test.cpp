#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <deque>
#include <dev/ivshmem/shared_ring.hpp>
#include <random>
#include <thread>
#include <vector>
using namespace ker::dev::ivshmem::shared_ring;
struct Backend {
    std::vector<uint8_t> data;
    uint32_t h = 0, t = 0;
    size_t writes = 0, reads = 0, head_stores = 0, tail_stores = 0;
    explicit Backend(size_t capacity) : data(capacity, 0xa5) {}
    auto head() -> uint32_t { return h; }
    auto tail() -> uint32_t { return t; }
    auto load(uint32_t pos) -> uint8_t {
        EXPECT_LT(pos, data.size());
        ++reads;
        return data.at(pos);
    }
    void store(uint32_t pos, uint8_t value) {
        EXPECT_LT(pos, data.size());
        ++writes;
        data.at(pos) = value;
    }
    void publish_head(uint32_t value) {
        ++head_stores;
        h = value;
    }
    void publish_tail(uint32_t value) {
        ++tail_stores;
        t = value;
    }
};
TEST(SharedRing, GeometryUsesWholeDisjointExtentsWithoutOverflow) {
    std::array<Extent, 2> rings = {{{64, 128}, {192, 128}}};
    EXPECT_TRUE(layout(4096, 64, rings, {320, 3776}));
    EXPECT_FALSE(layout(4095, 64, rings, {320, 3776}));
    for (auto bad : std::array<Extent, 5>{{{0, 128}, {65, 128}, {64, 8}, {64, 129}, {UINT32_MAX - 3, 128}}}) {
        auto copy = rings;
        copy[0] = bad;
        EXPECT_FALSE(layout(4096, 64, copy));
    }
    rings[1] = {188, 128};
    EXPECT_FALSE(layout(4096, 64, rings));
    rings[1] = {192, 128};
    EXPECT_FALSE(layout(4096, 64, rings, {316, 128}));
    EXPECT_FALSE(contains(UINT64_MAX, UINT64_MAX - 1, 4));
    EXPECT_TRUE(contains(UINT64_MAX, UINT64_MAX - 1, 1));
}
TEST(SharedRing, EveryWrappedFrameIsCompleteAndPublicationOccursOnce) {
    for (uint32_t start = 0; start < 64; start += 4)
        for (size_t size = 1; size <= 58; ++size) {
            Backend b(64);
            b.h = b.t = start;
            std::vector<uint8_t> payload(size), output(64, 0xcc);
            for (size_t i = 0; i < size; ++i) payload[i] = i * 17 + size;
            auto result = write(b, 64, payload);
            ASSERT_EQ(result.error, Error::none);
            EXPECT_EQ(result.bytes, size);
            EXPECT_EQ(b.head_stores, 1U);
            EXPECT_EQ(b.tail_stores, 0U);
            result = read(b, 64, output);
            ASSERT_EQ(result.error, Error::none);
            EXPECT_EQ(result.bytes, size);
            EXPECT_TRUE(std::equal(payload.begin(), payload.end(), output.begin()));
            EXPECT_TRUE(std::all_of(output.begin() + size, output.end(), [](auto x) { return x == 0xcc; }));
            EXPECT_EQ(b.h, b.t);
            EXPECT_EQ(b.tail_stores, 1U);
            EXPECT_EQ(read(b, 64, output).error, Error::empty);
        }
}
TEST(SharedRing, FullRingKeepsSentinelAndDoesNotMutateOnRefusal) {
    Backend b(16);
    std::array<uint8_t, 1> payload = {7};
    for (int i = 0; i < 3; ++i) ASSERT_EQ(write(b, 16, payload).error, Error::none);
    auto before = b.data;
    auto stores = b.writes;
    EXPECT_EQ(write(b, 16, payload).error, Error::full);
    EXPECT_EQ(b.data, before);
    EXPECT_EQ(b.writes, stores);
    EXPECT_NE(b.h, b.t);
    for (int i = 0; i < 3; ++i) EXPECT_EQ(read(b, 16, payload).error, Error::none);
    EXPECT_EQ(read(b, 16, payload).error, Error::empty);
}
TEST(SharedRing, MalformedIndicesNeverAccessDataOrPublish) {
    std::array<uint8_t, 8> payload{};
    for (uint32_t bad : {1U, 3U, 64U, UINT32_MAX})
        for (bool head : {false, true}) {
            Backend b(64);
            if (head)
                b.h = bad;
            else
                b.t = bad;
            EXPECT_EQ(write(b, 64, payload).error, Error::index);
            EXPECT_EQ(read(b, 64, payload).error, Error::index);
            EXPECT_EQ(b.reads + b.writes + b.head_stores + b.tail_stores, 0U);
        }
    for (uint32_t capacity : {0U, 4U, 7U, 63U}) {
        Backend b(64);
        EXPECT_EQ(read(b, capacity, payload).error, Error::geometry);
        EXPECT_EQ(write(b, capacity, payload).error, Error::geometry);
        EXPECT_EQ(b.reads + b.writes, 0U);
    }
}
TEST(SharedRing, IncompleteAndOversizedFramesPreserveConsumerAndOutput) {
    Backend b(64);
    b.h = 4;
    b.data[0] = 20;
    b.data[1] = 0;
    std::array<uint8_t, 32> output;
    output.fill(0xcc);
    EXPECT_EQ(read(b, 64, output).error, Error::incomplete);
    EXPECT_EQ(b.t, 0U);
    EXPECT_EQ(b.tail_stores, 0U);
    EXPECT_TRUE(std::all_of(output.begin(), output.end(), [](auto x) { return x == 0xcc; }));
    b.h = 24;
    EXPECT_EQ(read(b, 64, std::span(output).first(8)).error, Error::destination);
    EXPECT_EQ(b.t, 0U);
    EXPECT_EQ(b.tail_stores, 0U);
    EXPECT_EQ(output[0], 0xcc);
    b.data[0] = 0;
    EXPECT_EQ(read(b, 64, output).error, Error::length);
    EXPECT_EQ(b.t, 0U);
}
TEST(SharedRing, InvalidPayloadLengthsNeverTruncateOrPublish) {
    Backend b(64);
    std::vector<uint8_t> data(65536);
    EXPECT_EQ(write(b, 64, data).error, Error::length);
    EXPECT_EQ(write(b, 64, std::span<const uint8_t>{}).error, Error::length);
    EXPECT_EQ(b.writes + b.head_stores, 0U);
}
TEST(SharedRing, RandomizedProducerConsumerMatchesIndependentFifo) {
    Backend b(256);
    std::mt19937 random(0x20514);
    std::deque<std::vector<uint8_t>> expected;
    for (size_t step = 0; step < 20000; ++step) {
        if (random() % 2) {
            std::vector<uint8_t> payload(1 + random() % 200);
            for (auto& x : payload) x = random();
            auto result = write(b, 256, payload);
            ASSERT_TRUE(result.error == Error::none || result.error == Error::full);
            if (result.error == Error::none) expected.push_back(payload);
        } else {
            std::array<uint8_t, 256> output{};
            auto result = read(b, 256, output);
            if (expected.empty())
                ASSERT_EQ(result.error, Error::empty);
            else {
                ASSERT_EQ(result.error, Error::none);
                ASSERT_EQ(result.bytes, expected.front().size());
                ASSERT_TRUE(std::equal(expected.front().begin(), expected.front().end(), output.begin()));
                expected.pop_front();
            }
        }
    }
}
struct Sparse {
    static constexpr uint32_t CAPACITY = UINT32_MAX - 3;
    uint32_t h = CAPACITY - 4, t = 8;
    std::array<uint8_t, 4> end{};
    auto head() -> uint32_t { return h; }
    auto tail() -> uint32_t { return t; }
    auto load(uint32_t pos) -> uint8_t {
        EXPECT_GE(pos, CAPACITY - 4);
        EXPECT_LT(pos, CAPACITY);
        return end.at(pos - (CAPACITY - 4));
    }
    void store(uint32_t pos, uint8_t value) {
        EXPECT_GE(pos, CAPACITY - 4);
        EXPECT_LT(pos, CAPACITY);
        end.at(pos - (CAPACITY - 4)) = value;
    }
    void publish_head(uint32_t value) { h = value; }
    void publish_tail(uint32_t value) { t = value; }
};
TEST(SharedRing, LastUint32PositionsUseWideArithmetic) {
    Sparse b;
    std::array<uint8_t, 1> payload = {0x39}, output{};
    ASSERT_EQ(write(b, Sparse::CAPACITY, payload).error, Error::none);
    EXPECT_EQ(b.h, 0U);
    b.t = Sparse::CAPACITY - 4;
    ASSERT_EQ(read(b, Sparse::CAPACITY, output).error, Error::none);
    EXPECT_EQ(b.t, 0U);
    EXPECT_EQ(output, payload);
}

TEST(SharedRing, AcquireReleasePublicationSupportsConcurrentProducerAndConsumer) {
    struct Concurrent {
        std::array<uint8_t, 256> data{};
        std::atomic<uint32_t> h{0}, t{0};
        auto head() -> uint32_t { return h.load(std::memory_order_acquire); }
        auto tail() -> uint32_t { return t.load(std::memory_order_acquire); }
        auto load(uint32_t pos) -> uint8_t { return data.at(pos); }
        void store(uint32_t pos, uint8_t value) { data.at(pos) = value; }
        void publish_head(uint32_t value) { h.store(value, std::memory_order_release); }
        void publish_tail(uint32_t value) { t.store(value, std::memory_order_release); }
    } b;
    std::atomic<bool> stop{false};
    std::atomic<uint32_t> produced{0}, consumed{0};
    std::thread producer([&] {
        for (uint32_t sequence = 0; sequence < 10000 && !stop.load(); ++sequence) {
            std::array<uint8_t, 128> data{};
            auto size = 4 + sequence % 124;
            for (size_t i = 0; i < size; ++i) data[i] = static_cast<uint8_t>(sequence * 13 + i);
            for (size_t i = 0; i < 4; ++i) data[i] = static_cast<uint8_t>(sequence >> (i * 8));
            while (!stop.load()) {
                auto result = write(b, 256, std::span(data).first(size));
                if (result.error == Error::none) {
                    ++produced;
                    break;
                }
                if (result.error != Error::full) {
                    ADD_FAILURE() << "unexpected producer refusal";
                    stop = true;
                    break;
                }
                std::this_thread::yield();
            }
        }
    });
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    for (uint32_t sequence = 0; sequence < 10000 && !stop.load();) {
        if (std::chrono::steady_clock::now() > deadline) {
            ADD_FAILURE() << "concurrent ring deadline";
            break;
        }
        std::array<uint8_t, 128> data{};
        auto result = read(b, 256, data);
        if (result.error == Error::empty) {
            std::this_thread::yield();
            continue;
        }
        if (result.error != Error::none) {
            ADD_FAILURE() << "unexpected consumer refusal";
            break;
        }
        EXPECT_EQ(result.bytes, 4 + sequence % 124);
        for (size_t i = 0; i < result.bytes; ++i) EXPECT_EQ(data[i], static_cast<uint8_t>(i < 4 ? sequence >> (i * 8) : sequence * 13 + i));
        ++sequence;
        ++consumed;
    }
    stop = true;
    producer.join();
    EXPECT_EQ(produced.load(), 10000U);
    EXPECT_EQ(consumed.load(), 10000U);
    EXPECT_EQ(b.head(), b.tail());
}

TEST(SharedRing, BoundViewsShareRealIndicesAndRejectInvalidExtents) {
    alignas(4) std::array<uint32_t, 64> storage{};
    auto bytes = std::span(reinterpret_cast<uint8_t*>(storage.data()), sizeof(storage));
    auto writer = bind(bytes, {64, 64}), reader = bind(bytes, {64, 64});
    ASSERT_TRUE(writer);
    ASSERT_TRUE(reader);
    std::array<uint8_t, 37> data{}, output{};
    data.fill(0x57);
    ASSERT_EQ(write(*writer, writer->size, data).error, Error::none);
    EXPECT_NE(reader->head(), reader->tail());
    EXPECT_EQ(read(*reader, reader->size, output).error, Error::none);
    EXPECT_EQ(output, data);
    EXPECT_EQ(writer->head(), writer->tail());
    EXPECT_FALSE(bind(bytes, {65, 64}));
    EXPECT_FALSE(bind(bytes, {252, 64}));
    EXPECT_FALSE(bind(bytes, {64, 8}));
    EXPECT_FALSE(bind(bytes.subspan(1), {64, 64}));
}
TEST(SharedRing, LayoutSnapshotRetainsGeometryAndRequiresVersionAndMagic) {
    std::array<uint32_t, 64> words{};
    words[0] = 0x574b4944;
    words[1] = 1;
    words[2] = 64;
    words[3] = 64;
    words[4] = 128;
    words[5] = 64;
    words[6] = 192;
    words[7] = 64;
    auto bytes = std::span(reinterpret_cast<uint8_t*>(words.data()), sizeof(words));
    auto original = snapshot(bytes, words[0], true);
    ASSERT_TRUE(original);
    words[2] = UINT32_MAX;
    EXPECT_FALSE(snapshot(bytes, words[0], true));
    EXPECT_EQ(original->rings[0].offset, 64U);
    words[2] = 64;
    words[1] = 2;
    EXPECT_FALSE(snapshot(bytes, words[0], true));
    words[1] = 1;
    EXPECT_FALSE(snapshot(bytes, 0x12345678, true));
    words[6] = 128;
    EXPECT_FALSE(snapshot(bytes, words[0], true));
}
TEST(SharedRing, HeaderClaimHasOneInitializerAndNeverResetsPublishedHeader) {
    uint32_t magic = 0, initializer = 0;
    constexpr uint32_t EXPECTED = 0x574b4944;
    std::atomic<unsigned> initialized{0};
    std::array<std::thread, 8> peers;
    for (auto& peer : peers)
        peer = std::thread([&] {
            if (claim(&magic, &initializer, EXPECTED, -1).state == ClaimState::initialize) ++initialized;
        });
    for (auto& peer : peers) peer.join();
    EXPECT_EQ(initialized.load(), 1U);
    EXPECT_EQ(load_word(&magic), INITIALIZING);
    EXPECT_EQ(claim(&magic, &initializer, EXPECTED, 0).state, ClaimState::wait);
    store_word(&magic, EXPECTED);
    EXPECT_EQ(claim(&magic, &initializer, EXPECTED, 0).state, ClaimState::attach);
    EXPECT_EQ(load_word(&magic), EXPECTED);
    EXPECT_EQ(claim(&magic, &initializer, EXPECTED, -1).role, 1U);
    EXPECT_EQ(claim(&magic, &initializer, EXPECTED, 0).role, 0U);
    EXPECT_EQ(claim(&magic, &initializer, EXPECTED, 2).state, ClaimState::refused);
    EXPECT_EQ(claim(&magic, &initializer, 0x12345678, 0).state, ClaimState::refused);
    EXPECT_EQ(load_word(&magic), EXPECTED);
}
TEST(SharedRing, ConfiguredRoleOneCanInitializeBeforeLateExplicitOrAutomaticPeer) {
    uint32_t magic = 0, initializer = 99;
    constexpr uint32_t EXPECTED = 0x574b4944;
    auto first = claim(&magic, &initializer, EXPECTED, 1);
    ASSERT_EQ(first.state, ClaimState::initialize);
    ASSERT_EQ(first.role, 1U);
    EXPECT_EQ(claim(&magic, &initializer, EXPECTED, -1).state, ClaimState::wait);
    store_word(&initializer, first.role);
    store_word(&magic, EXPECTED);
    for (int32_t role : {-1, 0}) {
        auto late = claim(&magic, &initializer, EXPECTED, role);
        EXPECT_EQ(late.state, ClaimState::attach);
        EXPECT_EQ(late.role, 0U);
    }
    EXPECT_EQ(load_word(&magic), EXPECTED);
    EXPECT_EQ(load_word(&initializer), 1U);
    store_word(&initializer, 2);
    EXPECT_EQ(claim(&magic, &initializer, EXPECTED, -1).state, ClaimState::refused);
}
TEST(SharedRing, ConfiguredRacingPeersPreserveWinningRole) {
    constexpr uint32_t EXPECTED = 0x574b4944;
    for (unsigned round = 0; round < 100; ++round) {
        uint32_t magic = 0, initializer = 99;
        std::array<Claim, 2> claims;
        std::array<std::thread, 2> peers;
        for (int32_t role = 0; role < 2; ++role)
            peers[role] = std::thread([&, role] {
                claims[role] = claim(&magic, &initializer, EXPECTED, role);
                if (claims[role].state == ClaimState::initialize) {
                    store_word(&initializer, claims[role].role);
                    store_word(&magic, EXPECTED);
                }
            });
        for (auto& peer : peers) peer.join();
        unsigned winners = 0;
        for (uint32_t role = 0; role < 2; ++role) {
            if (claims[role].state == ClaimState::initialize) {
                ++winners;
                EXPECT_EQ(claims[role].role, role);
                EXPECT_EQ(load_word(&initializer), role);
            }
            EXPECT_EQ(claim(&magic, &initializer, EXPECTED, role).role, role);
        }
        EXPECT_EQ(winners, 1U);
        EXPECT_EQ(claim(&magic, &initializer, EXPECTED, -1).role, 1U - load_word(&initializer));
    }
}
TEST(SharedRing, RawRdmaExtentAndPageRoundingCannotWrapOrAdmitPartialPages) {
    EXPECT_EQ(remote_extent(16384, 4096, 4096, 8192), uint64_t{8192});
    EXPECT_FALSE(remote_extent(16384, 4096, UINT64_MAX - 4095, 1));
    EXPECT_FALSE(remote_extent(16384, 4096, 4096, 8193));
    EXPECT_FALSE(remote_extent(16384, 0, 0, 1));
    EXPECT_FALSE(remote_extent(16384, 16384, 0, 0));
    EXPECT_EQ(remote_extent(16384, 4096, 12288, 0), uint64_t{16384});
    EXPECT_FALSE(rounded_pages(0, 16384));
    EXPECT_FALSE(rounded_pages(UINT32_MAX, 16384));
    EXPECT_EQ(rounded_pages(4097, 8192), 2U);
    EXPECT_FALSE(rounded_pages(4097, 8191));
    EXPECT_EQ(rounded_pages(UINT32_MAX, UINT64_MAX), 1048576U);
}
