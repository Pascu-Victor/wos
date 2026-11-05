#include <gtest/gtest.h>

#include <array>
#include <atomic>
#include <barrier>
#include <chrono>
#include <dev/pci_boot_resource.hpp>
#include <latch>
#include <thread>
#include <type_traits>

namespace {
namespace resource = ker::dev::pci::resource;
using resource::BootResource;
using resource::BootState;

struct Mapping {
    uint64_t address = 0;
    uint64_t bytes = 0;
    std::array<uint64_t, 8> words{};
};

static_assert(!std::is_copy_constructible_v<BootResource<Mapping>>);
static_assert(!std::is_copy_assignable_v<BootResource<Mapping>>);
static_assert(!std::is_move_constructible_v<BootResource<Mapping>>);
static_assert(!std::is_move_assignable_v<BootResource<Mapping>>);
static_assert(std::is_same_v<decltype(std::declval<const BootResource<Mapping>&>().get()), const Mapping*>);

TEST(PciBootResource, EmptyStorageNeverExposesItsDefaultValue) {
    const BootResource<Mapping> cache;
    EXPECT_EQ(cache.state(), BootState::empty);
    EXPECT_EQ(cache.get(), nullptr);
}

TEST(PciBootResource, PublishesAnOwnedImmutableValueAndRetainsItsAddress) {
    BootResource<Mapping> cache;
    Mapping source{0x100000000, 0x20000000, {1, 2, 3, 4, 5, 6, 7, 8}};
    unsigned calls = 0;
    auto produce = [&]() -> std::optional<Mapping> {
        ++calls;
        EXPECT_EQ(cache.state(), BootState::preparing);
        EXPECT_EQ(cache.get(), nullptr);
        return source;
    };
    auto const* retained = cache.prepare(produce);
    ASSERT_NE(retained, nullptr);
    EXPECT_EQ(cache.state(), BootState::ready);
    EXPECT_NE(retained, &source);
    source = {};
    for (unsigned n = 0; n < 1024; ++n) {
        EXPECT_EQ(cache.get(), retained);
        EXPECT_EQ(cache.prepare(produce), retained);
        EXPECT_EQ(retained->address, 0x100000000ULL);
        EXPECT_EQ(retained->bytes, 0x20000000U);
        EXPECT_EQ(retained->words[7], 8U);
    }
    EXPECT_EQ(calls, 1U);
}

TEST(PciBootResource, ZeroValuedSuccessIsDistinctFromTerminalFailure) {
    BootResource<uint64_t> present;
    auto const* value = present.prepare([]() -> std::optional<uint64_t> { return 0; });
    ASSERT_NE(value, nullptr);
    EXPECT_EQ(*value, 0U);
    EXPECT_EQ(present.state(), BootState::ready);

    BootResource<uint64_t> failed;
    unsigned calls = 0;
    EXPECT_EQ(failed.prepare([&]() -> std::optional<uint64_t> {
        ++calls;
        return std::nullopt;
    }),
              nullptr);
    for (unsigned n = 0; n < 1024; ++n) {
        EXPECT_EQ(failed.state(), BootState::failed);
        EXPECT_EQ(failed.get(), nullptr);
        EXPECT_EQ(failed.prepare([&]() -> std::optional<uint64_t> {
            ++calls;
            return 42;
        }),
                  nullptr);
    }
    EXPECT_EQ(calls, 1U);
}

TEST(PciBootResource, ReentrantPreparationNeitherWaitsNorInvokesAnotherProducer) {
    for (bool succeed : {false, true}) {
        BootResource<Mapping> cache;
        unsigned nested_calls = 0;
        auto const* result = cache.prepare([&]() -> std::optional<Mapping> {
            EXPECT_EQ(cache.state(), BootState::preparing);
            EXPECT_EQ(cache.get(), nullptr);
            EXPECT_EQ(cache.prepare([&]() -> std::optional<Mapping> {
                ++nested_calls;
                return Mapping{};
            }),
                      nullptr);
            EXPECT_EQ(cache.state(), BootState::preparing);
            return succeed ? std::optional<Mapping>{Mapping{0x1000, 4096, {}}} : std::nullopt;
        });
        EXPECT_EQ(nested_calls, 0U);
        EXPECT_EQ(result != nullptr, succeed);
        EXPECT_EQ(cache.state(), succeed ? BootState::ready : BootState::failed);
    }
}

TEST(PciBootResource, ConcurrentCallersReturnBusyUntilSuccessOrFailureIsFullyPublished) {
    for (bool succeed : {false, true}) {
        BootResource<Mapping> cache;
        std::latch entered(1), release(1);
        std::atomic<unsigned> unexpected{0};
        const Mapping* produced = nullptr;
        std::thread producer([&] {
            produced = cache.prepare([&]() -> std::optional<Mapping> {
                entered.count_down();
                release.wait();
                return succeed ? std::optional<Mapping>{Mapping{0x2000, 8192, {1, 2, 3}}} : std::nullopt;
            });
        });
        entered.wait();
        EXPECT_EQ(cache.state(), BootState::preparing);
        std::array<std::thread, 8> competitors;
        for (auto& thread : competitors) {
            thread = std::thread([&] {
                for (unsigned n = 0; n < 128; ++n) {
                    if (cache.get() != nullptr) ++unexpected;
                    if (cache.prepare([&]() -> std::optional<Mapping> {
                            ++unexpected;
                            return Mapping{};
                        }) != nullptr)
                        ++unexpected;
                }
            });
        }
        for (auto& thread : competitors) thread.join();
        EXPECT_EQ(unexpected.load(), 0U);
        release.count_down();
        producer.join();
        EXPECT_EQ(cache.state(), succeed ? BootState::ready : BootState::failed);
        EXPECT_EQ(cache.get(), produced);
        EXPECT_EQ(produced != nullptr, succeed);
        EXPECT_EQ(cache.prepare([&]() -> std::optional<Mapping> {
            ++unexpected;
            return Mapping{};
        }),
                  produced);
        EXPECT_EQ(unexpected.load(), 0U);
        if (produced != nullptr) {
            EXPECT_EQ(produced->address, 0x2000U);
            EXPECT_EQ(produced->words[2], 3U);
        }
    }
}

TEST(PciBootResource, EmptyStateRaceInvokesExactlyOneProducer) {
    BootResource<Mapping> cache;
    std::barrier start(16);
    std::atomic<unsigned> producers{0};
    std::array<const Mapping*, 16> results{};
    std::array<std::thread, 16> threads;
    for (size_t i = 0; i < threads.size(); ++i) {
        threads[i] = std::thread([&, i] {
            start.arrive_and_wait();
            results[i] = cache.prepare([&]() -> std::optional<Mapping> {
                ++producers;
                return Mapping{0x4000, 16384, {}};
            });
        });
    }
    for (auto& thread : threads) thread.join();
    auto const* retained = cache.get();
    ASSERT_NE(retained, nullptr);
    EXPECT_EQ(producers.load(), 1U);
    for (auto const* result : results) EXPECT_TRUE(result == nullptr || result == retained);
    EXPECT_EQ(retained->address, 0x4000U);
}

TEST(PciBootResource, AcquireReadersObserveEveryInitializedFieldWithoutAnotherPublicationChannel) {
    BootResource<Mapping> cache;
    std::barrier start(9);
    std::atomic<unsigned> wrong{0};
    std::array<std::thread, 8> readers;
    for (auto& reader : readers) {
        reader = std::thread([&] {
            start.arrive_and_wait();
            auto const deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
            const Mapping* value = nullptr;
            while ((value = cache.get()) == nullptr && std::chrono::steady_clock::now() < deadline) std::this_thread::yield();
            if (value == nullptr || value->address != 0x800000000ULL || value->bytes != 0x80000000U ||
                value->words != std::array<uint64_t, 8>{1, 2, 3, 4, 5, 6, 7, 8})
                ++wrong;
        });
    }
    start.arrive_and_wait();
    EXPECT_NE(cache.prepare([]() -> std::optional<Mapping> { return Mapping{0x800000000, 0x80000000, {1, 2, 3, 4, 5, 6, 7, 8}}; }),
              nullptr);
    for (auto& reader : readers) reader.join();
    EXPECT_EQ(wrong.load(), 0U);
}

TEST(PciBootResource, RetainedReadAndPrepareNeverRepeatProbeOrMappingWork) {
    BootResource<Mapping> cache;
    std::atomic<unsigned> probes{0}, maps{0};
    auto produce = [&]() -> std::optional<Mapping> {
        ++probes;
        ++maps;
        return Mapping{0x80000000, 4096, {}};
    };
    auto const* retained = cache.prepare(produce);
    ASSERT_NE(retained, nullptr);
    std::array<std::thread, 8> consumers;
    for (auto& thread : consumers) {
        thread = std::thread([&] {
            for (unsigned n = 0; n < 1024; ++n) {
                EXPECT_EQ(cache.get(), retained);
                EXPECT_EQ(cache.prepare(produce), retained);
            }
        });
    }
    for (auto& thread : consumers) thread.join();
    EXPECT_EQ(probes.load(), 1U);
    EXPECT_EQ(maps.load(), 1U);
}

}  // namespace
