#include <array>
#include <cstring>
#include <dev/ivshmem/shared_ring.hpp>
#include <memory>
#include <net/packet.hpp>
#include <new>
#include <platform/sched/scheduler.hpp>
#include <test/ktest.hpp>

namespace {
namespace ring = ker::dev::ivshmem::shared_ring;
}

KTEST(IvshmemRing, SharedViewsPreserveJumboPacketsAndRejectPartialRecords) {
    auto const HOLD = ker::mod::sched::hold_kernel_shutdown();
    std::unique_ptr<std::array<uint32_t, 8192>> storage(new (std::nothrow) std::array<uint32_t, 8192>{});
    KREQUIRE_TRUE(storage != nullptr);
    auto const BEFORE = ker::net::pkt_pool_free_count();
    auto* sent = ker::net::pkt_alloc();
    auto* received = ker::net::pkt_alloc();
    if (sent == nullptr || received == nullptr) {
        ker::net::pkt_free(sent);
        ker::net::pkt_free(received);
        KREQUIRE_TRUE(false);
    }
    auto shared = std::span(reinterpret_cast<uint8_t*>(storage->data()), sizeof(*storage));
    constexpr uint32_t MAGIC = 0x574f534e;
    auto* words = storage->data();
    auto const CLAIM = ring::claim(words, words + 6, MAGIC, 1);
    KEXPECT_EQ(CLAIM.state, ring::ClaimState::initialize);
    KEXPECT_EQ(CLAIM.role, uint32_t{1});
    words[6] = CLAIM.role;
    words[1] = 1;
    words[2] = 64;
    words[3] = 16320;
    words[4] = 16384;
    words[5] = 16384;
    ring::store_word(words, MAGIC);
    KEXPECT_EQ(ring::claim(words, words + 6, MAGIC, -1).role, uint32_t{0});
    auto const LAYOUT = ring::snapshot(shared, MAGIC, false);
    KEXPECT_TRUE(LAYOUT.has_value());
    auto writer = ring::bind(shared, {64, 16320});
    auto reader = ring::bind(shared, {64, 16320});
    KEXPECT_TRUE(writer.has_value());
    KEXPECT_TRUE(reader.has_value());
    if (writer && reader) {
        for (size_t i = 0; i < sent->tailroom(); ++i) sent->data[i] = static_cast<uint8_t>(i * 31U + 7);
        for (size_t length : {1U, 64U, 1514U, 8193U, 9014U}) {
            ring::store_word(writer->head_ptr, writer->size - 4);
            ring::store_word(writer->tail_ptr, writer->size - 4);
            memset(received->data, 0xcc, received->tailroom());
            KEXPECT_EQ(ring::write(*writer, writer->size, std::span(sent->data, length)).error, ring::Error::none);
            auto const RESULT = ring::read(*reader, reader->size, std::span(received->data, received->tailroom()));
            KEXPECT_EQ(RESULT.error, ring::Error::none);
            KEXPECT_EQ(RESULT.bytes, length);
            KEXPECT_EQ(memcmp(sent->data, received->data, length), 0);
            KEXPECT_EQ(received->data[length], uint8_t{0xcc});
            KEXPECT_EQ(writer->head(), writer->tail());
        }
        ring::store_word(writer->head_ptr, 4);
        ring::store_word(writer->tail_ptr, 0);
        writer->data[0] = 250;
        writer->data[1] = 0;
        memset(received->data, 0xcc, received->tailroom());
        KEXPECT_EQ(ring::read(*reader, reader->size, std::span(received->data, received->tailroom())).error, ring::Error::incomplete);
        KEXPECT_EQ(reader->tail(), uint32_t{0});
        KEXPECT_EQ(received->data[0], uint8_t{0xcc});
        words[2] = UINT32_MAX;
        KEXPECT_FALSE(ring::snapshot(shared, MAGIC, false).has_value());
        KEXPECT_EQ(reader->size, uint32_t{16312});
    }
    ker::net::pkt_free(sent);
    ker::net::pkt_free(received);
    KEXPECT_EQ(ker::net::pkt_pool_free_count(), BEFORE);
}
