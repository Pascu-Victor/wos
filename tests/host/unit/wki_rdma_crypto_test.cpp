#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <limits>
#include <net/wki/rdma_crypto.hpp>
#include <string_view>
#include <vector>

namespace crypto = ker::net::wki::rdma_crypto;

namespace {

constexpr auto nybble(char value) -> uint8_t { return static_cast<uint8_t>(value >= '0' && value <= '9' ? value - '0' : value - 'a' + 10); }

template <size_t Size>
constexpr auto from_hex(std::string_view value) -> std::array<uint8_t, Size> {
    std::array<uint8_t, Size> result{};
    for (size_t index = 0; index < Size; ++index) {
        result[index] = static_cast<uint8_t>((nybble(value[index * 2]) << 4U) | nybble(value[index * 2 + 1]));
    }
    return result;
}

constexpr crypto::Key KEY = from_hex<32>("808182838485868788898a8b8c8d8e8f909192939495969798999a9b9c9d9e9f");
constexpr crypto::Nonce NONCE = from_hex<12>("070000004041424344454647");
constexpr auto AAD = from_hex<12>("50515253c0c1c2c3c4c5c6c7");
constexpr std::string_view PLAINTEXT =
    "Ladies and Gentlemen of the class of '99: If I could offer you only one tip for the future, sunscreen would be it.";
constexpr auto CIPHERTEXT = from_hex<114>(
    "d31a8d34648e60db7b86afbc53ef7ec2a4aded51296e08fea9e2b5a736ee62d6"
    "3dbea45e8ca9671282fafb69da92728b1a71de0a9e060b2905d6a5b67ecd3b36"
    "92ddbd7f2d778b8c9803aee328091b58fab324e4fad675945585808b4831d7bc"
    "3ff4def08e4b7a9de576d26586cec64b6116");
constexpr crypto::Tag TAG = from_hex<16>("1ae10b594f09e26a7e902ecbd0600691");

void expect_failed_open(const crypto::Key& key, const crypto::Nonce& nonce, const void* aad, size_t aad_length, const void* input,
                        size_t length, const crypto::Tag& tag) {
    std::array<uint8_t, CIPHERTEXT.size()> output{};
    output.fill(0xa5);
    auto const BEFORE = output;
    EXPECT_FALSE(crypto::open(key, nonce, aad, aad_length, input, length, output.data(), tag));
    EXPECT_EQ(output, BEFORE);
}

}  // namespace

// https://www.rfc-editor.org/rfc/rfc8439#section-2.8.2
TEST(WkiRdmaCrypto, Rfc8439AeadSealAndOpen) {
    std::array<uint8_t, PLAINTEXT.size()> output{};
    crypto::Tag tag{};
    ASSERT_TRUE(crypto::seal(KEY, NONCE, AAD.data(), AAD.size(), PLAINTEXT.data(), PLAINTEXT.size(), output.data(), tag));
    EXPECT_EQ(output, CIPHERTEXT);
    EXPECT_EQ(tag, TAG);
    ASSERT_TRUE(crypto::open(KEY, NONCE, AAD.data(), AAD.size(), CIPHERTEXT.data(), CIPHERTEXT.size(), output.data(), TAG));
    EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(output.data()), output.size()), PLAINTEXT);
}

TEST(WkiRdmaCrypto, ExactInPlaceSealAndOpen) {
    std::array<uint8_t, PLAINTEXT.size()> bytes{};
    std::memcpy(bytes.data(), PLAINTEXT.data(), bytes.size());
    crypto::Tag tag{};
    ASSERT_TRUE(crypto::seal(KEY, NONCE, AAD.data(), AAD.size(), bytes.data(), bytes.size(), bytes.data(), tag));
    EXPECT_EQ(bytes, CIPHERTEXT);
    EXPECT_EQ(tag, TAG);
    ASSERT_TRUE(crypto::open(KEY, NONCE, AAD.data(), AAD.size(), bytes.data(), bytes.size(), bytes.data(), tag));
    EXPECT_EQ(std::string_view(reinterpret_cast<const char*>(bytes.data()), bytes.size()), PLAINTEXT);
}

TEST(WkiRdmaCrypto, IndependentPaddingAndBlockBoundaryVectors) {
    // Independently generated with cryptography 50.0.1 ChaCha20Poly1305.
    // Key/nonce are ascending bytes; plaintext[i]=29*i+7, AAD[i]=11*i+3.
    struct Vector {
        size_t length;
        size_t aad_length;
        crypto::Tag tag;
    };
    constexpr std::array VECTORS = {
        Vector{0, 0, from_hex<16>("295a498b8841a1c5f55d4d606f731159")},   Vector{0, 17, from_hex<16>("2d2ae0dc5a4da7ea82d91ee9191d1624")},
        Vector{1, 0, from_hex<16>("0b7a7dc78960ce023ab69d45a828778c")},   Vector{15, 1, from_hex<16>("2c923d610fc38e9deca5307c52d3a19d")},
        Vector{16, 16, from_hex<16>("f0b437ac2f43053cefc816adbe344d15")}, Vector{17, 17, from_hex<16>("65f9591809923a5377007685cd9a9bb5")},
        Vector{63, 15, from_hex<16>("e8b19714777343edd8f07c98412882fa")}, Vector{64, 16, from_hex<16>("cbdb88dd217989f1a567f50d9ab3d507")},
        Vector{65, 17, from_hex<16>("b706040906979d8476b5b834b4a020b6")}, Vector{129, 31, from_hex<16>("4afb9b81ff7e751c48c2413413e6224a")},
    };
    crypto::Key key{};
    crypto::Nonce nonce{};
    for (size_t index = 0; index < key.size(); ++index) key[index] = static_cast<uint8_t>(index);
    for (size_t index = 0; index < nonce.size(); ++index) nonce[index] = static_cast<uint8_t>(index);
    for (auto const& vector : VECTORS) {
        SCOPED_TRACE(vector.length);
        std::vector<uint8_t> plaintext(vector.length), aad(vector.aad_length), ciphertext(vector.length), recovered(vector.length);
        for (size_t index = 0; index < plaintext.size(); ++index) plaintext[index] = static_cast<uint8_t>(index * 29 + 7);
        for (size_t index = 0; index < aad.size(); ++index) aad[index] = static_cast<uint8_t>(index * 11 + 3);
        crypto::Tag tag{};
        ASSERT_TRUE(crypto::seal(key, nonce, aad.data(), aad.size(), plaintext.data(), plaintext.size(), ciphertext.data(), tag));
        EXPECT_EQ(tag, vector.tag);
        ASSERT_TRUE(crypto::open(key, nonce, aad.data(), aad.size(), ciphertext.data(), ciphertext.size(), recovered.data(), vector.tag));
        EXPECT_EQ(recovered, plaintext);
    }
}

TEST(WkiRdmaCrypto, EveryTagBitRejectsBeforeOutput) {
    for (size_t index = 0; index < TAG.size(); ++index) {
        for (unsigned bit = 0; bit < 8; ++bit) {
            auto changed = TAG;
            changed[index] ^= static_cast<uint8_t>(1U << bit);
            expect_failed_open(KEY, NONCE, AAD.data(), AAD.size(), CIPHERTEXT.data(), CIPHERTEXT.size(), changed);
            auto inplace = CIPHERTEXT;
            EXPECT_FALSE(crypto::open(KEY, NONCE, AAD.data(), AAD.size(), inplace.data(), inplace.size(), inplace.data(), changed));
            EXPECT_EQ(inplace, CIPHERTEXT);
        }
    }
}

TEST(WkiRdmaCrypto, CiphertextAndAadTamperPreserveOutput) {
    for (size_t index = 0; index < CIPHERTEXT.size(); ++index) {
        auto changed = CIPHERTEXT;
        changed[index] ^= 0x80;
        expect_failed_open(KEY, NONCE, AAD.data(), AAD.size(), changed.data(), changed.size(), TAG);
    }
    for (size_t index = 0; index < AAD.size(); ++index) {
        auto changed = AAD;
        changed[index] ^= 1;
        expect_failed_open(KEY, NONCE, changed.data(), changed.size(), CIPHERTEXT.data(), CIPHERTEXT.size(), TAG);
    }
    expect_failed_open(KEY, NONCE, AAD.data(), AAD.size() - 1, CIPHERTEXT.data(), CIPHERTEXT.size(), TAG);
    expect_failed_open(KEY, NONCE, AAD.data(), AAD.size(), CIPHERTEXT.data(), CIPHERTEXT.size() - 1, TAG);
}

TEST(WkiRdmaCrypto, KeyAndNonceTamperPreserveOutput) {
    for (size_t index = 0; index < KEY.size(); ++index) {
        auto changed = KEY;
        changed[index] ^= 1;
        expect_failed_open(changed, NONCE, AAD.data(), AAD.size(), CIPHERTEXT.data(), CIPHERTEXT.size(), TAG);
    }
    for (size_t index = 0; index < NONCE.size(); ++index) {
        auto changed = NONCE;
        changed[index] ^= 1;
        expect_failed_open(KEY, changed, AAD.data(), AAD.size(), CIPHERTEXT.data(), CIPHERTEXT.size(), TAG);
    }
}

TEST(WkiRdmaCrypto, EmptyBuffersPermitNullButStillAuthenticate) {
    crypto::Tag tag{};
    ASSERT_TRUE(crypto::seal(KEY, NONCE, nullptr, 0, nullptr, 0, nullptr, tag));
    ASSERT_TRUE(crypto::open(KEY, NONCE, nullptr, 0, nullptr, 0, nullptr, tag));
    tag[0] ^= 1;
    EXPECT_FALSE(crypto::open(KEY, NONCE, nullptr, 0, nullptr, 0, nullptr, tag));
}

TEST(WkiRdmaCrypto, NullNonemptyBuffersRejectWithoutMutation) {
    std::array<uint8_t, 32> output{};
    output.fill(0xa5);
    auto const BEFORE = output;
    crypto::Tag tag{};
    tag.fill(0x5a);
    auto const ORIGINAL_TAG = tag;
    EXPECT_FALSE(crypto::seal(KEY, NONCE, nullptr, 1, output.data(), 1, output.data(), tag));
    EXPECT_FALSE(crypto::seal(KEY, NONCE, nullptr, 0, nullptr, 1, output.data(), tag));
    EXPECT_FALSE(crypto::seal(KEY, NONCE, nullptr, 0, output.data(), 1, nullptr, tag));
    EXPECT_FALSE(crypto::open(KEY, NONCE, nullptr, 1, output.data(), 1, output.data(), tag));
    EXPECT_FALSE(crypto::open(KEY, NONCE, nullptr, 0, nullptr, 1, output.data(), tag));
    EXPECT_FALSE(crypto::open(KEY, NONCE, nullptr, 0, output.data(), 1, nullptr, tag));
    EXPECT_EQ(output, BEFORE);
    EXPECT_EQ(tag, ORIGINAL_TAG);
}

TEST(WkiRdmaCrypto, PointerAndBlockCounterOverflowRejectBeforeReads) {
    std::array<uint8_t, 8> output{};
    output.fill(0xa5);
    auto const BEFORE = output;
    crypto::Tag tag{};
    tag.fill(0x5a);
    auto const ORIGINAL_TAG = tag;
    auto* invalid = reinterpret_cast<void*>(UINTPTR_MAX - 3);
    EXPECT_FALSE(crypto::seal(KEY, NONCE, invalid, 8, output.data(), 8, output.data(), tag));
    EXPECT_FALSE(crypto::seal(KEY, NONCE, nullptr, 0, invalid, 8, output.data(), tag));
    EXPECT_FALSE(crypto::seal(KEY, NONCE, nullptr, 0, output.data(), 8, invalid, tag));
    EXPECT_FALSE(crypto::open(KEY, NONCE, invalid, 8, output.data(), 8, output.data(), tag));
    EXPECT_FALSE(crypto::open(KEY, NONCE, nullptr, 0, invalid, 8, output.data(), tag));
    EXPECT_FALSE(crypto::open(KEY, NONCE, nullptr, 0, output.data(), 8, invalid, tag));
    if constexpr (sizeof(size_t) >= sizeof(uint64_t)) {
        size_t const TOO_LONG = static_cast<size_t>(uint64_t{UINT32_MAX} * 64 + 1);
        EXPECT_FALSE(crypto::seal(KEY, NONCE, nullptr, 0, output.data(), TOO_LONG, output.data(), tag));
        EXPECT_FALSE(crypto::open(KEY, NONCE, nullptr, 0, output.data(), TOO_LONG, output.data(), tag));
    }
    EXPECT_EQ(output, BEFORE);
    EXPECT_EQ(tag, ORIGINAL_TAG);
}

TEST(WkiRdmaCrypto, PartialInputOutputOverlapRejectsInBothDirections) {
    std::array<uint8_t, 128> bytes{};
    bytes.fill(0xa5);
    auto const BEFORE = bytes;
    crypto::Tag tag{};
    tag.fill(0x5a);
    auto const ORIGINAL_TAG = tag;
    for (size_t delta : {size_t{1}, size_t{15}, size_t{63}}) {
        EXPECT_FALSE(crypto::seal(KEY, NONCE, nullptr, 0, bytes.data(), 64, bytes.data() + delta, tag));
        EXPECT_FALSE(crypto::seal(KEY, NONCE, nullptr, 0, bytes.data() + delta, 64, bytes.data(), tag));
        EXPECT_FALSE(crypto::open(KEY, NONCE, nullptr, 0, bytes.data(), 64, bytes.data() + delta, tag));
        EXPECT_FALSE(crypto::open(KEY, NONCE, nullptr, 0, bytes.data() + delta, 64, bytes.data(), tag));
    }
    EXPECT_EQ(bytes, BEFORE);
    EXPECT_EQ(tag, ORIGINAL_TAG);
}

TEST(WkiRdmaCrypto, WritableMetadataOverlapRejectsWithoutMutation) {
    auto key = KEY;
    auto nonce = NONCE;
    auto aad = AAD;
    crypto::Tag tag = TAG;
    std::array<uint8_t, 32> bytes{};
    bytes.fill(0xa5);
    auto const BEFORE = bytes;
    for (void* output : {static_cast<void*>(key.data()), static_cast<void*>(nonce.data()), static_cast<void*>(aad.data()),
                         static_cast<void*>(tag.data())}) {
        EXPECT_FALSE(crypto::seal(key, nonce, aad.data(), aad.size(), bytes.data(), 1, output, tag));
        EXPECT_FALSE(crypto::open(key, nonce, aad.data(), aad.size(), bytes.data(), 1, output, tag));
    }
    EXPECT_FALSE(crypto::seal(key, nonce, tag.data(), tag.size(), bytes.data(), 1, bytes.data(), tag));
    EXPECT_FALSE(crypto::seal(key, nonce, nullptr, 0, tag.data(), 1, bytes.data(), tag));
    EXPECT_EQ(key, KEY);
    EXPECT_EQ(nonce, NONCE);
    EXPECT_EQ(aad, AAD);
    EXPECT_EQ(tag, TAG);
    EXPECT_EQ(bytes, BEFORE);
}

TEST(WkiRdmaCrypto, UnalignedBuffersAndAdjacentNonoverlapWork) {
    std::array<uint8_t, PLAINTEXT.size() * 2 + 2> bytes{};
    std::memcpy(bytes.data() + 1, PLAINTEXT.data(), PLAINTEXT.size());
    auto* output = bytes.data() + 1 + PLAINTEXT.size();
    crypto::Tag tag{};
    ASSERT_TRUE(crypto::seal(KEY, NONCE, AAD.data(), AAD.size(), bytes.data() + 1, PLAINTEXT.size(), output, tag));
    EXPECT_EQ(std::memcmp(output, CIPHERTEXT.data(), CIPHERTEXT.size()), 0);
    EXPECT_EQ(tag, TAG);
    ASSERT_TRUE(crypto::open(KEY, NONCE, AAD.data(), AAD.size(), output, PLAINTEXT.size(), bytes.data() + 1, tag));
    EXPECT_EQ(std::memcmp(bytes.data() + 1, PLAINTEXT.data(), PLAINTEXT.size()), 0);
}
