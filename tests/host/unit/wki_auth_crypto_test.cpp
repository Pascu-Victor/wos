#include <gtest/gtest.h>

#include <array>
#include <cstddef>
#include <cstdint>
#include <net/wki/auth_crypto.hpp>
#include <string_view>

namespace auth_crypto = ker::net::wki::auth_crypto;

namespace {

constexpr auto nybble(char value) -> uint8_t {
    if (value >= '0' && value <= '9') {
        return static_cast<uint8_t>(value - '0');
    }
    return static_cast<uint8_t>(value - 'a' + 10);
}

template <size_t Size>
constexpr auto from_hex(std::string_view value) -> std::array<uint8_t, Size> {
    std::array<uint8_t, Size> result{};
    for (size_t index = 0; index < Size; ++index) {
        result.at(index) = static_cast<uint8_t>((nybble(value.at(index * 2)) << 4U) | nybble(value.at(index * 2 + 1)));
    }
    return result;
}

}  // namespace

TEST(WkiAuthCrypto, Sha256KnownAnswerAndStreaming) {
    constexpr std::string_view MESSAGE = "abc";
    constexpr auto EXPECTED = from_hex<32>(
        "ba7816bf8f01cfea414140de5dae2223"
        "b00361a396177a9cb410ff61f20015ad");

    EXPECT_EQ(auth_crypto::sha256(MESSAGE.data(), MESSAGE.size()), EXPECTED);

    auth_crypto::Sha256 streaming;
    for (char character : MESSAGE) {
        streaming.update(&character, 1);
    }
    EXPECT_EQ(streaming.finish(), EXPECTED);
}

TEST(WkiAuthCrypto, HmacSha256Rfc4231CaseOne) {
    constexpr std::array<uint8_t, 20> KEY = {0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b,
                                             0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b};
    constexpr std::string_view MESSAGE = "Hi There";
    constexpr auto EXPECTED = from_hex<32>(
        "b0344c61d8db38535ca8afceaf0bf12b"
        "881dc200c9833da726e9376c2e32cff7");

    EXPECT_EQ(auth_crypto::hmac_sha256(KEY.data(), KEY.size(), MESSAGE.data(), MESSAGE.size()), EXPECTED);
}

TEST(WkiAuthCrypto, HkdfSha256Rfc5869CaseOne) {
    std::array<uint8_t, 22> input_key{};
    input_key.fill(0x0b);
    constexpr auto SALT = from_hex<13>("000102030405060708090a0b0c");
    constexpr auto INFO = from_hex<10>("f0f1f2f3f4f5f6f7f8f9");
    constexpr auto EXPECTED_PRK = from_hex<32>(
        "077709362c2e32df0ddc3f0dc47bba63"
        "90b6c73bb50f9c3122ec844ad7c2b3e5");
    constexpr auto EXPECTED_OKM = from_hex<42>(
        "3cb25f25faacd57a90434f64d0362f2a"
        "2d2d0a90cf1a5a4c5db02d56ecc4c5bf"
        "34007208d5b887185865");

    auto const PRK = auth_crypto::hkdf_extract(SALT.data(), SALT.size(), input_key.data(), input_key.size());
    EXPECT_EQ(PRK, EXPECTED_PRK);

    std::array<uint8_t, EXPECTED_OKM.size()> output{};
    ASSERT_TRUE(auth_crypto::hkdf_expand(PRK, INFO.data(), INFO.size(), output.data(), output.size()));
    EXPECT_EQ(output, EXPECTED_OKM);
}

TEST(WkiAuthCrypto, ConstantTimeComparisonHandlesMismatchAndEmptyInputs) {
    constexpr std::array<uint8_t, 4> LEFT = {1, 2, 3, 4};
    constexpr std::array<uint8_t, 4> RIGHT = {1, 2, 3, 5};

    EXPECT_TRUE(auth_crypto::constant_time_equal(LEFT.data(), LEFT.data(), LEFT.size()));
    EXPECT_FALSE(auth_crypto::constant_time_equal(LEFT.data(), RIGHT.data(), LEFT.size()));
    EXPECT_TRUE(auth_crypto::constant_time_equal(nullptr, nullptr, 0));
    EXPECT_FALSE(auth_crypto::constant_time_equal(nullptr, RIGHT.data(), RIGHT.size()));
}
