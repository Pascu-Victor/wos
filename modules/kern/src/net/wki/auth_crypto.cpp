#include "auth_crypto.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>

namespace ker::net::wki::auth_crypto {

namespace {

constexpr std::array<uint32_t, 64> ROUND_CONSTANTS = {
    0x428a2f98U, 0x71374491U, 0xb5c0fbcfU, 0xe9b5dba5U, 0x3956c25bU, 0x59f111f1U, 0x923f82a4U, 0xab1c5ed5U, 0xd807aa98U, 0x12835b01U,
    0x243185beU, 0x550c7dc3U, 0x72be5d74U, 0x80deb1feU, 0x9bdc06a7U, 0xc19bf174U, 0xe49b69c1U, 0xefbe4786U, 0x0fc19dc6U, 0x240ca1ccU,
    0x2de92c6fU, 0x4a7484aaU, 0x5cb0a9dcU, 0x76f988daU, 0x983e5152U, 0xa831c66dU, 0xb00327c8U, 0xbf597fc7U, 0xc6e00bf3U, 0xd5a79147U,
    0x06ca6351U, 0x14292967U, 0x27b70a85U, 0x2e1b2138U, 0x4d2c6dfcU, 0x53380d13U, 0x650a7354U, 0x766a0abbU, 0x81c2c92eU, 0x92722c85U,
    0xa2bfe8a1U, 0xa81a664bU, 0xc24b8b70U, 0xc76c51a3U, 0xd192e819U, 0xd6990624U, 0xf40e3585U, 0x106aa070U, 0x19a4c116U, 0x1e376c08U,
    0x2748774cU, 0x34b0bcb5U, 0x391c0cb3U, 0x4ed8aa4aU, 0x5b9cca4fU, 0x682e6ff3U, 0x748f82eeU, 0x78a5636fU, 0x84c87814U, 0x8cc70208U,
    0x90befffaU, 0xa4506cebU, 0xbef9a3f7U, 0xc67178f2U,
};

constexpr auto load_be32(const uint8_t* data) -> uint32_t {
    return (static_cast<uint32_t>(data[0]) << 24U) | (static_cast<uint32_t>(data[1]) << 16U) | (static_cast<uint32_t>(data[2]) << 8U) |
           static_cast<uint32_t>(data[3]);
}

void store_be32(uint8_t* output, uint32_t value) {
    output[0] = static_cast<uint8_t>(value >> 24U);
    output[1] = static_cast<uint8_t>(value >> 16U);
    output[2] = static_cast<uint8_t>(value >> 8U);
    output[3] = static_cast<uint8_t>(value);
}

void store_be64(uint8_t* output, uint64_t value) {
    for (size_t index = 0; index < sizeof(value); ++index) {
        output[index] = static_cast<uint8_t>(value >> ((sizeof(value) - index - 1U) * 8U));
    }
}

}  // namespace

Sha256::Sha256() : state_{0x6a09e667U, 0xbb67ae85U, 0x3c6ef372U, 0xa54ff53aU, 0x510e527fU, 0x9b05688cU, 0x1f83d9abU, 0x5be0cd19U} {}

void Sha256::transform(const uint8_t* block) {
    std::array<uint32_t, 64> words{};
    for (size_t index = 0; index < 16; ++index) {
        words.at(index) = load_be32(block + (index * sizeof(uint32_t)));
    }
    for (size_t index = 16; index < words.size(); ++index) {
        uint32_t const S0 = std::rotr(words.at(index - 15), 7) ^ std::rotr(words.at(index - 15), 18) ^ (words.at(index - 15) >> 3U);
        uint32_t const S1 = std::rotr(words.at(index - 2), 17) ^ std::rotr(words.at(index - 2), 19) ^ (words.at(index - 2) >> 10U);
        words.at(index) = words.at(index - 16) + S0 + words.at(index - 7) + S1;
    }

    uint32_t a = state_.at(0);
    uint32_t b = state_.at(1);
    uint32_t c = state_.at(2);
    uint32_t d = state_.at(3);
    uint32_t e = state_.at(4);
    uint32_t f = state_.at(5);
    uint32_t g = state_.at(6);
    uint32_t h = state_.at(7);

    for (size_t index = 0; index < words.size(); ++index) {
        uint32_t const SUM1 = std::rotr(e, 6) ^ std::rotr(e, 11) ^ std::rotr(e, 25);
        uint32_t const CHOICE = (e & f) ^ (~e & g);
        uint32_t const TEMP1 = h + SUM1 + CHOICE + ROUND_CONSTANTS.at(index) + words.at(index);
        uint32_t const SUM0 = std::rotr(a, 2) ^ std::rotr(a, 13) ^ std::rotr(a, 22);
        uint32_t const MAJORITY = (a & b) ^ (a & c) ^ (b & c);
        uint32_t const TEMP2 = SUM0 + MAJORITY;
        h = g;
        g = f;
        f = e;
        e = d + TEMP1;
        d = c;
        c = b;
        b = a;
        a = TEMP1 + TEMP2;
    }

    state_.at(0) += a;
    state_.at(1) += b;
    state_.at(2) += c;
    state_.at(3) += d;
    state_.at(4) += e;
    state_.at(5) += f;
    state_.at(6) += g;
    state_.at(7) += h;
    secure_erase(words.data(), sizeof(words));
}

void Sha256::update(const void* data, size_t length) {
    if (finished_ || length == 0) {
        return;
    }
    if (data == nullptr || length > (UINT64_MAX - total_bytes_)) {
        finished_ = true;
        return;
    }

    const auto* input = static_cast<const uint8_t*>(data);
    total_bytes_ += length;
    while (length != 0) {
        size_t const COPY = std::min(length, buffer_.size() - buffered_);
        std::memcpy(buffer_.data() + buffered_, input, COPY);
        buffered_ += COPY;
        input += COPY;
        length -= COPY;
        if (buffered_ == buffer_.size()) {
            transform(buffer_.data());
            buffered_ = 0;
        }
    }
}

auto Sha256::finish() -> Digest {
    Digest digest{};
    if (finished_) {
        return digest;
    }
    uint64_t const TOTAL_BITS = total_bytes_ * 8U;
    buffer_.at(buffered_++) = 0x80;
    if (buffered_ > 56) {
        std::fill(buffer_.begin() + static_cast<ptrdiff_t>(buffered_), buffer_.end(), 0);
        transform(buffer_.data());
        buffered_ = 0;
    }
    std::fill(buffer_.begin() + static_cast<ptrdiff_t>(buffered_), buffer_.begin() + 56, 0);
    store_be64(buffer_.data() + 56, TOTAL_BITS);
    transform(buffer_.data());
    for (size_t index = 0; index < state_.size(); ++index) {
        store_be32(digest.data() + (index * sizeof(uint32_t)), state_.at(index));
    }
    finished_ = true;
    secure_erase(buffer_.data(), buffer_.size());
    secure_erase(state_.data(), sizeof(state_));
    return digest;
}

HmacSha256::HmacSha256(const void* key, size_t key_length) {
    std::array<uint8_t, SHA256_BLOCK_SIZE> key_block{};
    if (key != nullptr && key_length > SHA256_BLOCK_SIZE) {
        Digest HASHED = sha256(key, key_length);
        std::ranges::copy(HASHED, key_block.begin());
        secure_erase(HASHED.data(), HASHED.size());
    } else if (key != nullptr && key_length != 0) {
        std::memcpy(key_block.data(), key, key_length);
    }

    std::array<uint8_t, SHA256_BLOCK_SIZE> inner_pad{};
    for (size_t index = 0; index < key_block.size(); ++index) {
        inner_pad.at(index) = key_block.at(index) ^ 0x36U;
        outer_pad_.at(index) = key_block.at(index) ^ 0x5cU;
    }
    inner_.update(inner_pad.data(), inner_pad.size());
    secure_erase(inner_pad.data(), inner_pad.size());
    secure_erase(key_block.data(), key_block.size());
}

void HmacSha256::update(const void* data, size_t length) {
    if (!finished_) {
        inner_.update(data, length);
    }
}

auto HmacSha256::finish() -> Digest {
    if (finished_) {
        return {};
    }
    Digest inner_digest = inner_.finish();
    Sha256 outer;
    outer.update(outer_pad_.data(), outer_pad_.size());
    outer.update(inner_digest.data(), inner_digest.size());
    Digest result = outer.finish();
    secure_erase(inner_digest.data(), inner_digest.size());
    secure_erase(outer_pad_.data(), outer_pad_.size());
    finished_ = true;
    return result;
}

auto sha256(const void* data, size_t length) -> Digest {
    Sha256 context;
    context.update(data, length);
    return context.finish();
}

auto hmac_sha256(const void* key, size_t key_length, const void* data, size_t length) -> Digest {
    HmacSha256 context(key, key_length);
    context.update(data, length);
    return context.finish();
}

auto hkdf_extract(const void* salt, size_t salt_length, const void* input_key, size_t input_key_length) -> Digest {
    std::array<uint8_t, SHA256_DIGEST_SIZE> zero_salt{};
    if (salt == nullptr || salt_length == 0) {
        return hmac_sha256(zero_salt.data(), zero_salt.size(), input_key, input_key_length);
    }
    return hmac_sha256(salt, salt_length, input_key, input_key_length);
}

auto hkdf_expand(const Digest& pseudorandom_key, const void* info, size_t info_length, void* output, size_t output_length) -> bool {
    if ((output_length != 0 && output == nullptr) || (info_length != 0 && info == nullptr) || output_length > 255U * SHA256_DIGEST_SIZE) {
        return false;
    }
    auto* out = static_cast<uint8_t*>(output);
    Digest previous{};
    size_t previous_length = 0;
    size_t written = 0;
    uint8_t counter = 1;
    while (written < output_length) {
        HmacSha256 hmac(pseudorandom_key.data(), pseudorandom_key.size());
        hmac.update(previous.data(), previous_length);
        hmac.update(info, info_length);
        hmac.update(&counter, sizeof(counter));
        previous = hmac.finish();
        previous_length = previous.size();
        size_t const COPY = std::min(output_length - written, previous.size());
        std::memcpy(out + written, previous.data(), COPY);
        written += COPY;
        if (counter == UINT8_MAX && written != output_length) {
            secure_erase(previous.data(), previous.size());
            return false;
        }
        ++counter;
    }
    secure_erase(previous.data(), previous.size());
    return true;
}

auto constant_time_equal(const void* lhs, const void* rhs, size_t length) -> bool {
    if ((lhs == nullptr || rhs == nullptr) && length != 0) {
        return false;
    }
    const auto* left = static_cast<const uint8_t*>(lhs);
    const auto* right = static_cast<const uint8_t*>(rhs);
    uint8_t difference = 0;
    for (size_t index = 0; index < length; ++index) {
        difference |= left[index] ^ right[index];
    }
    return difference == 0;
}

void secure_erase(void* data, size_t length) {
    auto* bytes = static_cast<volatile uint8_t*>(data);
    while (length-- != 0) {
        *bytes++ = 0;
    }
}

}  // namespace ker::net::wki::auth_crypto
