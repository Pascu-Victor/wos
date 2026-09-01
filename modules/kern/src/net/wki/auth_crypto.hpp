#pragma once

#include <array>
#include <cstddef>
#include <cstdint>

namespace ker::net::wki::auth_crypto {

constexpr size_t SHA256_BLOCK_SIZE = 64;
constexpr size_t SHA256_DIGEST_SIZE = 32;

using Digest = std::array<uint8_t, SHA256_DIGEST_SIZE>;

class Sha256 final {
   public:
    Sha256();

    void update(const void* data, size_t length);
    auto finish() -> Digest;

   private:
    void transform(const uint8_t* block);

    std::array<uint32_t, 8> state_{};
    std::array<uint8_t, SHA256_BLOCK_SIZE> buffer_{};
    uint64_t total_bytes_ = 0;
    size_t buffered_ = 0;
    bool finished_ = false;
};

class HmacSha256 final {
   public:
    HmacSha256(const void* key, size_t key_length);

    void update(const void* data, size_t length);
    auto finish() -> Digest;

   private:
    Sha256 inner_{};
    std::array<uint8_t, SHA256_BLOCK_SIZE> outer_pad_{};
    bool finished_ = false;
};

auto sha256(const void* data, size_t length) -> Digest;
auto hmac_sha256(const void* key, size_t key_length, const void* data, size_t length) -> Digest;
auto hkdf_extract(const void* salt, size_t salt_length, const void* input_key, size_t input_key_length) -> Digest;
auto hkdf_expand(const Digest& pseudorandom_key, const void* info, size_t info_length, void* output, size_t output_length) -> bool;

auto constant_time_equal(const void* lhs, const void* rhs, size_t length) -> bool;
void secure_erase(void* data, size_t length);

}  // namespace ker::net::wki::auth_crypto
