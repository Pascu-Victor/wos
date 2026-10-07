#include "rdma_crypto.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/wki/auth_crypto.hpp>

namespace ker::net::wki::rdma_crypto {

namespace {

constexpr size_t CHACHA_BLOCK_BYTES = 64;
constexpr uint64_t MAX_PLAINTEXT_BYTES = uint64_t{UINT32_MAX} * CHACHA_BLOCK_BYTES;
constexpr uint32_t LIMB_MASK = (uint32_t{1} << 26U) - 1U;

class EraseOnExit final {
   public:
    EraseOnExit(void* data, size_t size) : data_(data), size_(size) {}
    ~EraseOnExit() { auth_crypto::secure_erase(data_, size_); }
    EraseOnExit(const EraseOnExit&) = delete;
    auto operator=(const EraseOnExit&) -> EraseOnExit& = delete;

   private:
    void* data_;
    size_t size_;
};

auto load_le32(const uint8_t* input) -> uint32_t {
    return static_cast<uint32_t>(input[0]) | (static_cast<uint32_t>(input[1]) << 8U) | (static_cast<uint32_t>(input[2]) << 16U) |
           (static_cast<uint32_t>(input[3]) << 24U);
}

void store_le32(uint8_t* output, uint32_t value) {
    for (size_t index = 0; index < 4; ++index) output[index] = static_cast<uint8_t>(value >> (index * 8U));
}

void store_le64(uint8_t* output, uint64_t value) {
    for (size_t index = 0; index < 8; ++index) output[index] = static_cast<uint8_t>(value >> (index * 8U));
}

void quarter_round(uint32_t& a, uint32_t& b, uint32_t& c, uint32_t& d) {
    a += b;
    d = std::rotl(d ^ a, 16);
    c += d;
    b = std::rotl(b ^ c, 12);
    a += b;
    d = std::rotl(d ^ a, 8);
    c += d;
    b = std::rotl(b ^ c, 7);
}

void chacha20_block(const Key& key, const Nonce& nonce, uint32_t counter, std::array<uint8_t, CHACHA_BLOCK_BYTES>& output) {
    struct State {
        std::array<uint32_t, 16> initial{};
        std::array<uint32_t, 16> working{};
    } state;
    EraseOnExit const ERASE(&state, sizeof(state));
    state.initial[0] = 0x61707865;
    state.initial[1] = 0x3320646e;
    state.initial[2] = 0x79622d32;
    state.initial[3] = 0x6b206574;
    for (size_t index = 0; index < 8; ++index) state.initial[4 + index] = load_le32(key.data() + index * 4);
    state.initial[12] = counter;
    for (size_t index = 0; index < 3; ++index) state.initial[13 + index] = load_le32(nonce.data() + index * 4);
    state.working = state.initial;
    for (unsigned round = 0; round < 10; ++round) {
        quarter_round(state.working[0], state.working[4], state.working[8], state.working[12]);
        quarter_round(state.working[1], state.working[5], state.working[9], state.working[13]);
        quarter_round(state.working[2], state.working[6], state.working[10], state.working[14]);
        quarter_round(state.working[3], state.working[7], state.working[11], state.working[15]);
        quarter_round(state.working[0], state.working[5], state.working[10], state.working[15]);
        quarter_round(state.working[1], state.working[6], state.working[11], state.working[12]);
        quarter_round(state.working[2], state.working[7], state.working[8], state.working[13]);
        quarter_round(state.working[3], state.working[4], state.working[9], state.working[14]);
    }
    for (size_t index = 0; index < 16; ++index) {
        store_le32(output.data() + index * 4, state.working[index] + state.initial[index]);
    }
}

void chacha20_xor(const Key& key, const Nonce& nonce, const uint8_t* input, size_t length, uint8_t* output) {
    std::array<uint8_t, CHACHA_BLOCK_BYTES> block{};
    EraseOnExit const ERASE(block.data(), block.size());
    uint32_t counter = 1;
    while (length != 0) {
        chacha20_block(key, nonce, counter, block);
        size_t const CHUNK = std::min(length, block.size());
        for (size_t index = 0; index < CHUNK; ++index) output[index] = input[index] ^ block[index];
        input += CHUNK;
        output += CHUNK;
        length -= CHUNK;
        // The validated final block may use UINT32_MAX; never wrap it.
        if (length != 0) ++counter;
    }
}

class Poly1305 final {
   public:
    explicit Poly1305(const uint8_t* key) {
        r_[0] = load_le32(key) & LIMB_MASK;
        r_[1] = (load_le32(key + 3) >> 2U) & 0x3ffff03;
        r_[2] = (load_le32(key + 6) >> 4U) & 0x3ffc0ff;
        r_[3] = (load_le32(key + 9) >> 6U) & 0x3f03fff;
        r_[4] = (load_le32(key + 12) >> 8U) & 0x00fffff;
        for (size_t index = 0; index < 4; ++index) pad_[index] = load_le32(key + 16 + index * 4);
    }

    ~Poly1305() { auth_crypto::secure_erase(this, sizeof(*this)); }
    Poly1305(const Poly1305&) = delete;
    auto operator=(const Poly1305&) -> Poly1305& = delete;

    void block(const uint8_t* input, uint32_t high_bit = uint32_t{1} << 24U) {
        h_[0] += load_le32(input) & LIMB_MASK;
        h_[1] += (load_le32(input + 3) >> 2U) & LIMB_MASK;
        h_[2] += (load_le32(input + 6) >> 4U) & LIMB_MASK;
        h_[3] += (load_le32(input + 9) >> 6U) & LIMB_MASK;
        h_[4] += (load_le32(input + 12) >> 8U) | high_bit;

        struct Products {
            std::array<uint64_t, 5> d{};
            uint64_t carry = 0;
        } products;
        EraseOnExit const ERASE(&products, sizeof(products));
        // Radix 2^26, with wrapped terms multiplied by 5 because 2^130 = 5 mod p.
        for (size_t index = 0; index < 5; ++index) {
            for (size_t term = 0; term < 5; ++term) {
                size_t const R_INDEX = (index + 5 - term) % 5;
                uint64_t const FACTOR = term > index ? 5 : 1;
                products.d[index] += uint64_t{h_[term]} * r_[R_INDEX] * FACTOR;
            }
        }
        for (size_t index = 0; index < 4; ++index) {
            h_[index] = static_cast<uint32_t>(products.d[index]) & LIMB_MASK;
            products.carry = products.d[index] >> 26U;
            products.d[index + 1] += products.carry;
        }
        h_[4] = static_cast<uint32_t>(products.d[4]) & LIMB_MASK;
        h_[0] += static_cast<uint32_t>(products.d[4] >> 26U) * 5U;
        products.carry = h_[0] >> 26U;
        h_[0] &= LIMB_MASK;
        h_[1] += static_cast<uint32_t>(products.carry);
    }

    // AEAD authenticates zero-padding as actual bytes, so every processed
    // block here is a full 16-byte Poly1305 block, including a padded tail.
    void padded(const uint8_t* input, size_t length) {
        while (length >= 16) {
            block(input);
            input += 16;
            length -= 16;
        }
        if (length != 0) {
            std::array<uint8_t, 16> tail{};
            EraseOnExit const ERASE(tail.data(), tail.size());
            std::memcpy(tail.data(), input, length);
            block(tail.data());
        }
    }

    auto finish() -> Tag {
        struct Reduction {
            std::array<uint32_t, 5> g{};
            uint32_t carry = 0;
            uint32_t mask = 0;
            uint64_t word = 0;
        } reduction;
        EraseOnExit const ERASE(&reduction, sizeof(reduction));
        for (size_t index = 1; index < 4; ++index) {
            reduction.carry = h_[index] >> 26U;
            h_[index] &= LIMB_MASK;
            h_[index + 1] += reduction.carry;
        }
        reduction.carry = h_[4] >> 26U;
        h_[4] &= LIMB_MASK;
        h_[0] += reduction.carry * 5U;
        reduction.carry = h_[0] >> 26U;
        h_[0] &= LIMB_MASK;
        h_[1] += reduction.carry;

        reduction.g[0] = h_[0] + 5U;
        reduction.carry = reduction.g[0] >> 26U;
        reduction.g[0] &= LIMB_MASK;
        for (size_t index = 1; index < 4; ++index) {
            reduction.g[index] = h_[index] + reduction.carry;
            reduction.carry = reduction.g[index] >> 26U;
            reduction.g[index] &= LIMB_MASK;
        }
        reduction.g[4] = h_[4] + reduction.carry - (uint32_t{1} << 26U);
        // Underflow means h < p. Select h or h-p without a secret branch.
        reduction.mask = (reduction.g[4] >> 31U) - 1U;
        for (size_t index = 0; index < 5; ++index) {
            h_[index] = (h_[index] & ~reduction.mask) | (reduction.g[index] & reduction.mask);
        }

        Tag tag{};
        reduction.word = uint64_t{static_cast<uint32_t>(h_[0] | (h_[1] << 26U))} + pad_[0];
        store_le32(tag.data(), static_cast<uint32_t>(reduction.word));
        reduction.word = uint64_t{static_cast<uint32_t>((h_[1] >> 6U) | (h_[2] << 20U))} + pad_[1] + (reduction.word >> 32U);
        store_le32(tag.data() + 4, static_cast<uint32_t>(reduction.word));
        reduction.word = uint64_t{static_cast<uint32_t>((h_[2] >> 12U) | (h_[3] << 14U))} + pad_[2] + (reduction.word >> 32U);
        store_le32(tag.data() + 8, static_cast<uint32_t>(reduction.word));
        reduction.word = uint64_t{static_cast<uint32_t>((h_[3] >> 18U) | (h_[4] << 8U))} + pad_[3] + (reduction.word >> 32U);
        store_le32(tag.data() + 12, static_cast<uint32_t>(reduction.word));
        return tag;
    }

   private:
    std::array<uint32_t, 5> r_{};
    std::array<uint32_t, 5> h_{};
    std::array<uint32_t, 4> pad_{};
};

auto authenticate(const Key& key, const Nonce& nonce, const uint8_t* aad, size_t aad_length, const uint8_t* ciphertext, size_t length)
    -> Tag {
    std::array<uint8_t, CHACHA_BLOCK_BYTES> block{};
    EraseOnExit const ERASE(block.data(), block.size());
    chacha20_block(key, nonce, 0, block);
    Poly1305 poly(block.data());
    auth_crypto::secure_erase(block.data(), block.size());
    poly.padded(aad, aad_length);
    poly.padded(ciphertext, length);
    std::array<uint8_t, 16> lengths{};
    store_le64(lengths.data(), aad_length);
    store_le64(lengths.data() + 8, length);
    poly.block(lengths.data());
    return poly.finish();
}

struct Range {
    uintptr_t start = 0;
    uintptr_t end = 0;
};

auto checked_range(const void* data, size_t length, Range& result) -> bool {
    if (data == nullptr && length != 0) return false;
    uintptr_t const START = reinterpret_cast<uintptr_t>(data);
    if (length > UINTPTR_MAX - START) return false;
    result = {.start = START, .end = START + length};
    return true;
}

auto overlaps(const Range& lhs, const Range& rhs) -> bool {
    return lhs.start != lhs.end && rhs.start != rhs.end && lhs.start < rhs.end && rhs.start < lhs.end;
}

auto valid_buffers(const Key& key, const Nonce& nonce, const void* aad, size_t aad_length, const void* input, size_t length, void* output,
                   const Tag& tag, bool sealing) -> bool {
    static_assert(sizeof(size_t) <= sizeof(uint64_t));
    if (length > MAX_PLAINTEXT_BYTES) return false;
    Range key_range{}, nonce_range{}, aad_range{}, input_range{}, output_range{}, tag_range{};
    if (!checked_range(key.data(), key.size(), key_range) || !checked_range(nonce.data(), nonce.size(), nonce_range) ||
        !checked_range(aad, aad_length, aad_range) || !checked_range(input, length, input_range) ||
        !checked_range(output, length, output_range) || !checked_range(tag.data(), tag.size(), tag_range)) {
        return false;
    }
    if ((overlaps(input_range, output_range) && input_range.start != output_range.start) || overlaps(output_range, key_range) ||
        overlaps(output_range, nonce_range) || overlaps(output_range, aad_range) || overlaps(output_range, tag_range)) {
        return false;
    }
    return !sealing || (!overlaps(tag_range, key_range) && !overlaps(tag_range, nonce_range) && !overlaps(tag_range, aad_range) &&
                        !overlaps(tag_range, input_range));
}

}  // namespace

auto seal(const Key& key, const Nonce& nonce, const void* aad, size_t aad_length, const void* input, size_t length, void* output, Tag& tag)
    -> bool {
    if (!valid_buffers(key, nonce, aad, aad_length, input, length, output, tag, true)) return false;
    chacha20_xor(key, nonce, static_cast<const uint8_t*>(input), length, static_cast<uint8_t*>(output));
    Tag computed = authenticate(key, nonce, static_cast<const uint8_t*>(aad), aad_length, static_cast<const uint8_t*>(output), length);
    EraseOnExit const ERASE(computed.data(), computed.size());
    tag = computed;
    return true;
}

auto open(const Key& key, const Nonce& nonce, const void* aad, size_t aad_length, const void* input, size_t length, void* output,
          const Tag& tag) -> bool {
    if (!valid_buffers(key, nonce, aad, aad_length, input, length, output, tag, false)) return false;
    Tag computed = authenticate(key, nonce, static_cast<const uint8_t*>(aad), aad_length, static_cast<const uint8_t*>(input), length);
    EraseOnExit const ERASE(computed.data(), computed.size());
    if (!auth_crypto::constant_time_equal(computed.data(), tag.data(), tag.size())) return false;
    chacha20_xor(key, nonce, static_cast<const uint8_t*>(input), length, static_cast<uint8_t*>(output));
    return true;
}

}  // namespace ker::net::wki::rdma_crypto
