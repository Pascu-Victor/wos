#include "entropy.hpp"

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "platform/sys/spinlock.hpp"

namespace ker::mod::random::entropy {

namespace {

constexpr size_t CHACHA_BLOCK_BYTES = 64;
constexpr size_t CHACHA_KEY_WORDS = 8;
constexpr size_t CHACHA_NONCE_WORDS = 3;
constexpr size_t SEED_BYTES = 48;
constexpr size_t MIX_CHUNK_BYTES = 32;
constexpr unsigned HARDWARE_RETRIES = 64;
constexpr unsigned UNIFORM_RETRIES = 128;

struct CpuidLeaf {
    uint32_t eax{};
    uint32_t ebx{};
    uint32_t ecx{};
    uint32_t edx{};
};

struct DrbgState {
    sys::Spinlock lock;
    std::array<uint32_t, CHACHA_KEY_WORDS> key{};
    std::array<uint32_t, CHACHA_NONCE_WORDS> nonce{};
    uint32_t counter{};
    std::atomic<bool> ready{false};
};

DrbgState g_state;  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

constexpr auto rotate_left(uint32_t value, unsigned shift) -> uint32_t { return (value << shift) | (value >> (32U - shift)); }

void quarter_round(uint32_t& a, uint32_t& b, uint32_t& c, uint32_t& d) {
    a += b;
    d ^= a;
    d = rotate_left(d, 16);
    c += d;
    b ^= c;
    b = rotate_left(b, 12);
    a += b;
    d ^= a;
    d = rotate_left(d, 8);
    c += d;
    b ^= c;
    b = rotate_left(b, 7);
}

auto load_u32_le(const uint8_t* input) -> uint32_t {
    return static_cast<uint32_t>(input[0]) | (static_cast<uint32_t>(input[1]) << 8U) | (static_cast<uint32_t>(input[2]) << 16U) |
           (static_cast<uint32_t>(input[3]) << 24U);
}

void store_u32_le(uint8_t* output, uint32_t value) {
    output[0] = static_cast<uint8_t>(value);
    output[1] = static_cast<uint8_t>(value >> 8U);
    output[2] = static_cast<uint8_t>(value >> 16U);
    output[3] = static_cast<uint8_t>(value >> 24U);
}

void chacha20_block(const std::array<uint32_t, CHACHA_KEY_WORDS>& key, uint32_t counter,
                    const std::array<uint32_t, CHACHA_NONCE_WORDS>& nonce, std::array<uint8_t, CHACHA_BLOCK_BYTES>& output) {
    constexpr std::array<uint32_t, 4> CONSTANTS{0x61707865U, 0x3320646eU, 0x79622d32U, 0x6b206574U};
    std::array<uint32_t, 16> initial{};
    initial[0] = CONSTANTS[0];
    initial[1] = CONSTANTS[1];
    initial[2] = CONSTANTS[2];
    initial[3] = CONSTANTS[3];
    for (size_t i = 0; i < key.size(); ++i) {
        initial[4 + i] = key[i];
    }
    initial[12] = counter;
    initial[13] = nonce[0];
    initial[14] = nonce[1];
    initial[15] = nonce[2];

    auto working = initial;
    for (unsigned round = 0; round < 10; ++round) {
        quarter_round(working[0], working[4], working[8], working[12]);
        quarter_round(working[1], working[5], working[9], working[13]);
        quarter_round(working[2], working[6], working[10], working[14]);
        quarter_round(working[3], working[7], working[11], working[15]);
        quarter_round(working[0], working[5], working[10], working[15]);
        quarter_round(working[1], working[6], working[11], working[12]);
        quarter_round(working[2], working[7], working[8], working[13]);
        quarter_round(working[3], working[4], working[9], working[14]);
    }
    for (size_t i = 0; i < working.size(); ++i) {
        store_u32_le(output.data() + (i * sizeof(uint32_t)), working[i] + initial[i]);
    }
}

template <typename T>
void secure_clear(T& value) {
    auto* bytes = reinterpret_cast<volatile uint8_t*>(&value);
    for (size_t i = 0; i < sizeof(T); ++i) {
        bytes[i] = 0;
    }
}

auto cpuid(uint32_t leaf, uint32_t subleaf = 0) -> CpuidLeaf {
    CpuidLeaf result{};
    asm volatile("cpuid" : "=a"(result.eax), "=b"(result.ebx), "=c"(result.ecx), "=d"(result.edx) : "a"(leaf), "c"(subleaf) : "memory");
    return result;
}

auto rdseed64(uint64_t& value) -> bool {
    unsigned char ok = 0;
    // The value is written before the carry output's memory address is used;
    // early-clobber prevents Clang from allocating that address in value's
    // result register under sanitizer instrumentation.
    asm volatile("rdseed %0; setc %1" : "=&r"(value), "=qm"(ok)::"memory");
    return ok != 0;
}

auto rdrand64(uint64_t& value) -> bool {
    unsigned char ok = 0;
    asm volatile("rdrand %0; setc %1" : "=&r"(value), "=qm"(ok)::"memory");
    return ok != 0;
}

template <typename Reader>
auto retry_hardware_word(Reader reader, uint64_t& value) -> bool {
    for (unsigned attempt = 0; attempt < HARDWARE_RETRIES; ++attempt) {
        if (reader(value)) {
            return true;
        }
        asm volatile("pause" ::: "memory");
    }
    return false;
}

auto hardware_seed(std::array<uint8_t, SEED_BYTES>& seed) -> bool {
    CpuidLeaf const MAXIMUM = cpuid(0);
    bool const HAS_RDRAND = MAXIMUM.eax >= 1 && (cpuid(1).ecx & (1U << 30U)) != 0;
    bool const HAS_RDSEED = MAXIMUM.eax >= 7 && (cpuid(7).ebx & (1U << 18U)) != 0;
    if (!HAS_RDSEED && !HAS_RDRAND) {
        return false;
    }

    for (size_t offset = 0; offset < seed.size(); offset += sizeof(uint64_t)) {
        uint64_t sample = 0;
        bool ok = HAS_RDSEED && retry_hardware_word(rdseed64, sample);
        if (!ok) {
            ok = HAS_RDRAND && retry_hardware_word(rdrand64, sample);
        }
        if (!ok) {
            secure_clear(sample);
            return false;
        }
        std::memcpy(seed.data() + offset, &sample, sizeof(sample));
        secure_clear(sample);
    }
    return true;
}

// Advance the IETF ChaCha counter. Exhausting the 96-bit nonce is treated as a
// terminal failure even though it is unreachable in practice.
auto advance_counter(DrbgState& state) -> bool {
    ++state.counter;
    if (state.counter != 0) {
        return true;
    }
    for (auto& nonce_word : state.nonce) {
        ++nonce_word;
        if (nonce_word != 0) {
            return true;
        }
    }
    state.ready.store(false, std::memory_order_release);
    return false;
}

auto next_block(DrbgState& state, std::array<uint8_t, CHACHA_BLOCK_BYTES>& block) -> bool {
    chacha20_block(state.key, state.counter, state.nonce, block);
    return advance_counter(state);
}

// Replace all secret state from a non-exported ChaCha block. Production output
// generation does this after every exported block for backtracking resistance.
void install_rekey_block(DrbgState& state, const std::array<uint8_t, CHACHA_BLOCK_BYTES>& block) {
    for (size_t i = 0; i < state.key.size(); ++i) {
        state.key[i] = load_u32_le(block.data() + (i * sizeof(uint32_t)));
    }
    for (size_t i = 0; i < state.nonce.size(); ++i) {
        state.nonce[i] = load_u32_le(block.data() + ((8 + i) * sizeof(uint32_t)));
    }
    state.counter = load_u32_le(block.data() + (11 * sizeof(uint32_t)));
}

auto generate_one(std::array<uint8_t, CHACHA_BLOCK_BYTES>& output) -> bool {
    std::array<uint8_t, CHACHA_BLOCK_BYTES> rekey{};
    uint64_t const FLAGS = g_state.lock.lock_irqsave();
    bool ok = g_state.ready.load(std::memory_order_relaxed);
    if (ok) {
        ok = next_block(g_state, output) && next_block(g_state, rekey);
        if (ok) {
            install_rekey_block(g_state, rekey);
        }
    }
    g_state.lock.unlock_irqrestore(FLAGS);
    secure_clear(rekey);
    if (!ok) {
        secure_clear(output);
    }
    return ok;
}

auto uniform_from_sample(uint64_t sample, uint64_t upper_exclusive, uint64_t& output) -> bool {
    if (upper_exclusive == 0) {
        return false;
    }
    uint64_t const REJECTION_THRESHOLD = (uint64_t{0} - upper_exclusive) % upper_exclusive;
    if (sample < REJECTION_THRESHOLD) {
        return false;
    }
    output = sample % upper_exclusive;
    return true;
}

}  // namespace

auto initialize() -> bool {
    std::array<uint8_t, SEED_BYTES> seed{};
    if (!hardware_seed(seed)) {
        secure_clear(seed);
        return false;
    }

    uint64_t const FLAGS = g_state.lock.lock_irqsave();
    for (size_t i = 0; i < g_state.key.size(); ++i) {
        g_state.key[i] = load_u32_le(seed.data() + (i * sizeof(uint32_t)));
    }
    for (size_t i = 0; i < g_state.nonce.size(); ++i) {
        g_state.nonce[i] = load_u32_le(seed.data() + ((8 + i) * sizeof(uint32_t)));
    }
    g_state.counter = load_u32_le(seed.data() + (11 * sizeof(uint32_t)));

    // Diffuse all seed material and ensure the first raw ChaCha block is never
    // externally observable.
    std::array<uint8_t, CHACHA_BLOCK_BYTES> rekey{};
    chacha20_block(g_state.key, g_state.counter, g_state.nonce, rekey);
    install_rekey_block(g_state, rekey);
    g_state.ready.store(true, std::memory_order_release);
    g_state.lock.unlock_irqrestore(FLAGS);

    secure_clear(rekey);
    secure_clear(seed);
    return true;
}

auto is_ready() -> bool { return g_state.ready.load(std::memory_order_acquire); }

auto get_bytes(void* output, size_t size) -> bool {
    if (size == 0) {
        return true;
    }
    if (output == nullptr || !is_ready()) {
        return false;
    }

    auto* bytes = static_cast<uint8_t*>(output);
    size_t offset = 0;
    while (offset < size) {
        std::array<uint8_t, CHACHA_BLOCK_BYTES> block{};
        if (!generate_one(block)) {
            secure_clear(block);
            return false;
        }
        size_t const REMAINING = size - offset;
        size_t const COPY_SIZE = REMAINING < block.size() ? REMAINING : block.size();
        std::memcpy(bytes + offset, block.data(), COPY_SIZE);
        secure_clear(block);
        offset += COPY_SIZE;
    }
    return true;
}

auto mix_bytes(const void* input, size_t size) -> bool {
    if (size == 0) {
        return is_ready();
    }
    if (input == nullptr || !is_ready()) {
        return false;
    }

    auto const* bytes = static_cast<const uint8_t*>(input);
    size_t offset = 0;
    while (offset < size) {
        std::array<uint8_t, MIX_CHUNK_BYTES> chunk{};
        size_t const REMAINING = size - offset;
        size_t const CHUNK_SIZE = REMAINING < chunk.size() ? REMAINING : chunk.size();
        std::memcpy(chunk.data(), bytes + offset, CHUNK_SIZE);

        std::array<uint8_t, CHACHA_BLOCK_BYTES> rekey{};
        uint64_t const FLAGS = g_state.lock.lock_irqsave();
        bool ok = g_state.ready.load(std::memory_order_relaxed) && next_block(g_state, rekey);
        if (ok) {
            for (size_t i = 0; i < CHUNK_SIZE; ++i) {
                rekey[i] ^= chunk[i];
            }
            // Domain-separate writes and encode chunk boundaries.
            rekey[44] ^= 0x57U;
            rekey[45] ^= 0x4FU;
            rekey[46] ^= 0x53U;
            rekey[47] ^= static_cast<uint8_t>(CHUNK_SIZE);
            install_rekey_block(g_state, rekey);
        }
        g_state.lock.unlock_irqrestore(FLAGS);

        secure_clear(rekey);
        secure_clear(chunk);
        if (!ok) {
            return false;
        }
        offset += CHUNK_SIZE;
    }
    return true;
}

auto uniform_u64(uint64_t upper_exclusive, uint64_t& output) -> bool {
    if (upper_exclusive == 0) {
        return false;
    }
    for (unsigned attempt = 0; attempt < UNIFORM_RETRIES; ++attempt) {
        uint64_t sample = 0;
        if (!get_bytes(&sample, sizeof(sample))) {
            return false;
        }
        bool const ACCEPTED = uniform_from_sample(sample, upper_exclusive, output);
        secure_clear(sample);
        if (ACCEPTED) {
            return true;
        }
    }
    return false;
}

#ifdef WOS_SELFTEST
auto selftest_chacha20_known_answer() -> bool {
    std::array<uint32_t, CHACHA_KEY_WORDS> key{};
    for (size_t i = 0; i < key.size(); ++i) {
        std::array<uint8_t, sizeof(uint32_t)> word{};
        for (size_t j = 0; j < word.size(); ++j) {
            word[j] = static_cast<uint8_t>((i * word.size()) + j);
        }
        key[i] = load_u32_le(word.data());
    }
    std::array<uint32_t, CHACHA_NONCE_WORDS> nonce{0x09000000U, 0x4a000000U, 0x00000000U};
    std::array<uint8_t, CHACHA_BLOCK_BYTES> output{};
    chacha20_block(key, 1, nonce, output);
    constexpr std::array<uint8_t, CHACHA_BLOCK_BYTES> EXPECTED{
        0x10, 0xf1, 0xe7, 0xe4, 0xd1, 0x3b, 0x59, 0x15, 0x50, 0x0f, 0xdd, 0x1f, 0xa3, 0x20, 0x71, 0xc4, 0xc7, 0xd1, 0xf4, 0xc7, 0x33, 0xc0,
        0x68, 0x03, 0x04, 0x22, 0xaa, 0x9a, 0xc3, 0xd4, 0x6c, 0x4e, 0xd2, 0x82, 0x64, 0x46, 0x07, 0x9f, 0xaa, 0x09, 0x14, 0xc2, 0xd7, 0x05,
        0xd9, 0x8b, 0x02, 0xa2, 0xb5, 0x12, 0x9c, 0xd1, 0xde, 0x16, 0x4e, 0xb9, 0xcb, 0xd0, 0x83, 0xe8, 0xa2, 0x50, 0x3c, 0x4e,
    };
    bool const MATCHES = output == EXPECTED;
    secure_clear(output);
    secure_clear(key);
    return MATCHES;
}

auto selftest_uniform_from_sample(uint64_t sample, uint64_t upper_exclusive, uint64_t& output) -> bool {
    return uniform_from_sample(sample, upper_exclusive, output);
}
#endif

}  // namespace ker::mod::random::entropy
