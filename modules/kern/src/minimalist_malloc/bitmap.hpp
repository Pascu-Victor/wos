#pragma once
#include <array>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <iterator>

inline constexpr unsigned BITMAP_NO_BITS_LEFT = 0xFFFFFFFFU;

template <size_t SIZE>
class Bitmap {
   private:
    static constexpr unsigned BITS_PER_WORD = 32U;
    static constexpr uint32_t ALL_BITS_SET = 0xFFFFFFFFU;

    std::array<uint32_t, SIZE> m_bitmap_data;

    [[nodiscard]] static constexpr bool check_bit(uint32_t value, unsigned bit) { return ((value >> bit) & uint32_t{1}) != 0U; }
    [[nodiscard]] uint32_t& word_at(size_t word_index) {
        assert(word_index < m_bitmap_data.size());
        return *std::next(m_bitmap_data.begin(), static_cast<ptrdiff_t>(word_index));
    }
    [[nodiscard]] const uint32_t& word_at(size_t word_index) const {
        assert(word_index < m_bitmap_data.size());
        return *std::next(m_bitmap_data.begin(), static_cast<ptrdiff_t>(word_index));
    }
    [[nodiscard]] uint32_t& word_for_bit(size_t bit_index) { return word_at(bit_index / BITS_PER_WORD); }
    [[nodiscard]] const uint32_t& word_for_bit(size_t bit_index) const { return word_at(bit_index / BITS_PER_WORD); }

   public:
    void init();
    void set_used(unsigned position);
    void set_unused(unsigned position);
    unsigned find_unused(unsigned search_start = 0);
    unsigned find_used(unsigned search_start = 0);
    [[nodiscard]] bool check_used(unsigned position) const;
    [[nodiscard]] bool check_unused(unsigned position) const;

    // Diagnostic accessors for debugging only
    [[nodiscard]] unsigned word_count() const { return SIZE; }
    [[nodiscard]] uint32_t word_at(unsigned i) const { return m_bitmap_data[i]; }
};

template <size_t SIZE>
void Bitmap<SIZE>::init() {
    m_bitmap_data.fill(0U);
}

template <size_t SIZE>
void Bitmap<SIZE>::set_used(unsigned position) {
    assert(position < SIZE);
    word_for_bit(position) |= (uint32_t{1} << (position % BITS_PER_WORD));
}

template <size_t SIZE>
void Bitmap<SIZE>::set_unused(unsigned position) {
    assert(position < SIZE);
    word_for_bit(position) &= ~(uint32_t{1} << (position % BITS_PER_WORD));
}

template <size_t SIZE>
unsigned Bitmap<SIZE>::find_unused(unsigned search_start) {
    assert(search_start < SIZE);
    size_t bit_index = search_start;
    while (bit_index < SIZE) {
        // If all 32 bits in this word are set, skip the whole word
        if (word_for_bit(bit_index) == ALL_BITS_SET) {
            bit_index += BITS_PER_WORD;
            continue;
        }
        if (!check_bit(word_for_bit(bit_index), bit_index % BITS_PER_WORD)) {
            return bit_index;
        }

        bit_index++;
    }
    return BITMAP_NO_BITS_LEFT;
}

template <size_t SIZE>
unsigned Bitmap<SIZE>::find_used(unsigned search_start) {
    assert(search_start < SIZE);
    size_t bit_index = search_start;
    while (bit_index < SIZE) {
        // If all 32 bits in this word are clear, skip the whole word
        if (word_for_bit(bit_index) == 0U) {
            bit_index += BITS_PER_WORD;
            continue;
        }
        if (check_bit(word_for_bit(bit_index), bit_index % BITS_PER_WORD)) {
            return bit_index;
        }

        bit_index++;
    }
    return BITMAP_NO_BITS_LEFT;
}

template <size_t SIZE>
bool Bitmap<SIZE>::check_used(unsigned position) const {
    return check_bit(word_for_bit(position), position % BITS_PER_WORD);
}

template <size_t SIZE>
bool Bitmap<SIZE>::check_unused(unsigned position) const {
    return !check_bit(word_for_bit(position), position % BITS_PER_WORD);
}
