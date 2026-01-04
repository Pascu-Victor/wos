#pragma once
#include <defines/defines.hpp>

#define CHECK_BIT(value, bit) ((value >> bit) & 1)
#define BITMAP_NO_BITS_LEFT 0xFFFFFFFF

template <size_t SIZE>
class Bitmap {
   private:
    uint32_t m_bitmap_data[SIZE];

   public:
    void init();
    void set_used(unsigned position);
    void set_unused(unsigned position);
    unsigned find_unused(unsigned search_start = 0);
    unsigned find_used(unsigned search_start = 0);
    bool check_used(unsigned position);
    bool check_unused(unsigned position);

    // Diagnostic accessors for debugging only
    [[nodiscard]] unsigned word_count() const { return SIZE; }
    [[nodiscard]] uint32_t word_at(unsigned i) const { return m_bitmap_data[i]; }
};

template <size_t SIZE>
void Bitmap<SIZE>::init() {
    memset(m_bitmap_data, 0, sizeof(m_bitmap_data));
}

template <size_t SIZE>
void Bitmap<SIZE>::set_used(unsigned position) {
    assert(position < SIZE);
    m_bitmap_data[position / 32] |= (1 << (position % 32));
}

template <size_t SIZE>
void Bitmap<SIZE>::set_unused(unsigned position) {
    assert(position < SIZE);
    m_bitmap_data[position / 32] &= ~(1 << (position % 32));
}

template <size_t SIZE>
unsigned Bitmap<SIZE>::find_unused(unsigned search_start) {
    assert(search_start < SIZE);
    size_t bit_index = search_start;
    while (bit_index < SIZE) {
        // If all 32 bits in this word are set, skip the whole word
        if (m_bitmap_data[bit_index / 32] == 0xFFFFFFFFU) {
            bit_index += 32;
            continue;
        }
        if (!CHECK_BIT(m_bitmap_data[bit_index / 32], bit_index % 32)) return bit_index;

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
        if (m_bitmap_data[bit_index / 32] == 0U) {
            bit_index += 32;
            continue;
        }
        if (CHECK_BIT(m_bitmap_data[bit_index / 32], bit_index % 32)) {
            return bit_index;
        }

        bit_index++;
    }
    return BITMAP_NO_BITS_LEFT;
}

template <size_t SIZE>
bool Bitmap<SIZE>::check_used(unsigned position) {
    return CHECK_BIT(m_bitmap_data[position / 32], position % 32);
}

template <size_t SIZE>
bool Bitmap<SIZE>::check_unused(unsigned position) {
    return !CHECK_BIT(m_bitmap_data[position / 32], position % 32);
}
