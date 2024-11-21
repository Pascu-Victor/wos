#pragma once

#include <std/move.hpp>

namespace std {

template <typename T>
class initializer_list {
   public:
    using value_type = T;
    using reference = const T&;
    using const_reference = const T&;
    using size_type = size_t;

   private:
    const T* _array;
    size_type _size;

    constexpr initializer_list(const T* array, size_type size) noexcept : _array(array), _size(size) {}

   public:
    constexpr initializer_list() noexcept : _array(nullptr), _size(0) {}

    constexpr size_type size() const noexcept { return _size; }
    constexpr const T* begin() const noexcept { return _array; }
    constexpr const T* end() const noexcept { return _array + _size; }
};

}  // namespace std
