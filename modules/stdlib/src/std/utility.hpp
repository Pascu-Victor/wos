#ifndef STD_UTILITY_HPP
#define STD_UTILITY_HPP

#include <std/type_traits.hpp>

namespace std {

template <typename T>
T&& forward(typename remove_reference<T>::type& t) noexcept {
    return static_cast<T&&>(t);
}

template <typename T>
T&& forward(typename remove_reference<T>::type&& t) noexcept {
    static_assert(!std::is_lvalue_reference<T>::value, "Can not forward an rvalue as an lvalue.");
    return static_cast<T&&>(t);
}

template <typename T>
constexpr typename std::remove_reference<T>::type&& move(T&& arg) noexcept {
    return static_cast<typename std::remove_reference<T>::type&&>(arg);
}

template <class T>
constexpr T&& declval() noexcept;

}  // namespace std

#endif  // STD_UTILITY_HPP
