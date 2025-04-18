#pragma once

#include <std/type_traits.hpp>
#include <std/utility.hpp>

namespace std {

template <typename T>
class MaybeNull {
   private:
    T* ptr;

   public:
    // Constructors
    constexpr MaybeNull() noexcept : ptr(nullptr) {}
    constexpr MaybeNull(std::nullptr_t) noexcept : ptr(nullptr) {}
    constexpr explicit MaybeNull(T* p) noexcept : ptr(p) {}

    // Copy/move operations
    constexpr MaybeNull(const MaybeNull&) = default;
    constexpr MaybeNull(MaybeNull&&) = default;
    constexpr MaybeNull& operator=(const MaybeNull&) = default;
    constexpr MaybeNull& operator=(MaybeNull&&) = default;

    // Null state checking
    [[nodiscard]] constexpr bool hasValue() const noexcept { return ptr != nullptr; }
    [[nodiscard]] constexpr bool isNull() const noexcept { return ptr == nullptr; }
    constexpr explicit operator bool() const noexcept { return ptr != nullptr; }

    // Safe access - these methods require checking
    [[nodiscard]] constexpr T* getValue() const {
        if (ptr == nullptr) {
            // In a kernel context, we could panic/crash here
            // Or return an error code, depending on error handling strategy
            // For now, trigger a debug assertion failure
            __builtin_trap();  // Will be replaced with proper error handling
        }
        return ptr;
    }

    // Access with fallback for null case
    template <typename U>
    [[nodiscard]] constexpr T* valueOr(U&& defaultValue) const {
        return ptr != nullptr ? ptr : static_cast<T*>(std::forward<U>(defaultValue));
    }

    // Unsafe direct access - for performance-critical code
    // This skips null checks, so use with caution!
    [[nodiscard]] constexpr T* unsafeGetRaw() const noexcept { return ptr; }

    // Pointer operations when non-null
    [[nodiscard]] constexpr T& operator*() const { return *getValue(); }

    [[nodiscard]] constexpr T* operator->() const { return getValue(); }

    // Reset to null or set new value
    constexpr void reset() noexcept { ptr = nullptr; }

    constexpr void reset(T* p) noexcept { ptr = p; }
};

// Helper function to create a MaybeNull
template <typename T>
[[nodiscard]] constexpr MaybeNull<T> makeNullable(T* ptr) noexcept {
    return MaybeNull<T>(ptr);
}

}  // namespace std
