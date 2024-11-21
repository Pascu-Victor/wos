#pragma once
#include <std/except.hpp>
#include <std/function.hpp>
#include <std/initializer_list.hpp>
#include <std/move.hpp>

namespace std {
struct nullopt_t {
    explicit constexpr nullopt_t() noexcept {}
};
inline constexpr nullopt_t nullopt = nullopt_t{};

template <typename T>
class optional {
   public:
    optional() : has_value_{false} {}
    optional(const T& value) : has_value_{true} { new (&storage_) T(value); }
    optional(T&& value) : has_value_{true} { new (&storage_) T(std::move(value)); }
    optional(const optional& other) : has_value_{other.has_value_} {
        if (has_value_) {
            new (&storage_) T(*other);
        }
    }
    optional(optional&& other) : has_value_{other.has_value_} {
        if (has_value_) {
            new (&storage_) T(std::move(*other));
        }
    }
    ~optional() {
        if (has_value_) {
            value().~T();
        }
    }

    optional& operator=(const optional& other) {
        if (has_value_) {
            value().~T();
        }
        has_value_ = other.has_value_;
        if (has_value_) {
            new (&storage_) T(*other);
        }
        return *this;
    }

    optional& operator=(optional&& other) {
        if (has_value_) {
            value().~T();
        }
        has_value_ = other.has_value_;
        if (has_value_) {
            new (&storage_) T(std::move(*other));
        }
        return *this;
    }

    T& operator*() { return value(); }
    const T& operator*() const { return value(); }

    T* operator->() { return &value(); }
    const T* operator->() const { return &value(); }

    explicit operator bool() const { return has_value_; }

    T& value() {
        if (!has_value_) {
            throw std::bad_optional_access();
        }
        return *reinterpret_cast<T*>(&storage_);
    }

    const T& value() const {
        if (!has_value_) {
            throw std::bad_optional_access();
        }
        return *reinterpret_cast<const T*>(&storage_);
    }

    optional(nullopt_t) : has_value_{false} {}

    optional& operator=(nullopt_t) {
        if (has_value_) {
            value().~T();
        }
        has_value_ = false;
        return *this;
    }

    template <typename... Args>
    void emplace(Args&&... args) {
        if (has_value_) {
            value().~T();
        }
        has_value_ = true;
        new (&storage_) T(std::forward<Args>(args)...);
    }

    template <typename U, typename... Args>
    void emplace(std::initializer_list<U> il, Args&&... args) {
        if (has_value_) {
            value().~T();
        }
        has_value_ = true;
        new (&storage_) T(il, std::forward<Args>(args)...);
    }

    template <typename U>
    T value_or(U&& default_value) const {
        if (has_value_) {
            return value_;
        } else {
            return static_cast<T>(std::forward<U>(default_value));
        }
    }

    template <typename U>
    T value_or(U&& default_value) {
        if (has_value_) {
            return value_;
        } else {
            return static_cast<T>(std::forward<U>(default_value));
        }
    }

    template <typename U>
    optional<U> map(std::function<U(T)> f) {
        if (has_value_) {
            return optional<U>(f(value()));
        } else {
            return optional<U>();
        }
    }

    template <typename U>
    optional<U> map(std::function<U(T)> f) const {
        if (has_value_) {
            return optional<U>(f(value()));
        } else {
            return optional<U>();
        }
    }

    template <typename U>
    explicit operator optional<U>() const {
        if (has_value_) {
            return optional<U>(static_cast<U>(value()));
        } else {
            return optional<U>();
        }
    }

    template <typename U>
    optional<U> and_then(std::function<optional<U>(T)> f) {
        if (has_value_) {
            return f(value());
        } else {
            return optional<U>();
        }
    }

   private:
    bool has_value_;
    T value_;
    alignas(T) char storage_[sizeof(T)];
};
}  // namespace std
