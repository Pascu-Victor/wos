#pragma once
#include <std/except.hpp>
#include <std/function.hpp>
#include <std/initializer_list.hpp>

namespace std {
template <typename>
class function;  // primary template

template <typename R, typename... Args>
class function<R(Args...)> {
   public:
    function() noexcept : callable(nullptr) {}

    template <typename F>
    function(F f) : callable(new CallableImpl<F>(f)) {}

    function(const function& other) : callable(other.callable ? other.callable->clone() : nullptr) {}

    function(function&& other) noexcept : callable(other.callable) { other.callable = nullptr; }

    function& operator=(const function& other) {
        if (this != &other) {
            delete callable;
            callable = other.callable ? other.callable->clone() : nullptr;
        }
        return *this;
    }

    function& operator=(function&& other) noexcept {
        if (this != &other) {
            delete callable;
            callable = other.callable;
            other.callable = nullptr;
        }
        return *this;
    }

    ~function() { delete callable; }

    R operator()(Args... args) const {
        if (!callable) {
            throw std::bad_function_call();
        }
        return callable->invoke(std::forward<Args>(args)...);
    }

    explicit operator bool() const noexcept { return callable != nullptr; }

   private:
    struct CallableBase {
        virtual ~CallableBase() = default;
        virtual R invoke(Args... args) const = 0;
        virtual CallableBase* clone() const = 0;
    };

    template <typename F>
    struct CallableImpl : CallableBase {
        F f;
        CallableImpl(F f) : f(f) {}
        R invoke(Args... args) const override { return f(std::forward<Args>(args)...); }
        CallableBase* clone() const override { return new CallableImpl(f); }
    };

    CallableBase* callable;

    void swap(function<R(Args...)>& f1, function<R(Args...)>& f2) noexcept { std::swap(f1.callable, f2.callable); }
};

}  // namespace std
