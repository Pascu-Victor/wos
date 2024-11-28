#pragma once
#include <platform/sys/spinlock.hpp>
#include <std/mem.hpp>

namespace std {
template <typename T>
class Borrowable {
   private:
    T& _data;
    ker::mod::sys::Spinlock _borrowLock;

   public:
    friend class BorrowedRef;
    class BorrowedRef {
       private:
        Borrowable* _parent;

       public:
        BorrowedRef(Borrowable<T>* parent) : _parent(parent) { _parent->_borrowLock.lock(); }

        ~BorrowedRef() { _parent->_borrowLock.unlock(); }

        T& get() { return _parent->_data; }

        T copy() { return _parent->_data; }

        T operator*() { return _parent->_data; }

        T* operator->() { return &_parent->_data; }

        const T& get() const { return _parent->_data; }

        const T copy() const { return _parent->_data; }

        const T operator*() const { return _parent->_data; }

        const T* operator->() const { return &_parent->_data; }

        BorrowedRef(const BorrowedRef&) = delete;
        BorrowedRef& operator=(const BorrowedRef&) = delete;
    };

    // Borrowable(std::unique_ptr<T> defaultValue = nullptr)
    //     : _data(defaultValue.get() != nullptr ? *defaultValue : *(new T())), _borrowLock() {}

    Borrowable(T defaultValue) : _data(*(new T())), _borrowLock() { _data = defaultValue; }

    Borrowable() : _data(*(new T())), _borrowLock() {}

    ~Borrowable() { delete &_data; }

    BorrowedRef borrow() { return BorrowedRef(this); }

    Borrowable& operator=(const T& data) {
        _borrowLock.lock();
        _data = data;
        _borrowLock.unlock();
        return *this;
    }
};

}  // namespace std
