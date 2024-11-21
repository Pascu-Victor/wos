#pragma once

namespace std {
template <typename T>
class vector {
   private:
    T *_data;
    size_t _size;
    size_t _capacity;

   public:
    vector() : _data(nullptr), _size(0), _capacity(0) {}

    vector(size_t size) : _data(new T[size]), _size(size), _capacity(size) {}

    vector(size_t size, const T &value) : _data(new T[size]), _size(size), _capacity(size) {
        for (size_t i = 0; i < size; i++) {
            _data[i] = value;
        }
    }

    vector(const vector &other) : _data(new T[other._size]), _size(other._size), _capacity(other._size) {
        for (size_t i = 0; i < _size; i++) {
            _data[i] = other._data[i];
        }
    }

    vector(vector &&other) : _data(other._data), _size(other._size), _capacity(other._capacity) {
        other._data = nullptr;
        other._size = 0;
        other._capacity = 0;
    }

    ~vector() {
        if (_data != nullptr) {
            delete[] _data;
        }
    }

    T &operator[](size_t index) { return _data[index]; }

    const T &operator[](size_t index) const { return _data[index]; }

    void push_back(const T &value) {
        if (_size == _capacity) {
            _capacity = _capacity == 0 ? 1 : _capacity * 2;
            T *newData = new T[_capacity];
            for (size_t i = 0; i < _size; i++) {
                newData[i] = _data[i];
            }
            delete[] _data;
            _data = newData;
        }
        _data[_size++] = value;
    }

    void pop_back() {
        if (_size > 0) {
            _size--;
        }
    }

    size_t size() const { return _size; }

    size_t capacity() const { return _capacity; }

    T *data() { return _data; }

    const T *data() const { return _data; }

    vector &operator=(const vector &other) {
        if (this != &other) {
            if (_data != nullptr) {
                delete[] _data;
            }
            _data = new T[other._size];
            _size = other._size;
            _capacity = other._size;
            for (size_t i = 0; i < _size; i++) {
                _data[i] = other._data[i];
            }
        }
        return *this;
    }

    vector &operator=(vector &&other) {
        if (this != &other) {
            if (_data != nullptr) {
                delete[] _data;
            }
            _data = other._data;
            _size = other._size;
            _capacity = other._capacity;
            other._data = nullptr;
            other._size = 0;
            other._capacity = 0;
        }
        return *this;
    }

    bool empty() const { return _size == 0; }

    const T &back() const { return _data[_size - 1]; }
    const T &front() const { return _data[0]; }

    T &back() { return _data[_size - 1]; }
    T &front() { return _data[0]; }

    class iterator {
       private:
        T *_ptr;

       public:
        iterator(T *ptr) : _ptr(ptr) {}

        T &operator*() { return *_ptr; }

        T *operator->() { return _ptr; }

        iterator &operator++() {
            _ptr++;
            return *this;
        }

        iterator operator++(int) {
            iterator temp = *this;
            _ptr++;
            return temp;
        }

        iterator &operator--() {
            _ptr--;
            return *this;
        }

        iterator operator--(int) {
            iterator temp = *this;
            _ptr--;
            return temp;
        }

        iterator base() const { return iterator(_ptr); }

        bool operator==(const iterator &other) const { return _ptr == other._ptr; }

        bool operator!=(const iterator &other) const { return _ptr != other._ptr; }

        iterator operator+(size_t n) const { return iterator(_ptr + n); }

        iterator operator-(size_t n) const { return iterator(_ptr - n); }
    };

    iterator begin() { return iterator(_data); }
    iterator end() { return iterator(_data + _size); }

    class reverse_iterator {
       private:
        T *_ptr;

       public:
        reverse_iterator(T *ptr) : _ptr(ptr) {}

        T &operator*() { return *_ptr; }

        T *operator->() { return _ptr; }

        reverse_iterator &operator++() {
            _ptr--;
            return *this;
        }

        reverse_iterator operator++(int) {
            reverse_iterator temp = *this;
            _ptr--;
            return temp;
        }

        reverse_iterator &operator--() {
            _ptr++;
            return *this;
        }

        reverse_iterator operator--(int) {
            reverse_iterator temp = *this;
            _ptr++;
            return temp;
        }

        reverse_iterator base() const { return reverse_iterator(_ptr + 1); }

        bool operator==(const reverse_iterator &other) const { return _ptr == other._ptr; }

        bool operator!=(const reverse_iterator &other) const { return _ptr != other._ptr; }

        reverse_iterator operator+(size_t n) const { return reverse_iterator(_ptr - n); }

        reverse_iterator operator-(size_t n) const { return reverse_iterator(_ptr + n); }
    };

    reverse_iterator rbegin() { return reverse_iterator(_data + _size - 1); }
    reverse_iterator rend() { return reverse_iterator(_data - 1); }

    void clear() {
        if (_data != nullptr) {
            delete[] _data;
            _data = nullptr;
            _size = 0;
            _capacity = 0;
        }
    }

    void reserve(size_t newCapacity) {
        if (newCapacity > _capacity) {
            T *newData = new T[newCapacity];
            for (size_t i = 0; i < _size; i++) {
                newData[i] = _data[i];
            }
            delete[] _data;
            _data = newData;
            _capacity = newCapacity;
        }
    }

    void resize(size_t newSize) {
        if (newSize > _size) {
            if (newSize > _capacity) {
                reserve(newSize);
            }
            for (size_t i = _size; i < newSize; i++) {
                _data[i] = T();
            }
        }
        _size = newSize;
    }

    void resize(size_t newSize, const T &value) {
        if (newSize > _size) {
            if (newSize > _capacity) {
                reserve(newSize);
            }
            for (size_t i = _size; i < newSize; i++) {
                _data[i] = value;
            }
        }
        _size = newSize;
    }

    void shrink_to_fit() {
        if (_size < _capacity) {
            T *newData = new T[_size];
            for (size_t i = 0; i < _size; i++) {
                newData[i] = _data[i];
            }
            delete[] _data;
            _data = newData;
            _capacity = _size;
        }
    }

    void swap(vector &other) {
        T *tempData = _data;
        size_t tempSize = _size;
        size_t tempCapacity = _capacity;
        _data = other._data;
        _size = other._size;
        _capacity = other._capacity;
        other._data = tempData;
        other._size = tempSize;
        other._capacity = tempCapacity;
    }

    iterator erase(iterator pos) {
        if (pos == end()) {
            return pos;
        }
        for (iterator it = pos; it != end() - 1; ++it) {
            *it = *(it + 1);
        }
        --_size;
        return pos;
    }

    iterator erase(iterator first, iterator last) {
        if (first == last) {
            return first;
        }
        size_t count = last - first;
        for (iterator it = first; it != end() - count; ++it) {
            *it = *(it + count);
        }
        _size -= count;
        return first;
    }

    reverse_iterator erase(reverse_iterator pos) {
        if (pos == rend()) {
            return pos;
        }
        for (reverse_iterator it = pos; it != rend() - 1; ++it) {
            *it = *(it + 1);
        }
        --_size;
        return pos;
    }
};
}  // namespace std
