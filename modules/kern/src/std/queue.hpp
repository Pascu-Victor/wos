#pragma once

#include <defines/defines.hpp>
#include <platform/mm/dyn/kmalloc.hpp>

namespace std {
template <typename T>
class PriorityQueue {
   public:
    PriorityQueue() {
        this->size = 0;
        this->capacity = 10;
        this->data = (T*)kmalloc(sizeof(T) * this->capacity);
    }

    ~PriorityQueue() { kfree(this->data); }

    void push(T item) {
        if (this->size == this->capacity) {
            this->capacity *= 2;
            this->data = (T*)krealloc(this->data, sizeof(T) * this->capacity);
        }

        this->data[this->size] = item;
        this->size++;
        this->heapifyUp();
    }

    T pop() {
        if (this->size == 0) {
            return T();
        }

        T item = this->data[0];
        this->data[0] = this->data[this->size - 1];
        this->size--;
        this->heapifyDown();
        return item;
    }

    T peek() {
        if (this->size == 0) {
            return T();
        }

        return this->data[0];
    }

    bool empty() { return this->size == 0; }

    size_t getSize() { return this->size; }

   private:
    T* data;
    size_t size;
    size_t capacity;

    void heapifyUp() {
        size_t index = this->size - 1;
        while (index > 0) {
            size_t parentIndex = (index - 1) / 2;
            if (this->data[index] < this->data[parentIndex]) {
                T temp = this->data[index];
                this->data[index] = this->data[parentIndex];
                this->data[parentIndex] = temp;
                index = parentIndex;
            } else {
                break;
            }
        }
    }

    void heapifyDown() {
        size_t index = 0;
        while (true) {
            size_t leftChildIndex = 2 * index + 1;
            size_t rightChildIndex = 2 * index + 2;
            size_t smallestIndex = index;

            if (leftChildIndex < this->size && this->data[leftChildIndex] < this->data[smallestIndex]) {
                smallestIndex = leftChildIndex;
            }

            if (rightChildIndex < this->size && this->data[rightChildIndex] < this->data[smallestIndex]) {
                smallestIndex = rightChildIndex;
            }

            if (smallestIndex != index) {
                T temp = this->data[index];
                this->data[index] = this->data[smallestIndex];
                this->data[smallestIndex] = temp;
                index = smallestIndex;
            } else {
                break;
            }
        }
    }
};
}  // namespace std
