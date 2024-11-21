#pragma once

#include <defines/defines.hpp>

namespace std {
template <typename T>
class list {
   public:
    struct Node {
        T data;
        Node* next;
        Node* prev;
    };

    list() {
        head = nullptr;
        tail = nullptr;
        size = 0;
    }

    void push_back(T data) {
        Node* node = new Node();
        node->data = data;
        node->next = nullptr;
        node->prev = tail;

        if (tail) {
            tail->next = node;
        }

        tail = node;

        if (!head) {
            head = node;
        }

        size++;
    }

    void push_front(T data) {
        Node* node = new Node();
        node->data = data;
        node->next = head;
        node->prev = nullptr;

        if (head) {
            head->prev = node;
        }

        head = node;

        if (!tail) {
            tail = node;
        }

        size++;
    }

    T pop_back() {
        if (!tail) {
            return T();
        }

        T data = tail->data;
        Node* prev = tail->prev;

        if (prev) {
            prev->next = nullptr;
        }

        delete tail;
        tail = prev;

        if (!tail) {
            head = nullptr;
        }

        size--;

        return data;
    }

    T pop_front() {
        if (!head) {
            return T();
        }

        T data = head->data;
        Node* next = head->next;

        if (next) {
            next->prev = nullptr;
        }

        delete head;
        head = next;

        if (!head) {
            tail = nullptr;
        }

        size--;

        return data;
    }

    T front() { return head->data; }

    T back() { return tail->data; }

    T* begin() { return &head->data; }

    T* end() { return &tail->data; }

    uint64_t get_size() { return size; }

   private:
    Node* head;
    Node* tail;
    uint64_t size;
};
}  // namespace std
