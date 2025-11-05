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
        m_size = 0;
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

        m_size++;
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

        m_size++;
    }

    auto pop_back() -> T {
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

        m_size--;

        return data;
    }

    auto pop_front() -> T {
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

        m_size--;

        return data;
    }

    T front() { return head->data; }

    T back() { return tail->data; }

    T* begin() { return &head->data; }

    T* end() { return &tail->data; }

    void remove(const T& data) {
        Node* current = head;
        while (current) {
            if (current->data == data) {
                if (current->prev) {
                    current->prev->next = current->next;
                } else {
                    head = current->next;
                }

                if (current->next) {
                    current->next->prev = current->prev;
                } else {
                    tail = current->prev;
                }

                delete current;
                m_size--;
                return;
            }
            current = current->next;
        }
    }

    auto size() -> uint64_t { return this->m_size; }

   private:
    Node* head;
    Node* tail;
    uint64_t m_size;
};
}  // namespace std
