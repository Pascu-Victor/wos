#pragma once

#include <defines/defines.hpp>

namespace std {
template <typename T>
// NOLINTNEXTLINE(readability-identifier-naming)
class list {
   public:
    struct Node {
        T data;
        Node* next;
        Node* prev;
    };

    list() : head(nullptr), tail(nullptr), m_size(0) {}

    ~list() { clear(); }

    list(const list&) = delete;
    auto operator=(const list&) -> list& = delete;

    list(list&& other) noexcept : head(other.head), tail(other.tail), m_size(other.m_size) {
        other.head = nullptr;
        other.tail = nullptr;
        other.m_size = 0;
    }

    auto operator=(list&& other) noexcept -> list& {
        if (this == &other) {
            return *this;
        }
        clear();
        head = other.head;
        tail = other.tail;
        m_size = other.m_size;
        other.head = nullptr;
        other.tail = nullptr;
        other.m_size = 0;
        return *this;
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

        if (head == nullptr) {
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

        if (tail == nullptr) {
            tail = node;
        }

        m_size++;
    }

    auto pop_back() -> T {
        if (tail == nullptr) {
            return T();
        }

        T data = tail->data;
        Node* prev = tail->prev;

        if (prev) {
            prev->next = nullptr;
        }

        delete tail;
        tail = prev;

        if (tail == nullptr) {
            head = nullptr;
        }

        m_size--;

        return data;
    }

    auto pop_front() -> T {
        if (head == nullptr) {
            return T();
        }

        T data = head->data;
        Node* next = head->next;

        if (next) {
            next->prev = nullptr;
        }

        delete head;
        head = next;

        if (head == nullptr) {
            tail = nullptr;
        }

        m_size--;

        return data;
    }

    T front() { return head ? head->data : T(); }

    T back() { return tail ? tail->data : T(); }

    T* begin() { return head ? &head->data : nullptr; }

    T* end() { return tail ? &tail->data : nullptr; }

    void remove(const T& data) {
        Node* current = head;
        while (current) {
            Node* next_node = current->next;  // Save next before potentially deleting current
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
                // Continue to remove ALL occurrences, not just the first
            }
            current = next_node;
        }
    }

    auto size() -> uint64_t { return this->m_size; }

    void clear() {
        while (head) {
            Node* next = head->next;
            delete head;
            head = next;
        }
        tail = nullptr;
        m_size = 0;
    }

    Node* get_head() const { return head; }
    Node* get_tail() const { return tail; }

   private:
    Node* head;
    Node* tail;
    uint64_t m_size;
};
}  // namespace std
