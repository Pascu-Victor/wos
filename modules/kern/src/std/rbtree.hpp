#pragma once

#include <defines/defines.hpp>

namespace std {
template <typename T>
class rbtree {
   public:
    struct Node {
        T data;
        Node* left;
        Node* right;
        Node* parent;
        bool red;
    };

    rbtree() { root = nullptr; }

    void insert(T data) {
        Node* node = new Node();
        node->data = data;
        node->left = nullptr;
        node->right = nullptr;
        node->parent = nullptr;
        node->red = true;

        if (root == nullptr) {
            root = node;
            root->red = false;
            return;
        }

        Node* current = root;
        while (true) {
            if (data < current->data) {
                if (current->left == nullptr) {
                    current->left = node;
                    node->parent = current;
                    break;
                } else {
                    current = current->left;
                }
            } else {
                if (current->right == nullptr) {
                    current->right = node;
                    node->parent = current;
                    break;
                } else {
                    current = current->right;
                }
            }
        }

        fixInsert(node);
    }

    void fixInsert(Node* node) {
        while (node != root && node->parent->red) {
            if (node->parent == node->parent->parent->left) {
                Node* uncle = node->parent->parent->right;
                if (uncle != nullptr && uncle->red) {
                    node->parent->red = false;
                    uncle->red = false;
                    node->parent->parent->red = true;
                    node = node->parent->parent;
                } else {
                    if (node == node->parent->right) {
                        node = node->parent;
                        rotateLeft(node);
                    }
                    node->parent->red = false;
                    node->parent->parent->red = true;
                    rotateRight(node->parent->parent);
                }
            } else {
                Node* uncle = node->parent->parent->left;
                if (uncle != nullptr && uncle->red) {
                    node->parent->red = false;
                    uncle->red = false;
                    node->parent->parent->red = true;
                    node = node->parent->parent;
                } else {
                    if (node == node->parent->left) {
                        node = node->parent;
                        rotateRight(node);
                    }
                    node->parent->red = false;
                    node->parent->parent->red = true;
                    rotateLeft(node->parent->parent);
                }
            }
        }
        root->red = false;
    }

    void rotateLeft(Node* node) {
        Node* right = node->right;
        node->right = right->left;
        if (right->left != nullptr) {
            right->left->parent = node;
        }
        right->parent = node->parent;
        if (node->parent == nullptr) {
            root = right;
        } else if (node == node->parent->left) {
            node->parent->left = right;
        } else {
            node->parent->right = right;
        }
        right->left = node;
        node->parent = right;
    }

    void rotateRight(Node* node) {
        Node* left = node->left;
        node->left = left->right;
        if (left->right != nullptr) {
            left->right->parent = node;
        }
        left->parent = node->parent;
        if (node->parent == nullptr) {
            root = left;
        } else if (node == node->parent->right) {
            node->parent->right = left;
        } else {
            node->parent->left = left;
        }
        left->right = node;
        node->parent = left;
    }

   private:
    Node* root;
};
}  // namespace std
