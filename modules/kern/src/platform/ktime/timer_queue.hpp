#pragma once

#include <cstddef>
#include <cstdint>
#include <platform/ktime/arithmetic.hpp>
#include <span>

namespace ker::mod::time {

class TimerQueue;

enum class TimerState : uint8_t { IDLE, ARMED, READY, RUNNING };
enum class TimerCancel : uint8_t { INACTIVE, CANCELLED, RUNNING, WRONG_QUEUE };
enum class TimerDispatch : uint8_t { DEFERRED, SYNCHRONOUS };

// Intrusive storage: retain the node AND callback context until all extracted
// tokens have been consumed. Cancellation does not make outstanding tokens safe
// to dereference after freeing the node. All access uses the owning queue lock.
struct TimerNode {
    TimerNode() = default;
    TimerNode(const TimerNode&) = delete;
    auto operator=(const TimerNode&) -> TimerNode& = delete;

    [[nodiscard]] auto state() const -> TimerState { return state_; }
    [[nodiscard]] auto generation() const -> uint64_t { return generation_; }
    [[nodiscard]] auto deadline() const -> uint64_t { return deadline_; }

   private:
    friend class TimerQueue;
    TimerNode* left_{};
    TimerNode* right_{};
    TimerQueue* owner_{};
    uint64_t deadline_{};
    uint64_t period_{};
    uint64_t generation_{};
    void (*callback_)(void*){};
    void* context_{};
    int height_{1};
    TimerState state_{TimerState::IDLE};
    bool cancel_running_{};
};

struct TimerToken {
    TimerNode* node{};
    uint64_t generation{};
};

struct TimerInvocation {
    void (*callback)(void*){};
    void* context{};
};

// Externally serialized, allocation-free AVL deadline queue. The caller holds
// one IRQ-safe lock for EVERY public operation and node inspection. Queue and
// node addresses must remain stable. Callbacks run only in task context, outside
// the lock, between successful claim() and finish(). No public operation invokes
// a callback. Deferred association is permanent to prevent cross-lock ABA;
// synchronous queues cannot produce tokens and release association on removal.
class TimerQueue {
   public:
    explicit constexpr TimerQueue(TimerDispatch dispatch = TimerDispatch::DEFERRED) : dispatch_(dispatch) {}
    TimerQueue(const TimerQueue&) = delete;
    auto operator=(const TimerQueue&) -> TimerQueue& = delete;

    [[nodiscard]] auto size() const -> size_t { return size_; }
    [[nodiscard]] auto tree_height() const -> int { return height(root_); }
    [[nodiscard]] auto next_deadline(uint64_t& out) const -> bool {
        if (first_ == nullptr) return false;
        out = first_->deadline_;
        return true;
    }

    // Rearming READY invalidates its token. RUNNING must first finish. Failed
    // arms leave existing state unchanged, including at generation exhaustion.
    [[nodiscard]] auto arm_at(TimerNode& node, uint64_t deadline, void (*callback)(void*), void* context = nullptr, uint64_t period = 0)
        -> bool {
        if (callback == nullptr || (node.owner_ != nullptr && node.owner_ != this) || node.state_ == TimerState::RUNNING ||
            node.generation_ == UINT64_MAX)
            return false;
        if (node.state_ == TimerState::ARMED) remove(node);
        node.owner_ = this;
        ++node.generation_;
        node.deadline_ = deadline;
        node.period_ = period;
        node.callback_ = callback;
        node.context_ = context;
        node.cancel_running_ = false;
        insert(node);
        return true;
    }

    [[nodiscard]] auto arm_after(TimerNode& node, uint64_t now, uint64_t delay, void (*callback)(void*), void* context = nullptr,
                                 uint64_t period = 0) -> bool {
        return arm_at(node, arithmetic::saturating_add(now, delay), callback, context, period);
    }

    [[nodiscard]] auto cancel(TimerNode& node) -> TimerCancel {
        if (node.owner_ != nullptr && node.owner_ != this) return TimerCancel::WRONG_QUEUE;
        if (node.state_ == TimerState::IDLE) return TimerCancel::INACTIVE;
        if (node.state_ == TimerState::RUNNING) {
            node.cancel_running_ = true;
            return TimerCancel::RUNNING;
        }
        if (node.state_ == TimerState::ARMED) remove(node);
        node.state_ = TimerState::IDLE;
        if (dispatch_ == TimerDispatch::SYNCHRONOUS) node.owner_ = nullptr;
        return TimerCancel::CANCELLED;
    }

    // The span is both the IRQ work budget and available deferred transport
    // capacity. Entries beyond the budget remain armed; callers must schedule
    // another extraction while next_deadline <= now. Never discard tokens.
    [[nodiscard]] auto extract_due(uint64_t now, std::span<TimerToken> output) -> size_t {
        if (dispatch_ == TimerDispatch::SYNCHRONOUS) return 0;
        size_t count = 0;
        while (count < output.size() && first_ != nullptr && first_->deadline_ <= now) {
            auto& node = *first_;
            remove(node);
            node.state_ = TimerState::READY;
            output[count++] = TimerToken{&node, node.generation_};
        }
        return count;
    }

    // Synchronous queues never issue generation tokens. The caller consumes the
    // entry under its ownership lock; removal releases association so migration
    // can arm the embedded node on another queue. No callback is invoked here.
    [[nodiscard]] auto pop_due(uint64_t now, TimerInvocation& out) -> bool {
        out = {};
        if (dispatch_ != TimerDispatch::SYNCHRONOUS || first_ == nullptr || first_->deadline_ > now) return false;
        auto& node = *first_;
        out = {node.callback_, node.context_};
        remove(node);
        node.state_ = TimerState::IDLE;
        node.owner_ = nullptr;
        return true;
    }

    [[nodiscard]] auto claim(TimerToken token, TimerInvocation& out) -> bool {
        out = {};
        if (!matches(token, TimerState::READY)) return false;
        token.node->state_ = TimerState::RUNNING;
        out = {token.node->callback_, token.node->context_};
        return true;
    }

    // Returns true only when this exact running generation completed. If a
    // periodic callback finishes late, skip missed periods without phase drift.
    [[nodiscard]] auto finish(TimerToken token, uint64_t now) -> bool {
        if (!matches(token, TimerState::RUNNING)) return false;
        auto& node = *token.node;
        node.state_ = TimerState::IDLE;
        if (!node.cancel_running_ && node.period_ != 0 && node.generation_ != UINT64_MAX) {
            uint64_t const NEXT = arithmetic::periodic_next(node.deadline_, node.period_, now);
            if (NEXT > now && NEXT > node.deadline_) {
                node.deadline_ = NEXT;
                ++node.generation_;
                insert(node);
            }
        }
        return true;
    }

   private:
    TimerNode* root_{};
    TimerNode* first_{};
    size_t size_{};
    const TimerDispatch dispatch_;

    [[nodiscard]] auto matches(TimerToken token, TimerState state) const -> bool {
        return token.node != nullptr && token.node->owner_ == this && token.node->generation_ == token.generation &&
               token.node->state_ == state;
    }

    static auto height(const TimerNode* node) -> int { return node == nullptr ? 0 : node->height_; }
    static void update_height(TimerNode& node) {
        int const LEFT = height(node.left_);
        int const RIGHT = height(node.right_);
        node.height_ = 1 + (LEFT > RIGHT ? LEFT : RIGHT);
    }
    static auto before(const TimerNode& lhs, const TimerNode& rhs) -> bool {
        if (lhs.deadline_ != rhs.deadline_) return lhs.deadline_ < rhs.deadline_;
        // Numeric addresses provide a stable tie break without unrelated-object
        // relational pointer comparison. Addresses are never dereferenced here.
        return reinterpret_cast<uintptr_t>(&lhs) < reinterpret_cast<uintptr_t>(&rhs);
    }
    static auto rotate_left(TimerNode* node) -> TimerNode* {
        auto* top = node->right_;
        node->right_ = top->left_;
        top->left_ = node;
        update_height(*node);
        update_height(*top);
        return top;
    }
    static auto rotate_right(TimerNode* node) -> TimerNode* {
        auto* top = node->left_;
        node->left_ = top->right_;
        top->right_ = node;
        update_height(*node);
        update_height(*top);
        return top;
    }
    static auto balance(TimerNode* node) -> TimerNode* {
        if (node == nullptr) return nullptr;
        update_height(*node);
        int const DELTA = height(node->left_) - height(node->right_);
        if (DELTA > 1) {
            if (height(node->left_->left_) < height(node->left_->right_)) node->left_ = rotate_left(node->left_);
            return rotate_right(node);
        }
        if (DELTA < -1) {
            if (height(node->right_->right_) < height(node->right_->left_)) node->right_ = rotate_right(node->right_);
            return rotate_left(node);
        }
        return node;
    }
    static auto insert_into(TimerNode* root, TimerNode& node) -> TimerNode* {
        if (root == nullptr) return &node;
        if (before(node, *root))
            root->left_ = insert_into(root->left_, node);
        else
            root->right_ = insert_into(root->right_, node);
        return balance(root);
    }
    static auto take_first(TimerNode* root, TimerNode*& first) -> TimerNode* {
        if (root->left_ == nullptr) {
            first = root;
            return root->right_;
        }
        root->left_ = take_first(root->left_, first);
        return balance(root);
    }
    static auto erase_from(TimerNode* root, TimerNode& node) -> TimerNode* {
        if (root == &node) {
            if (root->right_ == nullptr) return root->left_;
            TimerNode* replacement = nullptr;
            auto* right = take_first(root->right_, replacement);
            replacement->left_ = root->left_;
            replacement->right_ = right;
            return balance(replacement);
        }
        if (before(node, *root))
            root->left_ = erase_from(root->left_, node);
        else
            root->right_ = erase_from(root->right_, node);
        return balance(root);
    }
    void insert(TimerNode& node) {
        node.left_ = node.right_ = nullptr;
        node.height_ = 1;
        root_ = insert_into(root_, node);
        if (first_ == nullptr || before(node, *first_)) first_ = &node;
        node.state_ = TimerState::ARMED;
        ++size_;
    }
    void remove(TimerNode& node) {
        root_ = erase_from(root_, node);
        --size_;
        node.left_ = node.right_ = nullptr;
        node.height_ = 1;
        first_ = root_;
        if (first_ != nullptr) {
            while (first_->left_ != nullptr) first_ = first_->left_;
        }
    }
};

}  // namespace ker::mod::time
