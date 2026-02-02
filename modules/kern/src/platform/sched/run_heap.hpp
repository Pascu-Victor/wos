#pragma once

#include <cstdint>

namespace ker::mod::sched {

namespace task {
struct Task;  // Forward declaration — full definition in task.hpp
}

// Fixed-capacity binary min-heap of Task*, keyed on Task::vdeadline.
// Zero allocations — the array is inline in the struct.
// Each Task stores its heapIndex for O(log n) removal without scanning.
static constexpr uint32_t PER_CPU_HEAP_CAP = 8192;

struct RunHeap {
    task::Task* entries[PER_CPU_HEAP_CAP];
    uint32_t size;

    void init();

    // Insert task into min-heap by vdeadline. O(log n). No allocations.
    // Returns false if heap is full.
    bool insert(task::Task* t);

    // Find the eligible task with the smallest vdeadline.
    // Eligible = (avgVruntime - task.vruntime) >= 0.
    // Does NOT remove it from the heap.
    // Returns nullptr if heap is empty.
    task::Task* pickBestEligible(int64_t avgVruntime);

    // Remove a specific task using its heapIndex. O(log n).
    // Returns false if task is not in this heap.
    bool remove(task::Task* t);

    // Update a task's position after its vdeadline changed. O(log n).
    void update(task::Task* t);

    // Peek at the task with the smallest vdeadline. O(1).
    task::Task* peekMin() const;

    // Check if task is in this heap.
    bool contains(task::Task* t) const;

   private:
    void siftUp(uint32_t idx);
    void siftDown(uint32_t idx);
    void swapEntries(uint32_t i, uint32_t j);
};

// Intrusive singly-linked list for wait queue and dead list.
// Uses Task::schedNext pointer — zero allocations.
// No ordering, just a bag of parked tasks.
struct IntrusiveTaskList {
    task::Task* head;
    uint32_t count;

    void init();

    // Prepend task to list. O(1).
    void push(task::Task* t);

    // Remove a specific task. O(n) scan.
    // Returns false if task was not found.
    bool remove(task::Task* t);

    // Find task by PID. O(n) scan.
    // Returns nullptr if not found.
    task::Task* findByPid(uint64_t pid);

    // Pop the head task. O(1).
    // Returns nullptr if empty.
    task::Task* pop();
};

}  // namespace ker::mod::sched
