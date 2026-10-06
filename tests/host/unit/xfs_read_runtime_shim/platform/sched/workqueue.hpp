#pragma once
// This test joins foreground reads. Background writeback is covered by its
// separate actual Workqueue tests; no daemon is launched in this fixture.
namespace ker::mod::sched {
struct WorkItem {
    void (*fn)(void*);
    void* arg;
    WorkItem* next;
};
struct Workqueue {
    static auto create(const char*) -> Workqueue* { return nullptr; }
    auto enqueue(WorkItem*) -> bool { return false; }
};
}  // namespace ker::mod::sched
