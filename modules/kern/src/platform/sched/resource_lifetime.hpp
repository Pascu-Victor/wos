#pragma once

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::mod::sched::resource {
// An immutable reference installed before publishing a user thread. Keeping
// the process metadata alive permits IRQ accounting without registry lookup.
// The destructor only drops a reference; scheduler GC owns actual reclamation.
class LifetimeOwner {
   public:
    LifetimeOwner() = default;
    ~LifetimeOwner();
    LifetimeOwner(const LifetimeOwner&) = delete;
    auto operator=(const LifetimeOwner&) -> LifetimeOwner& = delete;
    [[nodiscard]] auto attach(task::Task* owner) -> bool;
    // Retire the CPU contribution independently of the metadata lifetime ref.
    // Thread exit calls this after its final accounting flush; destruction is
    // the fallback for an unpublished thread that never ran.
    void finish_cpu();
    [[nodiscard]] auto cpu_active() const -> bool { return m_cpu_active; }
    auto get() const -> task::Task* { return m_owner; }

   private:
    task::Task* m_owner = nullptr;
    bool m_cpu_active = false;
};
}  // namespace ker::mod::sched::resource
