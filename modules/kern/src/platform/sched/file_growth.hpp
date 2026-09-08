#pragma once

#include <cstddef>
#include <cstdint>

namespace ker::mod::sched::task {
struct Task;
}

namespace ker::mod::sched::resource {
// Capture before the inode lock; destroy after it. Holds no retained reference
// or reservation across filesystem I/O (fatal exits do not unwind the stack).
class FileGrowth {
   public:
    FileGrowth();
    explicit FileGrowth(task::Task* caller);
    ~FileGrowth();
    FileGrowth(const FileGrowth&) = delete;
    auto operator=(const FileGrowth&) -> FileGrowth& = delete;

    [[nodiscard]] auto bound_write(uint64_t offset, size_t& count) -> bool;
    [[nodiscard]] auto allow_resize(uint64_t length) -> bool;
    void committed(uint64_t file_size);

   private:
    task::Task* m_caller;
    uint64_t m_limit;
    uint64_t m_size = 0;
    bool m_committed = false;
    bool m_denied = false;
    bool m_signal = false;
};
}  // namespace ker::mod::sched::resource
