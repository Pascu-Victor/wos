#pragma once
#include <object_pages.hpp>
namespace ker::syscall::vmem {
class SharedVmemPublicationGuard {
   public:
    SharedVmemPublicationGuard() : owner(std::this_thread::get_id()) {
        object_fixture::check(object_fixture::pub_depth == 0 && object_fixture::irq_depth == 0);
        object_fixture::pub_mutex.lock();
        ++object_fixture::pub_depth;
    }
    ~SharedVmemPublicationGuard() {
        object_fixture::check(acquired_by_current_executor());
        --object_fixture::pub_depth;
        object_fixture::pub_mutex.unlock();
    }
    auto acquired_by_current_executor() const noexcept -> bool {
        return owner == std::this_thread::get_id() && object_fixture::pub_depth != 0;
    }

   private:
    std::thread::id owner;
};
}  // namespace ker::syscall::vmem
