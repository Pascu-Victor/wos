#include <cerrno>
#include <memory>
#include <new>
#include <platform/mm/device_mapping_revoke.hpp>
#include <platform/mm/lifetime_retirement_executor.hpp>
#include <platform/mm/phys.hpp>
#include <platform/mm/virt.hpp>
#include <platform/sys/usercopy.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>

namespace ker::mod::mm::device_vm {

auto revoke_cpu_mappings(const ker::dev::mapping::ObjectRef& owner, Limits limits) -> int {
    namespace fm = file_mapping;
    if (!owner) return -EINVAL;
    if (!phys::can_wait_for_reclaim() || sys::usercopy::access_window_active() || fm::LifetimeRetirementExecutor::is_current_worker())
        return -EWOULDBLOCK;
    {
        ker::syscall::vmem::SharedVmemPublicationGuard publication;
        if (!owner.get()->begin_revoke(publication)) {
            return owner.get()->state() == ker::dev::mapping::ObjectState::revoked ? 0 : -EWOULDBLOCK;
        }
    }
    size_t capacity = 0;
    if (!fm::DeviceBindingRegistry::count(owner, capacity)) return -EAGAIN;
    if (capacity > limits.max_bindings || capacity > SIZE_MAX / sizeof(fm::FileBindingSnapshot)) return -E2BIG;
    std::unique_ptr<fm::FileBindingSnapshot[]> snapshots;
    if (capacity != 0) {
        snapshots.reset(new (std::nothrow) fm::FileBindingSnapshot[capacity]);
        if (!snapshots) return -ENOMEM;
    }
    size_t acquired = 0, pending = 0;
    if (!fm::DeviceBindingRegistry::snapshot(owner, snapshots.get(), capacity, acquired, pending)) return -EAGAIN;
    bool waiting = pending != 0;
    for (size_t i = 0; i < acquired; ++i) {
        auto& snapshot = snapshots[i];
        if (snapshot.retirement_state() == fm::BindingRetirementState::INDEXED) {
            PreparedRevoke withdrawal;
            int const PREPARED = withdrawal.prepare(snapshot, limits);
            if (PREPARED != 0) return PREPARED;
            {
                ker::syscall::vmem::SharedVmemPublicationGuard publication;
                virt::BindingRootMutationLease lease;
                auto const ADMISSION = virt::try_acquire_binding_root(snapshot, publication, lease);
                switch (ADMISSION) {
                    case virt::BindingRootAdmission::Acquired: {
                        int const RESULT = withdrawal.commit(lease, publication);
                        if (RESULT != 0) return RESULT;
                        break;
                    }
                    case virt::BindingRootAdmission::StaleRoot:
                    case virt::BindingRootAdmission::NotIndexed:
                    case virt::BindingRootAdmission::Busy:
                        break;  // Inspect the actual retirement state below.
                    case virt::BindingRootAdmission::Context:
                        return -EWOULDBLOCK;
                    default:
                        return -EINVAL;
                }
            }
            // Ordinary context, after the real TLB barrier and root lease.
            withdrawal.reset();
        }
        if (snapshot.retirement_state() != fm::BindingRetirementState::TLB_COMPLETE) waiting = true;
        snapshot.reset();
    }
    snapshots.reset();
    if (waiting) return -EAGAIN;
    // A family callback may still retain the Object after its last binding is
    // unlinked. Actual role counts, rather than an empty list, decide drain.
    ker::syscall::vmem::SharedVmemPublicationGuard publication;
    if (owner.get()->try_finish_revoke(publication)) return 0;
    return -EAGAIN;
}

}  // namespace ker::mod::mm::device_vm
