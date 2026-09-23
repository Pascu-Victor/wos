#include "dma.hpp"

#include <cstring>
#include <dev/dma/manager.hpp>
#include <dev/iommu/domains.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sys/spinlock.hpp>

namespace ker::dev::dma {
namespace {
// x86 PCI coherent memory backend. ECAP.C describes IOMMU page-table walks,
// not PCI data-buffer cache coherence. Translation tables have separate cache
// publication in iommu/. Callers must configure ordinary snooped PCI DMA;
// device-specific No Snoop/noncoherent operation is not supported or detected
// here. Driver conversion must establish that platform contract before use.
struct Backend {
    mod::sys::Spinlock registry_lock{};
    auto lock() -> uint64_t { return registry_lock.lock_irqsave(); }
    void unlock(uint64_t flags) { registry_lock.unlock_irqrestore(flags); }
    auto allocation_context() -> bool {
        // Driver probes/KTEST run after queue setup but before a current task
        // exists. During that BSP boot phase the caller must ensure ordinary
        // init context; there is no task preemption depth to inspect yet.
        return !mod::sched::can_query_current_task() || mod::mm::phys::can_wait_for_reclaim();
    }
    auto admit(const pci::PCIDevice* device) -> bool {
        auto const MODE = iommu::domain_info(device).mode;
        return MODE == iommu::DomainMode::software_passthrough || MODE == iommu::DomainMode::hardware_passthrough ||
               MODE == iommu::DomainMode::translated;
    }
    auto allocate(size_t bytes) -> detail::Storage {
        auto* cpu =
            static_cast<std::byte*>(mod::mm::phys::page_alloc_may_fail(mod::mm::PhysicalPageOwner::DMA_BUFFER, bytes, "dma_buffer"));
        return {cpu, cpu == nullptr ? 0 : reinterpret_cast<uint64_t>(cpu) - mod::mm::addr::get_hhdm_offset()};
    }
    void free(detail::Storage storage, size_t) { mod::mm::phys::page_free(storage.cpu); }
    static auto access(Direction direction) -> iommu::vtd::Access {
        return direction == Direction::to_device     ? iommu::vtd::Access::read
               : direction == Direction::from_device ? iommu::vtd::Access::write
                                                     : iommu::vtd::Access::read_write;
    }
    auto map(const pci::PCIDevice* device, uint64_t physical, size_t bytes, size_t, uint64_t mask, Direction direction) -> detail::Mapping {
        if (!detail::extent_fits(physical, bytes, mask)) return {.error = Error::address_limit};
        auto const MODE = iommu::domain_info(device).mode;
        if (MODE == iommu::DomainMode::software_passthrough || MODE == iommu::DomainMode::hardware_passthrough)
            return {.address = physical};
        if (MODE != iommu::DomainMode::translated) return {.error = Error::unavailable_device};
        auto const RESULT = iommu::map(device, physical, bytes, access(direction));
        return {.error = RESULT.error == iommu::vtd::TableError::none ? Error::none : Error::mapping_failed,
                .address = physical,
                .quarantine = RESULT.retain_data};
    }
    auto unmap(const pci::PCIDevice* device, uint64_t address, uint64_t physical, size_t bytes, Direction direction) -> bool {
        auto const MODE = iommu::domain_info(device).mode;
        if (MODE == iommu::DomainMode::translated) return iommu::unmap(device, address, physical, bytes, access(direction));
        if (MODE != iommu::DomainMode::software_passthrough && MODE != iommu::DomainMode::hardware_passthrough) return false;
        // Passthrough has no translation. The caller's hardware retirement and
        // CPU/device ordering still precede returning backing to the allocator.
        asm volatile("mfence" ::: "memory");
        return true;
    }
    auto kernel_buffer(std::span<std::byte> buffer) -> bool {
        auto const ADDRESS = reinterpret_cast<uint64_t>(buffer.data());
        return ADDRESS >= mod::mm::addr::get_hhdm_offset() && detail::extent_fits(ADDRESS, buffer.size(), UINT64_MAX);
    }
    void copy(std::byte* destination, const std::byte* source, size_t bytes) { std::memcpy(destination, source, bytes); }
    void for_device(std::byte*, size_t, Direction) { asm volatile("mfence" ::: "memory"); }
    void for_cpu(std::byte*, size_t, Direction) { asm volatile("mfence" ::: "memory"); }
};
constinit Backend backend{};
constinit detail::Manager<Backend, REGISTRY_CAPACITY> manager{backend};
static_assert(sizeof(manager) < 2 * 1024 * 1024, "DMA registry metadata footprint must stay below 2 MiB");
}  // namespace

auto allocate_coherent(const pci::PCIDevice* device, size_t bytes, size_t alignment, uint64_t mask) -> AllocationResult {
    auto hold = mod::sched::hold_kernel_shutdown();
    return manager.allocate_coherent(device, bytes, alignment, mask);
}
auto map_streaming(const pci::PCIDevice* device, std::span<std::byte> buffer, Direction direction, uint64_t mask) -> AllocationResult {
    auto hold = mod::sched::hold_kernel_shutdown();
    return manager.map_streaming(device, buffer, direction, mask);
}
auto allocate_streaming(const pci::PCIDevice* device, size_t capacity, Direction direction, uint64_t mask) -> AllocationResult {
    auto hold = mod::sched::hold_kernel_shutdown();
    return manager.allocate_streaming(device, capacity, direction, mask);
}
auto bind_streaming(Handle handle, std::span<std::byte> buffer, Direction direction) -> Error {
    return manager.bind_streaming(handle, buffer, direction);
}
auto prepare(Handle handle) -> SubmissionResult { return manager.prepare(handle); }
auto complete(Submission submission, size_t completed_bytes, Retirement proof) -> Error {
    return manager.complete(submission, completed_bytes, proof);
}
auto release(Handle handle) -> Error {
    auto hold = mod::sched::hold_kernel_shutdown();
    return manager.release(handle);
}
auto quarantine(Handle handle) -> Error { return manager.quarantine(handle); }
auto mapping_info(const pci::PCIDevice* device, uint64_t address) -> MappingInfo { return manager.mapping_info(device, address); }
auto stats(const pci::PCIDevice* device) -> Stats { return manager.stats(device); }
}  // namespace ker::dev::dma
