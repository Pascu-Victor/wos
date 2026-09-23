#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

namespace ker::dev::pci {
struct PCIDevice;
}
namespace ker::dev::dma {

// Four virtio NICs with four 256-descriptor RX/TX pairs need 8192 slots,
// plus rings and other PCI drivers. Metadata stays bounded; handles remain O(1).
constexpr size_t REGISTRY_CAPACITY = 16384;

enum class Direction : uint8_t { to_device, from_device, bidirectional };
enum class Error : uint8_t {
    none,
    invalid_argument,
    invalid_context,
    unavailable_device,
    exhausted,
    allocation_failed,
    address_limit,
    mapping_failed,
    invalid_handle,
    busy,
    invalid_completion,
    quarantined
};
// These are driver assertions, not observations made by the DMA subsystem.
// Both require that no IRQ/worker reader can still use this submission.
enum class Retirement : uint8_t { never_submitted, command_completed, device_quiesced };

struct Handle {
    const void* registry = nullptr;
    const pci::PCIDevice* device = nullptr;
    uint64_t generation = 0;
    size_t slot = SIZE_MAX;
};
struct Allocation {
    Handle handle{};
    // Owned CPU storage for coherent allocations/unbound streaming pools;
    // null for map_streaming. Never free it or access a device-owned stream.
    std::byte* cpu = nullptr;
    size_t size = 0;
};
struct AllocationResult {
    Error error = Error::none;
    Allocation allocation{};
};
struct Submission {
    Handle handle{};
    uint64_t sequence = 0;
    uint64_t address = 0;
    size_t size = 0;
};
struct SubmissionResult {
    Error error = Error::none;
    Submission submission{};
};
struct Stats {
    size_t regions = 0;
    size_t backing_pages = 0;
    size_t streaming = 0;
    size_t device_owned = 0;
    size_t quarantined = 0;
};

// A retained registry observation, not proof of a live hardware translation.
struct MappingInfo {
    Handle handle{};
    uint64_t physical = 0, address = 0;
    size_t bytes = 0;
    Direction direction = Direction::bidirectional;
    bool quarantined = false, release_pending = false;
};
// Boot/task diagnostics only. Does not extend ownership or authorize CPU/DMA access.
[[nodiscard]] auto mapping_info(const pci::PCIDevice* device, uint64_t address) -> MappingInfo;

// Boot/task context only. Fallible; may wait for allocation and unit admission.
// Domain policy selects explicit passthrough or page-scoped VT-d translation.
// A narrow mask can fail even if unrelated higher memory is available.
// Coherence assumes ordinary snooped x86 PCI DMA configured by the driver.
[[nodiscard]] auto allocate_coherent(const pci::PCIDevice* device, size_t bytes, size_t alignment = 4096, uint64_t mask = UINT64_MAX)
    -> AllocationResult;
// The caller keeps this kernel CPU span alive through completion/quarantine. DMA
// touches only exclusive bounce pages. No user pointer or direct partial-page
// map is accepted. Completion consumes the binding; bind again before reuse.
[[nodiscard]] auto map_streaming(const pci::PCIDevice* device, std::span<std::byte> buffer, Direction direction, uint64_t mask = UINT64_MAX)
    -> AllocationResult;
// Probe/task allocation of a reusable pool. It starts unbound. The requested
// direction bounds all later bindings; bidirectional permits either direction.
[[nodiscard]] auto allocate_streaming(const pci::PCIDevice* device, size_t capacity, Direction direction, uint64_t mask = UINT64_MAX)
    -> AllocationResult;
// Allocation-free, IRQ-safe binding while CPU-owned. Buffer must fit original
// capacity. It may be the pool's own CPU view (for bounded usercopy staging).
[[nodiscard]] auto bind_streaming(Handle handle, std::span<std::byte> buffer, Direction direction) -> Error;
// Copy/synchronize before making the returned address visible to hardware.
// Metadata is IRQ-safe and work is bounded by the already allocated region.
// The driver serializes CPU accesses with this operation and hardware use.
[[nodiscard]] auto prepare(Handle handle) -> SubmissionResult;
// Call only AFTER proving hardware retirement AND draining completion readers.
// never_submitted asserts the command was never made eligible for device access.
// Otherwise coherent rings require device_quiesced. For streaming receives, copy only
// completed_bytes back to the borrowed CPU span; its remainder stays untouched.
// Completion itself does not unmap or free, allowing preallocated IRQ/NAPI pools.
[[nodiscard]] auto complete(Submission submission, size_t completed_bytes, Retirement proof) -> Error;
// Boot/task context only. Release never-published or completed storage. Device-
// owned/busy/quarantined storage is refused. Invalidation precedes physical free.
[[nodiscard]] auto release(Handle handle) -> Error;
// Pin uncertain storage permanently; never infer retirement from a timeout.
// Busy means an operation owns the record: retry from a deferred safe context.
[[nodiscard]] auto quarantine(Handle handle) -> Error;
// Null selects the global totals; a device selects only its owned regions.
[[nodiscard]] auto stats(const pci::PCIDevice* device = nullptr) -> Stats;

}  // namespace ker::dev::dma
