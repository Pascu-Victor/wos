#pragma once

#include <cstdint>
#include <platform/mm/numa_policy.hpp>
#include <platform/mm/phys.hpp>

namespace ker::mod::mm::numa {

struct AllocationContext {
    Policy policy{};
    NodeId local_node{INVALID_NODE};
    uint64_t virtual_page_index{};
};
enum class ReclaimMode : uint8_t { NEVER, LEGACY_UMA };
struct AllocationResult;

// Exclusive references to independently freeable order-0 frames. The mapping
// installer must release ownership only after its successful publication.
class OwnedSpan {
   public:
    OwnedSpan() = default;
    ~OwnedSpan();
    OwnedSpan(const OwnedSpan&) = delete;
    auto operator=(const OwnedSpan&) -> OwnedSpan& = delete;
    OwnedSpan(OwnedSpan&& other) noexcept;
    auto operator=(OwnedSpan&& other) noexcept -> OwnedSpan&;
    [[nodiscard]] auto data() const noexcept -> void* { return data_; }
    [[nodiscard]] auto pages() const noexcept -> uint32_t { return pages_; }
    [[nodiscard]] auto empty() const noexcept -> bool { return data_ == nullptr; }
    [[nodiscard]] auto release() noexcept -> void*;
    void reset() noexcept;

   private:
    OwnedSpan(void* data, uint32_t pages) noexcept : data_(data), pages_(pages) {}
    friend auto try_allocate_pages(const AllocationContext&, uint32_t, uint64_t, PhysicalPageOwner, phys::PageInitialization, ReclaimMode,
                                   const char*) -> AllocationResult;
    void* data_{};
    uint32_t pages_{};
};

enum class AllocationError : uint8_t {
    NONE,
    NO_TOPOLOGY,
    INVALID_ARGUMENT,
    INVALID_POLICY,
    INVALID_LOCAL_NODE,
    INVALID_PLACEMENT,
    EXHAUSTED
};

// Capture from an already copied policy before publication/MM locks. No Task
// or root lifetime travels here. Output must already be addressable; failure
// leaves it unchanged. Success sets virtual_page_index to zero for the caller
// to replace with each destination VA's page index.
[[nodiscard]] auto capture_allocation_context(Policy copied_policy, AllocationContext& output) noexcept -> AllocationError;

struct AllocationResult {
    OwnedSpan span{};
    NodeId requested{INVALID_NODE};
    NodeId actual{INVALID_NODE};
    AllocationError error{AllocationError::NO_TOPOLOGY};
    [[nodiscard]] auto used_fallback() const noexcept -> bool { return !span.empty() && actual != requested; }
};

// NEVER and selected-node attempts do not wait. Exact UMA LOCAL/order-0
// LEGACY_UMA delegates to the existing reclaim helpers and their context rules.
// Caller supplies a pre-captured CPU/policy and owns quota/reserve admission.
// Only one-page and 512-page spans are supported.
// No user mapping or task policy is exposed here.
// Default UMA/order-0 requests retain the existing cache/initialization path.
[[nodiscard]] auto try_allocate_pages(const AllocationContext& context, uint32_t pages, uint64_t alignment_bytes, PhysicalPageOwner owner,
                                      phys::PageInitialization initialization = phys::PageInitialization::ZERO,
                                      ReclaimMode reclaim = ReclaimMode::NEVER, const char* name = nullptr) -> AllocationResult;

// Excluded callers must start the lifetime of an empty, addressable output
// before entry. Neither output nor its storage may alias context. This adapter
// keeps nontrivial factory return temporaries in an instrumented caller frame.
void try_allocate_pages_into(AllocationResult& output, const AllocationContext& context, uint32_t pages, uint64_t alignment_bytes,
                             PhysicalPageOwner owner, phys::PageInitialization initialization, ReclaimMode reclaim,
                             const char* name = nullptr);

// Immutable managed-affinity lookup; INVALID_NODE for holes/nonmanaged frames.
[[nodiscard]] auto physical_node(uint64_t physical_address) noexcept -> NodeId;

}  // namespace ker::mod::mm::numa
