#pragma once

#include <cstdint>
#include <optional>

namespace ker::dev::pci {

struct PCIDevice;

enum class DriverKind : uint8_t { diagnostic, ahci, e1000e, virtio_net, xhci, ivshmem };
enum class DriverPhase : uint8_t { unclaimed, open, closed, retained };

struct DriverStatus {
    DriverPhase phase = DriverPhase::unclaimed;
    DriverKind driver = DriverKind::diagnostic;
    uint64_t epoch = 0;
    uint32_t active_operations = 0;
};

// A metadata claim is exclusive until reboot. Closing, dropping or retaining it
// never proves hardware quiescence and never releases BARs, IRQs or DMA backing.
class DriverClaim {
   public:
    DriverClaim() = default;
    ~DriverClaim();
    DriverClaim(const DriverClaim&) = delete;
    auto operator=(const DriverClaim&) -> DriverClaim& = delete;
    DriverClaim(DriverClaim&& other) noexcept;
    auto operator=(DriverClaim&& other) noexcept -> DriverClaim&;

    [[nodiscard]] auto device() const -> PCIDevice* { return device_; }
    [[nodiscard]] auto epoch() const -> uint64_t { return epoch_; }
    [[nodiscard]] explicit operator bool() const { return device_ != nullptr; }
    // Bounded config serialization, no allocation or hardware access. Terminal
    // callers drain outside locks; no later operation can reopen the gate.
    void close_data_admission();
    [[nodiscard]] auto operations_drained() const -> bool;
    // Close and abandon the handle while keeping its slot/epoch pinned.
    void retain();

   private:
    PCIDevice* device_ = nullptr;
    uint64_t epoch_ = 0;
    DriverClaim(PCIDevice* device, uint64_t epoch) : device_(device), epoch_(epoch) {}
    friend auto try_claim_driver(PCIDevice*, DriverKind) -> std::optional<DriverClaim>;
};

// IRQ-safe counted operation. No allocator, config lock or hardware access on
// entry/release. A successful guard covers preparation/publication or complete
// allocation/map rollback. It does not cover the hardware submission lifetime;
// existing callback, IRQ and DMA retirement proofs remain necessary.
class DriverOperation {
   public:
    DriverOperation() = default;
    ~DriverOperation();
    DriverOperation(const DriverOperation&) = delete;
    auto operator=(const DriverOperation&) -> DriverOperation& = delete;
    DriverOperation(DriverOperation&& other) noexcept;
    auto operator=(DriverOperation&& other) noexcept -> DriverOperation&;
    [[nodiscard]] explicit operator bool() const { return device_ != nullptr; }

   private:
    const PCIDevice* device_ = nullptr;
    explicit DriverOperation(const PCIDevice* device) : device_(device) {}
    void release();
    friend auto try_dma_operation(const PCIDevice*) -> DriverOperation;
};

[[nodiscard]] auto try_claim_driver(PCIDevice*, DriverKind) -> std::optional<DriverClaim>;
// Allocation context only: may reserve an anonymous diagnostic claim before
// entering the gate. Call outside the DMA registry lock.
[[nodiscard]] auto begin_dma_allocation(const PCIDevice*) -> DriverOperation;
[[nodiscard]] auto try_dma_operation(const PCIDevice*) -> DriverOperation;
[[nodiscard]] auto driver_dma_admission_allowed(const PCIDevice*) -> bool;
// Invalid/copied/interior pointers return nullopt without being dereferenced.
[[nodiscard]] auto driver_claim_status(const PCIDevice*) -> std::optional<DriverStatus>;

}  // namespace ker::dev::pci
