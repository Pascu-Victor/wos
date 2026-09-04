#pragma once

#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <dev/block_device.hpp>

namespace ker::test {

enum class FaultBlockOperation : uint8_t {
    READ,
    WRITE,
    FLUSH,
};

enum class FaultBlockWriteMode : uint8_t {
    FAIL_BEFORE_IO,
    TORN_PREFIX,
};

struct FaultBlockEvent {
    FaultBlockOperation operation{};
    uint64_t scenario_seed{};
    uint64_t global_index{};
    uint64_t operation_index{};
    uint64_t block{};
    size_t count{};
    size_t transferred_bytes{};
    uint64_t durability_generation{};
    int result{};
};

// Test-only block device with a bounded controller-cache model. Writes update
// volatile media, while only a successful flush advances durable media. The
// callbacks allocate nothing and every injected fault names an exact one-based
// operation index, making a recorded failure directly replayable.
class FaultBlockDevice final {
   public:
    static constexpr size_t MAX_MEDIA_BYTES = 256 * 1024;
    static constexpr size_t MAX_HISTORY = 512;

    FaultBlockDevice(size_t block_size, uint64_t total_blocks, uint8_t initial_value = 0);

    FaultBlockDevice(const FaultBlockDevice&) = delete;
    FaultBlockDevice(FaultBlockDevice&&) = delete;
    auto operator=(const FaultBlockDevice&) -> FaultBlockDevice& = delete;
    auto operator=(FaultBlockDevice&&) -> FaultBlockDevice& = delete;

    auto valid() const -> bool { return valid_; }
    auto device() -> dev::BlockDevice* { return &device_; }
    auto device() const -> const dev::BlockDevice* { return &device_; }
    auto media_bytes() const -> size_t { return media_bytes_; }

    // Replace both volatile and durable media and reset all counters/history.
    void reset(uint8_t value = 0);

    // Seed a byte range in both media images without creating an I/O event.
    auto seed_bytes(size_t offset, const void* source, size_t length) -> bool;

    // Discard every unflushed byte, as a controller reset/power loss would.
    // History is retained so the cut can be diagnosed by its preceding event.
    void power_cut();

    void clear_history();
    void clear_fault();
    void set_unavailable(int error = -ENODEV);
    void clear_unavailable();

    // Arm a one-shot fault for the exact one-based occurrence of operation.
    // TORN_PREFIX is meaningful only for writes: the requested prefix becomes
    // volatile before the configured error is returned.
    auto fail_operation(FaultBlockOperation operation, uint64_t operation_index, int error = -EIO,
                        FaultBlockWriteMode write_mode = FaultBlockWriteMode::FAIL_BEFORE_IO, size_t torn_prefix_bytes = 0) -> bool;

    // Arm the exact consecutive operation-index window beginning at
    // operation_index. This models a bounded persistent failure through a
    // caller's retry loop while keeping every failed index reproducible.
    auto fail_operations(FaultBlockOperation operation, uint64_t operation_index, uint64_t operation_count, int error = -EIO) -> bool;

    // Copy an exact prefix into the caller's read buffer and then fail.  This
    // is the synchronous block ABI's deterministic representation of a short
    // read: partial transfer is observable in history, but success is never
    // reported for an incomplete page.
    auto fail_read_prefix(uint64_t operation_index, size_t prefix_bytes, int error = -EIO) -> bool;

    // Complete a read successfully after flipping one byte in the returned
    // data.  Integrity-aware consumers must reject this independently of the
    // transport result.
    auto corrupt_read(uint64_t operation_index, size_t byte_offset, uint8_t xor_mask = 1) -> bool;

    // Arm by the absolute one-based history index instead of an operation-
    // local index. This is useful when a complete baseline trace is replayed.
    auto fail_global(uint64_t global_index, int error = -EIO, FaultBlockWriteMode write_mode = FaultBlockWriteMode::FAIL_BEFORE_IO,
                     size_t torn_prefix_bytes = 0) -> bool;

    void set_scenario_seed(uint64_t seed) { scenario_seed_ = seed; }
    auto scenario_seed() const -> uint64_t { return scenario_seed_; }

    auto event_count() const -> size_t { return event_count_; }
    auto history_overflowed() const -> bool { return history_overflowed_; }
    auto event(size_t index) const -> const FaultBlockEvent*;
    auto operation_count(FaultBlockOperation operation) const -> uint64_t;
    auto power_cut_count() const -> uint64_t { return power_cut_count_; }
    auto durability_generation() const -> uint64_t { return durability_generation_; }
    auto durable_data() const -> const uint8_t* { return durable_.data(); }
    auto volatile_data() const -> const uint8_t* { return volatile_.data(); }
    auto volatile_matches_durable() const -> bool;

   private:
    struct FaultPlan {
        FaultBlockOperation operation{FaultBlockOperation::READ};
        uint64_t operation_index{};
        uint64_t operation_count{1};
        uint64_t global_index{};
        int error{};
        FaultBlockWriteMode write_mode{FaultBlockWriteMode::FAIL_BEFORE_IO};
        size_t torn_prefix_bytes{};
        size_t read_prefix_bytes{};
        size_t corrupt_byte_offset{};
        uint8_t corrupt_xor_mask{};
        bool corrupt_read{};
        bool armed{};
    };

    static auto read_callback(dev::BlockDevice* device, uint64_t block, size_t count, void* buffer) -> int;
    static auto write_callback(dev::BlockDevice* device, uint64_t block, size_t count, const void* buffer) -> int;
    static auto flush_callback(dev::BlockDevice* device) -> int;

    auto read(uint64_t block, size_t count, void* buffer) -> int;
    auto write(uint64_t block, size_t count, const void* buffer) -> int;
    auto flush() -> int;
    auto checked_range(uint64_t block, size_t count, size_t* offset_out, size_t* bytes_out) const -> bool;
    auto next_operation_index(FaultBlockOperation operation) -> uint64_t;
    auto consume_fault(FaultBlockOperation operation, uint64_t operation_index, uint64_t global_index, FaultPlan* plan_out) -> bool;
    void record(FaultBlockOperation operation, uint64_t operation_index, uint64_t block, size_t count, size_t transferred_bytes,
                int result);

    dev::BlockDevice device_{};
    std::array<uint8_t, MAX_MEDIA_BYTES> volatile_{};
    std::array<uint8_t, MAX_MEDIA_BYTES> durable_{};
    std::array<FaultBlockEvent, MAX_HISTORY> history_{};
    std::array<uint64_t, 3> operation_counts_{};
    FaultPlan fault_{};
    size_t media_bytes_{};
    size_t event_count_{};
    uint64_t scenario_seed_{};
    uint64_t global_operation_count_{};
    uint64_t durability_generation_{};
    uint64_t power_cut_count_{};
    int unavailable_error_{};
    bool valid_{};
    bool history_overflowed_{};
};

}  // namespace ker::test
