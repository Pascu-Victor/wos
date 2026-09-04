#pragma once

// Deterministic block-device fault model compiled by fault_block_device_ktest.cpp.

#include <algorithm>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <limits>
#include <test/fault_block_device.hpp>

namespace ker::test {

namespace {

auto operation_slot(FaultBlockOperation operation) -> size_t {
    switch (operation) {
        case FaultBlockOperation::READ:
            return 0;
        case FaultBlockOperation::WRITE:
            return 1;
        case FaultBlockOperation::FLUSH:
            return 2;
    }
    return 0;
}

auto normalized_error(int error) -> int { return error < 0 ? error : -EIO; }

}  // namespace

FaultBlockDevice::FaultBlockDevice(size_t block_size, uint64_t total_blocks, uint8_t initial_value)
    : valid_(block_size != 0 && total_blocks != 0 && total_blocks <= MAX_MEDIA_BYTES / block_size) {
    if (valid_) {
        media_bytes_ = static_cast<size_t>(total_blocks) * block_size;
    }

    device_.block_size = block_size;
    device_.total_blocks = total_blocks;
    device_.read_blocks = read_callback;
    device_.write_blocks = write_callback;
    device_.flush = flush_callback;
    device_.private_data = this;
    reset(initial_value);
}

void FaultBlockDevice::reset(uint8_t value) {
    std::memset(volatile_.data(), value, media_bytes_);
    std::memset(durable_.data(), value, media_bytes_);
    operation_counts_.fill(0);
    history_.fill({});
    fault_ = {};
    event_count_ = 0;
    scenario_seed_ = 0;
    global_operation_count_ = 0;
    durability_generation_ = 0;
    power_cut_count_ = 0;
    unavailable_error_ = 0;
    history_overflowed_ = false;
}

auto FaultBlockDevice::seed_bytes(size_t offset, const void* source, size_t length) -> bool {
    if (!valid_ || source == nullptr || offset > media_bytes_ || length > media_bytes_ - offset) {
        return false;
    }
    std::memcpy(volatile_.data() + offset, source, length);
    std::memcpy(durable_.data() + offset, source, length);
    return true;
}

void FaultBlockDevice::power_cut() {
    std::memcpy(volatile_.data(), durable_.data(), media_bytes_);
    fault_ = {};
    power_cut_count_++;
}

void FaultBlockDevice::clear_history() {
    operation_counts_.fill(0);
    history_.fill({});
    event_count_ = 0;
    global_operation_count_ = 0;
    history_overflowed_ = false;
}

void FaultBlockDevice::clear_fault() { fault_ = {}; }

void FaultBlockDevice::set_unavailable(int error) { unavailable_error_ = normalized_error(error); }

void FaultBlockDevice::clear_unavailable() { unavailable_error_ = 0; }

auto FaultBlockDevice::fail_operation(FaultBlockOperation operation, uint64_t operation_index, int error, FaultBlockWriteMode write_mode,
                                      size_t torn_prefix_bytes) -> bool {
    if (!valid_ || operation_index == 0 || (operation != FaultBlockOperation::WRITE && write_mode == FaultBlockWriteMode::TORN_PREFIX)) {
        return false;
    }
    fault_ = {
        .operation = operation,
        .operation_index = operation_index,
        .operation_count = 1,
        .global_index = 0,
        .error = normalized_error(error),
        .write_mode = write_mode,
        .torn_prefix_bytes = torn_prefix_bytes,
        .armed = true,
    };
    return true;
}

auto FaultBlockDevice::fail_operations(FaultBlockOperation operation, uint64_t operation_index, uint64_t operation_count, int error)
    -> bool {
    if (!valid_ || operation_index == 0 || operation_count == 0 || operation_count - 1 > UINT64_MAX - operation_index) {
        return false;
    }
    fault_ = {
        .operation = operation,
        .operation_index = operation_index,
        .operation_count = operation_count,
        .global_index = 0,
        .error = normalized_error(error),
        .write_mode = FaultBlockWriteMode::FAIL_BEFORE_IO,
        .torn_prefix_bytes = 0,
        .armed = true,
    };
    return true;
}

auto FaultBlockDevice::fail_read_prefix(uint64_t operation_index, size_t prefix_bytes, int error) -> bool {
    if (!valid_ || operation_index == 0) {
        return false;
    }
    fault_ = {
        .operation = FaultBlockOperation::READ,
        .operation_index = operation_index,
        .operation_count = 1,
        .global_index = 0,
        .error = normalized_error(error),
        .write_mode = FaultBlockWriteMode::FAIL_BEFORE_IO,
        .torn_prefix_bytes = 0,
        .read_prefix_bytes = prefix_bytes,
        .armed = true,
    };
    return true;
}

auto FaultBlockDevice::corrupt_read(uint64_t operation_index, size_t byte_offset, uint8_t xor_mask) -> bool {
    if (!valid_ || operation_index == 0 || xor_mask == 0) {
        return false;
    }
    fault_ = {
        .operation = FaultBlockOperation::READ,
        .operation_index = operation_index,
        .operation_count = 1,
        .global_index = 0,
        .error = 0,
        .write_mode = FaultBlockWriteMode::FAIL_BEFORE_IO,
        .torn_prefix_bytes = 0,
        .read_prefix_bytes = 0,
        .corrupt_byte_offset = byte_offset,
        .corrupt_xor_mask = xor_mask,
        .corrupt_read = true,
        .armed = true,
    };
    return true;
}

auto FaultBlockDevice::fail_global(uint64_t global_index, int error, FaultBlockWriteMode write_mode, size_t torn_prefix_bytes) -> bool {
    if (!valid_ || global_index == 0) {
        return false;
    }
    fault_ = {
        .operation = FaultBlockOperation::READ,
        .operation_index = 0,
        .operation_count = 1,
        .global_index = global_index,
        .error = normalized_error(error),
        .write_mode = write_mode,
        .torn_prefix_bytes = torn_prefix_bytes,
        .armed = true,
    };
    return true;
}

auto FaultBlockDevice::event(size_t index) const -> const FaultBlockEvent* { return index < event_count_ ? &history_.at(index) : nullptr; }

auto FaultBlockDevice::operation_count(FaultBlockOperation operation) const -> uint64_t {
    return operation_counts_.at(operation_slot(operation));
}

auto FaultBlockDevice::volatile_matches_durable() const -> bool {
    return std::memcmp(volatile_.data(), durable_.data(), media_bytes_) == 0;
}

auto FaultBlockDevice::read_callback(dev::BlockDevice* device, uint64_t block, size_t count, void* buffer) -> int {
    if (device == nullptr || device->private_data == nullptr) {
        return -EINVAL;
    }
    return static_cast<FaultBlockDevice*>(device->private_data)->read(block, count, buffer);
}

auto FaultBlockDevice::write_callback(dev::BlockDevice* device, uint64_t block, size_t count, const void* buffer) -> int {
    if (device == nullptr || device->private_data == nullptr) {
        return -EINVAL;
    }
    return static_cast<FaultBlockDevice*>(device->private_data)->write(block, count, buffer);
}

auto FaultBlockDevice::flush_callback(dev::BlockDevice* device) -> int {
    if (device == nullptr || device->private_data == nullptr) {
        return -EINVAL;
    }
    return static_cast<FaultBlockDevice*>(device->private_data)->flush();
}

auto FaultBlockDevice::read(uint64_t block, size_t count, void* buffer) -> int {
    uint64_t const OPERATION_INDEX = next_operation_index(FaultBlockOperation::READ);
    size_t offset = 0;
    size_t bytes = 0;
    if (buffer == nullptr || !checked_range(block, count, &offset, &bytes)) {
        record(FaultBlockOperation::READ, OPERATION_INDEX, block, count, 0, -EINVAL);
        return -EINVAL;
    }

    if (unavailable_error_ != 0) {
        record(FaultBlockOperation::READ, OPERATION_INDEX, block, count, 0, unavailable_error_);
        return unavailable_error_;
    }

    FaultPlan plan{};
    if (consume_fault(FaultBlockOperation::READ, OPERATION_INDEX, global_operation_count_, &plan)) {
        if (plan.corrupt_read) {
            std::memcpy(buffer, volatile_.data() + offset, bytes);
            if (plan.corrupt_byte_offset < bytes) {
                static_cast<uint8_t*>(buffer)[plan.corrupt_byte_offset] ^= plan.corrupt_xor_mask;
            }
            record(FaultBlockOperation::READ, OPERATION_INDEX, block, count, bytes, 0);
            return 0;
        }
        size_t const TRANSFERRED = std::min(bytes, plan.read_prefix_bytes);
        if (TRANSFERRED != 0) {
            std::memcpy(buffer, volatile_.data() + offset, TRANSFERRED);
        }
        record(FaultBlockOperation::READ, OPERATION_INDEX, block, count, TRANSFERRED, plan.error);
        return plan.error;
    }

    std::memcpy(buffer, volatile_.data() + offset, bytes);
    record(FaultBlockOperation::READ, OPERATION_INDEX, block, count, bytes, 0);
    return 0;
}

auto FaultBlockDevice::write(uint64_t block, size_t count, const void* buffer) -> int {
    uint64_t const OPERATION_INDEX = next_operation_index(FaultBlockOperation::WRITE);
    size_t offset = 0;
    size_t bytes = 0;
    if (buffer == nullptr || !checked_range(block, count, &offset, &bytes)) {
        record(FaultBlockOperation::WRITE, OPERATION_INDEX, block, count, 0, -EINVAL);
        return -EINVAL;
    }

    if (unavailable_error_ != 0) {
        record(FaultBlockOperation::WRITE, OPERATION_INDEX, block, count, 0, unavailable_error_);
        return unavailable_error_;
    }

    FaultPlan plan{};
    if (consume_fault(FaultBlockOperation::WRITE, OPERATION_INDEX, global_operation_count_, &plan)) {
        size_t transferred = 0;
        if (plan.write_mode == FaultBlockWriteMode::TORN_PREFIX) {
            transferred = std::min(bytes, plan.torn_prefix_bytes);
            std::memcpy(volatile_.data() + offset, buffer, transferred);
        }
        record(FaultBlockOperation::WRITE, OPERATION_INDEX, block, count, transferred, plan.error);
        return plan.error;
    }

    std::memcpy(volatile_.data() + offset, buffer, bytes);
    record(FaultBlockOperation::WRITE, OPERATION_INDEX, block, count, bytes, 0);
    return 0;
}

auto FaultBlockDevice::flush() -> int {
    uint64_t const OPERATION_INDEX = next_operation_index(FaultBlockOperation::FLUSH);
    if (unavailable_error_ != 0) {
        record(FaultBlockOperation::FLUSH, OPERATION_INDEX, 0, 0, 0, unavailable_error_);
        return unavailable_error_;
    }
    FaultPlan plan{};
    if (consume_fault(FaultBlockOperation::FLUSH, OPERATION_INDEX, global_operation_count_, &plan)) {
        record(FaultBlockOperation::FLUSH, OPERATION_INDEX, 0, 0, 0, plan.error);
        return plan.error;
    }

    std::memcpy(durable_.data(), volatile_.data(), media_bytes_);
    durability_generation_++;
    record(FaultBlockOperation::FLUSH, OPERATION_INDEX, 0, 0, media_bytes_, 0);
    return 0;
}

auto FaultBlockDevice::checked_range(uint64_t block, size_t count, size_t* offset_out, size_t* bytes_out) const -> bool {
    if (!valid_ || offset_out == nullptr || bytes_out == nullptr || block > device_.total_blocks || count > device_.total_blocks - block ||
        block > std::numeric_limits<size_t>::max() / device_.block_size ||
        count > std::numeric_limits<size_t>::max() / device_.block_size) {
        return false;
    }
    *offset_out = static_cast<size_t>(block) * device_.block_size;
    *bytes_out = count * device_.block_size;
    return *offset_out <= media_bytes_ && *bytes_out <= media_bytes_ - *offset_out;
}

auto FaultBlockDevice::next_operation_index(FaultBlockOperation operation) -> uint64_t {
    global_operation_count_++;
    return ++operation_counts_.at(operation_slot(operation));
}

auto FaultBlockDevice::consume_fault(FaultBlockOperation operation, uint64_t operation_index, uint64_t global_index, FaultPlan* plan_out)
    -> bool {
    bool const GLOBAL_MATCH = fault_.global_index != 0 && fault_.global_index == global_index;
    bool const OPERATION_MATCH = fault_.global_index == 0 && fault_.operation == operation && operation_index >= fault_.operation_index &&
                                 operation_index - fault_.operation_index < fault_.operation_count;
    if (!fault_.armed || (!GLOBAL_MATCH && !OPERATION_MATCH)) {
        return false;
    }
    if (fault_.write_mode == FaultBlockWriteMode::TORN_PREFIX && operation != FaultBlockOperation::WRITE) {
        return false;
    }
    if (plan_out != nullptr) {
        *plan_out = fault_;
    }
    if (GLOBAL_MATCH || operation_index - fault_.operation_index + 1 >= fault_.operation_count) {
        fault_.armed = false;
    }
    return true;
}

void FaultBlockDevice::record(FaultBlockOperation operation, uint64_t operation_index, uint64_t block, size_t count,
                              size_t transferred_bytes, int result) {
    if (event_count_ >= history_.size()) {
        history_overflowed_ = true;
        return;
    }
    history_.at(event_count_++) = {
        .operation = operation,
        .scenario_seed = scenario_seed_,
        .global_index = global_operation_count_,
        .operation_index = operation_index,
        .block = block,
        .count = count,
        .transferred_bytes = transferred_bytes,
        .durability_generation = durability_generation_,
        .result = result,
    };
}

}  // namespace ker::test
