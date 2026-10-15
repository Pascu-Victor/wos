#include "numa.hpp"

#include <array>
#include <cerrno>
#include <platform/acpi/numa_boot.hpp>
#include <platform/mm/numa_alloc.hpp>
#include <platform/sched/task.hpp>
#include <platform/sys/usercopy.hpp>

// Keep this syscall TU instrumented: fixed request/output records and the
// StableUserPage live on an ordinary syscall stack, outside MM spinlocks.
namespace ker::syscall::vmem {
namespace {
namespace abi = ker::abi::vmem::numa;
namespace topology = ker::mod::acpi::numa;
namespace placement = ker::mod::mm::numa;
namespace usercopy = ker::mod::sys::usercopy;
using Task = ker::mod::sched::task::Task;

auto failure(int error) -> uint64_t { return static_cast<uint64_t>(-error); }

auto topology_valid(const topology::Topology* value) -> bool {
    return value != nullptr && value->node_count != 0 && value->node_count <= topology::MAX_NODES && value->cpu_count != 0 &&
           value->cpu_count <= topology::MAX_CPUS;
}

auto decode_mode(uint32_t mode, placement::Mode& result) -> bool {
    switch (static_cast<abi::PolicyMode>(mode)) {
        case abi::PolicyMode::LOCAL:
            result = placement::Mode::LOCAL;
            return true;
        case abi::PolicyMode::PREFERRED:
            result = placement::Mode::PREFERRED;
            return true;
        case abi::PolicyMode::BIND:
            result = placement::Mode::BIND;
            return true;
        case abi::PolicyMode::INTERLEAVE:
            result = placement::Mode::INTERLEAVE;
            return true;
    }
    return false;
}

auto encode_mode(placement::Mode mode, uint32_t& result) -> bool {
    switch (mode) {
        case placement::Mode::LOCAL:
            result = static_cast<uint32_t>(abi::PolicyMode::LOCAL);
            return true;
        case placement::Mode::PREFERRED:
            result = static_cast<uint32_t>(abi::PolicyMode::PREFERRED);
            return true;
        case placement::Mode::BIND:
            result = static_cast<uint32_t>(abi::PolicyMode::BIND);
            return true;
        case placement::Mode::INTERLEAVE:
            result = static_cast<uint32_t>(abi::PolicyMode::INTERLEAVE);
            return true;
    }
    return false;
}

auto encode_provenance(const topology::Topology& value, const topology::BootStatus& status, uint32_t& result) -> bool {
    if (status.rejected_firmware) {
        result = static_cast<uint32_t>(abi::Provenance::UMA_REJECTED_FIRMWARE);
        return true;
    }
    switch (value.provenance) {
        case topology::Provenance::UMA_MISSING_SRAT:
            result = static_cast<uint32_t>(abi::Provenance::UMA_MISSING_SRAT);
            return true;
        case topology::Provenance::SRAT_DEFAULT_DISTANCES:
            result = static_cast<uint32_t>(abi::Provenance::SRAT_DEFAULT_DISTANCES);
            return true;
        case topology::Provenance::SRAT_SLIT:
            result = static_cast<uint32_t>(abi::Provenance::SRAT_SLIT);
            return true;
    }
    return false;
}

template <typename T>
auto write_record(Task& actor, uint64_t address, const T& record) -> uint64_t {
    return usercopy::copy_to_task(actor, address, &record, sizeof(record)) ? 0 : failure(EFAULT);
}
}  // namespace

auto query_numa_for_task(Task& actor, uint64_t request_address, uint64_t bytes, uint64_t unused3, uint64_t unused4) -> uint64_t {
    if (bytes != sizeof(abi::QueryRequest) || unused3 != 0 || unused4 != 0) return failure(EINVAL);
    abi::QueryRequest request{};
    if (!usercopy::copy_from_task(actor, request_address, &request, sizeof(request))) return failure(EFAULT);
    if (request.bytes != sizeof(request)) return failure(EINVAL);
    if (request.version != abi::VERSION || request.flags != 0) return failure(ENOTSUP);
    if (request.reserved32_a != 0 || request.reserved32_b != 0 || request.reserved64 != 0) return failure(EINVAL);

    uint32_t expected_bytes = 0;
    auto const KIND = static_cast<abi::QueryKind>(request.kind);
    switch (KIND) {
        case abi::QueryKind::SUMMARY:
            if (request.index != 0 || request.address != 0 || request.count != 0) return failure(EINVAL);
            expected_bytes = sizeof(abi::Summary);
            break;
        case abi::QueryKind::NODE:
            if (request.address != 0 || request.count != 0) return failure(EINVAL);
            expected_bytes = sizeof(abi::Node);
            break;
        case abi::QueryKind::SELF_POLICY:
            if (request.index != 0 || request.address != 0 || request.count != 0) return failure(EINVAL);
            expected_bytes = sizeof(abi::SelfPolicy);
            break;
        case abi::QueryKind::PAGE_NODES: {
            if (request.index != 0 || request.count == 0 || request.count > abi::MAX_PAGE_NODES || request.address % abi::PAGE_BYTES != 0)
                return failure(EINVAL);
            uint64_t const PAYLOAD_BYTES = static_cast<uint64_t>(request.count) * abi::PAGE_BYTES;
            if (request.address >= usercopy::USER_ADDR_LIMIT || PAYLOAD_BYTES > usercopy::USER_ADDR_LIMIT - request.address)
                return failure(EINVAL);
            expected_bytes = request.count * sizeof(abi::PageNode);
            break;
        }
        default:
            return failure(EINVAL);
    }
    if (request.output_bytes != expected_bytes) return failure(EINVAL);
    if (!usercopy::range_valid(request.output_address, expected_bytes)) return failure(EFAULT);
    auto const* const TOPOLOGY = topology::boot_topology();
    if (!topology_valid(TOPOLOGY)) return failure(ENODEV);

    switch (KIND) {
        case abi::QueryKind::SUMMARY: {
            auto const* const STATUS = topology::boot_status();
            if (STATUS == nullptr) return failure(ENODEV);
            abi::Summary result{};
            result.version = abi::VERSION;
            result.bytes = sizeof(result);
            result.features = abi::FEATURES_V1;
            if (!encode_provenance(*TOPOLOGY, *STATUS, result.provenance)) return failure(ENODEV);
            result.node_count = TOPOLOGY->node_count;
            result.cpu_count = TOPOLOGY->cpu_count;
            result.page_bytes = abi::PAGE_BYTES;
            result.max_page_nodes = abi::MAX_PAGE_NODES;
            result.cpu_mask_words = abi::CPU_MASK_WORDS;
            return write_record(actor, request.output_address, result);
        }
        case abi::QueryKind::NODE: {
            if (request.index >= TOPOLOGY->node_count) return failure(EINVAL);
            auto const& SOURCE = TOPOLOGY->nodes[request.index];
            abi::Node result{};
            result.version = abi::VERSION;
            result.bytes = sizeof(result);
            result.node = static_cast<uint16_t>(request.index);
            result.proximity_id = SOURCE.proximity_id;
            result.managed_bytes = SOURCE.usable_bytes;
            for (size_t i = 0; i < abi::CPU_MASK_WORDS; ++i) result.cpu_words[i] = SOURCE.cpus.words[i];
            return write_record(actor, request.output_address, result);
        }
        case abi::QueryKind::SELF_POLICY: {
            auto const POLICY = actor.memory_policy_snapshot();
            abi::SelfPolicy result{};
            result.version = abi::VERSION;
            result.bytes = sizeof(result);
            if (!placement::valid_policy(POLICY, TOPOLOGY->node_count) || !encode_mode(POLICY.mode, result.mode)) return failure(EINVAL);
            result.nodes = POLICY.nodes;
            return write_record(actor, request.output_address, result);
        }
        case abi::QueryKind::PAGE_NODES: {
            std::array<abi::PageNode, abi::MAX_PAGE_NODES> results{};
            for (uint32_t i = 0; i < request.count; ++i) {
                auto& result = results[i];
                result.node = abi::INVALID_NODE;
                result.status = static_cast<uint16_t>(abi::PageStatus::UNAVAILABLE);
                // This scope owns Task/root/frame lifetime for exactly one row.
                // No payload address is dereferenced and no fault-in is allowed.
                usercopy::StableUserPage pin;
                if (usercopy::pin_task_user_page(actor, request.address + static_cast<uint64_t>(i) * abi::PAGE_BYTES, false, false, pin) &&
                    pin.valid()) {
                    auto const NODE = placement::physical_node(pin.physical_address());
                    if (NODE < TOPOLOGY->node_count && pin.still_mapped()) {
                        result.node = NODE;
                        result.status = static_cast<uint16_t>(abi::PageStatus::RESIDENT);
                    }
                }
            }
            return usercopy::copy_to_task(actor, request.output_address, results.data(), expected_bytes) ? 0 : failure(EFAULT);
        }
    }
    return failure(EINVAL);
}

auto set_memory_policy_for_task(Task& actor, uint64_t request_address, uint64_t bytes, uint64_t unused3, uint64_t unused4) -> uint64_t {
    if (bytes != sizeof(abi::PolicyRequest) || unused3 != 0 || unused4 != 0) return failure(EINVAL);
    abi::PolicyRequest request{};
    if (!usercopy::copy_from_task(actor, request_address, &request, sizeof(request))) return failure(EFAULT);
    if (request.bytes != sizeof(request)) return failure(EINVAL);
    if (request.version != abi::VERSION || request.flags != 0) return failure(ENOTSUP);
    if (request.reserved64 != 0) return failure(EINVAL);
    placement::Policy policy{};
    policy.nodes = request.nodes;
    if (!decode_mode(request.mode, policy.mode)) return failure(EINVAL);
    auto const* const TOPOLOGY = topology::boot_topology();
    if (!topology_valid(TOPOLOGY)) return failure(ENODEV);
    if (!placement::valid_policy(policy, TOPOLOGY->node_count)) return failure(EINVAL);
    if (policy.mode != placement::Mode::LOCAL) return failure(ENOTSUP);
    actor.publish_memory_policy_snapshot(policy);
    return 0;
}
}  // namespace ker::syscall::vmem
