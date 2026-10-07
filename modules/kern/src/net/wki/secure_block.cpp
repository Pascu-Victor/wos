#include "secure_block.hpp"

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <cstring>
#include <dev/block_device.hpp>
#include <limits>
#include <net/wki/blk_ring.hpp>
#include <net/wki/dev_server.hpp>
#include <new>
#include <platform/sched/scheduler.hpp>

namespace ker::net::wki {

struct BlockSecureContext {
    WkiChannelIdentity identity{};
    rdma_protocol::RegionDescriptor grant{};
    uint8_t* backing = nullptr;
    std::atomic<bool> closed{false};
    std::atomic<bool> loan{false};
    std::atomic<bool> effect_started{false};
    bool registered = false;
    uint64_t last_sequence = 0;
    WkiSecureRdmaRequest request{};
    BlockSecureIntent intent{};
    DevServerBinding* retained_binding = nullptr;
};

namespace {
std::atomic<uint64_t> s_binding_generation{1};  // NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

auto no_effect(int status) -> WkiSecureRdmaResult { return {status, 0, WkiSecureRdmaOutcome::NO_EFFECT}; }

auto intent_valid(const BlockSecureIntent& intent, const DevServerBinding& binding, const WkiSecureRdmaRequest& request) -> int {
    if (intent.version != WKI_SECURE_BLOCK_VERSION || intent.opcode != static_cast<uint8_t>(request.opcode) ||
        !std::all_of(intent.reserved.begin(), intent.reserved.end(), [](uint8_t byte) { return byte == 0; }) || intent.sequence == 0 ||
        request.offset != 0 || request.flags != 0 || request.length == 0)
        return -EINVAL;
    if (intent.attach_cookie != binding.attach_cookie || intent.resource_id != binding.resource_id ||
        !wki_resource_incarnation_valid(intent.incarnation) ||
        !wki_resource_incarnation_equal(intent.incarnation, binding.resource_incarnation) ||
        intent.binding_generation != request.grant.binding_generation)
        return -ESTALE;
    auto* device = binding.block_dev;
    if (device == nullptr || intent.block_size != device->block_size) return -ESTALE;
    auto const TRANSFER = blk_validate_transfer(intent.lba, intent.count, device->block_size, device->total_blocks, WKI_SECURE_BLOCK_BYTES);
    if (!TRANSFER.valid || TRANSFER.bytes != request.length || intent.count == 0) return -EINVAL;
    if (request.opcode == rdma_protocol::Opcode::READ) return binding.block_read_allowed ? 0 : -EACCES;
    if (request.opcode != rdma_protocol::Opcode::WRITE) return -EINVAL;
    if (binding.block_read_only || !binding.block_writer_lease.active() || ker::dev::block_device_is_read_only(device)) return -EROFS;
    return 0;
}

auto request_matches(const BlockSecureContext& context, const WkiSecureRdmaRequest& request) -> bool {
    return request.operation_id != 0 && request.receive_ticket != 0 && request.peer_node == context.identity.peer_node_id &&
           request.intent_length == sizeof(BlockSecureIntent) && std::memcmp(&request.grant, &context.grant, sizeof(context.grant)) == 0;
}

auto exact_loan(const BlockSecureContext& context, const WkiSecureRdmaRequest& request) -> bool {
    return context.loan.load(std::memory_order_acquire) && request_matches(context, request) &&
           context.request.operation_id == request.operation_id && context.request.receive_ticket == request.receive_ticket &&
           context.request.opcode == request.opcode && context.request.offset == request.offset &&
           context.request.length == request.length && context.request.flags == request.flags &&
           context.request.native_intent == request.native_intent;
}

auto block_arm(void* opaque, const WkiSecureRdmaRequest& request, uint64_t& generation) -> int {
    auto* context = static_cast<BlockSecureContext*>(opaque);
    if (context == nullptr || context->closed.load(std::memory_order_acquire)) return -ESTALE;
    if (!request_matches(*context, request)) return -EACCES;
    BlockSecureIntent intent{};
    std::memcpy(&intent, request.native_intent.data(), sizeof(intent));
    auto* binding = wki_dev_server_retain_block_secure_binding(context->identity, context);
    if (binding == nullptr) return -EAGAIN;
    int const STATUS = intent_valid(intent, *binding, request);
    if (STATUS != 0) {
        wki_dev_server_release_block_secure_binding(binding);
        return STATUS;
    }
    bool expected = false;
    if (!context->loan.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        wki_dev_server_release_block_secure_binding(binding);
        return -EBUSY;
    }
    expected = false;
    if (!binding->block_native_busy.compare_exchange_strong(expected, true, std::memory_order_acq_rel)) {
        context->loan.store(false, std::memory_order_release);
        wki_dev_server_release_block_secure_binding(binding);
        return -EBUSY;
    }
    if (context->closed.load(std::memory_order_acquire) || intent.sequence <= context->last_sequence) {
        binding->block_native_busy.store(false, std::memory_order_release);
        context->loan.store(false, std::memory_order_release);
        wki_dev_server_release_block_secure_binding(binding);
        return -ESTALE;
    }
    context->last_sequence = intent.sequence;
    context->request = request;
    context->intent = intent;
    context->retained_binding = binding;
    context->effect_started.store(false, std::memory_order_release);
    generation = context->grant.binding_generation;
    return 0;
}

auto block_effect(void* opaque, const WkiSecureRdmaRequest& request, std::span<uint8_t> backing, std::span<const uint8_t> input,
                  std::span<uint8_t> output) -> WkiSecureRdmaResult {
    auto* context = static_cast<BlockSecureContext*>(opaque);
    if (context == nullptr || !exact_loan(*context, request)) return no_effect(-EACCES);
    if (context->closed.load(std::memory_order_acquire)) return no_effect(-ESTALE);
    auto* binding = context->retained_binding;
    if (binding == nullptr || backing.data() != context->backing || backing.size() != WKI_SECURE_BLOCK_BYTES) return no_effect(-EACCES);
    int const STATUS = intent_valid(context->intent, *binding, request);
    if (STATUS != 0) return no_effect(STATUS);
    if ((request.opcode == rdma_protocol::Opcode::WRITE && (input.size() != request.length || !output.empty())) ||
        (request.opcode == rdma_protocol::Opcode::READ && (!input.empty() || output.size() < request.length)))
        return no_effect(-EINVAL);
    bool expected = false;
    if (!context->effect_started.compare_exchange_strong(expected, true, std::memory_order_acq_rel))
        return {-EALREADY, 0, WkiSecureRdmaOutcome::UNCERTAIN};
    int native_status = 0;
    if (request.opcode == rdma_protocol::Opcode::WRITE) {
        std::copy(input.begin(), input.end(), context->backing);
        native_status = ker::dev::block_write(binding->block_dev, context->intent.lba, context->intent.count, context->backing);
    } else {
        native_status = ker::dev::block_read(binding->block_dev, context->intent.lba, context->intent.count, context->backing);
        if (native_status == 0) std::copy_n(context->backing, request.length, output.begin());
    }
    // Device completion, including DMA retirement, precedes this return. A
    // negative native WRITE can still have modified an initialized prefix.
    if (native_status != 0) return {native_status < 0 ? native_status : -EIO, 0, WkiSecureRdmaOutcome::UNCERTAIN};
    return {0, request.length, WkiSecureRdmaOutcome::EXACT};
}

void block_release(void* opaque, const WkiSecureRdmaRequest& request, const WkiSecureRdmaResult&) {
    auto* context = static_cast<BlockSecureContext*>(opaque);
    if (context == nullptr || !exact_loan(*context, request)) return;
    auto* binding = context->retained_binding;
    auth_crypto::secure_erase(context->backing, request.length);
    context->retained_binding = nullptr;
    context->request = {};
    context->intent = {};
    if (binding != nullptr) binding->block_native_busy.store(false, std::memory_order_release);
    context->loan.store(false, std::memory_order_release);
    wki_dev_server_release_block_secure_binding(binding);
}

auto prepare_valid(const DevServerBinding& binding, const BlockSecurePrepare& request) -> bool {
    return request.version == WKI_SECURE_BLOCK_VERSION && request.reserved == 0 && request.attach_cookie != 0 &&
           request.attach_cookie == binding.attach_cookie && request.resource_id == binding.resource_id &&
           wki_resource_incarnation_valid(request.incarnation) &&
           wki_resource_incarnation_equal(request.incarnation, binding.resource_incarnation);
}
}  // namespace

void wki_secure_block_close_context(BlockSecureContext* context) {
    if (context != nullptr) context->closed.store(true, std::memory_order_release);
}

void wki_secure_block_release_context(BlockSecureContext* context) {
    if (context == nullptr) return;
    wki_secure_block_close_context(context);
    while (context->registered) {
        if (wki_secure_rdma_unregister_region(context->grant) == 0)
            context->registered = false;
        else
            ker::mod::sched::kern_yield();
    }
    // Registration pins own hooks and backing until every native effect and
    // transmit loan retires. No timeout grants permission to free this memory.
    auth_crypto::secure_erase(context->backing, WKI_SECURE_BLOCK_BYTES);
    delete[] context->backing;
    auth_crypto::secure_erase(&context->grant, sizeof(context->grant));
    delete context;
}

auto wki_secure_block_prepare(DevServerBinding* binding, const BlockSecurePrepare& request, rdma_protocol::RegionDescriptor& output)
    -> int {
    if (binding == nullptr || binding->resource_type != ResourceType::BLOCK || !prepare_valid(*binding, request)) return -ESTALE;
    if (binding->block_secure_disabled) return -ENOTSUP;
    if (!wki_secure_rdma_available(binding->channel_identity, rdma_protocol::Service::BLOCK)) return -ENOTSUP;
    auto* context = wki_dev_server_block_secure_context(binding);
    if (context != nullptr) {
        int const CURRENT = wki_secure_rdma_local_region_current(binding->channel_identity, rdma_protocol::Service::BLOCK, context->grant);
        if (CURRENT == 0) {
            output = context->grant;
            return 0;
        }
        if (CURRENT != -ESTALE) return CURRENT;
        wki_secure_block_close_context(context);
        int const UNREGISTER = context->registered ? wki_secure_rdma_unregister_region(context->grant) : 0;
        if (UNREGISTER != 0) return UNREGISTER;
        context->registered = false;
        // Retirement can win this exact ownership transfer. Its existing task
        // cleanup then owns the context; this worker must never free it twice.
        if (!wki_dev_server_exchange_block_secure_context(binding, context, nullptr)) return -ENOTCONN;
        wki_secure_block_release_context(context);
    }
    uint64_t generation = s_binding_generation.load(std::memory_order_relaxed);
    if (generation == std::numeric_limits<uint64_t>::max()) return -ENOTSUP;
    if (!s_binding_generation.compare_exchange_strong(generation, generation + 1, std::memory_order_relaxed)) return -EAGAIN;
    context = new (std::nothrow) BlockSecureContext;
    if (context == nullptr) return -ENOMEM;
    context->backing = new (std::nothrow) uint8_t[WKI_SECURE_BLOCK_BYTES]{};
    if (context->backing == nullptr) {
        delete context;
        return -ENOMEM;
    }
    context->identity = binding->channel_identity;
    uint8_t const PERMISSIONS =
        (binding->block_read_allowed ? rdma_protocol::PERMIT_READ : 0) | (!binding->block_read_only ? rdma_protocol::PERMIT_WRITE : 0);
    WkiSecureRdmaHooks const HOOKS{block_arm, block_effect, block_release};
    int const STATUS = wki_secure_rdma_register_region(context->identity, rdma_protocol::Service::BLOCK, context->backing,
                                                       WKI_SECURE_BLOCK_BYTES, generation, PERMISSIONS, HOOKS, context, context->grant);
    if (STATUS != 0) {
        wki_secure_block_release_context(context);
        return STATUS;
    }
    context->registered = true;
    if (!wki_dev_server_exchange_block_secure_context(binding, nullptr, context)) {
        wki_secure_block_release_context(context);
        return -ENOTCONN;
    }
    output = context->grant;
    return 0;
}

}  // namespace ker::net::wki
