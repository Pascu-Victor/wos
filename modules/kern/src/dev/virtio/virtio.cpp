#include "virtio.hpp"

#include <cstdint>
#include <mod/io/port/port.hpp>
#include <new>

#include "net/packet.hpp"
#include "platform/dbg/dbg.hpp"

namespace ker::dev::virtio {

using log = ker::mod::dbg::logger<"virtio">;

auto virtq_desc_size(uint16_t qsz) -> size_t { return sizeof(VirtqDesc) * qsz; }

auto virtq_avail_size(uint16_t qsz) -> size_t {
    return sizeof(uint16_t) * (3 + qsz);  // flags + idx + ring[qsz] + used_event
}

auto virtq_used_size(uint16_t qsz) -> size_t {
    return (sizeof(uint16_t) * 3) + (sizeof(VirtqUsedElem) * qsz);  // flags + idx + ring[qsz] + avail_event
}

auto virtq_total_size(uint16_t qsz) -> size_t { return managed::layout(qsz).bytes; }

auto virtq_alloc(const pci::PCIDevice* device, uint16_t size, QueueKind kind, uint64_t mask) -> Virtqueue* {
    auto const LAYOUT = managed::layout(size);
    if (LAYOUT.bytes == 0 || (kind != QueueKind::control && kind != QueueKind::receive && kind != QueueKind::transmit)) return nullptr;
    auto* vq = new (std::nothrow) Virtqueue{};
    if (vq == nullptr) return nullptr;
    managed::DmaApi api;
    if (!vq->ring.allocate(api, device, LAYOUT.bytes, mask)) {
        delete vq;
        return nullptr;
    }
    auto* base = reinterpret_cast<uint8_t*>(vq->ring.allocation.cpu);

    vq->kind = kind;
    vq->size = size;
    vq->num_free = size;
    vq->free_head = 0;
    vq->last_used_idx = 0;

    // Layout: desc table | avail ring | (align 4096) | used ring
    vq->desc = reinterpret_cast<VirtqDesc*>(base);
    vq->avail = reinterpret_cast<VirtqAvail*>(base + LAYOUT.available);

    vq->used = reinterpret_cast<VirtqUsed*>(base + LAYOUT.used);

    // Initialize free list: each descriptor points to the next
    for (uint16_t i = 0; i < size - 1; i++) {
        vq->desc[i].next = i + 1;
    }
    vq->desc[size - 1].next = VIRTQ_NO_BUF;  // End of free list

    if (kind != QueueKind::control) {
        auto const DIRECTION = kind == QueueKind::receive ? dma::Direction::from_device : dma::Direction::to_device;
        for (uint16_t i = 0; i < size; ++i) {
            // Descriptor addresses are 64-bit even on the legacy transport;
            // its narrower PFN mask applies only to the split ring itself.
            auto const RESULT = dma::allocate_streaming(device, ker::net::PKT_BUF_SIZE, DIRECTION);
            if (RESULT.error != dma::Error::none) {
                (void)virtq_retire(vq, false);
                return nullptr;
            }
            vq->packet_dma[i].allocation = RESULT.allocation;
        }
    }
    if (!vq->ring.prepare(api)) {
        (void)virtq_retire(vq, false);
        return nullptr;
    }
    return vq;
}

auto virtq_retire(Virtqueue*& vq, bool quiesced) -> bool {
    if (vq == nullptr) return true;
    // Boot/task only, after draining every software reader. A failed reset
    // conservatively pins the entire exposed queue, including idle slots.
    bool const CAN_RELEASE = quiesced || !vq->ring.exposed;
    bool released = true;
    for (size_t i = 0; i < vq->packet_dma.size(); ++i) {
        auto& slot = vq->packet_dma[i];
        if (slot.allocation.handle.generation == 0) continue;
        bool borrow_detached = slot.submission.sequence == 0;
        bool retired = false;
        if (CAN_RELEASE) {
            if (slot.submission.sequence != 0) {
                auto const PROOF = quiesced ? dma::Retirement::device_quiesced : dma::Retirement::never_submitted;
                if (dma::complete(slot.submission, 0, PROOF) == dma::Error::none) {
                    slot.submission = {};
                    borrow_detached = true;
                }
            }
            if (borrow_detached && dma::release(slot.allocation.handle) == dma::Error::none) {
                slot.allocation = {};
                retired = true;
            }
        }
        if (!retired) {
            // Quarantine detaches the borrowed CPU packet; hardware can only
            // touch the exclusive bounce pages retained by the DMA registry.
            borrow_detached = dma::quarantine(slot.allocation.handle) == dma::Error::quarantined;
            released = false;
        }
        if (borrow_detached && vq->pkt_map[i] != nullptr) {
            ker::net::pkt_free(vq->pkt_map[i]);
            vq->pkt_map[i] = nullptr;
        }
    }
    for (auto* pkt : vq->pkt_map)
        if (pkt != nullptr) released = false;
    managed::DmaApi api;
    if (!released) {
        (void)dma::quarantine(vq->ring.allocation.handle);
        return false;
    }
    if (!vq->ring.retire(api, quiesced)) return false;
    delete vq;
    vq = nullptr;
    return true;
}

namespace {
auto fail_queue(Virtqueue* vq, const char* reason) -> int {
    if (!vq->failed.exchange(true, std::memory_order_acq_rel))
        log::error("virtqueue stopped q=%u: %s; outstanding DMA retained until reset", vq->queue_index, reason);
    return -1;
}
}  // namespace

auto virtq_add_buf(Virtqueue* vq, std::span<std::byte> buffer, ker::net::PacketBuffer* pkt) -> int {
    if (vq == nullptr || vq->size == 0 || vq->size > VIRTQ_MAX_SIZE || vq->kind == QueueKind::control ||
        vq->failed.load(std::memory_order_acquire) || pkt == nullptr || buffer.empty())
        return -1;
    // Keep packet metadata/lifetime callbacks outside every DMA binding.
    auto const START = reinterpret_cast<uintptr_t>(pkt->storage.data());
    auto const ADDRESS = reinterpret_cast<uintptr_t>(buffer.data());
    if (ADDRESS < START || ADDRESS - START >= ker::net::PKT_BUF_SIZE || buffer.size() > ker::net::PKT_BUF_SIZE - (ADDRESS - START))
        return -1;
    if (vq->num_free == 0) return -1;
    if (vq->num_free > vq->size) return fail_queue(vq, "invalid free count");
    uint16_t const IDX = vq->free_head;
    if (IDX >= vq->size) return fail_queue(vq, "invalid free head");
    uint16_t const NEXT = vq->desc[IDX].next;
    if ((vq->num_free > 1 && (NEXT >= vq->size || NEXT == IDX)) || (vq->num_free == 1 && NEXT != VIRTQ_NO_BUF))
        return fail_queue(vq, "invalid free link");
    auto& slot = vq->packet_dma[IDX];
    if (vq->pkt_map[IDX] != nullptr || slot.submission.sequence != 0) return fail_queue(vq, "free descriptor still owned");
    auto const DIRECTION = vq->kind == QueueKind::receive ? dma::Direction::from_device : dma::Direction::to_device;
    if (dma::bind_streaming(slot.allocation.handle, buffer, DIRECTION) != dma::Error::none) return fail_queue(vq, "streaming bind failed");
    auto const SUBMISSION = dma::prepare(slot.allocation.handle);
    if (SUBMISSION.error != dma::Error::none) {
        // Bind succeeded under sole queue ownership; a failed prepare has not
        // exposed this packet. Consume that CPU borrow before caller frees it.
        (void)dma::quarantine(slot.allocation.handle);
        return fail_queue(vq, "streaming prepare failed");
    }
    slot.submission = SUBMISSION.submission;
    vq->free_head = (vq->num_free == 1) ? VIRTQ_NO_BUF : NEXT;
    --vq->num_free;
    vq->desc[IDX].addr = slot.submission.address;
#ifdef WOS_SELFTEST
    if (vq->kind == QueueKind::transmit && vq->selftest_address != 0) vq->desc[IDX].addr = vq->selftest_address;
#endif
    vq->desc[IDX].len = static_cast<uint32_t>(slot.submission.size);
    vq->desc[IDX].flags = vq->kind == QueueKind::receive ? VRING_DESC_F_WRITE : 0;
    vq->desc[IDX].next = 0;
    vq->pkt_map[IDX] = pkt;
    uint16_t const AVAIL_IDX = vq->avail->idx;
    vq->avail->ring[AVAIL_IDX % vq->size] = IDX;
    __atomic_thread_fence(__ATOMIC_RELEASE);
    vq->avail->idx = AVAIL_IDX + 1;
    return static_cast<int>(IDX);
}

auto virtq_get_buf(Virtqueue* vq, uint32_t* out_len) -> uint16_t {
    if (vq == nullptr || vq->size == 0 || vq->size > VIRTQ_MAX_SIZE || vq->kind == QueueKind::control ||
        vq->failed.load(std::memory_order_acquire))
        return VIRTQ_NO_BUF;
    // Ring backing is page-aligned: packed idx at byte 2 is an aligned x86
    // 16-bit load. Acquire follows the producer index, before reading its entry.
    uint16_t const USED_RING_IDX = static_cast<volatile VirtqUsed*>(vq->used)->idx;
    __atomic_thread_fence(__ATOMIC_ACQUIRE);
    if (vq->last_used_idx == USED_RING_IDX) return VIRTQ_NO_BUF;
    auto bad = [&](const char* reason) -> uint16_t {
        (void)fail_queue(vq, reason);
        return VIRTQ_BAD_BUF;
    };
    auto const PENDING = static_cast<uint16_t>(USED_RING_IDX - vq->last_used_idx);
    if (vq->num_free >= vq->size || PENDING > vq->size - vq->num_free) return bad("invalid used count");
    uint16_t const USED_IDX = vq->last_used_idx % vq->size;
    volatile auto const& elem = vq->used->ring[USED_IDX];
    uint32_t const DESC_ID = elem.id;
    uint32_t const LENGTH = elem.len;
    if (DESC_ID >= vq->size) return bad("invalid used descriptor");
    auto const IDX = static_cast<uint16_t>(DESC_ID);
    auto& slot = vq->packet_dma[IDX];
    if (vq->pkt_map[IDX] == nullptr || slot.submission.sequence == 0) return bad("duplicate or unowned completion");
    if (vq->kind == QueueKind::receive && LENGTH > slot.submission.size) return bad("oversized receive completion");
    // Legacy TX used lengths are unreliable; no device-writable TX bytes exist.
    size_t const COPIED = vq->kind == QueueKind::receive ? LENGTH : 0;
    if (dma::complete(slot.submission, COPIED, dma::Retirement::command_completed) != dma::Error::none)
        return bad("streaming completion failed");
    slot.submission = {};
    ++vq->last_used_idx;
    vq->desc[IDX].next = vq->free_head;
    vq->free_head = IDX;
    ++vq->num_free;
    if (out_len != nullptr) *out_len = vq->kind == QueueKind::receive ? LENGTH : 0;
    return IDX;
}

void virtq_kick(Virtqueue* vq) {
    if (vq->failed.load(std::memory_order_acquire)) return;
    __atomic_thread_fence(__ATOMIC_RELEASE);
    if (vq->notify_addr != nullptr)
        *vq->notify_addr = vq->queue_index;
    else
        ::outw(vq->io_base + VIRTIO_REG_QUEUE_NOTIFY, vq->queue_index);
}

}  // namespace ker::dev::virtio
