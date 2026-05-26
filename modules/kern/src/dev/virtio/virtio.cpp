#include "virtio.hpp"

#include <cstdint>
#include <cstring>
#include <mod/io/port/port.hpp>
#include <platform/mm/phys.hpp>

#include "net/packet.hpp"
#include "platform/dbg/dbg.hpp"

namespace ker::dev::virtio {

using log = ker::mod::dbg::logger<"virtio">;

namespace {
// Align value up to alignment boundary
constexpr size_t PAGE_SIZE = 4096;

constexpr auto align_up(size_t val, size_t align) -> size_t { return (val + align - 1) & ~(align - 1); }
}  // namespace

auto virtq_desc_size(uint16_t qsz) -> size_t { return sizeof(VirtqDesc) * qsz; }

auto virtq_avail_size(uint16_t qsz) -> size_t {
    return sizeof(uint16_t) * (3 + qsz);  // flags + idx + ring[qsz] + used_event
}

auto virtq_used_size(uint16_t qsz) -> size_t {
    return (sizeof(uint16_t) * 3) + (sizeof(VirtqUsedElem) * qsz);  // flags + idx + ring[qsz] + avail_event
}

auto virtq_total_size(uint16_t qsz) -> size_t {
    // Descriptor table, then available ring (aligned to 2), then used ring (aligned to 4096)
    size_t const DESC_AVAIL = align_up(virtq_desc_size(qsz) + virtq_avail_size(qsz), PAGE_SIZE);
    return DESC_AVAIL + align_up(virtq_used_size(qsz), PAGE_SIZE);
}

auto virtq_alloc(uint16_t size) -> Virtqueue* {
    if (size == 0 || size > VIRTQ_MAX_SIZE) {
        log::warn("virtq_alloc: invalid size %u (max %u)", size, VIRTQ_MAX_SIZE);
        return nullptr;
    }

    log::debug("virtq_alloc: sizeof(Virtqueue)=%u, using new", sizeof(Virtqueue));
    // Allocate the Virtqueue control structure with zero initialization
    auto* vq = new Virtqueue{};

    // Allocate physically contiguous memory for descriptor table + rings
    size_t const TOTAL = virtq_total_size(size);
    size_t const ALLOC_BYTES = align_up(TOTAL, PAGE_SIZE);

    log::debug("virtq_alloc: size=%u, total=%zu bytes, alloc=%zu bytes", size, TOTAL, ALLOC_BYTES);

    auto* mem = ker::mod::mm::phys::page_alloc(ALLOC_BYTES);
    if (mem == nullptr) {
        log::warn("virtq_alloc: page_alloc(%zu) failed", ALLOC_BYTES);
        delete vq;
        return nullptr;
    }
    std::memset(mem, 0, ALLOC_BYTES);

    auto* base = reinterpret_cast<uint8_t*>(mem);

    vq->size = size;
    vq->num_free = size;
    vq->free_head = 0;
    vq->last_used_idx = 0;

    // Layout: desc table | avail ring | (align 4096) | used ring
    vq->desc = reinterpret_cast<VirtqDesc*>(base);
    vq->avail = reinterpret_cast<VirtqAvail*>(base + virtq_desc_size(size));

    size_t const USED_OFFSET = align_up(virtq_desc_size(size) + virtq_avail_size(size), PAGE_SIZE);
    vq->used = reinterpret_cast<VirtqUsed*>(base + USED_OFFSET);

    // Initialize free list: each descriptor points to the next
    for (uint16_t i = 0; i < size - 1; i++) {
        vq->desc[i].next = i + 1;
    }
    vq->desc[size - 1].next = 0xFFFF;  // End of free list

    // Clear packet map
    for (auto*& pkt : vq->pkt_map) {
        pkt = nullptr;
    }

    return vq;
}

auto virtq_add_buf(Virtqueue* vq, uint64_t phys, uint32_t len, uint16_t flags, ker::net::PacketBuffer* pkt) -> int {
    if (vq->num_free == 0) {
        return -1;
    }

    uint16_t const IDX = vq->free_head;
    vq->free_head = vq->desc[IDX].next;
    vq->num_free--;

    vq->desc[IDX].addr = phys;
    vq->desc[IDX].len = len;
    vq->desc[IDX].flags = flags;
    vq->desc[IDX].next = 0;

    vq->pkt_map.at(IDX) = pkt;

    // Add to available ring
    uint16_t const AVAIL_IDX = vq->avail->idx;
    vq->avail->ring[AVAIL_IDX % vq->size] = IDX;
    // Memory barrier: ensure descriptor and ring entries are written before idx update
    __atomic_thread_fence(__ATOMIC_RELEASE);
    vq->avail->idx = AVAIL_IDX + 1;

    return static_cast<int>(IDX);
}

auto virtq_get_buf(Virtqueue* vq, uint32_t* out_len) -> uint16_t {
    // Check if there are consumed buffers
    if (vq->last_used_idx == vq->used->idx) {
        return 0xFFFF;  // No buffers
    }

    __atomic_thread_fence(__ATOMIC_ACQUIRE);

    uint16_t const USED_IDX = vq->last_used_idx % vq->size;
    auto& elem = vq->used->ring[USED_IDX];

    auto desc_idx = static_cast<uint16_t>(elem.id);
    if (out_len != nullptr) {
        *out_len = elem.len;
    }

    // Return descriptor to free list
    vq->desc[desc_idx].next = vq->free_head;
    vq->free_head = desc_idx;
    vq->num_free++;

    vq->last_used_idx++;
    return desc_idx;
}

void virtq_kick(Virtqueue* vq) {
    __atomic_thread_fence(__ATOMIC_RELEASE);
    if (vq->notify_addr != nullptr) {
        *vq->notify_addr = vq->queue_index;
    } else {
        ::outw(vq->io_base + VIRTIO_REG_QUEUE_NOTIFY, vq->queue_index);
    }
}

}  // namespace ker::dev::virtio
