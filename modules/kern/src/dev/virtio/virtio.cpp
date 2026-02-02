#include "virtio.hpp"

#include <cstring>
#include <mod/io/port/port.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/dyn/kmalloc.hpp>
#include <platform/mm/phys.hpp>

#include "platform/dbg/dbg.hpp"

namespace ker::dev::virtio {

namespace {
// Align value up to alignment boundary
auto align_up(size_t val, size_t align) -> size_t { return (val + align - 1) & ~(align - 1); }
}  // namespace

auto virtq_desc_size(uint16_t qsz) -> size_t { return sizeof(VirtqDesc) * qsz; }

auto virtq_avail_size(uint16_t qsz) -> size_t {
    return sizeof(uint16_t) * (3 + qsz);  // flags + idx + ring[qsz] + used_event
}

auto virtq_used_size(uint16_t qsz) -> size_t {
    return sizeof(uint16_t) * 3 + sizeof(VirtqUsedElem) * qsz;  // flags + idx + ring[qsz] + avail_event
}

auto virtq_total_size(uint16_t qsz) -> size_t {
    // Descriptor table, then available ring (aligned to 2), then used ring (aligned to 4096)
    size_t desc_avail = align_up(virtq_desc_size(qsz) + virtq_avail_size(qsz), 4096);
    return desc_avail + align_up(virtq_used_size(qsz), 4096);
}

auto virtq_alloc(uint16_t size) -> Virtqueue* {
    if (size == 0 || size > VIRTQ_MAX_SIZE) {
        mod::dbg::log("virtq_alloc: invalid size %u (max %u)", size, VIRTQ_MAX_SIZE);
        return nullptr;
    }

    mod::dbg::log("virtq_alloc: sizeof(Virtqueue)=%u, using new", sizeof(Virtqueue));
    // Allocate the Virtqueue control structure with zero initialization
    auto* vq = new Virtqueue{};

    // Allocate physically contiguous memory for descriptor table + rings
    size_t total = virtq_total_size(size);
    size_t pages_needed = align_up(total, 4096);

    mod::dbg::log("virtq_alloc: size=%u, total=%u bytes, pages=%u", size, total, pages_needed);

    auto* mem = ker::mod::mm::phys::pageAlloc(pages_needed);
    if (mem == nullptr) {
        mod::dbg::log("virtq_alloc: pageAlloc(%u) failed", pages_needed);
        delete vq;
        return nullptr;
    }
    std::memset(mem, 0, pages_needed);

    auto* base = reinterpret_cast<uint8_t*>(mem);

    vq->size = size;
    vq->num_free = size;
    vq->free_head = 0;
    vq->last_used_idx = 0;

    // Layout: desc table | avail ring | (align 4096) | used ring
    vq->desc = reinterpret_cast<VirtqDesc*>(base);
    vq->avail = reinterpret_cast<VirtqAvail*>(base + virtq_desc_size(size));

    size_t used_offset = align_up(virtq_desc_size(size) + virtq_avail_size(size), 4096);
    vq->used = reinterpret_cast<VirtqUsed*>(base + used_offset);

    // Initialize free list: each descriptor points to the next
    for (uint16_t i = 0; i < size - 1; i++) {
        vq->desc[i].next = i + 1;
    }
    vq->desc[size - 1].next = 0xFFFF;  // End of free list

    // Clear packet map
    std::memset(vq->pkt_map, 0, sizeof(vq->pkt_map));

    return vq;
}

auto virtq_add_buf(Virtqueue* vq, uint64_t phys, uint32_t len, uint16_t flags, ker::net::PacketBuffer* pkt) -> int {
    if (vq->num_free == 0) {
        return -1;
    }

    uint16_t idx = vq->free_head;
    vq->free_head = vq->desc[idx].next;
    vq->num_free--;

    vq->desc[idx].addr = phys;
    vq->desc[idx].len = len;
    vq->desc[idx].flags = flags;
    vq->desc[idx].next = 0;

    vq->pkt_map[idx] = pkt;

    // Add to available ring
    uint16_t avail_idx = vq->avail->idx;
    vq->avail->ring[avail_idx % vq->size] = idx;
    // Memory barrier: ensure descriptor and ring entries are written before idx update
    __atomic_thread_fence(__ATOMIC_RELEASE);
    vq->avail->idx = avail_idx + 1;

    return static_cast<int>(idx);
}

auto virtq_get_buf(Virtqueue* vq, uint32_t* out_len) -> uint16_t {
    // Check if there are consumed buffers
    if (vq->last_used_idx == vq->used->idx) {
        return 0xFFFF;  // No buffers
    }

    __atomic_thread_fence(__ATOMIC_ACQUIRE);

    uint16_t used_idx = vq->last_used_idx % vq->size;
    auto& elem = vq->used->ring[used_idx];

    uint16_t desc_idx = static_cast<uint16_t>(elem.id);
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
    ::outw(vq->io_base + VIRTIO_REG_QUEUE_NOTIFY, vq->queue_index);
}

}  // namespace ker::dev::virtio
