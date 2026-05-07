// LibFuzzer target for slab allocator usage patterns.
//
// Uses the host kmalloc shim (→ libc malloc) so the patterns are identical
// to kernel code, and AddressSanitizer catches UAF / overflow / double-free.
//
// Each fuzz input is a byte stream of (opcode, slot, optional_extra) triples
// that drive alloc/write/free operations on a fixed-size live-pointer array.

#include <platform/mm/dyn/kmalloc.hpp>

#include <cstddef>
#include <cstdint>
#include <cstring>

namespace km = ker::mod::mm::dyn::kmalloc;

// Canonical slab size classes mirroring the kernel slab.opt.cpp table.
static constexpr size_t kSizeClasses[] = {
    0x10, 0x20, 0x40, 0x80, 0x100, 0x200, 0x300, 0x400, 0x800,
};
static constexpr size_t kNumClasses = sizeof(kSizeClasses) / sizeof(kSizeClasses[0]);

// Number of live-pointer slots (deterministic, not heap-allocated)
static constexpr size_t kMaxLive = 32;

// Operation opcodes (3 bits)
enum Op : uint8_t {
    OP_ALLOC      = 0,  // malloc size_class[data & 0xF]
    OP_FREE       = 1,  // free slot
    OP_WRITE_BYTE = 2,  // write 1 byte to allocated region (probes for OOB/UAF)
    OP_ALLOC_ZERO = 3,  // calloc-style: malloc then memset 0
    OP_REALLOC    = 4,  // realloc with next size class
    OP_ALLOC_TINY = 5,  // fixed 1-byte malloc (stress tiny allocator path)
    OP_ALLOC_BIG  = 6,  // fixed 0x800-byte malloc
    OP_NOP        = 7,
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    void* ptrs[kMaxLive]  = {};
    size_t sizes[kMaxLive] = {};

    size_t i = 0;
    while (i < size) {
        uint8_t ctrl = data[i++];
        uint8_t op   = ctrl & 0x07;
        uint8_t slot = (ctrl >> 3) & 0x1F;  // 0..31

        switch (static_cast<Op>(op)) {
            case OP_ALLOC: {
                size_t cls = (i < size ? data[i++] : 0) % kNumClasses;
                size_t sz  = kSizeClasses[cls];
                if (ptrs[slot] == nullptr) {
                    ptrs[slot]  = km::malloc(sz);
                    sizes[slot] = sz;
                    if (ptrs[slot]) {
                        memset(ptrs[slot], 0xAB, sz);
                    }
                }
                break;
            }
            case OP_FREE: {
                km::free(ptrs[slot]);
                ptrs[slot]  = nullptr;
                sizes[slot] = 0;
                break;
            }
            case OP_WRITE_BYTE: {
                if (ptrs[slot] && sizes[slot] > 0) {
                    size_t off = (i < size ? data[i++] : 0) % sizes[slot];
                    static_cast<uint8_t*>(ptrs[slot])[off] = 0x42;
                }
                break;
            }
            case OP_ALLOC_ZERO: {
                size_t cls = (i < size ? data[i++] : 0) % kNumClasses;
                size_t sz  = kSizeClasses[cls];
                if (ptrs[slot] == nullptr) {
                    ptrs[slot] = km::malloc(sz);
                    if (ptrs[slot]) {
                        memset(ptrs[slot], 0, sz);
                        sizes[slot] = sz;
                    }
                }
                break;
            }
            case OP_REALLOC: {
                if (ptrs[slot] != nullptr) {
                    size_t cls     = (i < size ? data[i++] : 0) % kNumClasses;
                    size_t new_sz  = kSizeClasses[cls];
                    void* new_ptr  = km::realloc(ptrs[slot], new_sz);
                    if (new_ptr) {
                        ptrs[slot]  = new_ptr;
                        sizes[slot] = new_sz;
                    }
                }
                break;
            }
            case OP_ALLOC_TINY: {
                if (ptrs[slot] == nullptr) {
                    ptrs[slot]  = km::malloc(1);
                    sizes[slot] = 1;
                }
                break;
            }
            case OP_ALLOC_BIG: {
                if (ptrs[slot] == nullptr) {
                    ptrs[slot]  = km::malloc(0x800);
                    sizes[slot] = 0x800;
                    if (ptrs[slot]) {
                        memset(ptrs[slot], 0xFF, 0x800);
                    }
                }
                break;
            }
            case OP_NOP:
            default:
                break;
        }
    }

    // Free all live pointers to avoid false positives in leak detection.
    for (size_t j = 0; j < kMaxLive; ++j) {
        km::free(ptrs[j]);
        ptrs[j]  = nullptr;
        sizes[j] = 0;
    }

    return 0;
}
