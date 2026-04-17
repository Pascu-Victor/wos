// LibFuzzer target for kernel data structures (RadixTree, SmallVec).
//
// Interprets fuzz input as a sequence of operations (insert, remove, lookup)
// and drives the data structures with them.  ASan catches use-after-free,
// buffer overflows, and other memory errors.

#include <util/radix_tree.hpp>
#include <util/smallvec.hpp>
#include <cstddef>
#include <cstdint>

using namespace ker::util;

// Operation opcodes
enum Op : uint8_t {
    OP_RADIX_INSERT = 0,
    OP_RADIX_REMOVE = 1,
    OP_RADIX_LOOKUP = 2,
    OP_RADIX_FIND_UNSET = 3,
    OP_VEC_PUSH = 4,
    OP_VEC_POP = 5,
    OP_VEC_REMOVE_AT = 6,
    OP_VEC_CLEAR = 7,
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    RadixTree<uintptr_t> tree;
    SmallVec<uint32_t, 8> vec;

    // Sentinel values to insert into the radix tree (not real pointers,
    // just non-zero values that satisfy the T{} != T check)
    constexpr uintptr_t SENTINEL_BASE = 0xDEAD0000;

    size_t i = 0;
    while (i < size) {
        uint8_t op = data[i++] & 0x07; // 8 ops

        switch (op) {
            case OP_RADIX_INSERT: {
                if (i + 2 > size) break;
                uint16_t key = static_cast<uint16_t>(data[i] | (data[i + 1] << 8));
                i += 2;
                // Use a bounded key to keep tree depth reasonable
                uint64_t bounded_key = key & 0x3FF; // max 1024
                (void)tree.insert(bounded_key, SENTINEL_BASE + bounded_key);
                break;
            }
            case OP_RADIX_REMOVE: {
                if (i + 2 > size) break;
                uint16_t key = static_cast<uint16_t>(data[i] | (data[i + 1] << 8));
                i += 2;
                uint64_t bounded_key = key & 0x3FF;
                tree.remove(bounded_key);
                break;
            }
            case OP_RADIX_LOOKUP: {
                if (i + 2 > size) break;
                uint16_t key = static_cast<uint16_t>(data[i] | (data[i + 1] << 8));
                i += 2;
                uint64_t bounded_key = key & 0x3FF;
                volatile auto val = tree.lookup(bounded_key);
                (void)val;
                break;
            }
            case OP_RADIX_FIND_UNSET: {
                if (i + 1 > size) break;
                uint8_t start = data[i++];
                volatile auto key = tree.find_first_unset(start);
                (void)key;
                break;
            }
            case OP_VEC_PUSH: {
                if (i + 4 > size) break;
                uint32_t val = static_cast<uint32_t>(data[i]) |
                               (static_cast<uint32_t>(data[i + 1]) << 8) |
                               (static_cast<uint32_t>(data[i + 2]) << 16) |
                               (static_cast<uint32_t>(data[i + 3]) << 24);
                i += 4;
                // Limit vec size to prevent OOM in fuzzer
                if (vec.size() < 256) {
                    (void)vec.push_back(val);
                }
                break;
            }
            case OP_VEC_POP: {
                vec.pop_back();
                break;
            }
            case OP_VEC_REMOVE_AT: {
                if (i + 1 > size) break;
                uint8_t idx = data[i++];
                if (vec.size() > 0) {
                    vec.remove_at(idx % vec.size());
                }
                break;
            }
            case OP_VEC_CLEAR: {
                vec.clear();
                break;
            }
        }
    }

    return 0;
}
