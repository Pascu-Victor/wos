#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dev/block_device.hpp>
#include <platform/mm/phys.hpp>
#include <test/ktest.hpp>

namespace {

namespace phys = ker::mod::mm::phys;

constexpr size_t BLOCK_SIZE = 512;
constexpr size_t IO_BYTES = 16 * 1024 * 1024;
constexpr size_t IO_BLOCKS = IO_BYTES / BLOCK_SIZE;

struct BlockIoChunkState {
    size_t calls = 0;
    uint64_t first_block = UINT64_MAX;
    size_t max_count = 0;
};

auto record_chunk(BlockIoChunkState& state, uint64_t block, size_t count) -> int {
    if (state.calls == 0) {
        state.first_block = block;
    }
    if (count > state.max_count) {
        state.max_count = count;
    }
    state.calls++;
    return 0;
}

auto chunking_read(ker::dev::BlockDevice* dev, uint64_t block, size_t count, void* buffer) -> int {
    if (dev == nullptr || dev->private_data == nullptr || buffer == nullptr) {
        return -1;
    }
    std::memset(buffer, 0xA5, count * dev->block_size);
    return record_chunk(*static_cast<BlockIoChunkState*>(dev->private_data), block, count);
}

auto chunking_write(ker::dev::BlockDevice* dev, uint64_t block, size_t count, const void* buffer) -> int {
    if (dev == nullptr || dev->private_data == nullptr || buffer == nullptr) {
        return -1;
    }
    return record_chunk(*static_cast<BlockIoChunkState*>(dev->private_data), block, count);
}

auto make_chunking_bdev(BlockIoChunkState* state) -> ker::dev::BlockDevice {
    ker::dev::BlockDevice dev{};
    dev.block_size = BLOCK_SIZE;
    dev.total_blocks = 65536;
    dev.read_blocks = chunking_read;
    dev.write_blocks = chunking_write;
    dev.private_data = state;
    return dev;
}

auto alloc_io_buffer() -> uint8_t* { return static_cast<uint8_t*>(phys::page_alloc(IO_BYTES, "block_io_chunk_ktest")); }

}  // namespace

KTEST(BlockDevice, SixteenMiBReadStaysSingleDriverRequest) {
    BlockIoChunkState state{};
    ker::dev::BlockDevice dev = make_chunking_bdev(&state);

    uint8_t* const buffer = alloc_io_buffer();
    KREQUIRE_NE(buffer, nullptr);

    KEXPECT_EQ(ker::dev::block_read(&dev, 128, IO_BLOCKS, buffer), 0);
    KEXPECT_EQ(state.calls, static_cast<size_t>(1));
    KEXPECT_EQ(state.first_block, static_cast<uint64_t>(128));
    KEXPECT_EQ(state.max_count, IO_BLOCKS);

    phys::page_free(buffer);
}

KTEST(BlockDevice, SixteenMiBWriteStaysSingleDriverRequest) {
    BlockIoChunkState state{};
    ker::dev::BlockDevice dev = make_chunking_bdev(&state);

    uint8_t* const buffer = alloc_io_buffer();
    KREQUIRE_NE(buffer, nullptr);

    KEXPECT_EQ(ker::dev::block_write(&dev, 256, IO_BLOCKS, buffer), 0);
    KEXPECT_EQ(state.calls, static_cast<size_t>(1));
    KEXPECT_EQ(state.first_block, static_cast<uint64_t>(256));
    KEXPECT_EQ(state.max_count, IO_BLOCKS);

    phys::page_free(buffer);
}
