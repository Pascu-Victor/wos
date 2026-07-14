#include <cstddef>
#include <cstdint>
#include <cstring>
#include <dev/block_device.hpp>
#include <platform/mm/phys.hpp>
#include <test/ktest.hpp>
#include <utility>

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

KTEST(BlockWriterLease, RemoteIsExclusiveWhileLocalMountsMayCoexist) {
    ker::dev::BlockDevice disk{};
    disk.total_blocks = 1024;

    ker::dev::BlockDevice first_partition{};
    first_partition.is_partition = true;
    first_partition.parent_disk = &disk;
    first_partition.partition_start_lba = 0;
    first_partition.partition_end_lba = 255;

    ker::dev::BlockDevice second_partition{};
    second_partition.is_partition = true;
    second_partition.parent_disk = &disk;
    second_partition.partition_start_lba = 256;
    second_partition.partition_end_lba = 511;

    ker::dev::BlockWriterLease first_local;
    ker::dev::BlockWriterLease second_local;
    ker::dev::BlockWriterLease overlapping_remote;
    ker::dev::BlockWriterLease disjoint_remote;
    ker::dev::BlockWriterLease remote_contender;

    KREQUIRE_TRUE(first_local.try_acquire(&first_partition, ker::dev::BlockWriterLeaseOwner::LOCAL_MOUNT));
    KEXPECT_TRUE(second_local.try_acquire(&first_partition, ker::dev::BlockWriterLeaseOwner::LOCAL_MOUNT));
    KEXPECT_FALSE(overlapping_remote.try_acquire(&first_partition, ker::dev::BlockWriterLeaseOwner::REMOTE_BINDING));
    KREQUIRE_TRUE(disjoint_remote.try_acquire(&second_partition, ker::dev::BlockWriterLeaseOwner::REMOTE_BINDING));
    KEXPECT_FALSE(remote_contender.try_acquire(&second_partition, ker::dev::BlockWriterLeaseOwner::REMOTE_BINDING));

    ker::dev::BlockWriterLease moved_remote(std::move(disjoint_remote));
    KEXPECT_FALSE(disjoint_remote.active());
    KEXPECT_TRUE(moved_remote.active());
    KEXPECT_FALSE(remote_contender.try_acquire(&second_partition, ker::dev::BlockWriterLeaseOwner::REMOTE_BINDING));

    moved_remote.release();
    KEXPECT_TRUE(remote_contender.try_acquire(&second_partition, ker::dev::BlockWriterLeaseOwner::REMOTE_BINDING));
}
