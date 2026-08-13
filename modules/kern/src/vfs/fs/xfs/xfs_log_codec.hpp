#pragma once

#include <cstddef>
#include <cstdint>
#include <vfs/fs/xfs/xfs_format.hpp>

namespace ker::vfs::xfs {

constexpr uint32_t WOS_XLOG_BODY_MAGIC = 0x574A4E4CU;  // "WJNL"
constexpr uint16_t WOS_XLOG_BODY_VERSION = 1;
constexpr uint32_t WOS_XLOG_BODY_FLAG_CLEAN = 1U << 0;
constexpr uint16_t WOS_XLOG_ITEM_BUFFER = 1;

struct WosLogBodyHeader {
    Be32 magic;
    Be16 version;
    Be16 header_bytes;
    Be32 flags;
    Be32 item_count;
    Be32 body_bytes;
    Be32 checksum;
    Be64 record_lsn;
} __attribute__((packed));
static_assert(sizeof(WosLogBodyHeader) == 32);
static_assert(offsetof(WosLogBodyHeader, checksum) == 20);

struct WosLogBufferItemHeader {
    Be16 type;
    Be16 flags;
    Be32 header_bytes;
    Be64 target_block;
    Be32 target_blocks;
    Be32 data_offset;
    Be32 data_bytes;
    Be32 reserved;
} __attribute__((packed));
static_assert(sizeof(WosLogBufferItemHeader) == 32);

struct WosLogBufferItemView {
    uint64_t target_block{};
    uint32_t target_blocks{};
    uint32_t data_offset{};
    uint32_t data_bytes{};
    const uint8_t* data{};
};

struct WosLogBodyCursor {
    const uint8_t* body{};
    size_t body_bytes{};
    size_t offset{};
    uint32_t item_count{};
    uint32_t emitted{};
    uint32_t flags{};
};

auto wos_log_body_begin(uint8_t* body, size_t capacity, uint32_t flags, uint32_t item_count, uint64_t record_lsn, size_t body_bytes) -> int;
auto wos_log_body_append_buffer(uint8_t* body, size_t body_bytes, size_t* offset, uint64_t target_block, uint32_t target_blocks,
                                uint32_t data_offset, const uint8_t* data, uint32_t data_bytes) -> int;
auto wos_log_body_finalize(uint8_t* body, size_t body_bytes, size_t final_offset) -> int;

auto wos_log_body_cursor_init(const uint8_t* body, size_t body_bytes, uint64_t expected_lsn, WosLogBodyCursor* cursor) -> int;
auto wos_log_body_cursor_next(WosLogBodyCursor* cursor, WosLogBufferItemView* item) -> int;
auto wos_log_body_cursor_finish(const WosLogBodyCursor* cursor) -> int;

// XFS stores the outer record checksum little-endian.  It covers the fixed
// header prefix through h_pad0 (with h_crc zeroed) and the exact body bytes.
auto wos_log_record_crc(const XlogRecHeader& header, const uint8_t* body, size_t body_bytes) -> uint32_t;

}  // namespace ker::vfs::xfs
