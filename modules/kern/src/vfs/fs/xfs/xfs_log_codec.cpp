#include "xfs_log_codec.hpp"

#include <cerrno>
#include <cstring>
#include <util/crc32c.hpp>

namespace ker::vfs::xfs {

auto wos_log_body_begin(uint8_t* body, size_t capacity, uint32_t flags, uint32_t item_count, uint64_t record_lsn, size_t body_bytes)
    -> int {
    if (body == nullptr || body_bytes < sizeof(WosLogBodyHeader) || body_bytes > capacity || body_bytes > UINT32_MAX ||
        (flags & ~WOS_XLOG_BODY_FLAG_CLEAN) != 0 || (((flags & WOS_XLOG_BODY_FLAG_CLEAN) != 0) != (item_count == 0))) {
        return -EINVAL;
    }
    __builtin_memset(body, 0, body_bytes);
    auto* header = reinterpret_cast<WosLogBodyHeader*>(body);
    header->magic = Be32::from_cpu(WOS_XLOG_BODY_MAGIC);
    header->version = Be16::from_cpu(WOS_XLOG_BODY_VERSION);
    header->header_bytes = Be16::from_cpu(sizeof(WosLogBodyHeader));
    header->flags = Be32::from_cpu(flags);
    header->item_count = Be32::from_cpu(item_count);
    header->body_bytes = Be32::from_cpu(static_cast<uint32_t>(body_bytes));
    header->record_lsn = Be64::from_cpu(record_lsn);
    return 0;
}

auto wos_log_body_append_buffer(uint8_t* body, size_t body_bytes, size_t* offset, uint64_t target_block, uint32_t target_blocks,
                                uint32_t data_offset, const uint8_t* data, uint32_t data_bytes) -> int {
    if (body == nullptr || offset == nullptr || data == nullptr || target_blocks == 0 || data_bytes == 0 || *offset > body_bytes ||
        sizeof(WosLogBufferItemHeader) > body_bytes - *offset || data_bytes > body_bytes - *offset - sizeof(WosLogBufferItemHeader)) {
        return -EINVAL;
    }
    auto* item = reinterpret_cast<WosLogBufferItemHeader*>(body + *offset);
    *item = {};
    item->type = Be16::from_cpu(WOS_XLOG_ITEM_BUFFER);
    item->header_bytes = Be32::from_cpu(sizeof(WosLogBufferItemHeader));
    item->target_block = Be64::from_cpu(target_block);
    item->target_blocks = Be32::from_cpu(target_blocks);
    item->data_offset = Be32::from_cpu(data_offset);
    item->data_bytes = Be32::from_cpu(data_bytes);
    *offset += sizeof(WosLogBufferItemHeader);
    __builtin_memcpy(body + *offset, data, data_bytes);
    *offset += data_bytes;
    return 0;
}

auto wos_log_body_finalize(uint8_t* body, size_t body_bytes, size_t final_offset) -> int {
    if (body == nullptr || body_bytes < sizeof(WosLogBodyHeader) || final_offset != body_bytes) {
        return -EINVAL;
    }
    auto* header = reinterpret_cast<WosLogBodyHeader*>(body);
    header->checksum = Be32{};
    header->checksum = Be32::from_cpu(ker::util::crc32c_block_with_cksum(body, body_bytes, offsetof(WosLogBodyHeader, checksum)));
    return 0;
}

auto wos_log_body_cursor_init(const uint8_t* body, size_t body_bytes, uint64_t expected_lsn, WosLogBodyCursor* cursor) -> int {
    if (body == nullptr || cursor == nullptr || body_bytes < sizeof(WosLogBodyHeader) || body_bytes > UINT32_MAX) {
        return -EINVAL;
    }
    const auto* header = reinterpret_cast<const WosLogBodyHeader*>(body);
    uint32_t const FLAGS = header->flags.to_cpu();
    uint32_t const ITEM_COUNT = header->item_count.to_cpu();
    if (header->magic.to_cpu() != WOS_XLOG_BODY_MAGIC || header->version.to_cpu() != WOS_XLOG_BODY_VERSION ||
        header->header_bytes.to_cpu() != sizeof(WosLogBodyHeader) || header->body_bytes.to_cpu() != body_bytes ||
        header->record_lsn.to_cpu() != expected_lsn || (FLAGS & ~WOS_XLOG_BODY_FLAG_CLEAN) != 0 ||
        (((FLAGS & WOS_XLOG_BODY_FLAG_CLEAN) != 0) != (ITEM_COUNT == 0))) {
        return -EUCLEAN;
    }
    uint32_t const EXPECTED = header->checksum.to_cpu();
    uint32_t const ACTUAL = ker::util::crc32c_block_with_cksum(body, body_bytes, offsetof(WosLogBodyHeader, checksum));
    if (EXPECTED == 0 || ACTUAL != EXPECTED) {
        return -EUCLEAN;
    }
    *cursor = {
        .body = body,
        .body_bytes = body_bytes,
        .offset = sizeof(WosLogBodyHeader),
        .item_count = ITEM_COUNT,
        .emitted = 0,
        .flags = FLAGS,
    };
    return 0;
}

auto wos_log_body_cursor_next(WosLogBodyCursor* cursor, WosLogBufferItemView* item) -> int {
    if (cursor == nullptr || item == nullptr || cursor->body == nullptr || cursor->emitted >= cursor->item_count ||
        cursor->offset > cursor->body_bytes || sizeof(WosLogBufferItemHeader) > cursor->body_bytes - cursor->offset) {
        return -EUCLEAN;
    }
    const auto* header = reinterpret_cast<const WosLogBufferItemHeader*>(cursor->body + cursor->offset);
    uint32_t const DATA_BYTES = header->data_bytes.to_cpu();
    if (header->type.to_cpu() != WOS_XLOG_ITEM_BUFFER || header->flags.to_cpu() != 0 ||
        header->header_bytes.to_cpu() != sizeof(WosLogBufferItemHeader) || header->target_blocks.to_cpu() == 0 || DATA_BYTES == 0 ||
        header->reserved.to_cpu() != 0 || DATA_BYTES > cursor->body_bytes - cursor->offset - sizeof(WosLogBufferItemHeader)) {
        return -EUCLEAN;
    }
    cursor->offset += sizeof(WosLogBufferItemHeader);
    *item = {
        .target_block = header->target_block.to_cpu(),
        .target_blocks = header->target_blocks.to_cpu(),
        .data_offset = header->data_offset.to_cpu(),
        .data_bytes = DATA_BYTES,
        .data = cursor->body + cursor->offset,
    };
    cursor->offset += DATA_BYTES;
    cursor->emitted++;
    return 0;
}

auto wos_log_body_cursor_finish(const WosLogBodyCursor* cursor) -> int {
    if (cursor == nullptr || cursor->body == nullptr || cursor->emitted != cursor->item_count || cursor->offset != cursor->body_bytes) {
        return -EUCLEAN;
    }
    return 0;
}

auto wos_log_record_crc(const XlogRecHeader& header, const uint8_t* body, size_t body_bytes) -> uint32_t {
    uint32_t zero = 0;
    const auto* bytes = reinterpret_cast<const uint8_t*>(&header);
    constexpr size_t CRC_OFFSET = offsetof(XlogRecHeader, h_crc);
    uint32_t crc = ker::util::crc32c(ker::util::CRC32C_SEED, bytes, CRC_OFFSET);
    crc = ker::util::crc32c(crc, &zero, sizeof(zero));
    crc = ker::util::crc32c(crc, bytes + CRC_OFFSET + sizeof(zero), XLOG_REC_CRC_HEADER_BYTES - CRC_OFFSET - sizeof(zero));
    crc = ker::util::crc32c(crc, body, body_bytes);
    return ker::util::crc32c_final(crc);
}

}  // namespace ker::vfs::xfs
