#include "usercopy.hpp"

#include <cstdint>
#include <cstring>

#include "platform/mm/addr.hpp"
#include "platform/mm/paging.hpp"
#include "platform/mm/virt.hpp"
#include "platform/sched/task.hpp"
#include "util/fast_copy.hpp"

namespace ker::mod::sys::usercopy {
namespace {

[[nodiscard]] auto copy_chunk(size_t remaining, uint64_t user_addr) -> size_t {
    uint64_t const PAGE_OFFSET = user_addr & (mm::paging::PAGE_SIZE - 1);
    uint64_t const PAGE_REMAINING = mm::paging::PAGE_SIZE - PAGE_OFFSET;
    return remaining < PAGE_REMAINING ? remaining : static_cast<size_t>(PAGE_REMAINING);
}

[[nodiscard]] auto copy_to_task_common(sched::task::Task& task, uint64_t user_addr, const void* src, size_t size, bool require_writable)
    -> bool {
    if (size == 0) {
        return true;
    }
    if (task.pagemap == nullptr || src == nullptr || !range_valid(user_addr, size)) {
        return false;
    }

    auto const* in = static_cast<const uint8_t*>(src);
    size_t copied = 0;
    bool const ACTIVE_COPY = mm::virt::active_pagemap_is(task.pagemap);
    bool const ACTIVE_WRITABLE_COPY = require_writable && ACTIVE_COPY;
    bool const ACTIVE_MAPPED_WRITABLE_COPY = !require_writable && ACTIVE_COPY;
    while (copied < size) {
        uint64_t const CUR = user_addr + copied;
        size_t const CHUNK = copy_chunk(size - copied, CUR);
        if (require_writable) {
            bool const PAGE_ALREADY_WRITABLE = ACTIVE_WRITABLE_COPY && mm::virt::user_page_writable_now(task.pagemap, CUR);
            if (!PAGE_ALREADY_WRITABLE && !mm::virt::ensure_user_page_writable(&task, CUR)) {
                return false;
            }
        }
        bool const CAN_COPY_TO_ACTIVE_USER =
            ACTIVE_WRITABLE_COPY || (ACTIVE_MAPPED_WRITABLE_COPY && mm::virt::user_page_writable_now(task.pagemap, CUR));
        if (CAN_COPY_TO_ACTIVE_USER) {
            auto* dst = reinterpret_cast<uint8_t*>(CUR);
            ker::util::copy_fast(dst, in + copied, CHUNK);
            copied += CHUNK;
            continue;
        }

        uint64_t const PHYS = mm::virt::translate(task.pagemap, CUR);
        if (PHYS == mm::virt::PADDR_INVALID) {
            return false;
        }
        auto* dst = reinterpret_cast<uint8_t*>(mm::addr::get_virt_pointer(PHYS));
        ker::util::copy_fast(dst, in + copied, CHUNK);
        copied += CHUNK;
    }
    return true;
}

}  // namespace

auto range_valid(uint64_t user_addr, size_t size) -> bool {
    if (size == 0) {
        return user_addr <= USER_ADDR_LIMIT;
    }

    uint64_t end = 0;
    if (__builtin_add_overflow(user_addr, static_cast<uint64_t>(size), &end)) {
        return false;
    }
    return user_addr < USER_ADDR_LIMIT && end <= USER_ADDR_LIMIT;
}

auto ensure_writable(sched::task::Task& task, uint64_t user_addr, size_t size) -> bool {
    if (size == 0) {
        return range_valid(user_addr, size);
    }
    if (task.pagemap == nullptr || !range_valid(user_addr, size)) {
        return false;
    }

    uint64_t const END = user_addr + static_cast<uint64_t>(size);
    for (uint64_t page = user_addr; page < END;) {
        if (!mm::virt::ensure_user_page_writable(&task, page)) {
            return false;
        }

        uint64_t const NEXT_PAGE = (page + mm::paging::PAGE_SIZE) & ~(mm::paging::PAGE_SIZE - 1);
        if (NEXT_PAGE <= page) {
            return false;
        }
        page = NEXT_PAGE;
    }
    return true;
}

auto copy_from_task(sched::task::Task& task, uint64_t user_addr, void* dst, size_t size) -> bool {
    if (size == 0) {
        return true;
    }
    if (task.pagemap == nullptr || dst == nullptr || !range_valid(user_addr, size)) {
        return false;
    }

    auto* out = static_cast<uint8_t*>(dst);
    size_t copied = 0;
    bool const ACTIVE_COPY = mm::virt::active_pagemap_is(task.pagemap);
    while (copied < size) {
        uint64_t const CUR = user_addr + copied;
        size_t const CHUNK = copy_chunk(size - copied, CUR);
        if (ACTIVE_COPY) {
            bool const PAGE_ALREADY_MAPPED = mm::virt::user_page_mapped_now(task.pagemap, CUR);
            if (!PAGE_ALREADY_MAPPED && !mm::virt::ensure_user_page_mapped(&task, CUR)) {
                return false;
            }
            auto const* src = reinterpret_cast<const uint8_t*>(CUR);
            ker::util::copy_fast(out + copied, src, CHUNK);
            copied += CHUNK;
            continue;
        }
        if (!mm::virt::ensure_user_page_mapped(&task, CUR)) {
            return false;
        }

        uint64_t const PHYS = mm::virt::translate(task.pagemap, CUR);
        if (PHYS == mm::virt::PADDR_INVALID) {
            return false;
        }
        auto const* src = reinterpret_cast<const uint8_t*>(mm::addr::get_virt_pointer(PHYS));
        ker::util::copy_fast(out + copied, src, CHUNK);
        copied += CHUNK;
    }
    return true;
}

auto copy_to_task(sched::task::Task& task, uint64_t user_addr, const void* src, size_t size) -> bool {
    return copy_to_task_common(task, user_addr, src, size, true);
}

auto copy_to_task_mapped(sched::task::Task& task, uint64_t user_addr, const void* src, size_t size) -> bool {
    return copy_to_task_common(task, user_addr, src, size, false);
}

auto copy_cstring_from_task(sched::task::Task& task, uint64_t user_addr, char* dst, size_t dst_size) -> bool {
    if (task.pagemap == nullptr || dst == nullptr || dst_size == 0 || user_addr == 0) {
        return false;
    }

    size_t written = 0;
    bool const ACTIVE_COPY = mm::virt::active_pagemap_is(task.pagemap);
    while (written + 1 < dst_size) {
        uint64_t cur = 0;
        if (__builtin_add_overflow(user_addr, static_cast<uint64_t>(written), &cur)) {
            return false;
        }
        size_t const CHUNK = copy_chunk((dst_size - 1) - written, cur);
        if (!range_valid(cur, CHUNK) || CHUNK == 0) {
            return false;
        }

        const uint8_t* src = nullptr;
        if (ACTIVE_COPY) {
            bool const PAGE_ALREADY_MAPPED = mm::virt::user_page_mapped_now(task.pagemap, cur);
            if (!PAGE_ALREADY_MAPPED && !mm::virt::ensure_user_page_mapped(&task, cur)) {
                return false;
            }
            src = reinterpret_cast<const uint8_t*>(cur);
        } else {
            if (!mm::virt::ensure_user_page_mapped(&task, cur)) {
                return false;
            }

            uint64_t const PHYS = mm::virt::translate(task.pagemap, cur);
            if (PHYS == mm::virt::PADDR_INVALID) {
                return false;
            }
            src = reinterpret_cast<const uint8_t*>(mm::addr::get_virt_pointer(PHYS));
        }

        auto const* const NUL = static_cast<const uint8_t*>(std::memchr(src, '\0', CHUNK));
        size_t const COPY_LEN = NUL != nullptr ? static_cast<size_t>(NUL - src) : CHUNK;
        ker::util::copy_fast(dst + written, src, COPY_LEN);
        written += COPY_LEN;
        if (NUL != nullptr) {
            dst[written] = '\0';
            return true;
        }
    }
    dst[written] = '\0';
    return true;
}

}  // namespace ker::mod::sys::usercopy
