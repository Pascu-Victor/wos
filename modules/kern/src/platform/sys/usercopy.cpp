#include "usercopy.hpp"

#include <cstdint>
#include <cstring>

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

[[nodiscard]] auto copy_to_task_common(sched::task::Task& task, uint64_t user_addr, const void* src, size_t size, bool fault_in)
    -> CopyResult {
    if (size == 0) {
        return {};
    }
    if (src == nullptr || !range_valid(user_addr, size)) {
        return {.bytes_copied = 0, .fault = true};
    }

    auto const* in = static_cast<const uint8_t*>(src);
    size_t copied = 0;
    while (copied < size) {
        uint64_t const CUR = user_addr + copied;
        size_t const CHUNK = copy_chunk(size - copied, CUR);
        StableUserPage pin{};
        if (!pin_task_user_page(task, CUR, true, fault_in, pin)) {
            return {.bytes_copied = copied, .fault = true};
        }
        auto* dst = static_cast<uint8_t*>(pin.kernel_address());
        ker::util::copy_fast(dst, in + copied, CHUNK);
        if (!pin.commit_write()) {
            return {.bytes_copied = copied, .fault = true};
        }
        copied += CHUNK;
    }
    return {.bytes_copied = copied, .fault = false};
}

}  // namespace

StableUserPage::~StableUserPage() { reset(); }

auto StableUserPage::valid() const -> bool { return m_task != nullptr && m_pin.hhdm_page != nullptr; }

auto StableUserPage::kernel_address() const -> void* {
    if (!valid()) {
        return nullptr;
    }
    return static_cast<uint8_t*>(m_pin.hhdm_page) + (m_user_address - m_pin.user_page);
}

auto StableUserPage::physical_address() const -> uint64_t {
    if (!valid()) {
        return mm::virt::PADDR_INVALID;
    }
    return m_pin.physical_page + (m_user_address - m_pin.user_page);
}

auto StableUserPage::was_dirty() const -> bool { return valid() && m_pin.dirty; }

auto StableUserPage::still_mapped() const -> bool { return valid() && mm::virt::user_page_pin_still_mapped(m_pin); }

auto StableUserPage::commit_write() const -> bool { return valid() && mm::virt::user_page_pin_commit_write(m_pin); }

void StableUserPage::reset() {
    auto* task = m_task;
    m_task = nullptr;
    m_user_address = 0;
    mm::virt::unpin_user_page(m_pin);
    if (task != nullptr) {
        task->release_usercopy_pagemap();
    }
}

auto pin_task_user_page(sched::task::Task& task, uint64_t user_addr, bool require_writable, bool fault_in, StableUserPage& out) -> bool {
    out.reset();
    if (!range_valid(user_addr, 1)) {
        return false;
    }

    mm::paging::PageTable* pagemap = nullptr;
    if (!task.try_acquire_usercopy_pagemap(pagemap)) {
        return false;
    }
    out.m_task = &task;

    if (!mm::virt::pin_user_page_for_task(&task, pagemap, user_addr, require_writable, fault_in, out.m_pin)) {
        out.reset();
        return false;
    }

    out.m_user_address = user_addr;
    return true;
}

auto mapped_physical_address(sched::task::Task& task, uint64_t user_addr, bool require_writable) -> uint64_t {
    StableUserPage pin;
    if (!pin_task_user_page(task, user_addr, require_writable, false, pin) || !pin.still_mapped()) {
        return mm::virt::PADDR_INVALID;
    }
    return pin.physical_address();
}

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
    if (!range_valid(user_addr, size)) {
        return false;
    }

    uint64_t const END = user_addr + static_cast<uint64_t>(size);
    for (uint64_t page = user_addr; page < END;) {
        StableUserPage pin{};
        if (!pin_task_user_page(task, page, true, true, pin) || !pin.still_mapped()) {
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

[[nodiscard]] auto copy_from_task_common(sched::task::Task& task, uint64_t user_addr, void* dst, size_t size, bool fault_in) -> CopyResult {
    if (size == 0) {
        return {};
    }
    if (dst == nullptr || !range_valid(user_addr, size)) {
        return {.bytes_copied = 0, .fault = true};
    }

    auto* out = static_cast<uint8_t*>(dst);
    size_t copied = 0;
    while (copied < size) {
        uint64_t const CUR = user_addr + copied;
        size_t const CHUNK = copy_chunk(size - copied, CUR);
        StableUserPage pin{};
        if (!pin_task_user_page(task, CUR, false, fault_in, pin)) {
            return {.bytes_copied = copied, .fault = true};
        }
        auto const* src = static_cast<const uint8_t*>(pin.kernel_address());
        ker::util::copy_fast(out + copied, src, CHUNK);
        if (!pin.still_mapped()) {
            return {.bytes_copied = copied, .fault = true};
        }
        copied += CHUNK;
    }
    return {.bytes_copied = copied, .fault = false};
}

auto copy_from_task_partial(sched::task::Task& task, uint64_t user_addr, void* dst, size_t size) -> CopyResult {
    return copy_from_task_common(task, user_addr, dst, size, true);
}

auto copy_from_task(sched::task::Task& task, uint64_t user_addr, void* dst, size_t size) -> bool {
    return copy_from_task_partial(task, user_addr, dst, size).complete(size);
}

auto copy_from_task_mapped(sched::task::Task& task, uint64_t user_addr, void* dst, size_t size) -> bool {
    return copy_from_task_common(task, user_addr, dst, size, false).complete(size);
}

auto copy_to_task_partial(sched::task::Task& task, uint64_t user_addr, const void* src, size_t size) -> CopyResult {
    return copy_to_task_common(task, user_addr, src, size, true);
}

auto copy_to_task_mapped_partial(sched::task::Task& task, uint64_t user_addr, const void* src, size_t size) -> CopyResult {
    return copy_to_task_common(task, user_addr, src, size, false);
}

auto copy_to_task(sched::task::Task& task, uint64_t user_addr, const void* src, size_t size) -> bool {
    return copy_to_task_partial(task, user_addr, src, size).complete(size);
}

auto copy_to_task_mapped(sched::task::Task& task, uint64_t user_addr, const void* src, size_t size) -> bool {
    return copy_to_task_mapped_partial(task, user_addr, src, size).complete(size);
}

auto copy_cstring_from_task_status(sched::task::Task& task, uint64_t user_addr, char* dst, size_t dst_size) -> CStringCopyStatus {
    if (dst == nullptr || dst_size == 0 || user_addr == 0) {
        return CStringCopyStatus::FAULT;
    }

    size_t written = 0;
    while (written < dst_size) {
        uint64_t cur = 0;
        if (__builtin_add_overflow(user_addr, static_cast<uint64_t>(written), &cur)) {
            return CStringCopyStatus::FAULT;
        }
        size_t const CHUNK = copy_chunk(dst_size - written, cur);
        if (!range_valid(cur, CHUNK) || CHUNK == 0) {
            return CStringCopyStatus::FAULT;
        }

        CopyResult const COPY = copy_from_task_partial(task, cur, dst + written, CHUNK);
        if (COPY.bytes_copied == 0 && COPY.fault) {
            return CStringCopyStatus::FAULT;
        }

        auto const* const CHUNK_START = reinterpret_cast<const uint8_t*>(dst + written);
        auto const* const NUL = static_cast<const uint8_t*>(std::memchr(CHUNK_START, '\0', COPY.bytes_copied));
        size_t const COPY_LEN = NUL != nullptr ? static_cast<size_t>(NUL - CHUNK_START) : COPY.bytes_copied;
        written += COPY_LEN;
        if (NUL != nullptr) {
            dst[written] = '\0';
            return CStringCopyStatus::COMPLETE;
        }
        if (COPY.fault) {
            return CStringCopyStatus::FAULT;
        }
    }
    dst[dst_size - 1] = '\0';
    return CStringCopyStatus::TOO_LONG;
}

auto copy_cstring_from_task(sched::task::Task& task, uint64_t user_addr, char* dst, size_t dst_size) -> bool {
    return copy_cstring_from_task_status(task, user_addr, dst, dst_size) != CStringCopyStatus::FAULT;
}

auto copy_cstring_from_task_strict(sched::task::Task& task, uint64_t user_addr, char* dst, size_t dst_size) -> bool {
    return copy_cstring_from_task_status(task, user_addr, dst, dst_size) == CStringCopyStatus::COMPLETE;
}

}  // namespace ker::mod::sys::usercopy
