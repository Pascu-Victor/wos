#include "user_layout.hpp"

#include <cstddef>
#include <cstdint>

#include "platform/mm/paging.hpp"
#include "platform/mm/virt.hpp"
#include "platform/random/entropy.hpp"

namespace ker::mod::mm::user_layout {

namespace {

constexpr uint64_t MAX_RANDOMIZED_COLLISION_PROBES = 256;

[[nodiscard]] constexpr auto is_power_of_two(uint64_t value) -> bool { return value != 0 && (value & (value - 1)) == 0; }

[[nodiscard]] auto align_up_checked(uint64_t value, uint64_t alignment, uint64_t& output) -> bool {
    if (!is_power_of_two(alignment) || value > UINT64_MAX - (alignment - 1)) {
        return false;
    }
    output = (value + alignment - 1) & ~(alignment - 1);
    return true;
}

[[nodiscard]] constexpr auto align_down(uint64_t value, uint64_t alignment) -> uint64_t { return value & ~(alignment - 1); }

struct CandidateSet {
    uint64_t first{};
    uint64_t count{};
    uint64_t alignment{};
};

[[nodiscard]] auto candidate_set(const AddressWindow& window, uint64_t span, CandidateSet& result) -> bool {
    if (!is_power_of_two(window.alignment) || window.begin >= window.end || span == 0 || span > window.end - window.begin) {
        return false;
    }
    uint64_t first = 0;
    if (!align_up_checked(window.begin, window.alignment, first)) {
        return false;
    }
    uint64_t const LAST = align_down(window.end - span, window.alignment);
    if (first > LAST) {
        return false;
    }
    uint64_t const DISTANCE = LAST - first;
    result = {
        .first = first,
        .count = (DISTANCE / window.alignment) + 1,
        .alignment = window.alignment,
    };
    return result.count != 0;
}

[[nodiscard]] auto candidate_at(const CandidateSet& candidates, uint64_t index, uint64_t& address) -> bool {
    if (index >= candidates.count || index > (UINT64_MAX - candidates.first) / candidates.alignment) {
        return false;
    }
    address = candidates.first + (index * candidates.alignment);
    return true;
}

[[nodiscard]] auto choose_free_candidate(paging::PageTable* page_table, const CandidateSet& candidates, uint64_t span, uint64_t& start)
    -> bool {
    if (page_table == nullptr || candidates.count == 0) {
        return false;
    }
    uint64_t initial_index = 0;
    if (!random::entropy::uniform_u64(candidates.count, initial_index)) {
        return false;
    }
    uint64_t const ATTEMPTS = candidates.count < MAX_RANDOMIZED_COLLISION_PROBES ? candidates.count : MAX_RANDOMIZED_COLLISION_PROBES;
    uint64_t index = initial_index;
    for (uint64_t attempt = 0; attempt < ATTEMPTS; ++attempt) {
        uint64_t candidate = 0;
        if (!candidate_at(candidates, index, candidate)) {
            return false;
        }
        if (range_is_free(page_table, candidate, span)) {
            start = candidate;
            return true;
        }
        index = index + 1 == candidates.count ? 0 : index + 1;
    }
    return false;
}

}  // namespace

auto range_is_free(paging::PageTable* page_table, uint64_t start, uint64_t size) -> bool {
    if (page_table == nullptr || size == 0 || (start & (paging::PAGE_SIZE - 1)) != 0 || (size & (paging::PAGE_SIZE - 1)) != 0 ||
        start > UINT64_MAX - size) {
        return false;
    }
    uint64_t const END = start + size;
    for (uint64_t address = start; address < END; address += paging::PAGE_SIZE) {
        if (virt::is_page_mapped_or_reserved(page_table, address)) {
            return false;
        }
    }
    return true;
}

auto choose_image_base(paging::PageTable* page_table, ImageRole role, uint64_t image_min_vaddr, uint64_t image_max_vaddr, uint64_t& base)
    -> bool {
    AddressWindow window{};
    switch (role) {
        case ImageRole::MAIN:
            window = MAIN_IMAGE_WINDOW;
            break;
        case ImageRole::INTERPRETER:
            window = INTERPRETER_WINDOW;
            break;
        default:
            return false;
    }
    if (page_table == nullptr || image_min_vaddr >= image_max_vaddr || (image_min_vaddr & (paging::PAGE_SIZE - 1)) != 0 ||
        (image_max_vaddr & (paging::PAGE_SIZE - 1)) != 0 || image_max_vaddr > window.end) {
        return false;
    }

    uint64_t const LOWER_UNALIGNED = window.begin > image_min_vaddr ? window.begin - image_min_vaddr : 0;
    uint64_t first_bias = 0;
    if (!align_up_checked(LOWER_UNALIGNED, window.alignment, first_bias)) {
        return false;
    }
    uint64_t const LAST_BIAS = align_down(window.end - image_max_vaddr, window.alignment);
    if (first_bias > LAST_BIAS) {
        return false;
    }
    CandidateSet const CANDIDATES{
        .first = first_bias,
        .count = ((LAST_BIAS - first_bias) / window.alignment) + 1,
        .alignment = window.alignment,
    };
    uint64_t const IMAGE_SPAN = image_max_vaddr - image_min_vaddr;

    uint64_t initial_index = 0;
    if (!random::entropy::uniform_u64(CANDIDATES.count, initial_index)) {
        return false;
    }
    uint64_t const ATTEMPTS = CANDIDATES.count < MAX_RANDOMIZED_COLLISION_PROBES ? CANDIDATES.count : MAX_RANDOMIZED_COLLISION_PROBES;
    uint64_t index = initial_index;
    for (uint64_t attempt = 0; attempt < ATTEMPTS; ++attempt) {
        uint64_t candidate_bias = 0;
        if (!candidate_at(CANDIDATES, index, candidate_bias) || candidate_bias > UINT64_MAX - image_min_vaddr) {
            return false;
        }
        uint64_t const RUNTIME_START = candidate_bias + image_min_vaddr;
        if (range_is_free(page_table, RUNTIME_START, IMAGE_SPAN)) {
            base = candidate_bias;
            return true;
        }
        index = index + 1 == CANDIDATES.count ? 0 : index + 1;
    }
    return false;
}

auto choose_thread_region(paging::PageTable* page_table, uint64_t total_span, uint64_t& start) -> bool {
    uint64_t aligned_span = 0;
    if (total_span == 0 || !align_up_checked(total_span, paging::PAGE_SIZE, aligned_span)) {
        return false;
    }
    CandidateSet candidates{};
    if (!candidate_set(THREAD_WINDOW, aligned_span, candidates)) {
        return false;
    }
    return choose_free_candidate(page_table, candidates, aligned_span, start);
}

auto choose_mmap_cursor(uint64_t& cursor) -> bool {
    CandidateSet candidates{};
    if (!candidate_set(MMAP_WINDOW, MMAP_ALIGNMENT, candidates)) {
        return false;
    }
    uint64_t index = 0;
    if (!random::entropy::uniform_u64(candidates.count, index)) {
        return false;
    }
    return candidate_at(candidates, index, cursor);
}

#ifdef WOS_SELFTEST
auto selftest_choose_aligned(const AddressWindow& window, uint64_t span, uint64_t sample, uint64_t& start) -> bool {
    CandidateSet candidates{};
    if (!candidate_set(window, span, candidates)) {
        return false;
    }
    return candidate_at(candidates, sample % candidates.count, start);
}
#endif

}  // namespace ker::mod::mm::user_layout
