#pragma once
#include <object_pages.hpp>
#include_next <platform/mm/cache_policy.hpp>

namespace ker::mod::mm::cache {
inline auto qualification() -> PatQualification {
    return {.expected_mask = 1,
            .reported_mask = object_fixture::pat_complete ? 1U : 0U,
            .mismatch_mask = object_fixture::pat_wb && !object_fixture::pat_other_bad ? 0U : 1U,
            .configured = true,
            .complete = object_fixture::pat_complete,
            .wb_encoding = object_fixture::pat_wb,
            .uc_encoding = !object_fixture::pat_other_bad,
            .wc_encoding = !object_fixture::pat_other_bad};
}
}  // namespace ker::mod::mm::cache
