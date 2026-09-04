#include <gtest/gtest.h>

#include <cerrno>
#include <cstdint>
#include <platform/mm/anonymous_swap_state.hpp>

namespace swap = ker::mod::mm::swap;

namespace {

constexpr uint64_t FRAME_A = 0x1000;
constexpr uint64_t FRAME_B = 0x2000;
constexpr swap::SwapSlot SLOT_A{.area = 1, .index = 7, .generation = 1};

auto resident_page() -> swap::AnonymousPageRecord {
    swap::AnonymousPageRecord page{};
    EXPECT_EQ(swap::anonymous_initialize_resident(page, FRAME_A), swap::AnonymousTransitionStatus::APPLIED);
    EXPECT_TRUE(swap::anonymous_page_state_valid(page));
    return page;
}

auto swapped_page(uint64_t* generation_out = nullptr) -> swap::AnonymousPageRecord {
    auto page = resident_page();
    uint64_t generation = 0;
    EXPECT_EQ(swap::anonymous_begin_pageout(page, SLOT_A, &generation), swap::AnonymousTransitionStatus::APPLIED);
    EXPECT_EQ(swap::anonymous_complete_pageout(page, generation, 0), swap::AnonymousTransitionStatus::APPLIED);
    EXPECT_TRUE(swap::anonymous_page_state_valid(page));
    if (generation_out != nullptr) {
        *generation_out = generation;
    }
    return page;
}

TEST(AnonymousSwapStateTest, HappyPathNamesEveryOwnershipState) {
    auto page = resident_page();
    uint64_t pageout_generation = 0;
    EXPECT_EQ(swap::anonymous_begin_pageout(page, SLOT_A, &pageout_generation), swap::AnonymousTransitionStatus::APPLIED);
    EXPECT_EQ(page.state, swap::AnonymousPageState::PAGEOUT_IN_PROGRESS);
    EXPECT_EQ(page.frame, FRAME_A);
    EXPECT_EQ(page.slot.generation, SLOT_A.generation);
    EXPECT_TRUE(swap::anonymous_page_state_valid(page));

    EXPECT_EQ(swap::anonymous_complete_pageout(page, pageout_generation, 0), swap::AnonymousTransitionStatus::APPLIED);
    EXPECT_EQ(page.state, swap::AnonymousPageState::SWAPPED);
    EXPECT_EQ(page.frame, swap::INVALID_FRAME);
    EXPECT_TRUE(swap::anonymous_page_state_valid(page));

    uint64_t pagein_generation = 0;
    EXPECT_EQ(swap::anonymous_begin_pagein(page, FRAME_B, &pagein_generation), swap::AnonymousTransitionStatus::APPLIED);
    EXPECT_EQ(page.state, swap::AnonymousPageState::PAGEIN_IN_PROGRESS);
    EXPECT_TRUE(swap::anonymous_page_state_valid(page));

    EXPECT_EQ(swap::anonymous_complete_pagein(page, pagein_generation, 0), swap::AnonymousTransitionStatus::APPLIED);
    EXPECT_EQ(page.state, swap::AnonymousPageState::RESIDENT);
    EXPECT_EQ(page.frame, FRAME_B);
    EXPECT_FALSE(swap::slot_valid(page.slot));
    EXPECT_TRUE(swap::anonymous_page_state_valid(page));
}

TEST(AnonymousSwapStateTest, PageoutFailuresRollBackWithoutLosingFrame) {
    for (int error : {-ENOMEM, -EIO, -ENODEV}) {
        auto page = resident_page();
        uint64_t generation = 0;
        ASSERT_EQ(swap::anonymous_begin_pageout(page, SLOT_A, &generation), swap::AnonymousTransitionStatus::APPLIED);
        EXPECT_EQ(swap::anonymous_complete_pageout(page, generation, error), swap::AnonymousTransitionStatus::APPLIED);
        EXPECT_EQ(page.state, swap::AnonymousPageState::RESIDENT);
        EXPECT_EQ(page.frame, FRAME_A);
        EXPECT_FALSE(swap::slot_valid(page.slot));
        EXPECT_TRUE(swap::anonymous_page_state_valid(page));
    }
}

TEST(AnonymousSwapStateTest, PageinFailuresBecomeResourceFreeTaskLocalFailure) {
    for (int error : {-ENOMEM, -EIO, -EILSEQ, -ENODEV, EIO}) {
        auto page = swapped_page();
        uint64_t generation = 0;
        ASSERT_EQ(swap::anonymous_begin_pagein(page, FRAME_B, &generation), swap::AnonymousTransitionStatus::APPLIED);
        EXPECT_EQ(swap::anonymous_complete_pagein(page, generation, error), swap::AnonymousTransitionStatus::APPLIED);
        EXPECT_EQ(page.state, swap::AnonymousPageState::FAILED);
        EXPECT_EQ(page.frame, swap::INVALID_FRAME);
        EXPECT_FALSE(swap::slot_valid(page.slot));
        EXPECT_LT(page.error, 0);
        EXPECT_TRUE(swap::anonymous_page_state_valid(page));
    }
}

TEST(AnonymousSwapStateTest, DuplicateAndStaleCompletionsAreInert) {
    auto page = resident_page();
    uint64_t first_generation = 0;
    ASSERT_EQ(swap::anonymous_begin_pageout(page, SLOT_A, &first_generation), swap::AnonymousTransitionStatus::APPLIED);
    auto const in_flight = page;
    EXPECT_EQ(swap::anonymous_complete_pageout(page, first_generation - 1, 0), swap::AnonymousTransitionStatus::STALE);
    EXPECT_EQ(page.state, in_flight.state);
    EXPECT_EQ(page.frame, in_flight.frame);
    EXPECT_EQ(page.slot.generation, in_flight.slot.generation);

    ASSERT_EQ(swap::anonymous_complete_pageout(page, first_generation, 0), swap::AnonymousTransitionStatus::APPLIED);
    auto const completed = page;
    EXPECT_EQ(swap::anonymous_complete_pageout(page, first_generation, 0), swap::AnonymousTransitionStatus::STALE);
    EXPECT_EQ(page.state, completed.state);
    EXPECT_EQ(page.frame, completed.frame);

    uint64_t second_generation = 0;
    ASSERT_EQ(swap::anonymous_begin_pagein(page, FRAME_B, &second_generation), swap::AnonymousTransitionStatus::APPLIED);
    EXPECT_GT(second_generation, first_generation);
    EXPECT_EQ(swap::anonymous_complete_pagein(page, first_generation, 0), swap::AnonymousTransitionStatus::STALE);
    EXPECT_EQ(page.state, swap::AnonymousPageState::PAGEIN_IN_PROGRESS);
}

TEST(AnonymousSwapStateTest, UnmapJoinsEveryOwnedResourceShape) {
    {
        auto page = resident_page();
        EXPECT_EQ(swap::anonymous_request_unmap(page), swap::AnonymousTransitionStatus::APPLIED);
        EXPECT_EQ(page.state, swap::AnonymousPageState::UNMAPPED);
        EXPECT_TRUE(swap::anonymous_page_state_valid(page));
        EXPECT_EQ(swap::anonymous_request_unmap(page), swap::AnonymousTransitionStatus::STALE);
    }
    {
        auto page = swapped_page();
        EXPECT_EQ(swap::anonymous_request_unmap(page), swap::AnonymousTransitionStatus::APPLIED);
        EXPECT_EQ(page.state, swap::AnonymousPageState::UNMAPPED);
        EXPECT_TRUE(swap::anonymous_page_state_valid(page));
    }
    for (int completion : {0, -EIO}) {
        auto page = resident_page();
        uint64_t generation = 0;
        ASSERT_EQ(swap::anonymous_begin_pageout(page, SLOT_A, &generation), swap::AnonymousTransitionStatus::APPLIED);
        EXPECT_EQ(swap::anonymous_request_unmap(page), swap::AnonymousTransitionStatus::APPLIED);
        EXPECT_TRUE(page.cancel_requested);
        EXPECT_EQ(swap::anonymous_complete_pageout(page, generation, completion), swap::AnonymousTransitionStatus::APPLIED);
        EXPECT_EQ(page.state, swap::AnonymousPageState::UNMAPPED);
        EXPECT_TRUE(swap::anonymous_page_state_valid(page));
    }
    for (int completion : {0, -EIO}) {
        auto page = swapped_page();
        uint64_t generation = 0;
        ASSERT_EQ(swap::anonymous_begin_pagein(page, FRAME_B, &generation), swap::AnonymousTransitionStatus::APPLIED);
        EXPECT_EQ(swap::anonymous_request_unmap(page), swap::AnonymousTransitionStatus::APPLIED);
        EXPECT_EQ(swap::anonymous_complete_pagein(page, generation, completion), swap::AnonymousTransitionStatus::APPLIED);
        EXPECT_EQ(page.state, swap::AnonymousPageState::UNMAPPED);
        EXPECT_TRUE(swap::anonymous_page_state_valid(page));
    }
}

TEST(AnonymousSwapStateTest, InvalidResourceCombinationsAndGenerationExhaustionFailClosed) {
    swap::AnonymousPageRecord invalid_resident{.state = swap::AnonymousPageState::RESIDENT};
    EXPECT_FALSE(swap::anonymous_page_state_valid(invalid_resident));
    EXPECT_EQ(swap::anonymous_begin_pageout(invalid_resident, SLOT_A), swap::AnonymousTransitionStatus::INVALID);

    auto page = resident_page();
    page.operation_generation = UINT64_MAX;
    EXPECT_EQ(swap::anonymous_begin_pageout(page, SLOT_A), swap::AnonymousTransitionStatus::INVALID);
    EXPECT_EQ(page.state, swap::AnonymousPageState::RESIDENT);

    auto swapped = swapped_page();
    EXPECT_EQ(swap::anonymous_begin_pagein(swapped, swap::INVALID_FRAME), swap::AnonymousTransitionStatus::INVALID);
    EXPECT_EQ(swapped.state, swap::AnonymousPageState::SWAPPED);
}

}  // namespace
