#include <gtest/gtest.h>

#include <cstdint>
#include <render_core.hpp>
#include <render_protocol.hpp>
#include <span>
#include <vector>

namespace {

auto parse(std::vector<char*>& argv, tracebench::Options& options) -> tracebench::ParseStatus {
    return tracebench::parse_options(static_cast<int>(argv.size()), argv.data(), tracebench::Backend::Ipc, options);
}

}  // namespace

TEST(RenderbenchOptions, DefaultFinalImageBudgetSkipsDuckBenchmarkFrame) {
    std::vector<char*> argv{
        const_cast<char*>("renderbench"), const_cast<char*>("--scene"),  const_cast<char*>("Duck.glb"), const_cast<char*>("--width"),
        const_cast<char*>("4000"),        const_cast<char*>("--height"), const_cast<char*>("4000"),
    };
    tracebench::Options options;
    ASSERT_EQ(parse(argv, options), tracebench::ParseStatus::Ok);

    tracebench::FilmView film{.width = options.width, .height = options.height, .rgb = std::span<float>()};
    uint64_t pixels = 0;
    EXPECT_TRUE(tracebench::final_image_pixel_count(film, pixels));
    EXPECT_EQ(pixels, 16'000'000u);
    EXPECT_TRUE(options.write_final_image);
    EXPECT_EQ(options.final_image_max_pixels, 8ULL * 1024ULL * 1024ULL);
    EXPECT_FALSE(tracebench::should_write_final_image(options, film));
}

TEST(RenderbenchOptions, FinalImageCanBeForcedOrDisabledExplicitly) {
    std::vector<char*> forced_argv{
        const_cast<char*>("renderbench"), const_cast<char*>("--width"), const_cast<char*>("4000"),
        const_cast<char*>("--height"),    const_cast<char*>("4000"),    const_cast<char*>("--final-image-max-pixels"),
        const_cast<char*>("0"),
    };
    tracebench::Options options;
    ASSERT_EQ(parse(forced_argv, options), tracebench::ParseStatus::Ok);
    tracebench::FilmView large_film{.width = options.width, .height = options.height, .rgb = std::span<float>()};
    EXPECT_TRUE(tracebench::should_write_final_image(options, large_film));

    std::vector<char*> disabled_argv{
        const_cast<char*>("renderbench"), const_cast<char*>("--width"), const_cast<char*>("64"),
        const_cast<char*>("--height"),    const_cast<char*>("64"),      const_cast<char*>("--disable-final-image"),
    };
    ASSERT_EQ(parse(disabled_argv, options), tracebench::ParseStatus::Ok);
    tracebench::FilmView small_film{.width = options.width, .height = options.height, .rgb = std::span<float>()};
    EXPECT_FALSE(tracebench::should_write_final_image(options, small_film));
}

TEST(RenderbenchOptions, RejectsNegativeFinalImageBudget) {
    std::vector<char*> argv{
        const_cast<char*>("renderbench"),
        const_cast<char*>("--final-image-max-pixels"),
        const_cast<char*>("-1"),
    };
    tracebench::Options options;
    EXPECT_EQ(parse(argv, options), tracebench::ParseStatus::Error);
}

TEST(RenderbenchOptions, TracksExplicitTileSize) {
    std::vector<char*> default_argv{
        const_cast<char*>("renderbench"),
    };
    tracebench::Options options;
    ASSERT_EQ(parse(default_argv, options), tracebench::ParseStatus::Ok);
    EXPECT_EQ(options.tile_size, 32);
    EXPECT_FALSE(options.tile_size_explicit);

    std::vector<char*> explicit_argv{
        const_cast<char*>("renderbench"),
        const_cast<char*>("--tile-size"),
        const_cast<char*>("24"),
    };
    ASSERT_EQ(parse(explicit_argv, options), tracebench::ParseStatus::Ok);
    EXPECT_EQ(options.tile_size, 24);
    EXPECT_TRUE(options.tile_size_explicit);
}

TEST(RenderbenchOptions, WorkerOutputQueueIsOptIn) {
    std::vector<char*> default_argv{
        const_cast<char*>("renderbench"),
    };
    tracebench::Options options;
    ASSERT_EQ(parse(default_argv, options), tracebench::ParseStatus::Ok);
    EXPECT_TRUE(options.disable_worker_output_queue);
    EXPECT_TRUE(options.disable_single_thread_worker_queue);

    std::vector<char*> enabled_argv{
        const_cast<char*>("renderbench"),
        const_cast<char*>("--enable-worker-output-queue"),
        const_cast<char*>("--enable-single-thread-worker-queue"),
    };
    ASSERT_EQ(parse(enabled_argv, options), tracebench::ParseStatus::Ok);
    EXPECT_FALSE(options.disable_worker_output_queue);
    EXPECT_FALSE(options.disable_single_thread_worker_queue);
}

TEST(RenderbenchOptions, LivePreviewUsesShortPreviewCadence) {
    std::vector<char*> default_argv{
        const_cast<char*>("renderbench"),
    };
    tracebench::Options options;
    ASSERT_EQ(parse(default_argv, options), tracebench::ParseStatus::Ok);
    EXPECT_FALSE(options.live_preview);
    EXPECT_DOUBLE_EQ(options.preview_update_interval_seconds, tracebench::DEFAULT_PREVIEW_UPDATE_INTERVAL_SECONDS);

    std::vector<char*> live_argv{
        const_cast<char*>("renderbench"),
        const_cast<char*>("--live"),
    };
    ASSERT_EQ(parse(live_argv, options), tracebench::ParseStatus::Ok);
    EXPECT_TRUE(options.live_preview);
    EXPECT_DOUBLE_EQ(options.preview_update_interval_seconds, tracebench::LIVE_PREVIEW_UPDATE_INTERVAL_SECONDS);
}

TEST(RenderbenchFilmView, StorageCompletenessRequiresRgbTriplesForEveryPixel) {
    auto storage = tracebench::make_film_storage(3, 2);
    tracebench::FilmView complete{
        .width = 3,
        .height = 2,
        .rgb = std::span<float>(storage.data(), storage.size()),
    };
    EXPECT_TRUE(tracebench::film_storage_is_complete(complete));

    tracebench::FilmView truncated{
        .width = 3,
        .height = 2,
        .rgb = std::span<float>(storage.data(), storage.size() - 1U),
    };
    EXPECT_FALSE(tracebench::film_storage_is_complete(truncated));
}

TEST(RenderbenchFilmView, StorageCompletenessRejectsInvalidDimensions) {
    std::vector<float> storage(3);
    tracebench::FilmView zero_width{
        .width = 0,
        .height = 1,
        .rgb = std::span<float>(storage.data(), storage.size()),
    };
    EXPECT_FALSE(tracebench::film_storage_is_complete(zero_width));

    tracebench::FilmView negative_height{
        .width = 1,
        .height = -1,
        .rgb = std::span<float>(storage.data(), storage.size()),
    };
    EXPECT_FALSE(tracebench::film_storage_is_complete(negative_height));
}

TEST(RenderbenchProtocol, AcceptsOnlyFirstTileFromAssignedWorker) {
    std::vector<unsigned char> tile_seen(4, 0);
    std::vector<int> tile_owner{0, 1, -1, 1};

    EXPECT_EQ(tracebench::decide_worker_tile(1, 1, tile_seen, tile_owner), tracebench::WorkerTileDecision::Accepted);
    EXPECT_EQ(tile_seen[1], 1);

    EXPECT_EQ(tracebench::decide_worker_tile(1, 1, tile_seen, tile_owner), tracebench::WorkerTileDecision::Duplicate);
    EXPECT_EQ(tile_seen[1], 1);
}

TEST(RenderbenchProtocol, RejectsForeignAndOutOfRangeTilesWithoutMarkingSeen) {
    std::vector<unsigned char> tile_seen(4, 0);
    std::vector<int> tile_owner{0, 1, -1, 1};

    EXPECT_EQ(tracebench::decide_worker_tile(3, 0, tile_seen, tile_owner), tracebench::WorkerTileDecision::Foreign);
    EXPECT_EQ(tile_seen[3], 0);

    EXPECT_EQ(tracebench::decide_worker_tile(9, 1, tile_seen, tile_owner), tracebench::WorkerTileDecision::OutOfRange);
    EXPECT_EQ(tile_seen, std::vector<unsigned char>({0, 0, 0, 0}));
}
