#include <gtest/gtest.h>

#include <dev/virtio/platform_features.hpp>

using namespace ker::dev::virtio;

TEST(VirtioPlatform, TranslationRequiresBothModernAndPlatformFeatures) {
    for (uint32_t offered = 0; offered < 4; ++offered) {
        auto const RESULT = platform_features(offered, true);
        EXPECT_EQ(RESULT.has_value(), offered == 3);
        if (RESULT) EXPECT_EQ(*RESULT, 3U);
    }
}
TEST(VirtioPlatform, PassthroughStillRequiresModernAndAcceptsPlatformWhenOffered) {
    EXPECT_FALSE(platform_features(0, false));
    EXPECT_FALSE(platform_features(VIRTIO_F_ACCESS_PLATFORM, false));
    EXPECT_EQ(platform_features(VIRTIO_F_VERSION_1, false), 1U);
    EXPECT_EQ(platform_features(3, false), 3U);
}
TEST(VirtioPlatform, UnknownUpperFeaturesAreNeverAccepted) {
    for (unsigned bit = 2; bit < 32; ++bit) {
        EXPECT_EQ(platform_features((1U << bit) | 3, true), 3U);
        EXPECT_EQ(platform_features((1U << bit) | 1, false), 1U);
        EXPECT_FALSE(platform_features((1U << bit) | 1, true));
    }
}
