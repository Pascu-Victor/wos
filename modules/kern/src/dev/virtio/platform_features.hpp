#pragma once

#include <cstdint>
#include <optional>

namespace ker::dev::virtio {
// Bits in upper feature word (device_feature_select / driver_feature_select 1).
constexpr uint32_t VIRTIO_F_VERSION_1 = 1U << 0;
constexpr uint32_t VIRTIO_F_ACCESS_PLATFORM = 1U << 1;

[[nodiscard]] constexpr auto platform_features(uint32_t offered, bool translated) -> std::optional<uint32_t> {
    if ((offered & VIRTIO_F_VERSION_1) == 0 || (translated && (offered & VIRTIO_F_ACCESS_PLATFORM) == 0)) return std::nullopt;
    return offered & (VIRTIO_F_VERSION_1 | VIRTIO_F_ACCESS_PLATFORM);
}
}  // namespace ker::dev::virtio
