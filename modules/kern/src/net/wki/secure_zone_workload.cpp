#include "secure_zone_workload.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <net/wki/zone.hpp>
#include <platform/sched/scheduler.hpp>

namespace ker::net::wki {
namespace {

auto pattern_byte(uint64_t seed, uint32_t offset) -> uint8_t {
    uint64_t value = seed ^ (0x9E3779B97F4A7C15ULL * (static_cast<uint64_t>(offset) + 1));
    value ^= value >> 33;
    value *= 0xFF51AFD7ED558CCDULL;
    value ^= value >> 33;
    return static_cast<uint8_t>(value ^ (value >> 8) ^ (value >> 16));
}

auto wait_secure_ready(const WkiZoneRef& owned) -> int {
    constexpr uint64_t TIMEOUT_US = 50000;
    constexpr uint64_t SLEEP_US = 1000;
    uint64_t const START = wki_now_us();
    uint64_t const DEADLINE = START > UINT64_MAX - TIMEOUT_US ? UINT64_MAX : START + TIMEOUT_US;
    uint64_t previous = START;
    for (uint32_t sleeps = 0;; ++sleeps) {
        uint64_t const NOW = wki_now_us();
        if (NOW < previous || NOW >= DEADLINE) return -ETIMEDOUT;
        previous = NOW;
        if (owned.secure_ready()) {
            uint64_t const READY_AT = wki_now_us();
            return READY_AT < NOW || READY_AT >= DEADLINE ? -ETIMEDOUT : WKI_OK;
        }
        if (!owned.live()) return WKI_ERR_ZONE_INACTIVE;
        if (sleeps == TIMEOUT_US / SLEEP_US) return -ETIMEDOUT;
        mod::sched::kern_sleep_us(std::min(SLEEP_US, DEADLINE - NOW));
    }
}

}  // namespace

auto wki_secure_zone_workload_run(uint16_t peer, uint32_t zone_id, uint32_t bytes, uint64_t seed, WkiSecureZoneWorkloadResult& result,
                                  bool require_secure) -> int {
    result = {};
    result.requested_bytes = bytes;
    if (peer == WKI_NODE_INVALID || peer == WKI_NODE_BROADCAST || zone_id == 0 || bytes == 0 ||
        bytes > WKI_SECURE_ZONE_WORKLOAD_MAX_BYTES) {
        result.status = -EINVAL;
        return result.status;
    }

    std::array<uint8_t, WKI_SECURE_ZONE_WORKLOAD_MAX_BYTES> expected{};
    std::array<uint8_t, WKI_SECURE_ZONE_WORKLOAD_MAX_BYTES> observed{};
    for (uint32_t offset = 0; offset < bytes; ++offset) expected.at(offset) = pattern_byte(seed, offset);

    WkiZoneRef owned;
    constexpr uint8_t ACCESS = ZONE_ACCESS_LOCAL_READ | ZONE_ACCESS_LOCAL_WRITE | ZONE_ACCESS_REMOTE_READ | ZONE_ACCESS_REMOTE_WRITE;
    result.create_status =
        wki_zone_create(peer, zone_id, WKI_SECURE_ZONE_WORKLOAD_MAX_BYTES, ACCESS, ZoneNotifyMode::NONE, ZoneTypeHint::BUFFER, &owned);
    result.status = result.create_status;
    result.created = result.create_status == WKI_OK;
    if (result.created && !owned) result.status = WKI_ERR_ZONE_INACTIVE;

    if (result.status == WKI_OK && require_secure) {
        result.prepare_status = wait_secure_ready(owned);
        result.status = result.prepare_status;
    }

    if (result.status == WKI_OK) {
        result.write_status = owned.write(0, expected.data(), bytes);
        result.status = result.write_status;
        if (result.write_status == WKI_OK) result.written_bytes = bytes;
    }
    if (result.status == WKI_OK) {
        result.read_status = owned.read(0, observed.data(), bytes);
        result.status = result.read_status;
        if (result.read_status == WKI_OK) {
            result.read_bytes = bytes;
            result.verified = true;
            for (uint32_t offset = 0; offset < bytes; ++offset) {
                if (observed.at(offset) != expected.at(offset)) {
                    result.verified = false;
                    result.mismatch_offset = offset;
                    result.status = -EIO;
                    break;
                }
            }
        }
    }

    if (owned) {
        result.destroy_status = owned.destroy();
        result.destroyed = result.destroy_status == WKI_OK;
        if (result.status == WKI_OK) result.status = result.destroy_status;
        owned.reset();
    }
    return result.status;
}

}  // namespace ker::net::wki
