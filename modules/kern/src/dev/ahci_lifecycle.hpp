#pragma once

#include <atomic>
#include <cstdint>

namespace ker::dev::ahci::lifecycle {
// Check under the port mutex. Closing does not cancel commands that already
// passed this check: teardown must acquire every port mutex before retirement.
class Admission {
    std::atomic<bool> open_{false};

   public:
    void open_for_probe() { open_.store(true, std::memory_order_release); }
    void close() { open_.store(false, std::memory_order_release); }
    [[nodiscard]] auto allows_command() const -> bool { return open_.load(std::memory_order_acquire); }
};

enum class Result : uint8_t { released, drain_failed, quiesce_failed, release_failed, flush_failed };

// Production and host tests share this ordering. Backend retains acquired
// reader locks until its destruction. Disable BME even when drain/stop fails,
// but never treat BME disable alone as proof of DMA retirement.
template <typename Backend>
[[nodiscard]] auto retire(Backend& backend) -> Result {
    backend.close_admission();
    if (!backend.drain_readers()) {
        (void)backend.disable_bus_master();
        return Result::drain_failed;
    }
    bool const DURABLE = backend.flush();
    bool const STOPPED = backend.stop_engines();
    bool const DISABLED = backend.disable_bus_master();
    if (!STOPPED || !DISABLED) {
        backend.quarantine();
        return Result::quiesce_failed;
    }
    if (!backend.release()) return Result::release_failed;
    return DURABLE ? Result::released : Result::flush_failed;
}
}  // namespace ker::dev::ahci::lifecycle
