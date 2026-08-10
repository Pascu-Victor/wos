#pragma once

#include <net/if.h>

#include <array>
#include <cstdint>

enum class NetworkProbeResult : uint8_t {
    PENDING,
    READY,
    ERROR,
};

struct NetworkProbe {
    std::array<char, IFNAMSIZ> interface_name{};
    int socket_fd = -1;
    int open_errno = 0;
    uint64_t attempts = 0;
    uint64_t addr_successes = 0;
    uint64_t addr_zero_results = 0;
    uint64_t addr_failures = 0;
    int first_errno = 0;
    int last_errno = 0;
    uint32_t last_ipv4 = 0;
};

auto network_probe_begin(NetworkProbe& probe, const char* interface_name) -> bool;

// ERROR describes one failed ioctl attempt. The runtime may continue polling
// until its readiness deadline, matching the previous netd readiness policy.
auto network_probe_poll(NetworkProbe& probe) -> NetworkProbeResult;

void network_probe_dump_diagnostics(const NetworkProbe& probe, const char* reason, uint64_t service_pid);
void network_probe_close(NetworkProbe& probe);
void network_probe_reset(NetworkProbe& probe);
