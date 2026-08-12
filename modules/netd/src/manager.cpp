#include <pthread.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>

#include "netd/config.hpp"
#include "netd/daemon.hpp"
#include "netd/log.hpp"
#include "netd/ra_worker.hpp"
#include "netd/time.hpp"

namespace netd {
namespace {

constexpr size_t MAX_RA_WORKERS = 8;
constexpr size_t RA_WORKER_STACK_SIZE = 128 * 1024;
constexpr uint32_t DHCP_RETRY_INITIAL_S = 5;
constexpr uint32_t DHCP_RETRY_MAX_S = 60;

struct RaWorkerSlot {
    std::array<char, INTERFACE_NAME_CAPACITY> ifname{};
    pthread_t thread{};
};

std::array<RaWorkerSlot, MAX_RA_WORKERS> worker_slots{};

auto ra_worker_entry(void* opaque) -> void* {
    auto const* slot = static_cast<const RaWorkerSlot*>(opaque);
    run_ra_worker(slot->ifname.data());
    return nullptr;
}

auto start_ra_worker(size_t index, const char* ifname) -> bool {
    auto& slot = worker_slots[index];
    size_t const LENGTH = std::min(std::strlen(ifname), slot.ifname.size() - 1);
    std::copy_n(ifname, LENGTH, slot.ifname.data());

    pthread_attr_t attributes{};
    if (pthread_attr_init(&attributes) != 0) {
        return false;
    }
    static_cast<void>(pthread_attr_setdetachstate(&attributes, PTHREAD_CREATE_DETACHED));
    static_cast<void>(pthread_attr_setstacksize(&attributes, RA_WORKER_STACK_SIZE));
    int const RESULT = pthread_create(&slot.thread, &attributes, ra_worker_entry, &slot);
    pthread_attr_destroy(&attributes);
    return RESULT == 0;
}

}  // namespace

auto run_network_daemon() -> int {
    std::array<InterfaceConfig, MAX_CONFIGURED_INTERFACES> interfaces{};
    size_t interface_count = load_interface_configs(interfaces);
    if (interface_count == 0) {
        interfaces[0].policy = InterfacePolicy::DHCP;
        std::copy_n("eth0", 5, interfaces[0].ifname.data());
        interface_count = 1;
        logger::warn("netd: /etc/netdevs unavailable or empty; using eth0 dhcp fallback");
    }

    const char* dhcp_ifname = nullptr;
    size_t worker_count = 0;
    for (size_t i = 0; i < interface_count; ++i) {
        auto const& interface = interfaces[i];
        logger::info("netd: policy %s=%s", interface.ifname.data(), interface_policy_name(interface.policy));
        if (interface.policy == InterfacePolicy::DHCP && dhcp_ifname == nullptr) {
            dhcp_ifname = interface.ifname.data();
        }
        // DHCP interfaces participate in IPv4 DHCP and IPv6 RS/RA. The
        // linklocal policy deliberately keeps only the kernel-seeded address;
        // WKI and unmanaged interfaces are never mutated by netd.
        if (interface.policy != InterfacePolicy::DHCP) {
            continue;
        }
        if (worker_count >= worker_slots.size()) {
            logger::warn("netd: IPv6 worker limit reached; leaving %s link-local only", interface.ifname.data());
            continue;
        }
        if (!start_ra_worker(worker_count, interface.ifname.data())) {
            logger::warn("netd: failed to create IPv6 worker for %s", interface.ifname.data());
            continue;
        }
        ++worker_count;
    }

    if (dhcp_ifname == nullptr) {
        logger::info("netd: no DHCP-managed interface; IPv6 workers remain active");
        for (;;) {
            sleep_for_seconds(86400);
        }
    }

    uint32_t retry_delay_s = DHCP_RETRY_INITIAL_S;
    for (;;) {
        int const RESULT = run_dhcp_client(dhcp_ifname);
        logger::warn("netd: DHCP client for %s returned %d; IPv6 workers remain active, retrying in %us", dhcp_ifname, RESULT,
                     retry_delay_s);
        sleep_for_seconds(retry_delay_s);
        retry_delay_s = std::min(DHCP_RETRY_MAX_S, retry_delay_s * 2U);
    }
}

}  // namespace netd
