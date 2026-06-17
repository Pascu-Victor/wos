#include "sys_power.hpp"

#include <abi/callnums/power.h>

#include <cerrno>
#include <cstdint>
#include <platform/power/power.hpp>

namespace ker::syscall::power {

auto sys_power(uint64_t op, uint64_t a2) -> uint64_t {
    switch (static_cast<ker::abi::power::ops>(op)) {
        case ker::abi::power::ops::REBOOT:
            return static_cast<uint64_t>(ker::mod::power::begin_reboot_command(a2));
        case ker::abi::power::ops::GET_STATE:
            return static_cast<uint64_t>(ker::mod::power::shutdown_phase());
        case ker::abi::power::ops::PREPARE:
            return static_cast<uint64_t>(ker::mod::power::prepare_shutdown());
        default:
            return static_cast<uint64_t>(-EINVAL);
    }
}

}  // namespace ker::syscall::power
