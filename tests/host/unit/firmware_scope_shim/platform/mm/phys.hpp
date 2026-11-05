#pragma once
#include <platform/sched/scheduler.hpp>
namespace ker::mod::mm::phys {
inline auto can_wait_for_reclaim() -> bool { return firmware_host::ordinary; }
}  // namespace ker::mod::mm::phys
