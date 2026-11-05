#pragma once
#include <platform/sched/shutdown_ownership.hpp>
namespace ker::mod::sched::task {
struct Task {
    ker::mod::sched::ShutdownOwnership kernel_shutdown_ownership;
};
}  // namespace ker::mod::sched::task
