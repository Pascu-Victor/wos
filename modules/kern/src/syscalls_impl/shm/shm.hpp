#pragma once

#include <cstdint>

namespace ker::mod::sched::task {
struct Task;
}  // namespace ker::mod::sched::task

namespace ker::syscall::shm {

auto sys_shm(uint64_t op, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t a4) -> uint64_t;

auto shm_clone_for_fork(ker::mod::sched::task::Task* parent, ker::mod::sched::task::Task* child) -> bool;
void shm_cleanup_for_task(ker::mod::sched::task::Task* task);

}  // namespace ker::syscall::shm
