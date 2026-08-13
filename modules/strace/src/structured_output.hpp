#pragma once

#include <time.h>

#include <cstdint>
#include <optional>
#include <string_view>

#include "common.hpp"
#include "output.hpp"

namespace wos::strace {

void emit_structured_syscall(TraceOutput& output, uint64_t pid, uint64_t tid, const PendingSyscall& pending, std::optional<int64_t> result,
                             const timespec& observed_at, std::string_view display, bool paired, bool deferred);
void emit_structured_signal(TraceOutput& output, uint64_t pid, uint64_t tid, uint32_t signal, const timespec& observed_at);
void emit_structured_fork(TraceOutput& output, uint64_t pid, uint64_t tid, uint64_t child_pid, const PendingSyscall& pending,
                          const timespec& observed_at);
void emit_structured_exec(TraceOutput& output, uint64_t pid, uint64_t tid, const PendingSyscall& pending, std::string_view phase,
                          std::optional<int64_t> result, const timespec& observed_at);
void emit_structured_termination(TraceOutput& output, uint64_t pid, uint64_t tid, int wait_status, const timespec& observed_at);

[[nodiscard]] auto is_exec_syscall(const PendingSyscall& pending) -> bool;

}  // namespace wos::strace
