#pragma once

#include <cstdint>
#include <span>
#include <string_view>

namespace ker::syscall::process {

uint64_t wos_proc_exec(const char* path, const char* const argv[], const char* const envp[]);

}
