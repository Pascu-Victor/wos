#pragma once

#include <cstdint>

namespace wkictl {

auto set_target_policy(const char* policy, uint32_t extra_flags) -> int64_t;
auto run_locally(int argc, char** argv) -> int;
auto run_remotely(int argc, char** argv) -> int;
auto run_anywhere(int argc, char** argv) -> int;
auto run_on(int argc, char** argv) -> int;
auto run_homeward(int argc, char** argv) -> int;
auto handle_target(int argc, char** argv) -> int;

}  // namespace wkictl
