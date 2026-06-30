#pragma once

namespace wkictl {

auto run_locally(int argc, char** argv) -> int;
auto run_remotely(int argc, char** argv) -> int;
auto run_on(int argc, char** argv) -> int;
auto run_homeward(int argc, char** argv) -> int;
auto handle_target(int argc, char** argv) -> int;

}  // namespace wkictl
