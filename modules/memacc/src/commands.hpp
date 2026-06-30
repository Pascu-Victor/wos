#pragma once

namespace memacc {

void usage();
auto run_dump(int argc, char** argv) -> int;
auto run_summary() -> int;
auto run_procs(int argc, char** argv) -> int;
auto run_proc(int argc, char** argv) -> int;
auto run_kernel() -> int;
auto run_allocs(int argc, char** argv) -> int;
auto run_raw(int argc, char** argv) -> int;
auto run_watch_command(int argc, char** argv) -> int;
auto run_track(int argc, char** argv) -> int;
auto run_reclaim(int argc, char** argv) -> int;

}  // namespace memacc
