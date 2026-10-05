#pragma once

namespace wkictl {

// Returns false for a management command; a recognized persona supplies its exit status.
auto run_launcher(int argc, char** argv, int& status) -> bool;
auto run(int argc, char** argv) -> int;

}  // namespace wkictl
