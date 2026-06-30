#include "wkictl/dispatch.hpp"

// NOLINTNEXTLINE(bugprone-exception-escape): this CLI uses literal std::println calls throughout; failures are process-fatal.
auto main(int argc, char** argv) -> int { return wkictl::run(argc, argv); }
