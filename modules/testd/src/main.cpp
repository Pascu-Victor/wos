// testd - WOS kernel test daemon (Track B full-system coverage driver).
//
// Spawned by init after all services are up. Exercises kernel subsystems via
// syscall-level suites, reports [TESTD] PASS/FAIL per test over stdout, then exits.

#include "testd.hpp"

// NOLINTNEXTLINE(bugprone-exception-escape): testd reports failures via process status/logging.
auto main(int argc, char** argv) -> int {
    // Remote helper mode: child process execed to run on a remote node.
    // argv: testd --rh <mode> <fd>
    if (argc >= 4 && std::strcmp(argv[1], "--rh") == 0) {
        int const RH_STATUS = run_remote_helper(argc, argv);
        // Helper processes report only their remote FD operation status.
        // Do not let libc/rtld finalizers obscure that result.
        _exit(RH_STATUS);
    }

    testd_logf("%s", "[TESTD] starting");

    // Ensure /tmp exists.
    mkdir("/tmp", MODE_0755);

    run_all_tests();

    if (g_fail == 0 && g_pass != total_tests()) {
        testd_logf("[TESTD] FAIL: accounting mismatch: expected %d checks, ran %d", total_tests(), g_pass);
        return 1;
    }
    testd_logf("[TESTD] DONE: %d passed, %d failed", g_pass, g_fail);

    return (g_fail == 0) ? 0 : 1;
}
