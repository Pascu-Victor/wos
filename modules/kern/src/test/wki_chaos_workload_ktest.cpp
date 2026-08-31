#include <net/wki/chaos_workload.hpp>
#include <test/ktest.hpp>

KTEST(WkiChaosWorkload, StrictParserAndScratchBounds) { KEXPECT_TRUE(ker::net::wki::wki_chaos_workload_selftest_parser_and_bounds()); }
