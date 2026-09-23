#include <dev/e1000e/e1000e.hpp>
#include <test/ktest.hpp>

KTEST_FINAL(E1000Dma, PacketOwnershipAndTerminalRetirement) {
    auto const R = ker::dev::e1000e::e1000e_selftest();
    if (!R.found) {
        KEXPECT_EQ(R.after.regions, size_t{0});
        return;
    }
    KEXPECT_TRUE(R.late_rejected);
    KEXPECT_TRUE(R.irq_drained);
    KEXPECT_TRUE(R.bme_disabled);
    KEXPECT_TRUE(R.repeat_stable);
    KEXPECT_EQ(R.pool_after, R.pool_before);
    if (R.fail_prepare) {
        KEXPECT_FALSE(R.prepared);
        KEXPECT_EQ(R.after.regions, R.before.regions);
        KEXPECT_EQ(R.after.backing_pages, R.before.backing_pages);
        return;
    }
    KREQUIRE_TRUE(R.prepared);
    KEXPECT_EQ(R.mapped.regions, size_t{514});
    KEXPECT_EQ(R.mapped.streaming, size_t{512});
    if (R.packets_requested) {
        KEXPECT_EQ(R.packets_verified, uint8_t{3});
        KEXPECT_EQ(R.echoes, size_t{300});
    }
    if (R.release_supported && !R.withheld) {
        KEXPECT_EQ(R.retirement_result, 0);
        KEXPECT_FALSE(R.withheld);
        KEXPECT_EQ(R.after.regions, R.before.regions);
        KEXPECT_EQ(R.after.backing_pages, R.before.backing_pages);
    } else {
        KEXPECT_NE(R.retirement_result, 0);
        KEXPECT_TRUE(R.after.quarantined >= size_t{258});
        KEXPECT_EQ(R.after.regions, R.after.quarantined);
    }
}
