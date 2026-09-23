#include <dev/usb/xhci.hpp>
#include <test/ktest.hpp>

KTEST_FINAL(XhciDma, ControllerProbeOwnershipAndHalt) {
    auto const R = ker::dev::usb::xhci_dma_selftest();
    if (!R.found) {
        KEXPECT_EQ(R.after.regions, size_t{0});
        return;
    }
    KEXPECT_TRUE(R.bme_off);
    if (R.fail_allocate) {
        KEXPECT_FALSE(R.prepared);
        KEXPECT_TRUE(R.released);
        KEXPECT_EQ(R.after.regions, R.before.regions);
        KEXPECT_EQ(R.after.backing_pages, R.before.backing_pages);
        return;
    }
    KREQUIRE_TRUE(R.prepared);
    KEXPECT_TRUE(R.mapped.regions >= size_t{4});
    KEXPECT_EQ(R.mapped.streaming, size_t{0});
    if (R.withheld) {
        KEXPECT_FALSE(R.released);
        KEXPECT_EQ(R.after.regions, R.mapped.regions);
        KEXPECT_EQ(R.after.quarantined, R.mapped.regions);
        KEXPECT_EQ(R.after.backing_pages, R.mapped.backing_pages);
    } else {
        KEXPECT_TRUE(R.released);
        KEXPECT_EQ(R.after.regions, R.before.regions);
        KEXPECT_EQ(R.after.backing_pages, R.before.backing_pages);
    }
}
