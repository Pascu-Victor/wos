#include <dev/virtio/virtio_net.hpp>
#include <test/ktest.hpp>

KTEST_FINAL(VirtioDma, ProbeRetiresMappingsOrPinsUnprovenReset) {
    auto const RESULT = ker::dev::virtio::virtio_selftest_probe_unwind();
    bool const WITHHOLD = RESULT.withhold_reset_proof;
    if (RESULT.reject_platform_requested) KREQUIRE_TRUE(RESULT.found);
    if (!RESULT.found) {
        KEXPECT_EQ(RESULT.before.regions, size_t{0});
        return;
    }
    KEXPECT_TRUE(RESULT.platform_admission_closed);
    if (RESULT.reject_platform_requested) {
        KEXPECT_FALSE(RESULT.prepared);
        KEXPECT_FALSE(RESULT.bme_enabled);
        KEXPECT_TRUE(RESULT.bme_disabled);
        KEXPECT_EQ(RESULT.mapped.regions, size_t{0});
        KEXPECT_EQ(RESULT.after.regions, RESULT.before.regions);
        KEXPECT_EQ(RESULT.after.backing_pages, RESULT.before.backing_pages);
        KEXPECT_EQ(RESULT.after.quarantined, RESULT.before.quarantined);
        return;
    }
    KREQUIRE_TRUE(RESULT.prepared);
    KEXPECT_TRUE(RESULT.bme_enabled);
    KEXPECT_TRUE(RESULT.mapped.streaming != 0);
    if (RESULT.terminal_requested) {
        KEXPECT_TRUE(RESULT.terminal_published);
        KEXPECT_TRUE(RESULT.terminal_late_rejected);
        KEXPECT_TRUE(RESULT.terminal_irq_drained);
        KEXPECT_TRUE(RESULT.terminal_repeat_stable);
    }
    if (RESULT.packets_requested) {
        KEXPECT_EQ(RESULT.packets_verified, uint8_t{2});
        KEXPECT_EQ(RESULT.packet_pool_after, RESULT.packet_pool_before);
    }
    KEXPECT_TRUE(RESULT.bme_disabled);
    KEXPECT_TRUE(RESULT.reset_observed);
    KREQUIRE_TRUE(RESULT.mapped.regions >= RESULT.before.regions + 2);
    KEXPECT_EQ(RESULT.mapped.device_owned + RESULT.mapped.streaming, RESULT.mapped.regions);
    if (RESULT.control_present) KEXPECT_TRUE(RESULT.control_ack);
    if (WITHHOLD) {
        KEXPECT_EQ(RESULT.after.regions, RESULT.mapped.regions);
        KEXPECT_EQ(RESULT.after.backing_pages, RESULT.mapped.backing_pages);
        KEXPECT_EQ(RESULT.after.quarantined, RESULT.mapped.regions);
    } else {
        KEXPECT_EQ(RESULT.after.regions, RESULT.before.regions);
        KEXPECT_EQ(RESULT.after.backing_pages, RESULT.before.backing_pages);
        KEXPECT_EQ(RESULT.after.quarantined, RESULT.before.quarantined);
    }
}
