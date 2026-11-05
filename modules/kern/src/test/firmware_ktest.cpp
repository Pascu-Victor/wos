#include <dev/firmware/archive.hpp>
#include <dev/firmware/boot_source.hpp>
#include <dev/firmware/firmware.hpp>
#include <test/ktest.hpp>

KTEST(Firmware, BootPublicationAndTaskContext) {
    namespace fw = ker::dev::firmware;
    auto const INDEX = fw::source_index();
    if (fw::source_status() != fw::Error::none) {
        // Includes a source whose last trailer byte is corrupt: a valid first
        // matching blob must never make the private partial index observable.
        KEXPECT_TRUE(INDEX.empty());
    } else {
        KEXPECT_TRUE(INDEX.size() <= fw::MAX_INDEX_ENTRIES);
        for (auto const& entry : INDEX) {
            KEXPECT_TRUE(fw::canonical_name(entry.name));
            KEXPECT_TRUE(entry.bytes.size() <= fw::MAX_BLOB_BYTES);
        }
    }
    // BSP-only --selftest is deliberately NOT an ordinary schedulable task.
    fw::Scope denied;
    fw::View out{};
    fw::Policy policy{};
    KEXPECT_EQ(denied.request(policy, 0, out), fw::Error::context);
    KEXPECT_TRUE(out.bytes.empty());
    fw::Stats stats{123, 0, 0, 0};
    KEXPECT_EQ(fw::snapshot(stats), fw::Error::context);
    KEXPECT_EQ(stats.objects, uint64_t{123});
}
