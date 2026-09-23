#include <cstdint>
#include <dev/iommu/vtd_model.hpp>
#include <test/ktest.hpp>

KTEST(VtdModel, TranslationEntriesAndInvalidationRetirement) {
    namespace vtd = ker::dev::iommu::vtd;
    auto const ENTRY = vtd::context_entry(0x1000, 48, 7, 4, 6, 16);
    KREQUIRE_TRUE(ENTRY.has_value());
    KEXPECT_EQ(ENTRY->low, uint64_t{0x1001});
    KEXPECT_EQ(ENTRY->high, uint64_t{0x702});
    KEXPECT_FALSE(vtd::table_entry(0x1001, 48).has_value());
    KEXPECT_FALSE(vtd::context_entry(0x1000, 48, 16, 4, 6, 16).has_value());
    vtd::Invalidation state;
    auto const TICKET = state.begin();
    KREQUIRE_NE(TICKET, uint64_t{0});
    KEXPECT_FALSE(state.completed(TICKET));
    KEXPECT_FALSE(state.issue_iotlb(TICKET));
    KEXPECT_EQ(state.observe_context(TICKET, vtd::BUSY), vtd::Observation::busy);
    KEXPECT_EQ(state.observe_context(TICKET, uint64_t{1} << 59), vtd::Observation::advance);
    KREQUIRE_TRUE(state.issue_iotlb(TICKET));
    KEXPECT_EQ(state.observe_iotlb(TICKET, uint64_t{1} << 57), vtd::Observation::advance);
    KEXPECT_TRUE(state.completed(TICKET));
    auto const NEXT = state.begin();
    KREQUIRE_NE(NEXT, TICKET);
    KEXPECT_FALSE(state.completed(TICKET));
    KEXPECT_TRUE(state.timeout(NEXT));
    KEXPECT_EQ(state.observe_context(NEXT, uint64_t{1} << 59), vtd::Observation::stale);
    KEXPECT_EQ(state.begin(), uint64_t{0});
}
