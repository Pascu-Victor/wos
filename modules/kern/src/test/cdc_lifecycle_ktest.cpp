#include <dev/usb/cdc_ether.hpp>
#include <test/ktest.hpp>

namespace {

using ker::dev::usb::cdc_io_release;
using ker::dev::usb::cdc_io_try_acquire;
using ker::dev::usb::CdcEtherDevice;
using ker::dev::usb::CdcEtherState;

KTEST(CdcLifecycle, RetirementClosesAdmissionAndDrainsExactReaders) {
    CdcEtherDevice cdc{};
    cdc.state.store(CdcEtherState::LIVE, std::memory_order_release);

    KREQUIRE_TRUE(cdc_io_try_acquire(cdc));
    KREQUIRE_TRUE(cdc_io_try_acquire(cdc));
    KEXPECT_EQ(cdc.io_readers.load(std::memory_order_acquire), 2U);

    cdc.state.store(CdcEtherState::RETIRING, std::memory_order_release);
    KEXPECT_FALSE(cdc_io_try_acquire(cdc));
    KEXPECT_TRUE(cdc_io_release(cdc));
    KEXPECT_TRUE(cdc_io_release(cdc));
    KEXPECT_FALSE(cdc_io_release(cdc));
    KEXPECT_EQ(cdc.io_readers.load(std::memory_order_acquire), 0U);
}

KTEST(CdcLifecycle, DrainedStaticSlotCanOpenAFreshGeneration) {
    CdcEtherDevice cdc{};
    cdc.state.store(CdcEtherState::LIVE, std::memory_order_release);
    KREQUIRE_TRUE(cdc_io_try_acquire(cdc));
    cdc.state.store(CdcEtherState::RETIRING, std::memory_order_release);
    KREQUIRE_TRUE(cdc_io_release(cdc));
    KREQUIRE_EQ(cdc.io_readers.load(std::memory_order_acquire), 0U);

    cdc.state.store(CdcEtherState::FREE, std::memory_order_release);
    KEXPECT_FALSE(cdc_io_try_acquire(cdc));
    cdc.state.store(CdcEtherState::PREPARING, std::memory_order_release);
    KEXPECT_FALSE(cdc_io_try_acquire(cdc));
    cdc.state.store(CdcEtherState::LIVE, std::memory_order_release);
    KEXPECT_TRUE(cdc_io_try_acquire(cdc));
    KEXPECT_TRUE(cdc_io_release(cdc));
}

}  // namespace
