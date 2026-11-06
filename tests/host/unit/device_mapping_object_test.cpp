#include <abi/callnums/vmem.h>
#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <dev/device_mapping_client.hpp>
#include <dev/device_mapping_grants.hpp>
#include <dev/device_mapping_object.hpp>
#include <memory>
#include <object_pages.hpp>
#include <platform/mm/phys.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
#include <thread>

namespace {
namespace mapping = ker::dev::mapping;
namespace fixture = object_fixture;
namespace phys = ker::mod::mm::phys;
namespace vm = ker::abi::vmem;
constexpr uint64_t PAGE = mapping::OBJECT_PAGE_SIZE;
constexpr uint64_t RW = vm::PROT_READ | vm::PROT_WRITE;

class DeviceMappingObject : public ::testing::Test {
   protected:
    void SetUp() override {
        fixture::ordinary = fixture::available = true;
        fixture::fail_pin = false;
        fixture::pat_complete = fixture::pat_wb = true;
        fixture::pat_other_bad = false;
        fixture::fail_allocate_after = fixture::fail_reserve_after = -1;
        fixture::drain();
        ASSERT_EQ(fixture::reserved(), 0U);
        ASSERT_EQ(fixture::live_pages(), 0U);
    }
    void TearDown() override {
        fixture::ordinary = fixture::available = true;
        fixture::fail_pin = false;
        fixture::pat_complete = fixture::pat_wb = true;
        fixture::pat_other_bad = false;
        fixture::fail_allocate_after = fixture::fail_reserve_after = -1;
        fixture::drain();
        EXPECT_EQ(fixture::reserved(), 0U);
        EXPECT_EQ(fixture::live_pages(), 0U);
        EXPECT_EQ(fixture::irq_depth, 0U);
        EXPECT_EQ(fixture::lock_depth, 0U);
        EXPECT_EQ(fixture::mutex_depth, 0U);
    }
    auto account(uint64_t pages = 16, uint64_t objects = 8, uint64_t maximum = 16) -> mapping::QuotaAccountRef {
        mapping::QuotaAccountRef result;
        EXPECT_EQ(mapping::QuotaAccount::create({pages, objects, maximum}, result), 0);
        return result;
    }
    auto object(const mapping::QuotaAccountRef& account, uint64_t pages = 3, uint64_t rights = RW) -> mapping::ObjectRef {
        mapping::ObjectRef result;
        EXPECT_EQ(mapping::Object::create(account, pages * PAGE, rights, result), 0);
        return result;
    }
    auto client(unsigned flags = 2, bool read_only = false) -> mapping::ClientRef {
        struct Context final : mapping::EndpointContext {};
        mapping::EndpointRef endpoint;
        EXPECT_EQ(mapping::Endpoint::create(240, 2, "grant-test", {}, std::make_unique<Context>(), endpoint), 0);
        mapping::ClientRef result;
        EXPECT_EQ(mapping::Client::open(endpoint, {}, {.open_flags = flags, .read_only = read_only}, result), 0);
        return result;
    }
};

TEST_F(DeviceMappingObject, OwnsInitializedIndependentPagesAndRealResidentQuotaThroughCallback) {
    auto quota = account();
    auto owned = object(quota);
    ASSERT_TRUE(owned);
    EXPECT_EQ(owned.get()->pages(), 3U);
    EXPECT_EQ(owned.get()->bytes(), 3 * PAGE);
    EXPECT_NE(owned.get()->generation(), 0U);
    EXPECT_EQ(quota.get()->usage().resident_pages, 3U);
    std::array<uint64_t, 3> physical{};
    for (size_t page = 0; page < physical.size(); ++page) {
        ASSERT_TRUE(owned.get()->frame_at(page * PAGE, physical[page]));
        auto* cpu = static_cast<unsigned char*>(owned.get()->cpu_page(page * PAGE));
        ASSERT_NE(cpu, nullptr);
        EXPECT_EQ(fixture::references(cpu), 1U);
        for (size_t byte = 0; byte < PAGE; ++byte) ASSERT_EQ(cpu[byte], 0);
    }
    EXPECT_NE(physical[0], physical[1]);
    uint64_t unchanged = 57;
    EXPECT_FALSE(owned.get()->frame_at(1, unchanged));
    EXPECT_FALSE(owned.get()->frame_at(3 * PAGE, unchanged));
    EXPECT_EQ(unchanged, 57U);
    owned.reset();
    EXPECT_EQ(fixture::live_pages(), 3U);
    EXPECT_EQ(quota.get()->usage().resident_pages, 3U);
    EXPECT_EQ(fixture::queued(), 1U);
    ASSERT_TRUE(fixture::run_one());
    EXPECT_EQ(fixture::live_pages(), 0U);
    EXPECT_EQ(quota.get()->usage().resident_pages, 0U);
    EXPECT_EQ(quota.get()->usage().objects, 0U);
}

TEST_F(DeviceMappingObject, AllocationQuotaAndTicketFailuresRollBackWithoutPublishingOrLeaking) {
    auto quota = account(5, 1, 4);
    mapping::ObjectRef empty;
    EXPECT_EQ(mapping::Object::create(quota, 0, RW, empty), -EINVAL);
    EXPECT_EQ(mapping::Object::create(quota, PAGE + 1, RW, empty), -EINVAL);
    EXPECT_EQ(mapping::Object::create(quota, PAGE, RW | vm::PROT_EXEC, empty), -EINVAL);
    EXPECT_EQ(mapping::Object::create(quota, 5 * PAGE, RW, empty), -EDQUOT);
    fixture::fail_allocate_after = 1;
    EXPECT_EQ(mapping::Object::create(quota, 3 * PAGE, RW, empty), -ENOMEM);
    EXPECT_FALSE(empty);
    EXPECT_EQ(fixture::live_pages(), 0U);
    EXPECT_EQ(quota.get()->usage().resident_pages, 0U);
    EXPECT_EQ(quota.get()->usage().objects, 0U);
    EXPECT_EQ(fixture::reserved(), 1U);  // Only the account's actual reserved slot.
    fixture::fail_allocate_after = -1;
    fixture::fail_reserve_after = 0;
    EXPECT_EQ(mapping::Object::create(quota, PAGE, RW, empty), -ENOSPC);
    EXPECT_FALSE(empty);
    EXPECT_EQ(quota.get()->usage().resident_pages, 0U);
    fixture::fail_reserve_after = -1;
    auto first = object(quota, 2);
    EXPECT_EQ(mapping::Object::create(quota, PAGE, RW, empty), -EDQUOT);
    EXPECT_FALSE(empty);
    fixture::ordinary = false;
    EXPECT_EQ(mapping::Object::create(quota, PAGE, RW, empty), -EWOULDBLOCK);
    fixture::ordinary = true;
}

TEST_F(DeviceMappingObject, NewWbAdmissionRequiresCompleteActualWbObservationOnly) {
    auto quota = account();
    mapping::ObjectRef empty;
    fixture::pat_complete = false;
    EXPECT_EQ(mapping::Object::create(quota, PAGE, RW, empty), -EOPNOTSUPP);
    fixture::pat_complete = true;
    fixture::pat_wb = false;
    EXPECT_EQ(mapping::Object::create(quota, PAGE, RW, empty), -EOPNOTSUPP);
    EXPECT_FALSE(empty);
    EXPECT_EQ(quota.get()->usage().resident_pages, 0U);
    EXPECT_EQ(fixture::live_pages(), 0U);
    EXPECT_EQ(fixture::reserved(), 1U);
    fixture::pat_wb = true;
    fixture::pat_other_bad = true;
    EXPECT_EQ(mapping::Object::create(quota, PAGE, RW, empty), 0);
    EXPECT_TRUE(empty);  // UC/WC mismatch is not a refusal of observed WB0.
}

TEST_F(DeviceMappingObject, LoanCancelAndGuardedTransferPreserveOneIndependentPteReference) {
    auto quota = account();
    auto owned = object(quota, 1);
    auto* cpu = owned.get()->cpu_page(0);
    mapping::PageLoan refused;
    fixture::fail_pin = true;
    EXPECT_FALSE(owned.get()->prepare_page(0, refused));
    EXPECT_FALSE(refused);
    EXPECT_EQ(owned.get()->owners().by_role[static_cast<size_t>(mapping::ObjectRole::loan)], 0U);
    fixture::fail_pin = false;
    mapping::PageLoan cancelled;
    ASSERT_TRUE(owned.get()->prepare_page(0, cancelled));
    EXPECT_EQ(fixture::references(cpu), 2U);
    cancelled.reset();
    EXPECT_EQ(fixture::references(cpu), 1U);
    mapping::PageLoan loan;
    mapping::ObjectRef binding;
    ASSERT_TRUE(owned.clone_into(binding, mapping::ObjectRole::binding));
    ASSERT_TRUE(owned.get()->prepare_page(0, loan));
    auto const physical = loan.physical();
    {
        ker::syscall::vmem::SharedVmemPublicationGuard pub;
        mapping::ObjectCommitGuard commit;
        EXPECT_FALSE(commit.try_acquire(owned, owned.get()->generation(), pub));
        ASSERT_TRUE(commit.try_acquire(binding, owned.get()->generation(), pub));
        EXPECT_FALSE(commit.commit_loan(loan, owned));
        ASSERT_TRUE(commit.commit_loan(loan, binding));
        EXPECT_FALSE(loan);
        commit.finish();
    }
    EXPECT_EQ(fixture::references(cpu), 2U);
    EXPECT_EQ(physical, reinterpret_cast<uint64_t>(cpu));
    EXPECT_EQ(phys::page_ref_dec(cpu), 1U);  // Real simulated PTE return, before BINDING owner.
    binding.reset();
}

TEST_F(DeviceMappingObject, ExportReturnUnderIrqKeepsBackingAndChargeUntilActualWorkerRuns) {
    auto quota = account();
    auto owned = object(quota, 1);
    auto* cpu = owned.get()->cpu_page(0);
    uint64_t physical{};
    ASSERT_TRUE(owned.get()->frame_at(0, physical));
    mapping::ObjectExportRef exported;
    EXPECT_FALSE(owned.get()->export_page(0, physical + PAGE, exported));
    ASSERT_TRUE(owned.get()->export_page(0, physical, exported));
    ASSERT_TRUE(phys::page_ref_try_inc(cpu));  // UserPageFrameRef's independent frame ref.
    EXPECT_EQ(owned.get()->owners().exports, 1U);
    owned.reset();
    EXPECT_EQ(fixture::queued(), 0U);
    {
        fixture::IrqScope irq;
        EXPECT_EQ(phys::page_ref_dec(cpu), 1U);
        exported.reset();
        EXPECT_EQ(fixture::live_pages(), 1U);
        EXPECT_EQ(quota.get()->usage().resident_pages, 1U);
        EXPECT_EQ(fixture::queued(), 1U);
    }
    fixture::drain();
    EXPECT_EQ(fixture::live_pages(), 0U);
    EXPECT_EQ(quota.get()->usage().resident_pages, 0U);
}

TEST_F(DeviceMappingObject, RevokeRequiresPubAndWaitsPreparedLoansAndIndependentExports) {
    auto quota = account();
    auto owned = object(quota, 1);
    mapping::ObjectRef binding;
    mapping::PageLoan loan;
    mapping::ObjectExportRef exported;
    uint64_t physical{};
    ASSERT_TRUE(owned.clone_into(binding, mapping::ObjectRole::binding));
    ASSERT_TRUE(owned.get()->prepare_page(0, loan));
    ASSERT_TRUE(owned.get()->frame_at(0, physical));
    ASSERT_TRUE(owned.get()->export_page(0, physical, exported));
    {
        ker::syscall::vmem::SharedVmemPublicationGuard pub;
        bool foreign = true;
        std::thread other([&] { foreign = owned.get()->begin_revoke(pub); });
        other.join();
        EXPECT_FALSE(foreign);
        ASSERT_TRUE(owned.get()->begin_revoke(pub));
        EXPECT_FALSE(owned.get()->try_finish_revoke(pub));
        mapping::ObjectExportRef refused;
        EXPECT_FALSE(owned.get()->export_page(0, physical, refused));
        mapping::ObjectCommitGuard commit;
        EXPECT_FALSE(commit.try_acquire(binding, owned.get()->generation(), pub));
    }
    loan.reset();
    binding.reset();
    {
        ker::syscall::vmem::SharedVmemPublicationGuard pub;
        EXPECT_FALSE(owned.get()->try_finish_revoke(pub));
    }
    exported.reset();
    {
        ker::syscall::vmem::SharedVmemPublicationGuard pub;
        EXPECT_TRUE(owned.get()->try_finish_revoke(pub));
        EXPECT_FALSE(owned.get()->begin_revoke(pub));
    }
    EXPECT_EQ(owned.get()->state(), mapping::ObjectState::revoked);
    EXPECT_FALSE(owned.get()->prepare_page(0, loan));
}

TEST_F(DeviceMappingObject, OffsetAllocatorReservesWholeDisjointIntervalsAndFailsTerminally) {
    mapping::detail::ObjectGenerations generations(UINT64_MAX - 1);
    EXPECT_EQ(generations.issue(), UINT64_MAX);
    EXPECT_EQ(generations.issue(), 0U);
    EXPECT_EQ(generations.issue(), 0U);
    mapping::detail::OffsetIntervals intervals(PAGE, 7 * PAGE);
    mapping::Grant first, second, exhausted;
    ASSERT_TRUE(intervals.reserve(2 * PAGE, first));
    ASSERT_TRUE(intervals.reserve(PAGE, second));
    EXPECT_EQ(first.base, PAGE);
    EXPECT_EQ(first.end, 3 * PAGE);
    EXPECT_EQ(second.base, 4 * PAGE);
    EXPECT_EQ(second.end, 5 * PAGE);
    EXPECT_NE(first.generation, second.generation);
    EXPECT_FALSE(intervals.reserve(PAGE, exhausted));
    EXPECT_FALSE(intervals.reserve(PAGE, exhausted));
    EXPECT_TRUE(exhausted.empty());
    EXPECT_FALSE(intervals.reserve(UINT64_MAX, exhausted));
    EXPECT_FALSE(intervals.reserve(0, exhausted));
    auto unchanged = first;
    EXPECT_FALSE(intervals.reserve(PAGE, unchanged));
    EXPECT_EQ(unchanged.base, first.base);
}

TEST_F(DeviceMappingObject, ClientScopedIntervalsRejectForeignStaleGuardAndExtentCrossingLookups) {
    auto quota = account();
    auto owned = object(quota, 4);
    auto first = client(), second = client();
    EXPECT_EQ(first.get()->configure_grants(0), -EINVAL);
    ASSERT_EQ(first.get()->configure_grants(1), 0);
    EXPECT_EQ(first.get()->configure_grants(1), -EALREADY);
    ASSERT_EQ(second.get()->configure_grants(1), 0);
    mapping::Grant a, b;
    ASSERT_EQ(first.get()->grant(owned, PAGE, 3 * PAGE, RW, a), 0);
    ASSERT_EQ(second.get()->grant(owned, 0, PAGE, RW, b), 0);
    EXPECT_NE(a.base, b.base);
    mapping::PreparedGrant prepared;
    EXPECT_EQ(second.get()->lookup(a.base, PAGE, vm::PROT_READ, vm::MAP_SHARED, prepared), -ENOENT);
    EXPECT_EQ(first.get()->lookup(a.end, PAGE, vm::PROT_READ, vm::MAP_SHARED, prepared), -ENOENT);
    EXPECT_EQ(first.get()->lookup(a.base + 2 * PAGE, 2 * PAGE, vm::PROT_READ, vm::MAP_SHARED, prepared), -EINVAL);
    ASSERT_EQ(first.get()->lookup(a.base + PAGE, 2 * PAGE, RW, vm::MAP_SHARED, prepared), 0);
    EXPECT_EQ(prepared.object_offset(), 2 * PAGE);
    EXPECT_EQ(prepared.length(), 2 * PAGE);
    EXPECT_EQ(prepared.object().get(), owned.get());
    EXPECT_EQ(prepared.object().role(), mapping::ObjectRole::binding);
    ASSERT_EQ(first.get()->remove_grant(a), 0);
    EXPECT_TRUE(prepared);  // Prepared owner survives table removal; commit still checks LIVE.
    prepared.reset();
    EXPECT_EQ(first.get()->lookup(a.base, PAGE, vm::PROT_READ, vm::MAP_SHARED, prepared), -ENOENT);
    mapping::Grant reminted;
    ASSERT_EQ(first.get()->grant(owned, 0, PAGE, RW, reminted), 0);
    EXPECT_NE(reminted.base, a.base);
    EXPECT_EQ(first.get()->remove_grant(a), -ENOENT);
    first.get()->close_grants();
    EXPECT_EQ(first.get()->lookup(reminted.base, PAGE, vm::PROT_READ, vm::MAP_SHARED, prepared), -ENODEV);
}

TEST_F(DeviceMappingObject, GrantRightsOnlyNarrowCapturedClientMountAndObjectCeilings) {
    auto quota = account();
    auto owned = object(quota, 2);
    auto readonly = client(0, true);
    ASSERT_EQ(readonly.get()->configure_grants(1), 0);
    mapping::Grant grant;
    ASSERT_EQ(readonly.get()->grant(owned, 0, PAGE, RW, grant), 0);
    mapping::PreparedGrant prepared;
    EXPECT_EQ(readonly.get()->lookup(grant.base, PAGE, vm::PROT_WRITE, vm::MAP_SHARED, prepared), -EACCES);
    EXPECT_EQ(readonly.get()->lookup(grant.base, PAGE, vm::PROT_EXEC, vm::MAP_SHARED, prepared), -EINVAL);
    EXPECT_EQ(readonly.get()->lookup(grant.base, PAGE, vm::PROT_READ, vm::MAP_PRIVATE, prepared), -EINVAL);
    ASSERT_EQ(readonly.get()->lookup(grant.base, PAGE, vm::PROT_NONE, vm::MAP_SHARED, prepared), 0);
    EXPECT_EQ(prepared.max_protections(), vm::PROT_READ);
    prepared.reset();
    mapping::Grant full;
    EXPECT_EQ(readonly.get()->grant(owned, PAGE, PAGE, RW, full), -ENOSPC);
    EXPECT_TRUE(full.empty());
    auto write_only = client(1);
    ASSERT_EQ(write_only.get()->configure_grants(1), 0);
    EXPECT_EQ(write_only.get()->grant(owned, 0, PAGE, RW, full), -EACCES);
}
}  // namespace
