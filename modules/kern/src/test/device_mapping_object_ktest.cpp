#include "device_mapping_object_ktest.hpp"

#include <abi/callnums/vmem.h>

#include <cerrno>
#include <dev/device_mapping_client.hpp>
#include <memory>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/mm/lifetime_retirement_executor.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sched/scheduler.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>

namespace ker::test {
namespace {
namespace mapping = ker::dev::mapping;
namespace fm = ker::mod::mm::file_mapping;
namespace phys = ker::mod::mm::phys;
namespace sched = ker::mod::sched;
namespace vm = ker::abi::vmem;
using log = ker::mod::dbg::logger<"device-object-test">;
constexpr uint64_t PAGE = mapping::OBJECT_PAGE_SIZE;
constexpr uint64_t RW = vm::PROT_READ | vm::PROT_WRITE;

struct Checks {
    unsigned assertions{};
    auto expect(bool valid, const char* reason) -> bool {
        ++assertions;
        if (!valid) log::error("DEVICE_OBJECT_RUNTIME FAIL %s", reason);
        return valid;
    }
    auto returned(fm::RetirementCompletion& completion) -> bool {
        uint64_t const deadline = sched::saturating_deadline_us(ker::mod::time::get_us(), 5'000'000);
        for (;;) {
            auto const status = completion.query();
            if (status.state == fm::RetirementReturnState::RETURNED)
                return expect(status.callback_result == 0, "actual permanent executor callback returned successfully");
            if (!expect(status.state == fm::RetirementReturnState::PENDING && ker::mod::time::get_us() < deadline,
                        "bounded exact-generation retirement wait"))
                return false;
            sched::kern_sleep_us(1000);
        }
    }
};

auto storage_and_irq_export(Checks& checks) -> bool {
    mapping::QuotaAccountRef account;
    mapping::ObjectRef object;
    fm::RetirementCompletion account_done, object_done;
    if (!checks.expect(mapping::QuotaAccount::create({.resident_pages = 4, .objects = 1, .max_object_pages = 4}, account) == 0 &&
                           account.get()->observe_return(account_done),
                       "actual retained quota and permanent account ticket") ||
        !checks.expect(mapping::Object::create(account, 3 * PAGE, RW, object) == 0 && object.get()->observe_return(object_done),
                       "actual zeroed PMM object and exact retirement observation"))
        return false;
    bool zeroed = true;
    for (size_t page = 0; page < object.get()->pages(); ++page) {
        auto* bytes = static_cast<unsigned char*>(object.get()->cpu_page(page * PAGE));
        zeroed = zeroed && bytes != nullptr && phys::page_ref_get(bytes) == 1;
        if (bytes != nullptr)
            for (size_t index = 0; index < PAGE; ++index) zeroed = zeroed && bytes[index] == 0;
    }
    if (!checks.expect(zeroed && account.get()->usage().resident_pages == 3 && account.get()->usage().objects == 1,
                       "every real order0 page initialized and charged"))
        return false;
    mapping::ObjectRef denied;
    if (!checks.expect(mapping::Object::create(account, PAGE, RW, denied) == -EDQUOT && !denied,
                       "object-count quota refuses before publishing"))
        return false;
    mapping::PageLoan cancelled;
    auto* page = object.get()->cpu_page(0);
    if (!checks.expect(object.get()->prepare_page(0, cancelled) && phys::page_ref_get(page) == 2, "actual independent PMM page loan"))
        return false;
    cancelled.reset();
    if (!checks.expect(phys::page_ref_get(page) == 1, "cancel returns only its owned PMM reference")) return false;
    uint64_t physical{};
    mapping::ObjectExportRef exported;
    if (!checks.expect(object.get()->frame_at(0, physical) && object.get()->export_page(0, physical, exported) &&
                           phys::page_ref_try_inc(page) && object.get()->owners().exports == 1,
                       "independent exported frame owns Object charge without File or root"))
        return false;
    object.reset();
    ker::mod::sys::Spinlock irq_lock;
    auto const irq_flags = irq_lock.lock_irqsave();
    uint32_t const frame_refs = phys::page_ref_dec(page);
    exported.reset();
    irq_lock.unlock_irqrestore(irq_flags);
    // No page/Object access after the last return: the actual worker may have
    // already freed it on another CPU. Only retained account/completion remain.
    if (!checks.expect(frame_refs == 1, "frame returned before final export token under actual IRQ guard") ||
        !checks.returned(object_done) ||
        !checks.expect(account.get()->usage().resident_pages == 0 && account.get()->usage().objects == 0,
                       "backing and resident charge released before executor RETURNED"))
        return false;
    account.reset();
    return checks.returned(account_done);
}

auto grants_and_revoke(Checks& checks) -> bool {
    mapping::QuotaAccountRef account;
    mapping::ObjectRef object;
    fm::RetirementCompletion object_done, account_done;
    if (!checks.expect(mapping::QuotaAccount::create({.resident_pages = 4, .objects = 2}, account) == 0 &&
                           account.get()->observe_return(account_done) && mapping::Object::create(account, 4 * PAGE, RW, object) == 0 &&
                           object.get()->observe_return(object_done),
                       "grant fixture uses actual owned backing and quota"))
        return false;
    struct Context final : mapping::EndpointContext {};
    std::unique_ptr<mapping::EndpointContext> context(new (std::nothrow) Context);
    mapping::EndpointRef endpoint;
    mapping::ClientRef first, second;
    if (!checks.expect(context != nullptr && mapping::Endpoint::create(240, 3, "object-grants", {}, std::move(context), endpoint) == 0 &&
                           mapping::Client::open(endpoint, {}, {.open_flags = 2}, first) == 0 &&
                           mapping::Client::open(endpoint, {}, {.open_flags = 0, .read_only = true}, second) == 0 &&
                           first.get()->configure_grants(2) == 0 && second.get()->configure_grants(2) == 0,
                       "real distinct Clients with lazy bounded grant tables"))
        return false;
    mapping::Grant grant, foreign;
    mapping::PreparedGrant prepared;
    if (!checks.expect(
            first.get()->grant(object, PAGE, 3 * PAGE, RW, grant) == 0 && second.get()->grant(object, 0, PAGE, RW, foreign) == 0 &&
                grant.base != foreign.base && second.get()->lookup(grant.base, PAGE, vm::PROT_READ, vm::MAP_SHARED, prepared) == -ENOENT &&
                first.get()->lookup(grant.end, PAGE, vm::PROT_READ, vm::MAP_SHARED, prepared) == -ENOENT &&
                first.get()->lookup(grant.base + PAGE, PAGE, RW, vm::MAP_SHARED, prepared) == 0 && prepared.object_offset() == 2 * PAGE,
            "nonreused intervals support delta and refuse foreign/guard page"))
        return false;
    mapping::PreparedGrant refused;
    if (!checks.expect(second.get()->lookup(foreign.base, PAGE, vm::PROT_WRITE, vm::MAP_SHARED, refused) == -EACCES && !refused &&
                           first.get()->remove_grant(grant) == 0 &&
                           first.get()->lookup(grant.base, PAGE, vm::PROT_READ, vm::MAP_SHARED, refused) == -ENOENT,
                       "captured mount/open ceiling and removed grant remain authoritative"))
        return false;
    {
        ker::syscall::vmem::SharedVmemPublicationGuard pub;
        if (!checks.expect(object.get()->begin_revoke(pub) && !object.get()->try_finish_revoke(pub),
                           "actual PUB closure waits prepared BINDING owner"))
            return false;
    }
    prepared.reset();
    {
        ker::syscall::vmem::SharedVmemPublicationGuard pub;
        if (!checks.expect(object.get()->try_finish_revoke(pub), "prepared return permits terminal never-reopen revoke")) return false;
    }
    first.reset();
    second.reset();
    endpoint.reset();
    object.reset();
    if (!checks.returned(object_done) || !checks.expect(account.get()->usage().resident_pages == 0, "grant teardown returns actual charge"))
        return false;
    account.reset();
    return checks.returned(account_done);
}
}  // namespace

auto run_device_mapping_object_selftest() -> bool {
    Checks checks;
    if (!checks.expect(phys::can_wait_for_reclaim(), "ordinary scheduled Object fixture context")) return false;
    if (!storage_and_irq_export(checks)) return false;
    log::info("DEVICE_OBJECT_RUNTIME CASE_PASS real_storage_quota_irq_export_return");
    if (!grants_and_revoke(checks)) return false;
    log::info("DEVICE_OBJECT_RUNTIME CASE_PASS client_intervals_prepared_revoke");
    log::info("DEVICE_OBJECT_RUNTIME PASS cases=2 assertions=%u no_mapping=1", checks.assertions);
    return true;
}
}  // namespace ker::test
