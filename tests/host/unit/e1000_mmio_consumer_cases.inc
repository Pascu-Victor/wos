// Included after unchanged complete e1000 source and shared actual-engine setup.
// Each named case runs in a fresh child; no production registry reset is used.
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <optional>

namespace e1000_host {
namespace driver = ker::dev::e1000e;
namespace dma = ker::dev::dma;
namespace mapping = ker::mod::mm::mmio;
namespace pci = ker::dev::pci;
inline constexpr std::array<std::string_view, 11> consumer_cases{
    "consumer_success",          "consumer_held_callback",     "consumer_probe_packet_failure", "consumer_probe_irq_failure",
    "consumer_stop_failure",     "consumer_mask_failure",      "consumer_initial_bme_refusal",  "consumer_withdrawal_retry",
    "consumer_observed_profile", "consumer_second_view_retry", "consumer_completed_va_reuse"};
inline constexpr uint64_t REUSE_BAR0 = 0x82060000, REUSE_BAR1 = 0x82080000;
inline std::optional<pci::DriverClaim> reuse_claim;
inline bool reuse_complete{};
inline size_t assertions{}, final_observations{};
inline std::string_view selected_case;
inline driver::E1000Device* device{};
inline std::unique_ptr<driver::CallbackGuard> held_callback;
inline bool held_released{};
inline uint64_t retry_ticket{}, retry_mapping{};
inline std::array<uint64_t, mapping::VIEW_CAPACITY> initial_tickets{}, pending_withdrawal_tickets{};
inline size_t stopped_effect_count{};

void expect(bool condition, const char* reason) {
    ++assertions;
    if (condition) return;
    std::fprintf(stderr, "e1000 consumer assertion: %s (%.*s)\n", reason, static_cast<int>(selected_case.size()), selected_case.data());
    std::abort();
}

void expect_sparse_views(const mapping::Report& report, bool mapped) {
    expect(report.view_count == driver::REGISTER_VIEWS.size(), "actual function owns both fixed sparse register views");
    for (size_t i = 0; i < report.view_count; ++i) {
        auto const& view = report.views[i];
        auto const& request = driver::REGISTER_VIEWS[i];
        expect(view.bar == request.bar && view.bar_offset == request.offset && view.bytes == request.bytes &&
                   view.physical == pci_cold_host::BAR0 + request.offset,
               "actual private PCI child geometry matches exact BAR-relative requested windows");
        expect(view.initial_ticket != 0 && view.cohort_incarnation == report.cohort_incarnation,
               "every real child retains its own completed initial CPU ticket from the same cohort");
        for (uint64_t offset = 0; offset < view.bytes; offset += 4096)
            expect(mmio_host::translate(view.virtual_base + offset) == (mapped ? view.physical + offset : UINT64_MAX),
                   "actual child hierarchy contains exactly its retained mapped pages or complete withdrawal");
        expect(mmio_host::translate(view.virtual_base - 4096) == UINT64_MAX &&
                   mmio_host::translate(view.virtual_base + view.bytes) == UINT64_MAX,
               "every child has genuinely absent guard pages");
        if (mapped)
            expect(view.phase == mapping::Phase::active && view.status == mapping::Status::ready,
                   "all children are ready before actual whole-function publication");
        else
            expect(view.phase == mapping::Phase::complete && view.status == mapping::Status::complete && view.withdrawal_ticket != 0,
                   "function completion requires actual completion of every child withdrawal");
    }
    expect(report.views[0].virtual_base != report.views[1].virtual_base && report.views[0].initial_ticket != report.views[1].initial_ticket,
           "distinct child VAs and maintenance tickets are retained separately");
    expect(mmio_host::translate(0xffff800000000000ULL + pci_cold_host::BAR0 + 0x1000) == UINT64_MAX &&
               mmio_host::translate(0xffff800000000000ULL + pci_cold_host::BAR0 + 0x6000) == UINT64_MAX,
           "parent-owned unmapped BAR gap and tail have no HHDM alias");
    expect(!mapping::detail::record_generic_physical_access(pci_cold_host::BAR0 + 0x1000, 4096) &&
               !mapping::detail::record_generic_physical_access(pci_cold_host::BAR0 + 0x6000, pci_cold_host::BAR_BYTES - 0x6000),
           "all unmapped BAR bytes remain protected by the actual parent physical tombstone");
}

void observe_boot(const controller_host::cm::detail::Controller&) {
    expect(driver::device_count == 1, "actual driver keeps exactly one permanently retained probe object");
    device = driver::devices[0];
    expect(device != nullptr && device->pci == pci_cold_host::selected_device(), "actual enumerated selected function owns the probe");
    expect(device->typed_mmio && device->mmio == nullptr, "selected production path never exports or falls back to a raw pointer");
    auto const STATE = device->retirement.load();
    auto const REPORT = device->registers.report();
    if (selected_case == "consumer_initial_bme_refusal") {
        expect(STATE == driver::E1000Device::Retirement::cold_failed, "initial firmware BME refuses before register initialization");
        expect(device->registers.empty() && register_effects.empty(), "cold refusal produces no public view or register effect");
        expect(dma::stats(device->pci).regions == 0, "cold refusal creates no DMA region");
    } else if (selected_case == "consumer_probe_packet_failure" || selected_case == "consumer_probe_irq_failure") {
        expect(STATE == driver::E1000Device::Retirement::released, "real failed probe drops its normal borrow before terminal retirement");
        expect(!device->published && !device->irq_owned && published == nullptr, "failed probe leaves no registered endpoint or IRQ");
        expect(REPORT.phase == mapping::Phase::complete && REPORT.normal_readers == 0 && !REPORT.terminal_active,
               "real failed probe withdraws the actual typed leaves after dropping terminal access");
        expect(dma::stats(device->pci).regions == 0 && packets.empty(), "real DMA registry and packet borrows fully unwind");
        expect_sparse_views(REPORT, false);
    } else {
        expect(STATE == driver::E1000Device::Retirement::live && published == &device->netdev && device->irq_owned,
               "actual driver publishes only after the real factory and complete setup");
        expect(REPORT.phase == mapping::Phase::active && REPORT.id != 0 && REPORT.initial_ticket != 0 && REPORT.normal_readers == 0 &&
                   !REPORT.admission_closed && !REPORT.terminal_active,
               "actual engine opens a real typed mapping after maintenance and PCI activation");
        expect_sparse_views(REPORT, true);
        for (size_t i = 0; i < REPORT.view_count; ++i) initial_tickets[i] = REPORT.views[i].initial_ticket;
        auto const DMA = dma::stats(device->pci);
        expect(DMA.regions == 514 && DMA.streaming == 512, "actual managed driver created its full real DMA registry");
        expect(device->netdev.ops->open(&device->netdev) == 0 && device->registers.report().normal_readers == 0,
               "actual open callback balances its counted normal view");
        if (selected_case == "consumer_observed_profile") {
            expect(REPORT.physical_spans == 3, "captured-profile route owns all three exact finite memory BARs");
            expect(pci::pci_find_capability(device->pci, 1) == 0xc8 && pci::pci_find_capability(device->pci, pci::PCI_CAP_ID_MSI) == 0xd0 &&
                       pci::pci_find_capability(device->pci, 0x10) == 0xe0 &&
                       pci::pci_find_capability(device->pci, pci::PCI_CAP_ID_MSIX) == 0xa0,
                   "actual capability walk retains the captured PM/MSI/RCiEPv1/MSI-X chain");
            expect(pci::pci_config_read16(device->pci->bus, device->pci->slot, device->pci->function, 0xe2) == 0x91,
                   "actual bounded collector accepts captured RCiEP version/type without inventing a Link group");
            expect(!mapping::detail::record_generic_physical_access(OBSERVED_PROFILE_BAR3, OBSERVED_PROFILE_BAR3_BYTES),
                   "actual engine permanently reserves the unmapped 16-KiB MSI-X companion BAR");
            expect(!mapping::detail::record_generic_physical_access(OBSERVED_PROFILE_BAR3 + OBSERVED_PROFILE_BAR3_BYTES - 4096, 4096) &&
                       (pci::pci_config_read32(device->pci->bus, device->pci->slot, device->pci->function, 0xa4) & 7U) == 3 &&
                       (pci::pci_config_read32(device->pci->bus, device->pci->slot, device->pci->function, 0xa8) & 7U) == 3,
                   "actual captured table/PBA both select BAR3 and its final finite page remains owned");
            expect((pci::pci_config_read16(device->pci->bus, device->pci->slot, device->pci->function, 0xd2) & 1U) != 0 &&
                       (pci::pci_config_read16(device->pci->bus, device->pci->slot, device->pci->function, 0xa2) & 0x8000U) == 0,
                   "actual consumer enables captured 64-bit MSI while inherited MSI-X remains disabled");
        }
    }
}

void release_held_callback() {
    expect(held_callback != nullptr && static_cast<bool>(*held_callback), "actual callback owner survives close until its own return");
    expect(device->admission.closed() && device->registers.report().admission_closed, "real terminal path first closes both admissions");
    expect(device->registers.report().normal_readers == 1 && !device->registers.report().terminal_active,
           "actual counted callback prevents unique terminal admission");
    expect(held_callback->registers().read32(driver::REG_STATUS) != UINT32_MAX,
           "a genuine existing borrow remains usable after admission closure");
    expect(held_callback->registers().read32(1, 0x5b00 - 0x2000) != UINT32_MAX,
           "the same held function Borrow keeps the second child usable while global terminal admission is blocked");
    expect(dma::stats(device->pci).regions == 514, "drain wait retains real DMA backing");
    held_callback.reset();
    held_released = true;
    yield_hook = nullptr;
}

void observe_retirement(const controller_host::cm::detail::Controller&) {
    ++final_observations;
    auto const RESULT = controller_host::last_retirement_result;
    auto const REPORT = device->registers.report();
    if (selected_case == "consumer_stop_failure" || selected_case == "consumer_mask_failure") {
        expect(RESULT == -EIO && device->retirement.load() == driver::E1000Device::Retirement::failed,
               "uncertain real hardware retirement never becomes a retryable CPU-only continuation");
        expect(
            REPORT.phase != mapping::Phase::complete && REPORT.withdrawal_ticket == 0 && REPORT.admission_closed && !REPORT.terminal_active,
            "uncertain stop drops terminal access but cannot issue a quiescence witness or withdraw leaves");
        auto const DMA = dma::stats(device->pci);
        expect(DMA.regions == 259 && DMA.quarantined == 259,
               "both rings, every exposed RX pool and the previously exposed TX pool remain in the real DMA registry");
        expect(device->irq_owned == (selected_case == "consumer_mask_failure"), "actual interrupt release requires verified mask");
        return;
    }
    if ((selected_case == "consumer_withdrawal_retry" || selected_case == "consumer_second_view_retry") && final_observations == 1) {
        expect(RESULT == -EAGAIN && REPORT.phase == mapping::Phase::withdrawal_pending && REPORT.withdrawal_ticket != 0 &&
                   REPORT.timed_out_ever && REPORT.normal_readers == 0 && !REPORT.terminal_active,
               "real withheld remote action retains the original withdrawal and its historical timeout");
        expect(device->retirement.load() == driver::E1000Device::Retirement::withdrawal_pending && dma::stats(device->pci).regions == 0,
               "only actually stopped and released hardware enters CPU withdrawal retry");
        retry_ticket = REPORT.withdrawal_ticket;
        retry_mapping = REPORT.id;
        expect(REPORT.view_count == 2, "actual retained parent still owns both children");
        if (selected_case == "consumer_second_view_retry") {
            expect(REPORT.views[0].phase == mapping::Phase::complete && REPORT.views[0].withdrawal_ticket != 0 &&
                       !REPORT.views[0].timed_out_ever && REPORT.views[1].phase == mapping::Phase::withdrawal_pending &&
                       REPORT.views[1].withdrawal_ticket != 0 && REPORT.views[1].timed_out_ever,
                   "second-child timeout retains a real completed first child and the original pending second ticket");
            for (uint64_t offset = 0; offset < REPORT.views[0].bytes; offset += 4096)
                expect(mmio_host::translate(REPORT.views[0].virtual_base + offset) == UINT64_MAX,
                       "completed first-child leaves are actually absent before parent completion");
            maintenance_host::pause_remote_after_ipis.store(UINT_MAX);
            maintenance_host::auto_progress.store(true);
        } else {
            expect(REPORT.views[0].phase == mapping::Phase::withdrawal_pending && REPORT.views[0].timed_out_ever &&
                       REPORT.views[1].phase != mapping::Phase::complete,
                   "one pending first child keeps the actual parent noncomplete and its sibling retained");
        }
        for (size_t i = 0; i < REPORT.view_count; ++i) pending_withdrawal_tickets[i] = REPORT.views[i].withdrawal_ticket;
        stopped_effect_count = register_effects.size();
        return;
    }
    expect(RESULT == 0 && device->retirement.load() == driver::E1000Device::Retirement::released,
           "actual accepted finalizer completes real consumer retirement");
    expect(REPORT.phase == mapping::Phase::complete && REPORT.withdrawal_ticket != 0 && REPORT.admission_closed &&
               REPORT.normal_readers == 0 && !REPORT.terminal_active,
           "actual typed VA release follows terminal destruction and exact maintenance completion");
    expect_sparse_views(REPORT, false);
    for (size_t i = 0; i < REPORT.view_count; ++i)
        expect(REPORT.views[i].initial_ticket == initial_tickets[i], "actual retirement keeps every original child initial identity");
    expect(!device->irq_owned && !device->published && dma::stats(device->pci).regions == 0 && packets.empty(),
           "actual IRQ, registered endpoint, packet owners and DMA regions are drained");
    auto const COMMAND = pci::pci_config_read16(device->pci->bus, device->pci->slot, device->pci->function, pci::PCI_COMMAND);
    expect((COMMAND & 7U) == 0, "actual fixed PCI retirement turns IO/MEM/BME decode off");
    if (selected_case == "consumer_observed_profile") {
        auto const MSIX = pci::pci_config_read16(device->pci->bus, device->pci->slot, device->pci->function, 0xa2);
        expect((MSIX & 0xc000U) == 0x4000U && (MSIX & 0x7ffU) == 4,
               "actual terminal path masks/disables captured MSI-X without changing its five-entry geometry");
        expect((pci::pci_config_read16(device->pci->bus, device->pci->slot, device->pci->function, 0xd2) & 1U) == 0 &&
                   !mapping::detail::record_generic_physical_access(OBSERVED_PROFILE_BAR3, OBSERVED_PROFILE_BAR3_BYTES),
               "actual retirement disables captured MSI and retains the third physical tombstone after VA withdrawal");
    }
    if (selected_case == "consumer_held_callback") expect(held_released, "actual retire wait released the genuine callback before stop");
    if (selected_case == "consumer_withdrawal_retry" || selected_case == "consumer_second_view_retry") {
        expect(final_observations == 2 && REPORT.withdrawal_ticket == retry_ticket && REPORT.id == retry_mapping && REPORT.timed_out_ever,
               "actual finalizer retry resumes exactly the original mapping and ticket");
        expect(register_effects.size() == stopped_effect_count, "CPU-only retry never touches any BAR again");
        for (size_t i = 0; i < REPORT.view_count; ++i)
            expect(pending_withdrawal_tickets[i] == 0 || REPORT.views[i].withdrawal_ticket == pending_withdrawal_tickets[i],
                   "actual retry preserves every already-issued child withdrawal ticket");
    }
}

void exercise_normal_callbacks() {
    auto const BEFORE = register_effects.size();
    pci_cold_host::registers[driver::REG_ICR / 4] = driver::ICR_LSC | driver::ICR_RXT0;
    invoke_irq(device->irq_vector);
    expect(register_effects.size() > BEFORE && napi != nullptr && napi->has_work.load(),
           "actual IRQ callback uses the real borrowed view and schedules its external NAPI endpoint");
    expect(device->registers.report().normal_readers == 0, "actual IRQ epilogue drops its normal Borrow");
    expect(napi->poll(napi, 64, ker::net::NapiPollSource::WORKER) == 0 && device->registers.report().normal_readers == 0,
           "actual NAPI callback balances its normal view and reenables interrupts");
    auto* packet = e1000_host::packet();
    expect(packet != nullptr, "finite external TX packet allocated");
    packet->len = 64;
    expect(device->netdev.ops->start_xmit(&device->netdev, packet) == 0, "actual TX uses a real prepared DMA submission and normal view");
    expect(device->registers.report().normal_readers == 0, "actual TX unlocks then releases its normal view");
    device->tx_descs[0].status = driver::TX_STATUS_DD;
    {
        driver::CallbackGuard const guard(device);
        expect(static_cast<bool>(guard), "real completed-TX callback scope admitted");
        auto const FLAGS = device->tx_lock.lock_irqsave();
        driver::process_tx(device, guard.registers());
        device->tx_lock.unlock_irqrestore(FLAGS);
    }
    expect(device->tx_bufs[0] == nullptr, "actual completion consumes the DMA submission before packet release");
}

void preclaim_reuse_function(const controller_host::cm::detail::Controller&) {
    expect(!reuse_claim.has_value(), "second function has no preexisting test claim");
    pci::PCIDevice* second = nullptr;
    for (size_t i = 0; i < pci::pci_device_count(); ++i) {
        auto* candidate = pci::pci_get_device(i);
        if (candidate != nullptr && candidate->bus == 0 && candidate->slot == 5 && candidate->function == 0) second = candidate;
    }
    expect(second != nullptr && second->vendor_id == 0x8086 && second->device_id == 0x10d3 &&
               second->class_code == pci::PCI_CLASS_NETWORK && second->subclass_code == pci::PCI_SUBCLASS_ETHERNET,
           "second real enumerated function keeps its ordinary NIC identity");
    reuse_claim = pci::try_claim_driver(second, pci::DriverKind::e1000e);
    expect(reuse_claim.has_value(), "actual exclusive ledger grants the second function's retained claim before normal init");
}

auto reused_leaf_raw(uint64_t address) -> uint64_t {
    auto* table = mmio_host::root;
    for (unsigned level = 4; level != 0; --level) {
        expect(table != nullptr, "actual reused leaf has a real retained hierarchy");
        uint64_t const raw = mmio_host::raw(table, (address >> (12 + 9 * (level - 1))) & 511);
        expect((raw & 1U) != 0, "actual reused owner leaf remains present");
        if (level == 1) return raw;
        expect((raw & 128U) == 0, "actual reused child uses the retained four-level 4-KiB hierarchy");
        auto found = mmio_host::tables.find(raw & mmio_host::FRAME);
        expect(found != mmio_host::tables.end(), "actual reused hierarchy resolves only known finite table backing");
        table = found->second;
    }
    std::abort();
}

void observe_boot_va_reuse(const controller_host::cm::detail::Controller& controller) {
    expect(driver::device_count == 1 && reuse_claim.has_value(), "ordinary driver initializes only the unclaimed first function");
    auto* first = driver::devices[0];
    expect(first != nullptr && first->pci == pci_cold_host::selected_device() && first->typed_mmio && first->mmio == nullptr,
           "failed probe retains the genuine first driver and typed mapping");
    auto const old = first->registers.report();
    expect(first->retirement.load() == driver::E1000Device::Retirement::withdrawal_pending && !first->published && !first->irq_owned &&
               dma::stats(first->pci).regions == 0 && packets.empty() && old.phase == mapping::Phase::withdrawal_pending &&
               old.admission_closed && old.normal_readers == 0 && !old.terminal_active && old.view_count == 2,
           "real failed-probe hardware retirement leaves only a closed CPU-withdrawal continuation");
    expect(old.views[0].phase == mapping::Phase::complete && old.views[0].withdrawal_ticket != 0 && !old.views[0].timed_out_ever &&
               old.views[1].phase == mapping::Phase::withdrawal_pending && old.views[1].withdrawal_ticket != 0 &&
               old.views[1].timed_out_ever,
           "actual first child completed before second-child timeout and VA reuse");
    expect(mmio_host::translate(old.views[0].virtual_base) == UINT64_MAX,
           "old first-child leaf is genuinely absent before another function adopts its VA");
    auto const effects_before = register_effects.size();
    auto const reads_before = mmio_host::register_reads;
    auto const writes_before = mmio_host::register_writes;
    auto const allocations_before = mmio_host::dma_allocs;
    auto const frees_before = mmio_host::dma_frees;
    auto const irq_drains_before = irq_drains;
    maintenance_host::pause_remote_after_ipis.store(UINT_MAX);
    maintenance_host::auto_progress.store(true);
    mapping::Mapping replacement;
    std::array<mapping::ViewRequest, 1> const request{{{0, 0, 4096}}};
    expect(pci::map_cold_uc_82574(*reuse_claim, controller, request, replacement) == mapping::Status::ready,
           "same genuine BootController maps the separately claimed function through the actual PCI factory");
    auto const fresh = replacement.report();
    expect(fresh.phase == mapping::Phase::active && fresh.id != 0 && fresh.id != old.id && fresh.view_count == 1 &&
               fresh.views[0].bar == 0 && fresh.views[0].bar_offset == 0 && fresh.views[0].bytes == 4096 &&
               fresh.views[0].physical == REUSE_BAR0 && fresh.views[0].virtual_base == old.views[0].virtual_base &&
               reuse_claim->device() != first->driver_claim.device() && reuse_claim->epoch() != first->driver_claim.epoch(),
           "actual allocator reuses the completed child VA for a distinct genuine function and generation");
    expect(mmio_host::translate(fresh.views[0].virtual_base) == REUSE_BAR0, "actual reused PTE names the second function's physical BAR");
    auto const new_leaf = reused_leaf_raw(fresh.views[0].virtual_base);
    expect(register_effects.size() == effects_before && mmio_host::register_reads == reads_before &&
               mmio_host::register_writes == writes_before && mmio_host::dma_allocs == allocations_before &&
               mmio_host::dma_frees == frees_before && irq_drains == irq_drains_before,
           "second function preparation performs no register, DMA or IRQ hardware operation");
    auto const result = driver::retire_device(first, &controller);
    expect(result == 0 && first->retirement.load() == driver::E1000Device::Retirement::released &&
               replacement.report().phase == mapping::Phase::active && replacement.report().id == fresh.id &&
               mmio_host::translate(fresh.views[0].virtual_base) == REUSE_BAR0 && reused_leaf_raw(fresh.views[0].virtual_base) == new_leaf,
           "completed-child retry preserves the actual newer function mapping");
    auto const completed = first->registers.report();
    expect(completed.id == old.id && completed.phase == mapping::Phase::complete && completed.view_count == old.view_count &&
               completed.views[0].withdrawal_ticket == old.views[0].withdrawal_ticket &&
               completed.views[1].withdrawal_ticket == old.views[1].withdrawal_ticket,
           "actual old retry completes only its original generation and withdrawal tickets");
    for (uint64_t offset = 0; offset < completed.views[1].bytes; offset += 4096)
        expect(mmio_host::translate(completed.views[1].virtual_base + offset) == UINT64_MAX,
               "old second-child leaves remain withdrawn after the original ticket completes");
    expect(register_effects.size() == effects_before && mmio_host::register_reads == reads_before &&
               mmio_host::register_writes == writes_before && mmio_host::dma_allocs == allocations_before &&
               mmio_host::dma_frees == frees_before && irq_drains == irq_drains_before,
           "actual old retry never repeats hardware retirement or accesses the separately owned function");
    expect(!first->registers.borrow().valid() && !first->registers.terminal_borrow().valid(),
           "old reports cannot reopen access to a historical VA after generation change");
    {
        auto retained = replacement.borrow();
        expect(retained.valid(), "new actual affine Borrow retains its new generation without issuing an instruction");
    }
    replacement.close();
    expect(replacement.report().phase == mapping::Phase::closed && !replacement.borrow().valid() &&
               mmio_host::translate(fresh.views[0].virtual_base) == REUSE_BAR0 &&
               !mapping::detail::record_generic_physical_access(pci_cold_host::BAR0, 4096) &&
               !mapping::detail::record_generic_physical_access(REUSE_BAR0, 4096),
           "both genuine physical owners remain retained without inventing second hardware quiescence");
    reuse_claim.reset();
    reuse_complete = true;
    std::printf("MMIO_VA_REUSE_HOST old_mapping=%llu new_mapping=%llu va=%llx physical=%llx old_ticket0=%llu old_ticket1=%llu\n",
                static_cast<unsigned long long>(old.id), static_cast<unsigned long long>(fresh.id),
                static_cast<unsigned long long>(fresh.virtual_base), static_cast<unsigned long long>(fresh.physical),
                static_cast<unsigned long long>(old.views[0].withdrawal_ticket),
                static_cast<unsigned long long>(old.views[1].withdrawal_ticket));
}

// Shared runner first initializes actual boot/cache/MM/PCI sources (without
// calling an issuer). The retry case's immutable command line must also include
// --selftest-e1000-mmio-withdraw-retry before actual MM boot initialization.
void run_consumer_case(std::string_view name) {
    selected_case = name;
    bool known = false;
    for (auto candidate : consumer_cases) known |= candidate == name;
    expect(known, "known fresh-process consumer case");
    if (name == "consumer_probe_packet_failure") packet_budget = 3;
    if (name == "consumer_probe_irq_failure") irq_request_failed = true;
    if (name == "consumer_initial_bme_refusal")
        pci_cold_host::put(pci_cold_host::nic().config, pci::PCI_COMMAND, pci::PCI_COMMAND_BUS_MASTER, 2);
    // Nonzero autoload MAC avoids depending on a separate EEPROM device model.
    pci_cold_host::registers[driver::REG_RAL / 4] = 0x33221102;
    pci_cold_host::registers[driver::REG_RAH / 4] = 0x5544 | driver::RAH_AV;
    pci_cold_host::registers[driver::REG_STATUS / 4] = 2;
    if (name == "consumer_completed_va_reuse") {
        packet_budget = 3;
        controller_host::before_boot_callback = preclaim_reuse_function;
        auto const rounds_before_second_withdrawal = 2 * driver::REGISTER_VIEWS.size() + 1;
        maintenance_host::pause_remote_after_ipis.store(
            static_cast<unsigned>(rounds_before_second_withdrawal * runtime_ports::configured.load() + 1));
        expect(controller_host::with_actual_boot_controller(observe_boot_va_reuse),
               "actual canonical boot failure and CPU-only retry demonstrate another function's VA reuse");
        controller_host::before_boot_callback = nullptr;
        expect(reuse_complete && !reuse_claim.has_value() && final_observations == 0,
               "canonical reuse case retains its new owner and never invents a scheduled finalizer");
        return;
    }
    expect(controller_host::with_actual_boot_controller(observe_boot), "actual one-shot InitExecutor invokes the production consumer");
    if (name == "consumer_probe_packet_failure" || name == "consumer_probe_irq_failure" || name == "consumer_initial_bme_refusal") {
        expect(final_observations == 0, "failed probe did not invent an accepted finalizer");
        return;
    }
    exercise_normal_callbacks();
    if (name == "consumer_held_callback") {
        held_callback = std::make_unique<driver::CallbackGuard>(device);
        expect(static_cast<bool>(*held_callback) && device->registers.report().normal_readers == 1, "actual retained callback counted");
        yield_hook = release_held_callback;
    }
    if (name == "consumer_stop_failure") master_stuck = true;
    if (name == "consumer_mask_failure") interrupt_mask_stuck = true;
    if (name == "consumer_second_view_retry")
        maintenance_host::pause_remote_after_ipis.store(maintenance_host::ipis.load() +
                                                        static_cast<unsigned>(runtime_ports::configured.load()) + 1);
    ker::mod::sched::task::Task actor;
    actor.pid = 1;
    controller_host::initialize(actor);
    bool refused = false;
    try {
        expect(controller_host::with_actual_finalizer_controller(observe_retirement) == 0,
               "actual authorized power command reaches terminal action");
    } catch (const controller_host::Refused&) {
        refused = true;
    }
    // Power's existing terminal policy may proceed with explicitly retained
    // uncertain DMA resources. The observer above requires the real -EIO and
    // forbids a CPU-complete mapping; it never turns that into release proof.
    expect(!refused, "accepted finalizer follows its actual platform policy after the checked consumer result");
    expect(final_observations == (name == "consumer_withdrawal_retry" || name == "consumer_second_view_retry" ? 2 : 1),
           "bounded actual finalizer attempt count");
}
}  // namespace e1000_host
