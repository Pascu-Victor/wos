// Included after unchanged complete e1000 source and shared actual-engine setup.
// Each named case runs in a fresh child; no production registry reset is used.
#include <cstdio>
#include <cstdlib>
#include <memory>

namespace e1000_host {
namespace driver = ker::dev::e1000e;
namespace dma = ker::dev::dma;
namespace mapping = ker::mod::mm::mmio;
namespace pci = ker::dev::pci;
inline constexpr std::array<std::string_view, 10> consumer_cases{
    "consumer_success",          "consumer_held_callback",    "consumer_probe_packet_failure", "consumer_probe_irq_failure",
    "consumer_stop_failure",     "consumer_mask_failure",     "consumer_initial_bme_refusal",  "consumer_withdrawal_retry",
    "consumer_observed_profile", "consumer_second_view_retry"};
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
