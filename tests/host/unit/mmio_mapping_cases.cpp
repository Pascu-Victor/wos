// These finite declarations and actual source splices have semantic order.
// clang-format off
#include "mmio_mapping_ports.inc"
#include "e1000_consumer_generated.hpp"
#include "e1000_mmio_consumer_cases.inc"
#include "mmio_writer_generated.hpp"
#include <dev/pci_extended.hpp>
#include <optional>
// clang-format on

namespace mmio_host {
namespace mapping = ker::mod::mm::mmio;
namespace cache = ker::mod::mm::mmio_cache;
namespace cm = ker::mod::mm::cpu_maintenance;
namespace pci = ker::dev::pci;
inline size_t assertions{};
inline limine_memmap_entry reuse_memory_entry{};
inline limine_memmap_entry large_memory_entry{};
inline std::array<limine_memmap_entry*, 6> extra_memory_pointers{};
inline constexpr uint64_t LARGE_BAR = 1ULL << 37, LARGE_BYTES = 1ULL << 37;
inline constexpr uint64_t HHDM = 0xffff800000000000ULL, RAM = 0x400000, ORDINARY_VA = 0x40000000;
inline constexpr uint64_t ORDINARY_FLAGS = 3 | (1ULL << 63);
inline std::string_view selected_engine_case;

auto large_case(std::string_view name) -> bool { return name.starts_with("engine_large_") || name.starts_with("consumer_admission_"); }

void check(bool value, const char* reason) {
    ++assertions;
    if (value) return;
    std::fprintf(stderr, "actual MM engine check failed: %s\n", reason);
    std::abort();
}

void setup(std::string_view name) {
    selected_engine_case = name;
    pci_cold_host::setup();
    std::copy(pci_cold_host::memory_pointers.begin(), pci_cold_host::memory_pointers.end(), extra_memory_pointers.begin());
    size_t memory_count = pci_cold_host::memory_pointers.size();
    if (name == "consumer_completed_va_reuse" || name == "consumer_admission_deadline" || name == "engine_large_epoch_saturation") {
        // Initial finite hardware only: neither function has been enumerated,
        // claimed or accessed. The second function needs no MMIO register bank.
        auto& second = pci_cold_host::functions[5];
        second = pci_cold_host::nic();
        pci_cold_host::put(second.config, 0x10, e1000_host::REUSE_BAR0, 4);
        pci_cold_host::put(second.config, 0x14, e1000_host::REUSE_BAR1, 4);
        pci_cold_host::put(second.config, 0x18, 0xc101, 4);
        reuse_memory_entry = {e1000_host::REUSE_BAR0, 2 * pci_cold_host::BAR_BYTES, LIMINE_MEMMAP_RESERVED};
        extra_memory_pointers[memory_count++] = &reuse_memory_entry;
    }
    if (name == "consumer_observed_profile") {
        // Captured firmware/chipset/capability bytes, with explicit finite BDF,
        // RAM and BAR geometry. Actual issuers/claim/MM/DMA paths stay shared.
        pci_cold_host::use_captured_profile();
        pci_cold_host::put(pci_cold_host::nic().config, pci::PCI_COMMAND, 3, 2);
        pci_cold_host::put(pci_cold_host::nic().config, 0x1c, e1000_host::OBSERVED_PROFILE_BAR3, 4);
        pci_cold_host::nic().sizing_masks[3] = 0xffffc000;
        pci_cold_host::memory_entries[1].length += e1000_host::OBSERVED_PROFILE_BAR3_BYTES;
    }
    if (name == "engine_bad_table_alias") table_alias_flags |= 4;  // Real USER leaf; ancestors alone cannot justify private storage.
    if (name == "engine_budget_refusal") {
        pci_cold_host::put(pci_cold_host::nic().config, 0x10, 0x84000000, 4);
        pci_cold_host::nic().sizing_masks[0] = 0xfe000000;
        large_memory_entry = {0x84000000, 32 * 1024 * 1024, LIMINE_MEMMAP_RESERVED};
        extra_memory_pointers[memory_count++] = &large_memory_entry;
    }
    if (large_case(name)) {
        // A finite 64-bit prefetchable resource: the high word and the entire
        // 128-GiB extent are real PCI geometry, never host-buffer allocation.
        auto& nic = pci_cold_host::nic();
        pci_cold_host::put(nic.config, 0x14, 0xc, 4);
        pci_cold_host::put(nic.config, 0x18, LARGE_BAR >> 32, 4);
        pci_cold_host::put(nic.config, 0x1c, 0xc001, 4);
        nic.sizing_masks[1] = 0xc;
        nic.sizing_masks[2] = 0xffffffe0;
        nic.sizing_masks[3] = 0xffffffe1;
        large_memory_entry = {LARGE_BAR, LARGE_BYTES, LIMINE_MEMMAP_RESERVED};
        extra_memory_pointers[memory_count++] = &large_memory_entry;
    }
    pci_cold_host::memory_map.entry_count = memory_count;
    pci_cold_host::memory_map.entries = extra_memory_pointers.data();
    maintenance_host::setup(4);
    for (size_t cpu = 0; cpu < 4; ++cpu) {
        auto& input = runtime_ports::cpus[cpu];
        input.apic = static_cast<uint32_t>(cpu);
        input.cpuid[{1, 0}].ebx = static_cast<uint32_t>(cpu << 24);
        if (large_case(name)) input.cpuid[{0x80000008U, 0}].eax = 48;
    }
    runtime_ports::select(0);
    check(cm::detail::initialize_boot(0x70) == cm::Status::ready, "actual permanent maintenance service initializes");
    check(ker::mod::mm::cache::configure_boot_cpu_count(4), "actual inherited PAT registry configures");
    check(cache::configure_boot_cohort(4) == cache::Status::ready, "actual immutable CPU registry configures");
    for (size_t cpu = 0; cpu < 4; ++cpu) {
        runtime_ports::select(cpu);
        check(ker::mod::mm::cache::capture_boot_cpu(cpu), "actual inherited PAT observation captures the finite CPU register");
        check(cache::capture_boot_cpu(cpu) == cache::Status::ready, "actual CPU report captures finite hardware inputs");
        check(cm::detail::note_current_cpu_ready() == cm::Status::ready, "actual service admits each captured CPU");
    }
    runtime_ports::select(0, 2);
    root = allocate_table();
    check(root != nullptr, "finite kernel root backing allocated");
    mmio_writer_host::kernel_pagemap = root;
    if (name == "consumer_withdrawal_retry") command_line += " --selftest-e1000-mmio-withdraw-retry";
    if (name == "consumer_admission_steps" || name == "consumer_admission_deadline")
        command_line += " --selftest-e1000-mmio --selftest-e1000-mmio-admission-steps";
    mapping::detail::begin_boot_history();
    mmio_writer_host::typed_mmio_guards.store(mapping::enabled());
    check(mapping::enabled(), "immutable real boot selection enabled");
    int const EXPECTED_FUNCTIONS =
        name == "consumer_completed_va_reuse" || name == "consumer_admission_deadline" || name == "engine_large_epoch_saturation" ? 4 : 3;
    check(pci::pci_enumerate_all() == EXPECTED_FUNCTIONS && pci::pci_enumeration_complete(),
          "actual PCI enumeration finds the complete finite topology");
    check(pci::extended_inventory().state == pci::ExtendedState::ready, "actual ECAM collector copies real finite configuration");
    pci::initialize_cold_mmio_inventory();
    if (name == "engine_bad_table_alias")
        check(raw(root, 505) == 0 && reloads == 0 && releases == 1, "actual unsafe HHDM leaf cannot register the permanent private bank");
    else
        check(raw(root, 505) != 0 && reloads == 1, "actual PCI finalization settles the genuine shared kernel arena");
    setup_allocs = allocs;
    building = false;
}

void verify_global_ports() {
    check(runtime_ports::violations.load() == 0, "finite CPU hardware ports saw only correctly excluded actions");
    check(runtime_ports::irq_saves.load() == runtime_ports::irq_restores.load(), "all actual CPU capture/maintenance IRQ scopes restore");
    check(pin_depth == 0 && pci_cold_host::config_depth == 0 && lock_stack.empty(), "all actual engine lock scopes balance");
    check(pci_cold_host::bad_probe_decodes == 0, "actual multi-BAR probe always holds IO/MEM/BME disabled");
}

void fences_after_boot(const controller_host::cm::detail::Controller& controller) {
    e1000_host::observe_boot(controller);
    auto* device = e1000_host::device;
    auto const report = device->registers.report();
    check(report.physical == pci_cold_host::BAR0 && report.bytes == 0x1000 && report.physical_spans == 2 && report.view_count == 2,
          "actual private PCI capability accounts both decoded physical BARs");
    for (size_t i = 0; i < report.view_count; ++i)
        for (uint64_t offset = 0; offset < report.views[i].bytes; offset += 4096)
            check(translate(report.views[i].virtual_base + offset) == report.views[i].physical + offset,
                  "actual typed page tables cover every requested sparse child page");
    {
        auto original = device->registers.borrow();
        check(original.valid() && device->registers.report().normal_readers == 1, "actual affine normal borrow increments its real gate");
        mapping::Borrow moved(std::move(original));
        check(!original.valid() && moved.valid() && device->registers.report().normal_readers == 1,
              "actual move transfers one counted view without duplication");
        device->registers.close();
        check(moved.valid() && moved.read32(e1000_host::driver::REG_STATUS) != UINT32_MAX,
              "existing genuine view survives actual gate closure");
        check(moved.read32(1, 0x5b00 - 0x2000) != UINT32_MAX,
              "the same genuine whole-function Borrow also retains its second child after closure");
        check(!device->registers.borrow().valid() && !device->registers.terminal_borrow().valid(),
              "closed admission refuses new normals and a held normal refuses terminal access");
        moved.reset();
        check(!moved.valid() && device->registers.borrows_drained(), "actual normal reset releases its final counted reference");
        auto terminal = device->registers.terminal_borrow();
        check(terminal.valid() && !device->registers.terminal_borrow().valid(), "actual terminal view is unique after closed-and-drained");
        check(terminal.read32(e1000_host::driver::REG_STATUS) != UINT32_MAX, "real terminal view accesses the original live hardware bank");
        check(terminal.read32(1, 0x5b00 - 0x2000) != UINT32_MAX,
              "one unique terminal Borrow grants actual access to both drained child views");
    }
    check(!device->registers.report().terminal_active, "terminal destruction precedes the genuine hardware lifecycle");
    check(!mapping::detail::generic_virtual_allowed(root, report.virtual_base, 4096), "generic virtual mutator cannot enter private arena");
    check(!mapping::detail::generic_virtual_allowed(root, report.views[1].virtual_base, report.views[1].bytes),
          "the second sparse child has the same actual generic-writer exclusion");
    check(!mapping::detail::record_generic_mapping(root, 0x400000, report.physical, 4096, 3),
          "actual physical tombstone blocks a generic incoming alias");
    auto* private_table = tables.at(raw(root, 505) & FRAME);
    check(!mapping::detail::generic_table_allowed(private_table), "private page-table frames cannot become generic destinations");
    check(!mapping::detail::generic_table_allowed(reinterpret_cast<Table*>(report.virtual_base)),
          "typed device VA cannot masquerade as an arithmetically plausible HHDM page-table pointer");
    {
        auto flags = mapping::detail::platform::lock_pin();
        check(!mapping::detail::generic_root_allowed_locked(private_table), "private table cannot be presented as an ordinary root");
        check(!mapping::detail::generic_leaf_allowed_locked(root, 0x400000, 0, report.physical | 3, 1),
              "same-PIN incoming leaf check rejects an actual owned BAR frame");
        check(!mapping::detail::generic_leaf_allowed_locked(root, 0x400000, 0, (report.physical & ~0x1fffffULL) | 0x1083, 2),
              "large-leaf PAT encoding cannot hide physical overlap");
        mapping::detail::platform::unlock_pin(flags);
    }
    check(!mapping::detail::record_generic_mapping(root, 0x500000, report.physical | (1ULL << 52), 4096, 3),
          "high-bit truncation cannot bypass ownership");
    check(!mapping::detail::record_generic_physical_access(report.physical, 4096),
          "off-lock physical backing readers cannot adopt the claimed BAR");
    check(!mapping::detail::record_generic_physical_access(pci_cold_host::BAR1, 4096),
          "unmapped decoded companion BAR remains in the same actual physical tombstone set");
}

void fences_after_retirement(const controller_host::cm::detail::Controller& controller) {
    e1000_host::observe_retirement(controller);
    auto const report = e1000_host::device->registers.report();
    for (size_t i = 0; i < report.view_count; ++i)
        for (uint64_t offset = 0; offset < report.views[i].bytes; offset += 4096)
            check(translate(report.views[i].virtual_base + offset) == UINT64_MAX, "actual withdrawal clears every original child leaf");
    check(!mapping::detail::record_generic_mapping(root, 0x400000, report.physical, 4096, 3),
          "exact ACK permits VA withdrawal but never returns the physical tombstone");
    check(!e1000_host::device->registers.borrow().valid() && !e1000_host::device->registers.terminal_borrow().valid(),
          "completed withdrawal cannot issue another hardware view");
    check(releases == 0 && allocs >= 4, "published private page-table storage remains pinned after exact ACK");
}

void run_fences() {
    e1000_host::selected_case = "consumer_success";
    check(controller_host::with_actual_boot_controller(fences_after_boot), "actual boot issuer runs consumer and fence observations");
    ker::mod::sched::task::Task actor;
    actor.pid = 1;
    controller_host::initialize(actor);
    check(controller_host::with_actual_finalizer_controller(fences_after_retirement) == 0,
          "actual finalizer runs genuine quiescence withdrawal");
}

void run_budget_refusal() {
    controller_host::before_boot_callback = [](const controller_host::cm::detail::Controller& controller) {
        auto claim = pci::try_claim_driver(pci_cold_host::selected_device(), pci::DriverKind::e1000e);
        check(claim.has_value(), "oversized materialization test uses an actual exclusive enumerated claim");
        mapping::Mapping output;
        constexpr mapping::ViewRequest REQUEST{0, 0, 32 * 1024 * 1024};
        auto const status = pci::map_cold_uc_82574(*claim, controller, std::span(&REQUEST, 1), output);
        check(status == mapping::Status::capacity && output.empty(), "oversized materialized view reports actual capacity");
        check(allocs == setup_allocs && register_reads == 0 && register_writes == 0 && dma_allocs == 0 &&
                  maintenance_host::events().empty() && pci_cold_host::mem_enable_writes == 0,
              "materialized-byte capacity refuses before tables, CPU work, decode, registers or DMA");
        check(mapping::detail::record_generic_physical_access(0x84000000, 4096),
              "pre-reservation materialized capacity refusal does not claim physical frames");
        std::printf("MMIO_VIEW_BUDGET_REFUSAL status=capacity bytes=33554432\n");
    };
    check(controller_host::with_actual_boot_controller(nullptr), "actual boot issuer reaches real materialized-view capacity refusal");
    controller_host::before_boot_callback = nullptr;
    check(e1000_host::driver::device_count == 0, "failed direct producer claim stays retained before ordinary driver dispatch");
}

void run_bad_table_alias() {
    check(controller_host::with_actual_boot_controller([](const controller_host::cm::detail::Controller&) {
              check(e1000_host::driver::device_count == 1, "actual malformed-alias probe retains its driver metadata");
              auto* device = e1000_host::driver::devices[0];
              check(device->typed_mmio && device->mmio == nullptr && device->registers.empty() &&
                        device->retirement.load() == e1000_host::driver::E1000Device::Retirement::cold_failed,
                    "malformed real HHDM permission never publishes typed mapping or raw fallback");
              check(register_reads == 0 && register_writes == 0 && dma_allocs == 0 && maintenance_host::events().empty(),
                    "unsafe private-table alias refuses before MMIO, DMA or any CPU-maintenance action");
          }),
          "actual issuer reaches selected refusal after unsafe real HHDM leaf");
}

template <typename Body>
void register_refuses_before_effects(Body body) {
    int pipefd[2];
    check(pipe(pipefd) == 0, "finite register effect observation pipe opens");
    auto child = fork();
    check(child >= 0, "actual register bounds refusal executes in a fresh child");
    if (child == 0) {
        close(pipefd[0]);
        register_effect_fd = pipefd[1];
        body();
        _exit(0);
    }
    close(pipefd[1]);
    int status = 0;
    check(waitpid(child, &status, 0) == child, "exact register-refusal child is joined");
    char effect = 0;
    auto const count = read(pipefd[0], &effect, 1);
    close(pipefd[0]);
    check(WIFSIGNALED(status) && WTERMSIG(status) == SIGILL, "actual view bounds trap before device instructions");
    check(count == 0, "invalid child, byte range or driver hole cannot enter a register instruction port");
}

void bounds_after_boot(const controller_host::cm::detail::Controller& controller) {
    e1000_host::observe_boot(controller);
    auto* device = e1000_host::device;
    auto const report = device->registers.report();
    auto borrow = device->registers.borrow();
    check(borrow.valid() && report.view_count == 2, "real function Borrow retains both actual sparse children");
    auto const reads_before = register_reads;
    // These boundary words are finite host bank input, not extra native NIC
    // register accesses or a claim that reserved hardware registers are safe.
    (void)borrow.read32(0, report.views[0].bytes - 4);
    (void)borrow.read32(1, report.views[1].bytes - 4);
    check(register_reads == reads_before + 2, "last complete finite word of each actual child reaches exactly one instruction");
    register_refuses_before_effects([&] { (void)borrow.read32(report.view_count, 0); });
    register_refuses_before_effects([&] { (void)borrow.read32(SIZE_MAX, 0); });
    register_refuses_before_effects([&] { (void)borrow.read32(0, report.views[0].bytes); });
    register_refuses_before_effects([&] { (void)borrow.read32(1, report.views[1].bytes); });
    register_refuses_before_effects([&] { (void)borrow.read32(0, 0x2000); });
    register_refuses_before_effects([&] { (void)borrow.read32(1, 1); });
    register_refuses_before_effects([&] { (void)borrow.read32(1, SIZE_MAX - 3); });
    register_refuses_before_effects([&] { borrow.write32(report.view_count, 0, 0); });
    register_refuses_before_effects([&] { borrow.write32(1, report.views[1].bytes, 0); });
    register_refuses_before_effects([&] { (void)e1000_host::driver::reg_read(device, borrow, 0x1000); });
    register_refuses_before_effects([&] { e1000_host::driver::reg_write(device, borrow, 0x6000, 0); });
    check(device->registers.report().normal_readers == 1, "rejected forked calls do not disturb the real parent Borrow count");
    borrow.reset();
    register_refuses_before_effects([&] { (void)borrow.read32(1, 0); });
    device->registers.close();
    check(device->registers.borrows_drained(), "actual Borrow reset drains the complete function set");
}

void run_view_bounds() {
    e1000_host::selected_case = "consumer_success";
    check(controller_host::with_actual_boot_controller(bounds_after_boot), "actual boot producer supplies real view-boundary targets");
    ker::mod::sched::task::Task actor;
    actor.pid = 1;
    controller_host::initialize(actor);
    check(controller_host::with_actual_finalizer_controller(e1000_host::observe_retirement) == 0,
          "actual hardware witness still retires every child after bounds refusals");
}

inline bool producer_case_observed{};

auto producer_claim() -> std::optional<pci::DriverClaim> {
    auto* device = pci::pci_find_by_class(pci::PCI_CLASS_NETWORK, pci::PCI_SUBCLASS_ETHERNET);
    check(device == pci_cold_host::selected_device(), "direct producer test uses the genuine enumerated function");
    auto claim = pci::try_claim_driver(device, pci::DriverKind::e1000e);
    check(claim.has_value(), "actual exclusive driver ledger grants the test caller's claim");
    return claim;
}

void producer_invalid_plans(const controller_host::cm::detail::Controller& controller) {
    auto claim = producer_claim();
    mapping::Mapping output;
    auto reject = [&](std::span<const mapping::ViewRequest> requests, mapping::Status expected) {
        auto const allocations_before = allocs;
        auto const actions_before = maintenance_host::events().size();
        auto const status = pci::map_cold_uc_82574(*claim, controller, requests, output);
        check(status == expected && output.empty(), "actual PCI producer rejects invalid view geometry without a public owner");
        check(allocs == allocations_before && maintenance_host::events().size() == actions_before && register_reads == 0 &&
                  register_writes == 0 && dma_allocs == 0 && pci_cold_host::mem_enable_writes == 0,
              "invalid view plans cannot allocate tables, begin CPU work, activate decode or access hardware");
    };
    for (auto request : {mapping::ViewRequest{6, 0, 4096}, mapping::ViewRequest{255, 0, 4096}, mapping::ViewRequest{2, 0, 4096},
                         mapping::ViewRequest{3, 0, 4096}, mapping::ViewRequest{0, 0, 0}, mapping::ViewRequest{0, 1, 4096},
                         mapping::ViewRequest{0, 0, 1}, mapping::ViewRequest{0, pci_cold_host::BAR_BYTES, 4096},
                         mapping::ViewRequest{0, UINT64_MAX - 4095, 4096}, mapping::ViewRequest{0, 0, UINT64_MAX - 4095}}) {
        reject(std::span<const mapping::ViewRequest>(&request, 1), mapping::Status::invalid_claim);
    }
    std::array<mapping::ViewRequest, 2> overlap{{{0, 0, 4096}, {0, 0, 4096}}};
    reject(overlap, mapping::Status::invalid_claim);
    std::array<mapping::ViewRequest, mapping::VIEW_CAPACITY + 1> too_many{};
    for (auto& request : too_many) request = {0, 0, 4096};
    reject(too_many, mapping::Status::capacity);
    check(mapping::detail::record_generic_physical_access(pci_cold_host::BAR1, 4096),
          "invalid plans produced no parent physical reservation before a later valid request");
    // The explicit history observation above makes this separate companion
    // unavailable to a real cold owner. Do not follow it with a fabricated
    // successful reservation; the compatibility case runs in its own process.
    producer_case_observed = true;
}

void producer_legacy_default(const controller_host::cm::detail::Controller& controller) {
    auto claim = producer_claim();
    mapping::Mapping output;
    check(pci::map_cold_uc_82574(*claim, controller, output) == mapping::Status::ready,
          "actual three-argument compatibility path still maps the complete original BAR0");
    auto const report = output.report();
    check(report.view_count == 1 && report.views[0].bar == 0 && report.views[0].bar_offset == 0 &&
              report.bytes == pci_cold_host::BAR_BYTES && report.views[0].bytes == pci_cold_host::BAR_BYTES,
          "compatibility report preserves full-BAR geometry through the new retained function engine");
    for (uint64_t offset = 0; offset < report.bytes; offset += 4096)
        check(translate(report.virtual_base + offset) == pci_cold_host::BAR0 + offset,
              "actual compatibility hierarchy materializes every original BAR page");
    output.close();
    check(output.report().phase == mapping::Phase::closed && !output.borrow().valid() &&
              !mapping::detail::record_generic_physical_access(pci_cold_host::BAR0, pci_cold_host::BAR_BYTES),
          "closing a genuine compatibility owner retains its physical exclusion and all backing");
    producer_case_observed = true;
}

void producer_cross_bar(const controller_host::cm::detail::Controller& controller) {
    auto claim = producer_claim();
    mapping::Mapping output;
    std::array<mapping::ViewRequest, 2> requests{{{0, 0, 4096}, {1, 0, 4096}}};
    check(pci::map_cold_uc_82574(*claim, controller, requests, output) == mapping::Status::ready,
          "actual PCI capability can select two independently bounded memory BARs");
    auto const report = output.report();
    check(report.view_count == 2 && report.views[0].bar == 0 && report.views[1].bar == 1 &&
              report.views[0].physical == pci_cold_host::BAR0 && report.views[1].physical == pci_cold_host::BAR1,
          "each real child retains its own actual BAR index and physical base");
    for (size_t i = 0; i < report.view_count; ++i)
        check(translate(report.views[i].virtual_base) == report.views[i].physical,
              "real private page tables select the actual requested BAR rather than scalar BAR0");
    check(register_reads == 0 && register_writes == 0 && dma_allocs == 0,
          "cross-BAR geometry test performs no unused native or finite device-register access");
    output.close();
    check(!output.borrow().valid() && !mapping::detail::record_generic_physical_access(pci_cold_host::BAR1, 4096),
          "closing the genuine function owner retains every companion resource");
    producer_case_observed = true;
}

void producer_prepare_resume(const controller_host::cm::detail::Controller& controller) {
    auto claim = producer_claim();
    mapping::Mapping output;
    auto const requests = e1000_host::driver::REGISTER_VIEWS;
    maintenance_host::pause_remote_after_ipis.store(2 * static_cast<unsigned>(runtime_ports::configured.load()) + 1);
    auto const status = pci::map_cold_uc_82574(*claim, controller, requests, output);
    check(status == mapping::Status::pending_retained && output.empty(),
          "real second-child CPU timeout retains private preparation without a public mapping");
    auto const first_actions = maintenance_host::events();
    check(first_actions.size() == 9 && pci_cold_host::mem_enable_writes == 0 && register_reads == 0 && register_writes == 0 &&
              dma_allocs == 0,
          "first child completes its eight actions while second remote work remains pending before function activation");
    check(translate(BANK + 4096) == pci_cold_host::BAR0 && translate(BANK + 4 * 4096) == UINT64_MAX,
          "first prepared child has actual private leaves while incomplete sibling remains unpublished");
    check(!mapping::detail::record_generic_physical_access(pci_cold_host::BAR0 + 0x6000, 4096),
          "partial preparation already retains the whole parent including its unmapped tail");
    auto changed = requests;
    changed[1].offset += 4096;
    auto const allocations_before = allocs;
    auto const probes_before = pci_cold_host::bar_probes;
    auto const mismatch = pci::map_cold_uc_82574(*claim, controller, changed, output);
    check(mismatch == mapping::Status::invalid_claim && output.empty() && allocs == allocations_before &&
              pci_cold_host::bar_probes == probes_before && maintenance_host::events().size() == first_actions.size(),
          "changed resume plan cannot re-probe, replace child geometry or advance the original CPU work");
    maintenance_host::pause_remote_after_ipis.store(UINT_MAX);
    maintenance_host::auto_progress.store(true);
    check(pci::map_cold_uc_82574(*claim, controller, requests, output) == mapping::Status::ready,
          "same real plan resumes retained preparation through actual pending CPU work");
    auto const report = output.report();
    check(report.view_count == 2 && report.views[0].initial_ticket == 1 && report.views[1].initial_ticket == 2 &&
              report.views[1].timed_out_ever && report.phase == mapping::Phase::active && maintenance_host::events().size() == 16 &&
              pci_cold_host::bar_probes == probes_before && pci_cold_host::mem_enable_writes == 1,
          "resumption preserves both original child tickets, skips completed first work and activates the function exactly once");
    output.close();
    check(!output.borrow().valid() && output.report().phase == mapping::Phase::closed,
          "API-only test closes and retains its genuine owner without manufacturing a hardware witness");
    producer_case_observed = true;
}

void run_producer_case(std::string_view name) {
    e1000_host::selected_case = name;
    controller_host::before_boot_callback = name == "engine_invalid_view_plans"    ? producer_invalid_plans
                                            : name == "engine_view_prepare_resume" ? producer_prepare_resume
                                            : name == "engine_cross_bar_views"     ? producer_cross_bar
                                                                                   : producer_legacy_default;
    check(controller_host::with_actual_boot_controller(nullptr), "real phase5 issuer dispatches the actual public producer test call");
    controller_host::before_boot_callback = nullptr;
    check(producer_case_observed && e1000_host::driver::device_count == 0,
          "exclusive actual test claim prevents the later ordinary driver from adopting the same function");
    check(register_reads == 0 && register_writes == 0 && dma_allocs == 0,
          "direct preparation tests claim no hardware access, DMA or actual driver-retirement proof");
}

void leave_empty_hhdm_leaf(uint64_t physical) {
    mmio_writer_host::map_page(root, HHDM + physical, RAM, ORDINARY_FLAGS);
    check(translate(HHDM + physical) == RAM, "actual generic writer first installs a different ordinary RAM frame");
    mmio_writer_host::unmap_page(root, HHDM + physical);
    check(translate(HHDM + physical) == UINT64_MAX && mmio_writer_host::cow_depth == 0,
          "actual COW then PIN unmap clears the RAM leaf while retaining ordinary page tables");
}

void fragment_large_resource() {
    auto const before = allocs;
    leave_empty_hhdm_leaf(pci_cold_host::BAR0);
    for (uint64_t i = 0; i < 65; ++i) leave_empty_hhdm_leaf(LARGE_BAR + i * 2 * 1024 * 1024);
    check(allocs - before < 80 && tables.size() * 4096 < 1024 * 1024,
          "over 32768 actual empty leaf walks need bounded sub-MiB table backing, never a resource-sized buffer");
}

auto resume_large(pci::DriverClaim& claim, const controller_host::cm::detail::Controller& controller,
                  std::span<const mapping::ViewRequest> requests, mapping::Mapping& output, size_t& calls) -> mapping::Status {
    auto status = mapping::Status::pending_retained;
    for (; status == mapping::Status::pending_retained && calls < 1024; ++calls)
        status = pci::map_cold_uc_82574(claim, controller, requests, output);
    return status;
}

void producer_large(const controller_host::cm::detail::Controller& controller) {
    auto const name = selected_engine_case;
    auto claim = producer_claim();
    if (name == "engine_large_epoch_saturation") e1000_host::preclaim_reuse_function(controller);
    mapping::Mapping output;
    std::array<mapping::ViewRequest, 2> const requests{{{0, 0, 4096}, {1, LARGE_BYTES - 4096, 4096}}};
    bool const fragmented = name != "engine_large_sparse" && name != "engine_large_epoch_saturation";
    if (fragmented) fragment_large_resource();
    if (name == "engine_large_epoch_saturation") {
        mmio_writer_host::map_page(root, ORDINARY_VA, RAM, ORDINARY_FLAGS);
        mmio_writer_host::unmap_page(root, ORDINARY_VA);
    }
    auto const before = allocs;
    // This is an actual writer preflight, deliberately before the admission
    // cursor. Its ordinary PA history cannot stand in for a later HHVA store.
    if (name == "engine_large_late_alias")
        mmio_writer_host::require_ordinary_mapping(root, HHDM + pci_cold_host::BAR0, RAM, 4096, ORDINARY_FLAGS);
    auto status = pci::map_cold_uc_82574(*claim, controller, requests, output);
    size_t calls = 1;
    check(status == mapping::Status::pending_retained && output.empty(),
          "actual full-resource walk yields a private physical-pending record and no public Mapping");
    check(allocs == before && maintenance_host::events().empty() && pci_cold_host::mem_enable_writes == 0 &&
              translate(BANK + 4096) == UINT64_MAX && register_reads == 0 && register_writes == 0 && dma_allocs == 0,
          "physical cursor does not assign VA, create child tables, submit CPU work or activate the function");
    if (name == "engine_large_late_alias" || name == "engine_large_transient_alias") {
        auto const topology_before = allocs;
        mmio_writer_host::map_page(root, HHDM + pci_cold_host::BAR0, RAM, ORDINARY_FLAGS);
        check(allocs == topology_before && translate(HHDM + pci_cold_host::BAR0) == RAM,
              "late HHVA store uses the preexisting ordinary L1 and a different RAM PA without topology changes");
        if (name == "engine_large_transient_alias") mmio_writer_host::unmap_page(root, HHDM + pci_cold_host::BAR0);
    } else if (name == "engine_large_device_history" || name == "engine_large_transient_device") {
        mmio_writer_host::map_page(root, ORDINARY_VA, LARGE_BAR + LARGE_BYTES - 4096, ORDINARY_FLAGS);
        check(translate(ORDINARY_VA) == LARGE_BAR + LARGE_BYTES - 4096,
              "pending admission has no provisional physical tombstone that would hide an ordinary writer effect");
        if (name == "engine_large_transient_device") mmio_writer_host::unmap_page(root, ORDINARY_VA);
    } else if (name == "engine_large_history_overflow") {
        for (uint64_t i = 0; i < 4200; ++i)
            check(mapping::detail::record_generic_physical_access(0x100000000ULL + i * 8192, 4096),
                  "real public ordinary physical history endpoint receives disjoint nondevice accesses");
    } else if (name == "engine_large_root_drift") {
        root = allocate_table();
        check(root != nullptr, "finite kernel-root input changes to another genuine allocated hierarchy");
        mmio_writer_host::kernel_pagemap = root;
    } else if (name == "engine_large_epoch_saturation") {
        // This case is selected only by the manifest-labelled compile-copy
        // saturation limit128 profile. No private epoch value is assigned.
        for (unsigned i = 0; i < 130; ++i) {
            mmio_writer_host::map_page(root, ORDINARY_VA, RAM, ORDINARY_FLAGS);
            mmio_writer_host::unmap_page(root, ORDINARY_VA);
        }
    }
    status = resume_large(*claim, controller, requests, output, calls);
    auto const expected = name == "engine_large_late_alias" ? mapping::Status::alias_present
                          : name == "engine_large_device_history" || name == "engine_large_transient_device"
                              ? mapping::Status::history_exposed
                          : name == "engine_large_history_overflow" ? mapping::Status::history_incomplete
                          : name == "engine_large_root_drift"       ? mapping::Status::bad_hierarchy
                          : name == "engine_large_epoch_saturation" ? mapping::Status::generation_exhausted
                                                                    : mapping::Status::ready;
    check(status == expected, "late actual HHVA effect invalidates previously scanned full-resource absence");
    if (expected != mapping::Status::ready) {
        check(output.empty() && maintenance_host::events().empty() && pci_cold_host::mem_enable_writes == 0 && register_reads == 0 &&
                  register_writes == 0 && dma_allocs == 0,
              "late root/history/alias rejection leaves no public owner, CPU ticket, decode or hardware exposure");
        if (name == "engine_large_late_alias")
            check(translate(HHDM + pci_cold_host::BAR0) == RAM,
                  "rejected full-resource admission preserves the real later ordinary RAM mapping");
        if (name == "engine_large_epoch_saturation") {
            mmio_writer_host::map_page(root, ORDINARY_VA, RAM, ORDINARY_FLAGS);
            check(translate(ORDINARY_VA) == RAM, "saturated admission epoch leaves ordinary generic RAM mapping usable");
            constexpr mapping::ViewRequest REQUEST{0, 0, 4096};
            check(pci::map_cold_uc_82574(*e1000_host::reuse_claim, controller, std::span(&REQUEST, 1), output) ==
                          mapping::Status::generation_exhausted &&
                      output.empty(),
                  "actual epoch saturation is sticky and cannot wrap into an old absence certificate");
            mmio_writer_host::unmap_page(root, ORDINARY_VA);
            e1000_host::reuse_claim.reset();
            std::printf("MMIO_EPOCH_SATURATION_HOST finite_limit=128 sticky=1 ordinary_ram_usable=1\n");
        }
    } else {
        auto const report = output.report();
        check(report.phase == mapping::Phase::active && report.physical_spans == 2 && report.view_count == 2 && report.bytes == 4096 &&
                  report.views[1].bar == 1 && report.views[1].bar_offset == LARGE_BYTES - 4096 &&
                  report.views[1].physical == LARGE_BAR + LARGE_BYTES - 4096 &&
                  translate(report.views[1].virtual_base) == LARGE_BAR + LARGE_BYTES - 4096,
              "actual high-word 128-GiB resource permits tiny exact child mappings with correct physical final-page geometry");
        check(report.resource_steps == calls && report.resource_root_frame == physical.at(root) && report.resource_epoch != 0 &&
                  report.views[0].initial_ticket == 1 && report.views[1].initial_ticket == 2 && maintenance_host::events().size() == 16 &&
                  pci_cold_host::mem_enable_writes == 1,
              "physical continuation preserves one claim, one activation and only the two original real CPU tickets");
        if (fragmented)
            check(report.resource_visits > 32768 && report.resource_steps > 256,
                  "actual fragmented HHDM cursor traverses more than the old 32768-walk ceiling across bounded resumptions");
        if (name == "engine_large_transient_alias")
            check(report.resource_restarts >= 1 && translate(HHDM + pci_cold_host::BAR0) == UINT64_MAX,
                  "transient actual HHVA RAM publication restarts all spans and proves fresh absence before commit");
        for (uint64_t offset : {uint64_t{0}, uint64_t{1} << 32, LARGE_BYTES / 2, LARGE_BYTES - 4096})
            check(!mapping::detail::record_generic_physical_access(LARGE_BAR + offset, 4096),
                  "full high-word resource including unmapped interior and tail remains owned after tiny publication");
        // Exclusion covers the owned PA, not every unrelated page at an HHVA.
        mmio_writer_host::map_page(root, HHDM + LARGE_BAR + 4096, RAM, ORDINARY_FLAGS);
        check(translate(HHDM + LARGE_BAR + 4096) == RAM,
              "committed physical ownership still permits an unrelated RAM PA at an ordinary HHDM virtual address");
        mmio_writer_host::unmap_page(root, HHDM + LARGE_BAR + 4096);
        output.close();
        check(!output.borrow().valid(), "API-only large owner closes and remains retained without a fabricated retirement witness");
        std::printf("MMIO_LARGE_RESOURCE_HOST steps=%llu visits=%llu restarts=%llu epoch=%llu root=%llx calls=%zu tables=%zu\n",
                    static_cast<unsigned long long>(report.resource_steps), static_cast<unsigned long long>(report.resource_visits),
                    static_cast<unsigned long long>(report.resource_restarts), static_cast<unsigned long long>(report.resource_epoch),
                    static_cast<unsigned long long>(report.resource_root_frame), calls, tables.size());
    }
    producer_case_observed = true;
}

void run_large_producer() {
    controller_host::before_boot_callback = producer_large;
    check(controller_host::with_actual_boot_controller(nullptr),
          "genuine boot Controller dispatches actual bounded large-resource producer");
    controller_host::before_boot_callback = nullptr;
    check(producer_case_observed && e1000_host::driver::device_count == 0 && register_reads == 0 && register_writes == 0 && dma_allocs == 0,
          "large-resource admission tests expose no second hardware model, device instruction or invented lifecycle authority");
}

void run_admission_consumer(std::string_view name) {
    if (name == "consumer_admission_clock_unavailable" || name == "consumer_admission_clock_overflow" ||
        name == "consumer_admission_clock_regression") {
        controller_host::before_boot_callback = [](const controller_host::cm::detail::Controller&) {
            if (selected_engine_case == "consumer_admission_clock_unavailable")
                maintenance_host::clock_available.store(false);
            else if (selected_engine_case == "consumer_admission_clock_overflow")
                maintenance_host::now.store(UINT64_MAX - 1);
            else {
                maintenance_host::now.store(1'000'000);
                maintenance_host::clock_step.store(UINT64_MAX - 100);
            }
        };
        check(controller_host::with_actual_boot_controller([](const controller_host::cm::detail::Controller&) {
                  check(e1000_host::driver::device_count == 1, "actual clock-failed boot probe retains its genuine metadata");
                  auto* failed = e1000_host::driver::devices[0];
                  check(failed->registers.empty() &&
                            failed->retirement.load() == e1000_host::driver::E1000Device::Retirement::cold_failed &&
                            allocs == setup_allocs && register_reads == 0 && register_writes == 0 && dma_allocs == 0 &&
                            maintenance_host::events().empty() && pci_cold_host::mem_enable_writes == 0,
                        "actual caller rejects unavailable, overflowing or regressing clock before producer or hardware exposure");
              }),
              "genuine boot caller applies the actual finite hardware clock failure input");
        controller_host::before_boot_callback = nullptr;
        return;
    }
    if (name == "consumer_admission_deadline") {
        e1000_host::selected_case = name;
        controller_host::before_boot_callback = [](const controller_host::cm::detail::Controller& controller) {
            e1000_host::preclaim_reuse_function(controller);
            maintenance_host::clock_step.store(3'000'000'000);
        };
        check(controller_host::with_actual_boot_controller([](const controller_host::cm::detail::Controller& controller) {
                  check(e1000_host::driver::device_count == 1, "actual timed-out caller retains its genuine first driver object");
                  auto* failed = e1000_host::driver::devices[0];
                  check(failed->registers.empty() &&
                            failed->retirement.load() == e1000_host::driver::E1000Device::Retirement::cold_failed && register_reads == 0 &&
                            register_writes == 0 && dma_allocs == 0 && maintenance_host::events().empty() &&
                            pci_cold_host::mem_enable_writes == 0,
                        "actual deadline after a physical-pending step cannot activate MEM, CPU work, DMA or MMIO");
                  maintenance_host::clock_step.store(50'000);
                  mapping::Mapping replacement;
                  constexpr mapping::ViewRequest REQUEST{0, 0, 4096};
                  size_t calls = 0;
                  check(resume_large(*e1000_host::reuse_claim, controller, std::span(&REQUEST, 1), replacement, calls) ==
                            mapping::Status::ready,
                        "same genuine boot Controller can map the separately preclaimed function after the first caller deadline");
                  auto const report = replacement.report();
                  check(report.virtual_base == BANK + 4096 && report.physical == e1000_host::REUSE_BAR0 && report.initial_ticket == 1 &&
                            translate(report.virtual_base) == report.physical,
                        "abandoned uncommitted physical record reserves no VA and does not displace the next actual owner");
                  replacement.close();
                  e1000_host::reuse_claim.reset();
                  std::printf("MMIO_ADMISSION_DEADLINE_HOST next_va=%llx next_ticket=%llu calls=%zu\n",
                              static_cast<unsigned long long>(report.virtual_base), static_cast<unsigned long long>(report.initial_ticket),
                              calls);
              }),
              "actual boot driver enforces the finite monotonic next-call deadline");
        controller_host::before_boot_callback = nullptr;
        return;
    }
    if (name == "consumer_admission_fragmented")
        controller_host::before_boot_callback = [](const controller_host::cm::detail::Controller&) { fragment_large_resource(); };
    if (name == "consumer_admission_cpu_retry")
        controller_host::before_boot_callback = [](const controller_host::cm::detail::Controller&) {
            maintenance_host::pause_remote_after_ipis.store(2 * static_cast<unsigned>(runtime_ports::configured.load()) + 1);
            maintenance_host::resume_remote_after_pauses.store(2600);
        };
    e1000_host::run_consumer_case("consumer_success");
    controller_host::before_boot_callback = nullptr;
    auto const report = e1000_host::device->registers.report();
    check(report.phase == mapping::Phase::complete && report.resource_steps > 1 && report.resource_visits > 128 &&
              report.views[0].initial_ticket == 1 && report.views[1].initial_ticket == 2,
          "actual driver resumes bounded resource admission then performs genuine traffic and original-ticket retirement");
    if (name == "consumer_admission_fragmented")
        check(report.resource_visits > 32768 && report.resource_steps > 256,
              "actual unmodified boot caller completes the real fragmented large-resource cursor beyond the old total limit");
    if (name == "consumer_admission_cpu_retry")
        check(report.views[1].timed_out_ever && report.views[0].initial_ticket == 1 && report.views[1].initial_ticket == 2 &&
                  maintenance_host::pauses.load() >= 2600,
              "actual boot caller resumes the timed-out second child with its original CPU ticket before genuine retirement");
}

template <typename Body>
void writer_refuses_before_effects(Body body) {
    std::map<uint64_t, std::array<uint8_t, 4096>> before;
    for (auto const& [physical, table] : tables) std::memcpy(before[physical].data(), table, 4096);
    int pipefd[2];
    check(pipe(pipefd) == 0, "finite effect observation pipe opens");
    auto child = fork();
    check(child >= 0, "actual mapper refusal executes in a fresh child");
    if (child == 0) {
        close(pipefd[0]);
        alarm(5);
        mmio_writer_host::effects = pipefd[1];
        body();
        _exit(92);  // A silently successful write is not a qualified refusal.
    }
    close(pipefd[1]);
    int status = 0;
    check(waitpid(child, &status, 0) == child, "exact mapper child is joined");
    char events[16];
    auto count = read(pipefd[0], events, sizeof(events));
    close(pipefd[0]);
    check(WIFSIGNALED(status) && WTERMSIG(status) == SIGILL, "actual typed guard traps before writer effects");
    check(count == 0, "actual map entrypoint refuses before allocation, owner tracking, refs or TLB work");
    for (auto const& [physical, bytes] : before)
        check(std::memcmp(bytes.data(), tables.at(physical), 4096) == 0, "all real shared-backed ancestor and leaf words stay unchanged");
}

void writers_after_boot(const controller_host::cm::detail::Controller& controller) {
    fences_after_boot(controller);
    namespace writer = mmio_writer_host;
    auto const report = e1000_host::device->registers.report();
    writer::typed_mmio_guards.store(mapping::enabled());  // Same actual immutable boot selection; creates no owner.
    writer::kernel_pagemap = root;
    auto* ordinary = allocate_table();
    auto* private_root = tables.at(raw(root, 505) & FRAME);
    auto* fake_root = reinterpret_cast<Table*>(report.virtual_base);
    constexpr uint64_t flags = 3 | (1ULL << 63);
    constexpr uint64_t data = 0x400000;
    writer_refuses_before_effects([=] { writer::map_page(ordinary, 0x800000, report.physical, flags); });
    writer_refuses_before_effects([=] { writer::map_page(ordinary, report.virtual_base, data, flags); });
    writer_refuses_before_effects([=] { writer::map_page(private_root, 0x800000, data, flags); });
    writer_refuses_before_effects([=] { (void)writer::entry_at(fake_root, 0); });
    writer_refuses_before_effects([=] { writer::map_page(fake_root, 0x800000, data, flags); });
    writer_refuses_before_effects([=] { writer::map_page(ordinary, 0x800000, report.physical | (1ULL << 52), flags); });
    writer_refuses_before_effects([=] { (void)writer::map_2m_page(ordinary, 0x800000, report.physical & ~0x1fffffULL, flags); });
    writer_refuses_before_effects([=] {
        writer::PageMapBatch batch;
        writer::init_page_map_batch(&batch, ordinary, flags);
        writer::map_page_batched(&batch, 0x800000, report.physical, flags);
    });
    writer_refuses_before_effects([=] {
        // The first page is not claimed. Whole-range preflight must reject the
        // interior claimed page before allocating even its first ancestor.
        writer::map_range(ordinary, {report.physical - 4096, report.physical + 4096}, flags, 0x100000000ULL);
    });
    writer_refuses_before_effects([=] { writer::map_range(ordinary, {data, data + 4096}, flags, report.virtual_base - data); });
    auto const old = writer::tracks;
    auto const old_untracks = writer::untracks;
    auto const old_allocations = writer::allocations;
    auto const old_refs = writer::refs;
    writer::map_page(ordinary, 0x800000, data, flags);
    check(writer::tracks == old + 1 && writer::untracks == old_untracks + 1 && writer::allocations == old_allocations + 3 &&
              writer::refs == old_refs,
          "same actual selected native writer admits ordinary non-device storage through real guards");
}

void run_writers() {
    e1000_host::selected_case = "consumer_success";
    check(controller_host::with_actual_boot_controller(writers_after_boot), "actual producer creates all native writer rejection targets");
    ker::mod::sched::task::Task actor;
    actor.pid = 1;
    controller_host::initialize(actor);
    check(controller_host::with_actual_finalizer_controller(e1000_host::observe_retirement) == 0,
          "real adapter still retires the original mapping after guarded writer attempts");
}
}  // namespace mmio_host

int main(int argc, char** argv) {
    if (argc != 2) return 2;
    std::string_view const name(argv[1]);
    mmio_host::setup(name);
    if (name == "engine_private_fences")
        mmio_host::run_fences();
    else if (name == "engine_budget_refusal")
        mmio_host::run_budget_refusal();
    else if (name == "engine_native_writers")
        mmio_host::run_writers();
    else if (name == "engine_bad_table_alias")
        mmio_host::run_bad_table_alias();
    else if (name == "engine_view_bounds")
        mmio_host::run_view_bounds();
    else if (name.starts_with("engine_large_"))
        mmio_host::run_large_producer();
    else if (name.starts_with("consumer_admission_"))
        mmio_host::run_admission_consumer(name);
    else if (name == "engine_invalid_view_plans" || name == "engine_view_prepare_resume" || name == "engine_cross_bar_views" ||
             name == "engine_legacy_view_default")
        mmio_host::run_producer_case(name);
    else
        e1000_host::run_consumer_case(name);
    mmio_host::verify_global_ports();
    std::printf("MMIO_MAPPING_HOST CASE_PASS name=%.*s assertions=%zu allocations=%llu dma_allocs=%zu dma_frees=%zu\n",
                static_cast<int>(name.size()), name.data(), e1000_host::assertions + mmio_host::assertions,
                static_cast<unsigned long long>(mmio_host::allocs), mmio_host::dma_allocs, mmio_host::dma_frees);
    return 0;
}
