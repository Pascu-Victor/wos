// These finite declarations and actual source splices have semantic order.
// clang-format off
#include "mmio_mapping_ports.inc"
#include "e1000_consumer_generated.hpp"
#include "e1000_mmio_consumer_cases.inc"
#include "mmio_writer_generated.hpp"
#include <dev/pci_extended.hpp>
// clang-format on

namespace mmio_host {
namespace mapping = ker::mod::mm::mmio;
namespace cache = ker::mod::mm::mmio_cache;
namespace cm = ker::mod::mm::cpu_maintenance;
namespace pci = ker::dev::pci;
inline size_t assertions{};

void check(bool value, const char* reason) {
    ++assertions;
    if (value) return;
    std::fprintf(stderr, "actual MM engine check failed: %s\n", reason);
    std::abort();
}

void setup(std::string_view name) {
    pci_cold_host::setup();
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
        pci_cold_host::put(pci_cold_host::nic().config, 0x14, 0x84000000, 4);
        pci_cold_host::nic().sizing_masks[1] = 0xfe000000;  // Genuine aligned 32-MiB companion sizing result.
    }
    maintenance_host::setup(4);
    for (size_t cpu = 0; cpu < 4; ++cpu) {
        auto& input = runtime_ports::cpus[cpu];
        input.apic = static_cast<uint32_t>(cpu);
        input.cpuid[{1, 0}].ebx = static_cast<uint32_t>(cpu << 24);
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
    mapping::detail::begin_boot_history();
    mmio_writer_host::typed_mmio_guards.store(mapping::enabled());
    check(mapping::enabled(), "immutable real boot selection enabled");
    check(pci::pci_enumerate_all() == 3 && pci::pci_enumeration_complete(), "actual PCI enumeration finds all three bounded functions");
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
    check(report.physical == pci_cold_host::BAR0 && report.bytes == pci_cold_host::BAR_BYTES && report.physical_spans == 2,
          "actual private PCI capability accounts both decoded physical BARs");
    for (uint64_t offset = 0; offset < report.bytes; offset += 4096)
        check(translate(report.virtual_base + offset) == report.physical + offset, "actual typed page tables cover every exact BAR page");
    {
        auto original = device->registers.borrow();
        check(original.valid() && device->registers.report().normal_readers == 1, "actual affine normal borrow increments its real gate");
        mapping::Borrow moved(std::move(original));
        check(!original.valid() && moved.valid() && device->registers.report().normal_readers == 1,
              "actual move transfers one counted view without duplication");
        device->registers.close();
        check(moved.valid() && moved.read32(e1000_host::driver::REG_STATUS) != UINT32_MAX,
              "existing genuine view survives actual gate closure");
        check(!device->registers.borrow().valid() && !device->registers.terminal_borrow().valid(),
              "closed admission refuses new normals and a held normal refuses terminal access");
        moved.reset();
        check(!moved.valid() && device->registers.borrows_drained(), "actual normal reset releases its final counted reference");
        auto terminal = device->registers.terminal_borrow();
        check(terminal.valid() && !device->registers.terminal_borrow().valid(), "actual terminal view is unique after closed-and-drained");
        check(terminal.read32(e1000_host::driver::REG_STATUS) != UINT32_MAX, "real terminal view accesses the original live hardware bank");
    }
    check(!device->registers.report().terminal_active, "terminal destruction precedes the genuine hardware lifecycle");
    check(!mapping::detail::generic_virtual_allowed(root, report.virtual_base, 4096), "generic virtual mutator cannot enter private arena");
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
    for (uint64_t offset = 0; offset < report.bytes; offset += 4096)
        check(translate(report.virtual_base + offset) == UINT64_MAX, "actual withdrawal clears every original leaf");
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
    check(controller_host::with_actual_boot_controller([](const controller_host::cm::detail::Controller&) {
              check(e1000_host::driver::device_count == 1, "actual rejected probe retains its sole real driver object");
              auto* device = e1000_host::driver::devices[0];
              check(device->typed_mmio && device->mmio == nullptr && device->registers.empty() &&
                        device->retirement.load() == e1000_host::driver::E1000Device::Retirement::cold_failed,
                    "over-budget exact own BARs cannot publish a view or raw fallback");
              check(allocs == setup_allocs && register_reads == 0 && register_writes == 0 && dma_allocs == 0,
                    "actual total-span budget refuses before table, register or DMA effects");
              check(mapping::detail::record_generic_physical_access(pci_cold_host::BAR0, 4096),
                    "pre-reservation capacity refusal does not claim physical frames");
          }),
          "actual boot issuer reaches the real capacity refusal");
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
    else
        e1000_host::run_consumer_case(name);
    mmio_host::verify_global_ports();
    std::printf("MMIO_MAPPING_HOST CASE_PASS name=%.*s assertions=%zu allocations=%llu dma_allocs=%zu dma_frees=%zu\n",
                static_cast<int>(name.size()), name.data(), e1000_host::assertions + mmio_host::assertions,
                static_cast<unsigned long long>(mmio_host::allocs), mmio_host::dma_allocs, mmio_host::dma_frees);
    return 0;
}
