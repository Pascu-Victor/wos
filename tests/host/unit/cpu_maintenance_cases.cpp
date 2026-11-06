#include "cpu_maintenance_ports.inc"

namespace {
namespace cm = ker::mod::mm::cpu_maintenance;
namespace cache = ker::mod::mm::mmio_cache;
namespace host = maintenance_host;
namespace ports = runtime_ports;
using cm::Kind;
using cm::Phase;
using cm::Status;
using cm::Ticket;

static_assert(!std::is_copy_constructible_v<Ticket> && !std::is_copy_assignable_v<Ticket>);
static_assert(std::is_nothrow_move_constructible_v<Ticket>);
static_assert(!std::is_constructible_v<Ticket, size_t, uint64_t>);

template <typename Body>
void isolated(Body body) {
    pid_t const child = fork();
    ASSERT_GE(child, 0);
    if (child == 0) {
        rlimit const no_core{0, 0};
        if (setrlimit(RLIMIT_CORE, &no_core) != 0) _exit(2);
        alarm(15);
        body();
        EXPECT_EQ(ports::violations.load(), 0U);
        EXPECT_EQ(ports::irq_saves.load(), ports::irq_restores.load());
        bool const failed = ::testing::Test::HasFailure();
        std::fflush(nullptr);
        _exit(failed ? 1 : 0);
    }
    int status = 0;
    pid_t waited;
    do {
        waited = waitpid(child, &status, 0);
    } while (waited == -1 && errno == EINTR);
    ASSERT_EQ(waited, child);
    ASSERT_TRUE(WIFEXITED(status)) << status;
    EXPECT_EQ(WEXITSTATUS(status), 0);
}

auto mask(size_t count) -> uint64_t { return count == 64 ? UINT64_MAX : (uint64_t{1} << count) - 1; }

void boot(size_t count) {
    host::setup(count);
    ASSERT_EQ(cm::detail::initialize_boot(0x70), Status::ready);
    ASSERT_EQ(cache::configure_boot_cohort(count), cache::Status::ready);
    for (size_t i = 0; i < count; ++i) {
        ports::select(i);
        ASSERT_EQ(cache::capture_boot_cpu(i), cache::Status::ready);
        ASSERT_EQ(cm::detail::note_current_cpu_ready(), Status::ready);
        EXPECT_EQ(cm::detail::note_current_cpu_ready(), Status::ready);
    }
    ports::select(0);
}

auto range(uint64_t physical = ports::BAR) -> cache::QualifiedUcRange {
    cache::QualifiedUcRange result;
    EXPECT_EQ(cache::qualify_uc_range(physical, 4096, result), cache::Status::ready);
    return result;
}

auto deadline(uint64_t duration = 100'000'000) -> uint64_t { return host::now.load() + duration; }
auto wait_flag(const std::atomic<bool>& flag) -> bool {
    auto const until = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (!flag.load(std::memory_order_acquire) && std::chrono::steady_clock::now() < until) std::this_thread::yield();
    return flag.load(std::memory_order_acquire);
}

TEST(CpuMaintenance, GeneralInactiveServiceDoesNotProbeBootstrapOrPoisonThePool) {
    isolated([] {
        host::setup(1);
        ports::cpus[0].current_status = cache::Status::wrong_cpu;
        cm::detail::service_current_cpu();
        EXPECT_EQ(ports::irq_saves.load(), 0U);
        EXPECT_EQ(cm::service_observation().status, Status::unconfigured);
        EXPECT_EQ(cm::service_observation().service_error, cache::Status::ready);
        EXPECT_EQ(cm::pool_stats().prepared, 0U);
    });
}

TEST(CpuMaintenance, GeneralConfigurationBoundsAndChangedInputsAreSticky) {
    for (size_t count : {size_t{0}, size_t{65}})
        isolated([count] {
            host::setup(count);
            EXPECT_EQ(cm::detail::initialize_boot(0x70), Status::invalid_configuration);
            ports::configured.store(1);
            EXPECT_EQ(cm::detail::initialize_boot(0x70), Status::invalid_configuration);
        });
    isolated([] {
        host::setup(1);
        EXPECT_EQ(cm::detail::initialize_boot(0), Status::invalid_configuration);
        EXPECT_EQ(cm::detail::initialize_boot(0x70), Status::invalid_configuration);
    });
    isolated([] {
        host::setup(1);
        EXPECT_EQ(cm::detail::initialize_boot(0x70), Status::ready);
        EXPECT_EQ(cm::detail::initialize_boot(0x70), Status::ready);
        EXPECT_EQ(cm::detail::initialize_boot(0x71), Status::invalid_configuration);
        EXPECT_EQ(cm::detail::initialize_boot(0x70), Status::invalid_configuration);
    });
}

TEST(CpuMaintenance, GeneralReadinessRequiresActualCaptureKernelRootAndAllConfiguredCpus) {
    isolated([] {
        host::setup(2);
        ASSERT_EQ(cm::detail::initialize_boot(0x70), Status::ready);
        EXPECT_EQ(cm::detail::note_current_cpu_ready(), Status::not_ready);
        ASSERT_EQ(cache::configure_boot_cohort(2), cache::Status::ready);
        ASSERT_EQ(cache::capture_boot_cpu(0), cache::Status::ready);
        host::kernel_root = false;
        EXPECT_EQ(cm::detail::note_current_cpu_ready(), Status::not_ready);
        host::kernel_root = true;
        EXPECT_EQ(cm::detail::note_current_cpu_ready(), Status::ready);
        EXPECT_EQ(cm::service_observation().ready_mask, 1U);
        ports::select(1);
        ASSERT_EQ(cache::capture_boot_cpu(1), cache::Status::ready);
        ports::select(0);
        auto token = range();
        Ticket ticket;
        EXPECT_EQ(cm::prepare(std::move(token), Kind::translations, ticket), Status::not_ready);
        EXPECT_FALSE(token.empty());
        EXPECT_TRUE(ticket.empty());
        ports::select(1);
        EXPECT_EQ(cm::detail::note_current_cpu_ready(), Status::ready);
        ports::select(0);
        EXPECT_EQ(cm::prepare(std::move(token), Kind::translations, ticket), Status::ready);
    });
}

TEST(CpuMaintenance, GeneralPrepareFailurePreservesTheRealInputAndPreparedDestructionCancels) {
    isolated([] {
        boot(1);
        auto token = range();
        uint64_t const incarnation = token.incarnation();
        Ticket ticket;
        host::controller = false;
        EXPECT_EQ(cm::prepare(std::move(token), Kind::translations, ticket), Status::invalid_context);
        host::controller = true;
        EXPECT_EQ(cm::prepare(std::move(token), static_cast<Kind>(255), ticket), Status::invalid_kind);
        EXPECT_EQ(token.incarnation(), incarnation);
        ASSERT_EQ(cm::prepare(std::move(token), Kind::translations, ticket), Status::ready);
        EXPECT_TRUE(token.empty());
        EXPECT_EQ(cm::observe(ticket).phase, Phase::prepared);
        EXPECT_EQ(cm::observe(ticket).status, Status::ready);
        auto second = range();
        EXPECT_EQ(cm::prepare(std::move(second), Kind::translations, ticket), Status::output_not_empty);
        EXPECT_FALSE(second.empty());
        EXPECT_TRUE(host::events().empty());
        Ticket moved(std::move(ticket));
        EXPECT_TRUE(ticket.empty());
        moved = Ticket{};
        EXPECT_EQ(cm::pool_stats().prepared, 0U);
        cache::QualifiedUcRange empty;
        EXPECT_EQ(cm::prepare(std::move(empty), Kind::translations, ticket), Status::empty_range);
    });
}

TEST(CpuMaintenance, GeneralOneAndSixtyFourTargetsIncludeOriginAndCurrentTaskRoots) {
    for (size_t count : {size_t{1}, size_t{64}})
        isolated([count] {
            boot(count);
            auto token = range();
            Ticket ticket;
            ASSERT_EQ(cm::prepare(std::move(token), Kind::translations, ticket), Status::ready);
            ASSERT_EQ(cm::publish(ticket), Status::pending);
            auto const done = cm::wait_until(ticket, deadline());
            ASSERT_EQ(done.status, Status::complete);
            EXPECT_EQ(done.phase, Phase::complete);
            EXPECT_EQ(done.targets, mask(count));
            EXPECT_EQ(done.translation_completed, mask(count));
            EXPECT_EQ(done.cache_claimed | done.cache_completed, 0U);
            EXPECT_TRUE(done.admission_closed);
            EXPECT_EQ(done.readers, 0U);
            for (size_t i = 0; i < count; ++i) {
                EXPECT_EQ(done.cpus[i].translation_completed, ticket.id());
                EXPECT_EQ(done.cpus[i].translation_apic, ports::cpus[i].apic);
                EXPECT_EQ(done.cpus[i].translation_cr3, (i + 1) * 4096 + 0x100000);
            }
            EXPECT_EQ(host::events().size(), count);
            EXPECT_EQ(cm::release_complete(ticket), Status::complete);
            EXPECT_TRUE(ticket.empty());
            EXPECT_EQ(cm::pool_stats().complete, 0U);
        });
}

TEST(CpuMaintenance, GeneralTlbDeferralCannotStartCacheAndExplicitSameIdRetryPreservesTimeoutHistory) {
    isolated([] {
        boot(4);
        auto token = range();
        Ticket ticket;
        ASSERT_EQ(cm::prepare(std::move(token), Kind::translations_then_cache, ticket), Status::ready);
        uint64_t const id = ticket.id();
        ASSERT_EQ(cm::diagnostic_defer(ticket, Phase::translations, 1), Status::ready);
        ASSERT_EQ(cm::publish(ticket), Status::pending);
        auto const timed = cm::wait_until(ticket, deadline(5'000'000));
        EXPECT_EQ(timed.status, Status::timeout_retained);
        EXPECT_EQ(timed.phase, Phase::translations);
        EXPECT_EQ(timed.translation_completed, 13U);
        EXPECT_EQ(timed.cache_claimed | timed.cache_completed, 0U);
        EXPECT_EQ(timed.translation_withheld, 2U);
        EXPECT_TRUE(timed.timed_out_ever);
        for (auto const& event : host::events()) EXPECT_EQ(event.phase, Phase::translations);
        host::drain();
        EXPECT_EQ(cm::observe(ticket).phase, Phase::translations);
        ASSERT_EQ(cm::diagnostic_resume(ticket, Phase::translations), Status::ready);
        auto const done = cm::wait_until(ticket, deadline());
        ASSERT_EQ(done.status, Status::complete);
        EXPECT_EQ(done.id, id);
        EXPECT_TRUE(done.timed_out_ever);
        EXPECT_EQ(done.translation_withheld, 2U);
        auto const events = host::events();
        ASSERT_EQ(events.size(), 8U);
        for (size_t i = 0; i < 4; ++i) EXPECT_EQ(events[i].phase, Phase::translations);
        for (size_t i = 4; i < 8; ++i) EXPECT_EQ(events[i].phase, Phase::caches);
        EXPECT_EQ(cm::release_complete(ticket), Status::complete);
    });
}

TEST(CpuMaintenance, GeneralCacheDeferralLateAckDoesNotCompleteOrAdvanceInTheBackground) {
    isolated([] {
        boot(2);
        auto token = range();
        Ticket ticket;
        ASSERT_EQ(cm::prepare(std::move(token), Kind::translations_then_cache, ticket), Status::ready);
        ASSERT_EQ(cm::diagnostic_defer(ticket, Phase::caches, 1), Status::ready);
        ASSERT_EQ(cm::publish(ticket), Status::pending);
        auto const timed = cm::wait_until(ticket, deadline(5'000'000));
        EXPECT_EQ(timed.status, Status::timeout_retained);
        EXPECT_EQ(timed.phase, Phase::caches);
        EXPECT_EQ(timed.translation_completed, 3U);
        EXPECT_EQ(timed.cache_completed, 1U);
        EXPECT_EQ(timed.cache_withheld, 2U);
        ASSERT_EQ(cm::diagnostic_resume(ticket, Phase::caches), Status::ready);
        host::service(1);
        auto const late = cm::observe(ticket);
        EXPECT_EQ(late.phase, Phase::caches);
        EXPECT_EQ(late.cache_completed, 3U);
        EXPECT_EQ(late.status, Status::pending);
        EXPECT_EQ(cm::release_complete(ticket), Status::busy);
        auto const done = cm::wait_until(ticket, deadline());
        EXPECT_EQ(done.status, Status::complete);
        EXPECT_TRUE(done.timed_out_ever);
        EXPECT_EQ(cm::release_complete(ticket), Status::complete);
    });
}

TEST(CpuMaintenance, GeneralDuplicateAndNestedServiceNeverRunsAClaimTwice) {
    isolated([] {
        boot(2);
        auto token = range();
        Ticket ticket;
        ASSERT_EQ(cm::prepare(std::move(token), Kind::translations, ticket), Status::ready);
        host::nested_service.store(true);
        ASSERT_EQ(cm::publish(ticket), Status::pending);
        host::drain();
        for (unsigned i = 0; i < 8; ++i) {
            host::service(0);
            host::service(1);
        }
        EXPECT_EQ(host::events().size(), 2U);
        EXPECT_EQ(cm::wait_until(ticket, deadline()).status, Status::complete);
        EXPECT_EQ(cm::release_complete(ticket), Status::complete);
    });
}

TEST(CpuMaintenance, GeneralControllerAndClockFailuresLeavePublishedOwnershipLive) {
    isolated([] {
        boot(1);
        auto token = range();
        Ticket ticket;
        ASSERT_EQ(cm::prepare(std::move(token), Kind::translations_then_cache, ticket), Status::ready);
        host::clock_available.store(false);
        EXPECT_EQ(cm::publish(ticket), Status::clock_unavailable);
        EXPECT_EQ(cm::observe(ticket).phase, Phase::prepared);
        host::clock_available.store(true);
        ASSERT_EQ(cm::publish(ticket), Status::pending);
        host::controller = false;
        EXPECT_EQ(cm::wait_until(ticket, deadline()).status, Status::invalid_context);
        host::controller = true;
        host::clock_available.store(false);
        EXPECT_EQ(cm::wait_until(ticket, deadline()).status, Status::clock_unavailable);
        EXPECT_EQ(cm::observe(ticket).phase, Phase::translations);
        EXPECT_TRUE(host::events().empty());
        host::clock_available.store(true);
        EXPECT_EQ(cm::wait_until(ticket, deadline()).status, Status::complete);
        EXPECT_EQ(cm::release_complete(ticket), Status::complete);
    });
}

TEST(CpuMaintenance, GeneralUnknownIdentityNeverAcknowledgesLogicalZeroAndPoisonsFutureAdmission) {
    isolated([] {
        boot(1);
        auto token = range(), next = range();
        Ticket ticket;
        ASSERT_EQ(cm::prepare(std::move(token), Kind::translations, ticket), Status::ready);
        ASSERT_EQ(cm::publish(ticket), Status::pending);
        ports::select(64);
        cm::detail::service_current_cpu();
        ports::select(0);
        auto const failed = cm::wait_until(ticket, deadline());
        EXPECT_EQ(failed.status, Status::failed_retained);
        EXPECT_EQ(failed.service_error, cache::Status::wrong_cpu);
        EXPECT_EQ(failed.translation_completed, 0U);
        EXPECT_TRUE(host::events().empty());
        auto const probes = ports::irq_saves.load();
        cm::detail::service_current_cpu();
        EXPECT_EQ(ports::irq_saves.load(), probes);
        Ticket output;
        EXPECT_EQ(cm::prepare(std::move(next), Kind::translations, output), Status::failed_retained);
        EXPECT_FALSE(next.empty());
        EXPECT_TRUE(output.empty());
    });
}

TEST(CpuMaintenance, GeneralActualRevalidationFaultAndValueDriftNeverRunTheLocalInstruction) {
    for (bool fault : {false, true})
        isolated([fault] {
            boot(2);
            auto token = range();
            Ticket ticket;
            ASSERT_EQ(cm::prepare(std::move(token), Kind::translations, ticket), Status::ready);
            ASSERT_EQ(cm::publish(ticket), Status::pending);
            if (fault)
                ports::cpus[1].faults.insert(0x277);
            else
                ports::cpus[1].msr[0x277] ^= 1;
            host::service(1);
            auto const failed = cm::wait_until(ticket, deadline());
            EXPECT_EQ(failed.status, Status::failed_retained);
            EXPECT_EQ(failed.failed, 2U);
            EXPECT_EQ(failed.cpus[1].translation_status, fault ? cache::Status::read_fault : cache::Status::state_drift);
            EXPECT_EQ(failed.cpus[1].translation_completed, 0U);
            EXPECT_TRUE(host::events().empty());
            EXPECT_EQ(cm::release_complete(ticket), Status::busy);
        });
}

TEST(CpuMaintenance, GeneralPublishedAbandonmentRetainsBoundedCapacityEvenAfterLateTlbAcks) {
    isolated([] {
        boot(1);
        host::auto_progress.store(false);
        for (size_t i = 0; i < cm::POOL_CAPACITY; ++i) {
            auto token = range();
            Ticket ticket;
            ASSERT_EQ(cm::prepare(std::move(token), Kind::translations_then_cache, ticket), Status::ready);
            ASSERT_EQ(cm::publish(ticket), Status::pending);
        }
        EXPECT_EQ(cm::pool_stats().retained, cm::POOL_CAPACITY);
        auto extra = range();
        Ticket output;
        EXPECT_EQ(cm::prepare(std::move(extra), Kind::translations, output), Status::capacity);
        EXPECT_FALSE(extra.empty());
        EXPECT_TRUE(output.empty());
        host::service(0);
        EXPECT_EQ(cm::pool_stats().retained, cm::POOL_CAPACITY);
        EXPECT_EQ(host::events().size(), cm::POOL_CAPACITY);
        for (auto const& event : host::events()) EXPECT_EQ(event.phase, Phase::translations);
    });
}

TEST(CpuMaintenance, GeneralRetryAndDeadlineBudgetsDoNotSendUnboundedIpis) {
    isolated([] {
        boot(2);
        host::auto_progress.store(false);
        host::clock_step.store(2'000'000);
        auto token = range();
        Ticket ticket;
        ASSERT_EQ(cm::prepare(std::move(token), Kind::translations, ticket), Status::ready);
        ASSERT_EQ(cm::publish(ticket), Status::pending);
        EXPECT_EQ(cm::wait_until(ticket, deadline(1'000'000'000)).status, Status::timeout_retained);
        EXPECT_LE(host::ipis.load(), 18U);
        EXPECT_TRUE(cm::observe(ticket).timed_out_ever);
    });
}

TEST(CpuMaintenance, HookPrejoinScannerAcrossReuseLoadsOnlyTheNewGeneration) {
    isolated([] {
        boot(2);
        host::auto_progress.store(false);
        auto token = range();
        Ticket old;
        ASSERT_EQ(cm::prepare(std::move(token), Kind::translations, old), Status::ready);
        uint64_t const old_id = old.id();
        ASSERT_EQ(cm::publish(old), Status::pending);
        host::service(0);
        host::service(1);
        host::pre_armed.store(true);
        std::thread late([] {
            ports::select(1, 2);
            cm::detail::service_current_cpu();
        });
        EXPECT_TRUE(wait_flag(host::pre_entered));
        EXPECT_EQ(cm::wait_until(old, deadline()).status, Status::complete);
        EXPECT_EQ(cm::release_complete(old), Status::complete);
        auto replacement = range(ports::BAR + 4096);
        Ticket next;
        auto const prepared = cm::prepare(std::move(replacement), Kind::translations, next);
        EXPECT_EQ(prepared, Status::ready);
        if (prepared == Status::ready) EXPECT_EQ(cm::publish(next), Status::pending);
        host::pre_release.store(true, std::memory_order_release);
        late.join();
        ASSERT_FALSE(next.empty());
        EXPECT_GT(next.id(), old_id);
        auto const observed = cm::observe(next);
        EXPECT_EQ(observed.cpus[1].translation_completed, next.id());
        EXPECT_NE(observed.cpus[1].translation_completed, old_id);
        EXPECT_EQ(cm::wait_until(next, deadline()).status, Status::complete);
        EXPECT_EQ(cm::release_complete(next), Status::complete);
    });
}

TEST(CpuMaintenance, HookAfterAckReaderTailBlocksCompletionAndSlotReuse) {
    isolated([] {
        boot(2);
        host::auto_progress.store(false);
        auto token = range();
        Ticket ticket;
        ASSERT_EQ(cm::prepare(std::move(token), Kind::translations, ticket), Status::ready);
        ASSERT_EQ(cm::publish(ticket), Status::pending);
        host::service(0);
        host::post_armed.store(true);
        std::thread reader([] {
            ports::select(1, 2);
            cm::detail::service_current_cpu();
        });
        EXPECT_TRUE(wait_flag(host::post_entered));
        auto const waiting = cm::wait_until(ticket, deadline(5'000'000));
        EXPECT_EQ(waiting.status, Status::timeout_retained);
        EXPECT_EQ(waiting.phase, Phase::closing);
        EXPECT_EQ(waiting.translation_completed, 3U);
        EXPECT_EQ(waiting.readers, 1U);
        EXPECT_TRUE(waiting.admission_closed);
        EXPECT_EQ(cm::release_complete(ticket), Status::busy);
        std::array<Ticket, cm::POOL_CAPACITY - 1> prepared;
        for (auto& output : prepared) {
            auto input = range();
            EXPECT_EQ(cm::prepare(std::move(input), Kind::translations, output), Status::ready);
        }
        auto input = range();
        Ticket output;
        EXPECT_EQ(cm::prepare(std::move(input), Kind::translations, output), Status::capacity);
        EXPECT_FALSE(input.empty());
        host::post_release.store(true, std::memory_order_release);
        reader.join();
        EXPECT_EQ(cm::wait_until(ticket, deadline()).status, Status::complete);
        EXPECT_EQ(cm::release_complete(ticket), Status::complete);
        EXPECT_EQ(cm::prepare(std::move(input), Kind::translations, output), Status::ready);
    });
}

TEST(CpuMaintenance, HookReaderSaturationDefersWithoutWrappingOrAcknowledging) {
    isolated([] {
        boot(2);
        host::auto_progress.store(false);
        auto token = range();
        Ticket ticket;
        ASSERT_EQ(cm::prepare(std::move(token), Kind::translations, ticket), Status::ready);
        ASSERT_EQ(cm::publish(ticket), Status::pending);
        host::post_armed.store(true);
        std::thread reader([] {
            ports::select(1, 2);
            cm::detail::service_current_cpu();
        });
        EXPECT_TRUE(wait_flag(host::post_entered));
        host::service(0);
        auto const held = cm::observe(ticket);
        EXPECT_EQ(held.readers, 1U);
        EXPECT_EQ(held.cpus[0].translation_claimed, 0U);
        EXPECT_EQ(host::events().size(), 1U);
        host::post_release.store(true, std::memory_order_release);
        reader.join();
        EXPECT_EQ(cm::wait_until(ticket, deadline()).status, Status::complete);
        EXPECT_EQ(cm::release_complete(ticket), Status::complete);
    });
}

TEST(CpuMaintenance, FiniteFrozenClockStillStopsAtTheIndependentStepBudget) {
    isolated([] {
        boot(2);
        host::auto_progress.store(false);
        host::clock_step.store(0);
        auto token = range();
        Ticket ticket;
        ASSERT_EQ(cm::prepare(std::move(token), Kind::translations, ticket), Status::ready);
        ASSERT_EQ(cm::publish(ticket), Status::pending);
        EXPECT_EQ(cm::wait_until(ticket, deadline()).status, Status::timeout_retained);
        EXPECT_EQ(host::yields.load(), 4U);
        EXPECT_LE(host::ipis.load(), 2U);
    });
}

TEST(CpuMaintenance, FiniteGenerationExhaustionNeverWrapsOrConsumesTheInput) {
    isolated([] {
        boot(1);
        for (uint64_t expected = 1; expected < 4; ++expected) {
            auto token = range();
            Ticket ticket;
            ASSERT_EQ(cm::prepare(std::move(token), Kind::translations, ticket), Status::ready);
            EXPECT_EQ(ticket.id(), expected);
        }
        for (unsigned attempt = 0; attempt < 2; ++attempt) {
            auto token = range();
            Ticket output;
            EXPECT_EQ(cm::prepare(std::move(token), Kind::translations, output), Status::generation_exhausted);
            EXPECT_FALSE(token.empty());
            EXPECT_TRUE(output.empty());
        }
        EXPECT_EQ(cm::pool_stats().prepared, 0U);
    });
}

TEST(CpuMaintenance, InjectedStaleCompletedIdCannotAdvanceTheRealRequest) {
    isolated([] {
        boot(2);
        {
            auto token = range();
            Ticket first;
            ASSERT_EQ(cm::prepare(std::move(token), Kind::translations, first), Status::ready);
        }
        auto token = range();
        Ticket second;
        ASSERT_EQ(cm::prepare(std::move(token), Kind::translations_then_cache, second), Status::ready);
        ASSERT_EQ(second.id(), 2U);
        ASSERT_EQ(cm::publish(second), Status::pending);
        auto const refused = cm::wait_until(second, deadline(5'000'000));
        EXPECT_EQ(refused.status, Status::timeout_retained);
        EXPECT_EQ(refused.phase, Phase::translations);
        EXPECT_FALSE(refused.admission_closed);
        EXPECT_EQ(refused.translation_completed, 0U);
        EXPECT_EQ(refused.cache_claimed, 0U);
        for (size_t i = 0; i < 2; ++i) {
            EXPECT_EQ(refused.cpus[i].translation_claimed, 2U);
            EXPECT_EQ(refused.cpus[i].translation_completed, 1U);
        }
        EXPECT_EQ(cm::release_complete(second), Status::busy);
    });
}
}  // namespace
