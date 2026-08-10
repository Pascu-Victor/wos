#include <gtest/gtest.h>

#include <array>
#include <cerrno>
#include <cstdint>
#include <initializer_list>
#include <string>
#include <string_view>

#include "service_manifest.h"
#include "supervisor_model.h"

namespace {

using namespace std::string_view_literals;
using wos::init::EnablementKind;
using wos::init::EnablementResult;
using wos::init::initialize_supervisor;
using wos::init::INVALID_SERVICE_ID;
using wos::init::ManifestParseError;
using wos::init::ManifestValidationError;
using wos::init::MAX_MANIFEST_BYTES;
using wos::init::MAX_SERVICES;
using wos::init::parse_service_manifest;
using wos::init::ReadinessKind;
using wos::init::ServiceClass;
using wos::init::ServiceLifecycle;
using wos::init::ServiceManifest;
using wos::init::ServiceState;
using wos::init::ServiceTopology;
using wos::init::ServiceType;
using wos::init::StdioPolicy;
using wos::init::supervisor_advance;
using wos::init::supervisor_begin_journal_shutdown;
using wos::init::supervisor_begin_shutdown;
using wos::init::supervisor_control;
using wos::init::supervisor_handle_event;
using wos::init::SupervisorActionKind;
using wos::init::SupervisorActionList;
using wos::init::SupervisorControl;
using wos::init::SupervisorEvent;
using wos::init::SupervisorEventKind;
using wos::init::SupervisorExitKind;
using wos::init::SupervisorModel;
using wos::init::SupervisorResult;
using wos::init::SupervisorShutdownPhase;
using wos::init::validate_service_manifest;

constexpr std::string_view VALID_MANIFEST = R"MANIFEST(version=1

[service journald]
exec=/sbin/journald
arg=/sbin/journald
type=daemon
class=journal
priority=10
environment=init
readiness=immediate
readiness-timeout-ms=5000
restart=never
backoff-initial-ms=250
backoff-max-ms=30000
restart-budget=5
restart-window-ms=60000
stable-run-ms=60000
stop-term-ms=2000
stop-kill-ms=1000
drain-timeout-ms=2000
stdio=inherit
enable=always

[service dropbear-keygen]
exec=/bin/dropbearkey
arg=/bin/dropbearkey
arg=-t
arg=rsa
arg=-f
arg=/etc/dropbear/dropbear_rsa_host_key
type=oneshot
class=normal
priority=0
environment=init
depends=journald
readiness=exit-success
readiness-timeout-ms=30000
restart=on-failure
backoff-initial-ms=250
backoff-max-ms=30000
restart-budget=5
restart-window-ms=60000
stable-run-ms=60000
stop-term-ms=2000
stop-kill-ms=1000
drain-timeout-ms=2000
stdio=journal
enable=path-missing
enable-path=/etc/dropbear/dropbear_rsa_host_key

[service netd]
exec=/sbin/netd
arg=/sbin/netd
type=daemon
class=network-provider
priority=0
environment=init
depends=journald
readiness=ipv4
readiness-interface=eth0
readiness-timeout-ms=180000
restart=on-failure
backoff-initial-ms=250
backoff-max-ms=30000
restart-budget=5
restart-window-ms=60000
stable-run-ms=60000
stop-term-ms=2000
stop-kill-ms=1000
drain-timeout-ms=2000
stdio=inherit
enable=always

[service httpd]
exec=/sbin/httpd
arg=/sbin/httpd
type=daemon
class=network-consumer
priority=10
environment=init
depends=netd
readiness=immediate
readiness-timeout-ms=5000
restart=on-failure
backoff-initial-ms=250
backoff-max-ms=30000
restart-budget=5
restart-window-ms=60000
stable-run-ms=60000
stop-term-ms=2000
stop-kill-ms=1000
drain-timeout-ms=2000
stdio=inherit
enable=always

[service dropbear]
exec=/bin/dropbear
arg=/bin/dropbear
arg=-r
arg=/etc/dropbear/dropbear_rsa_host_key
arg=-F
type=daemon
class=network-consumer
priority=-5
environment=init
env=DROPBEAR_DELAY_HOSTKEY=0
depends=netd
depends=dropbear-keygen
readiness=immediate
readiness-timeout-ms=5000
restart=on-failure
backoff-initial-ms=250
backoff-max-ms=30000
restart-budget=5
restart-window-ms=60000
stable-run-ms=60000
stop-term-ms=2000
stop-kill-ms=1000
drain-timeout-ms=2000
stdio=journal
enable=always

[service testd]
exec=/usr/bin/testd
arg=/usr/bin/testd
type=oneshot
class=normal
priority=0
environment=init
depends=netd
depends=httpd
depends=dropbear
readiness=exit-success
readiness-timeout-ms=5000
restart=never
backoff-initial-ms=250
backoff-max-ms=30000
restart-budget=1
restart-window-ms=60000
stable-run-ms=60000
stop-term-ms=2000
stop-kill-ms=1000
drain-timeout-ms=2000
stdio=inherit
enable=disabled
)MANIFEST";

auto parsed_valid_manifest() -> ServiceManifest {
    ServiceManifest manifest{};
    EXPECT_TRUE(parse_service_manifest(std::span(VALID_MANIFEST.data(), VALID_MANIFEST.size()), manifest));
    return manifest;
}

auto find_service(const ServiceManifest& manifest, std::string_view name) -> uint8_t {
    for (uint8_t i = 0; i < manifest.service_count; ++i) {
        if (manifest.services.at(i).name.view() == name) {
            return i;
        }
    }
    return INVALID_SERVICE_ID;
}

auto replace_once(std::string input, std::string_view from, std::string_view to) -> std::string {
    size_t const offset = input.find(from);
    EXPECT_NE(offset, std::string::npos);
    if (offset != std::string::npos) {
        input.replace(offset, from.size(), to);
    }
    return input;
}

TEST(InitServiceManifest, ParsesConditionalOneshotAndBuildsDeterministicDag) {
    auto manifest = parsed_valid_manifest();
    ServiceTopology topology{};
    ASSERT_TRUE(validate_service_manifest(manifest, topology));
    ASSERT_EQ(manifest.service_count, 6);

    uint8_t const journal = find_service(manifest, "journald");
    uint8_t const keygen = find_service(manifest, "dropbear-keygen");
    uint8_t const netd = find_service(manifest, "netd");
    uint8_t const dropbear = find_service(manifest, "dropbear");
    uint8_t const testd = find_service(manifest, "testd");
    ASSERT_NE(journal, INVALID_SERVICE_ID);
    ASSERT_NE(keygen, INVALID_SERVICE_ID);
    ASSERT_NE(netd, INVALID_SERVICE_ID);
    ASSERT_NE(dropbear, INVALID_SERVICE_ID);
    ASSERT_NE(testd, INVALID_SERVICE_ID);

    EXPECT_EQ(topology.journal_service, journal);
    EXPECT_EQ(topology.start_order.front(), journal);
    EXPECT_EQ(topology.stop_order.at(manifest.service_count - 1), journal);
    EXPECT_EQ(manifest.services.at(keygen).type, ServiceType::ONESHOT);
    EXPECT_EQ(manifest.services.at(keygen).readiness, ReadinessKind::PROCESS_EXIT_SUCCESS);
    EXPECT_EQ(manifest.services.at(keygen).enablement, EnablementKind::PATH_MISSING);
    EXPECT_EQ(manifest.services.at(keygen).enablement_path.view(), "/etc/dropbear/dropbear_rsa_host_key");
    EXPECT_EQ(manifest.services.at(netd).service_class, ServiceClass::NETWORK_PROVIDER);
    EXPECT_EQ(manifest.services.at(netd).readiness, ReadinessKind::IPV4);
    EXPECT_EQ(manifest.services.at(netd).readiness_interface.view(), "eth0");
    EXPECT_EQ(manifest.services.at(dropbear).priority, -5);
    EXPECT_EQ(manifest.services.at(dropbear).stdio, StdioPolicy::JOURNAL);
    EXPECT_EQ(manifest.services.at(testd).enablement, EnablementKind::DISABLED);
    EXPECT_NE(topology.transitive_dependencies.at(dropbear) & static_cast<uint16_t>(1U << journal), 0);
    EXPECT_NE(topology.transitive_dependencies.at(dropbear) & static_cast<uint16_t>(1U << netd), 0);
}

TEST(InitServiceManifest, ParserRejectsUnknownDuplicateMissingAndUnsupportedFields) {
    ServiceManifest manifest{};
    auto unsupported = replace_once(std::string(VALID_MANIFEST), "version=1", "version=2");
    auto result = parse_service_manifest(std::span(unsupported.data(), unsupported.size()), manifest);
    EXPECT_EQ(result.error, ManifestParseError::UNSUPPORTED_VERSION);

    auto unknown = replace_once(std::string(VALID_MANIFEST), "stdio=inherit", "mystery=yes");
    result = parse_service_manifest(std::span(unknown.data(), unknown.size()), manifest);
    EXPECT_EQ(result.error, ManifestParseError::UNKNOWN_FIELD);

    auto duplicate =
        replace_once(std::string(VALID_MANIFEST), "priority=10\nenvironment=init", "priority=10\npriority=0\nenvironment=init");
    result = parse_service_manifest(std::span(duplicate.data(), duplicate.size()), manifest);
    EXPECT_EQ(result.error, ManifestParseError::DUPLICATE_FIELD);

    auto missing = replace_once(std::string(VALID_MANIFEST), "restart-window-ms=60000\n", "");
    result = parse_service_manifest(std::span(missing.data(), missing.size()), manifest);
    EXPECT_EQ(result.error, ManifestParseError::MISSING_REQUIRED_FIELD);

    auto missing_condition = replace_once(std::string(VALID_MANIFEST), "enable-path=/etc/dropbear/dropbear_rsa_host_key\n", "");
    result = parse_service_manifest(std::span(missing_condition.data(), missing_condition.size()), manifest);
    EXPECT_EQ(result.error, ManifestParseError::MISSING_REQUIRED_FIELD);
}

TEST(InitServiceManifest, ParserEnforcesGlobalAndPerFieldCapacity) {
    ServiceManifest manifest{};
    std::array<char, MAX_MANIFEST_BYTES + 1> too_large{};
    auto result = parse_service_manifest(std::span<const char>(too_large), manifest);
    EXPECT_EQ(result.error, ManifestParseError::TOO_LARGE);

    std::string too_many_services = "version=1\n";
    for (size_t i = 0; i <= MAX_SERVICES; ++i) {
        too_many_services += "[service s" + std::to_string(i) + "]\n";
    }
    result = parse_service_manifest(std::span(too_many_services.data(), too_many_services.size()), manifest);
    EXPECT_EQ(result.error, ManifestParseError::TOO_MANY_SERVICES);

    std::string too_many_arguments = std::string(VALID_MANIFEST);
    size_t const insertion = too_many_arguments.find("type=daemon");
    ASSERT_NE(insertion, std::string::npos);
    too_many_arguments.insert(insertion, std::string(wos::init::MAX_ARGUMENT_BYTES, 'x'));
    too_many_arguments.insert(insertion, "arg=");
    too_many_arguments.insert(insertion + 4 + wos::init::MAX_ARGUMENT_BYTES, "\n");
    result = parse_service_manifest(std::span(too_many_arguments.data(), too_many_arguments.size()), manifest);
    EXPECT_EQ(result.error, ManifestParseError::VALUE_TOO_LONG);
}

TEST(InitServiceManifest, ValidationRejectsDuplicateUnknownCycleAndUnsafePath) {
    ServiceTopology topology{};

    auto duplicate = parsed_valid_manifest();
    ASSERT_TRUE(duplicate.services.at(1).name.assign("journald"));
    auto result = validate_service_manifest(duplicate, topology);
    EXPECT_EQ(result.error, ManifestValidationError::DUPLICATE_NAME);

    auto unknown = parsed_valid_manifest();
    uint8_t const httpd = find_service(unknown, "httpd");
    ASSERT_NE(httpd, INVALID_SERVICE_ID);
    ASSERT_TRUE(unknown.services.at(httpd).dependency_names.front().assign("ghost"));
    result = validate_service_manifest(unknown, topology);
    EXPECT_EQ(result.error, ManifestValidationError::UNKNOWN_DEPENDENCY);

    auto cycle = parsed_valid_manifest();
    uint8_t const journal = find_service(cycle, "journald");
    ASSERT_NE(journal, INVALID_SERVICE_ID);
    ASSERT_TRUE(cycle.services.at(journal).dependency_names.front().assign("dropbear"));
    cycle.services.at(journal).dependency_count = 1;
    result = validate_service_manifest(cycle, topology);
    EXPECT_EQ(result.error, ManifestValidationError::DEPENDENCY_CYCLE);

    auto unsafe = parsed_valid_manifest();
    ASSERT_TRUE(unsafe.services.at(1).executable.assign("/bin/../dropbearkey"));
    unsafe.services.at(1).arguments.front() = unsafe.services.at(1).executable;
    result = validate_service_manifest(unsafe, topology);
    EXPECT_EQ(result.error, ManifestValidationError::UNSAFE_EXECUTABLE);
}

TEST(InitServiceManifest, ValidationEnforcesJournaldNetworkAndReadinessPolicy) {
    ServiceTopology topology{};

    auto journal_recursion = parsed_valid_manifest();
    uint8_t const journal = find_service(journal_recursion, "journald");
    ASSERT_NE(journal, INVALID_SERVICE_ID);
    journal_recursion.services.at(journal).stdio = StdioPolicy::JOURNAL;
    auto result = validate_service_manifest(journal_recursion, topology);
    EXPECT_EQ(result.error, ManifestValidationError::JOURNAL_INVARIANT);

    auto missing_provider = parsed_valid_manifest();
    uint8_t const httpd = find_service(missing_provider, "httpd");
    ASSERT_NE(httpd, INVALID_SERVICE_ID);
    ASSERT_TRUE(missing_provider.services.at(httpd).dependency_names.front().assign("journald"));
    result = validate_service_manifest(missing_provider, topology);
    EXPECT_EQ(result.error, ManifestValidationError::NETWORK_INVARIANT);

    auto wrong_provider_readiness = parsed_valid_manifest();
    uint8_t const netd = find_service(wrong_provider_readiness, "netd");
    ASSERT_NE(netd, INVALID_SERVICE_ID);
    wrong_provider_readiness.services.at(netd).readiness = ReadinessKind::IMMEDIATE;
    wrong_provider_readiness.services.at(netd).readiness_interface = {};
    result = validate_service_manifest(wrong_provider_readiness, topology);
    EXPECT_EQ(result.error, ManifestValidationError::NETWORK_INVARIANT);

    auto wrong_oneshot_readiness = parsed_valid_manifest();
    uint8_t const keygen = find_service(wrong_oneshot_readiness, "dropbear-keygen");
    ASSERT_NE(keygen, INVALID_SERVICE_ID);
    wrong_oneshot_readiness.services.at(keygen).readiness = ReadinessKind::IMMEDIATE;
    result = validate_service_manifest(wrong_oneshot_readiness, topology);
    EXPECT_EQ(result.error, ManifestValidationError::INVALID_READINESS);
}

TEST(InitServiceManifest, ValidationRejectsRangeAndEnvironmentAmbiguity) {
    ServiceTopology topology{};

    auto bad_range = parsed_valid_manifest();
    bad_range.services.front().restart_window_ms = 0;
    auto result = validate_service_manifest(bad_range, topology);
    EXPECT_EQ(result.error, ManifestValidationError::INVALID_RANGE);

    auto duplicate_env = parsed_valid_manifest();
    uint8_t const dropbear = find_service(duplicate_env, "dropbear");
    ASSERT_NE(dropbear, INVALID_SERVICE_ID);
    ASSERT_TRUE(duplicate_env.services.at(dropbear).environment.at(1).assign("DROPBEAR_DELAY_HOSTKEY=1"));
    duplicate_env.services.at(dropbear).environment_count = 2;
    result = validate_service_manifest(duplicate_env, topology);
    EXPECT_EQ(result.error, ManifestValidationError::INVALID_ENVIRONMENT);
}

auto action_count(const SupervisorActionList& actions, SupervisorActionKind kind) -> size_t {
    size_t count = 0;
    for (size_t i = 0; i < actions.size(); ++i) {
        if (actions[i].kind == kind) {
            count++;
        }
    }
    return count;
}

auto has_action(const SupervisorActionList& actions, SupervisorActionKind kind, uint8_t service) -> bool {
    for (size_t i = 0; i < actions.size(); ++i) {
        if (actions[i].kind == kind && actions[i].service == service) {
            return true;
        }
    }
    return false;
}

class SupervisorModelTest : public ::testing::Test {
   protected:
    void SetUp() override {
        manifest = parsed_valid_manifest();
        ASSERT_TRUE(validate_service_manifest(manifest, topology));
        journal = find_service(manifest, "journald");
        keygen = find_service(manifest, "dropbear-keygen");
        netd = find_service(manifest, "netd");
        httpd = find_service(manifest, "httpd");
        dropbear = find_service(manifest, "dropbear");
        testd = find_service(manifest, "testd");
        ASSERT_NE(journal, INVALID_SERVICE_ID);
        ASSERT_NE(keygen, INVALID_SERVICE_ID);
        ASSERT_NE(netd, INVALID_SERVICE_ID);
        ASSERT_NE(httpd, INVALID_SERVICE_ID);
        ASSERT_NE(dropbear, INVALID_SERVICE_ID);
        ASSERT_NE(testd, INVALID_SERVICE_ID);

        enablement.fill(EnablementResult::CONDITION_NOT_MET);
        for (uint8_t service = 0; service < manifest.service_count; ++service) {
            enablement.at(service) = EnablementResult::CONDITION_MET;
        }
        enablement.at(keygen) = EnablementResult::CONDITION_NOT_MET;
        enablement.at(testd) = EnablementResult::CONDITION_NOT_MET;
    }

    void enable_only(std::initializer_list<uint8_t> services) {
        enablement.fill(EnablementResult::CONDITION_NOT_MET);
        for (uint8_t service : services) {
            enablement.at(service) = EnablementResult::CONDITION_MET;
        }
    }

    void initialize(uint64_t now_ms = 0) {
        ASSERT_EQ(initialize_supervisor(model, manifest, topology,
                                        std::span<const EnablementResult>(enablement.data(), manifest.service_count), now_ms),
                  SupervisorResult::OK);
    }

    void advance(uint64_t now_ms) { ASSERT_EQ(supervisor_advance(model, now_ms, actions), SupervisorResult::OK); }

    void spawn_succeeded(uint8_t service, int64_t pid, uint64_t now_ms) {
        uint32_t const GENERATION = model.services.at(service).generation;
        ASSERT_EQ(supervisor_handle_event(model, now_ms,
                                          SupervisorEvent{
                                              .kind = SupervisorEventKind::SPAWN_SUCCEEDED,
                                              .service = service,
                                              .generation = GENERATION,
                                              .pid = pid,
                                              .pgid = pid,
                                          },
                                          actions),
                  SupervisorResult::OK);
    }

    void exec_succeeded(uint8_t service, uint64_t now_ms) {
        ASSERT_EQ(supervisor_handle_event(model, now_ms,
                                          SupervisorEvent{
                                              .kind = SupervisorEventKind::EXEC_SUCCEEDED,
                                              .service = service,
                                              .generation = model.services.at(service).generation,
                                          },
                                          actions),
                  SupervisorResult::OK);
    }

    void spawn_and_exec(uint8_t service, int64_t pid, uint64_t now_ms) {
        ASSERT_EQ(model.services.at(service).state, ServiceState::STARTING);
        spawn_succeeded(service, pid, now_ms);
        exec_succeeded(service, now_ms);
    }

    void process_exit(uint8_t service, SupervisorExitKind kind, int32_t detail, uint64_t now_ms) {
        ASSERT_EQ(supervisor_handle_event(model, now_ms,
                                          SupervisorEvent{
                                              .kind = SupervisorEventKind::EXITED,
                                              .service = service,
                                              .generation = model.services.at(service).generation,
                                              .exit_kind = kind,
                                              .detail = detail,
                                          },
                                          actions),
                  SupervisorResult::OK);
    }

    void quiesce(uint8_t service, uint64_t now_ms) {
        ASSERT_EQ(supervisor_handle_event(model, now_ms,
                                          SupervisorEvent{
                                              .kind = SupervisorEventKind::QUIESCED,
                                              .service = service,
                                              .generation = model.services.at(service).generation,
                                          },
                                          actions),
                  SupervisorResult::OK);
    }

    void probe(uint8_t service, SupervisorEventKind kind, uint64_t now_ms, int32_t detail = 0) {
        ASSERT_EQ(supervisor_handle_event(model, now_ms,
                                          SupervisorEvent{
                                              .kind = kind,
                                              .service = service,
                                              .generation = model.services.at(service).generation,
                                              .detail = detail,
                                          },
                                          actions),
                  SupervisorResult::OK);
    }

    ServiceManifest manifest{};
    ServiceTopology topology{};
    std::array<EnablementResult, MAX_SERVICES> enablement{};
    SupervisorModel model{};
    SupervisorActionList actions{};
    uint8_t journal{INVALID_SERVICE_ID};
    uint8_t keygen{INVALID_SERVICE_ID};
    uint8_t netd{INVALID_SERVICE_ID};
    uint8_t httpd{INVALID_SERVICE_ID};
    uint8_t dropbear{INVALID_SERVICE_ID};
    uint8_t testd{INVALID_SERVICE_ID};
};

TEST_F(SupervisorModelTest, GatesNetworkConsumersUntilJournalAndIpv4AreReady) {
    initialize();
    advance(0);
    EXPECT_EQ(action_count(actions, SupervisorActionKind::SPAWN), 1);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SPAWN, journal));
    EXPECT_EQ(model.services.at(keygen).state, ServiceState::EXITED);
    EXPECT_TRUE(model.services.at(keygen).completion_success);

    spawn_and_exec(journal, 101, 1);
    EXPECT_EQ(model.services.at(journal).state, ServiceState::READY);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SPAWN, netd));

    spawn_and_exec(netd, 102, 2);
    EXPECT_EQ(model.services.at(netd).state, ServiceState::RUNNING);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::START_IPV4_PROBE, netd));
    EXPECT_EQ(model.services.at(httpd).state, ServiceState::WAITING);
    EXPECT_EQ(model.services.at(dropbear).state, ServiceState::WAITING);

    probe(netd, SupervisorEventKind::PROBE_ERROR, 3, EADDRNOTAVAIL);
    EXPECT_EQ(model.services.at(netd).state, ServiceState::RUNNING);
    EXPECT_EQ(action_count(actions, SupervisorActionKind::SPAWN), 0);
    probe(netd, SupervisorEventKind::PROBE_PENDING, 4);
    EXPECT_EQ(action_count(actions, SupervisorActionKind::SPAWN), 0);

    probe(netd, SupervisorEventKind::PROBE_READY, 5);
    EXPECT_EQ(model.services.at(netd).state, ServiceState::READY);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SPAWN, httpd));
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SPAWN, dropbear));
    EXPECT_EQ(action_count(actions, SupervisorActionKind::SPAWN), 2);
}

TEST_F(SupervisorModelTest, MissingExecStopsAndFailsWithoutAmbiguousExitCode) {
    enable_only({journal});
    initialize();
    advance(0);
    spawn_succeeded(journal, 201, 1);
    uint32_t const GENERATION = model.services.at(journal).generation;

    ASSERT_EQ(supervisor_handle_event(model, 2,
                                      SupervisorEvent{
                                          .kind = SupervisorEventKind::EXEC_FAILED,
                                          .service = journal,
                                          .generation = GENERATION,
                                          .detail = ENOENT,
                                      },
                                      actions),
              SupervisorResult::OK);
    EXPECT_EQ(model.services.at(journal).state, ServiceState::STOPPING);
    EXPECT_EQ(model.services.at(journal).last_detail, ENOENT);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SEND_TERM, journal));

    process_exit(journal, SupervisorExitKind::EXIT_CODE, 127, 3);
    EXPECT_EQ(model.services.at(journal).state, ServiceState::STOPPING);
    quiesce(journal, 4);
    EXPECT_EQ(model.services.at(journal).state, ServiceState::FAILED);
    EXPECT_EQ(model.services.at(journal).generation, GENERATION);
    EXPECT_EQ(action_count(actions, SupervisorActionKind::SPAWN), 0);
}

TEST_F(SupervisorModelTest, OneshotExitSuccessSatisfiesAndFailureBacksOff) {
    enable_only({journal, keygen});
    initialize();
    advance(0);
    spawn_and_exec(journal, 301, 1);
    ASSERT_EQ(model.services.at(keygen).state, ServiceState::STARTING);
    spawn_and_exec(keygen, 302, 2);
    EXPECT_EQ(model.services.at(keygen).state, ServiceState::RUNNING);
    process_exit(keygen, SupervisorExitKind::EXIT_CODE, 0, 3);
    EXPECT_EQ(model.services.at(keygen).state, ServiceState::STOPPING);
    quiesce(keygen, 4);
    EXPECT_EQ(model.services.at(keygen).state, ServiceState::EXITED);
    EXPECT_TRUE(model.services.at(keygen).completion_success);

    enablement.at(keygen) = EnablementResult::CONDITION_MET;
    initialize(10);
    advance(10);
    spawn_and_exec(journal, 303, 11);
    spawn_and_exec(keygen, 304, 12);
    process_exit(keygen, SupervisorExitKind::EXIT_CODE, 1, 13);
    quiesce(keygen, 14);
    EXPECT_EQ(model.services.at(keygen).state, ServiceState::BACKOFF);
    EXPECT_FALSE(model.services.at(keygen).completion_success);
    EXPECT_EQ(model.services.at(keygen).failures_in_window, 1U);
    EXPECT_EQ(model.services.at(keygen).restart_deadline_ms, 264U);
}

TEST_F(SupervisorModelTest, RestartBackoffCapsAndBudgetExhaustsExactly) {
    enable_only({journal});
    auto& spec = manifest.services.at(journal);
    spec.restart = wos::init::RestartPolicy::ALWAYS;
    spec.backoff_initial_ms = 10;
    spec.backoff_max_ms = 20;
    spec.restart_budget = 3;
    spec.restart_window_ms = 1000;
    spec.stable_run_ms = 1000;
    initialize();
    advance(0);
    spawn_and_exec(journal, 401, 1);

    process_exit(journal, SupervisorExitKind::SIGNAL, 9, 2);
    quiesce(journal, 2);
    EXPECT_EQ(model.services.at(journal).restart_deadline_ms, 12U);
    advance(12);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SPAWN, journal));
    spawn_and_exec(journal, 402, 12);

    process_exit(journal, SupervisorExitKind::SIGNAL, 9, 13);
    quiesce(journal, 13);
    EXPECT_EQ(model.services.at(journal).restart_deadline_ms, 33U);
    advance(33);
    spawn_and_exec(journal, 403, 33);

    process_exit(journal, SupervisorExitKind::SIGNAL, 9, 34);
    quiesce(journal, 34);
    EXPECT_EQ(model.services.at(journal).restart_deadline_ms, 54U);
    advance(54);
    spawn_and_exec(journal, 404, 54);

    process_exit(journal, SupervisorExitKind::SIGNAL, 9, 55);
    quiesce(journal, 55);
    EXPECT_EQ(model.services.at(journal).state, ServiceState::FAILED);
    EXPECT_EQ(model.services.at(journal).failures_in_window, 3U);
    EXPECT_EQ(model.services.at(journal).spawn_attempts, 4U);
    EXPECT_EQ(action_count(actions, SupervisorActionKind::SPAWN), 0);
}

TEST_F(SupervisorModelTest, RestartWindowExpiresWithoutResettingStableBackoff) {
    enable_only({journal});
    auto& spec = manifest.services.at(journal);
    spec.restart = wos::init::RestartPolicy::ALWAYS;
    spec.backoff_initial_ms = 10;
    spec.backoff_max_ms = 20;
    spec.restart_budget = 1;
    spec.restart_window_ms = 50;
    spec.stable_run_ms = 1000;
    initialize();
    advance(0);
    spawn_and_exec(journal, 411, 1);
    process_exit(journal, SupervisorExitKind::SIGNAL, 9, 2);
    quiesce(journal, 2);
    advance(12);
    spawn_and_exec(journal, 412, 12);

    process_exit(journal, SupervisorExitKind::SIGNAL, 9, 100);
    quiesce(journal, 100);
    EXPECT_EQ(model.services.at(journal).state, ServiceState::BACKOFF);
    EXPECT_EQ(model.services.at(journal).failures_in_window, 1U);
    EXPECT_EQ(model.services.at(journal).restart_deadline_ms, 120U);
}

TEST_F(SupervisorModelTest, StableReadyRunResetsBudgetAndBackoffExponent) {
    enable_only({journal});
    auto& spec = manifest.services.at(journal);
    spec.restart = wos::init::RestartPolicy::ALWAYS;
    spec.backoff_initial_ms = 10;
    spec.backoff_max_ms = 20;
    spec.restart_budget = 1;
    spec.restart_window_ms = 1000;
    spec.stable_run_ms = 30;
    initialize();
    advance(0);
    spawn_and_exec(journal, 421, 1);
    process_exit(journal, SupervisorExitKind::SIGNAL, 9, 2);
    quiesce(journal, 2);
    advance(12);
    spawn_and_exec(journal, 422, 12);

    advance(42);
    EXPECT_EQ(model.services.at(journal).failures_in_window, 0U);
    EXPECT_EQ(model.services.at(journal).backoff_exponent, 0U);
    process_exit(journal, SupervisorExitKind::SIGNAL, 9, 43);
    quiesce(journal, 43);
    EXPECT_EQ(model.services.at(journal).state, ServiceState::BACKOFF);
    EXPECT_EQ(model.services.at(journal).restart_deadline_ms, 53U);
    EXPECT_EQ(model.services.at(journal).spawn_attempts, 2U);
}

TEST_F(SupervisorModelTest, RejectsEarlyQuiescenceInvalidProcessGroupsAndStaleGenerations) {
    enable_only({journal});
    initialize();
    advance(0);
    uint32_t const FIRST_GENERATION = model.services.at(journal).generation;

    EXPECT_EQ(supervisor_handle_event(model, 1,
                                      SupervisorEvent{
                                          .kind = SupervisorEventKind::QUIESCED,
                                          .service = journal,
                                          .generation = FIRST_GENERATION,
                                      },
                                      actions),
              SupervisorResult::INVALID_STATE);
    EXPECT_TRUE(model.services.at(journal).spawn_result_pending);
    EXPECT_EQ(supervisor_handle_event(model, 1,
                                      SupervisorEvent{
                                          .kind = SupervisorEventKind::SPAWN_SUCCEEDED,
                                          .service = journal,
                                          .generation = FIRST_GENERATION,
                                          .pid = 501,
                                          .pgid = 502,
                                      },
                                      actions),
              SupervisorResult::INVALID_STATE);
    EXPECT_TRUE(model.services.at(journal).spawn_result_pending);

    spawn_succeeded(journal, 501, 1);
    EXPECT_EQ(supervisor_handle_event(model, 1,
                                      SupervisorEvent{
                                          .kind = SupervisorEventKind::QUIESCED,
                                          .service = journal,
                                          .generation = FIRST_GENERATION,
                                      },
                                      actions),
              SupervisorResult::INVALID_STATE);
    EXPECT_EQ(model.services.at(journal).pid, 501);
    exec_succeeded(journal, 1);
    process_exit(journal, SupervisorExitKind::SIGNAL, 9, 2);
    quiesce(journal, 2);
    ASSERT_EQ(supervisor_control(model, 3, journal, SupervisorControl::START, actions), SupervisorResult::OK);
    ASSERT_EQ(model.services.at(journal).state, ServiceState::STARTING);
    EXPECT_EQ(model.services.at(journal).generation, FIRST_GENERATION + 1);

    EXPECT_EQ(supervisor_handle_event(model, 4,
                                      SupervisorEvent{
                                          .kind = SupervisorEventKind::EXITED,
                                          .service = journal,
                                          .generation = FIRST_GENERATION,
                                          .exit_kind = SupervisorExitKind::SIGNAL,
                                          .detail = 9,
                                      },
                                      actions),
              SupervisorResult::STALE_GENERATION);
    EXPECT_TRUE(model.services.at(journal).spawn_result_pending);
    EXPECT_EQ(model.services.at(journal).generation, FIRST_GENERATION + 1);
}

TEST_F(SupervisorModelTest, ManualStartOverridesOnlyExplicitDisabledEnablement) {
    manifest.services.at(testd).dependency_mask = 0;
    manifest.services.at(httpd).enablement = EnablementKind::PATH_EXISTS;
    ASSERT_TRUE(manifest.services.at(httpd).enablement_path.assign("/missing/httpd-enable"));
    enablement.at(httpd) = EnablementResult::CONDITION_NOT_MET;
    initialize();
    EXPECT_EQ(model.services.at(testd).state, ServiceState::DISABLED);
    EXPECT_EQ(model.services.at(httpd).state, ServiceState::DISABLED);
    EXPECT_EQ(model.services.at(keygen).state, ServiceState::EXITED);

    EXPECT_EQ(supervisor_control(model, 1, httpd, SupervisorControl::START, actions), SupervisorResult::INVALID_STATE);
    EXPECT_EQ(supervisor_control(model, 1, keygen, SupervisorControl::START, actions), SupervisorResult::INVALID_STATE);
    ASSERT_EQ(supervisor_control(model, 1, testd, SupervisorControl::START, actions), SupervisorResult::OK);
    EXPECT_TRUE(model.services.at(testd).manual_enable_override);
    EXPECT_EQ(model.services.at(testd).state, ServiceState::STARTING);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SPAWN, testd));

    uint32_t const GENERATION = model.services.at(testd).generation;
    ASSERT_EQ(supervisor_control(model, 2, testd, SupervisorControl::STOP, actions), SupervisorResult::OK);
    EXPECT_EQ(model.services.at(testd).state, ServiceState::STOPPING);
    ASSERT_EQ(supervisor_handle_event(model, 2,
                                      SupervisorEvent{
                                          .kind = SupervisorEventKind::SPAWN_FAILED,
                                          .service = testd,
                                          .generation = GENERATION,
                                          .detail = ECANCELED,
                                      },
                                      actions),
              SupervisorResult::OK);
    EXPECT_EQ(model.services.at(testd).state, ServiceState::EXITED);
    ASSERT_EQ(supervisor_control(model, 3, testd, SupervisorControl::START, actions), SupervisorResult::OK);
    EXPECT_EQ(model.services.at(testd).state, ServiceState::STARTING);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SPAWN, testd));
}

TEST_F(SupervisorModelTest, ProbeErrorAfterReadyCascadesDependentsUntilRecovery) {
    initialize();
    advance(0);
    spawn_and_exec(journal, 601, 1);
    spawn_and_exec(netd, 602, 2);
    probe(netd, SupervisorEventKind::PROBE_READY, 3);
    spawn_and_exec(httpd, 603, 4);
    spawn_and_exec(dropbear, 604, 4);
    ASSERT_EQ(model.services.at(httpd).state, ServiceState::READY);
    ASSERT_EQ(model.services.at(dropbear).state, ServiceState::READY);

    probe(netd, SupervisorEventKind::PROBE_ERROR, 5, EIO);
    EXPECT_EQ(model.services.at(netd).state, ServiceState::RUNNING);
    EXPECT_TRUE(model.services.at(netd).probe_active);
    EXPECT_EQ(model.services.at(httpd).state, ServiceState::STOPPING);
    EXPECT_EQ(model.services.at(dropbear).state, ServiceState::STOPPING);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SEND_TERM, httpd));
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SEND_TERM, dropbear));

    process_exit(httpd, SupervisorExitKind::EXIT_CODE, 0, 6);
    quiesce(httpd, 6);
    process_exit(dropbear, SupervisorExitKind::EXIT_CODE, 0, 6);
    quiesce(dropbear, 6);
    EXPECT_EQ(model.services.at(httpd).state, ServiceState::WAITING);
    EXPECT_EQ(model.services.at(dropbear).state, ServiceState::WAITING);
    EXPECT_EQ(action_count(actions, SupervisorActionKind::SPAWN), 0);

    probe(netd, SupervisorEventKind::PROBE_READY, 7);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SPAWN, httpd));
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SPAWN, dropbear));
}

TEST_F(SupervisorModelTest, Ipv4ReadinessTimeoutStopsProbeAndBacksOffWithoutStartingConsumers) {
    initialize();
    advance(0);
    spawn_and_exec(journal, 611, 1);
    spawn_and_exec(netd, 612, 2);
    ASSERT_EQ(model.services.at(netd).state, ServiceState::RUNNING);
    ASSERT_EQ(model.services.at(netd).state_deadline_ms, 180002U);
    EXPECT_TRUE(model.services.at(netd).probe_active);

    probe(netd, SupervisorEventKind::PROBE_PENDING, 3);
    probe(netd, SupervisorEventKind::PROBE_ERROR, 4, EIO);
    EXPECT_EQ(model.services.at(netd).state, ServiceState::RUNNING);
    EXPECT_EQ(model.services.at(netd).state_deadline_ms, 180002U);
    EXPECT_EQ(action_count(actions, SupervisorActionKind::SPAWN), 0);

    advance(180001);
    EXPECT_EQ(model.services.at(netd).state, ServiceState::RUNNING);
    EXPECT_EQ(actions.size(), 0U);
    advance(180002);
    EXPECT_EQ(model.services.at(netd).state, ServiceState::STOPPING);
    EXPECT_FALSE(model.services.at(netd).probe_active);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::STOP_IPV4_PROBE, netd));
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SEND_TERM, netd));
    EXPECT_EQ(action_count(actions, SupervisorActionKind::SPAWN), 0);
    EXPECT_EQ(model.services.at(httpd).generation, 0U);
    EXPECT_EQ(model.services.at(dropbear).generation, 0U);

    process_exit(netd, SupervisorExitKind::SIGNAL, 15, 180003);
    quiesce(netd, 180003);
    EXPECT_EQ(model.services.at(netd).state, ServiceState::BACKOFF);
    EXPECT_EQ(model.services.at(netd).restart_deadline_ms, 180253U);
    EXPECT_EQ(model.services.at(netd).failures_in_window, 1U);
    EXPECT_EQ(action_count(actions, SupervisorActionKind::SPAWN), 0);
}

TEST_F(SupervisorModelTest, StopThenStartWhileSpawnPendingWaitsForOldGenerationQuiescence) {
    enable_only({journal});
    initialize();
    advance(0);
    uint32_t const FIRST_GENERATION = model.services.at(journal).generation;
    EXPECT_EQ(model.services.at(journal).state_deadline_ms, 5000U);

    ASSERT_EQ(supervisor_control(model, 100, journal, SupervisorControl::STOP, actions), SupervisorResult::OK);
    EXPECT_EQ(model.services.at(journal).state, ServiceState::STOPPING);
    EXPECT_EQ(model.services.at(journal).state_deadline_ms, 2100U);
    EXPECT_EQ(action_count(actions, SupervisorActionKind::SEND_TERM), 0);
    EXPECT_EQ(action_count(actions, SupervisorActionKind::SPAWN), 0);

    ASSERT_EQ(supervisor_control(model, 101, journal, SupervisorControl::START, actions), SupervisorResult::OK);
    EXPECT_TRUE(model.services.at(journal).desired_running);
    EXPECT_EQ(model.services.at(journal).state, ServiceState::STOPPING);
    EXPECT_EQ(action_count(actions, SupervisorActionKind::SPAWN), 0);

    spawn_succeeded(journal, 701, 102);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SEND_TERM, journal));
    EXPECT_EQ(model.services.at(journal).state_deadline_ms, 2102U);
    exec_succeeded(journal, 102);
    EXPECT_EQ(action_count(actions, SupervisorActionKind::SPAWN), 0);
    process_exit(journal, SupervisorExitKind::EXIT_CODE, 0, 103);
    EXPECT_EQ(action_count(actions, SupervisorActionKind::SPAWN), 0);

    quiesce(journal, 103);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SPAWN, journal));
    EXPECT_EQ(model.services.at(journal).generation, FIRST_GENERATION + 1);
    EXPECT_EQ(model.services.at(journal).spawn_attempts, 2U);
    EXPECT_EQ(model.services.at(journal).pid, -1);
    EXPECT_EQ(model.services.at(journal).pgid, -1);
}

TEST_F(SupervisorModelTest, ReverseDagShutdownKeepsJournalUntilExplicitSecondPhase) {
    initialize();
    advance(0);
    spawn_and_exec(journal, 801, 1);
    spawn_and_exec(netd, 802, 2);
    probe(netd, SupervisorEventKind::PROBE_READY, 3);
    spawn_and_exec(httpd, 803, 4);
    spawn_and_exec(dropbear, 804, 4);

    ASSERT_EQ(supervisor_begin_shutdown(model, 5, actions), SupervisorResult::OK);
    EXPECT_EQ(model.shutdown_phase, SupervisorShutdownPhase::SERVICES);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SEND_TERM, httpd));
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SEND_TERM, dropbear));
    EXPECT_FALSE(has_action(actions, SupervisorActionKind::SEND_TERM, netd));
    EXPECT_FALSE(has_action(actions, SupervisorActionKind::SEND_TERM, journal));
    EXPECT_EQ(action_count(actions, SupervisorActionKind::SPAWN), 0);

    process_exit(httpd, SupervisorExitKind::EXIT_CODE, 0, 6);
    quiesce(httpd, 6);
    EXPECT_FALSE(has_action(actions, SupervisorActionKind::SEND_TERM, netd));
    process_exit(dropbear, SupervisorExitKind::EXIT_CODE, 0, 6);
    quiesce(dropbear, 6);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::STOP_IPV4_PROBE, netd));
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SEND_TERM, netd));
    EXPECT_FALSE(has_action(actions, SupervisorActionKind::SEND_TERM, journal));

    process_exit(netd, SupervisorExitKind::EXIT_CODE, 0, 7);
    quiesce(netd, 7);
    EXPECT_EQ(model.shutdown_phase, SupervisorShutdownPhase::WAITING_FOR_JOURNAL);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::NON_JOURNAL_SHUTDOWN_COMPLETE, INVALID_SERVICE_ID));
    EXPECT_EQ(model.services.at(journal).state, ServiceState::READY);
    EXPECT_FALSE(has_action(actions, SupervisorActionKind::SEND_TERM, journal));

    ASSERT_EQ(supervisor_begin_journal_shutdown(model, 8, actions), SupervisorResult::OK);
    EXPECT_EQ(model.shutdown_phase, SupervisorShutdownPhase::JOURNAL);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SEND_TERM, journal));
    process_exit(journal, SupervisorExitKind::EXIT_CODE, 0, 9);
    quiesce(journal, 9);
    EXPECT_EQ(model.shutdown_phase, SupervisorShutdownPhase::COMPLETE);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SHUTDOWN_COMPLETE, INVALID_SERVICE_ID));
    EXPECT_EQ(action_count(actions, SupervisorActionKind::SPAWN), 0);
}

TEST_F(SupervisorModelTest, ShutdownDuringPendingSpawnDefersSignalUntilPgidExists) {
    enable_only({journal, netd});
    initialize();
    advance(0);
    spawn_and_exec(journal, 811, 1);
    ASSERT_EQ(model.services.at(netd).state, ServiceState::STARTING);
    ASSERT_TRUE(model.services.at(netd).spawn_result_pending);

    ASSERT_EQ(supervisor_begin_shutdown(model, 100, actions), SupervisorResult::OK);
    EXPECT_EQ(model.services.at(netd).state, ServiceState::STOPPING);
    EXPECT_EQ(model.services.at(netd).state_deadline_ms, 2100U);
    EXPECT_FALSE(has_action(actions, SupervisorActionKind::SEND_TERM, netd));
    EXPECT_EQ(action_count(actions, SupervisorActionKind::SPAWN), 0);

    spawn_succeeded(netd, 812, 101);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SEND_TERM, netd));
    exec_succeeded(netd, 101);
    process_exit(netd, SupervisorExitKind::EXIT_CODE, 0, 102);
    quiesce(netd, 102);
    EXPECT_EQ(model.shutdown_phase, SupervisorShutdownPhase::WAITING_FOR_JOURNAL);
    EXPECT_EQ(action_count(actions, SupervisorActionKind::SPAWN), 0);
}

TEST_F(SupervisorModelTest, ShutdownCancelsBackoffAndNeverStartsAnotherGeneration) {
    enable_only({journal, netd});
    manifest.services.at(netd).backoff_initial_ms = 10;
    manifest.services.at(netd).backoff_max_ms = 10;
    initialize();
    advance(0);
    spawn_and_exec(journal, 821, 1);
    spawn_and_exec(netd, 822, 2);
    process_exit(netd, SupervisorExitKind::SIGNAL, 9, 3);
    quiesce(netd, 3);
    ASSERT_EQ(model.services.at(netd).state, ServiceState::BACKOFF);
    uint32_t const GENERATION = model.services.at(netd).generation;

    ASSERT_EQ(supervisor_begin_shutdown(model, 4, actions), SupervisorResult::OK);
    EXPECT_EQ(model.services.at(netd).state, ServiceState::EXITED);
    EXPECT_EQ(action_count(actions, SupervisorActionKind::SPAWN), 0);
    advance(1000);
    EXPECT_EQ(model.services.at(netd).generation, GENERATION);
    EXPECT_EQ(action_count(actions, SupervisorActionKind::SPAWN), 0);
}

TEST_F(SupervisorModelTest, DrainTimeoutAbandonsGenerationUntilLateQuiescenceProvesItGone) {
    enable_only({journal});
    auto& spec = manifest.services.at(journal);
    spec.stop_term_ms = 10;
    spec.stop_kill_ms = 5;
    spec.drain_timeout_ms = 7;
    initialize();
    advance(0);
    spawn_and_exec(journal, 831, 1);
    uint32_t const FIRST_GENERATION = model.services.at(journal).generation;

    ASSERT_EQ(supervisor_control(model, 2, journal, SupervisorControl::STOP, actions), SupervisorResult::OK);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SEND_TERM, journal));
    process_exit(journal, SupervisorExitKind::SIGNAL, 15, 3);
    EXPECT_FALSE(model.services.at(journal).quiesced);
    EXPECT_EQ(model.services.at(journal).pid, 831);
    EXPECT_EQ(model.services.at(journal).pgid, 831);

    advance(12);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SEND_KILL, journal));
    advance(17);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::CLOSE_OUTPUT, journal));
    advance(24);
    EXPECT_EQ(model.services.at(journal).state, ServiceState::FAILED);
    EXPECT_TRUE(model.services.at(journal).abandoned);
    EXPECT_FALSE(model.services.at(journal).quiesced);
    EXPECT_EQ(model.services.at(journal).pid, 831);
    EXPECT_EQ(model.services.at(journal).pgid, 831);
    EXPECT_EQ(supervisor_control(model, 24, journal, SupervisorControl::START, actions), SupervisorResult::INVALID_STATE);
    EXPECT_EQ(action_count(actions, SupervisorActionKind::SPAWN), 0);

    quiesce(journal, 25);
    EXPECT_FALSE(model.services.at(journal).abandoned);
    EXPECT_TRUE(model.services.at(journal).quiesced);
    EXPECT_EQ(model.services.at(journal).pid, -1);
    EXPECT_EQ(model.services.at(journal).pgid, -1);
    EXPECT_EQ(model.services.at(journal).state, ServiceState::FAILED);

    ASSERT_EQ(supervisor_control(model, 26, journal, SupervisorControl::START, actions), SupervisorResult::OK);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SPAWN, journal));
    EXPECT_EQ(model.services.at(journal).generation, FIRST_GENERATION + 1);
}

TEST_F(SupervisorModelTest, NoSpawnOrActionAppearsAfterShutdownCompletes) {
    initialize();
    ASSERT_EQ(supervisor_begin_shutdown(model, 0, actions), SupervisorResult::OK);
    EXPECT_EQ(model.shutdown_phase, SupervisorShutdownPhase::WAITING_FOR_JOURNAL);
    EXPECT_EQ(action_count(actions, SupervisorActionKind::SPAWN), 0);
    ASSERT_EQ(supervisor_begin_journal_shutdown(model, 1, actions), SupervisorResult::OK);
    EXPECT_EQ(model.shutdown_phase, SupervisorShutdownPhase::COMPLETE);
    EXPECT_TRUE(has_action(actions, SupervisorActionKind::SHUTDOWN_COMPLETE, INVALID_SERVICE_ID));

    advance(100000);
    EXPECT_EQ(actions.size(), 0U);
    EXPECT_EQ(model.services.at(journal).generation, 0U);
    EXPECT_EQ(model.services.at(netd).generation, 0U);
}

}  // namespace
