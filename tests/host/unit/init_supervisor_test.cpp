#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <initializer_list>
#include <string>
#include <string_view>

#include "fstab_parse.h"
#include "mount_options.h"
#include "mount_retry.h"
#include "service_identity.h"
#include "service_manifest.h"
#include "service_state.h"
#include "supervisor_model.h"

namespace {

TEST(InitFstabParsing, CompleteFieldsPreservePolicyAndRejectTruncatedSuffixes) {
    std::array<char, 256> output{};
    std::string input = std::string(248, 'x') + ",nosuid next";
    const char* cursor = input.c_str();
    ASSERT_EQ(wos::init::parse_fstab_field(cursor, output), 1);
    EXPECT_STREQ(cursor, " next");
    wos::init::MountOptions options;
    ASSERT_EQ(wos::init::parse_mount_options(output.data(), options), 0);
    EXPECT_EQ(options.flags, MS_NOSUID);
    auto const before = output;
    input = std::string(249, 'x') + ",nosuid next";
    cursor = input.c_str();
    EXPECT_EQ(wos::init::parse_fstab_field(cursor, output), -E2BIG);
    EXPECT_EQ(output, before);
    EXPECT_STREQ(cursor, " next");
    ++cursor;
    ASSERT_EQ(wos::init::parse_fstab_field(cursor, output), 1);
    EXPECT_STREQ(output.data(), "next");
    EXPECT_EQ(wos::init::parse_fstab_field(cursor, output), 0);
}

TEST(InitFstabParsing, FullInputAdmissionHandlesShortReadsAndRejectsLostPolicy) {
    std::array<char, 4096> output{};
    auto load = [&](std::string_view input) {
        size_t offset = 0;
        return wos::init::read_complete_fstab(output, [&](char* buffer, size_t capacity) -> ssize_t {
            size_t count = std::min({capacity, input.size() - offset, size_t{7}});
            std::memcpy(buffer, input.data() + offset, count);
            offset += count;
            return static_cast<ssize_t>(count);
        });
    };
    std::string const boundary = std::string(4088, 'x') + ",nosuid";
    ASSERT_EQ(load(boundary), 4095);
    EXPECT_EQ(std::string_view(output.data()), boundary);
    EXPECT_EQ(load("x" + boundary), -E2BIG);
    EXPECT_EQ(load(std::string_view("x\0,nosuid", 9)), -EINVAL);
    EXPECT_EQ(load(""), 0);
    EXPECT_EQ(wos::init::read_complete_fstab(output, [](char*, size_t) -> ssize_t { return -EIO; }), -EIO);
    EXPECT_EQ(wos::init::read_complete_fstab(output,
                                             [](char* buffer, size_t capacity) -> ssize_t {
                                                 if (capacity == 1) return -EIO;
                                                 std::memset(buffer, 'x', capacity);
                                                 return static_cast<ssize_t>(capacity);
                                             }),
              -EIO);
}

TEST(InitMountOptions, SeparatesNosuidAndPreservesBackendOptions) {
    wos::init::MountOptions parsed;
    ASSERT_EQ(wos::init::parse_mount_options("defaults,nosuid,size=16M", parsed), 0);
    EXPECT_EQ(parsed.flags, MS_NOSUID);
    EXPECT_STREQ(parsed.data.data(), "defaults,size=16M");
    ASSERT_EQ(wos::init::parse_mount_options("nosuid,suid,size=32M", parsed), 0);
    EXPECT_EQ(parsed.flags, 0UL);
    EXPECT_STREQ(parsed.data.data(), "size=32M");
    ASSERT_EQ(wos::init::parse_mount_options("suid,nosuid", parsed), 0);
    EXPECT_EQ(parsed.flags, MS_NOSUID);
    EXPECT_STREQ(parsed.data.data(), "");
    ASSERT_EQ(wos::init::parse_mount_options("nosuidx,xnosuid,suid=1", parsed), 0);
    EXPECT_EQ(parsed.flags, 0UL);
    EXPECT_STREQ(parsed.data.data(), "nosuidx,xnosuid,suid=1");
}

TEST(InitMountOptions, BoundedInputPreservesOutputOnFailure) {
    wos::init::MountOptions parsed;
    ASSERT_EQ(wos::init::parse_mount_options("nosuid,size=1M", parsed), 0);
    auto const before = parsed;
    EXPECT_EQ(wos::init::parse_mount_options(std::string(256, 'x'), parsed), -E2BIG);
    EXPECT_EQ(parsed.flags, before.flags);
    EXPECT_EQ(parsed.data, before.data);
    ASSERT_EQ(wos::init::parse_mount_options(std::string(255, 'x'), parsed), 0);
    EXPECT_EQ(std::strlen(parsed.data.data()), 255U);
    ASSERT_EQ(wos::init::parse_mount_options("", parsed), 0);
    EXPECT_EQ(parsed.flags, 0UL);
    EXPECT_STREQ(parsed.data.data(), "");
}

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
user=root
group=root
supplementary-groups=none
umask=0022
authority-ceiling=65535
no-new-privileges=no
service-privileges=none
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
user=root
group=root
supplementary-groups=none
umask=0022
authority-ceiling=65535
no-new-privileges=no
service-privileges=none
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
user=root
group=root
supplementary-groups=none
umask=0022
authority-ceiling=65535
no-new-privileges=no
service-privileges=none
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
user=root
group=root
supplementary-groups=none
umask=0022
authority-ceiling=65535
no-new-privileges=no
service-privileges=none
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
user=root
group=root
supplementary-groups=none
umask=0022
authority-ceiling=65535
no-new-privileges=no
service-privileges=none
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
user=root
group=root
supplementary-groups=none
umask=0022
authority-ceiling=65535
no-new-privileges=no
service-privileges=none
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

TEST(InitServiceManifest, ResourceLimitsRetainDefaultsAndAcceptEverySupportedResource) {
    auto manifest = parsed_valid_manifest();
    for (size_t index = 0; index < wos::init::SERVICE_RESOURCE_COUNT; ++index) {
        uint64_t const EXPECTED = wos::init::SERVICE_RESOURCE_IDS.at(index) == 7 ? 256 : UINT64_MAX;
        EXPECT_EQ(manifest.services.at(0).resource_limits.at(index).soft, EXPECTED);
        EXPECT_EQ(manifest.services.at(0).resource_limits.at(index).hard, EXPECTED);
    }
    for (size_t index = 0; index < wos::init::SERVICE_RESOURCE_COUNT; ++index) {
        for (std::string_view value : {"0:0"sv, "0:1"sv, "1:1"sv, "255:256"sv, "256:256"sv}) {
            std::string input(VALID_MANIFEST);
            input += "limit-" + std::string(wos::init::SERVICE_RESOURCE_NAMES.at(index)) + "=" + std::string(value) + "\n";
            ASSERT_TRUE(parse_service_manifest(std::span(input.data(), input.size()), manifest));
            ServiceTopology topology{};
            EXPECT_TRUE(validate_service_manifest(manifest, topology));
            auto const& limit = manifest.services.at(manifest.service_count - 1).resource_limits.at(index);
            EXPECT_EQ(limit.soft, std::stoull(std::string(value.substr(0, value.find(':')))));
            EXPECT_EQ(limit.hard, std::stoull(std::string(value.substr(value.find(':') + 1))));
        }
        if (wos::init::SERVICE_RESOURCE_IDS.at(index) == 7) continue;
        std::string input(VALID_MANIFEST);
        input += "limit-" + std::string(wos::init::SERVICE_RESOURCE_NAMES.at(index)) + "=18446744073709551615:infinity\n";
        ASSERT_TRUE(parse_service_manifest(std::span(input.data(), input.size()), manifest));
        ServiceTopology topology{};
        EXPECT_TRUE(validate_service_manifest(manifest, topology));
        EXPECT_EQ(manifest.services.at(manifest.service_count - 1).resource_limits.at(index).soft, UINT64_MAX);
        EXPECT_EQ(manifest.services.at(manifest.service_count - 1).resource_limits.at(index).hard, UINT64_MAX);
    }
}

TEST(InitServiceManifest, ResourceLimitsRejectMalformedDuplicateAndOutOfRangePolicies) {
    for (std::string_view value : {""sv, "1"sv, "1:"sv, ":1"sv, "1:2:3"sv, "-1:2"sv, "1:infinite"sv, "1: 2"sv,
                                   "18446744073709551616:infinity"sv, "1:18446744073709551616"sv}) {
        std::string input = std::string(VALID_MANIFEST) + "limit-as=" + std::string(value) + "\n";
        ServiceManifest manifest{};
        EXPECT_EQ(parse_service_manifest(std::span(input.data(), input.size()), manifest).error, ManifestParseError::INVALID_VALUE);
    }
    for (std::string_view fields :
         {"limit-as=2:1\n"sv, "limit-as=infinity:1\n"sv, "limit-nofile=257:257\n"sv, "limit-nofile=1:infinity\n"sv}) {
        std::string input = std::string(VALID_MANIFEST) + std::string(fields);
        ServiceManifest manifest{};
        ASSERT_TRUE(parse_service_manifest(std::span(input.data(), input.size()), manifest));
        ServiceTopology topology{};
        EXPECT_EQ(validate_service_manifest(manifest, topology).error, ManifestValidationError::INVALID_RANGE);
    }
    std::string input = std::string(VALID_MANIFEST) + "limit-cpu=1:2\nlimit-cpu=2:3\n";
    ServiceManifest manifest{};
    EXPECT_EQ(parse_service_manifest(std::span(input.data(), input.size()), manifest).error, ManifestParseError::DUPLICATE_FIELD);
    input = std::string(VALID_MANIFEST) + "limit-unknown=1:2\n";
    EXPECT_EQ(parse_service_manifest(std::span(input.data(), input.size()), manifest).error, ManifestParseError::UNKNOWN_FIELD);
}

TEST(InitServiceManifest, DisposableResourceServiceFixtureIsValid) {
    auto const ROOT = std::filesystem::path(__FILE__).parent_path().parent_path().parent_path().parent_path();
    std::ifstream base(ROOT / "configs/wos-services.conf");
    std::ifstream extra(ROOT / "tests/fixtures/resource_governance_services.fragment");
    std::ifstream remote(ROOT / "tests/fixtures/resource_governance_remote_services.fragment");
    ASSERT_TRUE(base.good() && extra.good() && remote.good());
    std::string input(std::istreambuf_iterator<char>{base}, {});
    input += '\n';
    input.append(std::istreambuf_iterator<char>{extra}, {});
    input += '\n';
    input.append(std::istreambuf_iterator<char>{remote}, {});
    ServiceManifest manifest{};
    ASSERT_TRUE(parse_service_manifest(std::span(input.data(), input.size()), manifest));
    ServiceTopology topology{};
    ASSERT_TRUE(validate_service_manifest(manifest, topology));
    EXPECT_NE(find_service(manifest, "resource-policy"), INVALID_SERVICE_ID);
    EXPECT_NE(find_service(manifest, "resource-remote-finite"), INVALID_SERVICE_ID);
    EXPECT_NE(find_service(manifest, "resource-remote-fallback"), INVALID_SERVICE_ID);
    EXPECT_NE(find_service(manifest, "resource-cpu-restart"), INVALID_SERVICE_ID);
    EXPECT_EQ(manifest.services.at(find_service(manifest, "resource-nproc-zero")).resource_limits.at(4).soft, 0);
    EXPECT_EQ(manifest.services.at(find_service(manifest, "resource-as-denied")).resource_limits.at(6).soft, 0);
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

TEST(InitServiceIdentity, DisposableServiceProbeFixtureUsesTheRestrictedAccount) {
    auto const ROOT = std::filesystem::path(__FILE__).parent_path().parent_path().parent_path().parent_path();
    std::ifstream base(ROOT / "configs/wos-services.conf");
    std::ifstream extra(ROOT / "tests/fixtures/service_identity_services.fragment");
    ASSERT_TRUE(base.good());
    ASSERT_TRUE(extra.good());
    std::string input((std::istreambuf_iterator<char>(base)), std::istreambuf_iterator<char>());
    input += std::string((std::istreambuf_iterator<char>(extra)), std::istreambuf_iterator<char>());
    ServiceManifest manifest{};
    ASSERT_TRUE(parse_service_manifest(std::span(input.data(), input.size()), manifest));
    ServiceTopology topology{};
    ASSERT_TRUE(validate_service_manifest(manifest, topology));
    EXPECT_EQ(manifest.service_count, 8);
    for (size_t index : {6U, 7U}) {
        auto const& policy = manifest.services[index].identity;
        EXPECT_EQ(policy.user.view(), "httpd");
        EXPECT_EQ(policy.group.view(), "httpd");
        EXPECT_EQ(policy.supplementary_count, 0);
        EXPECT_EQ(policy.mask, 0077);
        EXPECT_EQ(policy.authority_ceiling, 0U);
        EXPECT_TRUE(policy.no_new_privileges);
    }
}

constexpr auto PASSWD_ACCOUNTS = "root:!:0:0:root:/root:/bin/bash\nhttpd:!:100:100:HTTP:/nonexistent:/bin/false\n"sv;
constexpr auto GROUP_ACCOUNTS = "root:x:0:root\nhttpd:x:100:\nreaders:x:101:httpd\n"sv;

auto http_identity() -> wos::init::ServiceIdentityPolicy {
    wos::init::ServiceIdentityPolicy policy{};
    policy.user.assign("httpd");
    policy.group.assign("httpd");
    policy.mask = 0077;
    policy.no_new_privileges = true;
    return policy;
}

TEST(InitServiceIdentity, ManifestRequiresEachIdentityFieldWithoutRootFallback) {
    for (auto field : {"user=root\n"sv, "group=root\n"sv, "supplementary-groups=none\n"sv, "umask=0022\n"sv, "authority-ceiling=65535\n"sv,
                       "no-new-privileges=no\n"sv, "service-privileges=none\n"sv}) {
        std::string input(VALID_MANIFEST);
        input.erase(input.find(field), field.size());
        ServiceManifest manifest{};
        EXPECT_EQ(parse_service_manifest(std::span(input.data(), input.size()), manifest).error,
                  ManifestParseError::MISSING_REQUIRED_FIELD);
    }
}

TEST(InitServiceIdentity, ManifestRejectsInvalidOrDuplicatePolicies) {
    for (auto pair : {std::pair{"umask=0022"sv, "umask=888"sv},
                      {"umask=0022", "umask=1000"},
                      {"umask=0022", "umask=22"},
                      {"authority-ceiling=65535", "authority-ceiling=65536"},
                      {"authority-ceiling=65535", "authority-ceiling=-1"},
                      {"no-new-privileges=no", "no-new-privileges=true"},
                      {"supplementary-groups=none", "supplementary-groups=root,root"},
                      {"supplementary-groups=none", "supplementary-groups=root,"},
                      {"supplementary-groups=none", "supplementary-groups=a,b,c,d,e,f,g,h,i"}}) {
        auto input = replace_once(std::string(VALID_MANIFEST), pair.first, pair.second);
        ServiceManifest manifest{};
        EXPECT_EQ(parse_service_manifest(std::span(input.data(), input.size()), manifest).error, ManifestParseError::INVALID_VALUE);
    }
    for (auto field : {"user=root\n", "group=root\n", "supplementary-groups=none\n", "umask=0022\n", "authority-ceiling=65535\n",
                       "no-new-privileges=no\n", "service-privileges=none\n"}) {
        auto input = std::string(VALID_MANIFEST) + field;
        ServiceManifest manifest{};
        EXPECT_EQ(parse_service_manifest(std::span(input.data(), input.size()), manifest).error, ManifestParseError::DUPLICATE_FIELD);
    }
}

TEST(InitServiceIdentity, AccountsAreStrictBoundedAndPublishedOnlyOnSuccess) {
    using namespace wos::init;
    ServiceAccounts accounts{};
    ASSERT_TRUE(parse_service_accounts(PASSWD_ACCOUNTS, GROUP_ACCOUNTS, accounts));
    EXPECT_EQ(accounts.user_count, 2U);
    EXPECT_EQ(accounts.group_count, 3U);
    for (auto passwd :
         {""sv, "root:!:0:0:root:/root:/bin/bash"sv, "root:!:-1:0:r:/root:/bin/bash\n"sv, "root:!:4294967295:0:r:/root:/bin/bash\n"sv,
          "root:!:4294967296:0:r:/root:/bin/bash\n"sv, "root:!:0:0:r:/root:/bin/bash:extra\n"sv, "root:!:0:4:r:/root:/bin/bash\n"sv,
          "root:!:0:0:r:relative:/bin/bash\n"sv, "root:!:0:0:r:/root:/bin/bash\r\n"sv}) {
        EXPECT_FALSE(parse_service_accounts(passwd, GROUP_ACCOUNTS, accounts));
        EXPECT_EQ(accounts.user_count, 2U);
        EXPECT_EQ(accounts.users[1].uid, 100U);
    }
    for (auto group :
         {""sv, "root:x:0:root\nhttpd:x:100:missing\n"sv, "root:x:0:root\nhttpd:x:100:httpd,\n"sv, "root:x:0:root\nhttpd:x:4294967295:\n"sv,
          "root:x:0:root\nhttpd:x:100:root\nhttpd:x:101:\n"sv, "root:x:0:root\nhttpd:x:0:\n"sv}) {
        EXPECT_FALSE(parse_service_accounts(PASSWD_ACCOUNTS, group, accounts));
        EXPECT_EQ(accounts.group_count, 3U);
    }
    EXPECT_FALSE(parse_service_accounts(std::string(PASSWD_ACCOUNTS) + "another:!:0:0:r:/root:/bin/false\n", GROUP_ACCOUNTS, accounts));
    EXPECT_FALSE(parse_service_accounts(std::string(PASSWD_ACCOUNTS) + "httpd:!:101:100:r:/root:/bin/false\n", GROUP_ACCOUNTS, accounts));
    EXPECT_FALSE(parse_service_accounts(std::string(MAX_ACCOUNT_FILE_BYTES + 1, '\n'), GROUP_ACCOUNTS, accounts));
    EXPECT_FALSE(parse_service_accounts("#" + std::string(MAX_MANIFEST_LINE_BYTES, 'x') + "\n" + std::string(PASSWD_ACCOUNTS),
                                        GROUP_ACCOUNTS, accounts));
    std::string many_users(PASSWD_ACCOUNTS);
    for (size_t i = 0; i < MAX_SERVICE_ACCOUNTS; ++i)
        many_users += "u" + std::to_string(i) + ":!:" + std::to_string(200 + i) + ":0:r:/root:/bin/false\n";
    EXPECT_FALSE(parse_service_accounts(many_users, GROUP_ACCOUNTS, accounts));
}

TEST(InitServiceIdentity, ResolveExactDeclaredGroupsAndRejectUnknownOrUnsafePolicy) {
    using namespace wos::init;
    ServiceAccounts accounts{};
    ASSERT_TRUE(parse_service_accounts(PASSWD_ACCOUNTS, GROUP_ACCOUNTS, accounts));
    auto policy = http_identity();
    ResolvedServiceIdentity identity{};
    ASSERT_TRUE(resolve_service_identity(policy, accounts, identity));
    EXPECT_EQ(identity.uid, 100U);
    EXPECT_EQ(identity.gid, 100U);
    EXPECT_EQ(identity.group_count, 0U);  // Database membership is never implicitly inherited.
    EXPECT_EQ(identity.mask, 0077);
    EXPECT_EQ(identity.authority_ceiling, 0U);
    EXPECT_TRUE(identity.no_new_privileges);
    policy.supplementary_count = 1;
    policy.supplementary_groups[0].assign("readers");
    ASSERT_TRUE(resolve_service_identity(policy, accounts, identity));
    EXPECT_EQ(identity.group_count, 1U);
    EXPECT_EQ(identity.groups[0], 101U);
    policy.supplementary_groups[0].assign("missing");
    EXPECT_FALSE(resolve_service_identity(policy, accounts, identity));
    EXPECT_EQ(identity.group_count, 1U);
    EXPECT_EQ(identity.groups[0], 101U);
    policy = http_identity();
    policy.no_new_privileges = false;
    EXPECT_FALSE(resolve_service_identity(policy, accounts, identity));
    policy = http_identity();
    policy.authority_ceiling = 1;
    EXPECT_FALSE(resolve_service_identity(policy, accounts, identity));
    policy = http_identity();
    policy.user.assign("missing");
    EXPECT_FALSE(resolve_service_identity(policy, accounts, identity));
    policy = http_identity();
    policy.group.assign("missing");
    EXPECT_FALSE(resolve_service_identity(policy, accounts, identity));
    policy = http_identity();
    policy.supplementary_count = MAX_SERVICE_GROUPS + 1;
    EXPECT_FALSE(resolve_service_identity(policy, accounts, identity));
    policy = http_identity();
    accounts.user_count = MAX_SERVICE_ACCOUNTS + 1;
    EXPECT_FALSE(resolve_service_identity(policy, accounts, identity));
}

struct IdentityOperationsProbe {
    int fail_at{}, calls{};
    std::string order{};
    auto step(char name) -> int {
        order += name;
        return ++calls == fail_at ? EACCES : 0;
    }
    auto no_new_privileges() -> int { return step('n'); }
    auto service_privileges(uint64_t value) -> int {
        EXPECT_EQ(value, 0U);
        return step('s');
    }
    auto groups(std::span<const uint32_t> values) -> int {
        EXPECT_TRUE(values.empty());
        return step('g');
    }
    auto gid(uint32_t value) -> int {
        EXPECT_EQ(value, 100U);
        return step('G');
    }
    auto uid(uint32_t value) -> int {
        EXPECT_EQ(value, 100U);
        return step('U');
    }
    auto mask(uint16_t value) -> int {
        EXPECT_EQ(value, 0077);
        return step('m');
    }
    auto ceiling(uint64_t value) -> int {
        EXPECT_EQ(value, 0U);
        return step('c');
    }
    auto verify(const wos::init::ResolvedServiceIdentity&) -> int { return step('v'); }
};

TEST(InitServiceIdentity, EveryDropFailureStopsBeforeLaterOperations) {
    using namespace wos::init;
    ServiceAccounts accounts{};
    ResolvedServiceIdentity identity{};
    ASSERT_TRUE(parse_service_accounts(PASSWD_ACCOUNTS, GROUP_ACCOUNTS, accounts));
    ASSERT_TRUE(resolve_service_identity(http_identity(), accounts, identity));
    constexpr auto ORDER = "nsgGUmcv"sv;
    for (int fail = 1; fail <= 8; ++fail) {
        IdentityOperationsProbe probe{.fail_at = fail};
        EXPECT_EQ(apply_service_identity(identity, probe), EACCES);
        EXPECT_EQ(probe.order, ORDER.substr(0, fail));
    }
    IdentityOperationsProbe success{};
    EXPECT_EQ(apply_service_identity(identity, success), 0);
    EXPECT_EQ(success.order, ORDER);
    identity.valid = false;
    IdentityOperationsProbe invalid{};
    EXPECT_EQ(apply_service_identity(identity, invalid), EINVAL);
    EXPECT_EQ(invalid.calls, 0);
}

TEST(InitServiceIdentity, DeclaredServicePrivilegesAreStrictAndBoundedByCeiling) {
    using namespace wos::init;
    for (auto value :
         {"", "root", "journal-read,", "network-admin,network-admin", "none,journal-read", "journal-read,network-admin,journal-read"}) {
        auto input = replace_once(std::string(VALID_MANIFEST), "service-privileges=none", std::string("service-privileges=") + value);
        ServiceManifest manifest{};
        EXPECT_EQ(parse_service_manifest(std::span(input.data(), input.size()), manifest).error, ManifestParseError::INVALID_VALUE);
    }
    ServiceAccounts accounts{};
    ASSERT_TRUE(parse_service_accounts(PASSWD_ACCOUNTS, GROUP_ACCOUNTS, accounts));
    for (uint64_t mask : {1U, 2U, 3U}) {
        auto policy = http_identity();
        policy.service_privileges = mask;
        policy.authority_ceiling = service_privilege_ceiling(mask);
        ResolvedServiceIdentity identity{};
        ASSERT_TRUE(resolve_service_identity(policy, accounts, identity));
        EXPECT_EQ(identity.service_privileges, mask);
        EXPECT_EQ(identity.authority_ceiling, service_privilege_ceiling(mask));
        policy.authority_ceiling |= 1;
        EXPECT_FALSE(resolve_service_identity(policy, accounts, identity));
        policy.authority_ceiling = 0;
        EXPECT_FALSE(resolve_service_identity(policy, accounts, identity));
        policy.authority_ceiling = service_privilege_ceiling(mask);
        policy.no_new_privileges = false;
        EXPECT_FALSE(resolve_service_identity(policy, accounts, identity));
        policy.user.assign("root");
        EXPECT_FALSE(resolve_service_identity(policy, accounts, identity));
        policy.no_new_privileges = true;
        EXPECT_TRUE(resolve_service_identity(policy, accounts, identity));
        policy.service_privileges = 4;
        EXPECT_FALSE(resolve_service_identity(policy, accounts, identity));
    }
    for (auto value : {"journal-read", "network-admin", "journal-read,network-admin", "network-admin,journal-read"}) {
        auto input = replace_once(std::string(VALID_MANIFEST), "service-privileges=none", std::string("service-privileges=") + value);
        input = replace_once(input, "no-new-privileges=no", "no-new-privileges=yes");
        ServiceManifest manifest{};
        ASSERT_TRUE(parse_service_manifest(std::span(input.data(), input.size()), manifest));
        ServiceTopology topology{};
        EXPECT_TRUE(validate_service_manifest(manifest, topology));
    }
}

TEST(InitServiceIdentity, DelegatedServiceFixturesHaveExactOperationCeilings) {
    auto const ROOT = std::filesystem::path(__FILE__).parent_path().parent_path().parent_path().parent_path();
    std::ifstream base(ROOT / "configs/wos-services.conf");
    std::ifstream extra(ROOT / "tests/fixtures/service_privilege_services.fragment");
    ASSERT_TRUE(base.good());
    ASSERT_TRUE(extra.good());
    std::string input((std::istreambuf_iterator<char>(base)), std::istreambuf_iterator<char>());
    input += std::string((std::istreambuf_iterator<char>(extra)), std::istreambuf_iterator<char>());
    ServiceManifest manifest{};
    ASSERT_TRUE(parse_service_manifest(std::span(input.data(), input.size()), manifest));
    ServiceTopology topology{};
    ASSERT_TRUE(validate_service_manifest(manifest, topology));
    ASSERT_EQ(manifest.service_count, 8U);
    wos::init::ServiceAccounts accounts{};
    ASSERT_TRUE(wos::init::parse_service_accounts(PASSWD_ACCOUNTS, GROUP_ACCOUNTS, accounts));
    for (size_t index : {6U, 7U}) {
        auto const& policy = manifest.services[index].identity;
        wos::init::ResolvedServiceIdentity identity{};
        ASSERT_TRUE(wos::init::resolve_service_identity(policy, accounts, identity));
        EXPECT_EQ(identity.uid, 100U);
        EXPECT_EQ(identity.service_privileges, index - 5);
        EXPECT_EQ(identity.authority_ceiling, wos::init::service_privilege_ceiling(index - 5));
        EXPECT_TRUE(identity.no_new_privileges);
    }
}

TEST(InitPrivateState, PreflightRejectsAllUnsafeMetadataBeforeOwnershipChanges) {
    using namespace wos::init;
    std::array<PrivateStateInode, 3> plan{{{1, 0, 0, 0100600, 1, false}, {2, 0, 0, 0100600, 1, false}, {3, 0, 0, 0040700, 2, true}}};
    for (unsigned variant = 0; variant < 8; ++variant) {
        auto bad = plan;
        if (variant == 0) bad[1].uid = 999;
        if (variant == 1) bad[1].gid = 999;
        if (variant == 2) bad[1].links = 2;
        if (variant == 3) bad[1].mode = 0120600;
        if (variant == 4) bad[1].mode = 0100644;
        if (variant == 5) bad[2].mode = 0040755;
        if (variant == 6) bad[2].directory = false;
        if (variant == 7) bad[0].fd = -1;
        unsigned writes = 0;
        EXPECT_EQ(change_private_state_owners(bad, 101, 101,
                                              [&](int, uint32_t, uint32_t) {
                                                  ++writes;
                                                  return 0;
                                              }),
                  EPERM);
        EXPECT_EQ(writes, 0U);
    }
    unsigned writes = 0;
    EXPECT_EQ(change_private_state_owners(plan, 0, 101,
                                          [&](int, uint32_t, uint32_t) {
                                              ++writes;
                                              return 0;
                                          }),
              EINVAL);
    EXPECT_EQ(writes, 0U);
}

TEST(InitPrivateState, MigrationPublishesDirectoryLastAndRollsBackFailedWrites) {
    using namespace wos::init;
    std::array<PrivateStateInode, 3> plan{{{1, 0, 0, 0100600, 1, false}, {2, 0, 0, 0100600, 1, false}, {3, 0, 0, 0040700, 2, true}}};
    for (int fail = 0; fail <= 3; ++fail) {
        std::array<uint32_t, 3> owners{};
        std::string order;
        auto change = [&](int fd, uint32_t uid, uint32_t gid) {
            EXPECT_EQ(uid, gid);
            order += static_cast<char>((uid == 0 ? 'a' : 'A') + fd - 1);
            if (uid != 0 && fd == fail) return EACCES;
            owners[fd - 1] = uid;
            return 0;
        };
        EXPECT_EQ(change_private_state_owners(plan, 101, 101, change), fail ? EACCES : 0);
        EXPECT_EQ(owners, (fail ? std::array<uint32_t, 3>{0, 0, 0} : std::array<uint32_t, 3>{101, 101, 101}));
        EXPECT_EQ(order, fail == 0 ? "ABC" : (fail == 1 ? "A" : (fail == 2 ? "ABa" : "ABCba")));
    }
    EXPECT_EQ(change_private_state_owners(plan, 101, 101, [](int fd, uint32_t uid, uint32_t) { return fd == 2 || uid == 0 ? EACCES : 0; }),
              EIO);
}

TEST(InitPrivateState, RetainedDirectoryRejectsUnexpectedLinksAndSpecialFiles) {
    using namespace wos::init;
    std::array<char, 64> path{};
    std::strcpy(path.data(), "/tmp/wos-init-private-state-XXXXXX");
    ASSERT_NE(::mkdtemp(path.data()), nullptr);
    std::filesystem::path base(path.data());
    int const DIR = ::open(path.data(), O_RDONLY | O_DIRECTORY);
    ASSERT_GE(DIR, 0);
    auto prepare = [&] {
        ::lseek(DIR, 0, SEEK_SET);
        return prepare_journal_state_at(DIR, ::getuid(), ::getgid());
    };
    EXPECT_EQ(prepare(), 0);
    int fd = ::openat(DIR, "wos.journal", O_CREAT | O_EXCL | O_WRONLY, 0600);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(::write(fd, "preserved", 9), 9);
    ::close(fd);
    EXPECT_EQ(prepare(), 0);
    ASSERT_EQ(::symlinkat("wos.journal", DIR, "wos.journal.1"), 0);
    EXPECT_NE(prepare(), 0);
    ASSERT_EQ(::unlinkat(DIR, "wos.journal.1", 0), 0);
    ASSERT_EQ(::linkat(DIR, "wos.journal", DIR, "wos.journal.1", 0), 0);
    EXPECT_EQ(prepare(), EPERM);
    ASSERT_EQ(::unlinkat(DIR, "wos.journal.1", 0), 0);
    ASSERT_EQ(::mkfifoat(DIR, "wos.journal.1", 0600), 0);
    EXPECT_EQ(prepare(), EPERM);
    ASSERT_EQ(::unlinkat(DIR, "wos.journal.1", 0), 0);
    fd = ::openat(DIR, "unexpected", O_CREAT | O_EXCL | O_WRONLY, 0600);
    ASSERT_GE(fd, 0);
    ::close(fd);
    EXPECT_EQ(prepare(), EPERM);
    ASSERT_EQ(::unlinkat(DIR, "unexpected", 0), 0);
    EXPECT_EQ(prepare(), 0);
    std::ifstream content(base / "wos.journal");
    std::string text((std::istreambuf_iterator<char>(content)), std::istreambuf_iterator<char>());
    EXPECT_EQ(text, "preserved");
    ::close(DIR);
    std::filesystem::remove_all(base);
}

TEST(InitRuntimeState, PublishesSeedOnlyAfterCompletePreparation) {
    using namespace wos::init;
    std::array<char, 64> path{};
    std::strcpy(path.data(), "/tmp/wos-init-runtime-state-XXXXXX");
    ASSERT_NE(::mkdtemp(path.data()), nullptr);
    int const DIR = ::open(path.data(), O_RDONLY | O_DIRECTORY);
    ASSERT_GE(DIR, 0);
    std::string const SEED = "options timeout:2 attempts:4\n";
    auto prepare = [&](uint32_t uid) { return prepare_netd_state_at(DIR, uid, ::getgid(), std::span(SEED.data(), SEED.size())); };
    EXPECT_EQ(prepare(0), EINVAL);
    struct stat state{};
    EXPECT_EQ(::fstatat(DIR, "netd", &state, AT_SYMLINK_NOFOLLOW), -1);
    // A non-root host cannot transfer to a foreign UID. Failure must remove only
    // this attempt's fresh staging directory and leave publication absent.
    if (::getuid() != 0) {
        // A sandbox user namespace may not map the foreign UID, producing
        // EINVAL before the ordinary unprivileged EPERM ownership check.
        int const DENIED = prepare(::getuid() + 1);
        EXPECT_TRUE(DENIED == EPERM || DENIED == EINVAL);
        EXPECT_EQ(::fstatat(DIR, ".netd-init", &state, AT_SYMLINK_NOFOLLOW), -1);
        EXPECT_EQ(::fstatat(DIR, "netd", &state, AT_SYMLINK_NOFOLLOW), -1);
    }
    ASSERT_EQ(prepare(::getuid()), 0);
    ASSERT_EQ(::fstatat(DIR, "netd", &state, AT_SYMLINK_NOFOLLOW), 0);
    EXPECT_EQ(state.st_mode, S_IFDIR | 0755);
    EXPECT_EQ(state.st_uid, ::getuid());
    EXPECT_EQ(state.st_gid, ::getgid());
    ASSERT_EQ(::fstatat(DIR, "netd/resolv.conf", &state, AT_SYMLINK_NOFOLLOW), 0);
    EXPECT_EQ(state.st_mode, S_IFREG | 0644);
    EXPECT_EQ(state.st_uid, ::getuid());
    EXPECT_EQ(state.st_nlink, 1U);
    std::ifstream input(std::filesystem::path(path.data()) / "netd/resolv.conf");
    EXPECT_EQ(std::string(std::istreambuf_iterator<char>(input), {}), SEED);
    EXPECT_EQ(prepare(::getuid()), EEXIST);
    EXPECT_EQ(::fstatat(DIR, ".netd-init", &state, AT_SYMLINK_NOFOLLOW), -1);
    ::close(DIR);
    std::filesystem::remove_all(path.data());
}

TEST(InitRuntimeState, RejectsExistingNamespaceAndOversizedSeed) {
    using namespace wos::init;
    std::array<char, 64> path{};
    std::strcpy(path.data(), "/tmp/wos-init-runtime-denial-XXXXXX");
    ASSERT_NE(::mkdtemp(path.data()), nullptr);
    int const DIR = ::open(path.data(), O_RDONLY | O_DIRECTORY);
    ASSERT_GE(DIR, 0);
    std::string const SEED = "seed";
    auto prepare = [&] { return prepare_netd_state_at(DIR, ::getuid(), ::getgid(), std::span(SEED.data(), SEED.size())); };
    ASSERT_EQ(::symlinkat("sentinel", DIR, "netd"), 0);
    EXPECT_EQ(prepare(), EEXIST);
    ASSERT_EQ(::unlinkat(DIR, "netd", 0), 0);
    ASSERT_EQ(::mkdirat(DIR, ".netd-init", 0700), 0);
    EXPECT_EQ(prepare(), EEXIST);
    ASSERT_EQ(::unlinkat(DIR, ".netd-init", AT_REMOVEDIR), 0);
    std::array<char, 4097> oversized{};
    EXPECT_EQ(prepare_netd_state_at(DIR, ::getuid(), ::getgid(), oversized), EINVAL);
    EXPECT_EQ(prepare_netd_state_at(DIR, ::getuid(), ::getgid(), {}), EINVAL);
    EXPECT_TRUE(std::filesystem::is_empty(path.data()));
    ::close(DIR);
    std::filesystem::remove_all(path.data());
}

TEST(InitRuntimeState, DeclarationRequiresUniqueNarrowNetworkProfile) {
    auto const ROOT = std::filesystem::path(__FILE__).parent_path().parent_path().parent_path().parent_path();
    std::ifstream file(ROOT / "configs/wos-services.conf");
    std::string const INPUT(std::istreambuf_iterator<char>{file}, {});
    for (const char* replacement : {"unknown", "netd\nruntime-state=netd", "netd\nruntime-state=none"}) {
        auto input = replace_once(INPUT, "runtime-state=netd", std::string("runtime-state=") + replacement);
        ServiceManifest manifest{};
        EXPECT_FALSE(parse_service_manifest(std::span(input.data(), input.size()), manifest));
    }
    for (unsigned variant = 0; variant < 6; ++variant) {
        ServiceManifest manifest{};
        ASSERT_TRUE(parse_service_manifest(std::span(INPUT.data(), INPUT.size()), manifest));
        auto& netd = manifest.services[find_service(manifest, "netd")];
        if (variant == 0) netd.service_class = wos::init::ServiceClass::NORMAL;
        if (variant == 1) netd.identity.service_privileges = 3;
        if (variant == 2) netd.identity.authority_ceiling = 65535;
        if (variant == 3) netd.identity.no_new_privileges = false;
        if (variant == 4) netd.runtime_state = static_cast<wos::init::ServiceSpec::RuntimeState>(2);
        if (variant == 5) manifest.services[find_service(manifest, "httpd")].runtime_state = netd.runtime_state;
        ServiceTopology topology{};
        EXPECT_EQ(validate_service_manifest(manifest, topology).error, ManifestValidationError::INVALID_IDENTITY);
    }
}

TEST(InitPrivateState, DeclarationRequiresNarrowJournalProfile) {
    auto const ROOT = std::filesystem::path(__FILE__).parent_path().parent_path().parent_path().parent_path();
    std::ifstream file(ROOT / "configs/wos-services.conf");
    std::string const INPUT((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    for (auto replacement : {"root", "/var/log/journal", "journal,none"}) {
        auto input = replace_once(INPUT, "private-state=journal", std::string("private-state=") + replacement);
        ServiceManifest manifest{};
        EXPECT_EQ(parse_service_manifest(std::span(input.data(), input.size()), manifest).error, ManifestParseError::INVALID_VALUE);
    }
    for (unsigned variant = 0; variant < 5; ++variant) {
        ServiceManifest manifest{};
        ASSERT_TRUE(parse_service_manifest(std::span(INPUT.data(), INPUT.size()), manifest));
        auto& journal = manifest.services[0];
        if (variant == 0) journal.service_class = wos::init::ServiceClass::NORMAL;
        if (variant == 1) journal.identity.service_privileges = 3;
        if (variant == 2) journal.identity.authority_ceiling = 65535;
        if (variant == 3) journal.identity.no_new_privileges = false;
        if (variant == 4) journal.private_state = static_cast<wos::init::ServiceSpec::PrivateState>(2);
        ServiceTopology topology{};
        EXPECT_EQ(validate_service_manifest(manifest, topology).error, ManifestValidationError::INVALID_IDENTITY);
    }
}

TEST(InitPrivateState, KeygenNamespaceRejectsUnsafeEntriesWithoutChangingData) {
    using namespace wos::init;
    std::array<char, 64> path{};
    std::strcpy(path.data(), "/tmp/wos-init-keygen-state-XXXXXX");
    ASSERT_NE(::mkdtemp(path.data()), nullptr);
    std::filesystem::path base(path.data());
    int const DIR = ::open(path.data(), O_RDONLY | O_DIRECTORY);
    ASSERT_GE(DIR, 0);
    auto prepare = [&] {
        ::lseek(DIR, 0, SEEK_SET);
        return prepare_keygen_state_at(DIR, ::getuid(), ::getgid());
    };
    EXPECT_EQ(prepare(), 0);
    int fd = ::openat(DIR, "dropbear_rsa_host_key", O_CREAT | O_EXCL | O_WRONLY, 0600);
    ASSERT_GE(fd, 0);
    ASSERT_EQ(::write(fd, "synthetic-key", 13), 13);
    ::close(fd);
    EXPECT_EQ(prepare(), 0);
    ASSERT_EQ(::symlinkat("dropbear_rsa_host_key", DIR, "dropbear_rsa_host_key.pub"), 0);
    EXPECT_NE(prepare(), 0);
    ASSERT_EQ(::unlinkat(DIR, "dropbear_rsa_host_key.pub", 0), 0);
    ASSERT_EQ(::linkat(DIR, "dropbear_rsa_host_key", DIR, "dropbear_rsa_host_key.pub", 0), 0);
    EXPECT_EQ(prepare(), EPERM);
    ASSERT_EQ(::unlinkat(DIR, "dropbear_rsa_host_key.pub", 0), 0);
    ASSERT_EQ(::mkfifoat(DIR, "dropbear_rsa_host_key.pub", 0600), 0);
    EXPECT_EQ(prepare(), EPERM);
    ASSERT_EQ(::unlinkat(DIR, "dropbear_rsa_host_key.pub", 0), 0);
    ASSERT_EQ(::fchmodat(DIR, "dropbear_rsa_host_key", 0644, 0), 0);
    EXPECT_EQ(prepare(), EPERM);
    ASSERT_EQ(::fchmodat(DIR, "dropbear_rsa_host_key", 0600, 0), 0);
    fd = ::openat(DIR, "unexpected", O_CREAT | O_EXCL | O_WRONLY, 0600);
    ASSERT_GE(fd, 0);
    ::close(fd);
    EXPECT_EQ(prepare(), EPERM);
    ASSERT_EQ(::unlinkat(DIR, "unexpected", 0), 0);
    if (::getuid() != 0) {
        ASSERT_EQ(::fchmod(DIR, 0755), 0);
        EXPECT_EQ(prepare(), EPERM);
        ASSERT_EQ(::fchmod(DIR, 0700), 0);
    }
    EXPECT_EQ(prepare(), 0);
    struct stat state{};
    ASSERT_EQ(::fstatat(DIR, "dropbear_rsa_host_key", &state, AT_SYMLINK_NOFOLLOW), 0);
    EXPECT_EQ(state.st_uid, ::getuid());
    EXPECT_EQ(state.st_mode, S_IFREG | 0600);
    std::ifstream content(base / "dropbear_rsa_host_key");
    std::string text((std::istreambuf_iterator<char>(content)), std::istreambuf_iterator<char>());
    EXPECT_EQ(text, "synthetic-key");
    ::close(DIR);
    std::filesystem::remove_all(base);
}

TEST(InitPrivateState, KeygenRequiresOneNarrowOneshotProfile) {
    auto const ROOT = std::filesystem::path(__FILE__).parent_path().parent_path().parent_path().parent_path();
    std::ifstream file(ROOT / "configs/wos-services.conf");
    std::string const INPUT((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
    for (unsigned variant = 0; variant < 9; ++variant) {
        ServiceManifest manifest{};
        ASSERT_TRUE(parse_service_manifest(std::span(INPUT.data(), INPUT.size()), manifest));
        auto& keygen = manifest.services[1];
        ASSERT_EQ(keygen.private_state, wos::init::ServiceSpec::PrivateState::KEYGEN);
        if (variant == 0) keygen.type = ServiceType::DAEMON;
        if (variant == 1) keygen.service_class = ServiceClass::JOURNAL;
        if (variant == 2) keygen.identity.authority_ceiling = 1;
        if (variant == 3) keygen.identity.no_new_privileges = false;
        if (variant == 4) keygen.identity.service_privileges = 1;
        if (variant == 5) keygen.identity.supplementary_count = 1;
        if (variant == 6) keygen.identity.mask = 0022;
        if (variant == 7) keygen.environment_profile = wos::init::EnvironmentProfile::INIT;
        if (variant == 8) manifest.services[2] = keygen;
        ServiceTopology topology{};
        EXPECT_EQ(validate_service_manifest(manifest, topology).error, ManifestValidationError::INVALID_IDENTITY);
    }
}

}  // namespace

TEST(InitMountRetry, RetriesStaleLookupsUntilPublicationSucceeds) {
    unsigned calls = 0;
    unsigned pauses = 0;
    EXPECT_EQ(wos::init::retry_mount([&]() { return ++calls < 4 ? -EAGAIN : 0; },
                                     [&](unsigned attempt) {
                                         ++pauses;
                                         EXPECT_EQ(attempt, calls);
                                     }),
              0);
    EXPECT_EQ(calls, 4U);
    EXPECT_EQ(pauses, 3U);
}

TEST(InitMountRetry, PermanentErrorsAndSuccessNeverRetry) {
    for (int result : {0, -EACCES, -EPERM, -EBUSY, -ENOENT, -ENOMEM, -EIO, -ESHUTDOWN, 1}) {
        unsigned calls = 0;
        EXPECT_EQ(wos::init::retry_mount(
                      [&]() {
                          ++calls;
                          return result;
                      },
                      [](unsigned) { ADD_FAILURE() << "unexpected retry"; }),
                  result);
        EXPECT_EQ(calls, 1U);
    }
}

TEST(InitMountRetry, ExhaustionIsBoundedWithoutAPauseAfterTheFinalAttempt) {
    unsigned calls = 0;
    unsigned pauses = 0;
    EXPECT_EQ(wos::init::retry_mount(
                  [&]() {
                      ++calls;
                      return -EAGAIN;
                  },
                  [&](unsigned attempt) {
                      ++pauses;
                      EXPECT_EQ(attempt, calls);
                  }),
              -EAGAIN);
    EXPECT_EQ(calls, wos::init::MOUNT_MAX_ATTEMPTS);
    EXPECT_EQ(pauses, wos::init::MOUNT_MAX_ATTEMPTS - 1);
}

TEST(InitMountRetry, PermanentDenialAfterChurnStopsImmediately) {
    unsigned calls = 0;
    unsigned pauses = 0;
    EXPECT_EQ(wos::init::retry_mount([&]() { return ++calls < 3 ? -EAGAIN : -EACCES; }, [&](unsigned) { ++pauses; }), -EACCES);
    EXPECT_EQ(calls, 3U);
    EXPECT_EQ(pauses, 2U);
}

TEST(InitSupervisor, MountOptionsKeepOrderedReadonlyPolicy) {
    wos::init::MountOptions parsed;
    ASSERT_EQ(wos::init::parse_mount_options("defaults,ro,nosuid,size=16M", parsed), 0);
    EXPECT_EQ(parsed.flags, MS_RDONLY | MS_NOSUID);
    EXPECT_STREQ(parsed.data.data(), "defaults,size=16M");
    ASSERT_EQ(wos::init::parse_mount_options("ro,rw,nosuid,suid", parsed), 0);
    EXPECT_EQ(parsed.flags, 0UL);
    EXPECT_STREQ(parsed.data.data(), "");
    ASSERT_EQ(wos::init::parse_mount_options("rw,ro,suid,nosuid,ro=1,xro", parsed), 0);
    EXPECT_EQ(parsed.flags, MS_RDONLY | MS_NOSUID);
    EXPECT_STREQ(parsed.data.data(), "ro=1,xro");
    auto const before = parsed;
    EXPECT_EQ(wos::init::parse_mount_options(std::string(256, 'r'), parsed), -E2BIG);
    EXPECT_EQ(parsed.flags, before.flags);
    EXPECT_EQ(parsed.data, before.data);
}
