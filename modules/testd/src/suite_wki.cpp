#include "testd.hpp"

TESTD_RUN(test_wki_target_policy_syscalls) {
    auto restore = [] { (void)ker::process::setwkitarget(nullptr, 0, 0); };

    restore();
    std::array<char, 64> hostname{};
    uint32_t flags = 0;
    int64_t rc = ker::process::getwkitarget(hostname.data(), hostname.size(), &flags);
    if (rc != 0 || flags != 0 || hostname[0] != '\0') {
        restore();
        fail("wki_target_clear_get", "clear target did not return auto policy");
        return;
    }
    TESTD_PASS("wki_target_clear_get");

    rc = ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL | ker::process::WKI_TARGET_FLAG_NOINHERIT);
    if (rc != 0) {
        restore();
        fail("wki_target_local_set", "local target set failed");
        return;
    }
    flags = 0;
    rc = ker::process::getwkitarget(hostname.data(), hostname.size(), &flags);
    if (rc != 0 || flags != (ker::process::WKI_TARGET_FLAG_LOCAL | ker::process::WKI_TARGET_FLAG_NOINHERIT) || hostname[0] != '\0') {
        restore();
        fail("wki_target_local_get", "local target flags did not round-trip");
        return;
    }
    TESTD_PASS("wki_target_local_roundtrip");

    rc = ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_BALANCED);
    if (rc != 0) {
        restore();
        fail("wki_target_balanced_set", "balanced target set failed");
        return;
    }
    flags = 0;
    rc = ker::process::getwkitarget(hostname.data(), hostname.size(), &flags);
    if (rc != 0 || flags != ker::process::WKI_TARGET_FLAG_BALANCED || hostname[0] != '\0') {
        restore();
        fail("wki_target_balanced_get", "balanced target flag did not round-trip");
        return;
    }
    TESTD_PASS("wki_target_balanced_roundtrip");

    constexpr std::string_view HOST = "testd-remote";
    constexpr auto HOST_LEN = static_cast<int64_t>(HOST.size());
    rc = ker::process::setwkitarget(HOST.data(), HOST.size(), ker::process::WKI_TARGET_FLAG_REMOTE | ker::process::WKI_TARGET_FLAG_STRICT);
    if (rc != 0) {
        restore();
        fail("wki_target_hostname_set", "hostname target set failed");
        return;
    }
    hostname.fill('\0');
    flags = 0;
    rc = ker::process::getwkitarget(hostname.data(), hostname.size(), &flags);
    if (rc != HOST_LEN || flags != (ker::process::WKI_TARGET_FLAG_REMOTE | ker::process::WKI_TARGET_FLAG_STRICT) ||
        std::strncmp(hostname.data(), HOST.data(), HOST.size()) != 0) {
        restore();
        fail("wki_target_hostname_get", "hostname target did not round-trip");
        return;
    }
    TESTD_PASS("wki_target_hostname_roundtrip");

    std::array<char, 4> small_hostname{};
    rc = ker::process::getwkitarget(small_hostname.data(), small_hostname.size(), nullptr);
    if (rc != -ENAMETOOLONG) {
        restore();
        fail("wki_target_small_buffer", "small hostname buffer was not rejected");
        return;
    }
    TESTD_PASS("wki_target_small_buffer");

    rc = ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL | ker::process::WKI_TARGET_FLAG_REMOTE);
    if (rc != -EINVAL) {
        restore();
        fail("wki_target_invalid_local_remote", "local+remote target flags were not rejected");
        return;
    }
    TESTD_PASS("wki_target_rejects_local_remote");

    rc = ker::process::setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_REMOTE | ker::process::WKI_TARGET_FLAG_BALANCED);
    if (rc != -EINVAL) {
        restore();
        fail("wki_target_invalid_remote_balanced", "remote+balanced target flags were not rejected");
        return;
    }
    TESTD_PASS("wki_target_rejects_remote_balanced");

    rc = ker::process::setwkitarget(HOST.data(), HOST.size(), ker::process::WKI_TARGET_FLAG_LOCAL);
    if (rc != -EINVAL) {
        restore();
        fail("wki_target_invalid_hostname_local", "hostname+local target was not rejected");
        return;
    }
    TESTD_PASS("wki_target_rejects_hostname_local");

    rc = ker::process::setwkitarget(HOST.data(), HOST.size(), ker::process::WKI_TARGET_FLAG_BALANCED);
    if (rc != -EINVAL) {
        restore();
        fail("wki_target_invalid_hostname_balanced", "hostname+balanced target was not rejected");
        return;
    }
    TESTD_PASS("wki_target_rejects_hostname_balanced");

    std::array<char, 64> too_long_hostname{};
    too_long_hostname.fill('x');
    rc = ker::process::setwkitarget(too_long_hostname.data(), too_long_hostname.size(), ker::process::WKI_TARGET_FLAG_REMOTE);
    if (rc != -ENAMETOOLONG) {
        restore();
        fail("wki_target_hostname_too_long", "oversized hostname was not rejected");
        return;
    }
    TESTD_PASS("wki_target_rejects_oversized_hostname");

    restore();
}
TESTD_RUN_END(test_wki_target_policy_syscalls)

TESTD_RUN(test_wki_vfs_rule_syscalls) {
    auto restore = [] { (void)ker::abi::vfs::wki_rule_clear_vfs(); };

    restore();
    std::array<char, 128> prefix{};
    uint32_t route = UINT32_MAX;
    int rc = ker::abi::vfs::wki_rule_get_vfs(0, prefix.data(), prefix.size(), &route);
    if (rc != -ENOENT) {
        restore();
        fail("wki_vfs_clear_empty", "clear did not leave task rule list empty");
        return;
    }
    TESTD_PASS("wki_vfs_clear_empty");

    rc = ker::abi::vfs::wki_rule_add_vfs("/tmp/testd-wki", WKI_VFS_ROUTE_HOST);
    if (rc != 0) {
        restore();
        fail("wki_vfs_add_host", "adding host route failed");
        return;
    }
    prefix.fill('\0');
    route = UINT32_MAX;
    rc = ker::abi::vfs::wki_rule_get_vfs(0, prefix.data(), prefix.size(), &route);
    if (rc <= 0 || route != WKI_VFS_ROUTE_HOST || std::strcmp(prefix.data(), "/tmp/testd-wki") != 0) {
        restore();
        fail("wki_vfs_get_host", "host route did not round-trip");
        return;
    }
    TESTD_PASS("wki_vfs_add_get_host");

    rc = ker::abi::vfs::wki_rule_add_vfs("/tmp/testd-wki", WKI_VFS_ROUTE_LOCAL);
    if (rc != 0) {
        restore();
        fail("wki_vfs_replace_local", "replacing route failed");
        return;
    }
    prefix.fill('\0');
    route = UINT32_MAX;
    rc = ker::abi::vfs::wki_rule_get_vfs(0, prefix.data(), prefix.size(), &route);
    if (rc <= 0 || route != WKI_VFS_ROUTE_LOCAL || std::strcmp(prefix.data(), "/tmp/testd-wki") != 0) {
        restore();
        fail("wki_vfs_get_local", "replacement route did not round-trip");
        return;
    }
    TESTD_PASS("wki_vfs_replace_get_local");

    std::array<char, 4> small_prefix{};
    rc = ker::abi::vfs::wki_rule_get_vfs(0, small_prefix.data(), small_prefix.size(), nullptr);
    if (rc != -ERANGE) {
        restore();
        fail("wki_vfs_small_buffer", "small prefix buffer was not rejected");
        return;
    }
    TESTD_PASS("wki_vfs_small_buffer");

    rc = ker::abi::vfs::wki_rule_add_vfs("/tmp/testd-wki-bad", 42);
    if (rc != -EINVAL) {
        restore();
        fail("wki_vfs_invalid_route", "invalid route was not rejected");
        return;
    }
    TESTD_PASS("wki_vfs_rejects_invalid_route");

    restore();
    rc = ker::abi::vfs::wki_rule_get_vfs(0, prefix.data(), prefix.size(), &route);
    if (rc != -ENOENT) {
        fail("wki_vfs_clear_final", "final clear did not empty task rules");
        return;
    }
    TESTD_PASS("wki_vfs_clear_final");
}
TESTD_RUN_END(test_wki_vfs_rule_syscalls)
