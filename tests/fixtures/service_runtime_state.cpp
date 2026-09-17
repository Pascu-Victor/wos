// Read-only qualification of the default service manifest in a disposable WOS VM.
#include <errno.h>
#include <grp.h>
#include <stdio.h>
#include <string.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <span>

namespace {
namespace abi = ker::abi::init_control;
unsigned checks{}, failures{};
void expect(bool ok, const char* label) {
    ++checks;
    if (!ok) {
        ++failures;
        printf("FAIL %s errno=%d\n", label, errno);
    }
}
struct Service {
    const char* name;
    unsigned id;
    bool network_consumer;
    uint64_t ceiling;
    uint64_t privileges;
    bool no_new_privileges;
};
constexpr std::array SERVICES{Service{"journald", 101, false, 16384, 1, true}, Service{"netd", 102, false, 4096, 2, true},
                              Service{"httpd", 100, true, 0, 0, true}, Service{"dropbear", 0, true, 65535, 0, false}};

void identity(const abi::ServiceStatus& service, const Service& expected, std::span<const gid_t> groups = {}) {
    unsigned const ID = expected.id;
    char path[80];
    snprintf(path, sizeof(path), "/proc/%lld/status", static_cast<long long>(service.pid));
    FILE* file = fopen(path, "r");
    expect(file != nullptr, "active service has process status");
    if (!file) return;
    char line[512];
    bool uid = false, gid = false;
    unsigned authority_fields = 0;
    std::array<char, 128> group_line{};
    size_t group_offset = snprintf(group_line.data(), group_line.size(), "Groups:\t");
    for (size_t i = 0; i < groups.size(); ++i) {
        int const WRITTEN = snprintf(group_line.data() + group_offset, group_line.size() - group_offset, "%s%u", i ? " " : "", groups[i]);
        if (WRITTEN < 0 || static_cast<size_t>(WRITTEN) >= group_line.size() - group_offset) {
            fclose(file);
            return expect(false, "bounded group expectation");
        }
        group_offset += static_cast<size_t>(WRITTEN);
    }
    if (group_offset + 1 >= group_line.size()) {
        fclose(file);
        return expect(false, "bounded group newline");
    }
    group_line[group_offset] = '\n';
    while (fgets(line, sizeof(line), file)) {
        auto field = [&](const char* name, uint64_t expected_value, unsigned bit) {
            size_t const LENGTH = strlen(name);
            if (strncmp(line, name, LENGTH)) return;
            unsigned long long value{};
            char extra{};
            bool const VALID = sscanf(line + LENGTH, "%llu %c", &value, &extra) == 1;
            expect(!(authority_fields & bit) && VALID && value == expected_value, name);
            authority_fields |= bit;
            printf("AUTH service=%s %s %llu\n", service.service, name, value);
        };
        field("NoNewPrivs:", expected.no_new_privileges, 1);
        field("WosAuthorityCeiling:", expected.ceiling, 2);
        field("WosServicePrivileges:", expected.privileges, 4);
        field("WosServicePrivilegesLocked:", expected.no_new_privileges, 8);
        if (!strncmp(line, "Groups:", 7)) {
            expect(!(authority_fields & 16) && !strcmp(line, group_line.data()), "supplementary groups match declaration");
            authority_fields |= 16;
            printf("AUTH service=%s %s", service.service, line);
        }
        unsigned real{}, effective{}, saved{}, fs{};
        if (sscanf(line, "Uid:\t%u %u %u %u", &real, &effective, &saved, &fs) == 4) {
            uid = real == ID && effective == ID && saved == ID && fs == ID;
            printf("IDS service=%s uid=%u,%u,%u,%u\n", service.service, real, effective, saved, fs);
        }
        if (sscanf(line, "Gid:\t%u %u %u %u", &real, &effective, &saved, &fs) == 4) {
            gid = real == ID && effective == ID && saved == ID && fs == ID;
            printf("IDS service=%s gid=%u,%u,%u,%u\n", service.service, real, effective, saved, fs);
        }
    }
    expect(!ferror(file), "complete process status read");
    fclose(file);
    expect(uid && gid, "active service real/effective/saved IDs match declaration");
    expect(authority_fields == 31, "all immutable credential fields are present once");
}

void credential_control() {
    fflush(stdout);
    pid_t const CHILD = fork();
    if (CHILD == 0) {
        alarm(20);
        checks = failures = 0;
        constexpr std::array<gid_t, 8> GROUPS{7, 17, 100, 101, 102, 1000, 65534, UINT32_MAX - 1};
        expect(setgroups(GROUPS.size(), GROUPS.data()) == 0, "owned child installs bounded supplementary groups");
        abi::ServiceStatus self{};
        self.pid = getpid();
        snprintf(self.service, sizeof(self.service), "credential-control");
        identity(self, Service{"credential-control", 0, false, 65535, 0, false}, GROUPS);
        using Control = ker::abi::process::CredentialControl;
        auto control = [](Control operation, uint64_t value) { return ker::process::prctl(static_cast<int>(operation), value, 0, 0, 0); };
        expect(control(Control::SET_NO_NEW_PRIVILEGES, 1) == 0, "owned child sets NNP");
        expect(control(Control::SET_SERVICE_PRIVILEGES, 0) == 0, "owned child seals empty service privileges");
        expect(control(Control::DROP_AUTHORITY_CEILING, 0) == 0, "owned child drops all authority");
        identity(self, Service{"credential-control", 0, false, 0, 0, true}, GROUPS);
        expect(control(Control::DROP_AUTHORITY_CEILING, 65535) == -EPERM, "dropped authority cannot be restored");
        expect(control(Control::SET_SERVICE_PRIVILEGES, 1) == -EPERM, "sealed empty service privileges cannot grow");
        identity(self, Service{"credential-control", 0, false, 0, 0, true}, GROUPS);
        printf("%s: %u owned credential transition checks, %u failed\n", failures ? "FAIL" : "PASS", checks, failures);
        fflush(stdout);
        _exit(failures ? 1 : 0);
    }
    int status{};
    pid_t waited = -1;
    if (CHILD > 0) {
        do {
            waited = waitpid(CHILD, &status, 0);
        } while (waited < 0 && errno == EINTR);
    }
    expect(CHILD > 0 && waited == CHILD && WIFEXITED(status) && WEXITSTATUS(status) == 0, "owned credential control normally reaped");
}

void inode(const char* path, unsigned uid, unsigned gid, unsigned mode) {
    struct stat s{};
    int result = lstat(path, &s);
    printf("INODE path=%s result=%d uid=%u gid=%u mode=%06o\n", path, result, s.st_uid, s.st_gid, s.st_mode);
    expect(result == 0 && s.st_uid == uid && s.st_gid == gid && s.st_mode == mode, path);
}
}  // namespace

int main(int argc, char** argv) {
    if (argc != 3 || (strcmp(argv[1], "lan") && strcmp(argv[1], "wki"))) return 2;
    // Remote execution may forward persistent paths home. Inspect this node.
    if (ker::abi::vfs::wki_rule_add_vfs("/", ker::abi::vfs::WKI_VFS_ROUTE_LOCAL)) return 3;
    char hostname[256]{};
    expect(gethostname(hostname, sizeof(hostname) - 1) == 0 && !strcmp(hostname, argv[2]), "inspection executes on intended node");
    printf("HOST %s\n", hostname);
    credential_control();
    bool lan = !strcmp(argv[1], "lan");
    abi::StatusSnapshot before{}, after{};
    if (ker::process::init_status_read(&before) != 0 || before.size != sizeof(before) || before.version != abi::ABI_VERSION ||
        before.service_count > abi::MAX_STATUS_SERVICES)
        return 4;
    expect(before.service_count == 6, "default manifest contains six services");
    unsigned found = 0;
    bool testd = false, keygen = false;
    for (unsigned i = 0; i < before.service_count; ++i) {
        const auto& row = before.services[i];
        if (!memchr(row.service, 0, sizeof(row.service))) return 5;
        printf("SERVICE name=%s state=%u readiness=%u pid=%lld generation=%llu\n", row.service, static_cast<unsigned>(row.state),
               static_cast<unsigned>(row.readiness), static_cast<long long>(row.pid), static_cast<unsigned long long>(row.generation));
        if (!strcmp(row.service, "testd")) {
            testd = true;
            expect(row.state == abi::ServiceState::DISABLED && row.pid <= 0, "testd is disabled");
        }
        if (!strcmp(row.service, "dropbear-keygen")) {
            keygen = true;
            expect((row.state == abi::ServiceState::DISABLED || row.state == abi::ServiceState::EXITED) && row.pid <= 0,
                   "key generation completed or existing key suppressed launch");
        }
        for (const auto& expected : SERVICES) {
            if (strcmp(row.service, expected.name)) continue;
            ++found;
            bool running = row.state == abi::ServiceState::RUNNING && row.pid > 1;
            // netd runs while waiting for an address; its consumers remain waiting.
            bool ready = row.readiness == abi::ReadinessState::READY ||
                         (!lan && !strcmp(row.service, "netd") && row.readiness == abi::ReadinessState::NOT_READY);
            expect((running && ready) || (!lan && expected.network_consumer && row.state == abi::ServiceState::WAITING && row.pid <= 0),
                   "service runs or waits for declared network readiness");
            if (running) identity(row, expected);
        }
    }
    expect(found == SERVICES.size() && testd && keygen, "all default service roles present");
    inode("/", 0, 0, S_IFDIR | 0755);
    inode("/etc", 0, 0, S_IFDIR | 0755);
    inode("/etc/passwd", 0, 0, S_IFREG | 0644);
    inode("/etc/group", 0, 0, S_IFREG | 0644);
    inode("/etc/wos-services.conf", 0, 0, S_IFREG | 0644);
    inode("/etc/netd-resolv.conf", 0, 0, S_IFREG | 0644);
    inode("/etc/resolv.conf", 0, 0, S_IFLNK | 0777);
    char target[128]{};
    auto length = readlink("/etc/resolv.conf", target, sizeof(target));
    constexpr char TARGET[] = "/run/netd/resolv.conf";
    expect(length == sizeof(TARGET) - 1 && !memcmp(target, TARGET, sizeof(TARGET) - 1), "resolver symlink names netd-owned state");
    inode("/run", 0, 0, S_IFDIR | 0755);
    inode("/run/netd", 102, 102, S_IFDIR | 0755);
    inode("/run/netd/resolv.conf", 102, 102, S_IFREG | 0644);
    inode("/var/log/journal", 101, 101, S_IFDIR | 0700);
    inode("/var/log/journal/wos.journal", 101, 101, S_IFREG | 0600);
    inode("/etc/dropbear", 103, 103, S_IFDIR | 0700);
    inode("/etc/dropbear/dropbear_rsa_host_key", 103, 103, S_IFREG | 0600);
    inode("/root/.ssh", 0, 0, S_IFDIR | 0700);
    inode("/root/.ssh/authorized_keys", 0, 0, S_IFREG | 0600);
    expect(ker::process::init_status_read(&after) == 0 && after.service_count == before.service_count, "final service status is available");
    for (unsigned i = 0; i < before.service_count; ++i) {
        const auto& a = before.services[i];
        const auto& b = after.services[i];
        expect(!memcmp(a.service, b.service, sizeof(a.service)) && a.pid == b.pid && a.generation == b.generation && a.state == b.state,
               "service generation stable during inspection");
    }
    printf("%s: %u service identity and ownership checks, %u failed\n", failures ? "FAIL" : "PASS", checks, failures);
    return failures ? 1 : 0;
}
