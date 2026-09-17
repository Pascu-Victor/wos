#include <errno.h>
#include <grp.h>
#include <stdio.h>
#include <string.h>
#include <sys/init_control.h>
#include <sys/process.h>
#include <sys/wait.h>
#include <unistd.h>

namespace {
namespace abi = ker::abi::init_control;
using Control = ker::abi::process::CredentialControl;
constexpr uint64_t INIT_BIT = uint64_t{1} << static_cast<unsigned>(ker::abi::process::CredentialAuthority::INIT_CONTROL);

bool control(Control op, uint64_t value) { return ker::process::prctl(static_cast<int>(op), value, 0, 0, 0) == 0; }
bool profile(unsigned kind) {
    switch (kind) {
        case 0:
            return true;
        case 1:
            return control(Control::DROP_AUTHORITY_CEILING, 0);
        case 2:
            return setgroups(0, nullptr) == 0 && setgid(2001) == 0 && setuid(1000) == 0 && control(Control::SET_NO_NEW_PRIVILEGES, 1) &&
                   control(Control::DROP_AUTHORITY_CEILING, 0);
        case 3:
            return control(Control::DROP_AUTHORITY_CEILING, INIT_BIT);
        case 4:
            return control(Control::DROP_AUTHORITY_CEILING, ker::abi::process::CREDENTIAL_AUTHORITY_ALL & ~INIT_BIT);
        case 5:
            return control(Control::SET_NO_NEW_PRIVILEGES, 1);
        case 6:
            return seteuid(1000) == 0;
        case 7:
            return control(Control::SET_NO_NEW_PRIVILEGES, 1) && control(Control::SET_SERVICE_PRIVILEGES, 3) &&
                   setgroups(0, nullptr) == 0 && setgid(2001) == 0 && setuid(1000) == 0;
    }
    return false;
}
bool zero(const void* bytes, size_t size) {
    const auto* data = static_cast<const unsigned char*>(bytes);
    for (size_t i = 0; i < size; ++i)
        if (data[i]) return false;
    return true;
}
bool name_valid(const char* name) {
    bool terminated = false;
    for (size_t i = 0; i < abi::SERVICE_NAME_CAPACITY; ++i) {
        char c = name[i];
        if (!c) {
            terminated = true;
            continue;
        }
        if (terminated ||
            !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_' || c == '.' || c == '@'))
            return false;
    }
    return name[0] && terminated;
}
bool status_valid(const abi::StatusSnapshot& s, uint64_t previous, const char* absent_service) {
    if (s.size != sizeof(s) || s.version != abi::ABI_VERSION || !s.sequence || s.sequence < previous ||
        s.service_count > abi::MAX_STATUS_SERVICES || static_cast<unsigned>(s.state) > 5 || (s.flags & ~abi::SNAPSHOT_FLAG_SHUTTING_DOWN) ||
        s.reserved0 || !zero(s.reserved, sizeof(s.reserved)))
        return false;
    for (unsigned i = 0; i < abi::MAX_STATUS_SERVICES; ++i) {
        const auto& row = s.services[i];
        if (i >= s.service_count) {
            if (!zero(&row, sizeof(row))) return false;
            continue;
        }
        if (!name_valid(row.service) || !strcmp(row.service, absent_service) || static_cast<unsigned>(row.state) > 8 ||
            static_cast<unsigned>(row.readiness) > 3 || (row.flags & ~7U) || row.pid < -1 || row.pgid < -1 || row.reserved0 ||
            row.reserved1 || !zero(row.reserved, sizeof(row.reserved)) || row.history_count > abi::MAX_TRANSITION_HISTORY)
            return false;
        for (unsigned j = 0; j < i; ++j)
            if (!strcmp(row.service, s.services[j].service)) return false;
        uint64_t timestamp = 0;
        for (unsigned j = 0; j < abi::MAX_TRANSITION_HISTORY; ++j) {
            const auto& h = row.history[j];
            if (j >= row.history_count) {
                if (!zero(&h, sizeof(h))) return false;
                continue;
            }
            if (static_cast<unsigned>(h.from_state) > 8 || static_cast<unsigned>(h.to_state) > 8 || static_cast<unsigned>(h.reason) > 9 ||
                h.reserved0 || h.timestamp_mono_ns < timestamp)
                return false;
            timestamp = h.timestamp_mono_ns;
        }
    }
    return true;
}
int check_profile(unsigned kind) {
    if (getpid() <= 1 || !profile(kind)) return 2;
    bool authorized = kind == 0 || kind == 3 || kind == 5;
    unsigned total = 0, failed = 0;
    auto check = [&](bool ok, const char* label, int64_t result, int expected) {
        ++total;
        if (!ok) {
            ++failed;
            printf("FAIL profile=%u case=%s result=%lld expected=%d\n", kind, label, static_cast<long long>(result), expected);
        }
    };
    abi::Request valid{};
    valid.size = sizeof(valid);
    valid.version = abi::ABI_VERSION;
    valid.action = abi::Action::START;
    valid.request_id = static_cast<uint64_t>(getpid()) << 8;
    snprintf(valid.service, sizeof(valid.service), "g19-control-absent-%ld", static_cast<long>(getpid()));
    for (unsigned field = 0; field < 13; ++field) {
        auto request = valid;
        switch (field) {
            case 0:
                --request.size;
                break;
            case 1:
                ++request.version;
                break;
            case 2:
                request.action = abi::Action::INVALID;
                break;
            case 3:
                request.action = static_cast<abi::Action>(999);
                break;
            case 4:
                request.flags = 1;
                break;
            case 5:
                request.request_id = 0;
                break;
            case 6:
                request.sender_pid = 1;
                break;
            case 7:
                request.sender_euid = 1;
                break;
            case 8:
                request.reserved0 = 1;
                break;
            case 9:
                request.reserved[2] = 1;
                break;
            case 10:
                request.service[0] = 0;
                break;
            case 11:
                request.service[0] = '/';
                break;
            case 12:
                memset(request.service, 'a', sizeof(request.service));
                break;
        }
        auto before = request;
        auto result = ker::process::init_control_submit(&request);
        int expected = authorized ? -EINVAL : -EPERM;
        check(result == expected && !memcmp(&before, &request, sizeof(request)), "invalid-submit", result, expected);
    }
    for (uintptr_t address = 0; address <= 1; ++address) {
        auto result = ker::process::init_control_submit(reinterpret_cast<const abi::Request*>(address));
        int expected = authorized ? -EFAULT : -EPERM;
        check(result == expected, "submit-pointer", result, expected);
    }
    for (unsigned pointer = 0; pointer < 3; ++pointer) {
        abi::Request request;
        memset(&request, 0xA5, sizeof(request));
        auto before = request;
        auto* dest = pointer == 2 ? &request : reinterpret_cast<abi::Request*>(static_cast<uintptr_t>(pointer));
        auto result = ker::process::init_control_receive(dest);
        check(result == -EPERM && !memcmp(&before, &request, sizeof(request)), "receive-role", result, -EPERM);
        abi::StatusSnapshot status;
        memset(&status, 0xA5, sizeof(status));
        auto old = status;
        auto* source = pointer == 2 ? &status : reinterpret_cast<abi::StatusSnapshot*>(static_cast<uintptr_t>(pointer));
        result = ker::process::init_status_publish(source);
        check(result == -EPERM && !memcmp(&old, &status, sizeof(status)), "publish-role", result, -EPERM);
    }
    for (uintptr_t address = 0; address <= 1; ++address) {
        auto result = ker::process::init_status_read(reinterpret_cast<abi::StatusSnapshot*>(address));
        check(result == -EFAULT, "public-read-pointer", result, -EFAULT);
    }
    uint64_t sequence = 0;
    bool safe_target = true;
    for (unsigned i = 0; i < 4; ++i) {
        abi::StatusSnapshot status;
        memset(&status, 0xA5, sizeof(status));
        auto result = ker::process::init_status_read(&status);
        bool ok = result == 0 && status_valid(status, sequence, valid.service);
        safe_target &= ok;
        check(ok, "public-status", result, 0);
        if (ok) sequence = status.sequence;
    }
    if (!safe_target) return 3;
    for (unsigned action = 1; action <= 3; ++action) {
        auto request = valid;
        request.action = static_cast<abi::Action>(action);
        request.request_id += action;
        request.service[sizeof(request.service) - 1] = 'x';  // kernel canonicalizes only its private copy
        auto before = request;
        auto result = ker::process::init_control_submit(&request);
        int expected = authorized ? 0 : -EPERM;
        check(result == expected && !memcmp(&before, &request, sizeof(request)), "valid-submit", result, expected);
        if (result == 0)
            printf("ACCEPT request=%llu pid=%ld service=%s\n", static_cast<unsigned long long>(request.request_id),
                   static_cast<long>(getpid()), valid.service);
    }
    printf("%s profile=%u checks=%u\n", failed ? "FAIL" : "PASS", kind, total);
    return failed || total != 30;
}
}  // namespace
int main() {
    for (unsigned kind = 0; kind < 8; ++kind) {
        pid_t child = fork();
        if (child == 0) {
            int result = check_profile(kind);
            fflush(stdout);
            _exit(result);
        }
        int status = 0;
        if (child < 0 || waitpid(child, &status, 0) != child || !WIFEXITED(status) || WEXITSTATUS(status)) return 1;
    }
    puts("PASS: 240 native init-control authority checks");
    return 0;
}
