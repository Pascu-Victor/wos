#include <fcntl.h>
#include <sys/process.h>
#include <sys/wait.h>
#include <unistd.h>

#include <abi/compartment.hpp>
#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <initializer_list>
#include <utility>

namespace {
namespace wire = ker::abi::compartment;
auto number(const char* text, uint64_t& out, unsigned base = 10) -> bool {
    if (text == nullptr || *text == '\0') return false;
    if (base == 10 && std::strcmp(text, "infinity") == 0) {
        out = UINT64_MAX;
        return true;
    }
    if (base == 10 && text[0] == '0' && text[1] == 'x') {
        base = 16;
        text += 2;
        if (*text == '\0') return false;
    }
    uint64_t value = 0;
    for (; *text != '\0'; ++text) {
        unsigned digit;
        if (*text >= '0' && *text <= '9')
            digit = static_cast<unsigned>(*text - '0');
        else if (*text >= 'a' && *text <= 'f')
            digit = static_cast<unsigned>(*text - 'a') + 10;
        else if (*text >= 'A' && *text <= 'F')
            digit = static_cast<unsigned>(*text - 'A') + 10;
        else
            return false;
        if (digit >= base || value > (UINT64_MAX - digit) / base) return false;
        value = value * base + digit;
    }
    out = value;
    return true;
}
auto string(const char* text) -> wire::String { return {reinterpret_cast<uint64_t>(text), std::strlen(text)}; }
void help() {
    std::puts(
        "Usage: wos-compartment [OPTIONS] -- /service/path [ARG ...]\n"
        "  --asset SOURCE DEST MODE   Copy a readable regular File; MODE is octal\n"
        "  --fd SOURCE_FD DEST_FD     Grant an existing local regular File\n"
        "  --env NAME=VALUE           Supply one environment entry (default: empty)\n"
        "  --limit ID SOFT HARD       Lower an inherited limit (IDs 0..15)\n"
        "  --root-bytes BYTES         Imported data budget, whole pages, 4096..33554432\n"
        "  --ceiling MASK             Retain authority bits (default: 0)\n"
        "  --service-privileges MASK  Retain admitted service privileges (default: 0)\n"
        "  --share-network            Explicit shared Host networking (default: denied)\n"
        "  --dumpable                 Request admitted dumpability (default: disabled)\n"
        "  --detach                   Return after launch (default: supervise and wait)\n"
        "  --help, --version\n"
        "Spec version 1. Limits cannot exceed the creator. No ambient FDs, directory,\n"
        "device, socket or WKI-backed descriptor grants. Regular grants retain their\n"
        "existing rights, offset and shared mapping identity. Import the executable,\n"
        "its interpreter and libraries explicitly. At most 64 assets, 256 grants or\n"
        "entries per vector; combined arguments/pointers are bounded at 2MiB.");
}
struct Launch {
    wire::Spec spec;
    std::array<wire::Asset, wire::ASSET_MAX> assets{};
    std::array<const char*, wire::ASSET_MAX> sources{};
    std::array<wire::Descriptor, wire::DESCRIPTOR_MAX> descriptors{};
    std::array<wire::String, wire::VECTOR_MAX> argv{}, envp{};
    bool detached{};
    ~Launch() {
        for (size_t i = 0; i < spec.asset_count; ++i) static_cast<void>(close(assets[i].source_fd));
    }
    auto inherited_limits() -> bool {
        for (size_t i = 0; i < spec.limits.size(); ++i) {
            auto result =
                static_cast<int64_t>(syscall(ker::abi::callnums::process, static_cast<uint64_t>(ker::abi::process::procmgmt_ops::GETRLIMIT),
                                             i, reinterpret_cast<uint64_t>(&spec.limits[i]), uint64_t{1}));
            if (result < 0) {
                errno = static_cast<int>(-result);
                std::perror("wos-compartment: inherited limits");
                return false;
            }
        }
        return true;
    }
    auto parse(int argc, char** args) -> bool {
        for (int i = 1; i < argc; ++i) {
            auto const OPTION = args[i];
            auto available = [&](int count) { return argc - i > count; };
            if (std::strcmp(OPTION, "--") == 0) {
                if (!available(1) || argc - i - 1 > static_cast<int>(wire::VECTOR_MAX)) return false;
                spec.executable = string(args[i + 1]);
                for (++i; i < argc; ++i) argv[spec.argc++] = string(args[i]);
                break;
            }
            if (std::strcmp(OPTION, "--asset") == 0) {
                uint64_t mode;
                if (!available(3) || spec.asset_count == assets.size() || !number(args[i + 3], mode, 8) || mode > 0777) return false;
                int const FD = open(args[i + 1], O_RDONLY | O_CLOEXEC);
                if (FD < 0) {
                    std::perror(args[i + 1]);
                    return false;
                }
                sources[spec.asset_count] = args[i + 1];
                assets[spec.asset_count++] = {FD, static_cast<uint32_t>(mode), string(args[i + 2]), 0};
                i += 3;
            } else if (std::strcmp(OPTION, "--fd") == 0) {
                uint64_t source, destination;
                if (!available(2) || spec.descriptor_count == descriptors.size() || !number(args[i + 1], source) ||
                    !number(args[i + 2], destination) || source >= wire::DESCRIPTOR_MAX || destination >= wire::DESCRIPTOR_MAX)
                    return false;
                descriptors[spec.descriptor_count++] = {static_cast<int32_t>(source), static_cast<uint32_t>(destination), 0};
                i += 2;
            } else if (std::strcmp(OPTION, "--env") == 0) {
                if (!available(1) || spec.envc == envp.size()) return false;
                envp[spec.envc++] = string(args[++i]);
            } else if (std::strcmp(OPTION, "--limit") == 0) {
                uint64_t id, soft, hard;
                if (!available(3) || !number(args[i + 1], id) || id >= spec.limits.size() || !number(args[i + 2], soft) ||
                    !number(args[i + 3], hard) || soft > hard || soft > spec.limits[id].soft || hard > spec.limits[id].hard)
                    return false;
                spec.limits[id] = {soft, hard};
                i += 3;
            } else if (std::strcmp(OPTION, "--root-bytes") == 0) {
                if (!available(1) || !number(args[++i], spec.root_bytes) || spec.root_bytes < 4096 ||
                    spec.root_bytes > wire::ROOT_BYTES_MAX || spec.root_bytes % 4096 != 0)
                    return false;
            } else if (std::strcmp(OPTION, "--ceiling") == 0) {
                if (!available(1) || !number(args[++i], spec.authority_ceiling)) return false;
            } else if (std::strcmp(OPTION, "--service-privileges") == 0) {
                if (!available(1) || !number(args[++i], spec.service_privileges)) return false;
            } else if (std::strcmp(OPTION, "--share-network") == 0) {
                spec.network = wire::Network::SHARED_HOST;
            } else if (std::strcmp(OPTION, "--dumpable") == 0) {
                spec.dumpable = 1;
            } else if (std::strcmp(OPTION, "--detach") == 0) {
                detached = true;
            } else
                return false;
        }
        if (spec.asset_count == 0 || spec.argc == 0 || spec.executable.bytes >= wire::PATH_BYTES_MAX) return false;
        size_t budget = (spec.argc + spec.envc + 2) * sizeof(uint64_t);
        for (auto vector : {std::pair{argv.data(), spec.argc}, std::pair{envp.data(), spec.envc}}) {
            for (size_t i = 0; i < vector.second; ++i) {
                if (vector.first[i].bytes >= wire::ARGUMENT_BYTES_MAX - budget) return false;
                budget += vector.first[i].bytes + 1;
            }
        }
        spec.assets = reinterpret_cast<uint64_t>(assets.data());
        spec.descriptors = reinterpret_cast<uint64_t>(descriptors.data());
        spec.argv = reinterpret_cast<uint64_t>(argv.data());
        spec.envp = reinterpret_cast<uint64_t>(envp.data());
        return true;
    }
    void manifest(int64_t pid) const {
        std::printf("compartment spec=1 host-pid=%lld exec=%s network=%s ceiling=0x%llx root-bytes=%llu\n", static_cast<long long>(pid),
                    reinterpret_cast<const char*>(spec.executable.address),
                    spec.network == wire::Network::DENIED ? "denied" : "shared-host",
                    static_cast<unsigned long long>(spec.authority_ceiling), static_cast<unsigned long long>(spec.root_bytes));
        for (size_t i = 0; i < spec.asset_count; ++i)
            std::printf("asset fd=%d source=%s destination=%s mode=%03o copied\n", assets[i].source_fd, sources[i],
                        reinterpret_cast<const char*>(assets[i].destination.address), assets[i].mode);
        for (size_t i = 0; i < spec.descriptor_count; ++i)
            std::printf("grant source-fd=%d destination-fd=%u shared-file rights-and-offset\n", descriptors[i].source_fd,
                        descriptors[i].destination);
        std::printf("arguments=%llu environment=%llu dumpable=%u service-privileges=0x%llx\n", static_cast<unsigned long long>(spec.argc),
                    static_cast<unsigned long long>(spec.envc), spec.dumpable, static_cast<unsigned long long>(spec.service_privileges));
        for (size_t i = 0; i < spec.limits.size(); ++i)
            std::printf("limit id=%zu soft=%llu hard=%llu\n", i, static_cast<unsigned long long>(spec.limits[i].soft),
                        static_cast<unsigned long long>(spec.limits[i].hard));
        static_cast<void>(std::fflush(stdout));
    }
};
}  // namespace

auto main(int argc, char** argv) -> int {
    if (argc == 2 && std::strcmp(argv[1], "--help") == 0) {
        help();
        return 0;
    }
    if (argc == 2 && std::strcmp(argv[1], "--version") == 0) {
        std::puts("wos-compartment spec version 1");
        return 0;
    }
    Launch launch;
    if (!launch.inherited_limits()) return 1;
    if (!launch.parse(argc, argv)) {
        std::fputs("wos-compartment: invalid or incomplete bounded launch specification; use --help\n", stderr);
        return 2;
    }
    int64_t const PID =
        static_cast<int64_t>(syscall(ker::abi::callnums::process, wire::CREATE_OPERATION, reinterpret_cast<uint64_t>(&launch.spec)));
    if (PID < 0) {
        errno = static_cast<int>(-PID);
        std::perror("wos-compartment: create");
        return 1;
    }
    launch.manifest(PID);
    if (launch.detached) return 0;
    int status = 0;
    int64_t result;
    do {
        result = static_cast<int64_t>(syscall(ker::abi::callnums::process, static_cast<uint64_t>(ker::abi::process::procmgmt_ops::WAITPID),
                                              static_cast<uint64_t>(PID), reinterpret_cast<uint64_t>(&status), uint64_t{0}));
    } while (result == -EINTR);
    if (result < 0) {
        errno = static_cast<int>(-result);
        std::perror("wos-compartment: wait");
        return 1;
    }
    return WIFEXITED(status) ? WEXITSTATUS(status) : WIFSIGNALED(status) ? 128 + WTERMSIG(status) : 1;
}
