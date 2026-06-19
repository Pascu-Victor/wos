#include "env.h"

#include <pwd.h>
#include <unistd.h>

#include <array>
#include <cstring>

#ifdef WOS_USERSPACE_COVERAGE
#include <sys/vfs.h>
#endif

namespace {

constexpr char DEFAULT_HOSTNAME[] = "wos";
constexpr char HOME_PREFIX[] = "HOME=";
constexpr char HOSTNAME_PREFIX[] = "HOSTNAME=";
constexpr char USER_PREFIX[] = "USER=";

#ifdef WOS_USERSPACE_COVERAGE
constexpr char LLVM_PROFILE_FILE_PREFIX[] = "LLVM_PROFILE_FILE=";
constexpr char LLVM_PROFILE_FILE_PATTERN[] = "/tmp/wos-userland-coverage/%m-%p.profraw%c";
constexpr char LLVM_PROFILE_PARENT_DIR[] = "/tmp";
constexpr char LLVM_PROFILE_DIR[] = "/tmp/wos-userland-coverage";

extern "C" void __llvm_profile_set_filename(const char* name);

void ensure_profile_dir() {
    (void)ker::abi::vfs::mkdir(LLVM_PROFILE_PARENT_DIR, 0755);
    (void)ker::abi::vfs::mkdir(LLVM_PROFILE_DIR, 0755);
}
#endif

}  // namespace

auto make_init_env() -> InitEnv {
    InitEnv env{};
    std::array<char, 64> hostname{};
    size_t envc = 0;

    if (::gethostname(hostname.data(), hostname.size() - 1) != 0 || hostname.front() == '\0') {
        std::strcpy(hostname.data(), DEFAULT_HOSTNAME);
    }

    if (passwd* pw = ::getpwuid(::getuid()); pw != nullptr) {
        if (pw->pw_name != nullptr && pw->pw_name[0] != '\0') {
            std::strcpy(env.user.data(), USER_PREFIX);
            std::strcat(env.user.data(), pw->pw_name);
            env.envp.at(envc++) = env.user.data();
        }
        if (pw->pw_dir != nullptr && pw->pw_dir[0] != '\0') {
            std::strcpy(env.home.data(), HOME_PREFIX);
            std::strcat(env.home.data(), pw->pw_dir);
            env.envp.at(envc++) = env.home.data();
        }
    }

    std::strcpy(env.hostname.data(), HOSTNAME_PREFIX);
    std::strcat(env.hostname.data(), hostname.data());
    env.envp.at(envc++) = env.hostname.data();

#ifdef WOS_USERSPACE_COVERAGE
    ensure_profile_dir();
    std::strcpy(env.llvm_profile_file.data(), LLVM_PROFILE_FILE_PREFIX);
    std::strcat(env.llvm_profile_file.data(), LLVM_PROFILE_FILE_PATTERN);
    env.envp.at(envc++) = env.llvm_profile_file.data();
#endif

    env.envp.at(envc) = nullptr;
    return env;
}

void configure_init_coverage_profile() {
#ifdef WOS_USERSPACE_COVERAGE
    ensure_profile_dir();
    __llvm_profile_set_filename(LLVM_PROFILE_FILE_PATTERN);
#endif
}
