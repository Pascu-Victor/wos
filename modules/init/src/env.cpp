#include "env.h"

#include <pwd.h>
#include <unistd.h>

#include <cstring>

namespace {

constexpr char DEFAULT_HOSTNAME[] = "wos";
constexpr char HOME_PREFIX[] = "HOME=";
constexpr char HOSTNAME_PREFIX[] = "HOSTNAME=";
constexpr char USER_PREFIX[] = "USER=";

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
    env.envp.at(envc) = nullptr;
    return env;
}
