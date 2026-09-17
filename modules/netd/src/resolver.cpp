#include "netd/resolver.hpp"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <cerrno>

#include "netd/log.hpp"

namespace netd {
namespace {
auto open_resolver_directory() -> int {
    constexpr int FLAGS = O_RDONLY | O_DIRECTORY | O_NOFOLLOW | O_CLOEXEC;
    int parent = ::open("/", FLAGS);
    if (parent < 0) return -errno;
    for (const char* component : {"run", "netd"}) {
        struct stat state{};
        int error = ::fstat(parent, &state) != 0 ? errno : 0;
        if (error == 0 && (!S_ISDIR(state.st_mode) || state.st_uid != 0 || state.st_gid != 0 || (state.st_mode & 0022) != 0)) error = EPERM;
        int const CHILD = error == 0 ? ::openat(parent, component, FLAGS) : -1;
        if (CHILD < 0 && error == 0) error = errno;
        ::close(parent);
        if (error != 0) return -error;
        parent = CHILD;
    }
    return parent;
}
}  // namespace

void write_resolv_conf(const DhcpLease& lease) {
    int const DIRECTORY = open_resolver_directory();
    int const ERROR = DIRECTORY < 0 ? -DIRECTORY : write_resolv_conf_at(DIRECTORY, lease);
    if (DIRECTORY >= 0) ::close(DIRECTORY);
    if (ERROR != 0) logger::warn("netd: failed to update /run/netd/resolv.conf: %d", ERROR);
}
}  // namespace netd
