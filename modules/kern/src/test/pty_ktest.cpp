#include <array>
#include <cerrno>
#include <cstdio>
#include <dev/pty.hpp>
#include <platform/mm/virt.hpp>
#include <test/ktest.hpp>
#include <vfs/file.hpp>
#include <vfs/fs/devfs.hpp>

namespace {

auto close_file(ker::vfs::File* file) -> int {
    if (file == nullptr || file->fops == nullptr || file->fops->vfs_close == nullptr) {
        return -1;
    }
    int const RESULT = file->fops->vfs_close(file);
    delete file;
    return RESULT;
}

auto close_and_clear(ker::vfs::File*& file) -> bool {
    auto* closing = file;
    file = nullptr;
    return closing == nullptr || close_file(closing) == 0;
}

auto query_and_unlock_ptmx(ker::vfs::File* master, int& pty_number, bool& identity_is_vmap) -> bool {
    if (master == nullptr) {
        return false;
    }

    const void* identity = ker::dev::pty::pty_file_identity_key(master);
    identity_is_vmap = ker::mod::mm::virt::kernel_vmap_contains(identity);
    int unlock = 0;
    int const QUERY_RESULT = ker::vfs::devfs::devfs_ioctl(master, ker::dev::pty::TIOCGPTN, reinterpret_cast<unsigned long>(&pty_number));
    int const UNLOCK_RESULT = ker::vfs::devfs::devfs_ioctl(master, ker::dev::pty::TIOCSPTLCK, reinterpret_cast<unsigned long>(&unlock));
    return identity != nullptr && QUERY_RESULT == 0 && UNLOCK_RESULT == 0;
}

struct PtyLifetimeResult {
    bool completed = false;
    bool closes_succeeded = true;
    bool master_identity_is_vmap = false;
    bool slave_identity_is_vmap = false;
    bool pts_entry_was_visible = false;
    bool pts_entry_was_removed = false;
    int first_pty = -1;
    int second_pty = -1;
};

auto run_pty_lifetime_scenario() -> PtyLifetimeResult {
    PtyLifetimeResult result{};
    ker::vfs::File* master = nullptr;
    ker::vfs::File* first_slave = nullptr;
    ker::vfs::File* second_slave = nullptr;
    ker::vfs::File* pts_directory = nullptr;
    ker::vfs::File* next_master = nullptr;

    do {
        pts_directory = ker::vfs::devfs::devfs_open_path("/dev/pts", 0, 0);
        master = ker::vfs::devfs::devfs_open_path("/dev/ptmx", 2, 0);
        if (pts_directory == nullptr || !query_and_unlock_ptmx(master, result.first_pty, result.master_identity_is_vmap)) {
            break;
        }

        ker::vfs::DirEntry entry{};
        result.pts_entry_was_visible = pts_directory->fops != nullptr && pts_directory->fops->vfs_readdir != nullptr &&
                                       pts_directory->fops->vfs_readdir(pts_directory, &entry, 2) == 0;

        std::array<char, 32> slave_path{};
        std::snprintf(slave_path.data(), slave_path.size(), "/dev/pts/%d", result.first_pty);
        first_slave = ker::vfs::devfs::devfs_open_path(slave_path.data(), 2, 0);
        if (first_slave == nullptr) {
            break;
        }
        result.slave_identity_is_vmap = ker::mod::mm::virt::kernel_vmap_contains(ker::dev::pty::pty_file_identity_key(first_slave));
        if (!close_and_clear(master)) {
            result.closes_succeeded = false;
            break;
        }

        second_slave = ker::vfs::devfs::devfs_open_path(slave_path.data(), 2, 0);
        if (second_slave == nullptr) {
            break;
        }
        bool endpoint_closes_succeeded = close_and_clear(first_slave);
        endpoint_closes_succeeded = close_and_clear(second_slave) && endpoint_closes_succeeded;
        result.closes_succeeded = endpoint_closes_succeeded;
        if (!result.closes_succeeded) {
            break;
        }

        entry = {};
        result.pts_entry_was_removed = pts_directory->fops->vfs_readdir(pts_directory, &entry, 2) == -ENOENT;

        next_master = ker::vfs::devfs::devfs_open_path("/dev/ptmx", 2, 0);
        bool next_identity_is_vmap = false;
        if (!query_and_unlock_ptmx(next_master, result.second_pty, next_identity_is_vmap) || !next_identity_is_vmap) {
            break;
        }
        result.completed = true;
    } while (false);

    result.closes_succeeded = close_and_clear(master) && result.closes_succeeded;
    result.closes_succeeded = close_and_clear(first_slave) && result.closes_succeeded;
    result.closes_succeeded = close_and_clear(second_slave) && result.closes_succeeded;
    result.closes_succeeded = close_and_clear(pts_directory) && result.closes_succeeded;
    result.closes_succeeded = close_and_clear(next_master) && result.closes_succeeded;
    return result;
}

}  // namespace

KTEST(PTY, VmapBackedMasterAndSlaveSurviveCloseOrdering) {
    PtyLifetimeResult const RESULT = run_pty_lifetime_scenario();
    KEXPECT_TRUE(RESULT.completed);
    KEXPECT_TRUE(RESULT.closes_succeeded);
    KEXPECT_TRUE(RESULT.master_identity_is_vmap);
    KEXPECT_TRUE(RESULT.slave_identity_is_vmap);
    KEXPECT_TRUE(RESULT.pts_entry_was_visible);
    KEXPECT_TRUE(RESULT.pts_entry_was_removed);
    KEXPECT_EQ(RESULT.second_pty, RESULT.first_pty);
}
