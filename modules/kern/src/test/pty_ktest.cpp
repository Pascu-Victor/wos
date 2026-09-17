#include <array>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <dev/pty.hpp>
#include <memory>
#include <platform/mm/virt.hpp>
#include <platform/sched/resource.hpp>
#include <platform/sched/session.hpp>
#include <platform/sched/task.hpp>
#include <platform/security/terminal.hpp>
#include <test/ktest.hpp>
#include <vfs/file.hpp>
#include <vfs/fs/devfs.hpp>
#include <vfs/vfs.hpp>

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
        pts_directory = ker::vfs::devfs::devfs_open_path(ker::mod::security::Snapshot::bootstrap_root(), "/dev/pts", 0, 0);
        master = ker::vfs::devfs::devfs_open_path(ker::mod::security::Snapshot::bootstrap_root(), "/dev/ptmx", 2, 0);
        if (pts_directory == nullptr || !query_and_unlock_ptmx(master, result.first_pty, result.master_identity_is_vmap)) {
            break;
        }

        ker::vfs::DirEntry entry{};
        result.pts_entry_was_visible = pts_directory->fops != nullptr && pts_directory->fops->vfs_readdir != nullptr &&
                                       pts_directory->fops->vfs_readdir(pts_directory, &entry, 2) == 0;

        std::array<char, 32> slave_path{};
        std::snprintf(slave_path.data(), slave_path.size(), "/dev/pts/%d", result.first_pty);
        first_slave = ker::vfs::devfs::devfs_open_path(ker::mod::security::Snapshot::bootstrap_root(), slave_path.data(), 2, 0);
        if (first_slave == nullptr) {
            break;
        }
        result.slave_identity_is_vmap = ker::mod::mm::virt::kernel_vmap_contains(ker::dev::pty::pty_file_identity_key(first_slave));
        if (!close_and_clear(master)) {
            result.closes_succeeded = false;
            break;
        }

        second_slave = ker::vfs::devfs::devfs_open_path(ker::mod::security::Snapshot::bootstrap_root(), slave_path.data(), 2, 0);
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

        next_master = ker::vfs::devfs::devfs_open_path(ker::mod::security::Snapshot::bootstrap_root(), "/dev/ptmx", 2, 0);
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

KTEST(PTY, SlavePublicationUsesOpeningSnapshotAndDeniesOtherUsers) {
    namespace security = ker::mod::security;
    namespace devfs = ker::vfs::devfs;
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto caller = ROOT;
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_GID, .real = 2000}, caller), 0);
    KREQUIRE_EQ(caller.prepare({.change = security::Change::SET_UID, .real = 1000}, caller), 0);
    auto other = ROOT;
    KREQUIRE_EQ(other.prepare({.change = security::Change::SET_GID, .real = 2001}, other), 0);
    KREQUIRE_EQ(other.prepare({.change = security::Change::SET_UID, .real = 1001}, other), 0);

    auto release = [](ker::vfs::File* file) { KEXPECT_EQ(close_file(file), 0); };
    using OwnedFile = std::unique_ptr<ker::vfs::File, decltype(release)>;
    OwnedFile master(devfs::devfs_open_path(caller, "/dev/ptmx", 2, 0), release);
    KREQUIRE_TRUE(master != nullptr);
    int number = -1;
    bool identity_is_vmap = false;
    KREQUIRE_TRUE(query_and_unlock_ptmx(master.get(), number, identity_is_vmap));
    std::array<char, 32> path{};
    // devfs_acquire_path takes a path relative to the devfs root.
    std::snprintf(path.data(), path.size(), "pts/%d", number);
    auto retained = devfs::devfs_acquire_path(path.data());
    KREQUIRE_TRUE(static_cast<bool>(retained));
    ker::vfs::Stat metadata{};
    KREQUIRE_EQ(devfs::devfs_stat_node(retained.get(), 0, &metadata), 0);
    KEXPECT_EQ(metadata.st_uid, 1000U);
    KEXPECT_EQ(metadata.st_gid, 2000U);
    KEXPECT_EQ(metadata.st_mode & 0777, 0600U);

    for (int flags : {0, 1, 2}) {
        int result = 123;
        OwnedFile denied(devfs::devfs_open_path(other, path.data(), flags, 0, &result), release);
        KEXPECT_TRUE(denied == nullptr);
        KEXPECT_EQ(result, -EACCES);
    }
    OwnedFile slave(devfs::devfs_open_path(caller, path.data(), 2, 0), release);
    KREQUIRE_TRUE(slave != nullptr);
    KEXPECT_EQ(devfs::devfs_chmod_node(retained.get(), other, 0666, 0), -EPERM);
    KEXPECT_EQ(devfs::devfs_chmod_node(retained.get(), caller, 0600, 0), 0);
    slave.reset();
    master.reset();

    // Reuse must publish fresh ownership, while a retained old inode continues
    // to describe its original endpoint rather than the recycled PTY number.
    master.reset(devfs::devfs_open_path(other, "/dev/ptmx", 2, 0));
    KREQUIRE_TRUE(master != nullptr);
    int reused = -1;
    KREQUIRE_TRUE(query_and_unlock_ptmx(master.get(), reused, identity_is_vmap));
    KEXPECT_EQ(reused, number);
    auto replacement = devfs::devfs_acquire_path(path.data());
    KREQUIRE_TRUE(static_cast<bool>(replacement));
    KEXPECT_TRUE(replacement.get() != retained.get());
    KREQUIRE_EQ(devfs::devfs_stat_node(replacement.get(), 0, &metadata), 0);
    KEXPECT_EQ(metadata.st_uid, 1001U);
    KEXPECT_EQ(metadata.st_gid, 2001U);
    KEXPECT_EQ(metadata.st_mode & 0777, 0600U);
    KREQUIRE_EQ(devfs::devfs_stat_node(retained.get(), 0, &metadata), 0);
    KEXPECT_EQ(metadata.st_uid, 1000U);
    KEXPECT_EQ(metadata.st_gid, 2000U);
}

KTEST(PTY, ControllingTerminalRejectsReusedAllocation) {
    namespace security = ker::mod::security;
    namespace devfs = ker::vfs::devfs;
    namespace pty = ker::dev::pty;
    auto release = [](ker::vfs::File* file) { KEXPECT_EQ(close_file(file), 0); };
    using OwnedFile = std::unique_ptr<ker::vfs::File, decltype(release)>;
    auto const ROOT = security::Snapshot::bootstrap_root();
    OwnedFile master(devfs::devfs_open_path(ROOT, "/dev/ptmx", 2, 0), release);
    int number = -1;
    bool vmap = false;
    KREQUIRE_TRUE(query_and_unlock_ptmx(master.get(), number, vmap));
    auto* pair = pty::pty_get(number);
    KREQUIRE_TRUE(pair != nullptr);
    auto const OLD = pair->identity;
    pty::pty_put(pair);
    master.reset();

    master.reset(devfs::devfs_open_path(ROOT, "/dev/ptmx", 2, 0));
    int reused = -1;
    KREQUIRE_TRUE(query_and_unlock_ptmx(master.get(), reused, vmap));
    KREQUIRE_EQ(reused, number);
    pair = pty::pty_get(reused);
    KREQUIRE_TRUE(pair != nullptr);
    auto const REPLACEMENT = pair->identity;
    KEXPECT_TRUE(REPLACEMENT != OLD);
    KEXPECT_EQ(REPLACEMENT.index(), OLD.index());
    KEXPECT_EQ(pair->slave_opened, 0);
    pty::pty_put(pair);
    auto* stale = pty::pty_open_controlling_slave(OLD, 1, 1);
    KEXPECT_TRUE(stale == nullptr);
    pair = pty::pty_get(reused);
    KREQUIRE_TRUE(pair != nullptr);
    KEXPECT_EQ(pair->slave_opened, 0);
    pty::pty_put(pair);

    // Early KTEST has no current process. Exercise publication on an isolated
    // Task; the native live fixture covers /dev/tty and scheduler delivery.
    ker::mod::sched::task::Task subject{};
    subject.set_controlling_terminal(OLD);
    KEXPECT_FALSE(security::may_receive_terminal_signal(REPLACEMENT, subject.controlling_terminal()));
    subject.set_controlling_terminal(REPLACEMENT);
    subject.clear_controlling_terminal_if(OLD);
    KEXPECT_TRUE(subject.controlling_terminal() == REPLACEMENT);
    KEXPECT_TRUE(security::may_receive_terminal_signal(REPLACEMENT, subject.controlling_terminal()));
    subject.clear_controlling_terminal_if(REPLACEMENT);
    KEXPECT_EQ(subject.controlling_terminal().value, 0U);
}

KTEST(PTY, TerminalIdentityBoundsAndSignalAuthority) {
    using ker::mod::security::may_receive_terminal_signal;
    using ker::mod::security::TerminalIdentity;
    auto const FIRST = TerminalIdentity::create(1, 0);
    auto const LAST = TerminalIdentity::create(TerminalIdentity::MAX_GENERATION, 255);
    KEXPECT_EQ(FIRST.index(), 0);
    KEXPECT_EQ(LAST.index(), 255);
    KEXPECT_EQ(LAST.value, UINT64_MAX);
    KEXPECT_EQ(TerminalIdentity::create(0, 0).value, 0U);
    KEXPECT_EQ(TerminalIdentity::create(1, 256).value, 0U);
    KEXPECT_EQ(TerminalIdentity::create(TerminalIdentity::MAX_GENERATION + 1, 0).value, 0U);
    KEXPECT_TRUE(may_receive_terminal_signal(FIRST, FIRST));
    KEXPECT_FALSE(may_receive_terminal_signal({}, {}));
    KEXPECT_FALSE(may_receive_terminal_signal(FIRST, {}));
    KEXPECT_FALSE(may_receive_terminal_signal(FIRST, TerminalIdentity::create(2, 0)));
    KEXPECT_FALSE(may_receive_terminal_signal(FIRST, TerminalIdentity::create(1, 1)));
}

KTEST(PTY, ForegroundRetainsGroupOriginAcrossNumericCollisions) {
    namespace session = ker::mod::sched::session;
    using ker::mod::security::TerminalIdentity;
    auto const TERMINAL = TerminalIdentity::create(31, 4);
    session::Origin const FOREIGN{.node = 0x7B42, .boot = 9};
    session::Identity const SELECTED{
        .session_id = 100, .pgid = 101, .terminal = TERMINAL, .terminal_generation = 7, .group_origin = FOREIGN};
    auto matches = [&](const session::Identity& candidate) {
        return session::terminal_foreground_matches(candidate, TERMINAL, 100, 7, 101, FOREIGN);
    };
    KEXPECT_TRUE(matches(SELECTED));
    auto candidate = SELECTED;
    candidate.group_origin = {};
    KEXPECT_FALSE(matches(candidate));  // A later local numeric alias cannot retarget selection.
    KEXPECT_TRUE(session::terminal_foreground_matches(candidate, TERMINAL, 100, 7, 101, {}));
    candidate = SELECTED;
    ++candidate.group_origin.boot;
    KEXPECT_FALSE(matches(candidate));
    candidate = SELECTED;
    ++candidate.group_origin.node;
    KEXPECT_FALSE(matches(candidate));
    candidate = SELECTED;
    candidate.origin = FOREIGN;
    KEXPECT_FALSE(matches(candidate));
    candidate = SELECTED;
    ++candidate.session_id;
    KEXPECT_FALSE(matches(candidate));
    candidate = SELECTED;
    ++candidate.pgid;
    KEXPECT_FALSE(matches(candidate));
    candidate = SELECTED;
    ++candidate.terminal_generation;
    KEXPECT_FALSE(matches(candidate));
    candidate = SELECTED;
    candidate.terminal = TerminalIdentity::create(32, 4);
    KEXPECT_FALSE(matches(candidate));
    candidate = SELECTED;
    candidate.terminal = TerminalIdentity::create(31, 5);
    KEXPECT_FALSE(matches(candidate));
    KEXPECT_FALSE(session::terminal_foreground_matches(SELECTED, {}, 100, 7, 101, FOREIGN));
    KEXPECT_FALSE(session::terminal_foreground_matches(SELECTED, TERMINAL, 0, 7, 101, FOREIGN));
    KEXPECT_FALSE(session::terminal_foreground_matches(SELECTED, TERMINAL, 100, 0, 101, FOREIGN));
    KEXPECT_FALSE(session::terminal_foreground_matches(SELECTED, TERMINAL, 100, 7, 0, FOREIGN));
}

KTEST(PTY, ExplicitOpenerKeepsInheritedTerminalAuthority) {
    namespace vfs = ker::vfs;
    namespace security = ker::mod::security;
    namespace session = ker::mod::sched::session;
    namespace pty = ker::dev::pty;
    using Task = ker::mod::sched::task::Task;
    auto const ROOT = security::Snapshot::bootstrap_root();
    auto release = [](vfs::File* file) { KEXPECT_EQ(close_file(file), 0); };
    std::unique_ptr<vfs::File, decltype(release)> master(vfs::devfs::devfs_open_path(ROOT, "/dev/ptmx", 2, 0), release);
    int number = -1;
    bool vmap = false;
    KREQUIRE_TRUE(query_and_unlock_ptmx(master.get(), number, vmap));
    auto release_pair = [](pty::PtyPair* pair) { pty::pty_put(pair); };
    std::unique_ptr<pty::PtyPair, decltype(release_pair)> pair(pty::pty_get(number), release_pair);
    KREQUIRE_TRUE(pair != nullptr);
    auto leader = std::make_unique<Task>();
    auto child = std::make_unique<Task>();
    auto no_terminal = std::make_unique<Task>();
    leader->pid = 0x96001;
    child->pid = 0x96002;
    no_terminal->pid = 0x96003;
    for (auto* task : {leader.get(), child.get(), no_terminal.get()}) {
        task->type = ker::mod::sched::task::TaskType::PROCESS;
        KREQUIRE_EQ(ker::mod::sched::resource::initialize_credentials(*task, ROOT), 0);
    }
    KREQUIRE_EQ(session::claim_terminal(*leader, *pair), 0);
    child->initialize_job_identity(leader->job_identity());
    auto const INHERITED = child->job_identity();
    auto const ENDPOINTS = pair->slave_opened;
    auto open_for = [&](Task& task, bool allowed) {
        int const FD = vfs::vfs_openat(&task, vfs::AT_FDCWD, "/dev/tty", 2, 0);
        if (allowed)
            KEXPECT_TRUE(FD >= 0);
        else
            KEXPECT_EQ(FD, -ENXIO);
        if (FD >= 0) {
            auto* file = vfs::vfs_get_file(&task, FD);
            KEXPECT_TRUE(pty::pty_file_identity_key(file) == &pair->slave_dev);
            KEXPECT_EQ(pair->slave_opened, ENDPOINTS + 1);
            vfs::Stat stat{};
            int stat_result = 0;
            KEXPECT_EQ(vfs::vfs_fstat_close_for_task(&task, FD, &stat, &stat_result), 0);
        }
        KEXPECT_TRUE(task.fd_table.empty());
        KEXPECT_EQ(task.fd_table.reserved_count(), 0U);
        KEXPECT_EQ(pair->slave_opened, ENDPOINTS);
    };
    // These tasks are deliberately unregistered and differ from the executing
    // KTEST task, as an unpublished spawn child differs from its parent.
    open_for(*child, true);
    open_for(*no_terminal, false);
    vfs::File sentinel{};
    vfs::File* output = &sentinel;
    KEXPECT_EQ(vfs::vfs_open_file_as(*child, ROOT, "/dev/tty", vfs::O_LOCAL, &output), -ENXIO);
    KEXPECT_TRUE(output == &sentinel);
    if (output != &sentinel) vfs::vfs_put_file(output);
    KEXPECT_EQ(pair->slave_opened, ENDPOINTS);

    KREQUIRE_EQ(session::detach_terminal(*leader, *pair), 0);
    KREQUIRE_EQ(session::claim_terminal(*leader, *pair), 0);
    auto const RECLAIMED = leader->job_identity();
    KEXPECT_NE(RECLAIMED.terminal_generation, INHERITED.terminal_generation);
    // A new live parent lease must not replace the child's stale inherited one.
    open_for(*child, false);
    KEXPECT_EQ(child->controlling_terminal().value, 0U);
    KEXPECT_TRUE(leader->controlling_terminal() == RECLAIMED.terminal);
    KEXPECT_EQ(leader->job_identity().terminal_generation, RECLAIMED.terminal_generation);
    child->initialize_job_identity(RECLAIMED);
    open_for(*child, true);
    KEXPECT_EQ(session::detach_terminal(*leader, *pair), 0);
}

KTEST(PTY, SessionAdmissionPreservesDeniedState) {
    namespace security = ker::mod::security;
    namespace session = ker::mod::sched::session;
    namespace pty = ker::dev::pty;
    using Task = ker::mod::sched::task::Task;
    auto release = [](ker::vfs::File* file) { KEXPECT_EQ(close_file(file), 0); };
    using OwnedFile = std::unique_ptr<ker::vfs::File, decltype(release)>;
    OwnedFile master(ker::vfs::devfs::devfs_open_path(security::Snapshot::bootstrap_root(), "/dev/ptmx", 2, 0), release);
    int number = -1;
    bool vmap = false;
    KREQUIRE_TRUE(query_and_unlock_ptmx(master.get(), number, vmap));
    auto release_pair = [](pty::PtyPair* pair) { pty::pty_put(pair); };
    std::unique_ptr<pty::PtyPair, decltype(release_pair)> pair(pty::pty_get(number), release_pair);
    KREQUIRE_TRUE(pair != nullptr);
    auto leader = std::make_unique<Task>();
    auto foreign = std::make_unique<Task>();
    auto member = std::make_unique<Task>();
    leader->type = foreign->type = member->type = ker::mod::sched::task::TaskType::PROCESS;
    leader->pid = 0x91001;
    session::Origin const GROUP_ORIGIN{.node = 0x7B42, .boot = 9};
    leader->initialize_job_identity({.session_id = leader->pid, .pgid = leader->pid, .group_origin = GROUP_ORIGIN});
    foreign->pid = 0x92001;
    member->pid = 0x91002;
    member->initialize_job_identity({.session_id = leader->pid, .pgid = leader->pid});
    KEXPECT_EQ(session::claim_terminal(*member, *pair), -EPERM);
    KEXPECT_EQ(pair->session_id, 0U);
    KEXPECT_EQ(member->controlling_terminal().value, 0U);
    KREQUIRE_EQ(session::claim_terminal(*leader, *pair), 0);
    KEXPECT_EQ(pair->session_id, leader->pid);
    KEXPECT_EQ(pair->foreground_pgrp, leader->pid);
    KEXPECT_TRUE(pair->foreground_origin == GROUP_ORIGIN);
    auto const BINDING = leader->controlling_terminal();
    uint64_t const GENERATION = leader->job_identity().terminal_generation;
    KEXPECT_NE(GENERATION, 0U);
    KEXPECT_TRUE(BINDING == pair->identity);
    KEXPECT_EQ(session::claim_terminal(*foreign, *pair), -EPERM);
    KEXPECT_EQ(foreign->controlling_terminal().value, 0U);
    KEXPECT_EQ(session::set_foreground(*foreign, *pair, leader->pid), -ENOTTY);
    KEXPECT_EQ(session::detach_terminal(*foreign, *pair), -ENOTTY);
    KEXPECT_EQ(session::set_foreground(*leader, *pair, -1), -EINVAL);
    KEXPECT_EQ(session::set_foreground(*leader, *pair, 0), -EINVAL);
    // Private fixtures are deliberately absent from the active registry.
    KEXPECT_EQ(session::set_foreground(*leader, *pair, 0x93001), -EPERM);
    KEXPECT_EQ(pair->foreground_pgrp, leader->pid);
    KEXPECT_TRUE(pair->foreground_origin == GROUP_ORIGIN);
    KEXPECT_EQ(pair->session_id, leader->pid);
    KEXPECT_TRUE(leader->controlling_terminal() == BINDING);
    KEXPECT_EQ(session::claim_terminal(*leader, *pair), 0);
    KEXPECT_EQ(pair->foreground_pgrp, leader->pid);
    KEXPECT_TRUE(pair->foreground_origin == GROUP_ORIGIN);
    member->set_controlling_terminal(BINDING, GENERATION);
    KEXPECT_EQ(session::detach_terminal(*member, *pair), 0);
    KEXPECT_EQ(member->controlling_terminal().value, 0U);
    KEXPECT_EQ(pair->session_id, leader->pid);
    KEXPECT_TRUE(leader->controlling_terminal() == BINDING);
    KEXPECT_EQ(session::detach_terminal(*leader, *pair), 0);
    KEXPECT_EQ(leader->controlling_terminal().value, 0U);
    KEXPECT_EQ(pair->session_id, 0U);
    KEXPECT_EQ(pair->foreground_pgrp, 0U);
    KEXPECT_TRUE(pair->foreground_origin.local());
    KEXPECT_FALSE(pty::pty_binding_is_live(BINDING, leader->pid, GENERATION));
    // An unpublished fork may retain the pre-detach snapshot. Reclaiming the
    // same allocation/SID must not revive that old attachment's authority.
    KEXPECT_EQ(session::claim_terminal(*leader, *pair), 0);
    KEXPECT_TRUE(leader->job_identity().terminal_generation > GENERATION);
    KEXPECT_FALSE(pty::pty_binding_is_live(BINDING, leader->pid, GENERATION));
    member->set_controlling_terminal(BINDING, GENERATION);
    KEXPECT_EQ(session::detach_terminal(*member, *pair), -ENOTTY);
    KEXPECT_EQ(session::set_foreground(*member, *pair, leader->pid), -ENOTTY);
    KEXPECT_FALSE(security::may_receive_terminal_signal(BINDING, pair->attachment_generation, BINDING, GENERATION));
    session::process_exit(*member);
    KEXPECT_EQ(pair->session_id, leader->pid);
    session::process_exit(*leader);
    KEXPECT_EQ(pair->session_id, 0U);
    KEXPECT_EQ(leader->controlling_terminal().value, 0U);
    KEXPECT_EQ(session::claim_terminal(*foreign, *pair), 0);
    KEXPECT_FALSE(pty::pty_binding_is_live(BINDING, leader->pid, GENERATION));
    KEXPECT_TRUE(pty::pty_binding_is_live(BINDING, foreign->pid, foreign->job_identity().terminal_generation));
    session::process_exit(*foreign);
    uint64_t const IRQF = pair->lock.lock_irqsave();
    pair->attachment_generation = UINT64_MAX;
    pair->lock.unlock_irqrestore(IRQF);
    KEXPECT_EQ(session::claim_terminal(*foreign, *pair), -EOVERFLOW);
    KEXPECT_EQ(pair->attachment_generation, UINT64_MAX);
    KEXPECT_EQ(pair->session_id, 0U);
    KEXPECT_EQ(foreign->controlling_terminal().value, 0U);
    // The File's last close invokes revocation while the retained pair is live.
    master.reset();
    KEXPECT_EQ(pair->master_opened, 0);
    KEXPECT_EQ(session::claim_terminal(*foreign, *pair), -EIO);
}

KTEST(PTY, LastMasterCloseRevokesAttachment) {
    namespace security = ker::mod::security;
    namespace session = ker::mod::sched::session;
    namespace pty = ker::dev::pty;
    auto release = [](ker::vfs::File* file) { KEXPECT_EQ(close_file(file), 0); };
    std::unique_ptr<ker::vfs::File, decltype(release)> master(
        ker::vfs::devfs::devfs_open_path(security::Snapshot::bootstrap_root(), "/dev/ptmx", 2, 0), release);
    int number = -1;
    bool vmap = false;
    KREQUIRE_TRUE(query_and_unlock_ptmx(master.get(), number, vmap));
    auto release_pair = [](pty::PtyPair* pair) { pty::pty_put(pair); };
    std::unique_ptr<pty::PtyPair, decltype(release_pair)> pair(pty::pty_get(number), release_pair);
    KREQUIRE_TRUE(pair != nullptr);
    auto leader = std::make_unique<ker::mod::sched::task::Task>();
    leader->type = ker::mod::sched::task::TaskType::PROCESS;
    leader->pid = 0x95001;
    KREQUIRE_EQ(session::claim_terminal(*leader, *pair), 0);
    auto const JOB = leader->job_identity();
    KEXPECT_TRUE(pty::pty_binding_is_live(JOB.terminal, JOB.session_id, JOB.terminal_generation));
    master.reset();
    KEXPECT_EQ(pair->session_id, 0U);
    KEXPECT_EQ(pair->foreground_pgrp, 0U);
    KEXPECT_TRUE(pair->foreground_origin.local());
    KEXPECT_EQ(pair->master_opened, 0);
    KEXPECT_FALSE(pty::pty_binding_is_live(JOB.terminal, JOB.session_id, JOB.terminal_generation));
    KEXPECT_EQ(session::claim_terminal(*leader, *pair), -EIO);
    // The private owner is outside the registry; exit still clears its store.
    session::process_exit(*leader);
    KEXPECT_EQ(leader->controlling_terminal().value, 0U);
}

KTEST(PTY, ProcessGroupAdmissionPolicy) {
    using ker::mod::security::authorize_process_group_change;
    KEXPECT_EQ(authorize_process_group_change(20, 10, {20, 10, 1, true}), 0);
    KEXPECT_EQ(authorize_process_group_change(20, 10, {21, 10, 20, false}), 0);
    KEXPECT_EQ(authorize_process_group_change(20, 10, {21, 10, 20, true}), -EACCES);
    KEXPECT_EQ(authorize_process_group_change(20, 10, {21, 10, 22, false}), -EPERM);
    KEXPECT_EQ(authorize_process_group_change(20, 10, {21, 11, 20, false}), -EPERM);
    KEXPECT_EQ(authorize_process_group_change(20, 10, {10, 10, 20, false}), -EPERM);
    KEXPECT_EQ(authorize_process_group_change(10, 10, {10, 10, 1, true}), -EPERM);
    KEXPECT_EQ(authorize_process_group_change(10, 10, {10, 10, 1, true, false}), 0);
}

KTEST(PTY, ImportedSessionAndGroupLeadershipAreIndependent) {
    namespace session = ker::mod::sched::session;
    using Task = ker::mod::sched::task::Task;
    Task subject{};
    subject.type = ker::mod::sched::task::TaskType::PROCESS;
    subject.pid = 0x96001;
    session::Origin const FOREIGN{.node = 0x7B42, .boot = 9};
    session::Identity const IMPORTED{.session_id = subject.pid, .pgid = subject.pid, .origin = FOREIGN, .group_origin = FOREIGN};
    subject.initialize_job_identity(IMPORTED);
    KEXPECT_FALSE(session::session_leader(subject.job_identity(), subject.pid));
    KEXPECT_FALSE(session::group_leader(subject.job_identity(), subject.pid));
    KEXPECT_EQ(session::create(subject), static_cast<int64_t>(subject.pid));
    KEXPECT_TRUE(subject.job_identity().origin.local());
    KEXPECT_TRUE(subject.job_identity().group_origin.local());
    KEXPECT_EQ(session::create(subject), static_cast<int64_t>(-EPERM));

    subject.initialize_job_identity(IMPORTED);
    KEXPECT_EQ(session::prepare_spawn_group(subject, 0), 0);
    KEXPECT_TRUE(subject.job_identity().origin == FOREIGN);
    KEXPECT_TRUE(subject.job_identity().group_origin.local());
    KEXPECT_FALSE(session::same_group(IMPORTED, subject.job_identity()));
    KEXPECT_TRUE(session::same_session(IMPORTED, subject.job_identity()));
    KEXPECT_EQ(session::create(subject), static_cast<int64_t>(-EPERM));
    // A new child inherits both origins but is not its parent's group leader.
    Task child{};
    child.type = subject.type;
    child.pid = subject.pid + 1;
    child.initialize_job_identity(subject.job_identity());
    KEXPECT_EQ(session::create(child), static_cast<int64_t>(child.pid));
    KEXPECT_TRUE(child.job_identity().origin.local() && child.job_identity().group_origin.local());
}

KTEST(PTY, ThreadsObserveOneProcessJobSnapshot) {
    using Task = ker::mod::sched::task::Task;
    Task process{};
    process.pid = 0x94001;
    process.type = ker::mod::sched::task::TaskType::PROCESS;
    Task first{};
    Task second{};
    first.type = second.type = process.type;
    first.is_thread = second.is_thread = true;
    first.owner_pid = second.owner_pid = process.pid;
    KREQUIRE_TRUE(ker::mod::sched::resource::bind_thread_owner(first, process));
    KREQUIRE_TRUE(ker::mod::sched::resource::bind_thread_owner(second, process));
    auto const TERMINAL = ker::mod::security::TerminalIdentity::create(42, 5);
    first.publish_job_identity({.session_id = 10, .pgid = 11, .terminal = TERMINAL});
    auto const SECOND = second.job_identity();
    KEXPECT_EQ(SECOND.session_id, 10U);
    KEXPECT_EQ(SECOND.pgid, 11U);
    KEXPECT_TRUE(SECOND.terminal == TERMINAL);
    second.mark_exec_completed();
    KEXPECT_TRUE(first.job_identity().exec_completed);
    second.clear_controlling_terminal_if(TERMINAL);
    KEXPECT_EQ(first.controlling_terminal().value, 0U);
    KEXPECT_EQ(process.controlling_terminal().value, 0U);
}
