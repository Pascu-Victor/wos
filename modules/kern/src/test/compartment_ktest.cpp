#include <array>
#include <cerrno>
#include <cstdio>
#include <initializer_list>
#include <memory>
#include <platform/sched/ipc_namespace.hpp>
#include <platform/sched/resource.hpp>
#include <platform/sched/task.hpp>
#include <platform/security/credentials.hpp>
#include <syscalls_impl/shm/shm.hpp>
#include <test/ktest.hpp>
#include <vfs/fs/tmpfs.hpp>
#include <vfs/mount.hpp>

namespace ipc = ker::mod::sched::ipc;
namespace resource = ker::mod::sched::resource;
using ker::mod::sched::task::Task;

KTEST(CompartmentIpc, ChildAndThreadUseExactProcessNamespace) {
    auto const BASELINE = ipc::live_namespace_count();
    {
        ipc::NamespaceRef scope;
        KREQUIRE_EQ(ipc::create_namespace(scope), 0);
        auto owner = std::make_unique<Task>();
        owner->pid = ker::mod::sched::task::get_next_pid();
        KREQUIRE_TRUE(owner->initialize_ipc_namespace(scope));
        auto thread = std::make_unique<Task>();
        thread->is_thread = true;
        thread->owner_pid = owner->pid;
        KREQUIRE_TRUE(resource::bind_thread_owner(*thread, *owner));
        KEXPECT_TRUE(thread->ipc_namespace() == owner->ipc_namespace());
        KEXPECT_FALSE(thread->initialize_ipc_namespace(scope));
        auto child = std::make_unique<Task>();
        KREQUIRE_TRUE(resource::inherit(*child, *thread));
        KEXPECT_TRUE(*child->ipc_namespace() == scope);
        KEXPECT_FALSE(child->initialize_ipc_namespace(scope));
        scope.reset();
        KEXPECT_FALSE(child->ipc_namespace()->is_host());
        thread.reset();
        owner.reset();
        KEXPECT_EQ(ipc::live_namespace_count(), BASELINE + 1);
    }
    KEXPECT_EQ(ipc::live_namespace_count(), BASELINE);
}

KTEST(CompartmentIpc, PublishedAndClosedInstallationFailsWithoutHostFallback) {
    ipc::NamespaceRef scope;
    KREQUIRE_EQ(ipc::create_namespace(scope), 0);
    auto task = std::make_unique<Task>();
    task->scheduler_published.store(true);
    KEXPECT_FALSE(task->initialize_ipc_namespace(scope));
    KEXPECT_TRUE(task->ipc_namespace()->is_host());
    task->scheduler_published.store(false);
    scope.close();
    KEXPECT_FALSE(task->initialize_ipc_namespace(scope));
    KEXPECT_TRUE(task->ipc_namespace()->is_host());
    auto thread = std::make_unique<Task>();
    thread->is_thread = true;
    KEXPECT_NULL(thread->ipc_namespace());
    auto child = std::make_unique<Task>();
    KEXPECT_FALSE(resource::inherit(*child, *thread));
}

KTEST(CompartmentIpc, SysvNamesAndGuessedIdsAreScopedBeforeEffects) { KEXPECT_TRUE(ker::syscall::shm::shm_selftest_namespace_isolation()); }

KTEST(CompartmentIpc, RetirementPreservesInheritedAttachmentsAndDrainsBacking) {
    KEXPECT_TRUE(ker::syscall::shm::shm_selftest_namespace_retirement());
}

namespace {
namespace vfs = ker::vfs;
namespace mount_ns = vfs::mount_ns;
namespace tmpfs = vfs::tmpfs;

struct PrivateMounts {
    mount_ns::NamespaceRef first, second;
    vfs::PreparedMount pending_first{}, pending_second{};
    ~PrivateMounts() {
        vfs::finish_prepared_mount(&pending_first);
        vfs::finish_prepared_mount(&pending_second);
        if (!first.is_host()) KEXPECT_EQ(vfs::retire_mount_namespace(first), 0);
        if (!second.is_host()) KEXPECT_EQ(vfs::retire_mount_namespace(second), 0);
    }
};
struct PutFile {
    void operator()(vfs::File* file) const { vfs::vfs_put_file(file); }
};
using NativeFile = std::unique_ptr<vfs::File, PutFile>;

auto native_root(vfs::MountPoint* mount) -> tmpfs::TmpNode* {
    return tmpfs::mount_root(static_cast<tmpfs::TmpfsMount*>(mount->private_data));
}
// Direct native backend fixture. Use real File/mount ownership, without
// claiming that Task pathwalk or public compartment creation is integrated.
auto native_file(vfs::MountPoint* mount, const char* path) -> NativeFile {
    auto* file = tmpfs::tmpfs_open_path(ker::mod::security::Snapshot::bootstrap_root(), native_root(mount), path, vfs::O_CREAT | 2, 0600);
    if (file != nullptr) {
        if (!vfs::retain_mount_for_open_file(mount)) {
            vfs::vfs_put_file(file);
            return {};
        }
        file->mount_owner = mount;
        file->mount_dev_id = mount->dev_id;
        file->mount_generation = vfs::mount_table_generation_snapshot();
    }
    return NativeFile(file);
}
auto private_mount(const mount_ns::NamespaceRef& scope, const char* path) -> int {
    return vfs::mount_filesystem(path, "tmpfs", nullptr, 0, "size=1M", nullptr, nullptr, scope);
}
auto scoped_mount(const mount_ns::NamespaceRef& scope, const char* path) -> vfs::MountRef {
    return vfs::find_mount_point(path, vfs::UNKNOWN_MOUNT_PATH_LEN, scope);
}
}  // namespace

KTEST(CompartmentMount, SamePathsHaveDistinctBackingAndCacheIdentity) {
    auto const NS_BASELINE = mount_ns::live_namespace_count();
    auto const HOST_COUNT = vfs::get_mount_count();
    auto host_root = vfs::find_mount_point("/");
    KREQUIRE_TRUE(host_root);
    {
        PrivateMounts views;
        KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
        KREQUIRE_EQ(mount_ns::create_namespace(views.second), 0);
        KREQUIRE_EQ(private_mount(views.first, "/"), 0);
        KREQUIRE_EQ(private_mount(views.second, "/"), 0);
        KREQUIRE_EQ(private_mount(views.first, "/service"), 0);
        KREQUIRE_EQ(private_mount(views.second, "/service"), 0);
        auto first = scoped_mount(views.first, "/service/data");
        auto second = scoped_mount(views.second, "/service/data");
        KREQUIRE_TRUE(first && second);
        KEXPECT_NE(first->dev_id, second->dev_id);
        KEXPECT_NE(native_root(first.get()), native_root(second.get()));
        auto first_root = scoped_mount(views.first, "/");
        auto second_root = scoped_mount(views.second, "/");
        KREQUIRE_TRUE(first_root && second_root);
        KEXPECT_NE(native_root(first_root.get()), tmpfs::get_root_node());
        KEXPECT_NE(native_root(second_root.get()), tmpfs::get_root_node());
        KEXPECT_NE(native_root(first_root.get()), native_root(second_root.get()));
        auto file_a = native_file(first.get(), "/data");
        auto file_b = native_file(second.get(), "/data");
        KREQUIRE_TRUE(file_a && file_b);
        char const A = 'a', B = 'b';
        KEXPECT_EQ(tmpfs::tmpfs_write(file_a.get(), &A, 1, 0), 1);
        KEXPECT_EQ(tmpfs::tmpfs_write(file_b.get(), &B, 1, 0), 1);
        vfs::mount_lookup_cache_reset_for_test();
        for (size_t iteration = 0; iteration < 32; ++iteration) {
            auto a = scoped_mount(views.first, "/service/data");
            auto b = scoped_mount(views.second, "/service/data");
            auto a_root = scoped_mount(views.first, "/unmounted/data");
            auto b_root = scoped_mount(views.second, "/unmounted/data");
            KEXPECT_EQ(a.get(), first.get());
            KEXPECT_EQ(b.get(), second.get());
            KEXPECT_EQ(a_root.get(), first_root.get());
            KEXPECT_EQ(b_root.get(), second_root.get());
            KEXPECT_EQ(vfs::find_mount_point("/unmounted/data").get(), host_root.get());
        }
        KEXPECT_TRUE(vfs::mount_lookup_cache_hits_for_test() > 0);
        char observed = 0;
        KEXPECT_EQ(tmpfs::tmpfs_read(file_a.get(), &observed, 1, 0), 1);
        KEXPECT_EQ(observed, A);
        KEXPECT_EQ(tmpfs::tmpfs_read(file_b.get(), &observed, 1, 0), 1);
        KEXPECT_EQ(observed, B);
        KEXPECT_EQ(vfs::get_mount_count(), HOST_COUNT);
        KEXPECT_EQ(vfs::get_mount_count(views.first), 2U);
        KEXPECT_EQ(vfs::get_mount_count(views.second), 2U);
        for (size_t index = 0; index < HOST_COUNT; ++index) {
            vfs::MountSnapshot snapshot{};
            KREQUIRE_TRUE(vfs::get_mount_snapshot_at(index, &snapshot));
            KEXPECT_NE(snapshot.dev_id, first->dev_id);
            KEXPECT_NE(snapshot.dev_id, second->dev_id);
            KEXPECT_NE(snapshot.dev_id, first_root->dev_id);
            KEXPECT_NE(snapshot.dev_id, second_root->dev_id);
        }
        for (size_t index = 0; index < 2; ++index) {
            auto selected = vfs::get_mount_at(index, views.first);
            KREQUIRE_TRUE(selected);
            KEXPECT_TRUE(selected->namespace_owner == views.first);
        }
        KEXPECT_FALSE(vfs::get_mount_at(2, views.first));
    }
    KEXPECT_EQ(mount_ns::live_namespace_count(), NS_BASELINE);
    KEXPECT_EQ(vfs::get_mount_count(), HOST_COUNT);
}

KTEST(CompartmentMount, ReservationsClosureAndUnsupportedViewsFailBeforePublication) {
    PrivateMounts views;
    KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
    KREQUIRE_EQ(mount_ns::create_namespace(views.second), 0);
    auto prepare = [](const mount_ns::NamespaceRef& scope, const char* type, vfs::PreparedMount& output) {
        return vfs::prepare_mount_filesystem("/same", type, nullptr, 0, nullptr, nullptr, nullptr, &output,
                                             vfs::MountPathKind::RETAINED_PHYSICAL, scope);
    };
    KREQUIRE_EQ(prepare(views.first, "tmpfs", views.pending_first), 0);
    KREQUIRE_EQ(prepare(views.second, "tmpfs", views.pending_second), 0);
    vfs::PreparedMount duplicate{};
    KEXPECT_EQ(prepare(views.first, "tmpfs", duplicate), -EBUSY);
    KEXPECT_NULL(duplicate.state);
    // Pending initialization holds the namespace; retirement closes admission
    // but preserves the prepared owner until its caller finishes rollback.
    KEXPECT_EQ(vfs::retire_mount_namespace(views.first), -EBUSY);
    KEXPECT_TRUE(views.first.closed());
    KEXPECT_EQ(vfs::publish_prepared_mount(&views.pending_first), -EIDRM);
    KEXPECT_EQ(vfs::publish_prepared_mount(&views.pending_second), 0);
    KEXPECT_EQ(vfs::get_mount_count(views.first), 0U);
    KEXPECT_EQ(vfs::get_mount_count(views.second), 1U);
    vfs::finish_prepared_mount(&views.pending_first);
    KEXPECT_EQ(vfs::retire_mount_namespace(views.first), 0);
    KEXPECT_EQ(prepare(views.first, "tmpfs", duplicate), -EIDRM);
    for (auto* type : {"devfs", "procfs", "remote", "fat32", "xfs"}) {
        KEXPECT_EQ(prepare(views.second, type, duplicate), -ENOTSUP);
        KEXPECT_NULL(duplicate.state);
    }
    mount_ns::NamespaceRef host;
    KEXPECT_EQ(vfs::retire_mount_namespace(host), -EINVAL);
    KEXPECT_FALSE(host.closed());
}

KTEST(CompartmentMount, ForeignDescendantsDoNotBlockOwnUnmount) {
    PrivateMounts views;
    KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
    KREQUIRE_EQ(mount_ns::create_namespace(views.second), 0);
    KREQUIRE_EQ(private_mount(views.first, "/same"), 0);
    KREQUIRE_EQ(private_mount(views.second, "/same"), 0);
    KREQUIRE_EQ(private_mount(views.second, "/same/child"), 0);
    KEXPECT_EQ(vfs::unmount_filesystem("/same", views.first), 0);
    KEXPECT_FALSE(scoped_mount(views.first, "/same"));
    KEXPECT_EQ(vfs::get_mount_count(views.second), 2U);
    KEXPECT_EQ(vfs::unmount_filesystem("/same", views.second), -EBUSY);
    KEXPECT_EQ(vfs::unmount_filesystem("/same/child", views.second), 0);
    KEXPECT_EQ(vfs::unmount_filesystem("/same", views.second), 0);
}

KTEST(CompartmentMount, PivotRebasesOnlyExactViewAndOwnerReopensEpoch) {
    PrivateMounts views;
    KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
    KREQUIRE_EQ(mount_ns::create_namespace(views.second), 0);
    KREQUIRE_EQ(private_mount(views.first, "/"), 0);
    KREQUIRE_EQ(private_mount(views.first, "/new"), 0);
    KREQUIRE_EQ(private_mount(views.first, "/cache"), 0);
    // Same destination in a sibling must not collide with this pivot.
    KREQUIRE_EQ(private_mount(views.second, "/"), 0);
    KREQUIRE_EQ(private_mount(views.second, "/new/old"), 0);
    auto const HOST_COUNT = vfs::get_mount_count();
    auto host = vfs::find_mount_point("/");
    auto sibling = scoped_mount(views.second, "/new/old");
    KREQUIRE_TRUE(host && sibling);
    auto const SIBLING_ID = sibling->dev_id;
    uint32_t old_id = 0, new_id = 0, cache_id = 0;
    {
        auto old = scoped_mount(views.first, "/");
        auto new_root = scoped_mount(views.first, "/new");
        auto cache = scoped_mount(views.first, "/cache");
        KREQUIRE_TRUE(old && new_root && cache);
        old_id = old->dev_id;
        new_id = new_root->dev_id;
        cache_id = cache->dev_id;
        auto const ROOT = ker::mod::security::Snapshot::bootstrap_root();
        KREQUIRE_EQ(tmpfs::tmpfs_mkdir_path(ROOT, native_root(new_root.get()), "/old", 0755), 0);
        KREQUIRE_EQ(tmpfs::tmpfs_mkdir_path(ROOT, native_root(new_root.get()), "/cache", 0755), 0);
        // Exact owned lookup pins still enforce the existing pivot busy rule.
        KEXPECT_EQ(vfs::remap_mounts_for_pivot("/new", "/new/old", nullptr, nullptr, views.first), -EBUSY);
        KEXPECT_FALSE(vfs::mount_pivot_pending_for_test());
    }
    {
        vfs::PivotMountReferences references;
        auto const ROOT = ker::mod::security::Snapshot::bootstrap_root();
        KREQUIRE_EQ(vfs::remap_mounts_for_pivot("/new", "/new/old", &ROOT, &references, views.first), 0);
        // Reopen before any assertion which could return; no lookup may enter
        // a deliberately pending pivot epoch.
        bool const PENDING = vfs::mount_pivot_pending_for_test();
        vfs::rebase_wki_mounts_for_new_root("/new", views.second);
        bool const FOREIGN_REJECTED = vfs::mount_pivot_pending_for_test();
        vfs::rebase_wki_mounts_for_new_root("/new", views.first);
        KEXPECT_TRUE(PENDING && FOREIGN_REJECTED);
        KEXPECT_FALSE(vfs::mount_pivot_pending_for_test());
    }
    auto old = scoped_mount(views.first, "/new/old");
    auto new_root = scoped_mount(views.first, "/new");
    auto cache = scoped_mount(views.first, "/new/cache");
    KREQUIRE_TRUE(old && new_root && cache);
    KEXPECT_EQ(old->dev_id, old_id);
    KEXPECT_EQ(new_root->dev_id, new_id);
    KEXPECT_EQ(cache->dev_id, cache_id);
    KEXPECT_TRUE(old->covered_directory && old->covered_directory->mount_owner == new_root.get());
    KEXPECT_TRUE(cache->covered_directory && cache->covered_directory->mount_owner == new_root.get());
    KEXPECT_EQ(scoped_mount(views.second, "/new/old")->dev_id, SIBLING_ID);
    KEXPECT_EQ(vfs::find_mount_point("/").get(), host.get());
    KEXPECT_EQ(vfs::get_mount_count(), HOST_COUNT);
}

KTEST(CompartmentMount, RetirementKeepsExplicitFileCapabilityUntilDrain) {
    auto const BASELINE = mount_ns::live_namespace_count();
    {
        PrivateMounts views;
        KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
        KREQUIRE_EQ(private_mount(views.first, "/same"), 0);
        auto mount = scoped_mount(views.first, "/same");
        KREQUIRE_TRUE(mount);
        auto file = native_file(mount.get(), "/data");
        KREQUIRE_TRUE(file);
        auto const ID = mount->dev_id;
        mount.reset();
        KEXPECT_EQ(vfs::retire_mount_namespace(views.first), -EBUSY);
        KEXPECT_TRUE(views.first.closed());
        KEXPECT_FALSE(scoped_mount(views.first, "/same/data"));
        KEXPECT_EQ(vfs::get_mount_count(views.first), 1U);
        char const VALUE = 'c';
        KEXPECT_EQ(tmpfs::tmpfs_write(file.get(), &VALUE, 1, 0), 1);
        char actual = 0;
        KEXPECT_EQ(tmpfs::tmpfs_read(file.get(), &actual, 1, 0), 1);
        KEXPECT_EQ(actual, VALUE);
        KEXPECT_EQ(file->mount_dev_id, ID);
        file.reset();
        KEXPECT_EQ(vfs::retire_mount_namespace(views.first), 0);
        KEXPECT_EQ(vfs::get_mount_count(views.first), 0U);
    }
    KEXPECT_EQ(mount_ns::live_namespace_count(), BASELINE);
}

KTEST(CompartmentMount, ForeignCoveredDirectoryCannotPublishMountEdge) {
    PrivateMounts views;
    KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
    KREQUIRE_EQ(mount_ns::create_namespace(views.second), 0);
    KREQUIRE_EQ(private_mount(views.first, "/"), 0);
    KREQUIRE_EQ(private_mount(views.second, "/"), 0);
    auto first = scoped_mount(views.first, "/");
    auto second = scoped_mount(views.second, "/");
    KREQUIRE_TRUE(first && second);
    auto const ROOT = ker::mod::security::Snapshot::bootstrap_root();
    KREQUIRE_EQ(tmpfs::tmpfs_mkdir_path(ROOT, native_root(first.get()), "/child", 0755), 0);
    KREQUIRE_EQ(tmpfs::tmpfs_mkdir_path(ROOT, native_root(second.get()), "/child", 0755), 0);
    auto pin_cover = [&](vfs::MountPoint* mount) -> NativeFile {
        tmpfs::TmpfsLookupHandle lookup;
        if (tmpfs::tmpfs_acquire_lookup(ROOT, native_root(mount), "/child", &lookup) != 0) return {};
        NativeFile file(tmpfs::tmpfs_pin_directory_lookup(ROOT, lookup));
        if (!file || !vfs::retain_mount_for_open_file(mount)) return {};
        file->mount_owner = mount;
        file->mount_dev_id = mount->dev_id;
        file->mount_generation = vfs::mount_table_generation_snapshot();
        return file;
    };
    auto foreign = pin_cover(second.get());
    auto own = pin_cover(first.get());
    KREQUIRE_TRUE(foreign && own);
    KREQUIRE_EQ(vfs::prepare_mount_filesystem("/child", "tmpfs", nullptr, 0, nullptr, nullptr, nullptr, &views.pending_first,
                                              vfs::MountPathKind::RETAINED_PHYSICAL, views.first),
                0);
    auto const GENERATION = vfs::mount_table_generation_snapshot();
    KEXPECT_EQ(vfs::publish_prepared_mount(&views.pending_first, "/child", foreign.get()), -EINVAL);
    KEXPECT_EQ(vfs::mount_table_generation_snapshot(), GENERATION);
    KEXPECT_EQ(vfs::get_mount_count(views.first), 1U);
    KEXPECT_EQ(vfs::publish_prepared_mount(&views.pending_first, "/child", own.get()), 0);
    auto child = scoped_mount(views.first, "/child");
    KREQUIRE_TRUE(child);
    KEXPECT_EQ(child->covered_directory, own.get());
    KEXPECT_EQ(child->covered_directory->mount_owner, first.get());
    KEXPECT_EQ(vfs::get_mount_count(views.second), 1U);
}

KTEST(CompartmentMount, PrivateMountLimitRollsBackPreparedBacking) {
    auto const BASELINE = mount_ns::live_namespace_count();
    {
        PrivateMounts views;
        KREQUIRE_EQ(mount_ns::create_namespace(views.first), 0);
        for (size_t index = 0; index < mount_ns::MAX_PRIVATE_MOUNTS; ++index) {
            std::array<char, 32> path{};
            std::snprintf(path.data(), path.size(), "/mount%zu", index);
            KREQUIRE_EQ(private_mount(views.first, path.data()), 0);
        }
        auto const GENERATION = vfs::mount_table_generation_snapshot();
        KEXPECT_EQ(private_mount(views.first, "/exhausted"), -ENOSPC);
        KEXPECT_EQ(vfs::get_mount_count(views.first), mount_ns::MAX_PRIVATE_MOUNTS);
        KEXPECT_EQ(vfs::mount_table_generation_snapshot(), GENERATION);
        KEXPECT_FALSE(scoped_mount(views.first, "/exhausted"));
        // Rollback released the path reservation; removing one exact row lets
        // a fresh preparation publish the formerly rejected path.
        KEXPECT_EQ(vfs::unmount_filesystem("/mount0", views.first), 0);
        KEXPECT_EQ(private_mount(views.first, "/exhausted"), 0);
    }
    KEXPECT_EQ(mount_ns::live_namespace_count(), BASELINE);
}
