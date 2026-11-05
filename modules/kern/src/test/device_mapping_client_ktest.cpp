#include "device_mapping_client_ktest.hpp"

#include <array>
#include <cerrno>
#include <cstring>
#include <dev/device_mapping_client.hpp>
#include <memory>
#include <new>
#include <platform/dbg/dbg.hpp>
#include <platform/mm/phys.hpp>
#include <platform/sched/scheduler.hpp>
#include <utility>
#include <vfs/fs/devfs.hpp>
#include <vfs/vfs.hpp>

namespace ker::test {
namespace {
namespace mapping = ker::dev::mapping;
namespace sched = ker::mod::sched;
namespace security = ker::mod::security;
namespace vfs = ker::vfs;
namespace devfs = vfs::devfs;
using log = ker::mod::dbg::logger<"device-client-test">;
constexpr char NAME[] = "ktest_owned_device_client";
constexpr char PATH[] = "/dev/ktest_owned_device_client";
constexpr char LEGACY_NAME[] = "ktest_legacy_device_client";
constexpr char LEGACY_PATH[] = "/dev/ktest_legacy_device_client";

struct Checks {
    unsigned assertions{}, failures{};
    auto expect(bool condition, const char* reason) -> bool {
        ++assertions;
        if (!condition) {
            ++failures;
            log::error("DEVICE_CLIENT_RUNTIME FAIL %s", reason);
        }
        return condition;
    }
};

struct Counts {
    unsigned reads{}, closes{}, destroyed{}, unsafe_context{};
};

auto ordinary_context() -> bool { return sched::interrupts_enabled() && sched::preempt_count() == 0; }

struct Context final : mapping::EndpointContext {
    Context(Counts& counts, uint8_t marker) : counts(counts), marker(marker) {}
    ~Context() override {
        if (!ordinary_context()) ++counts.unsafe_context;
        ++counts.destroyed;
    }
    Counts& counts;
    uint8_t marker;
};

auto context(vfs::File* file) -> Context* {
    if (file == nullptr || !file->device_client) return nullptr;
    return &static_cast<Context&>(file->device_client.get()->endpoint().context());
}

auto operations() -> ker::dev::CharDeviceOps {
    return {.close = [](vfs::File* file) -> int {
                auto* value = context(file);
                if (value == nullptr) return -EINVAL;
                if (!ordinary_context()) ++value->counts.unsafe_context;
                ++value->counts.closes;
                return 0;
            },
            .read = [](vfs::File* file, void* output, size_t count) -> ssize_t {
                auto* value = context(file);
                if (value == nullptr) return -EINVAL;
                if (!ordinary_context()) ++value->counts.unsafe_context;
                ++value->counts.reads;
                if (count == 0) return 0;
                *static_cast<uint8_t*>(output) = value->marker;
                return 1;
            }};
}

struct Fixture {
    explicit Fixture(Checks& checks) : checks(checks), actor(sched::get_current_task()) {}
    ~Fixture() {
        if (duplicate >= 0) checks.expect(vfs::vfs_close_for_task(actor, duplicate) == 0, "cleanup duplicated FD");
        if (fd >= 0) checks.expect(vfs::vfs_close_for_task(actor, fd) == 0, "cleanup original FD");
        if (file != nullptr) vfs::vfs_put_file(file);
        if (published) checks.expect(devfs::devfs_remove_node(NAME), "cleanup exact-owned typed node");
        first.reset();
        second.reset();
    }
    auto create(mapping::EndpointRef& endpoint, Counts& counts, uint8_t marker) -> bool {
        std::unique_ptr<mapping::EndpointContext> owned(new (std::nothrow) Context(counts, marker));
        auto copied_operations = operations();
        bool const CREATED =
            owned != nullptr && mapping::Endpoint::create(240, marker, NAME, copied_operations, std::move(owned), endpoint) == 0;
        copied_operations.read = nullptr;
        return CREATED;
    }
    auto publish() -> bool {
        auto const ROOT = security::Snapshot::bootstrap_root();
        published = devfs::devfs_add_mapping_endpoint(NAME, first, &ROOT) != nullptr;
        return published;
    }
    auto open(vfs::File*& output) -> bool {
        return vfs::vfs_open_file_as(*actor, security::Snapshot::bootstrap_root(), PATH, vfs::O_LOCAL, &output) == 0 && output != nullptr;
    }
    auto read(vfs::File* opened, uint8_t marker) -> bool {
        uint8_t byte{};
        return opened->fops->vfs_read(opened, &byte, 1, 0) == 1 && byte == marker;
    }
    Checks& checks;
    sched::task::Task* actor;
    Counts first_counts{}, second_counts{};
    mapping::EndpointRef first, second;
    vfs::File* file{};
    int fd = -1, duplicate = -1;
    bool published{};
};

auto replacement_and_dup(Checks& checks) -> bool {
    Fixture fixture(checks);
    if (!checks.expect(fixture.create(fixture.first, fixture.first_counts, 0x31) && fixture.publish(), "exclusive typed publication") ||
        !checks.expect(fixture.open(fixture.file), "actual VFS open creates typed client"))
        return false;
    auto* original = fixture.file;
    vfs::Stat original_stat{};
    if (!checks.expect(original->device_client && fixture.read(original, 0x31), "owned callback context readable") ||
        !checks.expect(original->device_client.get()->access().read_only == original->mount_policy.read_only &&
                           original->device_client.get()->access().nosuid == original->mount_policy.nosuid &&
                           original->device_client.get()->credentials().user().effective == 0,
                       "actual admitted mount policy and credentials captured"))
        return false;
    if (!checks.expect(vfs::vfs_fstat_file(original, &original_stat) == 0 && (original_stat.st_mode & 0777U) == 0600,
                       "capture selected typed inode identity"))
        return false;
    mapping::ClientRef retained;
    if (!checks.expect(original->device_client.clone_into(retained), "independent retained client")) return false;
    fixture.fd = vfs::vfs_alloc_fd(fixture.actor, original);
    if (!checks.expect(fixture.fd >= 0, "install real FD")) return false;
    fixture.file = nullptr;
    fixture.duplicate = vfs::vfs_dup(fixture.fd);
    if (!checks.expect(fixture.duplicate >= 0, "duplicate actual shared open description")) return false;
    vfs::File* duplicate_file = nullptr;
    bool const DUPLICATE =
        vfs::vfs_get_file_retain_checked(fixture.actor, fixture.duplicate, duplicate_file) == 0 && duplicate_file == original;
    vfs::vfs_put_file(duplicate_file);
    if (!checks.expect(DUPLICATE, "dup retains exact File/client")) return false;
    if (!checks.expect(fixture.create(fixture.second, fixture.second_counts, 0x42) &&
                           devfs::devfs_add_mapping_endpoint(NAME, fixture.second) != nullptr,
                       "replace node with distinct owned endpoint"))
        return false;
    fixture.first.reset();
    if (!checks.expect(fixture.first_counts.destroyed == 0 && fixture.read(original, 0x31), "replacement preserves old File endpoint") ||
        !checks.expect(fixture.open(fixture.file) && fixture.read(fixture.file, 0x42), "new open resolves replacement endpoint"))
        return false;
    vfs::Stat old_stat{}, new_stat{};
    if (!checks.expect(vfs::vfs_fstat_file(original, &old_stat) == 0 && vfs::vfs_fstat_file(fixture.file, &new_stat) == 0 &&
                           old_stat.st_ino == original_stat.st_ino && new_stat.st_ino != old_stat.st_ino &&
                           (old_stat.st_mode & 0777U) == 0600 && (new_stat.st_mode & 0777U) == 0666,
                       "replacement preserves old inode and publishes distinct new inode") ||
        !checks.expect(vfs::vfs_fchmod_file_as(original, security::Snapshot::bootstrap_root(), 0400) == 0 &&
                           vfs::vfs_fstat_file(original, &old_stat) == 0 && vfs::vfs_fstat_file(fixture.file, &new_stat) == 0 &&
                           (old_stat.st_mode & 0777U) == 0400 && (new_stat.st_mode & 0777U) == 0666,
                       "retained File metadata mutation cannot modify replacement inode"))
        return false;
    int result = vfs::vfs_close_for_task(fixture.actor, fixture.fd);
    fixture.fd = -1;
    if (!checks.expect(result == 0 && fixture.first_counts.closes == 0, "first descriptor close does not close shared File")) return false;
    result = vfs::vfs_close_for_task(fixture.actor, fixture.duplicate);
    fixture.duplicate = -1;
    if (!checks.expect(result == 0 && fixture.first_counts.closes == 1 && fixture.first_counts.destroyed == 0,
                       "final File close sees live client while independent owner survives"))
        return false;
    retained.reset();
    if (!checks.expect(fixture.first_counts.destroyed == 1 && fixture.first_counts.unsafe_context == 0,
                       "old context destroyed exactly once outside IRQ guards"))
        return false;
    if (!checks.expect(devfs::devfs_remove_node(NAME), "unlink replacement")) return false;
    fixture.published = false;
    fixture.second.reset();
    if (!checks.expect(fixture.second_counts.destroyed == 0, "unlinked replacement retained by File")) return false;
    vfs::vfs_put_file(std::exchange(fixture.file, nullptr));
    return checks.expect(
        fixture.second_counts.closes == 1 && fixture.second_counts.destroyed == 1 && fixture.second_counts.unsafe_context == 0,
        "replacement close precedes final context free outside IRQ guards");
}

auto admission_and_metadata(Checks& checks) -> bool {
    Fixture fixture(checks);
    if (!checks.expect(fixture.create(fixture.first, fixture.first_counts, 0x53) && fixture.publish(), "policy fixture publication"))
        return false;
    auto const ROOT = security::Snapshot::bootstrap_root();
    int error{};
    auto* missing = devfs::devfs_open_path(ROOT, NAME, 0, 0, &error);
    if (missing != nullptr) vfs::vfs_put_file(missing);
    if (!checks.expect(missing == nullptr && error == -EINVAL, "typed direct open refuses missing mount admission")) return false;
    vfs::MountPolicy const READ_ONLY{.read_only = true, .nosuid = true};
    auto* denied = devfs::devfs_open_path(ROOT, NAME, 2, 0, &error, false, nullptr, &READ_ONLY);
    if (denied != nullptr) vfs::vfs_put_file(denied);
    if (!checks.expect(denied == nullptr && error == -EROFS && fixture.first_counts.closes == 0,
                       "failed client admission publishes nothing and invokes no backend close") ||
        !checks.expect(fixture.open(fixture.file), "retained client before closing admission"))
        return false;
    auto metadata = devfs::devfs_acquire_path(NAME);
    if (!checks.expect(static_cast<bool>(metadata), "retain actual metadata independently")) return false;
    fixture.first.get()->close_admission();
    vfs::File* rejected = nullptr;
    bool const OPENED = fixture.open(rejected);
    if (rejected != nullptr) vfs::vfs_put_file(rejected);
    if (!checks.expect(!OPENED && fixture.read(fixture.file, 0x53), "closed admission refuses new opens and preserves old client") ||
        !checks.expect(devfs::devfs_remove_node(NAME), "unlink typed node with live metadata reference"))
        return false;
    fixture.published = false;
    fixture.first.reset();
    vfs::vfs_put_file(std::exchange(fixture.file, nullptr));
    if (!checks.expect(metadata && fixture.first_counts.closes == 1 && fixture.first_counts.destroyed == 0,
                       "detached inode retains endpoint through metadata owner"))
        return false;
    metadata.reset();
    return checks.expect(fixture.first_counts.destroyed == 1 && fixture.first_counts.unsafe_context == 0,
                         "last detached inode owner releases endpoint outside IRQ guards");
}

Counts legacy_counts;
ker::dev::CharDeviceOps legacy_operations{
    .close = [](vfs::File* file) -> int {
        if (file->device_client || !ordinary_context()) ++legacy_counts.unsafe_context;
        ++legacy_counts.closes;
        return 0;
    },
    .read = [](vfs::File* file, void* output, size_t count) -> ssize_t {
        struct Prefix {
            void* node;
            ker::dev::Device* device;
            uint32_t magic;
        };
        auto* prefix = static_cast<const Prefix*>(file->private_data);
        if (file->device_client || prefix == nullptr || prefix->magic != 0xDEADBEEF || prefix->device == nullptr) return -EINVAL;
        ++legacy_counts.reads;
        if (count == 0) return 0;
        *static_cast<uint8_t*>(output) = 0x64;
        return 1;
    }};
ker::dev::Device legacy_device{.name = LEGACY_NAME, .type = ker::dev::DeviceType::CHAR, .char_ops = &legacy_operations};

auto legacy_compatibility(Checks& checks) -> bool {
    legacy_counts = {};
    std::array<char, devfs::DEVFS_NAME_MAX> name{};
    std::memcpy(name.data(), LEGACY_NAME, sizeof(LEGACY_NAME));
    auto const ROOT = security::Snapshot::bootstrap_root();
    if (!checks.expect(devfs::devfs_add_device_node(name, &legacy_device, &ROOT) != nullptr, "exclusive static legacy publication"))
        return false;
    vfs::File* file = nullptr;
    bool const OPENED = vfs::vfs_open_file_as(*sched::get_current_task(), ROOT, LEGACY_PATH, vfs::O_LOCAL, &file) == 0 && file != nullptr;
    uint8_t byte{};
    bool const READ = OPENED && !file->device_client && file->fops->vfs_read(file, &byte, 1, 0) == 1 && byte == 0x64;
    if (file != nullptr) vfs::vfs_put_file(file);
    bool const REMOVED = devfs::devfs_remove_node(LEGACY_NAME);
    return checks.expect(
        OPENED && READ && REMOVED && legacy_counts.reads == 1 && legacy_counts.closes == 1 && legacy_counts.unsafe_context == 0,
        "default-null legacy File and devfs prefix remain compatible");
}
}  // namespace

auto run_device_mapping_client_selftest() -> bool {
    Checks checks;
    if (!checks.expect(sched::can_query_current_task() && sched::get_current_task() != nullptr && ordinary_context() &&
                           ker::mod::mm::phys::can_wait_for_reclaim(),
                       "ordinary scheduled test context"))
        return false;
    if (!replacement_and_dup(checks) || checks.failures != 0) return false;
    log::info("DEVICE_CLIENT_RUNTIME CASE_PASS retained_replace_dup_close");
    if (!admission_and_metadata(checks) || checks.failures != 0) return false;
    log::info("DEVICE_CLIENT_RUNTIME CASE_PASS admission_metadata_lifetime");
    if (!legacy_compatibility(checks) || checks.failures != 0) return false;
    log::info("DEVICE_CLIENT_RUNTIME CASE_PASS legacy_null_prefix");
    log::info("DEVICE_CLIENT_RUNTIME PASS cases=3 assertions=%u failures=%u no_mapping=1", checks.assertions, checks.failures);
    return checks.failures == 0;
}
}  // namespace ker::test
