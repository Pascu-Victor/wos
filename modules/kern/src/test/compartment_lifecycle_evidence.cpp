#ifdef WOS_SELFTEST
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <net/wki/auth_crypto.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <test/compartment_lifecycle_evidence.hpp>
#include <utility>
#include <vfs/exec_content.hpp>
#include <vfs/file.hpp>
#include <vfs/mount.hpp>
#include <vfs/stat.hpp>
#include <vfs/vfs.hpp>

namespace ker::test::lifecycle60::evidence {
namespace {
namespace wire = criterion9::wire;
namespace crypto = ker::net::wki::auth_crypto;
constexpr uint64_t IMAGE_LIMIT = 16 * 1024 * 1024;

class OwnedFile final {
   public:
    explicit OwnedFile(vfs::File* value = nullptr) noexcept : value_(value) {}
    ~OwnedFile() {
        if (value_ != nullptr) vfs::vfs_put_file(value_);
    }
    OwnedFile(const OwnedFile&) = delete;
    auto operator=(const OwnedFile&) -> OwnedFile& = delete;
    auto get() const noexcept -> vfs::File* { return value_; }
    auto slot() noexcept -> vfs::File** { return &value_; }
    auto finish() noexcept -> int {
        auto* value = std::exchange(value_, nullptr);
        return value != nullptr ? vfs::vfs_put_file_result(value) : 0;
    }

   private:
    vfs::File* value_{};
};

auto ordinary_backend_context(const EvidenceLease& lease) noexcept -> bool {
    return lease && lease.subject() != nullptr && mod::sched::interrupts_enabled() && mod::sched::preempt_count() == 0;
}

auto host_process(Task& subject) noexcept -> bool {
    auto const* pid = subject.pid_namespace();
    auto const* ipc = subject.ipc_namespace();
    auto const* mount = subject.mount_namespace();
    return subject.type == mod::sched::task::TaskType::PROCESS && !subject.is_thread && !subject.wki_proxy_task && pid != nullptr &&
           pid->is_host() && ipc != nullptr && ipc->is_host() && mount != nullptr && mount->is_host();
}

auto same_metadata(const vfs::Stat& a, const vfs::Stat& b) noexcept -> bool {
    return a.st_dev == b.st_dev && a.st_ino == b.st_ino && a.st_size == b.st_size && a.st_mode == b.st_mode && a.st_uid == b.st_uid &&
           a.st_gid == b.st_gid && a.st_nlink == b.st_nlink && a.st_mtim.tv_sec == b.st_mtim.tv_sec &&
           a.st_mtim.tv_nsec == b.st_mtim.tv_nsec && a.st_ctim.tv_sec == b.st_ctim.tv_sec && a.st_ctim.tv_nsec == b.st_ctim.tv_nsec;
}

auto trusted_read_view(vfs::File& file, uint64_t size, bool manifest, vfs::Stat& output) noexcept -> int {
    if (file.is_directory || file.mount_owner == nullptr || !file.mount_owner->namespace_owner.is_host() ||
        (static_cast<unsigned>(file.open_flags) & 3U) != 0 || file.exec_read_view == nullptr || file.exec_read_view->content_id == 0 ||
        file.refcount.load(std::memory_order_acquire) <= 0)
        return -EACCES;
    vfs::Stat stat{};
    int const RESULT = vfs::vfs_fstat_file_native(&file, &stat);
    if (RESULT != 0) return RESULT;  // This API accepts only the exact native TMPFS/XFS fops.
    if ((stat.st_mode & vfs::S_IFMT) != vfs::S_IFREG || stat.st_uid != 0 || stat.st_gid != 0 || stat.st_nlink != 1 || stat.st_size < 0 ||
        static_cast<uint64_t>(stat.st_size) != size || (stat.st_mode & 0222U) != 0 || (manifest && (stat.st_mode & 07777U) != 0444U) ||
        (!manifest && (stat.st_mode & 0111U) == 0) || !same_metadata(stat, file.exec_read_view->metadata))
        return -EACCES;
    output = stat;
    return 0;
}

template <size_t N>
auto all_zero(const std::array<uint64_t, N>& values) noexcept -> bool {
    return std::ranges::all_of(values, [](uint64_t value) { return value == 0; });
}

auto manifest_valid(const wire::Manifest& value) noexcept -> bool {
    if (value.magic != wire::MANIFEST_MAGIC || value.version != wire::VERSION || value.bytes != sizeof(value) ||
        value.record_count != value.entries.size() || value.nonce == 0 || value.warmups != WARMUPS || value.faults != FAULTS ||
        value.successes != SUCCESSES || !all_zero(value.reserved))
        return false;
    for (size_t i = 0; i < value.entries.size(); ++i) {
        auto const& entry = value.entries[i];
        if (entry.role != i + 1 || entry.bytes < 64 || entry.bytes > IMAGE_LIMIT || (entry.elf_type != 2 && entry.elf_type != 3) ||
            entry.entry == 0 || entry.entry >= 0x0000800000000000ULL || entry.phnum == 0 || entry.phnum > 128 ||
            !all_zero(entry.reserved) || std::ranges::all_of(entry.sha256, [](uint8_t byte) { return byte == 0; }))
            return false;
    }
    return true;
}

auto le(const uint8_t* bytes, size_t count) noexcept -> uint64_t {
    uint64_t result = 0;
    for (size_t i = 0; i < count; ++i) result |= static_cast<uint64_t>(bytes[i]) << (i * 8);
    return result;
}

auto elf_header_matches(const std::array<uint8_t, 64>& header, const wire::ManifestEntry& expected) noexcept -> bool {
    uint64_t const PHOFF = le(header.data() + 32, 8);
    return header[0] == 0x7f && header[1] == 'E' && header[2] == 'L' && header[3] == 'F' && header[4] == 2 && header[5] == 1 &&
           header[6] == 1 && le(header.data() + 16, 2) == expected.elf_type && le(header.data() + 18, 2) == 62 &&
           le(header.data() + 20, 4) == 1 && le(header.data() + 24, 8) == expected.entry && le(header.data() + 52, 2) == 64 &&
           le(header.data() + 54, 2) == 56 && le(header.data() + 56, 2) == expected.phnum && PHOFF >= 64 && PHOFF <= expected.bytes &&
           expected.phnum <= (expected.bytes - PHOFF) / 56;
}

auto check_image_file(vfs::File& file, const wire::ManifestEntry& expected, FileIdentity& output) noexcept -> int {
    if (expected.bytes < 64 || expected.bytes > IMAGE_LIMIT || expected.phnum == 0 || expected.phnum > 128) return -EINVAL;
    vfs::Stat before{};
    int result = trusted_read_view(file, expected.bytes, false, before);
    if (result != 0) return result;
    std::array<uint8_t, 64> header{};
    if (vfs::vfs_pread_file_direct(&file, header.data(), header.size(), 0) != static_cast<ssize_t>(header.size()) ||
        !elf_header_matches(header, expected))
        return -ENOEXEC;
    crypto::Sha256 sha;
    std::array<uint8_t, 4096> chunk{};
    for (uint64_t offset = 0; offset < expected.bytes;) {
        size_t const COUNT = static_cast<size_t>(std::min<uint64_t>(chunk.size(), expected.bytes - offset));
        ssize_t const READ = vfs::vfs_pread_file_direct(&file, chunk.data(), COUNT, static_cast<off_t>(offset));
        if (READ != static_cast<ssize_t>(COUNT)) return READ < 0 ? static_cast<int>(READ) : -EIO;
        sha.update(chunk.data(), COUNT);
        offset += COUNT;
    }
    if (sha.finish() != expected.sha256) return -ENOEXEC;
    vfs::Stat after{};
    result = trusted_read_view(file, expected.bytes, false, after);
    if (result != 0) return result;
    if (!same_metadata(before, after)) return -ESTALE;
    output = {.device = after.st_dev, .inode = after.st_ino, .bytes = expected.bytes};
    return 0;
}
}  // namespace

auto read_manifest(const EvidenceLease& lease, wire::Manifest& output) noexcept -> int {
    if (!ordinary_backend_context(lease)) return -EWOULDBLOCK;
    auto& subject = *lease.subject();
    if (!host_process(subject)) return -EACCES;
    OwnedFile file;
    int result = vfs::vfs_open_read_file_as(subject, subject.credentials(), wire::MANIFEST_PATH, false, file.slot());
    if (result != 0) return result;
    result = vfs::vfs_stabilize_exec_file(file.slot());
    if (result != 0) return result;
    vfs::Stat before{};
    result = trusted_read_view(*file.get(), sizeof(wire::Manifest), true, before);
    if (result != 0) return result;
    wire::Manifest value{};
    if (vfs::vfs_pread_file_direct(file.get(), &value, sizeof(value), 0) != static_cast<ssize_t>(sizeof(value)) || !manifest_valid(value))
        return -EBADMSG;
    vfs::Stat after{};
    result = trusted_read_view(*file.get(), sizeof(value), true, after);
    if (result != 0) return result;
    if (!same_metadata(before, after)) return -ESTALE;
    result = file.finish();
    if (result == 0) output = value;
    return result;
}

auto check_driver_image(const EvidenceLease& lease, const wire::ManifestEntry& expected) noexcept -> int {
    if (!ordinary_backend_context(lease)) return -EWOULDBLOCK;
    auto& subject = *lease.subject();
    if (!host_process(subject) || expected.role != static_cast<uint64_t>(wire::AssetRole::DRIVER)) return -EACCES;
    if (subject.exec_image_file != nullptr) {
        if (!vfs::vfs_try_retain_file(subject.exec_image_file)) return -EOVERFLOW;
        OwnedFile file(subject.exec_image_file);
        FileIdentity actual{};
        int const RESULT = check_image_file(*file.get(), expected, actual);
        int const CLOSE = file.finish();
        return RESULT != 0 ? RESULT : CLOSE;
    }
    if (!subject.elf_buffer_complete || subject.elf_buffer == nullptr || expected.bytes < 64 || expected.bytes > IMAGE_LIMIT ||
        subject.elf_buffer_size != expected.bytes)
        return -ENOEXEC;
    std::array<uint8_t, 64> header{};
    std::copy_n(subject.elf_buffer, header.size(), header.begin());
    if (!elf_header_matches(header, expected) || crypto::sha256(subject.elf_buffer, subject.elf_buffer_size) != expected.sha256)
        return -ENOEXEC;
    return 0;
}

auto check_source_fd(const EvidenceLease& lease, int fd, const wire::ManifestEntry& expected, FileIdentity& output) noexcept -> int {
    if (!ordinary_backend_context(lease)) return -EWOULDBLOCK;
    auto& subject = *lease.subject();
    if (!host_process(subject) || fd < 0 || expected.role < static_cast<uint64_t>(wire::AssetRole::STATIC) ||
        expected.role > static_cast<uint64_t>(wire::AssetRole::INTERPRETER))
        return -EACCES;
    OwnedFile file;
    int result = vfs::vfs_get_file_retain_checked(&subject, fd, *file.slot());
    if (result != 0) return result;
    if ((static_cast<unsigned>(file.get()->open_flags) & 3U) != 0) return -EACCES;
    result = vfs::vfs_stabilize_exec_file(file.slot());
    if (result != 0) return result;
    FileIdentity actual{};
    result = check_image_file(*file.get(), expected, actual);
    int const CLOSE = file.finish();
    if (result == 0 && CLOSE == 0) output = actual;
    return result != 0 ? result : CLOSE;
}
}  // namespace ker::test::lifecycle60::evidence
#endif
