#ifdef WOS_SELFTEST
#include <algorithm>
#include <array>
#include <bit>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <net/wki/auth_crypto.hpp>
#include <platform/dbg/dbg.hpp>
#include <platform/ktime/ktime.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sched/task.hpp>
#include <test/compartment_lifecycle_census_report.hpp>
#include <test/compartment_lifecycle_evidence.hpp>
#include <utility>
#include <vfs/file.hpp>
#include <vfs/stat.hpp>
#include <vfs/vfs.hpp>

namespace ker::test::lifecycle60 {
namespace {
namespace wire = criterion9::wire;
namespace crypto = ker::net::wki::auth_crypto;
using log = mod::dbg::logger<"lifecycle">;
enum Report : size_t { RAW, CONTROL, ATTEMPTS_FILE, EVIDENCE_FILE, FD_LEDGER, STATUS, REPORTS };
constexpr std::array<const char*, REPORTS> PATHS{"/srv/lifecycle60/child.raw",     "/srv/lifecycle60/control.bin",
                                                 "/srv/lifecycle60/attempts.bin",  "/srv/lifecycle60/evidence.bin",
                                                 "/srv/lifecycle60/fd-ledger.bin", "/srv/lifecycle60/driver-status"};
constexpr std::array<uint64_t, REPORTS> SIZES{wire::CHILD_BYTES,        wire::CONTROL_BYTES, wire::LEDGER_BYTES,
                                              wire::ALL_EVIDENCE_BYTES, wire::FD_BYTES,      wire::STATUS_BYTES};
struct AttemptProof {
    wire::Record prefix{};
    crypto::Digest evidence_digest{};
    uint64_t cookie{};
    int64_t actual_create{};
    bool requested{}, terminal{};
};
struct EvidenceState {
    wire::Manifest manifest{};
    wire::Record header{}, row{}, other{}, status{};
    wire::FdLedger fd_ledger{}, candidate_ledger{};
    std::array<uint64_t, wire::CONTROL_BYTES / 8> control{};
    wire::Evidence archive{};
    std::array<uint64_t, wire::CHILD_BYTES / 8> raw{};
    std::array<uint64_t, 32> input_limits{};
    std::array<uint8_t, wire::FD_BYTES> comparison{};
    std::array<AttemptProof, ATTEMPTS> attempts{};
    std::array<evidence::FileIdentity, 3> sources{};
    std::array<evidence::FileIdentity, REPORTS> reports{};
    uint64_t init_pid{}, driver_pid{}, image_epoch{}, ledger_hash{}, last_cookie{};
    // Last entered gate/checkpoint, for bounded failure diagnosis after every
    // execution/census lease has ended. Values never select kernel fault stages.
    uint64_t diagnostic_gate{}, diagnostic_step{}, diagnostic_index{}, diagnostic_cookie{}, diagnostic_boundary{};
    uint64_t prefork_if{}, prefork_preempt{}, prefork_euid{UINT64_MAX};
    int64_t prefork_error{};
    bool manifest_loaded{}, admitted{}, final_records{}, input_limits_saved{};
};
// The one permanent controller owns this fixed storage, including all large
// scratch arrays. No stack-sized FD256/evidence copies or observer owners.
EvidenceState saved{};

auto io_context(const EvidenceLease& lease) noexcept -> bool {
    return lease && lease.subject() != nullptr && mod::sched::interrupts_enabled() && mod::sched::preempt_count() == 0;
}
auto signed_word(uint64_t word) noexcept -> int64_t { return std::bit_cast<int64_t>(word); }
auto same_identity(const vfs::Stat& a, const vfs::Stat& b) noexcept -> bool {
    return a.st_dev == b.st_dev && a.st_ino == b.st_ino && a.st_size == b.st_size && a.st_mode == b.st_mode && a.st_uid == b.st_uid &&
           a.st_gid == b.st_gid && a.st_nlink == b.st_nlink;
}
auto report_stat(vfs::File& file, uint64_t size, vfs::Stat& output) noexcept -> bool {
    return vfs::vfs_fstat_file_native(&file, &output) == 0 && output.st_mode == (vfs::S_IFREG | 0600U) && output.st_uid == 100 &&
           output.st_gid == 100 && output.st_nlink == 1 && output.st_size >= 0 && static_cast<uint64_t>(output.st_size) == size;
}
struct ReportFile {
    vfs::File* file{};
    vfs::Stat opened{};
    uint64_t size{};
    ReportFile() noexcept = default;
    ~ReportFile() {
        if (file != nullptr) vfs::vfs_put_file(file);
    }
    ReportFile(const ReportFile&) = delete;
    auto operator=(const ReportFile&) -> ReportFile& = delete;
    auto open(Task& actor, size_t index) noexcept -> bool {
        if (file != nullptr || index >= REPORTS) return false;
        size = SIZES[index];
        return vfs::vfs_open_read_file_as(actor, actor.credentials(), PATHS[index], false, &file) == 0 && file != nullptr &&
               report_stat(*file, size, opened);
    }
    template <typename T>
    auto read(T& output, uint64_t offset = 0) noexcept -> bool {
        static_assert(sizeof(T) <= wire::FD_BYTES);
        if (file == nullptr || offset > size || sizeof(T) > size - offset ||
            vfs::vfs_pread_file_direct(file, &output, sizeof(T), static_cast<off_t>(offset)) != static_cast<ssize_t>(sizeof(T)) ||
            vfs::vfs_pread_file_direct(file, saved.comparison.data(), sizeof(T), static_cast<off_t>(offset)) !=
                static_cast<ssize_t>(sizeof(T)) ||
            std::memcmp(&output, saved.comparison.data(), sizeof(T)) != 0)
            return false;
        vfs::Stat now{};
        return report_stat(*file, size, now) && same_identity(opened, now);
    }
    auto finish() noexcept -> bool {
        auto* owned = std::exchange(file, nullptr);
        return owned == nullptr || vfs::vfs_put_file_result(owned) == 0;
    }
};
using Files = std::array<ReportFile, REPORTS>;
auto open_reports(Task& actor, Files& files) noexcept -> bool {
    for (size_t i = 0; i < files.size(); ++i)
        if (!files[i].open(actor, i)) return false;
    return true;
}
auto close_reports(Files& files) noexcept -> bool {
    bool good = true;
    for (auto& file : files) good = file.finish() && good;
    return good;
}
auto report_identities_unchanged(const Files& files) noexcept -> bool {
    for (size_t i = 0; i < files.size(); ++i) {
        auto const& actual = files[i].opened;
        auto const& admitted = saved.reports[i];
        if (actual.st_dev != admitted.device || actual.st_ino != admitted.inode || static_cast<uint64_t>(actual.st_size) != admitted.bytes)
            return false;
    }
    return true;
}
auto header_valid(const wire::Record& h, uint64_t actor) noexcept -> bool {
    if (h[wire::H_MAGIC] != wire::HEADER_MAGIC || h[wire::H_VERSION] != wire::VERSION || h[wire::H_BYTES] != 512 ||
        h[wire::H_NONCE] != saved.manifest.nonce || h[wire::H_DRIVER_PID] != actor || h[wire::H_PARENT_PID] != saved.init_pid ||
        h[wire::H_ATTEMPTS] != ATTEMPTS || h[wire::H_WARMUPS] != WARMUPS || h[wire::H_FAULTS] != FAULTS ||
        h[wire::H_SUCCESSES] != SUCCESSES || h[wire::H_RECORD_BYTES] != wire::RECORD_BYTES ||
        h[wire::H_EVIDENCE_BYTES] != wire::EVIDENCE_BYTES || h[wire::H_CHILD_BYTES] != wire::CHILD_BYTES ||
        h[wire::H_CONTROL_BYTES] != wire::CONTROL_BYTES || h[wire::H_FD_BYTES] != wire::FD_BYTES ||
        h[wire::H_STATUS_BYTES] != wire::STATUS_BYTES || h[wire::H_START_US] == 0 || h[wire::H_START_US] > UINT64_MAX - wire::CAMPAIGN_US ||
        h[wire::H_DEADLINE_US] != h[wire::H_START_US] + wire::CAMPAIGN_US || h[wire::H_START_US] > mod::time::get_us() || !wire::sealed(h))
        return false;
    for (size_t i = 18; i < 24; ++i)
        if (h[i] != 0) return false;
    for (size_t i = 33; i < wire::CHECKSUM; ++i)
        if (h[i] != 0) return false;
    for (size_t i = 0; i < 9; ++i) {
        uint64_t const FD = h[wire::H_STATIC_FD + i];
        if (FD >= 256) return false;
        for (size_t j = 0; j < i; ++j)
            if (h[wire::H_STATIC_FD + j] == FD) return false;
    }
    return true;
}
auto ledger_valid(const wire::FdLedger& data, const wire::Record& header) noexcept -> bool {
    wire::Record leading{};
    std::copy_n(data.begin(), leading.size(), leading.begin());
    if (leading[0] != wire::FD_MAGIC || leading[1] != wire::VERSION || leading[2] != wire::FD_BYTES || leading[3] != saved.manifest.nonce ||
        leading[4] != saved.driver_pid || leading[5] != 9 || leading[6] != 256 || leading[7] != 2 || !wire::sealed(leading))
        return false;
    for (size_t i = 8; i < wire::CHECKSUM; ++i)
        if (leading[i] != 0) return false;
    for (size_t i = 576; i < data.size(); ++i)
        if (data[i] != 0) return false;
    for (size_t fd = 0; fd < 256; ++fd) {
        size_t role = 9;
        for (size_t i = 0; i < 9; ++i)
            if (header[wire::H_STATIC_FD + i] == fd) role = i;
        if (role == 9) {
            if (signed_word(data[64 + fd]) != -9 || signed_word(data[320 + fd]) != -9) return false;
        } else if (data[64 + fd] != 1 || signed_word(data[320 + fd]) < 0 || (data[320 + fd] & 3U) != (role < 3 ? 0U : 2U) ||
                   (data[320 + fd] & vfs::O_NO_CACHE) == 0)
            return false;
    }
    return true;
}
auto no_shared_executor(Task& actor) noexcept -> bool {
    auto const ids = actor.credentials();
    auto const u = ids.user(), g = ids.group();
    if (actor.type != mod::sched::task::TaskType::PROCESS || actor.is_thread || actor.wki_proxy_task || u.real != 100 ||
        u.effective != 100 || u.saved != 100 || g.real != 100 || g.effective != 100 || g.saved != 100 || ids.service_privileges() != 0 ||
        actor.shares_user_pagemap.load(std::memory_order_acquire) || actor.resource_cpu_threads.load(std::memory_order_acquire) != 0 ||
        actor.resource_thread_owners.load(std::memory_order_acquire) != 0 || actor.pending_wki_fd_snapshots != nullptr ||
        actor.pending_wki_submissions != nullptr || actor.pending_signal_targets != nullptr ||
        actor.pending_wki_submit_buffers != nullptr || actor.pending_wki_filesystems != nullptr ||
        actor.pending_fd_reservations != nullptr || actor.pending_process_reservations != nullptr)
        return false;
    auto const* pid = actor.pid_namespace();
    auto const* ipc = actor.ipc_namespace();
    auto const* mount = actor.mount_namespace();
    return pid != nullptr && pid->is_host() && ipc != nullptr && ipc->is_host() && mount != nullptr && mount->is_host();
}
auto actual_descriptors(Task& actor, Files& files) noexcept -> bool {
    std::array<uint64_t, 9> identities{};
    uint64_t const FLAGS = actor.fd_table_lock.lock_irqsave();
    bool table = actor.fd_table.size() == 9 && actor.fd_table.reserved_count() == 0;
    for (size_t fd = 0; fd < 256; ++fd) {
        bool expected = false;
        for (size_t i = 0; i < 9; ++i) expected |= saved.header[wire::H_STATIC_FD + i] == fd;
        table &= (actor.fd_table.lookup(fd) != nullptr) == expected;
        if (expected) table &= actor.get_fd_cloexec(static_cast<unsigned>(fd));
    }
    actor.fd_table_lock.unlock_irqrestore(FLAGS);
    if (!table) return false;
    for (size_t i = 0; i < 9; ++i) {
        vfs::File* file = nullptr;
        if (vfs::vfs_get_file_retain_checked(&actor, static_cast<int>(saved.header[wire::H_STATIC_FD + i]), file) != 0) return false;
        identities[i] = reinterpret_cast<uint64_t>(file);
        bool good = true;
        for (size_t j = 0; j < i; ++j) good &= identities[i] != identities[j];
        // All nine descriptions are private; only the verified child shares
        // raw during execution, and that image never mutates status flags.
        good &= (static_cast<unsigned>(file->open_flags) & 3U) == (i < 3 ? 0U : 2U);
        good &= (file->open_flags & vfs::O_NO_CACHE) != 0;
        good &= file->open_flags == signed_word(saved.candidate_ledger[320 + saved.header[wire::H_STATIC_FD + i]]);
        if (i >= 3) {
            vfs::Stat st{};
            good &= report_stat(*file, SIZES[i - 3], st) && same_identity(st, files[i - 3].opened);
        }
        int const CLOSE = vfs::vfs_put_file_result(file);
        if (!good || CLOSE != 0) return false;
    }
    return true;
}
auto load_archive(Files& files, uint64_t index) noexcept -> bool {
    return index < ATTEMPTS && files[EVIDENCE_FILE].read(saved.archive, index * wire::EVIDENCE_BYTES);
}
auto read_control(Files& files) noexcept -> bool {
    if (!files[CONTROL].read(saved.control)) return false;
    for (size_t i = 2 * wire::RECORD_BYTES / 8; i < saved.control.size(); ++i)
        if (saved.control[i] != 0) return false;
    std::copy_n(saved.control.begin(), saved.other.size(), saved.other.begin());
    return true;
}
auto unused_work_fields_zero(const wire::Record& row) noexcept -> bool {
    for (size_t i = wire::READY_OBS_US; i <= wire::KILL_START_US; ++i)
        if (row[i] != 0) return false;
    for (size_t i = wire::READY_HASH; i <= wire::HB_B_HASH; ++i)
        if (row[i] != 0) return false;
    return row[wire::WAIT_STATUS] == 0 && row[wire::WAIT_DONE_US] == 0 && row[wire::WAIT_ZERO] == 0 && row[wire::WAIT_EINTR] == 0;
}
auto parts_valid(const wire::Record& row, bool work) noexcept -> bool {
    wire::Input input{};
    wire::Ready ready{};
    wire::Heartbeat a{}, b{};
    std::copy_n(saved.archive.begin(), input.size(), input.begin());
    std::copy_n(saved.archive.begin() + wire::READY_OFFSET / 8, ready.size(), ready.begin());
    std::copy_n(saved.archive.begin() + wire::HEARTBEAT_A_OFFSET / 8, a.size(), a.begin());
    std::copy_n(saved.archive.begin() + wire::HEARTBEAT_B_OFFSET / 8, b.size(), b.begin());
    for (size_t i = 3072 / 8; i < saved.archive.size(); ++i)
        if (saved.archive[i] != 0) return false;
    if (!wire::valid_input(input, row, saved.driver_pid) || input[58] != saved.fd_ledger[320 + saved.header[wire::H_CHILD_FD]])
        return false;
    if (saved.input_limits_saved) {
        for (size_t i = 0; i < saved.input_limits.size(); ++i)
            if (input[16 + i] != saved.input_limits[i]) return false;
    } else {
        if (row[wire::INDEX] != 0 || work || row[wire::STATE] != static_cast<uint64_t>(wire::State::REQUEST)) return false;
        std::copy_n(input.begin() + 16, saved.input_limits.size(), saved.input_limits.begin());
        saved.input_limits_saved = true;
    }
    if (work) return wire::valid_work_evidence(input, ready, a, b, row, saved.manifest, saved.driver_pid);
    return std::ranges::all_of(ready, [](uint64_t v) { return v == 0; }) && std::ranges::all_of(a, [](uint64_t v) { return v == 0; }) &&
           std::ranges::all_of(b, [](uint64_t v) { return v == 0; });
}
auto raw_matches(Files& files, bool work) noexcept -> bool {
    if (!files[RAW].read(saved.raw)) return false;
    for (size_t i = 0; i < 512 / 8; ++i)
        if (saved.raw[i] != saved.archive[i]) return false;
    if (!work) return std::ranges::all_of(saved.raw.begin() + 512 / 8, saved.raw.end(), [](uint64_t v) { return v == 0; });
    for (size_t i = 0; i < 2048 / 8; ++i)
        if (saved.raw[4096 / 8 + i] != saved.archive[wire::READY_OFFSET / 8 + i]) return false;
    for (size_t i = 512 / 8; i < saved.raw.size(); ++i) {
        if ((i >= 1024 / 8 && i < 1280 / 8) || (i >= 4096 / 8 && i < 6144 / 8)) continue;
        if (saved.raw[i] != 0) return false;
    }
    return true;  // The final mutable heartbeat is intentionally uncredited.
}
auto immutable_request_fields(const wire::Record& a, const wire::Record& b) noexcept -> bool {
    for (size_t i = 0; i < wire::STATE; ++i)
        if (a[i] != b[i]) return false;
    for (size_t i : {size_t{wire::START_US}, size_t{wire::DEADLINE_US}, size_t{wire::INPUT_HASH}, size_t{wire::RAW_DEV},
                     size_t{wire::RAW_INO}, size_t{wire::RAW_FD}, size_t{wire::FD_LEDGER_HASH}})
        if (a[i] != b[i]) return false;
    for (size_t i = wire::MAIN_DEV; i <= wire::PARENT_PID; ++i)
        if (a[i] != b[i]) return false;
    return true;
}
auto source_fields(const wire::Record& row) noexcept -> bool {
    auto const mode = wire::variant(row[wire::INDEX]);
    auto const& main = saved.sources[mode - 1];
    auto const& raw = saved.reports[RAW];
    if (row[wire::RAW_DEV] != raw.device || row[wire::RAW_INO] != raw.inode || row[wire::RAW_FD] != saved.header[wire::H_CHILD_FD] ||
        row[wire::FD_LEDGER_HASH] != saved.ledger_hash || row[wire::MAIN_DEV] != main.device || row[wire::MAIN_INO] != main.inode ||
        row[wire::MAIN_BYTES] != main.bytes || row[wire::START_US] < saved.header[wire::H_START_US] ||
        row[wire::START_US] > mod::time::get_us() || row[wire::DEADLINE_US] >= saved.header[wire::H_DEADLINE_US])
        return false;
    return mode == 2 ? row[wire::INTERP_DEV] == saved.sources[2].device && row[wire::INTERP_INO] == saved.sources[2].inode &&
                           row[wire::INTERP_BYTES] == saved.sources[2].bytes
                     : row[wire::INTERP_DEV] == 0 && row[wire::INTERP_INO] == 0 && row[wire::INTERP_BYTES] == 0;
}
auto completed(Files& files, uint64_t index) noexcept -> bool {
    if (index >= ATTEMPTS || !saved.attempts[index].terminal || !files[ATTEMPTS_FILE].read(saved.row, index * wire::RECORD_BYTES) ||
        !load_archive(files, index) || !wire::record_keys(saved.row, saved.manifest.nonce, index, wire::State::COMPLETED) ||
        !source_fields(saved.row) || !immutable_request_fields(saved.attempts[index].prefix, saved.row) ||
        saved.row[wire::FAILURE_STAGE] != 0 || saved.row[wire::CHECKS] == 0 ||
        saved.row[wire::END_US] < saved.row[wire::CREATE_RETURN_US] || saved.row[wire::CREATE_RETURN_US] < saved.row[wire::START_US] ||
        saved.row[wire::END_US] >= saved.row[wire::DEADLINE_US] || saved.row[wire::END_US] > mod::time::get_us() ||
        crypto::sha256(saved.archive.data(), sizeof(saved.archive)) != saved.attempts[index].evidence_digest)
        return false;
    bool const FAULT = wire::kind(index) == wire::Kind::FAULT;
    if (!parts_valid(saved.row, !FAULT) || signed_word(saved.row[wire::CREATE_RAW]) != saved.attempts[index].actual_create) return false;
    if (FAULT) {
        if (saved.attempts[index].actual_create != wire::expected_error(index) || saved.row[wire::PID] != 0 ||
            signed_word(saved.row[wire::KILL_RAW]) != wire::UNOBSERVED || signed_word(saved.row[wire::WAIT_RAW]) != wire::UNOBSERVED ||
            !unused_work_fields_zero(saved.row))
            return false;
    } else {
        auto const& prefix = saved.attempts[index].prefix;
        for (size_t i = wire::CREATE_RAW; i <= wire::KILL_RAW; ++i)
            if (saved.row[i] != prefix[i]) return false;
        for (size_t i = wire::INPUT_HASH; i <= wire::PARENT_PID; ++i)
            if (saved.row[i] != prefix[i]) return false;
        if (signed_word(saved.row[wire::WAIT_RAW]) != saved.attempts[index].actual_create || saved.row[wire::WAIT_STATUS] != 9 ||
            saved.row[wire::WAIT_DONE_US] < saved.row[wire::KILL_START_US] || saved.row[wire::WAIT_DONE_US] > saved.row[wire::END_US])
            return false;
    }
    return true;
}
}  // namespace

namespace evidence {
auto driver_wire_header() noexcept -> const wire::Record* { return saved.admitted ? &saved.header : nullptr; }
auto driver_wire_fd_ledger() noexcept -> const wire::FdLedger* { return saved.admitted ? &saved.fd_ledger : nullptr; }
}  // namespace evidence

auto validate_prefork_evidence(const EvidenceLease& lease) noexcept -> GateResult {
    saved.diagnostic_gate = 1;
    saved.diagnostic_step = 0;
    saved.prefork_if = mod::sched::interrupts_enabled() ? 1 : 0;
    saved.prefork_preempt = mod::sched::preempt_count();
    saved.prefork_euid = lease.subject() != nullptr ? lease.subject()->credentials().user().effective : UINT64_MAX;
    saved.prefork_error = 0;
    if (!io_context(lease)) {
        saved.diagnostic_step = 10;
        saved.prefork_error = -EWOULDBLOCK;
        return GateResult::REFUSE;
    }
    if (saved.manifest_loaded) {
        saved.diagnostic_step = 11;
        saved.prefork_error = -EALREADY;
        return GateResult::REFUSE;
    }
    if (saved.prefork_euid != 0) {
        saved.diagnostic_step = 12;
        saved.prefork_error = -EACCES;
        return GateResult::REFUSE;
    }
    saved.diagnostic_step = 13;
    int const RESULT = evidence::read_manifest(lease, saved.manifest);
    saved.prefork_error = RESULT;
    if (RESULT != 0) return GateResult::REFUSE;
    saved.init_pid = lease.subject()->pid;
    saved.manifest_loaded = true;
    saved.diagnostic_step = 1;
    return GateResult::ACCEPT;
}

auto admit_driver_image_and_sharing(const EvidenceLease& lease, uint64_t image_epoch) noexcept -> GateResult {
    saved.diagnostic_gate = 2;
    saved.diagnostic_step = 0;
    if (!io_context(lease) || !saved.manifest_loaded || !no_shared_executor(*lease.subject()) ||
        evidence::check_driver_image(lease, saved.manifest.entries[0]) != 0)
        return GateResult::REFUSE;
    auto& actor = *lease.subject();
    saved.diagnostic_step = 1;
    if (saved.admitted && (actor.pid != saved.driver_pid || image_epoch != saved.image_epoch)) return GateResult::REFUSE;
    Files files{};
    if (!open_reports(actor, files) || !read_control(files) || !header_valid(saved.other, actor.pid)) return GateResult::REFUSE;
    if (saved.admitted && saved.other != saved.header) return GateResult::REFUSE;
    saved.header = saved.other;
    saved.driver_pid = actor.pid;
    saved.diagnostic_step = 2;
    if (!files[FD_LEDGER].read(saved.candidate_ledger) || !ledger_valid(saved.candidate_ledger, saved.header) ||
        (saved.admitted && saved.candidate_ledger != saved.fd_ledger) || !actual_descriptors(actor, files))
        return GateResult::REFUSE;
    uint64_t const LEDGER = wire::hash_words(saved.candidate_ledger);
    if (saved.admitted && LEDGER != saved.ledger_hash) return GateResult::REFUSE;
    for (size_t i = 0; i < 3; ++i) {
        saved.diagnostic_step = 3 + i;
        evidence::FileIdentity actual{};
        if (evidence::check_source_fd(lease, static_cast<int>(saved.header[wire::H_STATIC_FD + i]), saved.manifest.entries[i + 1],
                                      actual) != 0 ||
            (saved.admitted && actual != saved.sources[i]))
            return GateResult::REFUSE;
        saved.sources[i] = actual;
    }
    for (size_t i = 0; i < REPORTS; ++i) {
        saved.diagnostic_step = 6;
        auto const& st = files[i].opened;
        evidence::FileIdentity actual{.device = st.st_dev, .inode = st.st_ino, .bytes = static_cast<uint64_t>(st.st_size)};
        if (saved.admitted && actual != saved.reports[i]) return GateResult::REFUSE;
        saved.reports[i] = actual;
    }
    if (!close_reports(files)) return GateResult::REFUSE;
    saved.fd_ledger = saved.candidate_ledger;
    saved.ledger_hash = LEDGER;
    saved.image_epoch = image_epoch;
    saved.admitted = true;
    saved.diagnostic_step = 7;
    return GateResult::ACCEPT;
}

auto validate_attempt_evidence(const EvidenceLease& lease, const AttemptView& view, Boundary boundary) noexcept -> GateResult {
    saved.diagnostic_gate = 3;
    saved.diagnostic_step = 0;
    saved.diagnostic_index = view.index;
    saved.diagnostic_cookie = view.cookie;
    saved.diagnostic_boundary = static_cast<uint64_t>(boundary);
    if (!io_context(lease) || !saved.admitted || lease.subject()->pid != saved.driver_pid || view.index >= ATTEMPTS || view.cookie == 0 ||
        (boundary != Boundary::BEFORE_CREATE && boundary != Boundary::AFTER_FAILURE && boundary != Boundary::AFTER_WAIT))
        return GateResult::REFUSE;
    Files files{};
    if (!open_reports(*lease.subject(), files) || !read_control(files) || saved.other != saved.header) return GateResult::REFUSE;
    std::copy_n(saved.control.begin() + wire::REQUEST_OFFSET / 8, saved.row.size(), saved.row.begin());
    if (!files[ATTEMPTS_FILE].read(saved.other, view.index * wire::RECORD_BYTES) || saved.row != saved.other ||
        !load_archive(files, view.index))
        return GateResult::REFUSE;
    bool const WORK = boundary == Boundary::AFTER_WAIT;
    saved.diagnostic_step = 1;
    auto const state = WORK ? wire::State::BEFORE_WAIT : wire::State::REQUEST;
    if (!wire::record_keys(saved.row, saved.manifest.nonce, view.index, state) || !source_fields(saved.row) ||
        !parts_valid(saved.row, WORK) || !raw_matches(files, WORK) || saved.row[wire::FAILURE_STAGE] != 0)
        return GateResult::REFUSE;
    auto& proof = saved.attempts[view.index];
    saved.diagnostic_step = 2;
    if (boundary == Boundary::BEFORE_CREATE) {
        if (proof.requested || view.cookie <= saved.last_cookie || view.create_returned || view.consumed || view.wait_consumed ||
            signed_word(saved.row[wire::CREATE_RAW]) != wire::UNOBSERVED || saved.row[wire::PID] != 0 ||
            signed_word(saved.row[wire::KILL_RAW]) != wire::UNOBSERVED || signed_word(saved.row[wire::WAIT_RAW]) != wire::UNOBSERVED ||
            !unused_work_fields_zero(saved.row) || saved.row[wire::CREATE_RETURN_US] != 0 || saved.row[wire::END_US] != 0 ||
            saved.row[wire::CHECKS] != 0 || view.expected_error != wire::expected_error(view.index) ||
            view.warmup != (view.index < WARMUPS))
            return GateResult::REFUSE;
        proof.prefix = saved.row;
        proof.cookie = view.cookie;
        proof.requested = true;
        saved.last_cookie = view.cookie;
    } else {
        if (!proof.requested || proof.terminal || proof.cookie != view.cookie || !view.create_returned ||
            !immutable_request_fields(proof.prefix, saved.row))
            return GateResult::REFUSE;
        if (WORK) {
            if (wire::kind(view.index) == wire::Kind::FAULT || view.consumed || !view.wait_consumed || view.wait_status != 9 ||
                view.create_result <= 2 || saved.row[wire::CREATE_RAW] != static_cast<uint64_t>(view.create_result) ||
                saved.row[wire::PID] != static_cast<uint64_t>(view.create_result) ||
                view.visible_pid != static_cast<uint64_t>(view.create_result) || signed_word(saved.row[wire::KILL_RAW]) != 0 ||
                signed_word(saved.row[wire::WAIT_RAW]) != wire::UNOBSERVED || saved.row[wire::WAIT_STATUS] != 0 ||
                saved.row[wire::WAIT_DONE_US] != 0 || saved.row[wire::WAIT_ZERO] != 0 || saved.row[wire::WAIT_EINTR] != 0 ||
                saved.row[wire::END_US] != 0 || saved.row[wire::CHECKS] == 0 ||
                saved.row[wire::CREATE_RETURN_US] < saved.row[wire::START_US] || saved.row[wire::KILL_START_US] > mod::time::get_us())
                return GateResult::REFUSE;
            for (size_t i = 0; i < view.index; ++i)
                if (saved.attempts[i].terminal && saved.attempts[i].actual_create == view.create_result) return GateResult::REFUSE;
        } else if (wire::kind(view.index) != wire::Kind::FAULT || !view.consumed || view.wait_consumed ||
                   view.create_result != view.expected_error || view.expected_error != wire::expected_error(view.index) ||
                   saved.row != proof.prefix)
            return GateResult::REFUSE;
        proof.prefix = saved.row;
        proof.actual_create = view.create_result;
        proof.evidence_digest = crypto::sha256(saved.archive.data(), sizeof(saved.archive));
        proof.terminal = true;
    }
    if (!close_reports(files)) return GateResult::REFUSE;
    saved.diagnostic_step = 3;
    return GateResult::ACCEPT;
}

auto validate_final_evidence(const EvidenceLease& lease, const CampaignSummary& summary) noexcept -> GateResult {
    saved.diagnostic_gate = 4;
    saved.diagnostic_step = 0;
    if (!io_context(lease) || !saved.admitted || lease.subject()->pid != saved.init_pid || summary.warmups != WARMUPS ||
        summary.faults != FAULTS || summary.successes != SUCCESSES || summary.consumed_faults != FAULTS ||
        summary.actual_wait9 != WARMUPS + SUCCESSES || !summary.original_init_reaped_driver || !summary.all_attempt_task_debts_empty ||
        summary.last_cookie != saved.last_cookie)
        return GateResult::REFUSE;
    Files files{};
    if (!open_reports(*lease.subject(), files) || !report_identities_unchanged(files) || !files[FD_LEDGER].read(saved.candidate_ledger) ||
        saved.candidate_ledger != saved.fd_ledger || !ledger_valid(saved.candidate_ledger, saved.header) || !read_control(files) ||
        saved.other != saved.header || !files[STATUS].read(saved.status))
        return GateResult::REFUSE;
    auto const& s = saved.status;
    saved.diagnostic_step = 1;
    if (s[0] != wire::STATUS_MAGIC || s[1] != wire::VERSION || s[2] != wire::STATUS_BYTES || s[3] != saved.manifest.nonce ||
        s[4] != ATTEMPTS || s[5] != WARMUPS || s[6] != FAULTS || s[7] != SUCCESSES || s[8] != ATTEMPTS ||
        s[9] != saved.header[wire::H_START_US] || s[10] != saved.header[wire::H_DEADLINE_US] || s[11] != saved.driver_pid ||
        s[12] != saved.init_pid || s[13] != 0 || s[14] != 0 || s[15] == 0 || s[16] < s[9] || s[16] >= s[10] ||
        s[16] > mod::time::get_us() || s[17] != saved.ledger_hash || !wire::sealed(s))
        return GateResult::REFUSE;
    for (size_t i = 18; i < wire::CHECKSUM; ++i)
        if (s[i] != 0) return GateResult::REFUSE;
    uint64_t previous_end = saved.header[wire::H_START_US];
    saved.diagnostic_step = 2;
    uint64_t previous_checks = 0;
    for (uint64_t index = 0; index < ATTEMPTS; ++index) {
        saved.diagnostic_index = index;
        if (!completed(files, index) || saved.row[wire::START_US] < previous_end || saved.row[wire::CHECKS] <= previous_checks)
            return GateResult::REFUSE;
        previous_end = saved.row[wire::END_US];
        previous_checks = saved.row[wire::CHECKS];
    }
    if (s[15] < previous_checks || s[16] < previous_end || !raw_matches(files, true)) return GateResult::REFUSE;
    for (size_t i = 0; i < saved.row.size(); ++i)
        if (saved.control[wire::REQUEST_OFFSET / 8 + i] != saved.row[i]) return GateResult::REFUSE;
    if (!close_reports(files)) return GateResult::REFUSE;
    saved.final_records = true;
    saved.diagnostic_step = 3;
    return GateResult::ACCEPT;
}

void publish_campaign_result(const CampaignSummary& summary, bool passed) noexcept {
    bool const QUALIFIED = passed && saved.final_records && saved.manifest_loaded && saved.admitted && summary.warmups == WARMUPS &&
                           summary.faults == FAULTS && summary.successes == SUCCESSES && summary.consumed_faults == FAULTS &&
                           summary.actual_wait9 == WARMUPS + SUCCESSES && summary.original_init_reaped_driver &&
                           summary.all_attempt_task_debts_empty;
    log::info(
        "%s lifecycle-runtime nonce=%016llx warmups=%llu faults=%llu successes=%llu consumed=%llu wait9=%llu init-reap=%u debts-empty=%u "
        "records=%u",
        QUALIFIED ? "PASS" : "FAIL", static_cast<unsigned long long>(saved.manifest.nonce),
        static_cast<unsigned long long>(summary.warmups), static_cast<unsigned long long>(summary.faults),
        static_cast<unsigned long long>(summary.successes), static_cast<unsigned long long>(summary.consumed_faults),
        static_cast<unsigned long long>(summary.actual_wait9), static_cast<unsigned>(summary.original_init_reaped_driver),
        static_cast<unsigned>(summary.all_attempt_task_debts_empty), static_cast<unsigned>(saved.final_records));
    log::info("lifecycle-evidence gate=%llu step=%llu index=%llu cookie=%llu boundary=%llu manifest=%u admitted=%u limits=%u",
              static_cast<unsigned long long>(saved.diagnostic_gate), static_cast<unsigned long long>(saved.diagnostic_step),
              static_cast<unsigned long long>(saved.diagnostic_index), static_cast<unsigned long long>(saved.diagnostic_cookie),
              static_cast<unsigned long long>(saved.diagnostic_boundary), static_cast<unsigned>(saved.manifest_loaded),
              static_cast<unsigned>(saved.admitted), static_cast<unsigned>(saved.input_limits_saved));
    if (!QUALIFIED)
        log::info("lifecycle-prefork-io if=%llu preempt=%llu euid=%llu error=%lld", static_cast<unsigned long long>(saved.prefork_if),
                  static_cast<unsigned long long>(saved.prefork_preempt), static_cast<unsigned long long>(saved.prefork_euid),
                  static_cast<long long>(saved.prefork_error));
    publish_census_report();
}
}  // namespace ker::test::lifecycle60
#endif
