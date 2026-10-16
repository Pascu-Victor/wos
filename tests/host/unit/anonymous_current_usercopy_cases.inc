// Finite Host endpoints for current-copy and VFS boundaries. The actual
// anonymous preparer/service/factory is exercised by the separate prepared
// page-in driver; this file neither replaces that algorithm nor qualifies I/O.
#include <sys/types.h>
#include <time.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <new>
#include <utility>
namespace ker::mod::mm::paging {
constexpr uint64_t PAGE_SIZE = 4096;
struct PageTable {};
}  // namespace ker::mod::mm::paging
namespace ker::mod::mm::phys {
struct PageLookupHint {};
}  // namespace ker::mod::mm::phys
namespace ker::mod::sched::task {
struct Task {
    bool ordinary = true;
    unsigned accesses{};
    mm::paging::PageTable root;
    auto try_acquire_usercopy_pagemap(mm::paging::PageTable*& out) -> bool {
        ++accesses;
        out = &root;
        return true;
    }
    void release_usercopy_pagemap() {
        if (!accesses) std::abort();
        --accesses;
    }
};
}  // namespace ker::mod::sched::task
namespace fixture {
constexpr uint64_t BASE = 0x4000;
unsigned checks{}, pins{}, preparations{}, scopes{}, accesses{}, backend_calls{};
bool irq{}, incoming_irq{}, allow = true, commit_ok = true, mapping_ok = true;
int fail_page = -1, prepare_error{};
struct Page {
    std::array<uint8_t, 4096> data{};
    bool resident{}, tracked = true, dirty{};
};
std::array<Page, 3> pages;
ker::mod::sched::task::Task actor;
void check(bool value, const char* why) {
    ++checks;
    if (!value) {
        std::fprintf(stderr, "FAIL %s\n", why);
        std::exit(1);
    }
}
auto index(uint64_t address) -> size_t { return (address - BASE) / 4096; }
auto owned() -> bool { return actor.accesses == 0 && pins == 0 && accesses == 0; }
void reset() {
    check(owned() && scopes == 0, "previous copy owners closed");
    irq = incoming_irq = false;
    allow = true;
    commit_ok = mapping_ok = true;
    fail_page = -1;
    prepare_error = 0;
    preparations = backend_calls = 0;
    actor.ordinary = true;
    for (size_t p = 0; p < pages.size(); ++p) {
        pages[p] = Page{};
        for (size_t i = 0; i < 4096; ++i) pages[p].data[i] = static_cast<uint8_t>((p * 71 + i * 17 + 3) % 251);
    }
}
}  // namespace fixture
namespace ker::mod::sys {
class SyscallPreparationScope {
    bool previous_, ready_;

   public:
    explicit SyscallPreparationScope(sched::task::Task& t)
        : previous_(fixture::irq), ready_(&t == &fixture::actor && t.ordinary && fixture::allow) {
        ++fixture::scopes;
        if (ready_) fixture::irq = true;
    }
    ~SyscallPreparationScope() {
        fixture::irq = previous_;
        --fixture::scopes;
    }
    auto ready() const -> bool { return ready_; }
};
}  // namespace ker::mod::sys
namespace ker::mod::mm::virt {
constexpr uint64_t PADDR_INVALID = UINT64_MAX;
struct UserPagePin {
    void* hhdm_page{};
    uint64_t user_page{}, physical_page{};
    bool dirty{};
};
struct UserFutexIdentity {
    uint64_t virtual_word{};
};
enum class PublicReadPrepareResult { NOT_TRACKED, READY, REFUSED };
auto prepare_tmpfs_read_shared_page(sched::task::Task&, uint64_t, bool) -> PublicReadPrepareResult {
    return PublicReadPrepareResult::NOT_TRACKED;
}
auto prepare_current_anonymous_user_page(sched::task::Task& t, uint64_t address, bool, const sys::SyscallPreparationScope& scope) -> int {
    fixture::check(scope.ready() && fixture::irq && fixture::owned(), "preparation has IF and no caller owners");
    ++fixture::preparations;
    fixture::check(&t == &fixture::actor, "exact current actor");
    if (fixture::prepare_error) return fixture::prepare_error;
    auto p = fixture::index(address);
    if (p >= fixture::pages.size() || static_cast<int>(p) == fixture::fail_page) return -EAGAIN;
    fixture::pages[p].resident = true;
    return 0;
}
auto pin_user_page_for_task(sched::task::Task*, paging::PageTable*, uint64_t address, bool, bool fault, UserPagePin& out) -> bool {
    fixture::check(fixture::scopes == 0 && fixture::irq == fixture::incoming_irq, "old pin observes restored incoming IF");
    auto p = fixture::index(address);
    if (p >= fixture::pages.size() || static_cast<int>(p) == fixture::fail_page) return false;
    auto& page = fixture::pages[p];
    if (!page.resident) {
        if (!fault || page.tracked) return false;
        page.resident = true;
    }
    ++fixture::pins;
    out = {page.data.data(), address & ~uint64_t{4095}, p * 4096, page.dirty};
    return true;
}
void unpin_user_page(UserPagePin& pin) {
    if (pin.hhdm_page) {
        fixture::check(fixture::pins > 0, "pin ref exactly once");
        --fixture::pins;
    }
    pin = {};
}
auto user_page_pin_still_mapped(const UserPagePin&) -> bool { return fixture::mapping_ok; }
auto user_page_pin_commit_write(const UserPagePin& pin) -> bool {
    if (!fixture::commit_ok) return false;
    fixture::pages[pin.physical_page / 4096].dirty = true;
    return true;
}
}  // namespace ker::mod::mm::virt
#include <platform/sys/usercopy.hpp>
namespace ker::util {
void copy_fast(void* dst, const void* src, size_t count) {
    fixture::check(fixture::accesses == 1, "copy within access window");
    std::memcpy(dst, src, count);
}
}  // namespace ker::util
namespace ker::mod::sys::usercopy {
UserAccessGuard::UserAccessGuard() {
    fixture::check(fixture::irq == fixture::incoming_irq && fixture::scopes == 0, "copy scope restored IF");
    m_flags = fixture::irq;
    fixture::irq = false;
    ++fixture::accesses;
}
UserAccessGuard::~UserAccessGuard() {
    --fixture::accesses;
    fixture::irq = m_flags != 0;
}
ACTUAL_USERCOPY
}  // namespace ker::mod::sys::usercopy
namespace ker::vfs {
constexpr size_t USER_IO_BOUNCE_STACK_CHUNK = 16 * 1024, USER_IO_BOUNCE_MAX_CHUNK = 1024 * 1024,
                 USER_IO_REMOTE_READ_BOUNCE_MAX_CHUNK = 1024 * 1024;
enum class FSType { TMPFS, REMOTE };
struct File;
struct Ops {
    ssize_t (*vfs_read)(File*, void*, size_t, size_t);
    ssize_t (*vfs_write)(File*, const void*, size_t, size_t);
};
struct File {
    Ops* fops{};
    FSType fs_type = FSType::TMPFS;
    bool writable = true, readonly = false;
};
auto vfs_file_can_write(File* f) -> bool { return f->writable; }
auto file_mount_read_only(const File& f) -> bool { return f.readonly; }
std::array<uint8_t, 12288> backend;
size_t backend_size{}, last_offset{};
ssize_t read_limit = SSIZE_MAX, write_limit = SSIZE_MAX;
auto read(File*, void* dst, size_t count, size_t offset) -> ssize_t {
    fixture::check(!fixture::irq && fixture::scopes == 0 && fixture::owned(), "backend read outside preparation/pin");
    ++fixture::backend_calls;
    last_offset = offset;
    if (read_limit < 0) return read_limit;
    auto n = std::min({count, backend_size - offset, static_cast<size_t>(read_limit)});
    std::memcpy(dst, backend.data() + offset, n);
    return static_cast<ssize_t>(n);
}
auto write(File*, const void* src, size_t count, size_t offset) -> ssize_t {
    fixture::check(!fixture::irq && fixture::scopes == 0 && fixture::owned(), "backend write outside preparation/pin");
    ++fixture::backend_calls;
    last_offset = offset;
    if (write_limit < 0) return write_limit;
    auto n = std::min(count, static_cast<size_t>(write_limit));
    fixture::check(offset + n <= backend.size(), "bounded backend write");
    std::memcpy(backend.data() + offset, src, n);
    return static_cast<ssize_t>(n);
}
auto vfs_write_file_direct(File* f, const void* src, size_t n, size_t*) -> ssize_t { return write(f, src, n, 0); }
ACTUAL_VFS
}  // namespace ker::vfs
namespace ker::syscall {
namespace mod = ker::mod;
constexpr uint64_t NSEC_PER_SEC = 1000000000, USEC_PER_SEC = 1000000;
ACTUAL_TIMEOUT
}  // namespace ker::syscall
int main() {
    using namespace fixture;
    namespace uc = ker::mod::sys::usercopy;
    namespace v = ker::vfs;
    reset();
    std::array<uint8_t, 5000> out{};
    auto copy = uc::copy_from_current_task_partial(actor, BASE + 4000, out.data(), out.size());
    check(copy.complete(out.size()) && preparations == 3, "current partial read prepares each page");
    for (size_t i = 0; i < out.size(); ++i) check(out[i] == pages[(4000 + i) / 4096].data[(4000 + i) % 4096], "exact copied pattern");
    check(owned() && !irq, "copy read closes leases/IF");
    reset();
    irq = incoming_irq = true;
    check(uc::copy_from_current_task_partial(actor, BASE, out.data(), 4).complete(4) && irq, "enabled incoming IF restored");
    reset();
    fail_page = 1;
    copy = uc::copy_from_current_task_partial(actor, BASE + 4000, out.data(), 300);
    check(copy.fault && copy.bytes_copied == 96, "read prefix before second-page failure");
    check(owned(), "failed copy releases refs");
    reset();
    std::array<uint8_t, 300> input;
    input.fill(0xa7);
    fail_page = 1;
    copy = uc::copy_to_current_task_partial(actor, BASE + 4000, input.data(), input.size());
    check(copy.fault && copy.bytes_copied == 96 && pages[0].dirty && !pages[1].dirty, "write prefix dirty accounting");
    reset();
    commit_ok = false;
    copy = uc::copy_to_current_task_partial(actor, BASE, input.data(), input.size());
    check(copy.fault && copy.bytes_copied == 0 && !pages[0].dirty, "failed commit not reported");
    reset();
    mapping_ok = false;
    copy = uc::copy_from_current_task_partial(actor, BASE, out.data(), 20);
    check(copy.fault && copy.bytes_copied == 0, "stale read not reported");
    reset();
    {
        uc::StableUserPage old;
        pages[0].resident = true;
        check(uc::pin_task_user_page(actor, BASE, false, true, old), "old reused pin");
        check(uc::pin_current_task_user_page(actor, BASE + 4096, false, old), "reused pin reset before preparation");
    }
    check(owned(), "reused pin closed");
    reset();
    actor.ordinary = false;
    check(!uc::copy_from_current_task_partial(actor, BASE, out.data(), 4).complete(4) && preparations == 0,
          "foreign/nonordinary tracked marker remains refused");
    pages[0].resident = true;
    check(uc::copy_from_current_task_partial(actor, BASE, out.data(), 4).complete(4) && preparations == 0,
          "foreign resident old behavior retained");
    reset();
    check(!uc::copy_from_task_partial(actor, BASE, out.data(), 4).complete(4) && preparations == 0, "generic marker refusal preserved");
    check(uc::copy_from_current_task_partial(actor, UINT64_MAX, out.data(), 0).complete(0) && preparations == 0, "empty copy no work");
    check(!uc::copy_from_current_task_partial(actor, UINT64_MAX, out.data(), 4).complete(4) && preparations == 0,
          "bad geometry before prepare");
    v::Ops ops{v::read, v::write};
    v::File file{&ops};
    v::backend_size = v::backend.size();
    v::backend.fill(0x5e);
    v::read_limit = v::write_limit = SSIZE_MAX;
    size_t actual = 999;
    reset();
    prepare_error = -EAGAIN;
    check(v::vfs_read_user_bounced(actor, &file, reinterpret_cast<void*>(BASE), 40, 0, &actual) == -EFAULT && backend_calls == 0 &&
              actual == 999,
          "read preflight before backend consumption");
    reset();
    check(v::vfs_read_user_bounced(actor, &file, reinterpret_cast<void*>(BASE), 40, 0, &actual) == 40 && actual == 40 &&
              pages[0].data[0] == 0x5e && pages[0].dirty,
          "read actual data and dirty commit");
    reset();
    fail_page = 1;
    actual = 999;
    check(v::vfs_write_user_bounced(actor, &file, reinterpret_cast<void*>(BASE + 4000), 300, &actual) == 96 && actual == 96 &&
              backend_calls == 1,
          "write commits exact initialized prefix");
    for (size_t i = 0; i < 96; ++i) check(v::backend[i] == pages[0].data[4000 + i], "backend prefix pattern");
    reset();
    fail_page = 1;
    check(v::vfs_pwrite_user_bounced(actor, &file, reinterpret_cast<void*>(BASE + 4000), 300, 17) == 96 && v::last_offset == 17,
          "pwrite prefix preserves explicit offset");
    reset();
    prepare_error = -ENOMEM;
    check(v::vfs_write_user_bounced(actor, &file, reinterpret_cast<void*>(BASE), 20, &actual) == -EFAULT && backend_calls == 0,
          "write failure before backend");
    reset();
    uint64_t us = 77;
    check(ker::syscall::relative_timeout_us(actor, 0, us) == 0 && us == 0 && preparations == 0, "zero timeout no copy");
    timespec ts{2, 1999};
    std::memcpy(pages[0].data.data(), &ts, sizeof(ts));
    check(ker::syscall::relative_timeout_us(actor, BASE, us) == 0 && us == 2000002, "timeout actual current-copy and round up");
    reset();
    ts = {-1, 0};
    std::memcpy(pages[0].data.data(), &ts, sizeof(ts));
    check(ker::syscall::relative_timeout_us(actor, BASE, us) == -EINVAL && us == 0, "invalid timeout retains error priority");
    reset();
    prepare_error = -EAGAIN;
    check(ker::syscall::relative_timeout_us(actor, BASE, us) == -EFAULT && us == 0, "queued timeout maps to old EFAULT");
    check(owned() && scopes == 0 && !irq, "all owners and IF closed");
    std::printf("PASS actual current-copy/VFS/timeout checks=%u\n", checks);
}
