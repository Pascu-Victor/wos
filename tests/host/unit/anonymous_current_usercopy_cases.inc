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
namespace ker::mod::debug::ptrace {
class PendingOperation;
}
namespace ker::mod::sched::task {
enum class TaskType { PROCESS, DAEMON };
enum class TaskState { ACTIVE, EXITING, DEAD };
struct Task {
    TaskType type = TaskType::PROCESS;
    std::atomic<TaskState> state{TaskState::ACTIVE};
    std::atomic<bool> scheduler_transition_active{}, exit_in_progress{}, process_exit_requested{}, usercopy_pagemap_closing{};
    bool has_exited{}, deferred_task_switch{}, wants_block{}, voluntary_block{};
    auto is_voluntary_blocked() const -> bool { return voluntary_block; }
    uint64_t syscall_account_start_us{1};
    unsigned accesses{};
    std::atomic<uint32_t> usercopy_pagemap_accesses{};
    debug::ptrace::PendingOperation* pending_ptrace_operations{};
    mm::paging::PageTable root;
    mm::paging::PageTable replacement;
    mm::paging::PageTable* pagemap{&root};
    auto try_acquire_usercopy_pagemap(mm::paging::PageTable*& out) -> bool {
        out = nullptr;
        if (usercopy_pagemap_closing || pagemap == nullptr) return false;
        ++accesses;
        ++usercopy_pagemap_accesses;
        out = pagemap;
        return true;
    }
    void release_usercopy_pagemap() {
        if (!accesses) std::abort();
        --accesses;
        --usercopy_pagemap_accesses;
    }
};
}  // namespace ker::mod::sched::task
namespace fixture {
constexpr uint64_t BASE = 0x4000;
unsigned checks{}, pins{}, preparations{}, scopes{}, accesses{}, backend_calls{};
bool irq{}, incoming_irq{}, allow = true, commit_ok = true, mapping_ok = true;
bool ac{};
bool queryable = true, run_queues = true;
unsigned preempt{}, yields{}, busy_attempts{}, typed_attempts{}, legacy_attempts{};
unsigned mapped_attempts{}, faulting_attempts{}, mapped_refusals{}, scope_entries{};
void (*on_yield)() = nullptr;
std::array<ker::mod::mm::paging::PageTable*, 32> attempted_roots{};
int fail_page = -1, prepare_error{};
struct Page {
    std::array<uint8_t, 4096> data{};
    bool resident{}, tracked = true, dirty{};
};
std::array<Page, 3> pages;
ker::mod::sched::task::Task actor;
ker::mod::sched::task::Task foreign;
ker::mod::sched::task::Task* current = &actor;
ker::mod::sched::task::Task* return_task = &actor;
void check(bool value, const char* why) {
    ++checks;
    if (!value) {
        std::fprintf(stderr, "FAIL %s\n", why);
        std::exit(1);
    }
}
auto index(uint64_t address) -> size_t { return (address - BASE) / 4096; }
auto owned() -> bool { return actor.accesses == 0 && foreign.accesses == 0 && pins == 0 && accesses == 0; }
struct ScopeObservation {
    ScopeObservation() {
        ++scopes;
        ++scope_entries;
    }
    ~ScopeObservation() { --scopes; }
};
void reset() {
    check(owned() && scopes == 0, "previous copy owners closed");
    irq = incoming_irq = false;
    ac = false;
    allow = true;
    commit_ok = mapping_ok = true;
    fail_page = -1;
    prepare_error = 0;
    preparations = backend_calls = 0;
    queryable = true;
    run_queues = true;
    current = &actor;
    return_task = &actor;
    preempt = yields = busy_attempts = typed_attempts = legacy_attempts = 0;
    mapped_attempts = faulting_attempts = mapped_refusals = scope_entries = 0;
    on_yield = nullptr;
    attempted_roots.fill(nullptr);
    for (auto* task : {&actor, &foreign}) {
        task->type = ker::mod::sched::task::TaskType::PROCESS;
        task->state = ker::mod::sched::task::TaskState::ACTIVE;
        task->scheduler_transition_active = task->exit_in_progress = task->process_exit_requested = false;
        task->usercopy_pagemap_closing = false;
        task->has_exited = false;
        task->deferred_task_switch = task->wants_block = task->voluntary_block = false;
        task->syscall_account_start_us = 1;
        task->pagemap = &task->root;
        task->pending_ptrace_operations = nullptr;
    }
    for (size_t p = 0; p < pages.size(); ++p) {
        pages[p] = Page{};
        for (size_t i = 0; i < 4096; ++i) pages[p].data[i] = static_cast<uint8_t>((p * 71 + i * 17 + 3) % 251);
    }
}
}  // namespace fixture
namespace ker::mod::debug::ptrace {
// Explicit pending-chain layout endpoints, not namespace/effect admission.
// The production predicate below still traverses the complete selected chain.
class PendingOperation {
   public:
    PendingOperation* next_{};
    sched::task::Task* local_effect_{};
};
ACTUAL_PENDING_EFFECT_QUERY
}  // namespace ker::mod::debug::ptrace
namespace ker::mod::sched {
auto can_query_current_task() -> bool { return fixture::queryable && fixture::allow; }
auto get_current_task() -> task::Task* { return fixture::current; }
auto get_return_task() -> task::Task* { return fixture::return_task; }
auto has_run_queues() -> bool { return fixture::run_queues; }
auto interrupts_enabled() -> bool { return fixture::irq; }
auto preempt_count() -> unsigned { return fixture::preempt; }
void kern_yield() {
    fixture::check(fixture::owned() && fixture::scopes == 1 && fixture::irq && fixture::preempt == 0,
                   "busy retry yields with IF and no caller owners");
    fixture::check(fixture::yields < 24, "finite advisory retry count");
    ++fixture::yields;
    if (fixture::on_yield) fixture::on_yield();
}
}  // namespace ker::mod::sched
namespace ker::mod::sys {
ACTUAL_PREPARATION
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
    // Port the real preparer's current-actor exit refusal. Resident handles
    // must not turn this pre-existing refusal into a successful fast return.
    if (t.exit_in_progress || t.has_exited) return -EWOULDBLOCK;
    if (fixture::prepare_error) return fixture::prepare_error;
    auto p = fixture::index(address);
    if (p >= fixture::pages.size() || static_cast<int>(p) == fixture::fail_page) return -EAGAIN;
    fixture::pages[p].resident = true;
    return 0;
}
auto pin_user_page_for_task(sched::task::Task*, paging::PageTable*, uint64_t address, bool, bool fault, UserPagePin& out) -> bool {
    ++fixture::legacy_attempts;
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
ACTUAL_PIN_ATTEMPT_TYPE
auto try_pin_user_page_for_task(sched::task::Task* task, paging::PageTable* root, uint64_t address, bool writable, bool fault,
                                UserPagePin& out) -> UserPagePinAttempt {
    fixture::check(fixture::typed_attempts < fixture::attempted_roots.size(), "bounded typed pin attempts");
    fixture::attempted_roots[fixture::typed_attempts++] = root;
    if (fault)
        ++fixture::faulting_attempts;
    else
        ++fixture::mapped_attempts;
    fixture::check(task != nullptr && task->accesses == 1 && root == task->pagemap, "fresh Task lease owns attempted root");
    fixture::check(fixture::scopes == 0 && fixture::irq == fixture::incoming_irq, "typed pin observes restored incoming IF");
    if (fixture::busy_attempts != 0) {
        --fixture::busy_attempts;
        out = {};
        return UserPagePinAttempt::Busy;
    }
    // One finite admission refusal, without claiming PTE/COW implementation.
    // Actual permission, COW, File and managed-leaf proofs live in their real
    // MM fixtures; this endpoint checks the wrapper's unchanged fallback.
    if (!fault && fixture::mapped_refusals != 0) {
        --fixture::mapped_refusals;
        out = {};
        return UserPagePinAttempt::Failed;
    }
    return pin_user_page_for_task(task, root, address, writable, fault, out) ? UserPagePinAttempt::Pinned : UserPagePinAttempt::Failed;
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
    unsigned resident_vectors = 0, fallback_vectors = 0, exit_vectors = 0, reused_exit_vectors = 0;
    for (bool enabled : {false, true}) {
        for (bool writable : {false, true}) {
            reset();
            irq = incoming_irq = enabled;
            pages[0].resident = true;
            prepare_error = -ENOMEM;
            std::array<uint8_t, 16> bytes{};
            if (writable) {
                bytes.fill(0x93);
                auto const copied = uc::copy_to_current_task_partial(actor, BASE + 7, bytes.data(), bytes.size());
                check(copied.complete(bytes.size()) && pages[0].dirty &&
                          std::memcmp(pages[0].data.data() + 7, bytes.data(), bytes.size()) == 0,
                      "resident current write copies and commits without preparation");
            } else {
                auto const copied = uc::copy_from_current_task_partial(actor, BASE + 7, bytes.data(), bytes.size());
                check(copied.complete(bytes.size()) && !pages[0].dirty &&
                          std::memcmp(pages[0].data.data() + 7, bytes.data(), bytes.size()) == 0,
                      "resident current read copies without preparation");
            }
            check(preparations == 0 && scope_entries == 0 && mapped_attempts == 1 && faulting_attempts == 0 && yields == 0,
                  "resident hit skips preparation and faulting admission");
            check(owned() && scopes == 0 && irq == enabled, "resident copy returns exact owners and incoming IF");
            ++resident_vectors;
        }

        reset();
        irq = incoming_irq = enabled;
        {
            uc::StableUserPage held;
            check(uc::pin_current_task_user_page(actor, BASE, false, held), "missing resident attempt keeps ordinary fallback");
            check(preparations == 1 && scope_entries == 1 && mapped_attempts == 1 && faulting_attempts == 1 && yields == 0,
                  "missing resident attempt prepares before one faulting pin");
            check(held.valid() && actor.accesses == 1 && pins == 1 && irq == enabled,
                  "missing fallback returns one real endpoint handle and restores IF");
        }
        check(owned() && scopes == 0, "missing fallback closes exact endpoint leases");
        ++fallback_vectors;

        reset();
        irq = incoming_irq = enabled;
        pages[0].resident = true;
        mapped_refusals = 1;
        {
            uc::StableUserPage held;
            check(uc::pin_current_task_user_page(actor, BASE, true, held), "failed mapped admission keeps ordinary fallback");
            check(preparations == 1 && scope_entries == 1 && mapped_attempts == 1 && faulting_attempts == 1 && mapped_refusals == 0,
                  "failed mapped admission does not skip preparation or duplicate its owner");
            check(held.valid() && actor.accesses == 1 && pins == 1 && irq == enabled,
                  "failed mapped admission restores IF before successful faulting pin");
        }
        check(owned() && scopes == 0 && yields == 0, "failed mapped admission returns every endpoint lease");
        ++fallback_vectors;

        reset();
        irq = incoming_irq = enabled;
        pages[0].resident = true;
        busy_attempts = 2;
        on_yield = +[] { actor.pagemap = &actor.replacement; };
        {
            uc::StableUserPage held;
            check(uc::pin_current_task_user_page(actor, BASE, false, held), "resident Busy keeps owner-free ordinary fallback");
            check(preparations == 1 && mapped_attempts == 1 && faulting_attempts == 2 && typed_attempts == 3 && yields == 1,
                  "initial mapped Busy returns before preparation and existing faulting retry");
            check(attempted_roots[0] == &actor.root && attempted_roots[1] == &actor.root && attempted_roots[2] == &actor.replacement,
                  "resident Busy fallback reacquires the actual replacement root");
            check(held.valid() && actor.accesses == 1 && pins == 1 && irq == enabled && scopes == 0,
                  "Busy fallback returns one handle with incoming IF");
        }
        check(owned(), "Busy fallback closes replacement-root owners");
        ++fallback_vectors;

        for (bool completed : {false, true}) {
            reset();
            irq = incoming_irq = enabled;
            pages[0].resident = true;
            if (completed)
                actor.has_exited = true;
            else
                actor.exit_in_progress = true;
            uc::StableUserPage held;
            check(!uc::pin_current_task_user_page(actor, BASE, false, held), "exiting current resident actor refuses before fast pin");
            check(preparations == 1 && scope_entries == 1 && mapped_attempts == 0 && faulting_attempts == 0 && !held.valid(),
                  "exit flags retain original preparation refusal");
            check(owned() && scopes == 0 && irq == enabled && yields == 0, "exit refusal restores owners and incoming IF");
            ++exit_vectors;
        }
    }
    for (bool completed : {false, true}) {
        reset();
        irq = incoming_irq = completed;
        pages[0].resident = true;
        uc::StableUserPage old;
        check(uc::pin_task_user_page(actor, BASE, false, false, old), "reused exiting handle setup");
        if (completed)
            actor.has_exited = true;
        else
            actor.exit_in_progress = true;
        check(!uc::pin_current_task_user_page(actor, BASE, false, old) && !old.valid(),
              "reused exiting handle resets before preparation refusal");
        check(owned() && scopes == 0 && irq == incoming_irq && preparations == 1 && mapped_attempts == 1 && faulting_attempts == 0,
              "reused exit refusal returns initial pin exactly once");
        ++reused_exit_vectors;
    }
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
    for (unsigned busy : {1U, 3U, 7U}) {
        reset();
        pages[0].resident = true;
        busy_attempts = busy;
        on_yield = +[] { actor.pagemap = &actor.replacement; };
        {
            uc::StableUserPage held;
            check(uc::pin_task_user_page(actor, BASE, false, true, held), "busy retry eventually pins");
            check(yields == busy && typed_attempts == busy + 1, "one advisory yield per Busy attempt");
            check(attempted_roots[0] == &actor.root, "first attempt uses original root");
            for (unsigned i = 1; i <= busy; ++i) check(attempted_roots[i] == &actor.replacement, "retry reacquires replacement root");
            check(!irq && scopes == 0, "busy retry restores disabled incoming IF");
        }
        check(owned(), "busy success releases every hold");
    }
    reset();
    pages[0].resident = true;
    irq = incoming_irq = true;
    busy_attempts = 2;
    {
        uc::StableUserPage held;
        check(uc::pin_task_user_page(actor, BASE, true, true, held), "enabled-IF Busy retry succeeds");
        check(yields == 2 && irq && scopes == 0, "busy retry restores enabled incoming IF");
    }
    reset();
    pages[0].resident = true;
    busy_attempts = 1;
    on_yield = +[] { actor.usercopy_pagemap_closing = true; };
    {
        uc::StableUserPage held;
        check(!uc::pin_task_user_page(actor, BASE, false, true, held), "closing target refuses fresh lease after yield");
        check(yields == 1 && typed_attempts == 1 && !held.valid(), "closing target never reuses old root");
    }
    check(owned() && !irq && scopes == 0, "closing refusal restores all owners and IF");
    reset();
    pages[0].resident = true;
    busy_attempts = 1;
    on_yield = +[] { foreign.usercopy_pagemap_closing = true; };
    {
        uc::StableUserPage held;
        check(!uc::pin_task_user_page(foreign, BASE, false, true, held), "foreign target closing refuses fresh lease");
        check(yields == 1 && typed_attempts == 1 && owned(), "closing foreign target leaks no old lease");
    }
    reset();
    pages[0].resident = true;
    busy_attempts = 1;
    on_yield = +[] { current = &foreign; };
    {
        uc::StableUserPage held;
        check(!uc::pin_task_user_page(actor, BASE, false, true, held), "changed executor refuses after yield");
        check(yields == 1 && typed_attempts == 1 && !held.valid(), "changed executor cannot retry pin");
    }
    check(owned() && !irq && scopes == 0, "executor change restores all owners and IF");
    reset();
    pages[0].resident = true;
    busy_attempts = 1;
    on_yield = +[] { foreign.pagemap = &foreign.replacement; };
    {
        uc::StableUserPage held;
        check(uc::pin_task_user_page(foreign, BASE, false, true, held), "ordinary executor may retry foreign target");
        check(yields == 1 && typed_attempts == 2 && attempted_roots[0] == &foreign.root && attempted_roots[1] == &foreign.replacement,
              "foreign target reacquires its fresh root");
    }
    check(owned(), "foreign retry releases target and executor owners");
    reset();
    pages[0].resident = true;
    busy_attempts = 1;
    ker::mod::debug::ptrace::PendingOperation empty_pending;
    actor.pending_ptrace_operations = &empty_pending;
    {
        uc::StableUserPage held;
        check(uc::pin_task_user_page(foreign, BASE, false, true, held) && yields == 1,
              "pending ptrace operation without effect remains retryable");
    }
    reset();
    pages[0].resident = true;
    busy_attempts = 1;
    ker::mod::debug::ptrace::PendingOperation held_effect;
    held_effect.local_effect_ = &foreign;
    empty_pending.next_ = &held_effect;
    actor.pending_ptrace_operations = &empty_pending;
    {
        uc::StableUserPage held;
        check(!uc::pin_task_user_page(foreign, BASE, false, true, held) && yields == 0 && typed_attempts == 1,
              "nested retained ptrace effect forbids advisory handoff");
        check(owned() && !irq && scopes == 0 && held_effect.local_effect_ == &foreign,
              "unsafe Busy leaves existing ptrace effect and IF untouched");
    }
    for (unsigned unsafe = 0; unsafe != 16; ++unsafe) {
        reset();
        pages[0].resident = true;
        busy_attempts = 1;
        switch (unsafe) {
            case 0:
                queryable = false;
                break;
            case 1:
                current = nullptr;
                break;
            case 2:
                preempt = 1;
                break;
            case 3:
                ac = true;
                break;
            case 4:
                actor.type = ker::mod::sched::task::TaskType::DAEMON;
                break;
            case 5:
                actor.syscall_account_start_us = 0;
                break;
            case 6:
                actor.state = ker::mod::sched::task::TaskState::EXITING;
                break;
            case 7:
                actor.exit_in_progress = true;
                break;
            case 8:
                actor.process_exit_requested = true;
                break;
            case 9:
                actor.deferred_task_switch = true;
                break;
            case 10:
                actor.wants_block = true;
                break;
            case 11:
                actor.voluntary_block = true;
                break;
            case 12:
                actor.has_exited = true;
                break;
            case 13:
                actor.usercopy_pagemap_closing = true;
                break;
            case 14:
                return_task = &foreign;
                break;
            case 15:
                run_queues = false;
                break;
        }
        uc::StableUserPage held;
        check(!uc::pin_task_user_page(foreign, BASE, false, true, held), "unsafe executor refuses Busy without scheduling");
        check(yields == 0 && typed_attempts == 1 && !irq && scopes == 0 && owned(),
              "unsafe Busy refusal retains incoming IF and leaks no owners");
    }
    reset();
    pages[0].resident = true;
    {
        uc::StableUserPage retained;
        check(uc::pin_task_user_page(actor, BASE, false, true, retained), "retained current pin setup");
        busy_attempts = 1;
        uc::StableUserPage denied;
        check(!uc::pin_task_user_page(foreign, BASE, false, true, denied) && yields == 0,
              "another current Task pin forbids advisory handoff");
        check(actor.accesses == 1 && pins == 1 && foreign.accesses == 0 && !irq, "Busy refusal preserves unrelated retained current pin");
    }
    check(owned(), "retained current pin later closes normally");
    reset();
    pages[0].resident = true;
    fail_page = 0;
    {
        uc::StableUserPage held;
        check(!uc::pin_task_user_page(actor, BASE, false, true, held) && typed_attempts == 1 && yields == 0,
              "terminal mapping failure is never retried as Busy");
        check(owned() && !irq && scopes == 0, "terminal mapping failure releases all owners");
    }
    reset();
    pages[0].resident = true;
    busy_attempts = 1;
    {
        uc::StableUserPage held;
        check(!uc::pin_task_user_page(actor, BASE, false, false, held) && yields == 0, "mapped-only Busy never schedules or enables IF");
        check(owned() && !irq && scopes == 0, "mapped-only Busy returns empty");
    }
    reset();
    actor.type = ker::mod::sched::task::TaskType::DAEMON;
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
    std::printf(
        "PASS actual current-copy/VFS/timeout checks=%u resident_vectors=%u fallback_vectors=%u exit_vectors=%u reused_exit_vectors=%u\n",
        checks, resident_vectors, fallback_vectors, exit_vectors, reused_exit_vectors);
}
