#!/usr/bin/env python3
"""Compile exact WOS caller bodies against small Host primitive backends.

This proves caller ordering/RAII/error and copy-prefix behavior in a Host model,
not hardware PTE behavior, concurrency, native KTEST, VM or Goal39 acceptance.
Default extracts current repository production with no evidence dependency.
Optional --futex-source/--vmem-source select explicit source files for archived
original-versus-current sensitivity. --expect-order-failure is a deliberate
negative-control mode; normal CTest must never use that option.
"""
import argparse
import hashlib
import json
import re
from pathlib import Path
import subprocess
import tempfile

ROOT = next(p for p in Path(__file__).resolve().parents
            if (p / "modules/kern/src").is_dir() and (p / "tests/host").is_dir())
FUTEX = ROOT / "modules/kern/src/syscalls_impl/futex/futex.cpp"
VMEM = ROOT / "modules/kern/src/syscalls_impl/vmem/sys_vmem.cpp"


def function(source, signature):
    """Balanced lexical extraction, including braces in comments/strings."""
    start = source.index(signature)
    left = source.index("{", start)
    depth, i, mode = 0, left, "code"
    while i < len(source):
        c = source[i]
        n = source[i:i + 2]
        if mode == "line":
            if c == "\n": mode = "code"
        elif mode == "block":
            if n == "*/": mode = "code"; i += 1
        elif mode in ("string", "char"):
            if c == "\\": i += 1
            elif c == ('"' if mode == "string" else "'"): mode = "code"
        elif n == "//": mode = "line"; i += 1
        elif n == "/*": mode = "block"; i += 1
        elif c == '"': mode = "string"
        elif c == "'": mode = "char"
        elif c == "{": depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0: return source[start:i + 1]
        i += 1
    raise AssertionError("unterminated function: " + signature)


PRELUDE = r'''
#include <algorithm>
#include <array>
#include <atomic>
#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <ctime>
#include <initializer_list>
#include <limits>
#include <new>
#include <string>
#include <vector>
#include <abi/callnums/vmem.h>

constexpr uint64_t PAGE = 4096, OLD = 0x400000, NEW = 0x800000;
constexpr uint64_t WORD = 0x500000, TIMEOUT = WORD + PAGE;
constexpr int64_t USEC_PER_SEC = 1000000, NSEC_PER_SEC = 1000000000;
unsigned failures = 0, cases = 0, futex_vectors = 0, mremap_vectors = 0;
unsigned timeout_order_observations = 0, cleanup_order_observations = 0;
const char* case_name = "unstarted";
void require(bool ok, const char* text) {
    if (!ok) { ++failures; std::fprintf(stderr, "%s: %s\n", case_name, text); }
}
struct FreeCall { uint64_t address, size; unsigned live_pins; };
struct State {
    bool futex_mode = true, current = true, root = true, range = true;
    bool table_init = true, table_valid = true, timeout_copy_ok = true, futex_pin_ok = true;
    int word = 7;
    timespec timeout{0, 5000};
    unsigned live_pins = 0, max_pins = 0, access = 0, order_violations = 0;
    unsigned timeout_order_violations = 0, cleanup_order_violations = 0;
    unsigned timeout_copies = 0, waiter_live = 0, insertions = 0, clock_calls = 0;
    unsigned futex_still_calls = 0, futex_still_fail_call = 0;
    uint64_t clock = 1000, clock_sample = 0, copy_delay = 11, pin_delay = 37, insert_delay = 23;
    bool in_insert = false, allocation_ok = true, allocated = false, old_freed = false, new_freed = false;
    int src_pin_fail = -1, ensure_fail = -1, dst_pin_fail = -1, still_fail = -1, commit_fail = -1;
    int64_t old_free_error = 0, destination_free_error = 0;
    unsigned allocations = 0, ensures = 0, src_pins = 0, dst_pins = 0;
    std::array<std::array<unsigned char, PAGE>, 2> source{};
    std::array<std::array<unsigned char, PAGE>, 3> destination{};
    std::vector<std::string> events;
    std::vector<FreeCall> frees;
} state;
void advance(uint64_t delta) {
    state.clock = delta > UINT64_MAX - state.clock ? UINT64_MAX : state.clock + delta;
}
namespace ker::mod::sched::task {
enum class WaitChannelKind { NONE, FUTEX };
struct Task {
    void* pagemap = reinterpret_cast<void*>(uintptr_t{0x123000});
    uint64_t pid = 41, wake_at_us = 0;
    bool deferred_task_switch = false;
    std::atomic<void*> futex_waiter{nullptr};
    WaitChannelKind channel = WaitChannelKind::NONE;
    uint64_t load_cpu() const { return 2; }
    void set_wait_channel(const char*, WaitChannelKind kind) { channel = kind; }
    void clear_wait_channel() { channel = WaitChannelKind::NONE; }
};
}
ker::mod::sched::task::Task fake_task;
namespace ker::mod::sched {
auto get_current_task() -> task::Task* { return state.current ? &fake_task : nullptr; }
}
namespace ker::mod::time {
auto get_us() -> uint64_t {
    ++state.clock_calls; state.clock_sample = state.clock; state.events.emplace_back("clock");
    require(state.in_insert, "absolute deadline sampled outside successful waiter insertion");
    return state.clock;
}
}
namespace ker::mod::mm::paging { constexpr uint64_t PAGE_SIZE = PAGE; }
namespace ker::mod::sys::usercopy {
class UserAccessGuard {
 public:
    UserAccessGuard() { ++state.access; }
    ~UserAccessGuard() { --state.access; }
};
// Frame ownership is qualified by futex_frame_lifetime_runtime_test. This
// caller-order seam keeps frame retention separate from mapping/access pins.
struct UserPageFrameRef { bool held = false; };
class StableUserPage {
 public:
    StableUserPage() = default;
    StableUserPage(const StableUserPage&) = delete;
    StableUserPage(StableUserPage&&) = delete;
    ~StableUserPage() {
        if (held) { --state.live_pins; state.events.emplace_back(writable ? "unpin-dst" : "unpin-read"); }
    }
    void* kernel_address() const { return bytes; }
    uint64_t physical_address() const { return 0xa51000; }
    bool retain_frame(UserPageFrameRef& out) const {
        if (!held || out.held) return false;
        out.held = true;
        return true;
    }
    bool still_mapped() const {
        if (state.futex_mode) return ++state.futex_still_calls != state.futex_still_fail_call;
        require(state.access == 0, "mapping check performed inside the memcpy access window");
        return writable || index != state.still_fail;
    }
    bool commit_write() const {
        require(held && writable, "write commit without writable pin");
        require(state.access == 0, "write commit performed inside the memcpy access window");
        return index != state.commit_fail;
    }
    bool held = false, writable = false;
    int index = -1;
    void* bytes = nullptr;
};
bool range_valid(uint64_t, size_t) { return state.range; }
bool pin_task_user_page(sched::task::Task&, uint64_t va, bool writable, bool fault_in, StableUserPage& pin) {
    require(!pin.held, "primitive received an already-live pin");
    state.events.emplace_back(writable ? "pin-dst" : "pin-read");
    if (state.futex_mode) {
        require(va == WORD && !writable && fault_in, "futex pin arguments changed");
        advance(state.pin_delay);
        if (!state.futex_pin_ok) return false;
        pin.bytes = &state.word;
    } else if (!writable) {
        ++state.src_pins;
        require(va >= OLD && va < OLD + 2 * PAGE && fault_in, "source pin arguments changed");
        pin.index = static_cast<int>((va - OLD) / PAGE);
        if (pin.index == state.src_pin_fail) return false;
        pin.bytes = state.source.at(static_cast<size_t>(pin.index)).data();
    } else {
        ++state.dst_pins;
        require(va >= NEW && va < NEW + 3 * PAGE && !fault_in, "destination pin arguments changed");
        pin.index = static_cast<int>((va - NEW) / PAGE);
        if (pin.index == state.dst_pin_fail) return false;
        pin.bytes = state.destination.at(static_cast<size_t>(pin.index)).data();
    }
    pin.writable = writable; pin.held = true;
    state.max_pins = std::max(state.max_pins, ++state.live_pins);
    return true;
}
template<class T> bool copy_value_from_task(sched::task::Task&, uint64_t va, T& value) {
    ++state.timeout_copies; state.events.emplace_back("timeout-copy");
    if (state.live_pins != 0) { ++state.order_violations; ++state.timeout_order_violations; }
    require(va == TIMEOUT && sizeof(T) == sizeof(timespec), "timeout copy geometry changed");
    advance(state.copy_delay);
    if (!state.timeout_copy_ok) return false;
    std::memcpy(&value, &state.timeout, sizeof(T)); return true;
}
}
namespace ker::mod::mm::virt {
bool ensure_user_page_writable(sched::task::Task*, uint64_t va) {
    ++state.ensures; state.events.emplace_back("ensure-dst");
    require(state.live_pins == 1, "destination resolution lost the source pin");
    return static_cast<int>((va - NEW) / PAGE) != state.ensure_fail;
}
}
struct FutexWaiter {
    uint64_t phys_addr = 0, task_pid = 0, task_cpu = 0;
    FutexWaiter* hash_next = nullptr;
    ker::mod::sys::usercopy::UserPageFrameRef frame;
    FutexWaiter() { ++state.waiter_live; }
    ~FutexWaiter() { --state.waiter_live; }
};
struct FakeTable {
    FutexWaiter* node = nullptr;
    bool valid() const { return state.table_valid; }
    template<class Compare, class Commit> bool insert_if(FutexWaiter* waiter, Compare compare, Commit commit) {
        require(state.live_pins == 1, "table insertion lacks exactly one futex pin");
        state.events.emplace_back("insert"); advance(state.insert_delay);
        if (!compare()) return false;
        ++state.insertions; node = waiter; state.in_insert = true; commit(); state.in_insert = false; return true;
    }
} futex_table;
bool ensure_futex_table() { return state.table_init; }
bool claim_task_waiter(ker::mod::sched::task::Task* task, FutexWaiter* waiter) {
    void* expected = waiter;
    return task->futex_waiter.compare_exchange_strong(expected, nullptr);
}
void retire_task_waiter_ref(FutexWaiter* waiter) {
    if (futex_table.node == waiter) futex_table.node = nullptr;
    delete waiter;
}
namespace ker::syscall::futex { namespace mod = ker::mod; }
namespace ker::syscall::vmem {
constexpr uint64_t USER_SPACE_END = 0x00007FFFFFFFFFFFULL, MREMAP_MAYMOVE = 1;
auto get_current_task() -> ker::mod::sched::task::Task* {
    fake_task.pagemap = state.root ? reinterpret_cast<void*>(uintptr_t{0x123000}) : nullptr;
    return ker::mod::sched::get_current_task();
}
int align_user_vmem_size(uint64_t size, uint64_t* out) {
    if (size == 0 || size > UINT64_MAX - (PAGE - 1)) return -EINVAL;
    *out = (size + PAGE - 1) & ~(PAGE - 1); return 0;
}
uint64_t anon_allocate(uint64_t hint, uint64_t size, uint64_t prot, uint64_t flags) {
    ++state.allocations; state.events.emplace_back("allocate");
    require(hint == 0 && size == 3 * PAGE && prot == (ker::abi::vmem::PROT_READ | ker::abi::vmem::PROT_WRITE) &&
            flags == (ker::abi::vmem::MAP_PRIVATE | ker::abi::vmem::MAP_ANONYMOUS), "destination allocation arguments changed");
    state.allocated = state.allocation_ok;
    return state.allocated ? NEW : static_cast<uint64_t>(-ENOMEM);
}
uint64_t anon_free(uint64_t addr, uint64_t size) {
    state.events.emplace_back("free"); state.frees.push_back({addr, size, state.live_pins});
    if (state.live_pins != 0) { ++state.order_violations; ++state.cleanup_order_violations; }
    if (addr == NEW) {
        require(size == 3 * PAGE, "destination cleanup size changed");
        if (state.destination_free_error == 0) state.new_freed = true;
        return static_cast<uint64_t>(state.destination_free_error);
    }
    require(addr == OLD || addr == OLD + PAGE, "unexpected source/tail cleanup address");
    if (state.old_free_error == 0) state.old_freed = true;
    return static_cast<uint64_t>(state.old_free_error);
}
}
'''

CASES = r'''
void reset(const char* name, bool futex_mode) {
    case_name = name; ++cases;
    if (futex_mode) ++futex_vectors; else ++mremap_vectors;
    require(state.live_pins == 0 && state.access == 0, "prior case leaked a pin/access window");
    if (futex_table.node != nullptr) { delete futex_table.node; futex_table.node = nullptr; }
    require(state.waiter_live == 0, "prior case leaked an unpublished waiter");
    state = State{}; state.futex_mode = futex_mode;
    fake_task.pagemap = reinterpret_cast<void*>(uintptr_t{0x123000}); fake_task.wake_at_us = 0;
    fake_task.deferred_task_switch = false; fake_task.channel = ker::mod::sched::task::WaitChannelKind::NONE;
    fake_task.futex_waiter.store(nullptr);
    for (size_t p = 0; p < state.source.size(); ++p)
        for (size_t b = 0; b < PAGE; ++b) state.source[p][b] = static_cast<unsigned char>((p * 71 + b * 13) % 251);
    for (auto& page : state.destination) page.fill(0xa5);
}
void finish() {
    timeout_order_observations += state.timeout_order_violations;
    cleanup_order_observations += state.cleanup_order_violations;
    require(state.live_pins == 0 && state.access == 0, "function return leaked pins/access window");
    if (state.order_violations != 0) {
        failures += state.order_violations;
        std::fprintf(stderr, "%s: %u forbidden live-pin ordering observations\n", case_name, state.order_violations);
    }
}
void fresult(int64_t wanted, uint64_t address = WORD, int expected = 7, uint64_t timeout = TIMEOUT) {
    int initial = state.word;
    require(ker::syscall::futex::futex_wait(address, expected, timeout) == wanted, "wrong signed futex errno/result");
    require(state.word == initial, "futex changed the user word"); finish();
}
void futex_cases() {
    reset("alignment-before-timeout", true); state.timeout_copy_ok = false; fresult(-EINVAL, WORD + 1);
    require(state.timeout_copies == 0, "unaligned call touched timeout");
    reset("table-before-timeout", true); state.table_init = false; fresult(-ENOMEM);
    require(state.timeout_copies == 0, "table refusal touched timeout");
    reset("null-current-before-timeout", true); state.current = false; fresult(-EINVAL);
    require(state.timeout_copies == 0, "missing Task touched timeout");
    reset("range-before-timeout", true); state.range = false; fresult(-EFAULT);
    require(state.timeout_copies == 0, "invalid user range touched timeout");
    reset("both-pointers-fault-priority", true); state.futex_pin_ok = false; state.timeout_copy_ok = false; fresult(-EFAULT);
    require(state.timeout_copies == 1, "valid-range request did not stage timeout");
    for (unsigned bad = 0; bad < 4; ++bad) {
        for (bool valid_pin : {false, true}) {
            reset(valid_pin ? "bad-timeout-valid-word" : "bad-timeout-invalid-word", true);
            state.futex_pin_ok = valid_pin;
            if (bad == 0) state.timeout.tv_sec = -1;
            if (bad == 1) state.timeout.tv_nsec = -1;
            if (bad == 2) state.timeout.tv_nsec = NSEC_PER_SEC;
            if (bad == 3) state.timeout.tv_sec = std::numeric_limits<decltype(state.timeout.tv_sec)>::max();
            fresult(valid_pin ? -EINVAL : -EFAULT, WORD, 8);
            require(state.insertions == 0 && state.waiter_live == 0, "invalid timeout published/allocated waiter");
        }
    }
    reset("timeout-EFAULT-after-word-admission", true); state.timeout_copy_ok = false; fresult(-EFAULT);
    require(state.events.size() >= 2 && state.events[0] == "timeout-copy" && state.events[1] == "pin-read", "timeout/pin staging order");
    reset("zero-timeout-matching", true); state.timeout = {0, 0}; fresult(-ETIMEDOUT);
    reset("zero-timeout-mismatching", true); state.timeout = {0, 0}; fresult(-EAGAIN, WORD, 8);
    reset("zero-timeout-remap-refusal", true); state.timeout = {0, 0}; state.futex_still_fail_call = 1; fresult(-EFAULT);
    reset("nonnull-timeout-mismatching", true); fresult(-EAGAIN, WORD, 8);
    require(state.clock_calls == 0 && state.waiter_live == 0, "mismatch started deadline or leaked waiter");
    reset("null-timeout-mismatching", true); fresult(-EAGAIN, WORD, 8, 0);
    require(state.timeout_copies == 0 && state.clock_calls == 0, "null timeout accessed memory/clock");
    reset("valid-duration-insertion-clock", true); state.timeout = {2, 1501}; fresult(0);
    require(state.clock_calls == 1 && fake_task.wake_at_us == state.clock_sample + 2000002, "duration was not rounded/anchored at insertion");
    require(state.clock_sample == 1000 + 11 + 37 + 23, "deadline omitted primitive admission delay");
    require(fake_task.deferred_task_switch && state.insertions == 1, "valid wait was not deferred/published");
    reset("deadline-saturates-at-insertion", true); state.clock = UINT64_MAX - 2; state.copy_delay = state.pin_delay = state.insert_delay = 0;
    fresult(0); require(fake_task.wake_at_us == UINT64_MAX && state.clock_calls == 1, "overflow deadline failed to saturate");
    reset("untimed-wait-no-clock", true); fresult(0, WORD, 7, 0);
    require(fake_task.wake_at_us == 0 && state.clock_calls == 0, "untimed wait started a deadline");
    reset("table-refusal-unwinds-first-pin", true); state.table_valid = false; fresult(-ENOMEM);
    require(state.waiter_live == 0, "table refusal leaked waiter");
}
uint64_t remap(uint64_t old_size = 2 * PAGE, uint64_t new_size = 3 * PAGE, uint64_t flags = 1) {
    return ker::syscall::vmem::anon_mremap(OLD, old_size, new_size, flags);
}
void merror(unsigned axis, int page) {
    const char* names[] = {"source-pin-failure", "destination-resolution-failure", "destination-pin-failure", "source-revalidation-failure", "destination-commit-failure"};
    reset(names[axis], false);
    if (axis == 0) state.src_pin_fail = page;
    if (axis == 1) state.ensure_fail = page;
    if (axis == 2) state.dst_pin_fail = page;
    if (axis == 3) state.still_fail = page;
    if (axis == 4) state.commit_fail = page;
    auto before = state.source;
    require(static_cast<int64_t>(remap()) == (axis == 1 ? -ENOMEM : -EFAULT), "mremap branch errno changed");
    require(state.source == before && !state.old_freed, "failed move changed/freed old bytes");
    require(state.allocations == 1 && state.frees.size() == 1 && state.frees[0].address == NEW && state.new_freed,
            "failed iteration did not clean destination exactly once");
    require(state.frees[0].live_pins == 0, "failed iteration cleanup ran before pin destructors");
    if (page == 1) require(state.destination[0] == before[0], "second-page failure skipped prior copy prefix");
    if (axis >= 3) require(state.destination[static_cast<size_t>(page)] == before[static_cast<size_t>(page)], "post-copy refusal lost its actual copy");
    finish();
}
void mremap_cases() {
    reset("no-current-mremap", false); state.current = false; require(static_cast<int64_t>(remap()) == -EFAULT, "missing task errno"); finish();
    reset("no-root-mremap", false); state.root = false; require(static_cast<int64_t>(remap()) == -EFAULT, "missing root errno"); finish();
    reset("equal-size-no-change", false); require(remap(2 * PAGE, 2 * PAGE) == OLD && state.allocations == 0 && state.frees.empty(), "equal-size mutation"); finish();
    reset("shrink-tail-only", false); require(remap(2 * PAGE, PAGE) == OLD && state.allocations == 0 && state.frees.size() == 1 &&
        state.frees[0].address == OLD + PAGE && state.frees[0].size == PAGE, "shrink tail changed"); finish();
    reset("MAYMOVE-denial-no-mutation", false); auto source = state.source; require(static_cast<int64_t>(remap(2 * PAGE, 3 * PAGE, 0)) == -ENOMEM &&
        state.source == source && state.allocations == 0 && state.frees.empty(), "MAYMOVE refusal changed state"); finish();
    reset("destination-allocation-failure", false); state.allocation_ok = false; require(static_cast<int64_t>(remap()) == -ENOMEM && state.frees.empty(), "allocation refusal cleanup"); finish();
    for (unsigned axis = 0; axis < 5; ++axis) for (int page : {0, 1}) merror(axis, page);
    reset("successful-two-page-move", false); source = state.source; require(remap() == NEW, "successful move address");
    require(state.destination[0] == source[0] && state.destination[1] == source[1] && state.source == source, "successful copy payload");
    require(state.max_pins == 2 && state.frees.size() == 1 && state.frees[0].address == OLD && state.frees[0].size == 2 * PAGE &&
        state.old_freed && !state.new_freed, "successful move ownership/free order"); finish();
    reset("final-source-free-failure", false); source = state.source; state.old_free_error = -EACCES;
    require(static_cast<int64_t>(remap()) == -EACCES && state.source == source && !state.old_freed, "final-free error/source preservation");
    require(state.frees.size() == 2 && state.frees[0].address == OLD && state.frees[1].address == NEW && state.new_freed,
            "final source-free failure did not attempt destination cleanup exactly once");
    require(state.destination[0] == source[0] && state.destination[1] == source[1], "final-free failure lost copied prefix"); finish();
    reset("cleanup-error-does-not-replace-copy-error", false); state.commit_fail = 0; state.destination_free_error = -EIO;
    require(static_cast<int64_t>(remap()) == -EFAULT && state.frees.size() == 1 && state.frees[0].address == NEW && !state.new_freed,
            "existing ignored destination-cleanup error policy changed"); finish();
}
int main() {
    futex_cases(); mremap_cases();
    if (futex_table.node != nullptr) { delete futex_table.node; futex_table.node = nullptr; }
    require(state.waiter_live == 0 && state.live_pins == 0, "final primitive owner leak");
    std::printf("Host extracted caller order/lifetime: %u vectors (%u futex, %u mremap), %u failures; hardware/concurrency/native/runtime unqualified\n",
                cases, futex_vectors, mremap_vectors, failures);
    std::printf("Order sensitivity: timeout-copy-live-pin=%u cleanup-live-pin=%u\n", timeout_order_observations, cleanup_order_observations);
    return failures == 0 ? 0 : 1;
}
'''


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--futex-source", type=Path, default=FUTEX)
    parser.add_argument("--vmem-source", type=Path, default=VMEM)
    parser.add_argument("--expect-order-failure", action="store_true")
    args = parser.parse_args()
    fs, vs = args.futex_source.read_text(), args.vmem_source.read_text()
    extracted = [("relative_timeout_us", function(fs, "auto relative_timeout_us(")),
                 ("deadline_from_now_us", function(fs, "auto deadline_from_now_us(")),
                 ("futex_addr_is_aligned", function(fs, "auto futex_addr_is_aligned(")),
                 ("futex_wait", function(fs, "int64_t futex_wait(")),
                 ("anon_mremap", function(vs, "auto anon_mremap("))]
    mode = "actual explicitly selected source" if args.futex_source != FUTEX or args.vmem_source != VMEM else "actual current repository source"
    print(json.dumps({"mode": mode, "source_paths": [str(args.futex_source.resolve()), str(args.vmem_source.resolve())],
                      "source_sha256": [hashlib.sha256(fs.encode()).hexdigest(), hashlib.sha256(vs.encode()).hexdigest()],
                      "extracted_function_sha256": {
        name: hashlib.sha256(text.encode()).hexdigest() for name, text in extracted
    }}, sort_keys=True))
    futex_bodies = "\n\n".join(text for name, text in extracted if name != "anon_mremap")
    code = (PRELUDE + "\nnamespace ker::syscall::futex {\n" + futex_bodies + "\n}\n"
            + "\nnamespace ker::syscall::vmem {\n" + extracted[-1][1] + "\n}\n" + CASES)
    with tempfile.TemporaryDirectory(prefix="wos-caller-pin-order-") as folder:
        work = Path(folder)
        cpp, executable = work / "check.cpp", work / "check"
        cpp.write_text(code)
        command = ["/usr/bin/c++", "-std=c++23", "-O1", "-g", "-Wall", "-Wextra", "-Werror",
                   "-I", str(ROOT / "modules/kern/src"), str(cpp), "-o", str(executable)]
        compile_result = subprocess.run(command, capture_output=True, timeout=30)
        if len(compile_result.stdout) + len(compile_result.stderr) > 8 * 1024 * 1024:
            raise AssertionError("Host compiler output budget exceeded")
        print(compile_result.stdout.decode(errors="replace"), end="")
        print(compile_result.stderr.decode(errors="replace"), end="")
        compile_result.check_returncode()
        run = subprocess.run([str(executable)], capture_output=True, timeout=15)
        if len(run.stdout) + len(run.stderr) > 8 * 1024 * 1024:
            raise AssertionError("Host test output budget exceeded")
        print(run.stdout.decode(errors="replace"), end="")
        print(run.stderr.decode(errors="replace"), end="")
        if args.expect_order_failure:
            sensitivity = re.search(rb"^Order sensitivity: timeout-copy-live-pin=(\d+) cleanup-live-pin=(\d+)$", run.stdout, re.MULTILINE)
            assert run.returncode == 1 and sensitivity and int(sensitivity[1]) > 0 and int(sensitivity[2]) > 0, "negative control did not demonstrate both exact order failures"
        else:
            run.check_returncode()


if __name__ == "__main__":
    main()
