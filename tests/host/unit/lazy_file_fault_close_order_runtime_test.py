#!/usr/bin/env python3
"""Compile complete lazy-file completion/worker/admission bodies with Host seams.

The complete actual finish, worker loop, admission, pool and root-release bodies
are compiled unchanged; actual Task usercopy acquire/release bodies are too.
Scheduler, materialization, File/backend close, locks and root-exclusive wait are
explicit controlled Host boundaries. This qualifies close ordering and owned
callback return, not native mapping bytes, hardware TLBs or complete lifetime.
The real range/ownership declarations are imported, but all vectors select RAW
FILE_BACKED. Native owner methods are fatal unreachable Host boundaries; this
driver does not model or qualify the permanent native retirement executor.
--virt-source accepts a complete unmodified predecessor and its actual old finish
body must compile then fail the lease oracle. No source-string order assertion.
"""

from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import subprocess
import sys
import tempfile
import time

ROOT = next(path for path in Path(__file__).resolve().parents
            if (path / "modules/kern/src").is_dir() and (path / ".github/instructions").is_dir())
SOURCE = ROOT / "modules/kern/src/platform/mm/virt.opt.cpp"
TASK = ROOT / "modules/kern/src/platform/sched/task.cpp"
TASK_HEADER = ROOT / "modules/kern/src/platform/sched/task.hpp"
FAMILY_HEADER = ROOT / "modules/kern/src/platform/mm/file_mapping_lifetime.hpp"

PRELUDE = r'''
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <thread>
#include <vector>
#include <platform/mm/file_mapping_lifetime.hpp>
namespace ker::mod::mm::paging { struct PageTable { size_t gate_index{}; }; }
namespace fixture {
std::atomic<unsigned> failures{}, assertions{}, order_failures{}, cases{}, close_calls{}, nonfinal_puts{};
std::mutex output_lock;
thread_local unsigned lock_depth{};
void check(bool value, const char* message) {
    ++assertions;
    if (!value) {
        unsigned const before = failures.fetch_add(1);
        if (before < 16) {
            std::lock_guard guard(output_lock);
            std::fprintf(stderr, "FAIL: %s\n", message);
        }
    }
}
struct Lock {
    std::mutex mutex;
    uint64_t lock_irqsave() { mutex.lock(); ++lock_depth; return 0; }
    void unlock_irqrestore(uint64_t) { --lock_depth; mutex.unlock(); }
};
struct Gate {
    std::atomic<uint32_t> readers{};
    std::atomic<bool> teardown{};
};
using PageTable = ker::mod::mm::paging::PageTable;
struct State;
State* state{};  // Cases are sequential; worker threads are joined before reset.
}
namespace ker::mod::mm::file_mapping {
// Link-only boundaries for branches excluded by every RAW fixture. A native
// path is an error here, never a substitute for the real scheduled executor.
auto FileMappingLifetime::try_acquire(FamilyOwner) noexcept -> bool {
    fixture::check(false, "RAW-only Host entered native family acquire"); std::abort();
}
void FileMappingLifetime::release(FamilyOwner) noexcept {
    fixture::check(false, "RAW-only Host entered native family release"); std::abort();
}
auto FileMappingBinding::reclassify(BindingOwner, BindingOwner) noexcept -> bool { std::abort(); }
void FileMappingBinding::release(BindingOwner) noexcept { std::abort(); }
auto FileMappingIndex::retain_for_pin_locked(paging::PageTable*, uint64_t, uint64_t, FileMappingBinding*&) noexcept
    -> FileMappingPinResult { std::abort(); }
}
namespace ker::mod::sys { using Spinlock = fixture::Lock; }
namespace ker::vfs { struct File; void vfs_retain_file(File*); void vfs_put_file(File*); }
namespace ker::mod::mm::paging { struct PageFault { unsigned writable{}, fetch{}; }; }
namespace ker::mod::sched::task {
enum class TaskType { PROCESS, DAEMON };
enum class TaskState { ACTIVE, EXITING, DEAD };
// ACTUAL_RAW_RANGE_TYPES
struct Task {
    TaskType type{TaskType::PROCESS};
    std::atomic<TaskState> state{TaskState::ACTIVE};
    std::atomic<uint32_t> ref_count{1}, usercopy_pagemap_accesses{}, holds{};
    std::atomic<bool> usercopy_pagemap_closing{};
    fixture::PageTable* pagemap{};
    fixture::Lock lazy_vmem_lock;
    std::vector<LazyVmemRange> lazy_vmem_ranges;
    uint32_t preempt_disable_depth{};
    uint64_t lazy_file_fault_generation{}, lazy_file_fault_vaddr{};
    int lazy_file_fault_status{777};
    std::atomic<uint64_t> lazy_file_fault_completion_generation{};
    bool try_acquire() {
        if (state.load() != TaskState::ACTIVE || ref_count.load() == 0) return false;
        ++ref_count; return true;
    }
    void release() { fixture::check(ref_count.fetch_sub(1) != 0, "Task reference underflow"); }
    auto try_acquire_usercopy_pagemap(fixture::PageTable*& out) -> bool;
    void release_usercopy_pagemap();
};
}
namespace ker::mod::sched {
thread_local task::Task* current{};
task::Task* get_current_task() { return current; }
bool kernel_threads_shutdown_requested();
struct Hold {
    task::Task* owner{};
    explicit Hold(task::Task* task) : owner(task) { if (owner) ++owner->holds; }
    ~Hold() { if (owner) fixture::check(owner->holds.fetch_sub(1) != 0, "shutdown hold underflow"); }
};
Hold hold_kernel_shutdown() { return Hold(current); }
void wake_task_from_event(task::Task*);
void kern_block();
}
namespace ker::mod::mm::virt {
namespace sched = ker::mod::sched;
namespace sys = ker::mod::sys;
using PageTable = fixture::PageTable;
constexpr uint64_t PADDR_INVALID = UINT64_MAX;
constexpr size_t USER_PAGEMAP_ACCESS_GATE_NONE = SIZE_MAX;
std::array<fixture::Gate, 2> user_pagemap_access_gates;
fixture::Lock cow_pte_lock;
fixture::Lock user_mapping_pin_lock;
file_mapping::FileMappingIndex file_mapping_index;
struct RawExcludedRootControl { uint64_t serial{}; };
auto find_user_root_control_locked(PageTable*) -> RawExcludedRootControl* { std::abort(); }
auto prepare_captured_tmpfs_read_shared_page(sched::task::Task&,const sched::task::LazyVmemRange&,uint64_t,uint64_t,
                                            file_mapping::FileMappingBinding&) -> bool { std::abort(); }
uint64_t page_align_down(uint64_t address) { return address & ~uint64_t{4095}; }
uint64_t translate(PageTable*, uint64_t) { return PADDR_INVALID; }
bool acquire_user_pagemap_access(PageTable* root, bool, size_t& index) {
    auto& gate = user_pagemap_access_gates.at(root->gate_index);
    if (gate.teardown.load()) return false;
    ++gate.readers;
    if (gate.teardown.load()) { --gate.readers; return false; }
    index = root->gate_index; return true;
}
void release_user_pagemap_access(size_t);
}
namespace ker::syscall::vmem {
bool materialize_lazy_file_page(ker::mod::sched::task::Task*, const ker::mod::sched::task::LazyVmemRange&,
                                uint64_t, const ker::mod::mm::paging::PageFault&);
}
'''

CASES = r'''
namespace fixture {
using Task = ker::mod::sched::task::Task;
namespace sched = ker::mod::sched;
namespace virt = ker::mod::mm::virt;
struct State {
    Task* subject{};
    Task* worker{};
    std::mutex mutex;
    std::condition_variable condition;
    bool close_entered{}, permit_close{}, close_returned{}, file_destroyed{}, exclusive_entered{};
    bool materialize_ok{true}, final_put{true}, self_target{};
    int close_result{}, recorded_close_result{777};
    unsigned puts{}, materializations{}, target_wakes{}, worker_wakes{};
    uint64_t generation{};
};
void owned_pending(Task& subject, ker::vfs::File* file) {
    check(lock_depth == 0, "backend close entered under Host MM lock");
    check(subject.ref_count.load() == (state->self_target ? 3U : 2U), "accepted Task reference missing during close");
    check(state->worker->holds.load() == 1, "shutdown hold missing during callback");
    uint64_t const flags = virt::lazy_file_fault_lock.lock_irqsave();
    auto const& slot = virt::lazy_file_fault_work.at(0);
    check(slot.task == &subject && slot.range.file == file && slot.issued && slot.generation == state->generation,
          "issued work ownership missing during callback");
    check(subject.lazy_file_fault_completion_generation.load() == 0 && subject.lazy_file_fault_status == 0,
          "fault completion published before callback return");
    virt::lazy_file_fault_lock.unlock_irqrestore(flags);
}
}
namespace ker::vfs {
struct File {
    std::atomic<unsigned> refs{1};
    fixture::Task* target{};
    ~File() {
        if (target != nullptr) target->release();  // Explicit procfs-style close seam.
        fixture::state->file_destroyed = true;
    }
};
void vfs_retain_file(File* file) {
    fixture::check(fixture::lock_depth != 0 && file != nullptr && file->refs.load() != 0,
                   "File retain missing accepted publication ownership");
    ++file->refs;
}
void vfs_put_file(File* file) {
    using namespace fixture;
    ++state->puts;
    bool const no_mm_leases = virt::user_pagemap_access_gates.at(0).readers.load() == 0 &&
                              state->subject->usercopy_pagemap_accesses.load() == 0;
    if (!no_mm_leases) ++order_failures;
    check(no_mm_leases, "actual finish put retained root/usercopy leases");
    check(lock_depth == 0 && file != nullptr, "actual finish put invalid or under lock");
    owned_pending(*state->subject, file);
    unsigned const before = file->refs.fetch_sub(1);
    check(before != 0, "File reference underflow");
    if (before != 1) { ++nonfinal_puts; return; }
    ++close_calls;
    {
        std::lock_guard guard(state->mutex);
        state->close_entered = true;
    }
    state->condition.notify_all();
    // Backend-close seam enters the same abstract exclusive condition that
    // real root retirement needs. This finite wait exposes the original order
    // as an oracle failure, without making its negative control hang.
    auto& gate = virt::user_pagemap_access_gates.at(0);
    check(!gate.teardown.exchange(true), "exclusive root gate already owned");
    {
        std::unique_lock guard(state->mutex);
        bool const drained = state->condition.wait_for(guard, std::chrono::milliseconds(75), [&] {
            return gate.readers.load() == 0;
        });
        state->exclusive_entered = drained;
        check(drained, "backend close could not acquire simulated root exclusive gate");
        state->condition.notify_all();
        bool const permitted = state->condition.wait_for(guard, std::chrono::milliseconds(750), [&] { return state->permit_close; });
        check(permitted, "bounded backend-close continuation was not supplied");
    }
    owned_pending(*state->subject, file);
    state->recorded_close_result = state->close_result;
    delete file;  // The negative backend result is terminal and destructive too.
    gate.teardown.store(false);
    {
        std::lock_guard guard(state->mutex);
        state->close_returned = true;
    }
    state->condition.notify_all();
    // vfs_put_file is void in production; close errno does not replace fault status.
}
}
namespace ker::mod::sched {
bool kernel_threads_shutdown_requested() { return true; }  // Drain a preaccepted case.
void kern_block() { fixture::check(false, "accepted worker unexpectedly blocked instead of draining"); }
void wake_task_from_event(task::Task* target) {
    using namespace fixture;
    check(lock_depth == 0 && target != nullptr && target->ref_count.load() > 0, "event wake lost Task ownership");
    if (target == state->worker) { ++state->worker_wakes; return; }
    ++state->target_wakes;
    check(target == state->subject && target->ref_count.load() == 2, "completion wake missing accepted Task ref");
    check(target->lazy_file_fault_completion_generation.load() == state->generation,
          "wake preceded completion token");
    check(!state->final_put || (state->close_returned && state->file_destroyed), "wake preceded destructive callback return");
}
}
namespace ker::syscall::vmem {
bool materialize_lazy_file_page(ker::mod::sched::task::Task* subject,
    const ker::mod::sched::task::LazyVmemRange& range, uint64_t, const ker::mod::mm::paging::PageFault&) {
    using namespace fixture;
    ++state->materializations;
    check(subject == state->subject && range.file != nullptr && range.file->refs.load() > 0,
          "materializer lost accepted raw File owner");
    check(subject->usercopy_pagemap_accesses.load() == 1 && virt::user_pagemap_access_gates.at(0).readers.load() == 1,
          "worker released MM leases before materialization returned");
    check(state->worker->holds.load() == 1, "materialization lacks shutdown hold");
    return state->materialize_ok;  // Native fill/install mechanics are explicitly outside this test.
}
}
namespace fixture {
void one(unsigned disposition, int close_result, bool self_target, bool final_put) {
    ++cases;
    State local;
    state = &local;
    Task subject, worker;
    PageTable original{0}, replacement{1};
    subject.pagemap = &original;
    worker.type = sched::task::TaskType::DAEMON;
    worker.ref_count.store(2);  // Actual worker's scheduler + published-worker owner.
    local.subject = &subject; local.worker = &worker; local.close_result = close_result;
    local.self_target = self_target; local.final_put = final_put; local.materialize_ok = disposition == 0;
    auto* file = new ker::vfs::File;
    if (self_target) { file->target = &subject; ++subject.ref_count; }
    sched::task::LazyVmemRange range{.start=0x10000, .end=0x11000, .prot=3,
        .kind=sched::task::LazyVmemKind::FILE_BACKED, .file=file, .file_dev=1, .file_ino=2};
    subject.lazy_vmem_ranges.push_back(range);
    check(virt::user_pagemap_access_gates.at(0).readers.load() == 0 &&
          virt::user_pagemap_access_gates.at(1).readers.load() == 0, "previous root lease leaked");
    virt::lazy_file_fault_work = {};
    virt::lazy_file_fault_admission = true;
    virt::lazy_file_fault_worker_starting = false;
    virt::lazy_file_fault_worker = &worker;
    virt::lazy_file_fault_next_sequence = 1;
    sched::current = &subject;
    uint64_t generation{};
    auto const accepted = virt::probe_lazy_file_fault(&subject, range.start, {}, true, generation);
    check(accepted == virt::LazyFileFaultDisposition::PARK && generation == 1, "actual admission did not publish owned work");
    local.generation = generation;
    check(file->refs.load() == 2 && subject.usercopy_pagemap_accesses.load() == 1 &&
          subject.ref_count.load() == (self_target ? 3U : 2U), "accepted File/Task/MM lease counts wrong");
    if (final_put) {
        // Retire the initial range owner; the pool's independent retained File
        // is now last. The materializer's success/error is a declared seam.
        subject.lazy_vmem_ranges.clear();
        check(file->refs.fetch_sub(1) == 2, "initial range owner did not leave accepted File sole owner");
    }
    if (disposition == 2) subject.state.store(sched::task::TaskState::EXITING);
    if (disposition == 3) subject.pagemap = &replacement;
    std::thread executor([&] { sched::current = &worker; virt::lazy_file_fault_worker_loop(); sched::current = nullptr; });
    if (final_put) {
        std::unique_lock guard(local.mutex);
        bool const entered = local.condition.wait_for(guard, std::chrono::milliseconds(1000), [&] { return local.close_entered; });
        check(entered, "accepted completion did not reach backend close");
        if (entered) {
            check(!local.close_returned && subject.lazy_file_fault_completion_generation.load() == 0,
                  "blocked callback already exposed completion");
            check(worker.holds.load() == 1 && subject.ref_count.load() == (self_target ? 3U : 2U),
                  "blocked callback lost shutdown/Task ownership");
            uint64_t const flags = virt::lazy_file_fault_lock.lock_irqsave();
            check(virt::lazy_file_fault_work.at(0).task == &subject && virt::lazy_file_fault_work.at(0).issued,
                  "blocked callback lost issued pool entry");
            virt::lazy_file_fault_lock.unlock_irqrestore(flags);
        }
        local.permit_close = true;
        guard.unlock(); local.condition.notify_all();
    }
    executor.join();  // Always join the owned in-process worker before case storage leaves scope.
    unsigned const expected_reads = disposition < 2 ? 1 : 0;
    int const expected_status = disposition == 0 ? 0 : -EFAULT;
    check(local.materializations == expected_reads && local.puts == 1 && local.target_wakes == 1 && local.worker_wakes == 1,
          "actual worker callback/put/wake counts wrong");
    check(subject.lazy_file_fault_status == expected_status && subject.lazy_file_fault_completion_generation.load() == generation,
          "materializer result did not remain the terminal fault status");
    check(subject.usercopy_pagemap_accesses.load() == 0 && virt::user_pagemap_access_gates.at(0).readers.load() == 0 &&
          virt::user_pagemap_access_gates.at(1).readers.load() == 0, "actual completion leaked MM lease");
    check(worker.ref_count.load() == 1 && worker.holds.load() == 0 && virt::lazy_file_fault_worker == nullptr &&
          !virt::lazy_file_fault_admission, "actual worker drain leaked publication/hold/ref");
    for (auto const& work : virt::lazy_file_fault_work) check(work.task == nullptr, "accepted pool slot not retired");
    if (final_put) {
        check(local.file_destroyed && local.close_returned && local.recorded_close_result == close_result && local.exclusive_entered,
              "terminal close/exclusive/destruction trace wrong");
        check(subject.ref_count.load() == 1, "self-target or accepted Task ownership leaked after destructive close");
    } else {
        check(!local.file_destroyed && !local.close_entered && file->refs.load() == 1 && subject.ref_count.load() == 1,
              "nonfinal raw File put closed or lost remaining owner");
        delete file;  // Explicit fixture owner cleanup after actual worker returned.
    }
    subject.lazy_vmem_ranges.clear();
    sched::current = nullptr;
    state = nullptr;
}
}
int main() {
    for (unsigned disposition = 0; disposition < 4; ++disposition)
        for (int close_result : {0, -EIO}) for (bool self_target : {false, true})
            fixture::one(disposition, close_result, self_target, true);
    for (unsigned disposition = 0; disposition < 4; ++disposition)
        fixture::one(disposition, 0, false, false);
    std::printf("%s cases=%u assertions=%u closes=%u nonfinal_puts=%u order_failures=%u failures=%u actual_finish=1 actual_worker=1\n",
        fixture::failures.load() == 0 ? "PASS" : "FAIL", fixture::cases.load(), fixture::assertions.load(),
        fixture::close_calls.load(), fixture::nonfinal_puts.load(), fixture::order_failures.load(), fixture::failures.load());
    return fixture::failures.load() == 0 ? 0 : 1;
}
'''

def pin(path: Path) -> dict:
    data = path.read_bytes()
    return {"path": str(path.resolve()), "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def capture(source: str, name: str) -> tuple[str, dict]:
    """Return a complete actual definition; no body edits or substitutions."""
    clean = re.sub(r'//[^\n]*|/\*[\s\S]*?\*/|"(?:\\.|[^"\\])*"|\'(?:\\.|[^\'\\])*\'',
                   lambda match: " " * len(match[0]), source)
    matches = list(re.finditer(rf"^(?:auto|void|bool) {re.escape(name)}\([^{{;]*\{{", clean, re.M))
    if len(matches) != 1:
        raise ValueError(f"expected one complete actual definition for {name}, found {len(matches)}")
    start = matches[0].start()
    opening = clean.index("{", start)
    depth = 0
    for end in range(opening, len(clean)):
        if clean[end] == "{": depth += 1
        elif clean[end] == "}":
            depth -= 1
            if depth == 0:
                body = source[start:end + 1]
                return body, {"name": name, "line": source.count("\n", 0, start) + 1,
                              "bytes": len(body.encode()), "sha256": hashlib.sha256(body.encode()).hexdigest()}
    raise ValueError(f"unterminated actual definition: {name}")


def generate(virt: str, task: str, virt_path: Path, task_header: str) -> tuple[str, list[dict]]:
    begin = task_header.index("enum class LazyVmemKind")
    end = task_header.index("using LazyVmemRangeVec", begin)
    types = task_header[begin:end]
    actual = [{"name": "actual-LazyVmemKind-Range-and-pure-helpers", "bytes": len(types.encode()),
               "sha256": hashlib.sha256(types.encode()).hexdigest()},
              {"name": "whole-real-file_mapping_lifetime.hpp", **pin(FAMILY_HEADER)}]
    prefix = virt.index("enum class LazyFileFaultDisposition")
    suffix = virt.index("void wake_lazy_file_fault_worker()", prefix)
    declarations = virt[prefix:suffix]
    actual.append({"name": "actual-work-pool-declarations", "bytes": len(declarations.encode()),
                   "sha256": hashlib.sha256(declarations.encode()).hexdigest()})
    bodies = []
    for name in ["lazy_file_range_matches", "file_binding_matches_range", "release_user_pagemap_access", "wake_lazy_file_fault_worker",
                 "probe_lazy_file_fault", "take_lazy_file_fault", "finish_lazy_file_fault", "lazy_file_fault_worker_loop"]:
        if name == "file_binding_matches_range" and "auto file_binding_matches_range(" not in virt:
            continue  # Genuine older whole sources have no demand binding branch.
        body, row = capture(virt, name)
        actual.append(row)
        bodies.append(f'#line {row["line"]} {json.dumps(str(virt_path))}\n{body}')
    task_bodies = []
    for name in ["Task::try_acquire_usercopy_pagemap", "Task::release_usercopy_pagemap"]:
        body, row = capture(task, name)
        actual.append(row)
        task_bodies.append(body)
    # The complete actual Task methods use the fixture PageTable declaration,
    # aliased through the same namespace names; no body/type-name replacement.
    text = PRELUDE.replace("// ACTUAL_RAW_RANGE_TYPES", types)
    text += "namespace ker::mod::sched::task { namespace mm = ker::mod::mm;\n" + "\n".join(task_bodies) + "\n}\n"
    text += "namespace ker::mod::mm::virt {\n" + declarations + "\n" + "\n\n".join(bodies) + "\n}\n"
    return text + CASES, actual


SCRATCH_BUDGET = 64 * 1024 * 1024
LOG_BUDGET = 1024 * 1024
FILE_BUDGET = 8 * 1024 * 1024

# The bounded Linux child-session runner below is copied unchanged from the
# reviewed XFS actual-body regression; sequential compiler/runtime children only.


def allocated(path: Path) -> int:
    return sum(p.stat().st_blocks * 512 for p in path.rglob("*") if p.is_file())


def subreaper() -> None:
    # No privilege is required. Compiler grandchildren remain ours to reap if
    # their parent exits during a timeout. Never wait for another process group.
    if not sys.platform.startswith("linux"):
        raise ValueError("the bounded runner requires Linux child subreaping")
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:  # PR_SET_CHILD_SUBREAPER
        raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER")


def process_identity(pid: int) -> tuple[int, int, str, int, int]:
    fields = Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()
    return pid, int(fields[19]), fields[0], int(fields[1]), int(fields[3])


def owned_session(leader: int) -> list[tuple[int, int, str, int, int]]:
    result = []
    for path in Path("/proc").iterdir():
        if not path.name.isdecimal():
            continue
        try:
            identity = process_identity(int(path.name))
        except (FileNotFoundError, ProcessLookupError):
            continue
        # Same session includes children that changed their process group.
        # Sequential execution creates no other children in this driver; a
        # reparented direct child is therefore an owned compiler descendant.
        if identity[4] == leader or identity[3] == os.getpid():
            result.append(identity)
    return result


def kill_identity(identity: tuple[int, int, str, int, int]) -> None:
    pid, started, state, _, _ = identity
    if state == "Z":
        return
    try:
        fd = os.pidfd_open(pid, 0)
    except ProcessLookupError:
        return
    try:
        if process_identity(pid)[1] == started:
            signal.pidfd_send_signal(fd, signal.SIGKILL)
    except (FileNotFoundError, ProcessLookupError):
        pass
    finally:
        os.close(fd)


def reap_group(process: subprocess.Popen, deadline: float) -> None:
    # Keep the leader unreaped until its session and adopted descendants are
    # inactive. pidfds plus start times prevent PID reuse from changing targets.
    while True:
        identities = owned_session(process.pid)
        for identity in identities:
            kill_identity(identity)
        for pid, _, _, _, _ in identities:
            if pid == process.pid:
                continue
            try:
                os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                pass  # Still under its dying parent; subreaper will adopt it.
        remaining = owned_session(process.pid)
        if all(pid == process.pid and state == "Z" for pid, _, state, _, _ in remaining):
            process.wait(timeout=max(0.01, deadline - time.monotonic()))
            return  # Retained leader was the final owned session member.
        if time.monotonic() >= deadline:
            raise TimeoutError("owned session remains live; preserve its scratch")
        time.sleep(0.01)


def command(argv: list[str], log: Path, scratch: Path, deadline: float, groups: list[int]) -> dict:
    if time.monotonic() >= deadline - 5:
        raise TimeoutError("120-second whole-test budget exhausted before command")

    def limits() -> None:
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        resource.setrlimit(resource.RLIMIT_FSIZE, (FILE_BUDGET, FILE_BUDGET))

    started = time.monotonic()
    process = None
    with log.open("xb") as output:
        try:
            process = subprocess.Popen(argv, stdout=output, stderr=subprocess.STDOUT,
                                       start_new_session=True, preexec_fn=limits,
                                       env=dict(os.environ, TMPDIR=str(scratch),
                                                ASAN_OPTIONS="detect_leaks=0:halt_on_error=1",
                                                LSAN_OPTIONS="detect_leaks=0"))
            groups.append(process.pid)
            while True:
                if log.stat().st_size > LOG_BUDGET:
                    raise ValueError("command log exceeded 1 MiB")
                if allocated(scratch) > SCRATCH_BUDGET:
                    raise ValueError("private scratch exceeded 64 MiB")
                if time.monotonic() >= deadline - 5:
                    raise TimeoutError("120-second whole-test budget exhausted")
                # WNOWAIT deliberately leaves the owned leader's PID reserved.
                info = os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                if info is not None:
                    break
                time.sleep(0.02)
        finally:
            if process is not None:
                handlers = {signum: signal.signal(signum, signal.SIG_IGN)
                            for signum in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
                try:
                    reap_group(process, deadline)
                    groups.remove(process.pid)
                finally:
                    for signum, handler in handlers.items():
                        signal.signal(signum, handler)
    if log.stat().st_size > LOG_BUDGET or allocated(scratch) > SCRATCH_BUDGET:
        raise ValueError("completed command exceeded storage/output budget")
    return {"argv": argv, "exit": process.returncode, "seconds": round(time.monotonic() - started, 3),
            "owned_group_reaped": True, "log_bytes": log.stat().st_size}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--virt-source", type=Path, default=SOURCE)
    parser.add_argument("--task-source", type=Path, default=TASK)
    parser.add_argument("--cxx", default=os.environ.get("CXX", os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++")))
    args = parser.parse_args()
    started = time.monotonic()
    deadline = started + 120
    scratch = None
    groups = []
    watched, before = [], []
    result = {"pass": False, "commands": [], "scratch_removed": False,
              "raw_file_backed_vectors_only": True, "native_runtime_qualified": False,
              "native_owner_method_seams": "fatal unreachable branches, never a native executor model"}
    try:
        virt_path, task_path = args.virt_source.resolve(strict=True), args.task_source.resolve(strict=True)
        compiler = Path(args.cxx).resolve(strict=True)
        if not compiler.is_file() or not os.access(compiler, os.X_OK):
            raise ValueError("--cxx/CXX must name one host ELF compiler executable")
        with compiler.open("rb") as executable:
            if executable.read(4) != b"\x7fELF":
                raise ValueError("--cxx/CXX must name one host ELF compiler executable")
        subreaper()
        watched = [virt_path, task_path, TASK_HEADER, FAMILY_HEADER, Path(__file__).resolve(), compiler]
        before = [pin(path) for path in watched]
        result["virt_source"] = before[0]
        harness, actual = generate(virt_path.read_text(), task_path.read_text(), virt_path, TASK_HEADER.read_text())
        result["actual_definitions"] = actual
        scratch = Path(tempfile.mkdtemp(prefix="wos-lazy-file-close-", dir="/tmp"))
        result["scratch"] = str(scratch)
        source, binary = scratch / "close.cpp", scratch / "close-test"
        family = scratch / "include/platform/mm/file_mapping_lifetime.hpp"
        family.parent.mkdir(parents=True)
        family.write_bytes(FAMILY_HEADER.read_bytes())
        source.write_text(harness)
        commands = [
            [str(compiler), "-std=c++23", "-O1", "-g0", "-pipe", "-pthread", "-fsanitize=address,undefined",
             "-fno-omit-frame-pointer", "-I", str(scratch / "include"), str(source), "-o", str(binary)],
            [str(binary)],
        ]
        for index, argv in enumerate(commands):
            log_path = scratch / f"command-{index}.log"
            row = command(argv, log_path, scratch, deadline, groups)
            result["commands"].append(row)
            log = log_path.read_text(errors="replace")
            if index == 1 or row["exit"] != 0:
                print(log[-32768:], end="")
            if row["exit"] != 0:
                raise ValueError(f"actual-source {'compile/link' if index == 0 else 'runtime oracle'} failed with exit {row['exit']}")
            if index == 1 and not re.search(r"^PASS cases=20 assertions=\d+ closes=16 nonfinal_puts=4 order_failures=0 failures=0 actual_finish=1 actual_worker=1$", log, re.M):
                raise ValueError("complete compiled suite terminal PASS row is missing")
        result["pass"] = True
    except (OSError, ValueError, TimeoutError, subprocess.TimeoutExpired, KeyboardInterrupt) as error:
        result["error"] = f"{type(error).__name__}: {error}"
    finally:
        # The unchanged runner holds the leader identity until all owned session
        # members/adopted descendants are inactive and reaped. Never delete a
        # scratch tree while a command cleanup is still incomplete.
        if scratch is not None and not groups:
            shutil.rmtree(scratch)
            result["scratch_removed"] = not scratch.exists()
        if groups:
            result["pass"] = False
            result["cleanup_error"] = f"owned groups {groups} could not be reaped; scratch preserved"
        if before:
            try:
                result["sources_held"] = [pin(path) for path in watched] == before
            except OSError:
                result["sources_held"] = False
            if not result["sources_held"]:
                result["pass"] = False
                result["source_error"] = "selected actual source/driver/compiler changed during execution"
        if time.monotonic() > deadline:
            result["pass"] = False
            result["deadline_error"] = "whole invocation exceeded 120 seconds including cleanup"
        result["seconds"] = round(time.monotonic() - started, 3)
        print(json.dumps({key: value for key, value in result.items() if key != "actual_definitions"}))
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f"signal {signum}")

    for signum in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, interrupted)
    raise SystemExit(main())
