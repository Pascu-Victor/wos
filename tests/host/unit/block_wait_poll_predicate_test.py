#!/usr/bin/env python3
"""Compile the actual private wait qualification functions without exporting them."""
import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "modules/kern/src/dev/block_request.cpp"


def extract(text, anchor):
    begin = text.index(anchor)
    end = text.index("\n}", begin) + 2
    if anchor.startswith("struct "):
        end += 1
    return text[begin:end]


def main():
    actual = SOURCE.read_text()
    sections = [extract(actual, anchor) for anchor in ("struct BlockRequestQueue {", "auto safe_syscall_task()",
                                                     "auto wait_poll_context(", "auto wait_poll_allowed(")]
    harness = r'''
#include <cassert>
#include <dev/block_device.hpp>
#include <dev/block_request.hpp>
#include <mod/io/serial/serial.hpp>
#include <platform/sched/scheduler.hpp>
#include <platform/sys/mutex.hpp>
#include <platform/sys/spinlock.hpp>
namespace ker::dev {
namespace sched = ker::mod::sched;
ACTUAL
}
using namespace ker::dev;
namespace sched = ker::mod::sched;
int main() {
    BlockRequestQueue q;
    BlockRequest owner, impostor;
    sched::task::Task task;
    task.syscall_account_start_us = 1;
    sched::block_test_current_task = &task;
    q.native = true;
    q.limits.depth = 8;
    BlockRequestDescription description{BlockRequestOperation::READ, 0, 1, 512};
    assert(block_model_submit(q.model, description, owner.token) == 0);
    BlockRequestToken issued;
    assert(block_model_next_issue(q.model, issued));
    assert(issued == owner.token);
    q.owners[owner.token.slot] = &owner;
    owner.deadline_us = 2000;
    unsigned checks = 0;
    auto check = [&](bool expected) {
        assert(wait_poll_allowed(q, owner, 1000) == expected);
        assert(ker::mod::sys::block_test_spinlock_depth == 0);
        ++checks;
    };
    check(true);
    task.deferred_task_switch = task.yield_switch = true;
    check(true); // Root-authorized exact channel-free return yield.
    task.wait_channel = "waitpid";
    task.wait_channel_kind = sched::task::WaitChannelKind::WAITPID;
    check(false);
    task.wait_channel = nullptr;
    check(false); // A stale/non-NONE kind is not the authorized pair.
    task.wait_channel_kind = sched::task::WaitChannelKind::NONE;
    task.yield_switch = false;
    check(false);
    task.deferred_task_switch = false;
    task.syscall_account_start_us = 0; check(false); task.syscall_account_start_us = 1;
    task.preempt_disable_depth = 1; check(false); task.preempt_disable_depth = 0;
    task.scheduler_transition_active.store(true); check(false); task.scheduler_transition_active.store(false);
    task.wants_block = true; check(false); task.wants_block = false;
    task.voluntary_block = true; check(false); task.voluntary_block = false;
    task.wki_proxy_task_id = 9; check(false); task.wki_proxy_task_id = 0;
    task.wait_channel = "proxy"; task.wait_channel_kind = sched::task::WaitChannelKind::WKI_EXECVE_PROXY;
    check(false); task.wait_channel = nullptr; task.wait_channel_kind = sched::task::WaitChannelKind::NONE;
    task.interrupting_signal_pending = true; check(false); task.interrupting_signal_pending = false;
    sched::block_test_shutdown.store(true); check(false); sched::block_test_shutdown.store(false);
    ker::mod::io::serial::block_test_panic_mode.store(true); check(false); ker::mod::io::serial::block_test_panic_mode.store(false);
    sched::block_test_run_queues.store(false); check(false); sched::block_test_run_queues.store(true);
    sched::block_test_current_task = nullptr; check(false); sched::block_test_current_task = &task;
    task.type = sched::task::TaskType::DAEMON; check(false);
    task.type = sched::task::TaskType::IDLE; check(false); task.type = sched::task::TaskType::PROCESS;
    q.native = false; check(false); q.native = true;
    q.limits.depth = 1; check(false); q.limits.depth = 8;
    q.model.admission_open = false; check(false); q.model.admission_open = true;
    q.closing = true; check(false); q.closing = false;
    q.caller_driven = true; check(false); q.caller_driven = false;
    q.model.resetting = true; check(false); q.model.resetting = false;
    q.owners[owner.token.slot] = &impostor; check(false); q.owners[owner.token.slot] = &owner;
    ++owner.token.request_generation; check(false); --owner.token.request_generation;
    owner.deadline_us = 1000; check(false); owner.deadline_us = 2000;
    auto* state = block_model_detail::lookup(q.model, owner.token);
    state->state = BlockRequestState::QUEUED; check(false);
    state->state = BlockRequestState::COMPLETING; check(false); state->state = BlockRequestState::ISSUED;
    assert(block_model_cancel(q.model, owner.token, -ECANCELED) == BlockCancelResult::NEEDS_RETIREMENT);
    check(false);
    assert(task.references.load() == 1);
    assert(checks == 31);
}
'''.replace("ACTUAL", "\n".join(sections))
    temp_root = Path(os.environ.get("TMPDIR", ROOT / "build/goal30-tmp"))
    temp_root.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="block-wait-predicate-", dir=temp_root) as directory:
        work = Path(directory)
        src, exe = work / "test.cpp", work / "test"
        src.write_text(harness)
        subprocess.run([os.environ.get("WOS_HOST_CXX", "/usr/bin/clang++"), "-std=c++23", "-O1", "-g",
                        "-fsanitize=address,undefined", "-fno-omit-frame-pointer", "-pthread",
                        "-I", str(ROOT / "tests/host/unit/block_request_runtime_shim"),
                        "-I", str(ROOT / "modules/kern/src"), str(src), "-o", str(exe)], check=True)
        subprocess.run([str(exe)], check=True, env=dict(os.environ, ASAN_OPTIONS="detect_leaks=0", LSAN_OPTIONS="detect_leaks=0"))
    print(json.dumps({"actual_source_predicate_cases": 31, "pass": True, "source": str(SOURCE.relative_to(ROOT))}))


if __name__ == "__main__":
    main()
