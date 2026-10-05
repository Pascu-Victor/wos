#!/usr/bin/env python3
"""Execute actual pipe close callbacks and SmallVec with controlled scheduler endpoints."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
CORE = ROOT / 'modules/kern/src/vfs/core.cpp'
SMALLVEC = ROOT / 'modules/kern/src/util/smallvec.hpp'


def block(source, marker):
    start = source.index(marker)
    brace = source.index('{', start)
    depth = 1
    end = brace + 1
    while depth:
        depth += (source[end] == '{') - (source[end] == '}')
        end += 1
    return source[start:end]


PREFIX = r'''
#include <array>
#include <atomic>
#include <cassert>
#include <cstdlib>
#include <cstdio>
#include <new>
#include <utility>
bool locked = false, deny_allocations = false;
size_t array_allocations = 0;
void* operator new[](size_t size) {
    assert(!locked);
    ++array_allocations;
    if (deny_allocations) throw std::bad_alloc();
    void* result = std::malloc(size ? size : 1);
    if (!result) throw std::bad_alloc();
    return result;
}
void operator delete[](void* pointer) noexcept { assert(!locked); std::free(pointer); }
void operator delete[](void* pointer, size_t) noexcept { assert(!locked); std::free(pointer); }
void* operator new[](size_t size, const std::nothrow_t&) noexcept {
    try { return ::operator new[](size); }
    catch (const std::bad_alloc&) { return nullptr; }
}
void operator delete[](void* pointer, const std::nothrow_t&) noexcept { ::operator delete[](pointer); }
namespace ker::mod::sys {
struct Spinlock {
    uint64_t lock_irqsave() { assert(!locked); locked = true; return 17; }
    void unlock_irqrestore(uint64_t flags) { assert(locked && flags == 17); locked = false; }
};
}
struct File { void* private_data{}; int open_flags{}; };
struct log { template<class... Args> static void warn(const char*, Args...) {} };
bool pipe_diag_enabled() { return false; }
uint64_t perf_current_pid() { return 1; }
const char* current_task_name() { return "fixture"; }
struct Counts { unsigned finds{}, wakes{}, releases{}; uint64_t signals{}; };
Counts observed[4096]{};
bool missing = false, closing_read = false;
void check_delivery();
namespace ker::mod::sched {
struct Task {
    uint64_t pid{};
    void signal_add_pending_mask(uint64_t mask) { check_delivery(); observed[pid].signals |= mask; }
    void release() { check_delivery(); ++observed[pid].releases; }
};
Task tasks[4096]{};
Task* find_task_by_pid_safe(uint64_t pid) {
    check_delivery(); assert(pid < 4096); ++observed[pid].finds;
    if (missing && pid % 7 == 0) return nullptr;
    tasks[pid].pid = pid; return &tasks[pid];
}
void wake_task_from_event(Task* task) { check_delivery(); ++observed[task->pid].wakes; }
}
'''

MIDDLE = r'''
PipeState* active_state = nullptr;
unsigned destroyed = 0;
void check_delivery() {
    assert(!locked && active_state != nullptr);
    assert(closing_read ? active_state->read_closed : active_state->write_closed);
}
void pipe_destroy_state(PipeState* state) {
    assert(!locked && state == active_state);
    ++destroyed; active_state = nullptr; delete state;
}
'''

SUFFIX = r'''
int main() {
    constexpr size_t sizes[] = {0, 1, 2, 31, 32, 33, 63, 64, 65, 96, 128, 200, 256, 1024};
    unsigned cases = 0;
    for (bool close_read : {false, true}) for (size_t count : sizes)
        for (bool holes : {false, true}) for (int ends : {1, 2}) {
        closing_read = close_read; missing = holes; destroyed = 0;
        for (auto& row : observed) row = {};
        auto* state = new PipeState{};
        active_state = state; state->open_ends.store(ends); state->count = 7; state->capacity = 4096;
        auto& direct = close_read ? state->writers_waiting : state->readers_waiting;
        auto& pollers = close_read ? state->write_poll_waiting : state->read_poll_waiting;
        for (size_t i = 0; i < count; ++i) {
            assert(direct.push_back(i + 1)); assert(pollers.push_back(i + 2049));
        }
        File file{state, 1};
        size_t allocations_before = array_allocations;
        deny_allocations = true;
        assert((close_read ? pipe_close_read(&file) : pipe_close_write(&file)) == 0);
        deny_allocations = false;
        assert(array_allocations == allocations_before && !locked);
        for (size_t i = 0; i < count; ++i) for (unsigned list = 0; list < 2; ++list) {
            uint64_t pid = i + (list ? 2049 : 1);
            auto const& row = observed[pid];
            assert(row.finds == 1 && "ALL_TERMINAL_WAITERS_WOKEN");
            bool present = !(holes && pid % 7 == 0);
            assert(row.wakes == present && row.releases == present);
            assert(row.signals == (present && close_read && list == 0 ? uint64_t{1} << 12 : 0));
        }
        if (ends == 1) assert(destroyed == 1 && active_state == nullptr);
        else {
            assert(destroyed == 0 && active_state == state && state->open_ends.load() == 1);
            assert(direct.empty() && pollers.empty());
            assert(state->count == 7 && state->capacity == 4096);
            delete state; active_state = nullptr;
        }
        ++cases;
    }
    std::printf("ACTUAL_PIPE_CLOSE_CASES_PASS %u\n", cases);
}
'''


def harness(source):
    constants = source[source.index('constexpr size_t PIPE_WAKE_BATCH ='):source.index('constexpr size_t PIPE_DEFAULT_CAPACITY =')]
    aliases = ('using PipeWakeList = std::array<uint64_t, PIPE_WAKE_BATCH>;\n'
               'using PipeWaiterList = ker::util::SmallVec<uint64_t, PIPE_WAITER_INLINE_CAPACITY>;\n')
    pieces = [PREFIX, '#include "' + str(SMALLVEC) + '"\n', constants, aliases,
              block(source, 'struct PipeState {') + ';\n', MIDDLE,
              block(source, 'void pipe_collect_waiters_locked('),
              block(source, 'void pipe_reschedule_waiters(')]
    if 'void pipe_reschedule_all_waiters(' in source:
        pieces.append(block(source, 'void pipe_reschedule_all_waiters('))
    pieces.extend([block(source, 'static auto pipe_close_read = [](') + ';\n',
                   block(source, 'static auto pipe_close_write = [](') + ';\n', SUFFIX])
    return '\n'.join(pieces)


def execute(folder, label, source):
    generated = folder / (label + '.cpp')
    generated.write_text(harness(source))
    command = ['/usr/bin/clang++', '-std=c++23', '-O1', '-g', '-fsanitize=address,undefined',
               '-fno-omit-frame-pointer', '-I' + str(folder / 'include'), str(generated), '-o', str(folder / label)]
    compiled = subprocess.run(command, text=True, capture_output=True, timeout=45)
    assert compiled.returncode == 0, compiled.stderr
    environment = os.environ.copy()
    environment['ASAN_OPTIONS'] = 'detect_leaks=1:abort_on_error=1'
    environment['UBSAN_OPTIONS'] = 'halt_on_error=1:print_stacktrace=1'
    result = subprocess.run([str(folder / label)], text=True, capture_output=True, timeout=20, env=environment)
    record = dict(label=label, compile_command=command, returncode=result.returncode,
                  stdout=result.stdout, stderr=result.stderr,
                  generated_sha256=hashlib.sha256(generated.read_bytes()).hexdigest())
    (folder / (label + '-result.json')).write_text(json.dumps(record, indent=2) + '\n')
    return record


def run(folder, args):
    for path, text in [('platform/mm/dyn/kmalloc.hpp', '#pragma once\n'),
                       ('util/hcf.hpp', '#pragma once\n#include <cstdlib>\n[[noreturn]] inline void hcf() { std::abort(); }\n')]:
        destination = folder / 'include' / path
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(text)
    source = args.core.read_text()
    good = execute(folder, 'actual', source)
    assert good['returncode'] == 0, good['stderr']
    mutants = {
        'lost-readers': ('pending_readers = std::move(st->readers_waiting);', '/* lost readers */'),
        'lost-writers': ('pending_writers = std::move(st->writers_waiting);', '/* lost writers */'),
        'lost-read-pollers': ('pending_read_pollers = std::move(st->read_poll_waiting);', '/* lost read pollers */'),
        'lost-write-pollers': ('pending_write_pollers = std::move(st->write_poll_waiting);', '/* lost write pollers */'),
        'lost-sigpipe': ('pipe_reschedule_all_waiters(pending_writers, true);', 'pipe_reschedule_all_waiters(pending_writers);'),
        'lost-release': ('waiter->release();', '/* lost release */'),
        'lost-terminal-publication': ('st->write_closed = true;', 'st->write_closed = false;'),
    }
    rejected = []
    if args.negative_controls:
        for label, (old, new) in mutants.items():
            assert old in source, label
            changed = source.replace(old, new)
            failed = execute(folder, label, changed)
            assert failed['returncode'] != 0, label
            rejected.append(label)
    proof = dict(status='ACTUAL_PIPE_CLOSE_112_CASES_PASS', cases=112, rejected_mutants=rejected,
                 source_sha256=hashlib.sha256(source.encode()).hexdigest(),
                 smallvec_sha256=hashlib.sha256(SMALLVEC.read_bytes()).hexdigest(),
                 scope='Actual close lambdas, collection/wake helpers and real SmallVec with controlled '
                       'scheduler/lock/log/destruction endpoints, native ASan/UBSan. All terminal existing '
                       'waiter IDs, pollers, writer-only SIGPIPE, missing PID handling, retained task releases, '
                       'inline/spilled queue moves, allocation-free close and destruction-after-delivery. '
                       'Not live kernel scheduling or pressure recovery evidence.')
    (folder / 'proof.json').write_text(json.dumps(proof, indent=2) + '\n')
    print(proof['status'], 'mutants', len(rejected), flush=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--core', type=Path, default=CORE)
    parser.add_argument('--evidence-dir', type=Path)
    parser.add_argument('--negative-controls', action='store_true')
    args = parser.parse_args()
    if args.evidence_dir:
        args.evidence_dir.mkdir(exist_ok=False)
        run(args.evidence_dir, args)
    else:
        with tempfile.TemporaryDirectory(prefix='wos-pipe-close-') as temporary:
            run(Path(temporary), args)


if __name__ == '__main__':
    main()
