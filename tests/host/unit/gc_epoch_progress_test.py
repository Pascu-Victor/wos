#!/usr/bin/env python3
"""Execute actual epoch guards and exclusive GC progress without BSP timer ticks."""
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile

from exec_trace_ownership_test import block, function

ROOT = Path(__file__).resolve().parents[3]

PRELUDE = r'''
#include <array>
#include <atomic>
#include <cassert>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <thread>
#include <vector>
namespace ker::mod::cpu {
thread_local uint64_t on_cpu = 5;
uint64_t current_cpu() { return on_cpu; }
}
namespace ker::mod::smt {
uint64_t get_core_count() { return 8; }
uint64_t get_cpu_index_from_apic_id(uint64_t id) { return id; }
}
namespace ker::mod::apic { uint64_t get_apic_id() { return cpu::current_cpu(); } }
namespace ker::mod::dbg { void log(const char*) { assert(false); } }
HEADER
EPOCH_IMPLEMENTATION
namespace ker::mod::time {
uint64_t now = 100;
uint64_t get_us() { return now++; }
}
namespace ker::mod::sched {
DIAGNOSTICS
RESULT
std::atomic<bool> scheduler_gc_reclaim_active{false};
uint32_t gc_expired_tasks_budgeted_impl(uint32_t, uint64_t, uint32_t, uint64_t,
                                      uint64_t*, bool*, bool*, GcPressureDiagnostics*);
unsigned recorded = 0, scanned_calls = 0;
void note_gc_pass_result(uint32_t, uint64_t) {
    assert(scheduler_gc_reclaim_active.load()); ++recorded;
}
OWNED_PASS
// The cleanup seam uses the actual epoch predicate. It does not model Task,
// current-task/ref/zombie checks or pagemap destruction; those remain unchanged.
std::vector<uint64_t> deaths;
unsigned pool_free = 0;
bool check_nested = false, exhausted = false, pending = false;
uint32_t gc_expired_tasks_budgeted_impl(uint32_t budget, uint64_t us, uint32_t pages,
                                      uint64_t scan_budget, uint64_t* scanned,
                                      bool* timed_out, bool* has_pending,
                                      GcPressureDiagnostics* diagnostics) {
    assert(scheduler_gc_reclaim_active.load());
    assert(us == 50000 && pages == 65536);
    ++scanned_calls;
    if (check_nested) {
        auto before = EpochManager::current_epoch();
        auto nested = run_gc_reclaim_pass_exclusive(512, 50000, 65536, UINT64_MAX, nullptr);
        assert(!nested.ran && !nested.reclaimed && !nested.scanned);
        assert(EpochManager::current_epoch() == before);
    }
    uint32_t reclaimed = 0;
    *scanned = 0;
    for (auto it = deaths.begin(); it != deaths.end() && reclaimed < budget && *scanned < scan_budget;) {
        ++*scanned;
        if (EpochManager::is_safe_to_reclaim(*it)) {
            it = deaths.erase(it); ++reclaimed; ++pool_free;
        } else {
            if (diagnostics) ++diagnostics->epoch_skips;
            ++it;
        }
    }
    *timed_out = exhausted; *has_pending = pending;
    return reclaimed;
}
std::array<CpuEpoch, 8> states;
void reset() {
    assert(!scheduler_gc_reclaim_active.load());
    EpochManager::cpu_epochs = states.data();
    EpochManager::global_epoch.store(40);
    for (auto& state : states) {
        state.local_epoch.store(0); state.in_critical_section.store(false);
    }
    deaths.clear(); pool_free = recorded = scanned_calls = 0;
    check_nested = exhausted = pending = false;
    cpu::on_cpu = 5;
}
GcPassResult pass(uint32_t budget = 512, uint64_t scan_budget = UINT64_MAX,
                  GcPressureDiagnostics* diagnostics = nullptr) {
    return run_gc_reclaim_pass_exclusive(budget, 50000, 65536, scan_budget, diagnostics);
}
}
CASES
'''

CASES = r'''
int main() {
    using namespace ker::mod;
    using namespace sched;
    // Reproduce the observed same-epoch cohort without any CPU0 IRQ.
    reset(); deaths.assign(130, EpochManager::current_epoch());
    check_nested = true;
    auto result = pass();
    assert(pool_free == 130 && result.reclaimed == 130 && result.scanned == 130);
    assert(result.ran && recorded == 1 && scanned_calls == 1);
    assert(EpochManager::current_epoch() == 41 && !scheduler_gc_reclaim_active.load());

    // A concurrent owner must not manufacture additional progress or scan.
    reset(); scheduler_gc_reclaim_active.store(true);
    auto before = EpochManager::current_epoch(); result = pass();
    assert(!result.ran && !result.reclaimed && !result.scanned);
    assert(EpochManager::current_epoch() == before && !recorded && !scanned_calls);
    scheduler_gc_reclaim_active.store(false);

    // An old reader on every CPU, including the caller CPU, still blocks.
    for (uint64_t reader = 0; reader < 8; ++reader) {
        reset(); deaths.assign(130, EpochManager::current_epoch());
        EpochManager::enter_critical_for_cpu(reader);
        GcPressureDiagnostics diagnostics{};
        result = pass(512, UINT64_MAX, &diagnostics);
        assert(result.reclaimed == 0 && pool_free == 0 && diagnostics.epoch_skips == 130);
        assert(states[reader].in_critical_section.load() && states[reader].local_epoch.load() == 40);
        assert(!EpochManager::is_safe_to_reclaim(40));
        EpochManager::exit_critical_for_cpu(reader);
        result = pass(); assert(result.reclaimed == 130 && pool_free == 130);
    }

    // A reader entering after the age advance cannot hold the old cohort;
    // it still protects tasks retired in its own epoch through later passes.
    reset(); EpochManager::advance_epoch();
    EpochManager::enter_critical_for_cpu(3);
    assert(EpochManager::is_safe_to_reclaim(40));
    assert(!EpochManager::is_safe_to_reclaim(41));
    deaths = {40, 41}; result = pass();
    assert(result.reclaimed == 1 && deaths.size() == 1 && deaths[0] == 41);
    EpochManager::exit_critical_for_cpu(3); result = pass(); assert(result.reclaimed == 1);

    // Preserve both task and scan bounds and the original pending/time flags.
    reset(); deaths.assign(130, 40); pending = exhausted = true;
    result = pass(7, 3);
    assert(result.reclaimed == 3 && result.scanned == 3 && deaths.size() == 127);
    assert(result.time_budget_exhausted && result.has_pending_work);
    result = pass(7, 100); assert(result.reclaimed == 7 && result.scanned == 7);

    // Existing timer writers and the GC owner can increment concurrently.
    // This exercises the actual atomic increment; no read-side state is forced.
    reset(); std::vector<std::thread> writers;
    for (unsigned n = 0; n < 8; ++n) writers.emplace_back([] {
        for (unsigned i = 0; i < 1000; ++i) EpochManager::advance_epoch();
    });
    for (unsigned i = 0; i < 1000; ++i) { result = pass(); assert(result.ran); }
    for (auto& writer : writers) writer.join();
    assert(EpochManager::current_epoch() == 9040 && recorded == 1000);
    for (const auto& state : states) assert(!state.in_critical_section.load() && !state.local_epoch.load());
    puts("PASS actual GC epoch progress without BSP ticks; all old reader guards, busy ownership, bounds and concurrent atomic writers preserved");
}
'''


def main():
    scheduler = (ROOT / 'modules/kern/src/platform/sched/scheduler.cpp').read_text()
    header = (ROOT / 'modules/kern/src/platform/sched/epoch.hpp').read_text()
    # Expose only private state for deterministic host setup/inspection.
    header = header.replace('#pragma once', '').replace('   private:', '   public:')
    epoch = (ROOT / 'modules/kern/src/platform/sched/epoch.cpp').read_text()
    epoch = re.sub(r'^#include[^\n]*\n', '', epoch, flags=re.M)
    gc = function(scheduler, 'run_gc_reclaim_pass_exclusive')
    advance = '    EpochManager::advance_epoch();\n'
    assert gc.count(advance) == 1
    source = PRELUDE.replace('HEADER', header).replace('EPOCH_IMPLEMENTATION', epoch)
    sched_header = (ROOT / 'modules/kern/src/platform/sched/scheduler.hpp').read_text()
    source = source.replace('DIAGNOSTICS', block(sched_header, r'^struct GcPressureDiagnostics \{') + ';')
    source = source.replace('RESULT', block(scheduler, r'^struct GcPassResult \{') + ';')
    source = source.replace('CASES', CASES)
    early = gc.replace(advance, '')
    brace = early.index('{')
    early = early[:brace + 1] + '\n' + advance + early[brace + 1:]
    unsafe = source.replace('if (CPU_EPOCH <= death_epoch)', 'if (false && CPU_EPOCH <= death_epoch)')
    cases = [
        ('actual', source.replace('OWNED_PASS', gc), None),
        ('missing-advance', source.replace('OWNED_PASS', gc.replace(advance, '')), 'pool_free == 130'),
        ('advance-before-ownership', source.replace('OWNED_PASS', early), 'current_epoch() == before'),
        ('bypass-old-reader', unsafe.replace('OWNED_PASS', gc), 'result.reclaimed == 0'),
    ]
    compiler = shutil.which('c++', path=os.defpath)
    assert compiler
    with tempfile.TemporaryDirectory(prefix='wos-gc-epoch-') as directory:
        temp = Path(directory)
        for name, text, failure in cases:
            cpp, binary = temp / (name + '.cpp'), temp / name
            cpp.write_text(text)
            subprocess.run([compiler, '-std=c++23', '-O1', '-g', '-pthread', '-Wall', '-Wextra', '-Werror',
                            '-fsanitize=address,undefined', '-fno-sanitize-recover=all',
                            '-fno-omit-frame-pointer', str(cpp), '-o', str(binary)], check=True)
            result = subprocess.run([str(binary)], capture_output=True, text=True)
            if failure is None:
                assert result.returncode == 0, result.stdout + result.stderr
                print(result.stdout, end='')
            else:
                assert result.returncode != 0 and failure in result.stderr, result.stderr
                print(name + ': rejected by identical behavior assertions')


if __name__ == '__main__':
    main()
