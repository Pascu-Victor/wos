#!/usr/bin/env python3
"""Execute real IDT publication bodies with finite construction/LIDT boundaries.

Native threads run the complete initializer/getter and real descriptor mapper,
setter, types and storage. Only mapper-entry scheduling and the privileged LIDT
instruction are finite endpoints. This does not qualify hardware IDTR fetching,
interrupt delivery, AP startup or the cause of any observed reset.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import tempfile
import time

from file_mmap_registry_runtime_test import capture, command, dependency_pins, pin, subreaper

ROOT = next(p for p in Path(__file__).resolve().parents if (p / 'modules/kern/src').is_dir())
INTERRUPT = ROOT / 'modules/kern/src/platform/interrupt'
SOURCE = INTERRUPT / 'idt.cpp'
HEADER = INTERRUPT / 'idt.hpp'
GDT = INTERRUPT / 'gdt.hpp'

# Complete pre679 production storage, initializer and readiness getter.
ORIGINAL_STATE = '''alignas(0x10) std::array<IdtEntry, IDT_ENTRIES> idt{};  // at most 256 interrupts
IdtPtr idt_ptr;
uint8_t idt_ready_state = 0;
'''
ORIGINAL_INIT = '''void idt_init() {
    map_isr_entries();

    idt_ptr.limit = static_cast<uint16_t>(sizeof(idt)) - 1;
    idt_ptr.base = reinterpret_cast<uint64_t>(idt.data());

    load_idt();
    __atomic_store_n(&idt_ready_state, static_cast<uint8_t>(1), __ATOMIC_RELEASE);
}'''
ORIGINAL_READY = '''auto is_idt_ready() -> bool { return __atomic_load_n(&idt_ready_state, __ATOMIC_ACQUIRE) != 0; }'''
ORIGINAL_SHA256 = 'ca3bcd14ec46f9c3ffe1cb9469f31818189fd5bc019453adc0edf8e48eab7f4e'

PRELUDE = r'''
#include <array>
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string_view>
#include <thread>
namespace fixture {
constexpr unsigned CPUS = 8;
thread_local unsigned cpu = 0;
std::atomic<unsigned> checks{}, builders{}, started{}, returned{}, loads{};
std::atomic<const char*> failure{};
std::array<std::atomic<unsigned>, CPUS> local_loads{};
std::atomic<bool> entered{}, allow_build{}, builder_loaded{}, allow_load_return{};
const auto expires = std::chrono::steady_clock::now() + std::chrono::seconds(5);
void check(bool value, const char* why) {
    checks.fetch_add(1, std::memory_order_relaxed);
    if (!value) {
        const char* none = nullptr;
        failure.compare_exchange_strong(none, why);
    }
}
template<class Predicate> void wait(Predicate done) {
    while (!done()) {
        if (std::chrono::steady_clock::now() >= expires) {
            check(false, "bounded endpoint wait");
            allow_build.store(true);
            allow_load_return.store(true);
            return;
        }
        std::this_thread::yield();
    }
}
struct Threads {
    std::array<std::thread, CPUS> values;
    ~Threads() {
        allow_build.store(true, std::memory_order_release);
        allow_load_return.store(true, std::memory_order_release);
        for (auto& thread : values) if (thread.joinable()) thread.join();
    }
};
void enter_map();
void load();
}
namespace ker::mod::desc::gdt {
@SELECTOR@
}
namespace ker::mod::desc::idt {
@LAYOUT@
@STATE@
@CALC@
@SETTER@
@ISR_STUBS@
@MACROS@
@MAPPER@
auto is_idt_ready() -> bool;
void load_idt();
void map_isr_entries() { fixture::enter_map(); actual_map_isr_entries(); }
@INITIALIZER@
@READY@
@LOAD@
static_assert(sizeof(IdtEntry) == 16 && sizeof(IdtPtr) == 10);
static_assert(__atomic_always_lock_free(sizeof(idt_ready_state), nullptr));
}
'''

CASES = r'''
namespace fixture {
namespace actual = ker::mod::desc::idt;
void enter_map() {
    check(builders.fetch_add(1, std::memory_order_relaxed) == 0, "duplicate construction");
    entered.store(true, std::memory_order_release);
    wait([] { return allow_build.load(std::memory_order_acquire); });
}
void load() {
    check(cpu < CPUS, "local CPU identity");
    check(builders.load() == 1, "exactly one completed constructor");
    const std::array<uintptr_t, actual::IDT_ENTRIES> targets = {@TARGETS@};
    const std::array<unsigned, actual::IDT_ENTRIES> dpl = {@DPL@};
    const std::array<unsigned, actual::IDT_ENTRIES> gate = {@GATES@};
    for (size_t i = 0; i < actual::idt.size(); ++i) {
        const auto& entry = actual::idt[i];
        const uint64_t address = uint64_t(entry.offset0) | (uint64_t(entry.offset1) << 16) | (uint64_t(entry.offset2) << 32);
        check(address == targets[i], "all256 complete ISR addresses before local load");
        check(entry.kernel_cs == ker::mod::desc::gdt::GDT_KERN_CS && entry.ist == 0 && entry.reserved0 == 0 &&
              entry.gate_type == gate[i] && entry.zero0 == 0 && entry.dpl == dpl[i] && entry.present == 1 &&
              entry.reserved1 == 0, "all descriptor fields before local load");
    }
    check(actual::idt_ptr.base == reinterpret_cast<uint64_t>(actual::idt.data()) &&
          actual::idt_ptr.limit == sizeof(actual::idt) - 1, "complete IDTR operand before local load");
    const unsigned previous = local_loads[cpu].fetch_add(1);
    loads.fetch_add(1);
    if (cpu == 0 && previous == 0) {
        check(!actual::is_idt_ready(), "constructor local load precedes ready");
        builder_loaded.store(true, std::memory_order_release);
        wait([] { return allow_load_return.load(std::memory_order_acquire); });
    } else {
        check(actual::is_idt_ready(), "other local loads follow acquire-ready");
    }
}
void invoke(unsigned id) {
    cpu = id;
    started.fetch_add(1);
    actual::idt_init();
    check(local_loads[id].load() != 0 && actual::is_idt_ready(), "each caller returns after its own load and ready");
    returned.fetch_add(1);
}
int run(std::string_view mode) {
    check(!actual::is_idt_ready(), "uninitialized is not ready");
    if (mode == "concurrent") {
        Threads threads;
        threads.values[0] = std::thread([] { invoke(0); });
        wait([] { return entered.load(std::memory_order_acquire); });
        check(!actual::is_idt_ready(), "building is not ready");
        for (unsigned i = 1; i < CPUS; ++i) threads.values[i] = std::thread([i] { invoke(i); });
        wait([] { return started.load() == CPUS; });
        check(returned.load() == 0 && loads.load() == 0, "no load or return while construction blocked");
        allow_build.store(true, std::memory_order_release);
        wait([] { return builder_loaded.load(std::memory_order_acquire); });
        check(!actual::is_idt_ready(), "local load must return before ready");
        check(loads.load() == 1 && returned.load() == 0, "losers wait through constructor local load");
        allow_load_return.store(true, std::memory_order_release);
        // Deferred oracle failures are reported only after this scope releases
        // both constructor gates and joins every thread, including exceptions.
    } else {
        check(mode == "repeat", "known test mode");
        allow_build.store(true);
        allow_load_return.store(true);
        invoke(0);
        // The genuine old initializer is run only sequentially. Its second
        // call records duplicate construction, never creating a C++ race.
        for (unsigned i = 1; i < CPUS; ++i) invoke(i);
    }
    check(returned.load() == CPUS && builders.load() == 1, "all callers complete one construction");
    std::array<unsigned char, sizeof(actual::idt)> snapshot{};
    std::memcpy(snapshot.data(), actual::idt.data(), snapshot.size());
    const auto before = actual::idt_ptr;
    for (unsigned i = 0; i < CPUS; ++i) invoke(i);
    check(builders.load() == 1 && loads.load() == 2 * CPUS && returned.load() == 2 * CPUS,
          "subsequent calls only reload their own IDTR");
    check(std::memcmp(snapshot.data(), actual::idt.data(), snapshot.size()) == 0 &&
          before.base == actual::idt_ptr.base && before.limit == actual::idt_ptr.limit,
          "published descriptors and operand remain immutable");
    for (const auto& count : local_loads) check(count.load() == 2, "every CPU loads once per invocation");
    if (const char* why = failure.load()) {
        std::fprintf(stderr, "FAIL idt_once %s\n", why);
        return 1;
    }
    std::printf("PASS idt_once mode=%.*s callers=8 constructions=1 loads=16 assertions=%u\n",
                int(mode.size()), mode.data(), checks.load());
    return 0;
}
}
int main(int argc, char** argv) {
    if (argc != 2) return 1;
    return fixture::run(argv[1]);
}
'''


def generate(source: str, header: str, gdt: str, control: str) -> tuple[str, dict]:
    retained = '\n'.join((ORIGINAL_STATE, ORIGINAL_INIT, ORIGINAL_READY))
    if hashlib.sha256(retained.encode()).hexdigest() != ORIGINAL_SHA256:
        raise ValueError('complete retained pre679 source changed')
    bodies = {}
    facts = {}
    for name, anchor in [('calc', 'inline auto calc_idt_entry('), ('setter', 'void idt_set_entry('),
                         ('mapper', 'void map_isr_entries()'), ('initializer', 'void idt_init()'),
                         ('ready', 'auto is_idt_ready()'), ('load', 'void load_idt()')]:
        bodies[name], facts[name] = capture(source, anchor)
    layout = header[header.index('inline constexpr uint64_t IDT_ENTRIES'):header.index('\nvoid idt_set_entry(')]
    state = source[source.index('alignas(0x10) std::array<IdtEntry'):source.index('\ninline auto calc_idt_entry(')]
    selector = re.findall(r'^constexpr static uint64_t GDT_KERN_CS = [^\n]+', gdt, re.M)
    macros = re.findall(r'^#define ISR_[A-Z_]+\(n\) [^\n]+', source, re.M)
    entries = re.findall(r'\b(ISR_ENTRY|ISR_ENTRY_USER|ISR_TRAP_ENTRY|ISR_EXCEPT_ENTRY)\((\d+)\);', bodies['mapper'])
    if len(selector) != 1 or len(macros) != 4 or [int(n) for _, n in entries] != list(range(256)):
        raise ValueError('actual selector/macros/all256 ordered map entries required')
    instruction = '__asm__ volatile("lidt %0" : : "m"(idt_ptr) : "memory");'
    if re.sub(r'//[^\n]*|\s+', '', bodies['load']) != re.sub(r'\s+', '', 'void load_idt() {' + instruction + '}'):
        raise ValueError('whole actual LIDT body must retain exact operand and compiler memory clobber')
    if control == 'original':
        state, bodies['initializer'], bodies['ready'] = ORIGINAL_STATE, ORIGINAL_INIT, ORIGINAL_READY
    elif control == 'early-ready':
        before = '== IDT_READY'
        if bodies['ready'].count(before) != 1:
            raise ValueError('one exact readiness weakening required')
        bodies['ready'] = bodies['ready'].replace(before, '!= IDT_UNINITIALIZED')
    elif control != 'current':
        raise ValueError('unknown control')
    names = [('isr_except' if 'EXCEPT' in kind or 'TRAP' in kind else 'isr') + n for kind, n in entries]
    values = {'SELECTOR': selector[0], 'LAYOUT': layout, 'STATE': state, 'CALC': bodies['calc'],
              'SETTER': bodies['setter'], 'ISR_STUBS': '\n'.join('void ' + name + '() {}' for name in names),
              'MACROS': '\n'.join(macros),
              'MAPPER': bodies['mapper'].replace('void map_isr_entries()', 'void actual_map_isr_entries()', 1),
              'INITIALIZER': bodies['initializer'], 'READY': bodies['ready'],
              'LOAD': bodies['load'].replace(instruction, 'fixture::load();'),
              'TARGETS': ','.join('reinterpret_cast<uintptr_t>(&actual::' + name + ')' for name in names),
              'DPL': ','.join('3' if kind == 'ISR_ENTRY_USER' else '0' for kind, _ in entries),
              'GATES': ','.join('actual::IDT_TRAP_GATE' if kind == 'ISR_TRAP_ENTRY' else 'actual::IDT_INTERRUPT_GATE'
                                for kind, _ in entries)}
    text = PRELUDE + CASES
    for name, value in values.items():
        marker = '@' + name + '@'
        if text.count(marker) != 1:
            raise ValueError('one explicit finite seam required: ' + name)
        text = text.replace(marker, value)
    facts.update({name: dict(bytes=len(value.encode()), sha256=hashlib.sha256(value.encode()).hexdigest())
                  for name, value in [('layout', layout), ('state', state), ('selector', selector[0])]})
    return text, facts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cxx', default=os.environ.get('CXX', os.environ.get('WOS_HOST_CXX', '/usr/bin/clang++')))
    args = parser.parse_args()
    deadline = time.monotonic() + 90
    scratch = None
    scratch_identity = None
    groups, watched, before, staged, staged_before = [], [], [], [], []
    result = dict(pass_=False, commands=[], scratch_removed=False, hardware_IDTR_IRQ_AP_qualified=False,
                  hardware_reset_cause_qualified=False, whole_budget_seconds=90)
    try:
        compiler = Path(args.cxx).resolve(strict=True)
        if not compiler.is_file() or not os.access(compiler, os.X_OK):
            raise ValueError('--cxx must name an executable compiler ELF')
        with compiler.open('rb') as executable:
            if executable.read(4) != b'\x7fELF':
                raise ValueError('--cxx must name an executable compiler ELF')
        subreaper()
        watched = [SOURCE, HEADER, GDT, Path(__file__).resolve(),
                   ROOT / 'tests/host/unit/file_mmap_registry_runtime_test.py', compiler]
        before = [pin(path) for path in watched]
        result['sources_before'] = before
        scratch = Path(tempfile.mkdtemp(prefix='wos-idt-once-', dir='/tmp'))
        info = scratch.stat()
        scratch_identity = (info.st_dev, info.st_ino, info.st_uid)
        if scratch.is_symlink() or info.st_uid != os.getuid():
            raise ValueError('fresh scratch ownership is not exact')
        for control in ('current', 'original', 'early-ready'):
            text, facts = generate(SOURCE.read_text(), HEADER.read_text(), GDT.read_text(), control)
            result.setdefault('actual_definitions', {})[control] = facts
            translation, binary, depfile = (scratch / (control + suffix) for suffix in ('.cpp', '', '.d'))
            translation.write_text(text)
            if staged_before and [pin(path) for path in staged] != staged_before:
                raise ValueError('earlier staged source changed before next control')
            staged = sorted(scratch.glob('*.cpp'))
            staged_before = [pin(path) for path in staged]
            row = command([str(compiler), '-std=c++23', '-O1', '-g0', '-pipe', '-Wall', '-Wextra', '-Werror',
                           '-pthread', '-fsanitize=address,undefined', '-fno-omit-frame-pointer',
                           '-MD', '-MF', str(depfile), str(translation), '-o', str(binary)],
                          scratch / (control + '-compile.log'), scratch, deadline, groups)
            result['commands'].append(row)
            if depfile.is_file():
                result.setdefault('emitted_dependencies_after_only', {})[control] = dependency_pins(depfile)
            if row['exit'] != 0:
                print((scratch / (control + '-compile.log')).read_text(errors='replace')[-32768:])
                raise ValueError(control + ': compilation is not an accepted negative control')
            modes = ('repeat', 'concurrent') if control == 'current' else (('repeat',) if control == 'original' else ('concurrent',))
            for mode in modes:
                log_path = scratch / (control + '-' + mode + '.log')
                row = command([str(binary), mode], log_path, scratch, deadline, groups)
                result['commands'].append(row)
                log = log_path.read_text(errors='replace')
                print(log[-32768:], end='')
                if control == 'current':
                    if row['exit'] != 0 or not re.search(r'^PASS idt_once mode=' + mode +
                                                       r' callers=8 constructions=1 loads=16 assertions=\d+$', log, re.M):
                        raise ValueError('actual current initializer failed its complete publication oracle')
                else:
                    marker = 'FAIL idt_once ' + ('duplicate construction' if control == 'original' else 'building is not ready')
                    if row['exit'] != 1 or marker not in log.splitlines():
                        raise ValueError(control + ': compiled control must fail the same intended exact oracle')
        result['pass_'] = True
    except (OSError, ValueError, TimeoutError, KeyboardInterrupt) as error:
        result['error'] = f'{type(error).__name__}: {error}'
    finally:
        if staged_before:
            try:
                result['staged_sources_held'] = [pin(path) for path in staged] == staged_before
            except OSError:
                result['staged_sources_held'] = False
            if not result['staged_sources_held']:
                result['pass_'] = False
        if before:
            try:
                result['sources_after'] = [pin(path) for path in watched]
                result['sources_held'] = result['sources_after'] == before
            except OSError:
                result['sources_held'] = False
            if not result['sources_held']:
                result['pass_'] = False
        if scratch is not None and not groups:
            info = scratch.stat()
            if scratch.is_symlink() or (info.st_dev, info.st_ino, info.st_uid) != scratch_identity:
                result['pass_'] = False
                result['cleanup_error'] = 'scratch ownership changed; preserve it'
            else:
                shutil.rmtree(scratch)
                result['scratch_removed'] = not scratch.exists()
        if groups:
            result['pass_'] = False
            result['cleanup_error'] = f'owned groups {groups}; scratch preserved'
        if time.monotonic() > deadline:
            result['pass_'] = False
            result['deadline_error'] = 'whole90-second invocation including cleanup exceeded'
        print(json.dumps(result), flush=True)
    return 0 if result['pass_'] and result['scratch_removed'] and time.monotonic() < deadline else 1


if __name__ == '__main__':
    def interrupted(signum, frame):
        raise KeyboardInterrupt(f'signal {signum}')
    for signum in (signal.SIGTERM, signal.SIGHUP):
        signal.signal(signum, interrupted)
    raise SystemExit(main())
