#!/usr/bin/env python3
"""Qualify actual CPU maintenance/18b TUs with finite Host ports.

No host CR3/MSR/cache instruction executes. Original production sources are
pinned and unchanged. Explicit compile copies add two approved pause calls,
narrow private limits, or inject a stale ACK for paired negative controls.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import sys
import tempfile
import time

sys.dont_write_bytecode = True
from file_mmap_registry_runtime_test import command, dependency_pins, pin, subreaper

ROOT = next(p for p in Path(__file__).resolve().parents if (p / 'modules/kern/src').is_dir())
KERN = ROOT / 'modules/kern/src'
UNIT = Path(__file__).resolve().parent
CORE = KERN / 'platform/mm/cpu_maintenance.cpp'
CASES = UNIT / 'cpu_maintenance_cases.cpp'
PORTS = UNIT / 'cpu_maintenance_ports.inc'
TOKEN_PORTS = UNIT / 'mmio_cache_runtime_test.cpp'
COMMON = [KERN / 'platform/mm' / (name + '.cpp') for name in
          ('mmio_cache', 'mtrr_policy', 'mmio_cache_model', 'mmio_cache_amd', 'mmio_cache_protected')]
OVERALL_SECONDS = 155


def replace_once(source: str, old: str, new: str) -> str:
    if source.count(old) != 1:
        raise ValueError(f'production anchor must occur exactly once: {old!r}')
    return source.replace(old, new, 1)


def copies(source: str) -> dict[str, tuple[str, str, bool]]:
    finite = replace_once(source, "constexpr size_t WAIT_STEPS = 1'000'000;", 'constexpr size_t WAIT_STEPS = 4096;')
    finite = replace_once(finite, 'constexpr uint64_t MAX_READERS = CLOSED - 1;', 'constexpr uint64_t MAX_READERS = 1;')
    before = '// Join precedes every ID/payload load; a pre-CAS scanner owns no old ID.\n'
    finite = replace_once(finite, before, before + '        maintenance_host::before_join(&slot);\n')
    ack = 'row.completed[logical].store(id, std::memory_order_release);'
    finite = replace_once(finite, ack, ack + '\n        maintenance_host::after_ack(&slot, id, logical);')
    generation = replace_once(source, 'constexpr uint64_t REQUEST_ID_LIMIT = UINT64_MAX;', 'constexpr uint64_t REQUEST_ID_LIMIT = 4;')
    stale = replace_once(finite, ack, 'row.completed[logical].store(id - 1, std::memory_order_release);')
    no_reader = replace_once(finite, 'slot_ = &slot;', 'slot.gate.fetch_sub(1, std::memory_order_release);\n        slot_ = &slot;')
    no_reader = replace_once(no_reader, 'if (slot_ != nullptr) slot_->gate.fetch_sub(1, std::memory_order_release);', '')
    no_phase = replace_once(finite, 'if (phase == Phase::translations && all_completed(*slot, 0)) {',
                           'if (phase == Phase::translations) {')
    begin = stale.index('auto all_completed(')
    end = stale.index('\n}\n\nclass Reader', begin) + 2
    weakened = replace_once(stale[begin:end], '!= slot.id', '== 0')
    no_id = stale[:begin] + weakened + stale[end:]
    return {
        'pristine': (source, 'CpuMaintenance.General*', False),
        'finite-pauses-and-reader-limit': (finite, 'CpuMaintenance.Hook*:CpuMaintenance.FiniteFrozen*', False),
        'finite-id-exhaustion': (generation, 'CpuMaintenance.FiniteGeneration*', False),
        'injected-stale-ack': (stale, 'CpuMaintenance.InjectedStale*', False),
        'control-reader-hold-removed': (no_reader, 'CpuMaintenance.HookAfterAckReaderTailBlocksCompletionAndSlotReuse', True),
        'control-phase-barrier-removed': (no_phase, 'CpuMaintenance.GeneralTlbDeferralCannotStartCacheAndExplicitSameIdRetryPreservesTimeoutHistory', True),
        'control-exact-id-removed': (no_id, 'CpuMaintenance.InjectedStaleCompletedIdCannotAdvanceTheRealRequest', True),
    }


def gtest_context(host_build: Path) -> tuple[Path, list[dict], dict]:
    """Use the exact maintained CMake target's headers with its GTest archives."""
    database = (host_build / 'compile_commands.json').resolve(strict=True)
    cache = (host_build / 'CMakeCache.txt').resolve(strict=True)
    context_pins = [pin(database), pin(cache)]
    rows = [row for row in json.loads(database.read_text())
            if (Path(row['directory']) / row['file']).resolve() == TOKEN_PORTS]
    if len(rows) != 1:
        raise ValueError('CMake must contain exactly one maintained mmio_cache_runtime_test compile row')
    row = rows[0]
    arguments = row.get('arguments')
    if arguments is None:
        arguments = shlex.split(row['command'])
    includes = []
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        value = None
        if argument in ('-I', '-isystem', '-iquote'):
            index += 1
            if index == len(arguments):
                raise ValueError('incomplete CMake include argument')
            value = arguments[index]
        elif argument.startswith('-isystem'):
            value = argument[len('-isystem'):]
        elif argument.startswith('-I'):
            value = argument[2:]
        if value:
            directory = (Path(row['directory']) / value).resolve()
            if (directory / 'gtest/gtest.h').is_file():
                includes.append(directory)
        index += 1
    roots = sorted(set(includes))
    if len(roots) != 1:
        raise ValueError('CMake compile row must select exactly one explicit GTest header root')
    header = roots[0] / 'gtest/gtest.h'
    context_pins.append(pin(header))
    if [pin(Path(item['path'])) for item in context_pins] != context_pins:
        raise ValueError('CMake/GTest context changed while selecting matching headers')
    return roots[0], context_pins, row


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--compiler', '--cxx', dest='compiler', default='/usr/bin/clang++')
    parser.add_argument('--host-build', type=Path, default=ROOT / 'build_tests')
    parser.add_argument('--evidence-dir', type=Path)
    args = parser.parse_args()
    deadline = time.monotonic() + OVERALL_SECONDS
    result = dict(pass_=False, commands=[], variants=[], sources_held=False,
                  scratch_removed=False, real_protocol_and_18b_tus=True,
                  host_instruction_ports_only=True, native_cpu_cache_proof=False,
                  whole_budget_seconds=OVERALL_SECONDS)
    scratch = None
    groups: list[int] = []
    watched: list[Path] = []
    before: list[dict] = []
    try:
        driver = Path(args.compiler).absolute()
        compiler = driver.resolve(strict=True)
        if not compiler.is_file() or not os.access(compiler, os.X_OK):
            raise ValueError('--compiler must name an executable compiler ELF')
        with compiler.open('rb') as executable:
            if executable.read(4) != b'\x7fELF':
                raise ValueError('--compiler must name an executable compiler ELF')
        libs = [args.host_build / 'lib/libgtest_main.a', args.host_build / 'lib/libgtest.a']
        gtest_include, cmake_pins, compile_row = gtest_context(args.host_build)
        result['gtest_context'] = dict(include=str(gtest_include), compile_row=compile_row, pins=cmake_pins)
        watched = [Path(__file__).resolve(), CORE, CASES, PORTS, TOKEN_PORTS, compiler,
                   UNIT / 'file_mmap_registry_runtime_test.py', *COMMON, *libs,
                   *(Path(item['path']) for item in cmake_pins)]
        before = [pin(p) for p in watched]
        if [pin(Path(item['path'])) for item in cmake_pins] != cmake_pins:
            raise ValueError('CMake/GTest context changed before input pinning')
        subreaper()
        scratch = Path(tempfile.mkdtemp(prefix='wos-cpu-maintenance-', dir='/tmp'))
        prefix_source = TOKEN_PORTS.read_text()
        boundary = 'namespace {\nnamespace cache = ker::mod::mm::mmio_cache;'
        if prefix_source.count(boundary) != 1:
            raise ValueError('maintained finite18b port prefix boundary changed')
        prefix = scratch / 'mmio_cache_runtime_ports_generated.hpp'
        prefix.write_text(prefix_source[:prefix_source.index(boundary)])
        hooks = scratch / 'host_hook_declarations.hpp'
        hooks.write_text('#include <cstddef>\n#include <cstdint>\nnamespace maintenance_host {\n'
                         'void before_join(void*);\nvoid after_ack(void*, uint64_t, size_t);\n}\n')
        flags = [str(driver), '-std=c++23', '-O1', '-g0', '-pipe', '-pthread', '-Wall', '-Wextra', '-Werror',
                 '-DWOS_SELFTEST=1', '-fsanitize=address,undefined', '-fno-sanitize-recover=all',
                 '-isystem', str(gtest_include),
                 '-fno-omit-frame-pointer', '-I', str(scratch), '-I', str(UNIT), '-I', str(KERN),
                 '-I', str(KERN / 'platform/mm'), '-I', str(ROOT / 'modules'), '-include', str(hooks)]
        variants = copies(CORE.read_text())
        sources: dict[str, Path] = {}
        for name, (source, _, _) in variants.items():
            p = scratch / (name + '.cpp')
            p.write_text(source)
            sources[name] = p
            result['variants'].append(dict(name=name, source=pin(p), compile_copy=source != CORE.read_text(),
                                           injected_stale_ack='stale' in name or 'exact-id' in name,
                                           negative_control=variants[name][2]))
        units = [(p.stem, p) for p in COMMON] + [('cases', CASES)] + list(sources.items())
        dependencies: dict[str, Path] = {}
        for name, source in units:
            dep = scratch / (name + '.discovered.d')
            row = command(flags + ['-M', '-MF', str(dep), '-MT', name, str(source)],
                          scratch / (name + '-discover.log'), scratch, deadline, groups)
            result['commands'].append(row)
            if row['exit'] != 0:
                print((scratch / (name + '-discover.log')).read_text(errors='replace')[-16000:])
                raise ValueError(f'dependency discovery failed: {name}')
            for item in dependency_pins(dep): dependencies[item['path']] = Path(item['path'])
        # Discover then pin the COMPLETE compiled header/input closure before
        # compilation; compare actual compile depfiles to this immutable set.
        if [pin(p) for p in watched] != before:
            raise ValueError('original inputs changed during dependency discovery')
        watched = sorted(set(watched) | set(dependencies.values()))
        before = [pin(p) for p in watched]
        result['compiled_input_pins'] = before
        objects: dict[str, Path] = {}
        for name, source in units:
            obj = scratch / (name + '.o')
            dep = scratch / (name + '.actual.d')
            row = command(flags + ['-MD', '-MF', str(dep), '-c', str(source), '-o', str(obj)],
                          scratch / (name + '-compile.log'), scratch, deadline, groups)
            result['commands'].append(row)
            if row['exit'] != 0:
                print((scratch / (name + '-compile.log')).read_text(errors='replace')[-24000:])
                raise ValueError(f'actual full-TU compile failed: {name}')
            actual = dependency_pins(dep)
            if any(item['path'] not in dependencies for item in actual):
                raise ValueError(f'compiled undeclared input: {name}')
            objects[name] = obj
        common_objects = [str(objects[p.stem]) for p in COMMON] + [str(objects['cases'])]
        for name, (_, test_filter, control) in variants.items():
            binary = scratch / (name + '.bin')
            row = command([str(driver), '-pthread', '-fsanitize=address,undefined', '-fno-sanitize-recover=all',
                           *common_objects, str(objects[name]), *map(str, libs), '-o', str(binary)],
                          scratch / (name + '-link.log'), scratch, deadline, groups)
            result['commands'].append(row)
            if row['exit'] != 0:
                print((scratch / (name + '-link.log')).read_text(errors='replace')[-16000:])
                raise ValueError(f'actual full-TU link failed: {name}')
            log_path = scratch / (name + '-run.log')
            row = command([str(binary), '--gtest_filter=' + test_filter, '--gtest_color=no'],
                          log_path, scratch, deadline, groups)
            result['commands'].append(row)
            variant_receipt = next(item for item in result['variants'] if item['name'] == name)
            variant_receipt.update(binary=pin(binary), run_exit=row['exit'], test_filter=test_filter)
            log = log_path.read_text(errors='replace')
            print(f'[{name}]\n{log[-20000:]}', end='')
            if re.search(r'AddressSanitizer|runtime error:|UndefinedBehaviorSanitizer', log):
                raise ValueError(f'sanitizer failure, not a qualified oracle: {name}')
            if control:
                exact = test_filter.removeprefix('CpuMaintenance.')
                if row['exit'] != 1 or f'[  FAILED  ] CpuMaintenance.{exact}' not in log:
                    raise ValueError(f'compile-success control did not fail its exact oracle: {name}')
            elif row['exit'] != 0 or not re.search(r'\[  PASSED  \] [1-9][0-9]* tests?\.', log):
                raise ValueError(f'actual protocol oracle failed: {name}')
            # command() has reaped the exact owned session before disposable
            # compiler outputs are removed; source copies/logs stay for pins.
            binary.unlink()
            objects[name].unlink()
        result['pass_'] = True
    except (OSError, ValueError, TimeoutError, KeyboardInterrupt) as error:
        result['error'] = f'{type(error).__name__}: {error}'
    finally:
        if before:
            try: result['sources_held'] = [pin(p) for p in watched] == before
            except OSError: result['sources_held'] = False
            if not result['sources_held']: result['pass_'] = False
        if args.evidence_dir is not None:
            try:
                args.evidence_dir.mkdir(parents=True, exist_ok=True)
                if scratch is not None:
                    for p in scratch.glob('*.log'):
                        destination = args.evidence_dir / p.name
                        if destination.exists(): raise ValueError(f'evidence exists: {destination}')
                        shutil.copyfile(p, destination)
            except (OSError, ValueError) as error:
                result['pass_'] = False
                result['evidence_error'] = str(error)
        if scratch is not None and not groups:
            shutil.rmtree(scratch)
            result['scratch_removed'] = not scratch.exists()
        if groups:
            result['pass_'] = False
            result['cleanup_error'] = f'owned sessions remain: {groups}; scratch preserved'
        if time.monotonic() > deadline:
            result['pass_'] = False
            result['deadline_error'] = 'whole invocation/cleanup deadline exceeded'
        if args.evidence_dir is not None:
            try:
                destination = args.evidence_dir / 'result.json'
                if destination.exists(): raise ValueError('result evidence already exists')
                destination.write_text(json.dumps(result, indent=2) + '\n')
            except (OSError, ValueError) as error:
                result['pass_'] = False
                result['evidence_error'] = str(error)
        print(json.dumps(result))
    return 0 if result['pass_'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
