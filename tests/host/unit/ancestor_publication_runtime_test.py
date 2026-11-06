#!/usr/bin/env python3
"""Run the actual ancestor publisher and paging primitives on finite Host effects.

--expect-old-race records an expected failing unchanged-old-code oracle. That
mode never grants current green qualification, even when reproduction completes.
No endpoint installs the rival: the allocator re-enters the actual function.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import time

sys.dont_write_bytecode = True
from cpu_maintenance_runtime_test import actual_definition, gtest_context
from file_mmap_registry_runtime_test import command, dependency_pins, pin, subreaper

ROOT = next(p for p in Path(__file__).resolve().parents if (p / 'modules/kern/src').is_dir())
KERN = ROOT / 'modules/kern/src'
UNIT = Path(__file__).resolve().parent
SOURCE = KERN / 'platform/mm/virt.opt.cpp'
PAGING = KERN / 'platform/mm/paging.opt.cpp'
PORTS = UNIT / 'ancestor_publication_ports.inc'
CASES = UNIT / 'ancestor_publication_cases.cpp'
ORACLE = 'AncestorPublication.OrdinaryAllocatorRivalIsPreservedAndLoserReturned'
ANCHOR = 'auto advance_page_table(paging::PageTable* page_table, size_t level, uint64_t flags, bool* promoted) -> paging::PageTable* {'


def replace_once(text, old, new):
    if text.count(old) != 1:
        raise ValueError(f'actual control anchor must be unique: {old}')
    return text.replace(old, new, 1)


def controls(body):
    result = []
    result.append(('skip-rival-recheck', replace_once(body, 'if (entry.present != 0) {',
                  'if (entry.present != 0 && spare == nullptr) {'), ORACLE))
    child = 'auto* child = reinterpret_cast<PageTable*>(addr::get_virt_pointer(entry.frame << paging::PAGE_SHIFT));'
    moved = replace_once(body, child + '\n            require_ordinary_page_table(child);', 'PageTable* child;')
    moved = replace_once(moved, '            user_mapping_pin_lock.unlock_irqrestore(pin);\n            if (spare != nullptr',
        '            user_mapping_pin_lock.unlock_irqrestore(pin);\n'
        '            child = reinterpret_cast<PageTable*>(addr::get_virt_pointer(entry.frame << paging::PAGE_SHIFT));\n'
        '            require_ordinary_page_table(child);\n            if (spare != nullptr')
    result.append(('lookup-child-after-pin', moved,
                   'AncestorPublication.SelectedChildObservationIsInsidePinInOrdinaryMode'))
    history = 'if (typed_mmio_guards.load(std::memory_order_relaxed)) mmio::detail::note_topology_publication_locked();'
    if body.count(history) != 2:
        raise ValueError('actual two effect-specific typed history gates changed')
    result.append(('ordinary-calls-typed-history', body.replace(history, 'mmio::detail::note_topology_publication_locked();', 1),
                   'AncestorPublication.TypedHistoryTracksOnlyActualTypedEffects'))
    loser = 'if (spare != nullptr && !try_release_page_table_to_pool(spare, {__builtin_return_address(0), 0})) phys::page_free(spare);'
    result.append(('lose-unused-spare', replace_once(body, loser, '(void)spare;'), ORACLE))
    promotion = '''            if (changed) {
                if (promoted != nullptr) *promoted = true;
                wrcr3(rdcr3());
            }'''
    result.append(('omit-promotion-report-and-flush', replace_once(body, promotion, '            (void)promoted;'),
                   'AncestorPublication.ExistingAncestorMergesOnlyRequestedPermissions'))
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source', type=Path, default=SOURCE)
    parser.add_argument('--cxx', default='/usr/bin/clang++')
    parser.add_argument('--host-build', type=Path, default=ROOT / 'build_tests')
    parser.add_argument('--evidence-dir', type=Path)
    parser.add_argument('--expect-old-race', action='store_true')
    args = parser.parse_args()
    deadline = time.monotonic() + 120
    scratch, watched, before, groups = None, [], [], []
    result = dict(pass_=False, old_bug_reproduced=False, commands=[], runs=[], controls=[], sources_held=False,
                  compiled_inputs_held=False, scratch_removed=False, actual_ancestor=True,
                  actual_paging=True, finite_effect_ports=True, mmio_capability_factory=False,
                  native_runtime_qualified=False, expected_old_failure=args.expect_old_race)
    completed = False
    try:
        source = args.source.resolve(strict=True)
        driver = Path(args.cxx).absolute()
        compiler = driver.resolve(strict=True)
        include, cmake_pins, _ = gtest_context(args.host_build)
        libs = [args.host_build / 'lib/libgtest_main.a', args.host_build / 'lib/libgtest.a']
        watched = sorted(set([Path(__file__).resolve(), PORTS, CASES, source, PAGING, compiler,
            UNIT / 'cpu_maintenance_runtime_test.py', UNIT / 'file_mmap_registry_runtime_test.py',
            ROOT / 'tests/CMakeLists.txt', ROOT / 'tests/host/CMakeLists.txt', *libs,
            *(Path(row['path']) for row in cmake_pins)]))
        before = [pin(p) for p in watched]
        text = source.read_text()
        body = actual_definition(text, ANCHOR)
        origin = actual_definition(text, 'struct PageTableReleaseOrigin {', True)
        helpers = ''.join(actual_definition(text, anchor) for anchor in (
            '[[gnu::always_inline]] auto typed_mmio_table_allowed(',
            'void require_ordinary_page_table(', 'auto entry_at(PageTable* table, size_t index) -> PageTableEntry& {'))
        result['actual_source'] = pin(source)
        result['ancestor_body_sha256'] = hashlib.sha256(body.encode()).hexdigest()
        if args.expect_old_race and result['ancestor_body_sha256'] != 'ca3219c04c8e618d3cf30eddc9c50a8ccb73a50cb641fec233e209d9da101e17':
            raise ValueError('old proof requires exact unchanged79585ff9 ancestor body')
        subreaper()
        scratch = Path(tempfile.mkdtemp(prefix='wos-ancestor-', dir='/tmp'))
        ports = PORTS.read_text()
        if ports.count('// ACTUAL_RELEASE_ORIGIN') != 1:
            raise ValueError('origin insertion marker changed')
        (scratch / PORTS.name).write_text(ports.replace('// ACTUAL_RELEASE_ORIGIN', origin, 1))
        prefix = '#include "ancestor_publication_ports.inc"\nnamespace ker::mod::mm::virt {\nstd::atomic<bool> typed_mmio_guards{};\n' + helpers
        core = scratch / 'actual-ancestor.cpp'
        core.write_text(prefix + body + '\n}\n')
        plans = [('old' if args.expect_old_race else 'pristine', ORACLE if args.expect_old_race else '*', core)]
        control_units = []
        if not args.expect_old_race:
            for name, mutated, oracle in controls(body):
                unit = scratch / (name + '.cpp')
                unit.write_text(prefix + mutated + '\n}\n')
                control_units.append(unit)
                plans.append((name, oracle, unit))
        cases = scratch / CASES.name
        cases.write_bytes(CASES.read_bytes())
        flags = [str(driver), '-std=c++23', '-O1', '-g0', '-pthread', '-Wall', '-Wextra', '-Werror',
            '-UNDEBUG', '-Wno-packed-non-pod', '-fsanitize=address,undefined', '-fno-sanitize-recover=all',
            '-fno-omit-frame-pointer', '-I', str(scratch), '-I', str(ROOT / 'tests/host/kern_shim'),
            '-I', str(KERN), '-I', str(ROOT / 'modules'), '-isystem', str(include)]
        units = [core, cases, PAGING, *control_units]
        dependencies = {}
        for i, unit in enumerate(units):
            dep = scratch / f'{i}-discover.d'
            row = command(flags + ['-M', '-MF', str(dep), '-MT', str(i), str(unit)],
                          scratch / f'{i}-discover.log', scratch, deadline, groups)
            result['commands'].append(row)
            if row['exit'] != 0: raise ValueError(f'dependency discovery failed: {unit.name}')
            for item in dependency_pins(dep): dependencies[item['path']] = Path(item['path'])
        if [pin(p) for p in watched] != before: raise ValueError('source changed during discovery')
        watched = sorted(set(watched) | set(dependencies.values()))
        before = [pin(p) for p in watched]
        result['compiled_input_pins'] = before
        result['persistent_input_pins'] = [row for row in before if not Path(row['path']).is_relative_to(scratch)]
        objects = {}
        for i, unit in enumerate(units):
            obj, dep = scratch / f'{i}.o', scratch / f'{i}-actual.d'
            row = command(flags + ['-MD', '-MF', str(dep), '-c', str(unit), '-o', str(obj)],
                          scratch / f'{i}-compile.log', scratch, deadline, groups)
            result['commands'].append(row)
            if row['exit'] != 0: raise ValueError(f'actual source compile failed: {unit.name}')
            if any(item['path'] not in dependencies for item in dependency_pins(dep)):
                raise ValueError('compiled undeclared dependency')
            objects[unit] = obj
        for name, selected, unit in plans:
            binary = scratch / (name + '.bin')
            row = command([str(driver), '-pthread', '-fsanitize=address,undefined', '-fno-sanitize-recover=all',
                          *map(str, (objects[unit], objects[cases], objects[PAGING])), *map(str, libs), '-o', str(binary)],
                          scratch / (name + '-link.log'), scratch, deadline, groups)
            result['commands'].append(row)
            if row['exit'] != 0: raise ValueError(f'actual source link failed: {name}')
            log = scratch / (name + '-run.log')
            row = command([str(binary), '--gtest_color=no', '--gtest_filter=' + selected], log, scratch, deadline, groups)
            result['commands'].append(row)
            output = log.read_text(errors='replace')
            result['runs'].append(dict(name=name, test_filter=selected, exit=row['exit'], binary=pin(binary), log=pin(log)))
            if re.search(r'AddressSanitizer|runtime error:|UndefinedBehaviorSanitizer', output):
                raise ValueError(f'sanitizer failure is not a qualified ancestor oracle: {name}')
            if args.expect_old_race:
                if row['exit'] != 1 or '[  FAILED  ] ' + ORACLE not in output or 'outer allocation must not overwrite' not in output:
                    raise ValueError('old source did not reproduce exact winner-overwrite assertion')
                result['old_bug_reproduced'] = True
                result['qualification_status'] = 'expected old-code failure; no current green credit'
            elif name == 'pristine':
                passed = re.search(r'\[  PASSED  \] (\d+) tests?\.', output)
                if row['exit'] != 0 or not passed: raise ValueError('actual ancestor GTests failed')
                result['positive_tests'] = int(passed[1])
            elif row['exit'] != 1 or '[  FAILED  ] ' + selected not in output:
                raise ValueError(f'compiled control did not fail its exact connected oracle: {name}')
            else:
                result['controls'].append(dict(name=name, test=selected, compiled=True, detected=True))
            binary.unlink()
        if not args.expect_old_race: result['pass_'] = True
        completed = True
    except (OSError, ValueError, TimeoutError, KeyboardInterrupt) as error:
        result['error'] = f'{type(error).__name__}: {error}'
    finally:
        if before:
            try: result['sources_held'] = [pin(p) for p in watched] == before
            except OSError: result['sources_held'] = False
            result['compiled_inputs_held'] = result['sources_held'] and 'compiled_input_pins' in result
            if not result['sources_held']: result['pass_'] = completed = False
        if args.evidence_dir is not None:
            try:
                args.evidence_dir.mkdir(parents=True, exist_ok=True)
                if scratch is not None:
                    for item in scratch.iterdir():
                        if item.suffix not in ('.log', '.cpp', '.inc'): continue
                        dest = args.evidence_dir / item.name
                        if dest.exists(): raise ValueError('evidence path already exists')
                        shutil.copyfile(item, dest)
                result['retained_files'] = [pin(p) for p in sorted(args.evidence_dir.iterdir()) if p.is_file()]
            except (OSError, ValueError) as error:
                result['evidence_error'] = str(error); result['pass_'] = completed = False
        if scratch is not None and not groups:
            shutil.rmtree(scratch); result['scratch_removed'] = not scratch.exists()
        if groups:
            result['cleanup_error'] = f'owned sessions remain: {groups}'; result['pass_'] = completed = False
        if time.monotonic() > deadline:
            result['deadline_error'] = True; result['pass_'] = completed = False
        if args.evidence_dir is not None:
            path = args.evidence_dir / 'result.json'
            if path.exists(): result['pass_'] = completed = False
            else: path.write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps({key: result[key] for key in ('pass_', 'positive_tests', 'controls', 'old_bug_reproduced', 'sources_held',
            'compiled_inputs_held', 'scratch_removed', 'error') if key in result}))
    return 0 if completed else 1


if __name__ == '__main__':
    raise SystemExit(main())
