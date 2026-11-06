#!/usr/bin/env python3
"""Qualify actual CPU policy GTests and a precise selector5 removal control.

Only the existing finite CPU observation ports replace hardware effects. Every
token is minted by the actual constinit Runtime after real configure/capture.
"""
from __future__ import annotations

import argparse
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

ROOT = next(path for path in Path(__file__).resolve().parents if (path / 'modules/kern/src').is_dir())
KERN = ROOT / 'modules/kern/src'
UNIT = Path(__file__).resolve().parent
MODELS = ['mtrr_policy', 'mmio_cache_model', 'mmio_cache_amd', 'mmio_cache_protected', 'mmio_cache']
TESTS = ['mtrr_policy', 'mmio_cache_model', 'mmio_cache_amd', 'mmio_cache_protected', 'mmio_cache_observation', 'mmio_cache_runtime']
ORACLE = 'MmioCacheRuntime.WcSelectorRefusalCannotMintAndDoesNotPoisonUc'


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cxx', default='/usr/bin/clang++')
    parser.add_argument('--host-build', type=Path, default=ROOT / 'build_tests')
    parser.add_argument('--evidence-dir', type=Path)
    args = parser.parse_args()
    deadline = time.monotonic() + 180
    scratch, groups, watched, before = None, [], [], []
    result = dict(pass_=False, commands=[], runs=[], sources_held=False, compiled_inputs_held=False,
                  scratch_removed=False, actual_policy_runtime=True, no_capability_factory=True,
                  host_instruction_ports_only=True, native_hardware_qualified=False)
    try:
        driver = Path(args.cxx).absolute()
        compiler = driver.resolve(strict=True)
        if not compiler.is_file() or not os.access(compiler, os.X_OK):
            raise ValueError('--cxx must name an executable compiler')
        gtest_include, cmake_pins, _ = gtest_context(args.host_build)
        libs = [args.host_build / 'lib/libgtest_main.a', args.host_build / 'lib/libgtest.a']
        production = [KERN / 'platform/mm' / (name + '.cpp') for name in MODELS]
        cases = [UNIT / (name + '_test.cpp') for name in TESTS]
        watched = [Path(__file__).resolve(), UNIT / 'cpu_maintenance_runtime_test.py',
                   UNIT / 'file_mmap_registry_runtime_test.py', ROOT / 'tests/CMakeLists.txt',
                   ROOT / 'tests/host/CMakeLists.txt', compiler, *production, *cases, *libs,
                   *(Path(row['path']) for row in cmake_pins)]
        watched = sorted(set(watched))
        before = [pin(path) for path in watched]
        subreaper()
        scratch = Path(tempfile.mkdtemp(prefix='wos-wc-policy-', dir='/tmp'))
        model = KERN / 'platform/mm/mmio_cache_model.cpp'
        original = model.read_text()
        body = actual_definition(original, 'auto qualify_cpu_wc(')
        selector = 'if (((policy.pat >> 40) & 255U) != 1) return Status::not_strict_wc;'
        if body.count(selector) != 1:
            raise ValueError('actual fixed-selector5 control anchor must be unique')
        changed = body.replace(selector, '(void)0;  // compile-copy removal of actual fixed WC selector gate', 1)
        control = scratch / 'control-mmio_cache_model.cpp'
        control.write_text(original.replace(body, changed, 1))
        result['selector_control_copy'] = pin(control)
        result['selector_control_anchor'] = selector
        flags = [str(driver), '-std=c++23', '-O1', '-g0', '-pipe', '-pthread', '-Wall', '-Wextra', '-Werror', '-UNDEBUG',
                 '-fsanitize=address,undefined', '-fno-sanitize-recover=all', '-fno-omit-frame-pointer',
                 '-I', str(ROOT / 'tests/host/kern_shim'), '-I', str(KERN), '-I', str(KERN / 'platform/mm'), '-I', str(KERN / 'std'),
                 '-I', str(ROOT / 'modules'), '-isystem', str(gtest_include)]
        units = production + cases + [control]
        dependencies = {}
        for index, source in enumerate(units):
            dep = scratch / f'{index}-discover.d'
            row = command(flags + ['-M', '-MF', str(dep), '-MT', str(index), str(source)],
                          scratch / f'{index}-discover.log', scratch, deadline, groups)
            result['commands'].append(row)
            if row['exit'] != 0:
                raise ValueError(f'dependency discovery failed: {source.name}')
            for item in dependency_pins(dep):
                dependencies[item['path']] = Path(item['path'])
        if [pin(path) for path in watched] != before:
            raise ValueError('watched input changed during discovery')
        watched = sorted(set(watched) | set(dependencies.values()))
        before = [pin(path) for path in watched]
        result['compiled_input_pins'] = before
        objects = {}
        for index, source in enumerate(units):
            obj, dep = scratch / f'{index}.o', scratch / f'{index}-actual.d'
            row = command(flags + ['-MD', '-MF', str(dep), '-c', str(source), '-o', str(obj)],
                          scratch / f'{index}-compile.log', scratch, deadline, groups)
            result['commands'].append(row)
            if row['exit'] != 0:
                raise ValueError(f'actual source compile failed: {source.name}')
            if any(item['path'] not in dependencies for item in dependency_pins(dep)):
                raise ValueError(f'undeclared compiled input: {source.name}')
            objects[source] = obj
        for name in ('pristine', 'remove-fixed-wc-selector'):
            selected = [objects[control] if name != 'pristine' and source == model else objects[source]
                        for source in production + cases]
            binary = scratch / (name + '.bin')
            row = command([str(driver), '-pthread', '-fsanitize=address,undefined', '-fno-sanitize-recover=all',
                           *map(str, selected), *map(str, libs), '-o', str(binary)],
                          scratch / (name + '-link.log'), scratch, deadline, groups)
            result['commands'].append(row)
            if row['exit'] != 0:
                raise ValueError(f'actual policy link failed: {name}')
            log = scratch / (name + '-run.log')
            row = command([str(binary), '--gtest_color=no', '--gtest_filter=' + ('*' if name == 'pristine' else ORACLE)],
                          log, scratch, deadline, groups)
            result['commands'].append(row)
            output = log.read_text(errors='replace')
            if re.search(r'AddressSanitizer|runtime error:|UndefinedBehaviorSanitizer', output):
                raise ValueError('sanitizer failure is not a qualified WC oracle')
            if name == 'pristine':
                passed = re.search(r'\[  PASSED  \] (\d+) tests?\.', output)
                if row['exit'] != 0 or not passed:
                    raise ValueError('actual policy/runtime GTests failed')
                result['positive_tests'] = int(passed[1])
            elif row['exit'] != 1 or '[  FAILED  ] ' + ORACLE not in output:
                raise ValueError('compiled missing-selector control did not fail its exact actual-runtime oracle')
            else:
                result['selector_control_detected'] = True
            result['runs'].append(dict(name=name, exit=row['exit'], binary=pin(binary), log=pin(log),
                                       test_filter='*' if name == 'pristine' else ORACLE))
        result['pass_'] = True
    except (OSError, ValueError, TimeoutError, KeyboardInterrupt) as error:
        result['error'] = f'{type(error).__name__}: {error}'
    finally:
        if before:
            try:
                result['sources_held'] = [pin(path) for path in watched] == before
                result['compiled_inputs_held'] = result['sources_held'] and 'compiled_input_pins' in result
            except OSError:
                result['sources_held'] = False
            if not result['sources_held']:
                result['pass_'] = False
        if args.evidence_dir is not None:
            try:
                args.evidence_dir.mkdir(parents=True, exist_ok=True)
                if scratch is not None:
                    for log in scratch.glob('*.log'):
                        destination = args.evidence_dir / log.name
                        if destination.exists():
                            raise ValueError(f'evidence already exists: {destination}')
                        shutil.copyfile(log, destination)
            except (OSError, ValueError) as error:
                result['pass_'] = False
                result['evidence_error'] = str(error)
        if scratch is not None and not groups:
            shutil.rmtree(scratch)
            result['scratch_removed'] = not scratch.exists()
        if groups:
            result['pass_'] = False
            result['cleanup_error'] = f'owned sessions remain: {groups}'
        if time.monotonic() > deadline:
            result['pass_'] = False
            result['deadline_error'] = 'bounded whole invocation deadline exceeded'
        if args.evidence_dir is not None:
            target = args.evidence_dir / 'result.json'
            if target.exists():
                result['pass_'] = False
                result['evidence_error'] = f'evidence already exists: {target}'
            else:
                target.write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps({key: value for key, value in result.items() if key not in ('commands', 'compiled_input_pins')}))
    return 0 if result['pass_'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
