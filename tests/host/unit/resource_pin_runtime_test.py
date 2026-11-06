#!/usr/bin/env python3
"""Exercise actual pin-accounting CPP and resource/export bridges on finite ports.

The runtime and selected unchanged Owner/LifetimeOwner, resource and procfs/WKI
bodies issue every charge. Task layout, IRQ locks, usercopy, namespace and VFS
view endpoints are explicit Host seams. No page, root, DMA, syscall scheduler or
complete WKI completion lifecycle is qualified. The completion export branch
is copied unchanged into controlled dispatch; its surrounding network loop is
not modeled. Finite profiles alter only private production constants.
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
CORE = KERN / 'platform/sched/resource_pin.cpp'
PORTS = UNIT / 'resource_pin_ports.inc'
CASES = UNIT / 'resource_pin_cases.cpp'
CONTROLS = {
    'remove-process-close': ('bridges', 'close_pin_account_locked(*owner);', '(void)0;',
                             'PinAccounting.WholeProcessClosePublishesToRetainedCharges'),
    'remove-global-refund': ('core', 'statistics_.current_bytes -= BYTES;', '(void)0;',
                             'PinAccounting.RefundReturnsActualGlobalCurrentAfterTaskDestruction'),
    'remove-syscall-refresh': ('bridges', 'if (resource == static_cast<unsigned>(Id::MEMLOCK)) refresh_pin_usage_locked(*process);',
                              '(void)0;', 'PinAccounting.SyscallUsageRefreshesRefundAndPreservesImportedHistory'),
    'remove-procfs-refresh': ('procfs', 'refresh_pin_usage_locked(*process);', '(void)0;',
                             'PinAccounting.ProcfsFreshSnapshotRefreshesWhileCachedViewStaysStable'),
    'remove-wki-submit-refresh': ('submit', 'ker::mod::sched::resource::refresh_pin_usage_locked(*owner.get());', '(void)0;',
                                 'PinAccounting.WkiSubmissionRefreshesRefundWithoutErasingLiteralHistory'),
    'remove-wki-completion-refresh': ('completion', 'ker::mod::sched::resource::refresh_pin_usage_locked(*task);', '(void)0;',
                                     'PinAccounting.WkiCompletionBranchRefreshesActualRetainedTaskUsage'),
}


def replace_once(text: str, old: str, new: str) -> str:
    if text.count(old) != 1:
        raise ValueError(f'actual compile-copy anchor must be unique: {old}')
    return text.replace(old, new, 1)


def generate(scratch: Path) -> tuple[dict, list[dict], list[Path]]:
    sources = {name: KERN / rel for name, rel in {
        'resource': 'platform/sched/resource.cpp', 'task': 'platform/sched/task.hpp',
        'procfs': 'vfs/fs/procfs.cpp', 'wki': 'net/wki/remote_compute.cpp'}.items()}
    raw = {name: path.read_text() for name, path in sources.items()}
    pieces = []

    def selected(name, anchor):
        body = actual_definition(raw[name], anchor)
        pieces.append(dict(source=pin(sources[name]), anchor=anchor, bytes=len(body.encode()),
                           sha256=hashlib.sha256(body.encode()).hexdigest(), unchanged=True))
        return body

    task_methods = ''.join(selected('task', anchor) for anchor in (
        'bool try_acquire() {', 'void release() {',
        '[[nodiscard]] auto credential_owner() -> Task* {',
        '[[nodiscard]] auto credential_owner() const -> const Task* {'))
    ports = replace_once(PORTS.read_text(), '// ACTUAL_TASK_REFERENCE_METHODS', task_methods)
    ports = replace_once(ports, '// ACTUAL_PROCFS_LIMITS',
                        'auto procfs_read_limits(const ker::mod::security::Snapshot&, uint64_t, ProcFileData&, void*, size_t, size_t) -> ssize_t;')
    ports = replace_once(ports, '// ACTUAL_WKI_EXPORT_DECLARATIONS', '''
auto snapshot_task_resources(Task*, ker::net::wki::TaskResourceSnapshot&, bool) -> bool;
auto completion_resources(Task*) -> std::optional<ker::net::wki::TaskResourceSnapshot>;
''')
    (scratch / 'resource_pin_ports.inc').write_text(ports)
    for relative in ('platform/sched/task.hpp', 'platform/sys/spinlock.hpp'):
        target = scratch / 'shim' / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text('#pragma once\n#include "resource_pin_ports.inc"\n')
    bridges = ''.join(selected('resource', anchor) for anchor in (
        'LifetimeOwner::~LifetimeOwner()', 'void LifetimeOwner::reset()', 'auto LifetimeOwner::attach(',
        'void LifetimeOwner::finish_cpu()', 'Owner::Owner(', 'Owner::~Owner()',
        'auto bind_thread_owner(', 'auto inherit(', 'auto close_thread_cpu_admission(',
        'auto close_idle_thread_cpu_admission(', 'ExecAdmission::~ExecAdmission()',
        'auto ExecAdmission::begin(', 'void ExecAdmission::finish()', 'auto limit_for_subject('))
    procfs = selected('procfs', 'auto authorize_procfs_limits(') + selected('procfs', 'auto procfs_read_limits(')
    submit = selected('wki', 'auto snapshot_task_resources(')
    completion = selected('wki', 'if (rt.resource_contract && task != nullptr) {')
    fragments = dict(bridges=bridges, procfs=procfs, submit=submit, completion=completion, core=CORE.read_text())
    (scratch / 'resource_pin_cases.cpp').write_bytes(CASES.read_bytes())
    return fragments, pieces, list(sources.values())


def bridge_source(fragments):
    return '''#include "resource_pin_ports.inc"
#include <platform/sched/resource.hpp>
#include <net/wki/resource_contract_model.hpp>
namespace ker::mod::sched::resource {
''' + fragments['bridges'] + '''\n}
namespace pin_host {
namespace resource_contract = ker::net::wki::resource_contract;
namespace child_events = ker::syscall::process::child_events;
using TaskResourceSnapshot = ker::net::wki::TaskResourceSnapshot;
using ker::net::wki::WKI_RESOURCE_CHILD_OBSERVATIONS;
using ker::net::wki::WKI_RESOURCE_STDIO_SHIFT;
''' + fragments['procfs'] + fragments['submit'] + '''
auto completion_resources(Task* task) -> std::optional<TaskResourceSnapshot> {
    struct { bool resource_contract = true; } rt;
    std::optional<TaskResourceSnapshot> resources;
''' + fragments['completion'] + '''
    return resources;
}
}
'''


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cxx', default='/usr/bin/clang++')
    parser.add_argument('--host-build', type=Path, default=ROOT / 'build_tests')
    parser.add_argument('--evidence-dir', type=Path)
    args = parser.parse_args()
    deadline = time.monotonic() + 300
    scratch, watched, before, groups = None, [], [], []
    result = dict(pass_=False, commands=[], runs=[], controls=[], sources_held=False, compiled_inputs_held=False,
                  scratch_removed=False, actual_runtime=True, actual_owner=True, no_charge_factory=True,
                  quota_only=True, page_dma_or_native_qualification=False)
    try:
        driver = Path(args.cxx).absolute()
        compiler = driver.resolve(strict=True)
        if not compiler.is_file() or not os.access(compiler, os.X_OK):
            raise ValueError('--cxx must name an executable compiler')
        include, cmake_pins, _ = gtest_context(args.host_build)
        libs = [args.host_build / 'lib/libgtest_main.a', args.host_build / 'lib/libgtest.a']
        source_paths = [KERN / path for path in ('platform/sched/resource_pin.hpp', 'platform/sched/resource_pin.cpp',
                        'platform/sched/resource.cpp', 'platform/sched/task.hpp', 'vfs/fs/procfs.cpp', 'net/wki/remote_compute.cpp')]
        baseline_models = [UNIT / 'resource_model_test.cpp', UNIT / 'wki_resource_contract_test.cpp']
        watched = sorted(set([Path(__file__).resolve(), PORTS, CASES, UNIT / 'cpu_maintenance_runtime_test.py',
                              UNIT / 'file_mmap_registry_runtime_test.py', ROOT / 'tests/CMakeLists.txt',
                              ROOT / 'tests/host/CMakeLists.txt', compiler, *source_paths, *baseline_models, *libs,
                              *(Path(row['path']) for row in cmake_pins)]))
        before = [pin(path) for path in watched]
        subreaper()
        scratch = Path(tempfile.mkdtemp(prefix='wos-pin-account-', dir='/tmp'))
        fragments, pieces, _ = generate(scratch)
        result['actual_extractions'] = pieces
        variants = [('pristine', '*', 0, fragments)]
        for name, (part, anchor, replacement, test) in CONTROLS.items():
            changed = dict(fragments)
            changed[part] = replace_once(changed[part], anchor, replacement)
            variants.append((name, test, 1, changed))
        finite = dict(fragments)
        finite['core'] = replace_once(finite['core'], 'static constexpr uint32_t ACCOUNT_CAPACITY = 4096;',
                                      'static constexpr uint32_t ACCOUNT_CAPACITY = 4;')
        finite['core'] = replace_once(finite['core'], 'static constexpr uint64_t GENERATION_LIMIT = UINT64_MAX;',
                                      'static constexpr uint64_t GENERATION_LIMIT = 3;')
        variants.append(('finite-generation', 'PinAccounting.FiniteGenerationExhaustionNeverRecyclesOrWraps', 0, finite))
        flags = [str(driver), '-std=c++23', '-O1', '-g0', '-pthread', '-Wall', '-Wextra', '-Werror', '-UNDEBUG',
                 '-Wno-self-move', '-Wno-packed-non-pod', '-fsanitize=address,undefined',
                 '-fno-sanitize-recover=all', '-fno-omit-frame-pointer',
                 '-I', str(scratch), '-I', str(scratch / 'shim'), '-I', str(KERN),
                 '-I', str(KERN / 'platform/sched'), '-isystem', str(include)]
        units, plans, common_units = {}, [], {}
        for name, test, expected, parts in variants:
            sources = []
            for kind, content in (('core', parts['core']), ('bridges', bridge_source(parts)),
                                  ('cases', CASES.read_text())):
                finite_case = name == 'finite-generation' and kind == 'cases'
                identity = (kind, content, finite_case)
                if identity not in common_units:
                    path = scratch / f'{len(units)}-{kind}.cpp'
                    path.write_text(content)
                    extra = ['-DWOS_PIN_FINITE_GENERATION=3'] if finite_case else []
                    units[path] = extra
                    common_units[identity] = path
                sources.append(common_units[identity])
            plans.append((name, test, expected, sources))
        for source in baseline_models:
            units[source] = []
        plans.append(('existing-models', '*', 0, baseline_models))
        dependencies = {}
        for index, (source, extra) in enumerate(units.items()):
            dep = scratch / f'{index}-discover.d'
            row = command(flags + extra + ['-M', '-MF', str(dep), '-MT', str(index), str(source)],
                          scratch / f'{index}-discover.log', scratch, deadline, groups)
            result['commands'].append(row)
            if row['exit'] != 0:
                raise ValueError(f'actual dependency discovery failed: {source.name}')
            for item in dependency_pins(dep):
                dependencies[item['path']] = Path(item['path'])
        if [pin(path) for path in watched] != before:
            raise ValueError('watched source changed during preparation/discovery')
        watched = sorted(set(watched) | set(dependencies.values()))
        before = [pin(path) for path in watched]
        result['compiled_input_pins'] = before
        result['persistent_input_pins'] = [row for row in before if not Path(row['path']).is_relative_to(scratch)]
        result['generated_input_pins'] = [row for row in before if Path(row['path']).is_relative_to(scratch)]
        objects = {}
        for index, (source, extra) in enumerate(units.items()):
            obj, dep = scratch / f'{index}.o', scratch / f'{index}-actual.d'
            row = command(flags + extra + ['-MD', '-MF', str(dep), '-c', str(source), '-o', str(obj)],
                          scratch / f'{index}-compile.log', scratch, deadline, groups)
            result['commands'].append(row)
            if row['exit'] != 0:
                raise ValueError(f'actual source compile failed: {source.name}')
            if any(item['path'] not in dependencies for item in dependency_pins(dep)):
                raise ValueError(f'undeclared compiled input: {source.name}')
            objects[source] = obj
        for name, test, expected, sources in plans:
            binary = scratch / f'{name}.bin'
            row = command([str(driver), '-pthread', '-fsanitize=address,undefined', '-fno-sanitize-recover=all',
                           *map(str, (objects[source] for source in sources)), *map(str, libs), '-o', str(binary)],
                          scratch / f'{name}-link.log', scratch, deadline, groups)
            result['commands'].append(row)
            if row['exit'] != 0:
                raise ValueError(f'actual source link failed: {name}')
            binary_pin = pin(binary)
            log = scratch / f'{name}-run.log'
            row = command([str(binary), '--gtest_color=no', '--gtest_filter=' + test], log, scratch, deadline, groups)
            result['commands'].append(row)
            output = log.read_text(errors='replace')
            if re.search(r'AddressSanitizer|runtime error:|UndefinedBehaviorSanitizer', output):
                raise ValueError(f'sanitizer failure does not qualify the oracle: {name}')
            passed = re.search(r'\[  PASSED  \] (\d+) tests?\.', output)
            if expected == 0:
                if row['exit'] != 0 or not passed:
                    raise ValueError(f'actual positive GTests failed: {name}')
                result[{'pristine': 'positive_tests', 'finite-generation': 'finite_tests',
                        'existing-models': 'baseline_model_tests'}[name]] = int(passed[1])
            else:
                if row['exit'] != 1 or '[  FAILED  ] ' + test not in output:
                    raise ValueError(f'compiled control did not fail its exact connected assertion: {name}')
                result['controls'].append(dict(name=name, test=test, detected=True, compiled=True))
            result['runs'].append(dict(name=name, test_filter=test, exit=row['exit'], binary=binary_pin, log=pin(log)))
            binary.unlink()
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
                    result['retained_logs'] = [pin(path) for path in sorted(args.evidence_dir.glob('*.log'))]
            except (OSError, ValueError) as error:
                result['pass_'] = False
                result['evidence_error'] = str(error)
        if scratch is not None and not groups:
            shutil.rmtree(scratch)
            result['scratch_removed'] = not scratch.exists()
        if groups:
            result['pass_'] = False
            result['cleanup_error'] = f'owned command sessions remain: {groups}'
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
        print(json.dumps({key: value for key, value in result.items() if key in (
            'pass_', 'error', 'positive_tests', 'finite_tests', 'baseline_model_tests', 'controls', 'sources_held',
            'compiled_inputs_held', 'scratch_removed', 'cleanup_error', 'evidence_error')}))
    return 0 if result['pass_'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
