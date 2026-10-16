#!/usr/bin/env python3
"""Actual MM activity/core and resident lifetime bodies on bounded Host primitives.

Reuses both maintained legacy generators unchanged. Scheduler/physical/PTE
endpoints remain controlled; no kernel runtime, attribution or acceptance credit.
The one Host Epoch::end callback checks the actual post-end boundary.
"""
from __future__ import annotations
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import signal
import tempfile
import time
ROOT = Path(__file__).resolve().parents[3]

def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=ROOT)
    parser.add_argument('--source-root', type=Path)
    parser.add_argument('--evidence-dir', type=Path)
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    source_root = (args.source_root or root).resolve(strict=True)
    if args.evidence_dir is not None:
        args.evidence_dir.mkdir(mode=448)
    process = load(root / 'tests/host/unit/file_mmap_registry_runtime_test.py', 'bounded_owned')
    core = load(root / 'tests/host/unit/numa_mm_debt_core_runtime_test.py', 'old_core')
    pin = load(root / 'tests/host/unit/numa_mm_debt_pin_runtime_test.py', 'old_pin')

    def selected(relative):
        path = source_root / relative
        return path if path.is_file() else root / relative
    virt = selected('modules/kern/src/platform/mm/virt.opt.cpp')
    header = selected('modules/kern/src/test/numa_lifecycle_mm_debt.hpp')
    paths = [Path(__file__), Path(__file__).with_name('numa_mm_activity_core.inc'), Path(__file__).with_name('numa_mm_activity_resident.inc'), virt, header, root / 'modules/kern/src/platform/mm/lifecycle_census.hpp', root / 'tests/host/unit/numa_mm_debt_core_runtime_test.py', root / 'tests/host/unit/numa_mm_debt_pin_runtime_test.py', root / 'tests/host/unit/file_mmap_registry_runtime_test.py', pin.TASK, pin.COPY, pin.VH, pin.CH]
    before = [process.pin(x) for x in paths]
    result = {'pass': False, 'errors': [], 'commands': [], 'runtime_qualified': False, 'source_held': False, 'scratch_removed': False, 'preserved_scratch': None, 'test_seams': ['real unchanged legacy generators', 'controlled physical/PTE/Task primitives', 'actual Epoch end callback only in Host overlay']}
    groups = []
    scratch = None
    dependencies = []
    started = time.monotonic()
    deadline = started + 120
    handlers = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}

    def interrupted(number, _):
        raise KeyboardInterrupt(str(number))
    try:
        for s in handlers:
            signal.signal(s, interrupted)
        process.subreaper()
        scratch = Path(tempfile.mkdtemp(prefix='wos-mm-activity-', dir='/tmp'))
        include = scratch / 'include/platform/mm'
        include.mkdir(parents=True)
        epoch = (root / 'modules/kern/src/platform/mm/lifecycle_census.hpp').read_text()
        old = 'if (inflight_.compare_exchange_weak(value, value - 1, std::memory_order_seq_cst)) return;'
        new = 'if (inflight_.compare_exchange_weak(value, value - 1, std::memory_order_seq_cst)) { ::mm_activity_epoch_ended(); return; }'
        assert epoch.count(old) == 1
        epoch = epoch.replace('namespace ker::mod::mm::lifecycle_census {', 'void mm_activity_epoch_ended() noexcept;\nnamespace ker::mod::mm::lifecycle_census {', 1).replace(old, new, 1)
        (include / 'lifecycle_census.hpp').write_text(epoch)
        core.SOURCE = virt
        scalar = core.program()
        resident = pin.program(virt.read_text(), pin.TASK.read_text(), pin.COPY.read_text(), pin.VH.read_text(), pin.CH.read_text())
        for kind, body, extra, call in [('scalar', scalar, 'numa_mm_activity_core.inc', 'activity_fixture::scalar_tests();'), ('resident', resident, 'numa_mm_activity_resident.inc', 'activity_fixture::resident_test();')]:
            start = body.index('int main(){')
            body = body[:start] + Path(__file__).with_name(extra).read_text() + '\n' + body[start:]
            pos = body.rindex('std::printf(')
            body = body[:pos] + call + body[pos:]
            variants = {'actual': body}
            if kind == 'scalar':
                line = '    increment_activity(increase ? activity.add_attempts[INDEX] : activity.drop_attempts[INDEX]);'
                variants['missing-drop-attempt'] = body.replace(line, '    if(increase) increment_activity(activity.add_attempts[INDEX]);', 1)
                adjust = core.extract(body, 'void adjust(')
                moved = adjust.replace(line + '\n', '').replace('    state.epoch.end();\n}', '    state.epoch.end();\n' + line + '\n}')
                variants['attempt-after-end'] = body.replace(adjust, moved, 1)
                variants['ignore-matching-anchor'] = body.replace('sample.after.version == mm.version_end', 'true', 1)
                variants['wrap-diagnostic-counter'] = body.replace('if (value + 1 == UINT64_MAX) activity.invalid.store(true, std::memory_order_seq_cst);', '/* omitted reaching-sentinel invalidation */', 1)
            else:
                variants['missing-return-tail'] = body.replace('debt::drop(DebtField::PIN_RETURNS);', '/* omitted actual return-tail drop */', 1)
            for name, content in variants.items():
                assert name == 'actual' or content != body
                source = scratch / (kind + '-' + name + '.cpp')
                source.write_text(content)
                binary = source.with_suffix('')
                argv = [os.environ.get('WOS_HOST_CXX', '/usr/bin/clang++'), '-std=c++23', '-DWOS_SELFTEST=1', '-DWOS_HOST_TEST=1', '-O1', '-g0', '-fno-pie', '-no-pie', '-fsanitize=address,undefined', '-fno-omit-frame-pointer', '-pthread', '-I' + str(scratch / 'include'), '-I' + str(root / 'tests/host/kern_shim'), '-I' + str(source_root / 'modules/kern/src'), '-I' + str(root / 'modules/kern/src'), '-I' + str(root / 'modules'), str(source), '-o', str(binary)]
                if name == 'actual':
                    dep = scratch / (kind + '.d')
                    discovery = argv[:-2] + ['-M', '-MF', str(dep), '-MT', 'private']
                    log = scratch / (kind + '-discovery.log')
                    mask = signal.pthread_sigmask(signal.SIG_BLOCK, set(handlers))
                    try:
                        row = process.command(discovery, log, scratch, deadline, groups)
                    finally:
                        signal.pthread_sigmask(signal.SIG_SETMASK, mask)
                    row.update(kind=kind, phase='discovery')
                    result['commands'].append(row)
                    if args.evidence_dir is not None:
                        shutil.copyfile(log, args.evidence_dir / log.name)
                    if row['exit'] != 0:
                        raise ValueError('actual compiler dependency discovery failed')
                    dependencies.extend(process.dependency_pins(dep))
                for phase, command in [('compile', argv), ('run', [str(binary)])]:
                    log = scratch / (kind + '-' + name + '-' + phase + '.log')
                    mask = signal.pthread_sigmask(signal.SIG_BLOCK, set(handlers))
                    try:
                        row = process.command(command, log, scratch, deadline, groups)
                    finally:
                        signal.pthread_sigmask(signal.SIG_SETMASK, mask)
                    row.update(kind=kind, variant=name, phase=phase)
                    result['commands'].append(row)
                    if args.evidence_dir is not None:
                        shutil.copyfile(log, args.evidence_dir / log.name)
                    print(log.read_text()[-4096:], end='', flush=True)
                    expected = 0 if phase == 'compile' or name == 'actual' else -signal.SIGABRT
                    if row['exit'] != expected:
                        raise ValueError(f"{kind}/{name}/{phase}: expected{expected}, got{row['exit']}")
                if args.evidence_dir is not None:
                    shutil.copyfile(source, args.evidence_dir / source.name)
        result['pass'] = True
    except BaseException as error:
        result['errors'].append(f'{type(error).__name__}: {error}')
    finally:
        for s in handlers:
            signal.signal(s, signal.SIG_IGN)
        try:
            try:
                result['dependencies_before'] = dependencies
                result['dependencies_after'] = [process.pin(Path(x['path'])) for x in dependencies]
                result['emitted_inputs_held'] = dependencies == result['dependencies_after']
                if not result['emitted_inputs_held']:
                    result['errors'].append('compiler emitted inputs changed')
                result['inputs_before'] = before
                result['inputs_after'] = [process.pin(x) for x in paths]
                result['source_held'] = before == result['inputs_after']
                if not result['source_held']:
                    result['errors'].append('source changed')
            except BaseException as error:
                result['errors'].append('input recheck: ' + repr(error))
            result['owned_groups_pending'] = groups[:]
            if scratch is not None:
                if groups:
                    result['preserved_scratch'] = str(scratch)
                    result['errors'].append('unproven live group; scratch retained')
                else:
                    try:
                        shutil.rmtree(scratch)
                        result['scratch_removed'] = not scratch.exists()
                    except BaseException as error:
                        result['errors'].append('scratch cleanup: ' + repr(error))
                        result['preserved_scratch'] = str(scratch)
            result['seconds'] = time.monotonic() - started
            result['pass'] = result['pass'] and (not result['errors']) and result['scratch_removed']
            if args.evidence_dir is not None:
                (args.evidence_dir / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
            print(json.dumps({key: result.get(key) for key in ['pass', 'errors', 'seconds', 'scratch_removed', 'preserved_scratch', 'source_held', 'emitted_inputs_held']}))
        finally:
            for s, h in handlers.items():
                signal.signal(s, h)
    return 0 if result['pass'] else 1
if __name__ == '__main__':
    raise SystemExit(main())
