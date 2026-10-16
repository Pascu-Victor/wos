#!/usr/bin/env python3
"""Execute actual private swap/facade boundaries; no scheduled Task substitute."""
import argparse
import hashlib
import importlib.util
import json
from pathlib import Path
import shlex
import shutil
import signal
import sys
import tempfile
import time
import traceback

sys.dont_write_bytecode = True


def definition(text, anchor):
    start = text.index(anchor)
    start = text.rfind('\n', 0, start) + 1
    end = text.index('{', start) + 1
    depth = 1
    while depth:
        depth += (text[end] == '{') - (text[end] == '}')
        end += 1
    return text[start:end]


def pin(path):
    path = Path(path).resolve(strict=True)
    first = path.stat()
    data = path.read_bytes()
    last = path.stat()
    identity = lambda st: (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns)
    if identity(first) != identity(last):
        raise RuntimeError('input changed ' + str(path))
    return dict(path=str(path), bytes=len(data), sha256=hashlib.sha256(data).hexdigest())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root', type=Path)
    parser.add_argument('--kernel-overlay', type=Path)
    parser.add_argument('--host-build', type=Path, required=True)
    parser.add_argument('--evidence-dir', type=Path)
    args = parser.parse_args()
    root = (args.source_root or next(p for p in Path(__file__).resolve().parents if (p / '.github/instructions').is_dir())).resolve()
    kernel = root / 'modules/kern/src'
    directory = Path(__file__).resolve().parent

    def source(relative):
        overlay = args.kernel_overlay / relative if args.kernel_overlay else None
        return overlay.resolve() if overlay is not None and overlay.is_file() else (kernel / relative).resolve()

    helper = root / 'tests/host/unit/file_mmap_registry_runtime_test.py'
    spec = importlib.util.spec_from_file_location('owned', helper)
    owned = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(owned)
    owned.FILE_BUDGET, owned.SCRATCH_BUDGET, owned.LOG_BUDGET = 32 * 1024**2, 128 * 1024**2, 2 * 1024**2
    ephemeral = args.evidence_dir is None
    out = Path(tempfile.mkdtemp(prefix='wos-scheduled-pagein-evidence-')) if ephemeral else args.evidence_dir.resolve()
    if not ephemeral:
        out.mkdir(parents=True, exist_ok=False)
    scratch = Path(tempfile.mkdtemp(prefix='scratch-', dir=out))
    start = time.monotonic()
    groups, commands, errors, before, after = [], [], [], [], []
    active, pending = False, None
    handlers = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}

    def interrupted(number, frame):
        nonlocal pending
        if active:
            pending = number
            return
        raise KeyboardInterrupt('signal ' + str(number))

    def command(label, argv, expected=0):
        nonlocal active, pending
        active = True
        try:
            row = owned.command(list(map(str, argv)), out / (label + '.log'), scratch, start + 120, groups)
        finally:
            active = False
            if pending is not None:
                number, pending = pending, None
                raise KeyboardInterrupt('deferred signal ' + str(number))
        commands.append(dict(row, label=label))
        if (row['exit'] == 0) != (expected == 0):
            raise RuntimeError(label + ': ' + (out / (label + '.log')).read_text(errors='replace')[-6500:])
        if owned.allocated(out) > 128 * 1024**2:
            raise RuntimeError('128MiB total evidence/scratch cap')

    removed, preserved = False, None
    try:
        for number in handlers:
            signal.signal(number, interrupted)
        owned.subreaper()
        swap_path, runtime_path = source('platform/mm/swap.cpp'), source('test/anonymous_usercopy_runtime.cpp')
        mutex_path = source('platform/sys/mutex.cpp')
        user_path = source('platform/sys/usercopy.cpp')
        swap, runtime, mutex = swap_path.read_text(), runtime_path.read_text(), mutex_path.read_text()
        template = directory / 'anonymous_scheduled_usercopy_cases.inc'
        text = template.read_text()
        names = ['copy_name', 'destroy_area', 'block_device_usable_for_swap', 'slot_to_extent_locked', 'slot_metadata_locked',
                 'find_area_locked', 'find_area_by_name_locked', 'activate_extents_locked', 'activate_extents',
                 'selftest_try_detach_empty_area', 'allocate_slot', 'free_slot', 'submit_slot_io', 'finish_slot_io', 'write_slot', 'read_slot', 'get_stats']
        def actual_swap(name):
            prefix = 'void ' if name in ['destroy_area', 'get_stats'] else 'auto '
            return definition(swap, prefix + name + '(')
        replacements = dict(ACTUAL_MUTEX=definition(mutex, 'auto Mutex::try_lock(') + '\n' + definition(mutex, 'void Mutex::unlock('),
                            ACTUAL_SWAP_TYPES=swap[swap.index('constexpr size_t MAX_SWAP_NAME'):swap.index('auto copy_name(')],
                            ACTUAL_SWAP_FUNCTIONS='\n\n'.join(actual_swap(name) for name in names),
                            ACTUAL_RANGE_VALID=definition(user_path.read_text(), 'auto range_valid('),
                            ACTUAL_REQUEST_HELPERS='\n'.join(definition(runtime, 'auto ' + name + '(') for name in ['overlaps', 'request_valid']),
                            ACTUAL_DEVICE_HISTORY=definition(runtime, 'struct DeviceHistory') + ';',
                            ACTUAL_SERIALIZED_FACADE=definition(runtime, 'class SerializedFaultStorage') + ';')
        for token, body in replacements.items():
            if text.count(token) != 1:
                raise RuntimeError('extraction token ' + token)
            text = text.replace(token, body)
        shim = scratch / 'shim'
        for relative, body in [('util/hcf.hpp', '#pragma once\n#include <cstdlib>\n[[noreturn]] inline void hcf() noexcept {std::abort();}\n'),
                               ('platform/mm/dyn/kmalloc.hpp', '#pragma once\n')]:
            target = shim / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(body)
        database = (args.host_build / 'compile_commands.json').resolve(strict=True)
        row = next(row for row in json.loads(database.read_text()) if row['file'].endswith('/page_allocator_interval_test.cpp'))
        compiler = shlex.split(row['command'])[0]
        base = [compiler, '-std=gnu++23', '-O1', '-g', '-Wall', '-Wextra', '-DWOS_SELFTEST=1', '-DWOS_HOST_TEST=1',
                '-fno-omit-frame-pointer', '-fsanitize=address,undefined', '-fno-sanitize=vptr', '-I' + str(shim)]
        if args.kernel_overlay:
            base.append('-I' + str(args.kernel_overlay.resolve()))
        base.append('-I' + str(kernel))
        normal = scratch / 'actual.cpp'
        normal.write_text(text)
        (out / 'actual.cpp').write_text(text)
        crc = source('util/crc32c.cpp')
        dep = scratch / 'actual.d'
        command('dependencies', base + ['-M', '-MF', dep, '-MT', 'out', normal])
        deps = {Path(p).resolve() for p in shlex.split(dep.read_text().replace('\\\n', ' ').split(':', 1)[1])
                if not str(Path(p).resolve()).startswith(str(scratch))}
        deps.update([swap_path, runtime_path, mutex_path, user_path, template, Path(__file__), helper, database, crc,
                     source('platform/mm/virt.hpp'), source('platform/mm/virt.opt.cpp'),
                     source('platform/init/init_wrappers.cpp'), source('syscalls_impl/vmem/sys_vmem.cpp')])
        before = [pin(p) for p in sorted(deps)]
        command('compile-actual', base + [normal, crc, '-o', scratch / 'actual'])
        command('execute-actual', [scratch / 'actual'])
        if 'PASS actual scheduled diagnostic storage cases=9' not in (out / 'execute-actual.log').read_text():
            raise RuntimeError('missing actual-vector completion')
        mutations = [
            ('lost-used-guard', 'area->state != AreaState::ACTIVE || area->used_pages != 0 || area->io_in_flight != 0',
             'area->state != AreaState::ACTIVE || area->io_in_flight != 0', 'occupied detach leaves exact descriptor'),
            ('lost-slot-io-guard', 'if (area->slots[i].io_in_flight) {', 'if (false) {', 'every slot IO flag checked'),
            ('lost-overlap-sticky', 'overlap_rejected_.store(true, std::memory_order_release);', '(void)0;',
             'missing history event still records sticky overlap refusal'),
            ('lost-flush-gate', 'if (!self->enter()) return -EBUSY;\n        auto* inner = self->inner_.device();\n        int const RESULT = inner->flush(inner);',
             'auto* inner = self->inner_.device();\n        int const RESULT = inner->flush(inner);', 'all callbacks use one nonwaiting gate'),
            ('premature-history', 'if (active_ || !detached_ || !provider_is_private() || !gate_.try_lock()) return false;',
             'if (!provider_is_private() || !gate_.try_lock()) return false;', 'history refused before actual detach'),
        ]
        for label, old, new, expected in mutations:
            if text.count(old) != 1:
                raise RuntimeError('mutation token ' + label)
            mutant = scratch / (label + '.cpp')
            mutant.write_text(text.replace(old, new))
            command('compile-' + label, base + [mutant, crc, '-o', scratch / label])
            command('execute-' + label, [scratch / label], 1)
            log = (out / ('execute-' + label + '.log')).read_text()
            if 'FAIL ' + expected not in log or 'AddressSanitizer' in log or 'runtime error:' in log:
                raise RuntimeError('mutant refused for unexpected reason: ' + label)
        after = [pin(p) for p in sorted(deps)]
        if before != after:
            raise RuntimeError('input drift')
    except BaseException as error:
        errors.append(type(error).__name__ + ': ' + str(error))
        (out / 'failure.txt').write_text(traceback.format_exc())
    finally:
        for number in handlers:
            signal.signal(number, signal.SIG_IGN)
        try:
            try:
                if before and not after:
                    after = [pin(p['path']) for p in before]
                if before and before != after:
                    errors.append('source drift at cleanup')
            except BaseException as error:
                errors.append('source recheck: ' + str(error))
            if groups:
                errors.append('unproved owned process groups')
                preserved = str(scratch)
            else:
                try:
                    shutil.rmtree(scratch)
                    removed = not scratch.exists()
                except BaseException as error:
                    errors.append('scratch cleanup: ' + str(error))
                    preserved = str(scratch)
            result = dict(result='PASS_ACTUAL_DIAGNOSTIC_STORAGE_HOST' if not errors and removed else 'FAIL_DIAGNOSTIC_STORAGE_HOST',
                          commands=commands, errors=errors, input_before=before, input_after=after,
                          source_held=bool(before) and before == after, scratch_removed=removed, preserved_scratch=preserved,
                          owned_groups=groups, seconds=time.monotonic() - start,
                          scope='Actual swap/facade/FaultBlockDevice/Mutex CAS/CRC bodies; explicit block, queue and owner endpoints. No current Task/CPL3/I-O scheduler or SMP qualification.')
            (out / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
            print(json.dumps({k: result[k] for k in ['result', 'errors', 'seconds', 'scratch_removed']}))
        finally:
            for number, handler in handlers.items():
                signal.signal(number, handler)
        if ephemeral and removed and not groups:
            shutil.rmtree(out)
    return int(bool(errors) or not removed or bool(groups))


if __name__ == '__main__':
    raise SystemExit(main())
