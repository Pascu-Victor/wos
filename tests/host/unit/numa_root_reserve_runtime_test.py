#!/usr/bin/env python3
"""Compile actual root reserve/factory/buddy with disclosed Host primitives."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import resource
import shlex
import shutil
import signal
import subprocess
import tempfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cxx', default=os.environ.get('CXX', 'clang++'))
    parser.add_argument('--source-root', type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument('--source-overlay', type=Path, help='Test-only finite relative-path -> source-file JSON map')
    parser.add_argument('--result', type=Path, help='Optional new saved result; binaries/depfiles are always temporary')
    args = parser.parse_args()
    root = args.source_root.resolve()
    here = Path(__file__).resolve().parent
    kernel = root / 'modules/kern/src'
    cxx = Path(shutil.which(args.cxx) or args.cxx).resolve()
    if not cxx.is_file(): parser.error('compiler is not an existing file')
    if args.result and args.result.exists(): parser.error('refuse existing result')
    allowed = {'modules/kern/src/' + p for p in [
        'platform/mm/page_alloc.cpp', 'platform/mm/page_alloc.hpp',
        'platform/mm/numa_alloc.cpp', 'platform/mm/numa_alloc.hpp',
        'platform/mm/numa_root_reserve.cpp', 'platform/mm/numa_root_reserve.hpp',
        'platform/mm/phys.opt.cpp', 'platform/mm/phys.hpp',
        'platform/sched/scheduler.hpp', 'platform/asm/cpu.hpp']}
    replacements = {}
    if args.source_overlay:
        replacements = json.loads(args.source_overlay.read_text())
        if not isinstance(replacements, dict) or not set(replacements) <= allowed:
            parser.error('overlay contains an unapproved source path')
        for key, value in replacements.items():
            if not isinstance(value, str) or not Path(value).is_absolute() or not Path(value).is_file():
                parser.error('overlay source must be an absolute existing file')
            if Path(key).is_absolute() or '..' in Path(key).parts: parser.error('escaping overlay key')

    def source(relative):
        p = Path(replacements.get(relative, str(root / relative))).resolve()
        if not p.is_file(): raise RuntimeError('missing actual source: ' + relative)
        return p

    def pin(p):
        p = Path(p).resolve(); a = p.stat(); data = p.read_bytes(); b = p.stat()
        if (a.st_ino, a.st_size, a.st_mtime_ns, a.st_ctime_ns) != (b.st_ino, b.st_size, b.st_mtime_ns, b.st_ctime_ns):
            raise RuntimeError('source changed during pin')
        return dict(path=str(p), bytes=len(data), sha256=hashlib.sha256(data).hexdigest(),
                    device=b.st_dev, inode=b.st_ino, mtime_ns=b.st_mtime_ns, ctime_ns=b.st_ctime_ns)

    def body(text, marker, trailer='}'):
        start = text.index(marker)
        first = text.index('{', start)
        depth = 0
        quote, line, block = None, False, False
        i = first
        while i < len(text):
            char, following = text[i], text[i:i + 2]
            if line:
                if char == '\n': line = False
            elif block:
                if following == '*/': block = False; i += 1
            elif quote:
                if char == '\\': i += 1
                elif char == quote: quote = None
            elif following == '//': line = True; i += 1
            elif following == '/*': block = True; i += 1
            elif char in ('"', "'"): quote = char
            elif char == '{': depth += 1
            elif char == '}':
                depth -= 1
                if depth == 0:
                    end = i + len(trailer)
                    if text[i:end] != trailer: raise RuntimeError('helper terminator changed')
                    return text[start:end]
            i += 1
        raise RuntimeError('unterminated actual helper')

    started = time.monotonic()
    commands, errors, emitted_before, emitted_after = [], [], [], []
    selected_before, selected_after, owned = [], [], []
    removed = False
    scratch_bytes = 0
    free_before = shutil.disk_usage(root).free
    result = {}
    try:
        with tempfile.TemporaryDirectory(prefix='wos-root-reserve-host-') as directory:
            scratch = Path(directory)
            env = dict(os.environ, TMPDIR=directory, TMP=directory, TEMP=directory,
                       ASAN_OPTIONS='detect_leaks=0:halt_on_error=1', UBSAN_OPTIONS='halt_on_error=1:print_stacktrace=1')

            def limits():
                resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
                resource.setrlimit(resource.RLIMIT_FSIZE, (16 * 1024**2, 16 * 1024**2))

            def group_absent(pid):
                try: os.killpg(pid, 0); return False
                except ProcessLookupError: return True

            def run(label, argv):
                remaining = 110 - (time.monotonic() - started)
                used = sum(p.stat().st_blocks * 512 for p in scratch.rglob('*') if p.is_file())
                if remaining <= 0 or used > 128 * 1024**2 or shutil.disk_usage(root).free < 40 * 1024**3:
                    raise RuntimeError('time/storage budget')
                log = scratch / (label + '.log')
                begin = time.monotonic()
                with log.open('wb') as stream:
                    proc = subprocess.Popen([str(x) for x in argv], cwd=root, env=env, stdout=stream, stderr=subprocess.STDOUT,
                                            start_new_session=True, preexec_fn=limits)
                    owned.append(proc.pid)
                    failure = None
                    try: code = proc.wait(timeout=min(35, remaining))
                    except BaseException as error:
                        os.killpg(proc.pid, signal.SIGKILL); code = proc.wait()
                        failure = type(error).__name__ + ': ' + str(error)
                absent = group_absent(proc.pid)
                if not absent:
                    os.killpg(proc.pid, signal.SIGKILL)
                data = log.read_bytes()
                commands.append(dict(label=label, argv=[str(x) for x in argv], exit=code,
                                     seconds=time.monotonic() - begin, leader_reaped=True,
                                     owned_group_absent=group_absent(proc.pid), output_bytes=len(data),
                                     output_sha256=hashlib.sha256(data).hexdigest(), output=data[-12000:].decode(errors='replace'),
                                     interrupted_or_timed_out=failure))
                if failure or code != 0 or not absent or len(data) > 2 * 1024**2:
                    raise RuntimeError(label + ' failed exit=' + str(code) + (' ' + failure if failure else ''))

            try:
                actual_module = source('modules/kern/src/platform/mm/numa_root_reserve.cpp')
                physical = source('modules/kern/src/platform/mm/phys.opt.cpp')
                scheduler = source('modules/kern/src/platform/sched/scheduler.hpp')
                cpu = source('modules/kern/src/platform/asm/cpu.hpp')
                selected = [Path(__file__), here / 'numa_root_reserve_test.cpp', actual_module, physical,
                            scheduler, cpu, cxx, *[source(p) for p in sorted(allowed) if p not in
                            {'modules/kern/src/platform/sched/scheduler.hpp', 'modules/kern/src/platform/asm/cpu.hpp'}]]
                selected += [p for p in (here / 'numa_root_reserve_shim').rglob('*') if p.is_file()]
                if args.source_overlay: selected.append(args.source_overlay.resolve())
                selected = list(dict.fromkeys(selected))
                selected_before = [pin(p) for p in selected]
                overlay = scratch / 'source-overlay.json'
                overlay.write_text(json.dumps({'version': 0, 'roots': [dict(type='file', name=str(root / k),
                                      **{'external-contents': v}) for k, v in replacements.items()]}))
                # Only the declaration primitives are Host seams. Use the exact
                # current guard body, including default/O0 construction.
                guard = body(scheduler.read_text(), 'struct PreemptGuard {', '};')
                if 'auto current_cpu() -> uint64_t;' not in cpu.read_text(): raise RuntimeError('CPU declaration changed')
                generated_shim = scratch / 'shim'
                sched_header = generated_shim / 'platform/sched/scheduler.hpp'
                sched_header.parent.mkdir(parents=True)
                sched_header.write_text('#pragma once\nnamespace ker::mod::sched {\nvoid preempt_disable();\nvoid preempt_enable();\n'
                                        + guard + '\n}\n')
                cpu_header = generated_shim / 'platform/asm/cpu.hpp'
                cpu_header.parent.mkdir(parents=True)
                cpu_header.write_text('#pragma once\n#include <cstdint>\nnamespace ker::mod::cpu { auto current_cpu() -> uint64_t; }\n')
                module = scratch / 'actual_root_reserve.cpp'
                module.write_text(actual_module.read_text() + '''\n// Private same-TU accessor; no production mutation or fault API.\nvoid host_corrupt_root_reserve_member_index() {\n namespace rs = ker::mod::mm::numa::root_reserve;\n rs::reserve.nodes[0].members[0] = UINT16_MAX;\n}\n''')
                extracted = scratch / 'actual_physical_transition.cpp'
                transition = body(physical.read_text(), 'auto page_table_owner_transition_if_exclusive(')
                extracted.write_text('#include <platform/mm/phys.hpp>\nnamespace ker::mod::mm::phys {\n' + transition + '\n}\n')
                sources = [here / 'numa_root_reserve_test.cpp', source('modules/kern/src/platform/mm/page_alloc.cpp'),
                           source('modules/kern/src/platform/mm/numa_alloc.cpp'), module, extracted]
                common = [str(cxx), '-std=gnu++23', '-O1', '-fno-omit-frame-pointer', '-fsanitize=address,undefined',
                          '-fno-sanitize-recover=all', '-Wall', '-Wextra', '-Werror', '-DWOS_HOST_TEST=1',
                          '-I' + str(generated_shim), '-I' + str(here / 'numa_root_reserve_shim'),
                          '-I' + str(root / 'tests/host/kern_shim'), '-I' + str(kernel), '-I' + str(kernel / 'std'),
                          '-I' + str(kernel / 'platform/mm'), '-I' + str(root / 'modules')]
                if replacements: common[1:1] = ['-ivfsoverlay', str(overlay)]
                deps = set(selected)
                for mode, flags in [('normal', []), ('provenance', ['-DWOS_PHYS_ALLOC_CALLER_STATS=1'])]:
                    for i, src in enumerate(sources):
                        dep = scratch / f'{mode}-{i}.d'
                        run(f'deps-{mode}-{i}', common + flags + ['-M', '-MT', 'output', '-MF', dep, src])
                        deps.update(Path(p).resolve() for p in shlex.split(dep.read_text().replace('\\\n', ' ').split(':', 1)[1]))
                emitted_before = [pin(p) for p in sorted(deps)]
                for mode, flags in [('normal', []), ('provenance', ['-DWOS_PHYS_ALLOC_CALLER_STATS=1'])]:
                    objects = []
                    for i, src in enumerate(sources):
                        obj = scratch / f'{mode}-{i}.o'
                        run(f'compile-{mode}-{i}', common + flags + ['-c', src, '-o', obj])
                        objects.append(obj)
                    binary = scratch / (mode + '-test')
                    run('link-' + mode, [str(cxx), '-fsanitize=address,undefined', *objects, '-pthread', '-o', binary])
                    run('run-' + mode + '-1000cycles', [binary])
                    run('run-' + mode + '-badindex', [binary, '--corrupt-member-index'])
                emitted_after = [pin(p) for p in sorted(deps)]
                selected_after = [pin(p) for p in selected]
                if selected_before != selected_after or emitted_before != emitted_after: raise RuntimeError('source drift')
            finally:
                if selected_before and not selected_after:
                    selected_after = [pin(p['path']) for p in selected_before]
                if emitted_before and not emitted_after:
                    emitted_after = [pin(p['path']) for p in emitted_before]
                if selected_before != selected_after: errors.append('selected source drift')
                if emitted_before != emitted_after: errors.append('emitted dependency drift')
                for pid in owned:
                    if not group_absent(pid):
                        os.killpg(pid, signal.SIGKILL)
                        errors.append('remaining exact owned group ' + str(pid))
                scratch_bytes = sum(p.stat().st_blocks * 512 for p in scratch.rglob('*') if p.is_file())
        removed = not scratch.exists()
    except BaseException as error:
        errors.append(type(error).__name__ + ': ' + str(error))
        removed = 'scratch' in locals() and not scratch.exists()
    result.update(qualification='PASS_ACTUAL_ALLOCATOR_ROOT_RESERVE_HOST_ONLY' if not errors else 'FAIL_ROOT_RESERVE_HOST',
                  errors=errors, commands=commands, seconds=time.monotonic() - started,
                  selected_before=selected_before, selected_after=selected_after, emitted_before=emitted_before,
                  emitted_after=emitted_after, emitted_input_count=len(emitted_before), scratch_removed=removed,
                  scratch_allocated_bytes_removed=scratch_bytes, free_before=free_before, free_after=shutil.disk_usage(root).free,
                  limits=['Actual allocator/factory/reserve/exact physical helper with explicit IRQ/address/zone/ref/topology primitives.',
                          'No full phys TU/global counter/native root control/TLB/scheduled migration/hardware first-touch credit.',
                          'Exact current PreemptGuard body extracted; scheduler execution remains a Host primitive seam.',
                          'Private malformed-index accessor only in temporary module copy; each corruption vector gets a new process.'])
    if args.result:
        args.result.write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(dict(qualification=result['qualification'], errors=errors, commands=len(commands),
                         seconds=result['seconds'], emitted_inputs=len(emitted_before), scratch_removed=removed)))
    return bool(errors)


if __name__ == '__main__':
    raise SystemExit(main())
