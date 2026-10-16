#!/usr/bin/env python3
"""Execute actual current-copy, VFS bounce and timeout bodies against finite Host endpoints."""
from pathlib import Path
import argparse
import hashlib
import importlib.util
import json
import shlex
import shutil
import signal
import sys
import tempfile
import time
import traceback

sys.dont_write_bytecode = True
ap = argparse.ArgumentParser(description=__doc__)
ap.add_argument('--source-root', type=Path)
ap.add_argument('--kernel-overlay', type=Path)
ap.add_argument('--host-build', type=Path, required=True)
ap.add_argument('--evidence-dir', type=Path)
opt = ap.parse_args()
R = (opt.source_root or next(p for p in Path(__file__).resolve().parents if (p / '.github/instructions').is_dir())).resolve()
K = R / 'modules/kern/src'
D = Path(__file__).resolve().parent
DB = (opt.host_build / 'compile_commands.json').resolve()
def source(relative):
    overlay = opt.kernel_overlay / relative if opt.kernel_overlay else None
    return overlay.resolve() if overlay is not None and overlay.is_file() else (K / relative).resolve()
spec = importlib.util.spec_from_file_location('owned', R / 'tests/host/unit/file_mmap_registry_runtime_test.py')
owned = importlib.util.module_from_spec(spec)
spec.loader.exec_module(owned)
owned.FILE_BUDGET = 32 * 1024**2
owned.SCRATCH_BUDGET = 96 * 1024**2
ephemeral = opt.evidence_dir is None
OUT = Path(tempfile.mkdtemp(prefix='wos-current-copy-evidence-')) if ephemeral else opt.evidence_dir.resolve()
if not ephemeral:
    OUT.mkdir(parents=True, exist_ok=False)
T = Path(tempfile.mkdtemp(prefix='scratch-', dir=OUT))
start = time.monotonic()
groups, commands, errors, before, after = [], [], [], [], []
def pin(path):
    path = Path(path).resolve()
    st = path.stat()
    data = path.read_bytes()
    end = path.stat()
    assert (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns) == (end.st_dev, end.st_ino, end.st_size, end.st_mtime_ns, end.st_ctime_ns)
    return dict(path=str(path), bytes=len(data), sha256=hashlib.sha256(data).hexdigest())
def definition(text, anchor):
    begin = text.index(anchor)
    begin = text.rfind('\n', 0, begin) + 1
    end = text.index('{', begin) + 1
    depth = 1
    while depth:
        depth += (text[end] == '{') - (text[end] == '}')
        end += 1
    return text[begin:end]
def run(label, argv, expected=0):
    global owned_command_active, pending_signal
    owned_command_active = True
    try:
        row = owned.command(list(map(str, argv)), OUT / (label + '.log'), T, start + 120, groups)
    finally:
        owned_command_active = False
        if pending_signal is not None:
            number = pending_signal
            pending_signal = None
            raise KeyboardInterrupt('deferred signal ' + str(number))
    row['label'] = label
    commands.append(row)
    if (row['exit'] == 0) != (expected == 0):
        raise RuntimeError(label + ': ' + (OUT / (label + '.log')).read_text(errors='replace')[-6000:])
    if owned.allocated(OUT) > 96 * 1024**2:
        raise RuntimeError('96MiB evidence/scratch cap')
owned_command_active = False
pending_signal = None
handlers = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
def interrupted(number, frame):
    global pending_signal
    if owned_command_active:
        pending_signal = number
        return
    raise KeyboardInterrupt('signal ' + str(number))
try:
    for s in handlers:
        signal.signal(s, interrupted)
    owned.subreaper()
    user_path = source('platform/sys/usercopy.cpp')
    vfs_path = source('vfs/core.cpp')
    futex_path = source('syscalls_impl/futex/futex.cpp')
    user = user_path.read_text()
    vfs = vfs_path.read_text()
    futex = futex_path.read_text()
    anchors = ['auto copy_chunk(', 'auto copy_to_task_common(', 'StableUserPage::~StableUserPage()',
               'auto StableUserPage::valid(', 'auto StableUserPage::kernel_address(',
               'auto StableUserPage::still_mapped(', 'auto StableUserPage::commit_write(',
               'void StableUserPage::reset(', 'auto pin_task_user_page(', 'auto pin_current_task_user_page(',
               'auto range_valid(', 'auto ensure_writable(', 'auto ensure_current_task_writable(',
               'auto copy_from_task_common(', 'auto copy_from_current_task_partial(',
               'auto copy_to_current_task_partial(', 'auto copy_from_task_partial(']
    actual = '\n\n'.join(definition(user, a) for a in anchors)
    vfs_names = ['clamp_io_count', 'user_io_read_bounce_size_for', 'vfs_read_user_bounced',
                 'vfs_write_user_bounced', 'vfs_pwrite_user_bounced']
    actual_vfs = '\n\n'.join(definition(vfs, 'auto ' + n + '(') for n in vfs_names)
    template = D / 'anonymous_current_usercopy_cases.inc'
    text = template.read_text()
    for token, replacement in [('ACTUAL_USERCOPY', actual), ('ACTUAL_VFS', actual_vfs),
                               ('ACTUAL_TIMEOUT', definition(futex, 'auto relative_timeout_us('))]:
        assert text.count(token) == 1, token
        text = text.replace(token, replacement)
    include = T / 'include'
    for relative in ['platform/mm/virt.hpp', 'platform/sched/task.hpp']:
        p = include / relative
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text('#pragma once\n')
    hpp = source('platform/sys/usercopy.hpp')
    p = include / 'platform/sys/usercopy.hpp'
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(hpp.read_bytes())
    normal = T / 'actual.cpp'
    normal.write_text(text)
    (OUT / 'actual.cpp').write_text(text)
    row = next(x for x in json.loads(DB.read_text()) if x['file'].endswith('/page_allocator_interval_test.cpp'))
    cxx = shlex.split(row['command'])[0]
    base = [cxx, '-I' + str(include), '-std=gnu++23', '-O1', '-g', '-Wall', '-Wextra',
            '-fno-omit-frame-pointer', '-fsanitize=address,undefined', '-fno-sanitize=vptr']
    dep = T / 'actual.d'
    run('dependencies', base + ['-M', '-MF', dep, '-MT', 'out', normal])
    deps = {Path(x).resolve() for x in shlex.split(dep.read_text().replace('\\\n', ' ').split(':', 1)[1]) if not str(Path(x).resolve()).startswith(str(T))}
    deps.update([user_path, vfs_path, futex_path, hpp, source('platform/mm/virt.hpp'),
                 source('platform/mm/virt.opt.cpp'), K / 'platform/sys/syscall_preparation.hpp',
                 template, Path(__file__), DB, R / 'tests/host/unit/file_mmap_registry_runtime_test.py'])
    before = [pin(p) for p in sorted(deps)]
    run('compile-actual', base + [normal, '-o', T / 'actual'])
    run('execute-actual', [T / 'actual'])
    assert 'PASS actual current-copy/VFS/timeout checks=' in (OUT / 'execute-actual.log').read_text()
    mutations = [
        ('lost-reused-reset', '    out.reset();\n    if (!range_valid(user_addr, 1)) return false;',
         '    if (!range_valid(user_addr, 1)) return false;', 'preparation has IF and no caller owners'),
        ('lost-current-copy', 'copy_from_task_common(task, user_addr, dst, size, true, true)',
         'copy_from_task_common(task, user_addr, dst, size, true, false)', 'current partial read prepares each page'),
        ('lost-write-commit', 'if (!pin.commit_write())', 'if (false)', 'write prefix dirty accounting'),
        ('lost-read-preflight', 'if (!ker::mod::sys::usercopy::ensure_current_task_writable(task, USER_BASE + total, TO_READ))',
         'if (false)', 'read preflight before backend consumption'),
    ]
    for label, old, new, expected in mutations:
        assert text.count(old) == 1, (label, text.count(old))
        mutant = T / (label + '.cpp')
        mutant.write_text(text.replace(old, new))
        run('compile-' + label, base + [mutant, '-o', T / label])
        run('execute-' + label, [T / label], 1)
        assert expected in (OUT / ('execute-' + label + '.log')).read_text(), 'unexpected mutant refusal'
    after = [pin(p) for p in sorted(deps)]
    assert before == after, 'source drift'
except BaseException as e:
    errors.append(type(e).__name__ + ': ' + str(e))
    (OUT / 'failure.txt').write_text(traceback.format_exc())
finally:
    for s in handlers:
        signal.signal(s, signal.SIG_IGN)
    removed, preserved = False, None
    try:
        try:
            if before and not after:
                after = [pin(p['path']) for p in before]
            if before and before != after:
                errors.append('source drift at cleanup')
        except BaseException as e:
            errors.append('recheck: ' + str(e))
        if groups:
            errors.append('unproved owned process groups')
            preserved = str(T)
        else:
            try:
                shutil.rmtree(T)
                removed = not T.exists()
            except BaseException as e:
                errors.append('cleanup: ' + str(e))
                preserved = str(T)
        result = dict(qualification='PASS_ACTUAL_CURRENT_USERCOPY_HOST_PORTS' if not errors else 'FAIL_PRIVATE_CURRENT_USERCOPY_HOST',
                      commands=commands, errors=errors, input_before=before, input_after=after,
                      source_held=bool(before) and before == after, scratch_removed=removed,
                      preserved_scratch=preserved, owned_groups_pending=groups[:], seconds=time.monotonic() - start,
                      scope='Actual usercopy/VFS/timeout bodies. Privileged Scope/Task/PTE/pin/backend are finite Host ports; actual preparer/service/factory is qualified separately, no scheduled page-in claim.')
        (OUT / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps({k: result[k] for k in ['qualification', 'errors', 'source_held', 'scratch_removed', 'seconds']}))
    finally:
        for s, h in handlers.items():
            signal.signal(s, h)
    if ephemeral and removed and not groups:
        shutil.rmtree(OUT)
raise SystemExit(bool(errors) or not removed or bool(groups))
