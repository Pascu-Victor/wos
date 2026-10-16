#!/usr/bin/env python3
"""Build only the private scheduled fixture using explicit source/compiler/sysroot."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import stat
import sys
import tempfile
import time
import traceback

sys.dont_write_bytecode = True


def pin(path):
    path = Path(path).resolve(strict=True)
    first = path.stat()
    data = path.read_bytes()
    last = path.stat()
    identity = lambda st: (st.st_dev, st.st_ino, st.st_size, st.st_mtime_ns, st.st_ctime_ns)
    if identity(first) != identity(last):
        raise RuntimeError('input changed while reading ' + str(path))
    return dict(path=str(path), bytes=len(data), sha256=hashlib.sha256(data).hexdigest())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root', type=Path, required=True)
    parser.add_argument('--kernel-overlay', type=Path)
    parser.add_argument('--sysroot', type=Path, required=True)
    parser.add_argument('--compiler', type=Path, required=True)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--evidence-dir', type=Path, required=True)
    args = parser.parse_args()
    root, sysroot, compiler = args.source_root.resolve(strict=True), args.sysroot.resolve(strict=True), args.compiler.absolute()
    output, evidence = args.output.absolute(), args.evidence_dir.absolute()
    if output.exists() or output.is_symlink() or evidence.exists() or evidence.is_symlink():
        parser.error('output and evidence must be fresh exact paths')
    helper = root / 'tests/host/unit/file_mmap_registry_runtime_test.py'
    spec = importlib.util.spec_from_file_location('owned', helper)
    owned = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(owned)
    owned.FILE_BUDGET, owned.SCRATCH_BUDGET, owned.LOG_BUDGET = 32 * 1024**2, 128 * 1024**2, 2 * 1024**2
    evidence.mkdir(parents=True, exist_ok=False)
    scratch = Path(tempfile.mkdtemp(prefix='scratch-', dir=evidence))
    start = time.monotonic()
    groups, commands, before, after, errors = [], [], [], [], []
    active, pending = False, None
    handlers = {s: signal.getsignal(s) for s in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}

    def interrupted(number, frame):
        nonlocal pending
        if active:
            pending = number
            return
        raise KeyboardInterrupt('signal ' + str(number))

    def command(label, argv):
        nonlocal active, pending
        active = True
        try:
            row = owned.command(list(map(str, argv)), evidence / (label + '.log'), scratch, start + 120, groups)
        finally:
            active = False
            if pending is not None:
                number, pending = pending, None
                raise KeyboardInterrupt('deferred signal ' + str(number))
        commands.append(dict(row, label=label))
        if row['exit'] != 0:
            raise RuntimeError(label + ' failed')

    removed = False
    artifact = None
    published = None
    try:
        for number in handlers:
            signal.signal(number, interrupted)
        owned.subreaper()
        source = Path(__file__).with_name('anonymous_usercopy_scheduled.cpp')
        kernel = root / 'modules/kern/src'
        include = args.kernel_overlay.resolve(strict=True) if args.kernel_overlay else kernel
        base = [str(compiler), '--target=x86_64-pc-wos', '--sysroot=' + str(sysroot), '-std=c++23', '-O2', '-g', '-fPIE',
                '-fno-stack-protector', '-fno-stack-check', '-Wall', '-Wextra', '-Werror', '-fno-exceptions', '-fno-rtti',
                '-fno-sanitize=safe-stack', '-isystem', str(sysroot / 'include/c++/v1'), '-isystem', str(sysroot / 'include'),
                '-I' + str(include), '-I' + str(kernel)]
        dep = scratch / 'fixture.d'
        command('dependencies', base + ['-M', '-MF', dep, '-MT', 'out', source])
        deps = {Path(p).resolve() for p in shlex.split(dep.read_text().replace('\\\n', ' ').split(':', 1)[1])}
        link = ['-fuse-ld=lld', '-L' + str(sysroot / 'lib'), '-nostdlib++', '--unwindlib=none', '-lc']
        temporary = scratch / 'anonymous-usercopy-scheduled'
        command('link-plan', base + [source] + link + ['-###', '-o', temporary])
        for line in (evidence / 'link-plan.log').read_text().splitlines():
            try:
                words = shlex.split(line)
            except ValueError:
                continue
            for word in words:
                candidate = Path(word)
                if candidate.is_absolute() and candidate.is_file():
                    deps.add(candidate.resolve())
        # Bind every candidate sysroot link input before invoking the real link;
        # the actual --trace stream below must select only this held set.
        deps.update(p.resolve() for p in (sysroot / 'lib').rglob('*') if p.is_file())
        deps.update([source, Path(__file__), compiler, helper])
        if len(deps) > 8192:
            raise RuntimeError('dependency count cap')
        before = [pin(p) for p in sorted(deps)]
        emitted = scratch / 'compile.d'
        command('compile', base + ['-MD', '-MF', emitted, source] + link + ['-Wl,--trace', '-o', temporary])
        compiled = {Path(p).resolve() for p in shlex.split(emitted.read_text().replace('\\\n', ' ').split(':', 1)[1])}
        linked = set()
        for line in (evidence / 'compile.log').read_text().splitlines():
            selected = line.strip().split('(', 1)[0]
            if selected.startswith('/') and Path(selected).is_file():
                linked.add(Path(selected).resolve())
        if not linked or not compiled.issubset(deps) or not linked.issubset(deps):
            raise RuntimeError('actual compile/link emitted an unheld dependency')
        (evidence / 'emitted-dependencies.json').write_text(json.dumps(dict(compile=sorted(map(str, compiled)), link=sorted(map(str, linked))), indent=2) + '\n')
        after = [pin(p) for p in sorted(deps)]
        if before != after or groups or owned.allocated(evidence) > 128 * 1024**2:
            raise RuntimeError('source/group/storage qualification failed')
        data = temporary.read_bytes()
        if not 0 < len(data) <= 32 * 1024**2:
            raise RuntimeError('artifact byte cap')
        output.parent.mkdir(parents=True, exist_ok=True)
        active = True
        fd = None
        try:
            fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o755)
            published = os.fstat(fd)
            with os.fdopen(fd, 'wb') as stream:
                fd = None
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            artifact = pin(output)
        finally:
            if fd is not None:
                os.close(fd)
            active = False
            if pending is not None:
                number, pending = pending, None
                raise KeyboardInterrupt('deferred publication signal ' + str(number))
        if time.monotonic() >= start + 115:
            raise TimeoutError('build publication exceeded active deadline')
    except BaseException as error:
        errors.append(type(error).__name__ + ': ' + str(error))
        (evidence / 'failure.txt').write_text(traceback.format_exc())
    finally:
        for number in handlers:
            signal.signal(number, signal.SIG_IGN)
        try:
            artifact_removed = False
            preserved_artifact = None
            try:
                if before and not after:
                    after = [pin(p['path']) for p in before]
                if before and before != after:
                    errors.append('source drift')
            except BaseException as error:
                errors.append('final source recheck: ' + type(error).__name__ + ': ' + str(error))
            if groups:
                errors.append('unproved owned process groups; scratch preserved')
            else:
                try:
                    shutil.rmtree(scratch)
                    removed = not scratch.exists()
                    if not removed:
                        errors.append('scratch removal not proven')
                except BaseException as error:
                    errors.append('scratch cleanup: ' + type(error).__name__ + ': ' + str(error))
            if errors:
                try:
                    current = output.lstat()
                    if (groups or published is None
                            or (current.st_dev, current.st_ino, current.st_uid, current.st_nlink)
                            != (published.st_dev, published.st_ino, published.st_uid, 1)
                            or not stat.S_ISREG(current.st_mode)):
                        preserved_artifact = str(output)
                        errors.append('artifact identity/ownership not proven; preserved')
                    else:
                        output.unlink()
                        artifact_removed = not output.exists() and not output.is_symlink()
                        if not artifact_removed:
                            preserved_artifact = str(output)
                            errors.append('artifact removal not proven')
                except FileNotFoundError:
                    artifact_removed = True
                except BaseException as error:
                    preserved_artifact = str(output)
                    errors.append('artifact cleanup: ' + type(error).__name__ + ': ' + str(error))
            result = dict(result='PASS_BUILD_ONLY' if not errors and removed else 'FAIL_BUILD', errors=errors,
                          commands=commands, input_before=before, input_after=after, source_held=bool(before) and before == after,
                          artifact=artifact, groups=groups, scratch_removed=removed, preserved_scratch=None if removed else str(scratch),
                          artifact_removed=artifact_removed, preserved_artifact=preserved_artifact,
                          seconds=time.monotonic() - start, scheduled_runtime_qualified=False)
            try:
                (evidence / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
            except BaseException as error:
                errors.append('result publication: ' + type(error).__name__ + ': ' + str(error))
                result['result'] = 'FAIL_BUILD'
                if published is not None and not artifact_removed:
                    result['preserved_artifact'] = str(output)
            print(json.dumps({k: result[k] for k in ['result', 'errors', 'seconds', 'scratch_removed',
                                                   'preserved_scratch', 'artifact_removed', 'preserved_artifact', 'groups']}),
                  file=sys.stderr if errors else sys.stdout)
        finally:
            for number, handler in handlers.items():
                signal.signal(number, handler)
    return int(bool(errors) or not removed or bool(groups))


if __name__ == '__main__':
    raise SystemExit(main())
