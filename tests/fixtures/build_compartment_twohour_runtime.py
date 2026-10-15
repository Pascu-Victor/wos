#!/usr/bin/env python3
"""Build fresh hosted WOS two-hour guest and CLI artifacts; never launch them.

Only standard-library modules are imported. No prior evidence or Host-test
builder is needed. See compartment_twohour_runtime.md for protocol and limits.
"""
import argparse
import ctypes
import hashlib
import json
import os
from pathlib import Path
import re
import resource
import shlex
import shutil
import signal
import struct
import subprocess
import sys
import tempfile
import time

WHOLE = 60
OUTPUT_BUDGET = 16 << 20
SCRATCH_BUDGET = 64 << 20
LOG_BUDGET = 1 << 20
FILE_BUDGET = 8 << 20


def require(value, message):
    if not value:
        raise ValueError(message)


def pin(path):
    path = Path(path).absolute()
    before = path.stat()
    checksum = hashlib.sha256()
    with path.open('rb') as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b''):
            checksum.update(chunk)
    require(path.stat().st_size == before.st_size, 'input size changed while hashing: ' + str(path))
    return {'path': str(path), 'bytes': before.st_size, 'sha256': checksum.hexdigest(),
            'resolved': str(path.resolve(strict=True)), 'symlink': path.is_symlink(),
            'link': os.readlink(path) if path.is_symlink() else None}


def allocated(path: Path) -> int:
    return sum(p.stat().st_blocks * 512 for p in path.rglob("*") if p.is_file())

def subreaper() -> None:
    if not sys.platform.startswith("linux"):
        raise ValueError("bounded owned-session runner requires Linux")
    libc = ctypes.CDLL(None, use_errno=True)
    if libc.prctl(36, 1, 0, 0, 0) != 0:
        raise OSError(ctypes.get_errno(), "PR_SET_CHILD_SUBREAPER")

def process_identity(pid: int) -> tuple[int, int, str, int, int]:
    fields = Path(f"/proc/{pid}/stat").read_text().rpartition(")")[2].split()
    return pid, int(fields[19]), fields[0], int(fields[1]), int(fields[3])

def owned_session(leader: int) -> list[tuple[int, int, str, int, int]]:
    result = []
    for path in Path("/proc").iterdir():
        if not path.name.isdecimal():
            continue
        try:
            identity = process_identity(int(path.name))
        except (FileNotFoundError, ProcessLookupError):
            continue
        # Sequential driver children only; the subreaper adopts escaped owned
        # compiler descendants. An original unreaped leader reserves the SID.
        if identity[4] == leader or identity[3] == os.getpid():
            result.append(identity)
    return result

def kill_identity(identity: tuple[int, int, str, int, int]) -> None:
    pid, started, state, _, _ = identity
    if state == "Z":
        return
    try:
        fd = os.pidfd_open(pid, 0)
    except ProcessLookupError:
        return
    try:
        if process_identity(pid)[1] == started:
            signal.pidfd_send_signal(fd, signal.SIGKILL)
    except (FileNotFoundError, ProcessLookupError):
        pass
    finally:
        os.close(fd)

def reap_group(process: subprocess.Popen, deadline: float) -> None:
    while True:
        identities = owned_session(process.pid)
        for identity in identities:
            kill_identity(identity)
        for pid, _, _, _, _ in identities:
            if pid == process.pid:
                continue
            try:
                os.waitpid(pid, os.WNOHANG)
            except ChildProcessError:
                pass
        remaining = owned_session(process.pid)
        if all(pid == process.pid and state == "Z" for pid, _, state, _, _ in remaining):
            process.wait(timeout=max(0.01, deadline - time.monotonic()))
            if owned_session(process.pid):
                raise ValueError("owned session/adopted descendants remain after leader reap")
            return
        if time.monotonic() >= deadline:
            raise TimeoutError("owned session not proven absent; preserve scratch")
        time.sleep(0.01)

def command(argv: list[str], log: Path, scratch: Path, deadline: float, groups: list[int]) -> dict:
    if time.monotonic() >= deadline - 5:
        raise TimeoutError("60-second whole budget exhausted before command")

    def limits() -> None:
        resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
        resource.setrlimit(resource.RLIMIT_FSIZE, (FILE_BUDGET, FILE_BUDGET))

    started = time.monotonic()
    process = None
    with log.open("xb") as output:
        try:
            process = subprocess.Popen(argv, stdout=output, stderr=subprocess.STDOUT,
                                       start_new_session=True, preexec_fn=limits,
                                       env=dict(os.environ, TMPDIR=str(scratch),
                                                ASAN_OPTIONS="detect_leaks=0:halt_on_error=1",
                                                LSAN_OPTIONS="detect_leaks=0"))
            groups.append(process.pid)
            while True:
                if log.stat().st_size > LOG_BUDGET or allocated(scratch) > SCRATCH_BUDGET:
                    raise ValueError("child log/private scratch exceeded bounded budget")
                if time.monotonic() >= deadline - 5:
                    raise TimeoutError("60-second whole budget exhausted")
                info = os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT)
                if info is not None:
                    break
                time.sleep(0.02)
        finally:
            if process is not None:
                handlers = {sig: signal.signal(sig, signal.SIG_IGN)
                            for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
                try:
                    reap_group(process, deadline)
                    groups.remove(process.pid)
                finally:
                    for sig, handler in handlers.items():
                        signal.signal(sig, handler)
    if log.stat().st_size > LOG_BUDGET or allocated(scratch) > SCRATCH_BUDGET:
        raise ValueError("completed command exceeded storage/output budget")
    return {"argv": argv, "exit": process.returncode, "seconds": round(time.monotonic() - started, 6),
            "leader_reaped": True, "owned_session_absent": True, "log": pin(log)}

def elf_gate(path):
    data = path.read_bytes()
    if len(data) < 64 or data[:7] != b"\x7fELF\x02\x01\x01":
        raise ValueError("expected an ELF64 little-endian artifact")
    fields = struct.unpack_from("<HHIQQQIHHHHHH", data, 16)
    kind, machine, version, entry, phoff = fields[:5]
    if (kind, machine, version) != (3, 62, 1) or fields[8] != 56 or not 1 <= fields[9] <= 32:
        raise ValueError("expected x86_64 PIE with bounded ELF64 program headers")
    count = fields[9]
    if phoff > len(data) or count * 56 > len(data) - phoff:
        raise ValueError("program headers exceed the file")
    segments = [struct.unpack_from("<IIQQQQQQ", data, phoff + i * 56) for i in range(count)]
    for segment in segments:
        if segment[2] > len(data) or segment[5] > len(data) - segment[2] or segment[5] > segment[6]:
            raise ValueError("segment has invalid bounds")
        if segment[0] == 1 and segment[1] & 3 == 3:
            raise ValueError("writable executable LOAD segment")
        if segment[0] == 0x6474E551 and segment[1] & 1:
            raise ValueError("executable stack")
    if len([segment for segment in segments if segment[0] == 0x6474E551]) != 1:
        raise ValueError("expected one explicit non-executable stack segment")
    interpreters = [segment for segment in segments if segment[0] == 3]
    if len(interpreters) != 1:
        raise ValueError("expected exactly one PT_INTERP")
    interp = interpreters[0]
    if data[interp[2]:interp[2] + interp[5]] != b"/lib/ld.so\0":
        raise ValueError("unexpected interpreter")
    if not any(s[0] == 1 and s[1] & 1 and s[3] <= entry < s[3] + s[5] for s in segments):
        raise ValueError("entry is not within file-backed executable bytes")
    dynamic = [segment for segment in segments if segment[0] == 2]
    if len(dynamic) != 1 or dynamic[0][5] % 16:
        raise ValueError("invalid dynamic segment")
    tags = []
    for offset in range(dynamic[0][2], dynamic[0][2] + dynamic[0][5], 16):
        tag, value = struct.unpack_from("<QQ", data, offset)
        if tag == 0:
            break
        tags.append((tag, value))
    else:
        raise ValueError("unterminated dynamic segment")
    strings = [value for tag, value in tags if tag == 5]
    sizes = [value for tag, value in tags if tag == 10]
    if len(strings) != 1 or len(sizes) != 1 or sizes[0] == 0:
        raise ValueError("missing bounded dynamic string table")
    mapped = [s for s in segments if s[0] == 1 and s[3] <= strings[0] and strings[0] - s[3] <= s[5]
              and sizes[0] <= s[5] - (strings[0] - s[3])]
    if len(mapped) != 1:
        raise ValueError("dynamic string table is not file-backed unambiguously")
    string_offset = mapped[0][2] + strings[0] - mapped[0][3]
    table = data[string_offset:string_offset + sizes[0]]
    needed = []
    for tag, value in tags:
        if tag != 1:
            continue
        if value >= len(table) or table.find(b"\0", value) < 0:
            raise ValueError("unbounded NEEDED name")
        needed.append(table[value:table.index(0, value)].decode("ascii"))
    if needed != ["libc.so"]:
        raise ValueError(f"unexpected dependency closure: {needed}")
    return {"type": "ET_DYN", "machine": machine, "entry": entry, "program_headers": count,
            "interpreter": "/lib/ld.so", "needed": needed, "bytes": len(data)}



def dependencies(path, root):
    require(path.stat().st_size <= LOG_BUDGET, 'dependency file exceeds 1MiB')
    body = path.read_text().replace('\\\n', ' ').splitlines()[0]
    require(': ' in body, 'dependency target separator missing')
    paths = {Path(os.path.normpath(str(root / p))) for p in shlex.split(body.split(': ', 1)[1])}
    require(len(paths) <= 2048, 'dependency count exceeds 2048')
    return paths


def output_usage(directory):
    paths = list(directory.rglob('*'))
    require(all(not p.is_symlink() for p in paths), 'output symlink appeared')
    files = [p for p in paths if p.is_file()]
    total = sum(p.stat().st_size for p in files)
    require(len(files) <= 32 and total <= OUTPUT_BUDGET, 'output storage budget exceeded')
    return {'files': len(files), 'logical_bytes': total,
            'allocated_bytes': sum(p.stat().st_blocks * 512 for p in files)}


def save(path, value):
    data = (json.dumps(value, indent=2, sort_keys=True) + '\n').encode()
    require(len(data) <= FILE_BUDGET, 'receipt exceeds8MiB file budget')
    with path.open('xb') as stream:
        stream.write(data)


def interrupted(signum, frame):
    raise InterruptedError('builder interrupted by signal ' + str(signum))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', required=True, type=Path)
    parser.add_argument('--source-dir', type=Path, default=Path(__file__).resolve().parent)
    parser.add_argument('--output-dir', required=True, type=Path)
    parser.add_argument('--sysroot', type=Path)
    parser.add_argument('--llvm-bin', type=Path)
    parser.add_argument('--resource-dir', type=Path)
    args = parser.parse_args()
    root = args.root.resolve(strict=True)
    sources = args.source_dir.resolve(strict=True)
    output = args.output_dir.absolute()
    sysroot = (args.sysroot or root / 'toolchain/sysroot').resolve(strict=True)
    llvm = (args.llvm_bin or root / 'toolchain/host/bin').resolve(strict=True)
    resource_dir = (args.resource_dir or llvm.parent / 'lib/clang/22').resolve(strict=True)
    require((root / 'AGENTS.md').is_file(), 'explicit WOS repository root required')
    require(output == Path(os.path.normpath(str(output))) and
            output.parent.resolve(strict=True) == output.parent and
            not os.path.lexists(output), 'fresh output under an exact existing parent required')
    require(shutil.disk_usage(output.parent).free >= 1 << 30, 'artifact free-space floor1GiB')
    source = sources / 'compartment_twohour_runtime.cpp'
    cli = root / 'modules/compartment/src/main.cpp'
    for path in (source, cli, sources / 'validate_compartment_twohour_runtime.py',
                 sources / 'compartment_twohour_runtime.md'):
        require(path.is_file() and not path.is_symlink(), 'regular source required: ' + str(path))
    clang = llvm / 'clang++'
    linker = llvm / 'ld.lld'
    started = time.monotonic()
    deadline = started + WHOLE
    groups, commands, dep_rows = [], [], []
    scratch = scratch_identity = None
    held, generated, emitted = {}, {}, set()
    result = {'qualification': 'FAIL', 'errors': [], 'commands': commands,
              'whole_budget_seconds': WHOLE, 'runtime_qualified': False,
              'criterion8_qualified': False, 'full_goal_qualified': False,
              'VM_actions': False, 'existing_artifact_or_sysroot_writes': False}
    output.mkdir(mode=0o700)
    handlers = {sig: signal.signal(sig, interrupted)
                for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)}
    try:
        explicit = [Path(__file__).resolve(), source, cli,
                    sources / 'validate_compartment_twohour_runtime.py',
                    sources / 'compartment_twohour_runtime.md', root / 'AGENTS.md',
                    root / '.vscode/tasks.json', root / 'modules/compartment/CMakeLists.txt',
                    root / 'modules/CMakeLists.txt', clang, linker, Path(sys.executable),
                    sysroot / 'lib/ld.so', sysroot / 'lib/libc.so']
        candidates = set(explicit)
        for base in (root / 'modules/kern/src/abi', sysroot / 'include', sysroot / 'lib',
                     resource_dir / 'include', resource_dir / 'lib'):
            require(base.is_dir(), 'target input directory missing: ' + str(base))
            candidates.update(p for p in base.rglob('*') if p.is_file())
        require(len(candidates) <= 32768, 'input census count exceeds 32768')
        held = {str(p): pin(p) for p in sorted(candidates)}
        require(sum(row['bytes'] for row in held.values()) <= 2 << 30, 'input census exceeds2GiB')
        result['candidate_input_count'] = len(held)
        result['candidate_input_census_sha256'] = hashlib.sha256(
            json.dumps(held, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        save(output / 'candidate-inputs-before.json', list(held.values()))
        subreaper()
        scratch = Path(tempfile.mkdtemp(prefix='wos-twohour-fixture-build-', dir='/tmp'))
        st = scratch.stat()
        scratch_identity = (st.st_dev, st.st_ino, st.st_uid)
        require(st.st_uid == os.getuid() and not scratch.is_symlink(), 'owned private compiler scratch')
        result['storage_before'] = {'free_bytes': shutil.disk_usage(output.parent).free,
                                    'output': output_usage(output),
                                    'scratch_budget_bytes': SCRATCH_BUDGET,
                                    'output_budget_bytes': OUTPUT_BUDGET}

        def run(name, argv, depfile):
            output_usage(output)
            row = {'name': name}
            commands.append(row)
            row.update(command(list(map(str, argv)), output / (name + '.log'), scratch, deadline, groups))
            require(row['exit'] == 0, 'actual compile/link failed: ' + name)
            inputs = []
            for path in sorted(dependencies(depfile, root)):
                key = str(path)
                if key in held:
                    require(pin(path) == held[key], 'emitted external input changed: ' + key)
                    emitted.add(key)
                    inputs.append(held[key])
                elif key in generated:
                    require(pin(path) == generated[key], 'owned intermediate changed: ' + key)
                    inputs.append(generated[key])
                else:
                    raise ValueError('emitted external input not preheld: ' + key)
            dep_rows.append({'command': name, 'depfile': pin(depfile), 'inputs': inputs})
            output_usage(output)

        target = ['--no-default-config', '--target=x86_64-pc-wos',
                  '--sysroot=' + str(sysroot), '-resource-dir', str(resource_dir)]
        includes = ['-nostdinc', '-nostdinc++', '-isystem', str(sysroot / 'include/c++/v1'),
                    '-isystem', str(resource_dir / 'include'), '-isystem', str(sysroot / 'include'),
                    '-I', str(root / 'modules/kern/src')]
        rows = []
        for label, input_source in (('guest', source), ('cli', cli)):
            obj = scratch / (label + '.o')
            compile_dep = output / (label + '-compile.d')
            run('compile-' + label, [clang, *target, '-std=c++23', '-O2', '-g', '-fPIE',
                '-fno-stack-protector', '-fno-stack-check', '-Wall', '-Wextra', '-Werror',
                '-fno-exceptions', '-fno-rtti', '-fno-sanitize=safe-stack', *includes,
                '-MD', '-MF', compile_dep, '-c', input_source, '-o', obj], compile_dep)
            generated[str(obj)] = pin(obj)
            executable = output / ('compartment-twohour-runtime' if label == 'guest' else 'wos-compartment')
            link_dep = output / (label + '-link.d')
            run('link-' + label, [clang, *target, '-fPIE', '-pie', obj, '-fuse-ld=' + str(linker),
                '-L' + str(sysroot / 'lib'), '-nostdlib++', '--unwindlib=none', '-lc',
                '-Wl,--dynamic-linker=/lib/ld.so', '-Wl,--dependency-file=' + str(link_dep),
                '-o', executable], link_dep)
            require(executable.stat().st_size <= FILE_BUDGET, 'ELF output exceeded8MiB')
            metadata = elf_gate(executable)
            executable.chmod(0o555)
            rows.append({'label': label, 'source': held[str(input_source)],
                         'artifact': pin(executable), 'elf': metadata})
        selected = {str(p) for p in explicit} | emitted
        result['source_before'] = [held[p] for p in sorted(selected)]
        result['source_after'] = [pin(Path(p)) for p in sorted(selected)]
        result['source_held'] = result['source_before'] == result['source_after']
        require(result['source_held'], 'selected source/tool/emitted dependency closure changed')
        result['actual_emitted_external_dependency_count'] = len(emitted)
        result['emitted_dependency_files'] = dep_rows
        result['owned_intermediate_inputs'] = list(generated.values())
        result['artifacts'] = rows
        result['qualification'] = 'PASS_ACTUAL_TWOHOUR_FIXTURE_ELFS_ONLY'
    except BaseException as error:
        result['errors'].append(type(error).__name__ + ': ' + str(error))
    finally:
        cleanup_handlers = {sig: signal.signal(sig, signal.SIG_IGN) for sig in handlers}
        result['owned_groups_pending'] = groups.copy()
        result['scratch_removed'] = scratch is None
        try:
            if scratch is not None:
                st = scratch.stat()
                exact = (st.st_dev, st.st_ino, st.st_uid) == scratch_identity and not scratch.is_symlink()
                result['scratch_allocated_bytes_before_cleanup'] = allocated(scratch)
                if exact and not groups:
                    shutil.rmtree(scratch)
                result['scratch_removed'] = not scratch.exists()
                require(result['scratch_removed'], 'owned compiler scratch cleanup unproved')
            result['elapsed_seconds'] = time.monotonic() - started
            require(result['elapsed_seconds'] < WHOLE, 'whole60s compile/watch/cleanup budget exceeded')
            result['storage_after'] = {'free_bytes': shutil.disk_usage(output.parent).free,
                                      'output': output_usage(output)}
        except BaseException as error:
            result['errors'].append('cleanup ' + type(error).__name__ + ': ' + str(error))
        if result['errors']:
            result['qualification'] = 'FAIL'
        save(output / 'result.json', result)
        for sig, old in handlers.items():
            signal.signal(sig, old)
        print(json.dumps({'result': pin(output / 'result.json'), 'qualification': result['qualification'],
                          'errors': result['errors'], 'scratch_removed': result['scratch_removed']}))
    return 0 if not result['errors'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
