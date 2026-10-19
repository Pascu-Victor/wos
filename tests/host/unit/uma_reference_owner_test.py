#!/usr/bin/env python3
"""Compile and execute the actual direct-suite owner with explicit Linux seams.

Linux waitid emulates WOS waitpid(WNOWAIT) only in the C++ Host fixture. Actual
fork/session/FD/signal/status operations execute. No WOS/runtime qualification.
"""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import resource
import shutil
import shlex
import signal
import subprocess
import tempfile
import time
import traceback


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, required=True, help='canonical repository for maintained process helper')
    parser.add_argument('--source-root', type=Path, default=Path(__file__).resolve().parents[3])
    parser.add_argument('--cxx', required=True)
    parser.add_argument('--evidence-dir', type=Path)
    args = parser.parse_args()
    # Canonical exact-owned cleanup requires these real Linux Python APIs.
    missing = [name for name, module, attribute in (
        ('os.pidfd_open', os, 'pidfd_open'),
        ('signal.pidfd_send_signal', signal, 'pidfd_send_signal'),
        ('os.waitid', os, 'waitid'),
    ) if not callable(getattr(module, attribute, None))]
    if not isinstance(getattr(os, 'WNOWAIT', None), int) or os.WNOWAIT <= 0:
        missing.append('os.WNOWAIT')
    if missing:
        parser.error('requires Linux Python with pidfd-safe owned cleanup and retained wait authority; missing: ' + ', '.join(missing))
    # Literal oracle from the retained pre-opt-in default116 source, not the
    # mutable installed suite path (which includes these opt-in blocks).
    default116_sha256 = '5d748710ad31d9709319cad5a6a2d5e1fe0f6908f1d6fcc1e401e7573792cb16'
    source_root = args.source_root.resolve()
    owner = source_root / 'tests/fixtures/uma_reference_owner.cpp'
    header = owner.with_suffix('.hpp')
    catalog = owner.with_name('uma_reference_catalog.hpp')
    cases = source_root / 'tests/host/unit/uma_reference_owner_cases.cpp'
    suite = source_root / 'configs/drive/srv/wos_userland_suite.sh'
    helper_path = args.root.resolve() / 'tests/host/unit/file_mmap_registry_runtime_test.py'
    spec = importlib.util.spec_from_file_location('uma_owner_process', helper_path)
    if spec is None or spec.loader is None:
        raise ValueError('missing maintained owned-session helper')
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)  # Before owned directories or children.

    def pin(path):
        path = Path(path).resolve()
        data = path.read_bytes()
        return dict(path=str(path), bytes=len(data), sha256=hashlib.sha256(data).hexdigest())

    support=[owner.with_name('uma_reference_arm.cpp'),owner.with_name('uma_reference_arm.hpp'),owner.with_name('uma_reference_stamp.cpp'),
             source_root/'tests/host/unit/uma_reference_arm_host.cpp']
    abi=args.root.resolve()/'toolchain/src/mlibc/sysdeps/wos/include/sys/init_control.h'
    init_root=args.root.resolve()/'modules/init/src'
    init_sources=[init_root/'services.cpp',init_root/'supervisor_model.h',init_root/'service_manifest.h',
                  args.root.resolve()/'configs/wos-services-uma-reference.conf']
    sources = [owner, header, catalog, cases, suite, Path(__file__), helper_path,abi]+support+init_sources
    before = [pin(path) for path in sources]
    started = time.monotonic()
    active, uncertain, commands, checks, errors = {}, [], [], [], []
    emitted_before, emitted_after = [], []
    out = scratch = None
    output_owned = False
    handled = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)
    original_handlers = {sig: signal.getsignal(sig) for sig in handled}

    def interrupted(number, _frame):
        raise KeyboardInterrupt('Host signal ' + str(number))

    def check(name, passed):
        checks.append(dict(name=name, **{'pass': bool(passed)}))
        if not passed:
            raise AssertionError(name)

    def command(label, argv, expected=0, timeout=15, escape=False):
        if time.monotonic() >= started + 100:
            raise TimeoutError('whole Host budget')
        log = out / (label + '.log')
        process = None
        identities = []
        then = time.monotonic()

        def limits():
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)
            resource.setrlimit(resource.RLIMIT_CORE, (0, 0))

        with log.open('xb') as stream:
            try:
                previous = signal.pthread_sigmask(signal.SIG_BLOCK, handled)
                try:
                    process = subprocess.Popen(list(map(str, argv)), stdout=stream, stderr=subprocess.STDOUT,
                                               start_new_session=True, preexec_fn=limits)
                    active[process.pid] = process
                finally:
                    signal.pthread_sigmask(signal.SIG_SETMASK, previous)
                until = min(started + 100, then + timeout)
                while os.waitid(os.P_PID, process.pid, os.WEXITED | os.WNOHANG | os.WNOWAIT) is None:
                    if time.monotonic() >= until:
                        raise TimeoutError(label)
                    if log.stat().st_size > 2 << 20:
                        raise ValueError('command log bound')
                    if sum(p.stat().st_blocks * 512 for p in scratch.rglob('*') if p.is_file()) > 64 << 20:
                        raise ValueError('allocated Host scratch bound')
                    time.sleep(.01)
                identities = helper.owned_session(process.pid)
                live = [row for row in identities if row[0] != process.pid and row[2] != 'Z']
                if escape:
                    check('real escaped group is present before independent outer cleanup',
                          any(row[4] != process.pid for row in live))
                else:
                    check(label + ' no unexpectedly live descendants before outer cleanup', not live)
            finally:
                if process is not None:
                    old = {sig: signal.signal(sig, signal.SIG_IGN) for sig in handled}
                    try:
                        helper.reap_group(process, min(started + 110, time.monotonic() + 5))
                        active.pop(process.pid)
                    except BaseException:
                        if process.returncode is not None:
                            active.pop(process.pid, None)
                            uncertain.append(process.pid)
                        raise
                    finally:
                        for sig, handler in old.items():
                            signal.signal(sig, handler)
        code = process.returncode
        commands.append(dict(label=label, argv=list(map(str, argv)), exit=code, expected=expected,
                             seconds=time.monotonic() - then, log=pin(log), owned_absence_proven=True,
                             independently_cleaned_escaped_descendants=escape,
                             observed_owned_before_outer_cleanup=identities))
        check(label + ' exact exit and owned absence', code == expected)
        return log.read_text()

    try:
        for sig in handled:
            signal.signal(sig, interrupted)
        helper.subreaper()
        previous = signal.pthread_sigmask(signal.SIG_BLOCK, handled)
        try:
            out = args.evidence_dir.resolve() if args.evidence_dir else Path(tempfile.mkdtemp(prefix='wos-uma-owner-evidence-'))
            if args.evidence_dir:
                out.mkdir(parents=True, exist_ok=False)
            output_owned = True
            scratch = Path(tempfile.mkdtemp(prefix='wos-uma-owner-'))
        finally:
            signal.pthread_sigmask(signal.SIG_SETMASK, previous)
        shim=scratch/'abi/sys'
        shim.mkdir(parents=True)
        shutil.copyfile(abi,shim/'init_control.h')
        # Select the WHOLE actual producer. The finite TU supplies only its
        # runtime object using genuine init model/manifest types; no copied
        # aggregate-state formula or preferred RUNNING value is used.
        producer=init_sources[0].read_text()
        begin='auto supervisor_status_state() -> control_abi::SupervisorState {'
        end='\nvoid publish_status(uint64_t now_ms) {'
        check('one whole actual aggregate producer',producer.count(begin)==1 and producer.count(end)==1)
        a=producer.index(begin);b=producer.index(end,a)
        aggregate=scratch/'aggregate.cpp'
        aggregate.write_text('#include "uma_reference_arm.hpp"\n#include "supervisor_model.h"\n'
            'namespace {\nnamespace control_abi=ker::abi::init_control;\n'
            'using wos::init::ServiceState; using wos::init::SupervisorShutdownPhase;\n'
            'struct Runtime { bool failed{}, model_ready{true}, shutdown_complete{}; '
            'wos::init::ServiceManifest manifest{}; wos::init::SupervisorModel model{}; } runtime;\n'
            +producer[a:b]+'\n}\n'
            'auto uma_arm_host_aggregate(const wos::init::SupervisorModel& value) -> control_abi::SupervisorState {\n'
            'runtime={}; runtime.model_ready=true; runtime.manifest.service_count=value.service_count; '
            'runtime.model=value; return supervisor_status_state();\n}\n')
        common = [args.cxx, '-std=c++23', '-O2', '-g', '-Wall', '-Wextra', '-Werror', '-DWOS_UMA_OWNER_HOST=1','-DWOS_UMA_ARM_HOST=1','-DWOS_UMA_TIMING_HOST=1','-DWOS_UMA_LINKED_AUDIT=1',
                  '-I',str(shim.parent),'-I', str(owner.parent),'-I',str(init_root)]
        binary = scratch / 'owner-cases'
        command('compile-owner', common + ['-MD', '-MF', str(out / 'owner.d'), '-c', owner, '-o', scratch / 'owner.o'])
        command('compile-cases', common + ['-MD', '-MF', str(out / 'cases.d'), '-c', cases, '-o', scratch / 'cases.o'])
        lib=[]
        for label,source in [('arm',support[0]),('stamp',support[2]),('host',support[3]),('aggregate',aggregate)]:
            obj=scratch/(label+'.o');lib.append(obj)
            command('compile-'+label,common+['-MD','-MF',out/(label+'.d'),'-c',source,'-o',obj])
        command('link-owner', [args.cxx, scratch / 'owner.o', scratch / 'cases.o']+lib+['-o', binary])
        dependencies = set()
        for dep in [out/(label+'.d')for label in ('owner','cases','arm','stamp','host','aggregate')]:
            text = dep.read_text().replace('\\\n', '')
            dependencies.update(Path(value).resolve() for value in shlex.split(text.split(':', 1)[1]))
        emitted_before = [pin(path) for path in sorted(dependencies)]

        def tree(mode):
            root = scratch / mode
            root.mkdir(mode=0o700)
            phase = root / 'warmup'
            phase.mkdir(mode=0o700)
            for part in ('logs', 'timing', 'work'):
                (phase / part).mkdir(mode=0o700)
            if mode == 'setup':
                (phase / 'work').chmod(0o755)
            return root

        for mode in ('success', 'raw', 'cancel', 'late', 'deadline', 'timeout', 'output', 'setup', 'escape'):
            text = command('owner-' + mode, [binary, mode, tree(mode)], escape=mode == 'escape')
            check(mode + ' actual assertions and explicit unknown closure',
                  re.search(r'OWNER_CASE mode=' + mode + r' checks=[1-9][0-9]* failures=0 .*after=0 closure=UNKNOWN\n', text) is not None)

        # Genuine faulty predecessor control: the first completed "peek" consumes
        # membership. The same actual oracle rejects the subsequent final wait.
        old = owner.read_text()
        needle = 'child_wait(out.child, raw, WNOHANG | WNOWAIT)'
        check('unique actual ownership control', old.count(needle) == 1)
        broken = out / 'consuming-peek.cpp'
        broken.write_text(old.replace(needle, 'child_wait(out.child, raw, WNOHANG)'))
        mutant = scratch / 'consuming-peek'
        command('compile-consuming-peek', common + ['-c', broken, '-o', scratch / 'consuming-peek.o'])
        command('link-consuming-peek', [args.cxx, scratch / 'consuming-peek.o', scratch / 'cases.o']+lib+['-o', mutant])
        rejected = command('reject-consuming-peek', [mutant, 'success', tree('control')], expected=1)
        check('real consumed-membership control fails actual status oracle', 'FAIL actual owner exact failure reason' in rejected)

        shell = suite.read_text()
        marked = re.compile(r'^[ \t]*# UMA_TIMING_BEGIN ([a-z_]+)\n.*?^[ \t]*# UMA_TIMING_END \1\n', re.M | re.S)
        erased = marked.sub('', shell)
        check('all timing blocks erase to exact frozen default116',
              hashlib.sha256(erased.encode()).hexdigest() == default116_sha256)
        block = re.search(r'^            # UMA_TIMING_BEGIN watchdog_timeout\n(.*?)^            # UMA_TIMING_END watchdog_timeout\n', shell, re.M | re.S)
        check('one mode1-only watchdog block', block is not None and shell.count('# UMA_TIMING_BEGIN watchdog_timeout') == 1)
        check('new timeout branch never signals or changes polling/grace constants',
              'kill' not in block.group(1) and 'sleep' not in block.group(1) and 'exit 0' in block.group(1))
        # Execute the actual watchdog body with a shell kill observer, preserving
        # the original 1s loop text but using a zero deadline to reach expiry now.
        start = shell.index("            trap 'exit 0' TERM INT HUP")
        end = shell.index('\n        ) &', start)
        watchdog = shell[start:end]
        for version, signals in ((1, False), (0, True)):
            wd = scratch / ('watchdog-' + str(version))
            wd.mkdir()
            script = wd / 'run.sh'
            script.write_text('''#!/bin/sh
case_status="$1/status"
timeout_marker="$1/timeout"
CASE_TIMEOUT_SECONDS=0
TIMEOUT_KILL_GRACE_SECONDS=0
case_has_group=1
case_pid=123456789
WOS_SUITE_TIMING_VERSION="$2"
kill() { printf 'CALLED\\n' >> "$1_unused"; }
'''.replace('"$1_unused"', '"' + str(wd / 'signals') + '"') + watchdog)
            command('actual-watchdog-' + str(version), ['/bin/sh', script, wd, str(version)])
            check('actual watchdog version' + str(version) + ' marker and expected signal behavior',
                  (wd / 'timeout').read_text() == 'timeout after 0s\n' and (wd / 'signals').exists() == signals)
        command('shell-syntax', ['/bin/sh', '-n', suite])
    except BaseException as error:
        errors.append(type(error).__name__ + ': ' + str(error))
        if output_owned:
            try:
                (out / 'failure.txt').write_text(traceback.format_exc())
            except BaseException as save_error:
                errors.append('failure evidence ' + repr(save_error))
    finally:
        for sig in handled:
            signal.signal(sig, signal.SIG_IGN)
        try:
            after = [pin(path) for path in sources]
            if before != after:
                errors.append('source drift')
            emitted_after = [pin(row['path']) for row in emitted_before]
            if emitted_before != emitted_after:
                errors.append('actual emitted Host input drift')
        except BaseException as error:
            after = []
            errors.append('source recheck ' + repr(error))
        for pid, process in list(active.items()):
            try:
                if process.returncode is not None:
                    active.pop(pid)
                    uncertain.append(pid)
                    continue
                helper.reap_group(process, time.monotonic() + 5)
                active.pop(pid)
            except BaseException as error:
                if process.returncode is not None:
                    active.pop(pid, None)
                    uncertain.append(pid)
                errors.append('owned cleanup ' + repr(error))
        removed = scratch is None
        if not active and not uncertain and scratch is not None:
            try:
                shutil.rmtree(scratch)
                removed = not scratch.exists()
            except BaseException as error:
                errors.append('scratch cleanup ' + repr(error))
        elif active or uncertain:
            errors.append('uncertain owned process closure: scratch preserved')
        receipt = dict(qualification='PASS_ACTUAL_HOST_DIRECT_SUITE_OWNER_ONLY' if not errors else 'FAIL_HOST_SUITE_OWNER',
                       errors=errors, checks=checks, commands=commands, source_before=before, source_after=after,
                       emitted_inputs_before=emitted_before, emitted_inputs_after=emitted_after,
                       seconds=time.monotonic() - started, scratch_removed=removed, active=list(active), uncertain=uncertain,
                       limits=['Linux waitid is an explicit Host adaptation, not actual WOS waitpid runtime.',
                               'Raw one-phase outcome only: full117+117 actual WOS/init/witness/natural-stop gates are not exercised by these component tests.',
                               'Actual escaped group is UNKNOWN/FAIL before independent exact-owned outer Host cleanup.',
                               'Production 5s grace is scaled to50ms in Host cases; no hard syscall latency guarantee.'])
        try:
            if output_owned:
                (out / 'result.json').write_text(json.dumps(receipt, indent=2) + '\n')
        finally:
            for sig, handler in original_handlers.items():
                signal.signal(sig, handler)
        print(json.dumps({key: receipt[key] for key in ('qualification', 'errors', 'seconds', 'scratch_removed', 'active', 'uncertain')}))
        if not args.evidence_dir and not errors and output_owned:
            shutil.rmtree(out)
    return bool(errors)


if __name__ == '__main__':
    raise SystemExit(main())
