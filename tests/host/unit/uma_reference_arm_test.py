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
    source_root = args.source_root.resolve()
    owner = source_root / 'tests/fixtures/uma_reference_owner.cpp'
    header = owner.with_suffix('.hpp')
    catalog = owner.with_name('uma_reference_catalog.hpp')
    cases = source_root / 'tests/host/unit/uma_reference_arm_cases.cpp'
    suite = source_root / 'configs/drive/srv/wos_userland_suite.sh'
    helper_path = args.root.resolve() / 'tests/host/unit/file_mmap_registry_runtime_test.py'
    spec = importlib.util.spec_from_file_location('uma_arm_process', helper_path)
    if spec is None or spec.loader is None:
        raise ValueError('missing maintained owned-session helper')
    helper = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(helper)  # Before owned directories or children.

    def pin(path):
        path = Path(path).resolve()
        data = path.read_bytes()
        return dict(path=str(path), bytes=len(data), sha256=hashlib.sha256(data).hexdigest())

    baseline = args.root.resolve() / 'configs/drive/srv/wos_userland_suite.sh'
    support=[owner.with_name('uma_reference_arm.cpp'),owner.with_name('uma_reference_arm.hpp'),owner.with_name('uma_reference_stamp.cpp'),
             source_root/'tests/host/unit/uma_reference_arm_host.cpp',owner.with_name('uma_reference_control.cpp'),owner.with_name('uma_reference_exit_witness.cpp')]
    abi=args.root.resolve()/'toolchain/src/mlibc/sysdeps/wos/include/sys/init_control.h'
    decoder=owner.with_name('validate_uma_reference.py')
    audit=source_root/'scripts/test/runtime_test_audit.py'
    sources = [owner, header, catalog, cases, suite, Path(__file__), helper_path, baseline,abi,decoder,audit]+support
    spec=importlib.util.spec_from_file_location('uma_whole_arm_decoder',decoder)
    codec=importlib.util.module_from_spec(spec);spec.loader.exec_module(codec)
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
        common = [args.cxx, '-std=c++23', '-O2', '-g', '-Wall', '-Wextra', '-Werror', '-DWOS_UMA_OWNER_HOST=1','-DWOS_UMA_ARM_HOST=1','-DWOS_UMA_TIMING_HOST=1','-DWOS_UMA_LINKED_AUDIT=1',
                  '-I',str(shim.parent),'-I', str(owner.parent)]
        objects=[]
        units=[('owner',owner,None),('arm',support[0],None),('stamp',support[2],None),('host',support[3],None),
               ('controller',support[4],'uma_controller_main'),('observer',support[5],'uma_witness_main'),('cases',cases,None)]
        for label,source,rename in units:
            obj=scratch/(label+'.o');objects.append(obj)
            extra=['-Dmain='+rename]if rename else[]
            command('compile-'+label,common+extra+['-MD','-MF',out/(label+'.d'),'-c',source,'-o',obj])
        binary=scratch/'arm-cases'
        command('link-arm',[args.cxx]+objects+['-o',binary])
        dependencies=set()
        for label,_,_ in units:
            text=(out/(label+'.d')).read_text().replace('\\\n','')
            dependencies.update(Path(value).resolve()for value in shlex.split(text.split(':',1)[1]))
        emitted_before=[pin(path)for path in sorted(dependencies)]
        clock='version=1\nsource=hpet:smp-unqualified\ninvariant_tsc=1\nfrequency_hz=10000000\nresolution_ns=1\nconversion_multiplier=100\nconversion_divisor=1\ncalibration_spread_ppm=0\nwatchdog_deviation_ns=0\nkvm_registered_cpus=0\nkvm_expected_cpus=8\nkvm_watchdog_checks=0\nrtc_valid=1\nwall_steps=0\nwall_slews=0\nwall_rejected=0\nwall_pending_ns=0\nslew_limit_ppm=500\nprocess_timer_real_depth=0\nprocess_timer_wake_depth=0\nprocess_timer_expirations=0\nprocess_timer_late_us_max=0\nprocess_timer_work_budget=64\nclockevent=x2apic-oneshot\n'
        def tree(mode):
            root=scratch/mode
            root.with_suffix('.clock').write_text(clock)
            return root
        for mode in ('validators','positive','eintr','cancel','child-refuse','bad-warm','bad-init','storm'):
            case_root=tree(mode)
            text=command('arm-'+mode,[binary,mode,case_root])
            check(mode+' actual compiled controller/audit/observer assertions',
                  re.search(r'ARM_CASE mode='+mode+r' checks=[1-9][0-9]* failures=0\n',text)is not None)
            if mode=='positive':
                shutil.copyfile(case_root/'host-poweroff.txt',out/'actual-host-poweroff.txt')
                (case_root/'host-poweroff.txt').unlink()
                result=codec.decode_complete_arm(case_root,'0123456789abcdef0123456789abcdef',audit_source=audit,suite_source=suite,uid=os.getuid(),gid=os.getgid())
                check('actual Host raw234/initial/consumed/controller/observer joins',result['init_exit']['sequence']>result['init_start']['sequence'])
                controls=[('consumed-readiness',case_root/'init-exit.bin',lambda raw:raw[:64+6*256+68]+(1).to_bytes(4,'little')+raw[64+6*256+72:]),
                          ('raw-completion-CR',case_root/'completion.txt',lambda raw:raw[:-1]+b'\r'),
                          ('source-work-fallback',case_root/'warmup/logs/make_large_file.log',lambda raw:raw.replace(b'using /dev/zero',b'using /dev/none')),
                          ('phase-log-missing-case',case_root/'warmup/phase.log',lambda raw:raw.replace(b'PASS shell_validate (0s)\n',b''))]
                for label,path,mutate in controls:
                    old=path.read_bytes();path.write_bytes(mutate(old))
                    try:
                        codec.decode_complete_arm(case_root,'0123456789abcdef0123456789abcdef',audit_source=audit,suite_source=suite,uid=os.getuid(),gid=os.getgid())
                    except (ValueError,UnicodeError,RuntimeError):check('actual saved decoder rejects '+label,True)
                    else:raise AssertionError('decoder accepted '+label)
                    finally:path.write_bytes(old)

        for name,source,old,new,mode,object_label in (
            ('missing-child-profile',owner,'if (!arm::process_admitted(expected_parent))','if (false)','child-refuse','owner'),
            ('wrong-daemon-readiness',support[0],'row.readiness != init::ReadinessState::READY','row.readiness != init::ReadinessState::NOT_READY','bad-init','arm')):
            text=source.read_text()
            check(name+' one actual control site',text.count(old)==1)
            changed=out/(name+'.cpp');changed.write_text(text.replace(old,new))
            obj=scratch/(name+'.o')
            command('compile-'+name,common+['-Wno-unused-parameter','-c',changed,'-o',obj])
            mutant=scratch/name
            linked=[obj if p.name==object_label+'.o'else p for p in objects]
            command('link-'+name,[args.cxx]+linked+['-o',mutant])
            refused=command('reject-'+name,[mutant,mode,tree(name)],expected=1)
            check(name+' compiled real oracle rejects','FAIL actual 'in refused)
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
        receipt = dict(qualification='PASS_ACTUAL_HOST_WHOLE_ARM_COMPONENTS_ONLY' if not errors else 'FAIL_HOST_ARM_COMPONENTS',
                       errors=errors, checks=checks, commands=commands, source_before=before, source_after=after,
                       emitted_inputs_before=emitted_before, emitted_inputs_after=emitted_after,
                       seconds=time.monotonic() - started, scratch_removed=removed, active=list(active), uncertain=uncertain,
                       limits=['Linux waitid is an explicit Host adaptation, not actual WOS waitpid runtime.',
                               'Host raw234 inputs test actual parser/sequence/profile/observer mechanics; no full117 work or WOS/init/runtime qualification.',
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
