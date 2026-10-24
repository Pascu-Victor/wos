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
    init_root=args.root.resolve()/'modules/init/src'
    init_sources=[init_root/'services.cpp',init_root/'supervisor_model.h',init_root/'service_manifest.h',
                  source_root/'configs/wos-services-uma-reference.conf',init_root/'supervisor_model.cpp',init_root/'service_manifest.cpp']
    sources = [owner, header, catalog, cases, suite, Path(__file__), helper_path, baseline,abi,decoder,audit]+support+init_sources
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
        objects=[]
        units=[('owner',owner,None),('arm',support[0],None),('stamp',support[2],None),('host',support[3],None),
               ('controller',support[4],'uma_controller_main'),('observer',support[5],'uma_witness_main'),('cases',cases,None),
               ('aggregate',aggregate,None),('supervisor',init_root/'supervisor_model.cpp',None),('manifest',init_root/'service_manifest.cpp',None)]
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
            root.with_suffix('.services').write_bytes((source_root/'configs/wos-services-uma-reference.conf').read_bytes())
            return root
        for mode in ('validators','consumed','positive','pending-exec','eintr','cancel','child-refuse','bad-warm','bad-init','storm','pending-final','bad-final'):
            case_root=tree(mode)
            text=command('arm-'+mode,[binary,mode,case_root])
            check(mode+' actual compiled controller/audit/observer assertions',
                  re.search(r'ARM_CASE mode='+mode+r' checks=[1-9][0-9]* failures=0\n',text)is not None)
            if mode in ('pending-exec','pending-final'):
                if mode=='pending-exec':check('actual pending observer reaches final0', 'PENDING_EXEC_OBSERVER_EXIT=0\n'in text)
                shutil.copyfile(case_root/'host-poweroff.txt',out/'pending-host-poweroff.txt')
                (case_root/'host-poweroff.txt').unlink()
                codec.decode_complete_arm(case_root,'0123456789abcdef0123456789abcdef',audit_source=audit,suite_source=suite,uid=os.getuid(),gid=os.getgid())
                check('pending path publishes only final strict saved wire',True)
                if mode=='pending-final':check('actual pending finalizer reaches0','PENDING_FINALIZER_EXIT=0\n'in text)
            if mode=='positive':
                shutil.copyfile(case_root/'host-poweroff.txt',out/'actual-host-poweroff.txt')
                (case_root/'host-poweroff.txt').unlink()
                result=codec.decode_complete_arm(case_root,'0123456789abcdef0123456789abcdef',audit_source=audit,suite_source=suite,uid=os.getuid(),gid=os.getgid())
                check('actual Host raw234/initial/consumed/controller/observer joins',result['init_exit']['sequence']>result['init_start']['sequence'] and result['init_final']['sequence']>result['init_exit']['sequence'])
                controls=[('consumed-readiness',case_root/'init-exit.bin',lambda raw:raw[:64+6*256+68]+(1).to_bytes(4,'little')+raw[64+6*256+72:]),
                          ('raw-completion-CR',case_root/'completion.txt',lambda raw:raw[:-1]+b'\r'),
                          ('source-work-fallback',case_root/'warmup/logs/make_large_file.log',lambda raw:raw.replace(b'using /dev/zero',b'using /dev/none')),
                          ('phase-log-missing-case',case_root/'warmup/phase.log',lambda raw:raw.replace(b'PASS shell_validate (0s)\n',b''))]
                # Same actual saved decoder oracle, with exact ABI offsets.
                import struct
                def wire_field(raw,row,offset,fmt,value):
                    changed=bytearray(raw);struct.pack_into(fmt,changed,64+row*256+offset,value);return bytes(changed)
                final_raw=(case_root/'init-exit.bin').read_bytes()
                observer=struct.unpack_from('<q',final_raw,64+7*256+80)[0]
                controls.extend([
                    ('aggregate-running',case_root/'init-exit.bin',lambda raw:raw[:8]+(2).to_bytes(4,'little')+raw[12:]),
                    ('aggregate-stopping',case_root/'init-exit.bin',lambda raw:raw[:8]+(3).to_bytes(4,'little')+raw[12:]),
                    ('observer-PID',case_root/'init-exit.bin',lambda raw:wire_field(raw,7,80,'<q',observer+1)),
                    ('observer-generation',case_root/'init-exit.bin',lambda raw:wire_field(raw,7,96,'<Q',2)),
                    ('observer-restart',case_root/'init-exit.bin',lambda raw:wire_field(raw,7,104,'<Q',1)),
                    ('observer-deadline',case_root/'init-exit.bin',lambda raw:wire_field(raw,7,128,'<Q',struct.unpack_from('<Q',raw,64+7*256+128)[0]+1)),
                    ('observer-history',case_root/'init-exit.bin',lambda raw:wire_field(raw,7,176,'<I',4)),
                    ('other-pending',case_root/'init-exit.bin',lambda raw:wire_field(raw,0,64,'<I',1)),
                    ('other-failed',case_root/'init-exit.bin',lambda raw:wire_field(raw,0,64,'<I',7)),
                    ('other-restarted',case_root/'init-exit.bin',lambda raw:wire_field(raw,0,104,'<Q',1)),
                    ('keygen-pending',case_root/'init-exit.bin',lambda raw:wire_field(raw,1,64,'<I',3)),
                    ('testd-active',case_root/'init-exit.bin',lambda raw:wire_field(raw,5,64,'<I',3)),
                    ('controller-nonzero',case_root/'init-exit.bin',lambda raw:wire_field(raw,6,112,'<i',1)),
                    ('observer-text-join',case_root/'init-exit.txt',lambda raw:raw.replace((' observer='+str(observer)+' ').encode(),(' observer='+str(observer+1)+' ').encode())),
                ])
                controls.extend([
                    ('unstarted-finalizer-STARTING',case_root/'init-exit.bin',lambda raw:wire_field(raw,8,64,'<I',2)),
                    ('witness-still-running',case_root/'init-final.bin',lambda raw:wire_field(raw,7,64,'<I',3)),
                    ('witness-final-nonzero',case_root/'init-final.bin',lambda raw:wire_field(raw,7,112,'<i',1)),
                    ('witness-final-restart',case_root/'init-final.bin',lambda raw:wire_field(raw,7,104,'<Q',1)),
                    ('finalizer-already-exited',case_root/'init-final.bin',lambda raw:wire_field(raw,8,64,'<I',6)),
                    ('window-renewed-service-deadline',case_root/'witness-window.txt',lambda raw:re.sub(rb'deadline=[0-9]+',b'deadline='+str(struct.unpack_from('<Q',final_raw,64+7*256+128)[0]).encode(),raw)),
                    ('final-deadline-text-join',case_root/'init-final.txt',lambda raw:re.sub(rb'deadline=([0-9]+)',lambda m:b'deadline='+str(int(m[1])+1).encode(),raw)),
                ])
                for label,path,mutate in controls:
                    old=path.read_bytes();path.write_bytes(mutate(old))
                    try:
                        codec.decode_complete_arm(case_root,'0123456789abcdef0123456789abcdef',audit_source=audit,suite_source=suite,uid=os.getuid(),gid=os.getgid())
                    except (ValueError,UnicodeError,RuntimeError):check('actual saved decoder rejects '+label,True)
                    else:raise AssertionError('decoder accepted '+label)
                    finally:path.write_bytes(old)

                # Exercise the actual full decoder without changing the genuine
                # completion's retained-byte totals or any protocol record.
                codec_text=decoder.read_text()
                utf8_call='text=raw.decode("utf-8",errors="strict")'
                check('one actual UTF-8 conversion for prior-ASCII control',codec_text.count(utf8_call)==1)
                prior_spec=importlib.util.spec_from_file_location('uma_prior_ascii_decoder',decoder)
                prior_codec=importlib.util.module_from_spec(prior_spec)
                exec(compile(codec_text.replace(utf8_call,'text=raw.decode("ascii")'),str(decoder),'exec'),prior_codec.__dict__)
                for phase in ('warmup','measure'):
                    path=case_root/phase/'phase.log';original=path.read_bytes()
                    progress=b'PASS shell_validate (0s)\n'
                    sample='[perf] best=0.1µs'.encode('utf-8')
                    padding=b' '*(len(progress)-len(sample)-1)
                    token=sample+padding;prose=token+b'\n'
                    check(phase+' unique equal-byte prose seam',original.count(progress)==1 and len(prose)==len(progress) and len(padding)>=3)
                    valid=original.replace(progress,prose,1)
                    def adjusted(before,after):
                        check(phase+' unique structured mutation seam',valid.count(before)==1)
                        changed=valid.replace(before,after,1)
                        spaces=len(padding)-(len(changed)-len(valid))
                        check(phase+' compensation remains only prose padding',spaces>=0 and changed.count(token)==1)
                        changed=changed.replace(token,sample+b' '*spaces,1)
                        check(phase+' control retains exact completion byte count',len(changed)==len(original))
                        return changed
                    try:
                        path.write_bytes(valid)
                        decoded=codec.decode_complete_arm(case_root,'0123456789abcdef0123456789abcdef',audit_source=audit,suite_source=suite,uid=os.getuid(),gid=os.getgid())
                        check(phase+' valid UTF-8 prose preserves all raw234 and init joins',decoded==result)
                        try:
                            prior_codec.decode_complete_arm(case_root,'0123456789abcdef0123456789abcdef',audit_source=audit,suite_source=suite,uid=os.getuid(),gid=os.getgid())
                        except UnicodeDecodeError as error:check(phase+' prior ASCII conversion fails same UTF-8 positive',error.encoding=='ascii')
                        else:raise AssertionError('prior ASCII conversion accepted UTF-8 prose')
                        malformed=(('invalid-byte',valid.replace(prose,b'\xff'+prose[1:],1)),
                                   ('truncated-at-EOF',valid[:-2]+b'\xe2\x82'))
                        for label,bad in malformed:
                            check(phase+' '+label+' exact-byte control',len(bad)==len(original))
                            path.write_bytes(bad)
                            try:
                                codec.decode_complete_arm(case_root,'0123456789abcdef0123456789abcdef',audit_source=audit,suite_source=suite,uid=os.getuid(),gid=os.getgid())
                            except UnicodeDecodeError as error:
                                check(phase+' rejects '+label+' as strict UTF-8',error.encoding=='utf-8' and (label!='truncated-at-EOF' or error.end==len(bad)))
                            else:raise AssertionError('decoder accepted '+label)
                            finally:path.write_bytes(valid)
                        controls=[
                            ('Unicode-counts',adjusted(b'PASS=117 FAIL=0 SKIP=0', 'PASS=١١٧ FAIL=0 SKIP=0'.encode('utf-8')),'ASCII phase log summary'),
                            ('Unicode-count-space',adjusted(b'PASS=117 FAIL=0 SKIP=0','PASS=117\u00a0FAIL=0 SKIP=0'.encode('utf-8')),'ASCII phase log summary'),
                            ('Unicode-summary-detail',adjusted(b'shell_validate\tPASS\t0s','shell_validate\tPASS\tµs'.encode('utf-8')),'ASCII phase log summary'),
                            ('wrong-count',adjusted(b'PASS=117 FAIL=0 SKIP=0',b'PASS=116 FAIL=0 SKIP=0'),'count mismatch'),
                            ('missing-summary-row',adjusted(b'shell_validate\tPASS\t0s\n',b''),'missing case rows'),
                        ]
                        for separator in ('\u0085','\u2028','\u2029'):
                            controls.append(('Unicode-separator-'+hex(ord(separator)),adjusted(prose,token+separator.encode('utf-8')),'non-ASCII phase log line separator'))
                        for label,bad,reason in controls:
                            path.write_bytes(bad)
                            try:
                                codec.decode_complete_arm(case_root,'0123456789abcdef0123456789abcdef',audit_source=audit,suite_source=suite,uid=os.getuid(),gid=os.getgid())
                            except (ValueError,RuntimeError) as error:check(phase+' UTF-8 keeps strict '+label,reason in str(error))
                            else:raise AssertionError('decoder accepted '+label)
                            finally:path.write_bytes(valid)
                    finally:path.write_bytes(original)

        # The exact prior consumed body (only the new unused argument is
        # adapted) must FAIL the SAME composed active-observer positive.
        arm_text=support[0].read_text()
        a=arm_text.index('auto consumed(');b=arm_text.index('auto awaiting_exec(',a)
        previous_consumed='auto consumed(const init::StatusSnapshot& s, pid_t) -> bool {\n    init::ServiceStatus row{};\n    if (!snapshot_valid(s, row) || s.state != init::SupervisorState::RUNNING || row.state != init::ServiceState::EXITED ||\n        row.readiness != init::ReadinessState::READY || row.flags != 3 || row.generation != 1 || row.restart_count || row.pid != -1 ||\n        row.pgid != -1 || row.last_wait_status || row.last_error || row.deadline_mono_ns)\n        return false;\n    for (size_t i = 0; i < row.history_count; ++i)\n        if (row.history[i].reason == init::TransitionReason::EXIT && row.history[i].to_state == init::ServiceState::EXITED &&\n            row.history[i].timestamp_mono_ns >= row.state_since_mono_ns)\n            return true;\n    return false;\n}\n'
        for name,source,old,new,mode,object_label in (
            ('old-aggregate-premise',support[0],arm_text[a:b],previous_consumed,'consumed','arm'),
            ('old-pending-exec-guard',support[5],'!arm::awaiting_exec(final, ::getpid()) &&','','pending-exec','observer'),
            ('missing-child-profile',owner,'if (!arm::process_admitted(expected_parent))','if (false)','child-refuse','owner'),
            ('wrong-daemon-readiness',support[0],'row.readiness != init::ReadinessState::READY','row.readiness != init::ReadinessState::NOT_READY','bad-init','arm')):
            text=source.read_text()
            check(name+' one actual control site',text.count(old)==1)
            changed=out/(name+'.cpp');changed.write_text(text.replace(old,new))
            obj=scratch/(name+'.o')
            # The old-body control adds only an unnamed ABI argument. Keep
            # existing suppression solely for the two unchanged legacy controls.
            warnings=[] if name in('old-aggregate-premise','old-pending-exec-guard') else ['-Wno-unused-parameter']
            rename=['-Dmain=uma_witness_main'] if object_label=='observer' else []
            command('compile-'+name,common+warnings+rename+['-c',changed,'-o',obj])
            mutant=scratch/name
            linked=[obj if p.name==object_label+'.o'else p for p in objects]
            command('link-'+name,[args.cxx]+linked+['-o',mutant])
            refused=command('reject-'+name,[mutant,mode,tree(name+'-case')],expected=1)
            check(name+' compiled real oracle rejects','FAIL actual 'in refused)
            if name=='old-pending-exec-guard':
                check('actual old pending guard refuses82', 'PENDING_EXEC_OBSERVER_EXIT=82\n'in refused)
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
