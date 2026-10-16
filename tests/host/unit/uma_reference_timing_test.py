#!/usr/bin/env python3
"""Portable actual stamp executable and extracted shell bridge, with owned cleanup."""
import argparse
from collections import Counter
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import resource
import shutil
import signal
import stat
import subprocess
import tempfile
import time
import traceback


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root',type=Path,default=Path(__file__).resolve().parents[3])
    parser.add_argument('--cxx',required=True,help='actual Host CMake CXX compiler; never infer the cross compiler from PATH')
    parser.add_argument('--evidence-dir',type=Path)
    args=parser.parse_args()
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
    root=args.source_root.resolve();fixture=root/'tests/fixtures/uma_reference_stamp.cpp'
    header=root/'tests/fixtures/uma_reference_catalog.hpp';suite=root/'configs/drive/srv/wos_userland_suite.sh';decoder=root/'tests/fixtures/validate_uma_reference.py'
    source=fixture.read_text();shell=suite.read_text()
    spec=importlib.util.spec_from_file_location('uma_reference_decoder',decoder)
    codec=importlib.util.module_from_spec(spec);spec.loader.exec_module(codec)
    functions=[]
    for name in ('record_summary','print_command','cat_log_file','status_file_value','run_case'):
        match=re.search(r'^'+name+r'\(\) \{\n.*?^}\n',shell,re.M|re.S)
        if not match:raise ValueError('missing actual shell '+name)
        functions.append(match.group())
    extracted='\n'.join(functions)
    process_helper_path=Path(__file__).resolve().parents[3]/'tests/host/unit/file_mmap_registry_runtime_test.py'
    process_spec=importlib.util.spec_from_file_location('uma_timing_process',process_helper_path)
    if process_spec is None or process_spec.loader is None:raise ValueError('missing maintained Host process helper')
    process_helper=importlib.util.module_from_spec(process_spec);process_spec.loader.exec_module(process_helper)
    before=[]
    def pin(path):
        path=Path(path).resolve();data=path.read_bytes();return dict(path=str(path),bytes=len(data),sha256=hashlib.sha256(data).hexdigest())
    before=[pin(p)for p in(fixture,header,suite,decoder,Path(__file__),process_helper_path)]
    out=None;scratch=None;output_owned=False
    started=time.monotonic();active={};commands=[];checks=[];errors=[];failures=[];uncertain=[]
    env=None
    handled=(signal.SIGINT,signal.SIGTERM,signal.SIGHUP)
    handlers={number:signal.getsignal(number)for number in handled}
    def interrupted(number,frame):raise KeyboardInterrupt('Host signal '+str(number))
    def check(name,passed):
        checks.append(dict(name=name,**{'pass':bool(passed)}))
        if not passed:raise AssertionError(name)
    def command(label,argv,expected=0,environment=None,limit=15):
        if time.monotonic()-started>110:raise TimeoutError('whole test budget')
        if sum(p.stat().st_blocks*512 for p in scratch.rglob('*')if p.is_file())>64<<20:raise RuntimeError('scratch budget')
        log=out/(label+'.log');then=time.monotonic();process=None
        def limits():
            signal.pthread_sigmask(signal.SIG_SETMASK,previous)
            resource.setrlimit(resource.RLIMIT_CORE,(0,0))
        with log.open('xb')as stream:
            try:
                previous=signal.pthread_sigmask(signal.SIG_BLOCK,handled)
                try:
                    process=subprocess.Popen(list(map(str,argv)),stdout=stream,stderr=subprocess.STDOUT,env=environment or env,start_new_session=True,preexec_fn=limits)
                    active[process.pid]=process
                finally:signal.pthread_sigmask(signal.SIG_SETMASK,previous)
                until=min(started+110,then+limit)
                while os.waitid(os.P_PID,process.pid,os.WEXITED|os.WNOHANG|os.WNOWAIT)is None:
                    if time.monotonic()>=until:raise TimeoutError('command budget '+label)
                    if log.stat().st_size>32<<20:raise RuntimeError('command log budget')
                    if sum(p.stat().st_blocks*512 for p in scratch.rglob('*')if p.is_file())>64<<20:raise RuntimeError('scratch budget')
                    time.sleep(.01)
            finally:
                if process is not None:
                    restore={number:signal.signal(number,signal.SIG_IGN)for number in handled}
                    try:
                        process_helper.reap_group(process,min(started+120,time.monotonic()+5))
                        active.pop(process.pid)
                    except BaseException:
                        # A consumed leader no longer reserves its numeric SID.
                        # Do not retry signaling that SID; preserve uncertainty.
                        if process.returncode is not None:
                            active.pop(process.pid,None);uncertain.append(process.pid)
                        raise
                    finally:
                        for number,handler in restore.items():signal.signal(number,handler)
        code=process.returncode;gone=True  # Actual reap_group returned absence proof.
        commands.append(dict(label=label,argv=list(map(str,argv)),exit=code,expected=expected,seconds=time.monotonic()-then,owned_group_absent=gone,leader_reaped=True,log=pin(log)))
        check(label+' exact exit and no process group',code==expected and gone)
        return log.read_text()
    def expect_refusal(name,call):
        try:call()
        except (ValueError,UnicodeError,FileNotFoundError):checks.append(dict(name=name,**{'pass':True}));return
        raise AssertionError(name+' accepted')
    def tree(label):
        base=scratch/label;base.mkdir(mode=0o700)
        phase=base/'warmup';phase.mkdir(mode=0o700)
        for n in('timing','logs','work'):(phase/n).mkdir(mode=0o700)
        return base,phase
    def bridge(binary,label,status=0,expect=0,mutate=None,body_command=None):
        base,phase=tree(label)
        script=scratch/(label+'.sh')
        text=extracted.replace('/usr/libexec/wos-uma-reference',str(binary))
        if mutate:text=mutate(text)
        prelude=f'''#!/bin/sh
umask 0022
PASS=0
FAIL=0
SKIP=0
LOG_DIR='{phase}/logs'
WORK_DIR='{phase}/work'
SUMMARY_FILE='{phase}/summary.tsv'
CASE_TIMEOUT_SECONDS=4
TIMEOUT_KILL_GRACE_SECONDS=1
WOS_SUITE_TIMING_VERSION=1
WOS_SUITE_TIMING_PHASE=warmup
WOS_SUITE_TIMING_NONCE=0123456789abcdef0123456789abcdef
WOS_SUITE_TIMING_DEADLINE_NS={time.monotonic_ns()+30_000_000_000}
'''
        if body_command:
            text += '\nbody_override() { '+str(body_command)+'; }\n'
        text=prelude+text+f'\nbody() {{ printf "BODY_DOLLAR=%s\\n" "$$"; return {status}; }}\nrun_case testprog_smoke {"body_override" if body_command else "body"}\nprintf "SUITE_PID=%s PASS=%s FAIL=%s\\n" "$$" "$PASS" "$FAIL"\n[ "$FAIL" = 0 ]\n'
        script.write_text(text)
        environment=dict(env,WOS_UMA_TEST_ROOT=str(base))
        output=command(label,['/bin/sh',script],expected=expect,environment=environment,limit=10)
        match=re.search(r'SUITE_PID=([0-9]+) PASS=([0-9]+) FAIL=([0-9]+)',output)
        check(label+' shell terminal facts',match is not None)
        saved=out/(label+'-records');saved.mkdir()
        for path in (phase/'timing').iterdir():
            if path.is_file()and not path.is_symlink()and path.stat().st_size<=768:shutil.copyfile(path,saved/path.name)
        return phase,int(match[1]),int(re.search(r'WOS_SUITE_TIMING_DEADLINE_NS=([0-9]+)',text)[1])
    try:
        for number in handled:signal.signal(number,interrupted)
        process_helper.subreaper()
        previous=signal.pthread_sigmask(signal.SIG_BLOCK,handled)
        try:
            if args.evidence_dir:
                out=args.evidence_dir.resolve();out.mkdir(parents=True,exist_ok=False)
            else:out=Path(tempfile.mkdtemp(prefix='uma-timing-evidence-'))
            output_owned=True
            scratch=Path(tempfile.mkdtemp(prefix='scratch-',dir=out))
        finally:signal.pthread_sigmask(signal.SIG_SETMASK,previous)
        env=dict(os.environ,TMPDIR=str(scratch),TMP=str(scratch),TEMP=str(scratch))
        binary=scratch/'stamp'
        common=[args.cxx,'-std=c++23','-O1','-g','-Wall','-Wextra','-Werror','-DWOS_UMA_TIMING_HOST=1','-I',str(header.parent)]
        command('compile-helper',common+[str(fixture),'-o',str(binary)])
        phase,suite_pid,deadline=bridge(binary,'actual-pass')
        blobs={kind:(phase/'timing'/('testprog_smoke.'+kind)).read_bytes()for kind in codec.LIMITS}
        row=codec.decode_case(blobs,'0123456789abcdef0123456789abcdef','warmup','testprog_smoke',deadline,suite_pid)
        check('real child PPIDs +$!+single decoded wait+positive clock join',row['elapsed_ns']>0)
        command('duplicate-BEGIN',[str(binary),'--begin','warmup','testprog_smoke','0123456789abcdef0123456789abcdef',str(deadline)],expected=126,environment=dict(env,WOS_UMA_TEST_ROOT=str(phase.parent)))
        # Synthetic full-catalogue codec vectors only: no117-case execution is claimed.
        catalogue,catalogue_phase=tree('codec-catalogue')
        codec_deadline=(1<<64)-1
        start=row['start_ns'];interval=row['elapsed_ns']
        for index,name in enumerate(codec.CASES):
            for kind,data in blobs.items():
                data=data.replace(b'case=testprog_smoke',b'case='+name.encode())
                if kind in ('begin','end'):
                    data=re.sub(rb' start=[0-9]+',b' start='+str(start+index*(interval+1)).encode(),data)
                if kind=='begin':data=re.sub(rb' deadline=[0-9]+',b' deadline='+str(codec_deadline).encode(),data)
                if kind=='end':data=re.sub(rb' end=[0-9]+',b' end='+str(start+index*(interval+1)+interval).encode(),data)
                path=catalogue_phase/'timing'/(name+'.'+kind);path.write_bytes(data);path.chmod(0o600)
        summary=catalogue_phase/'summary.tsv';summary.write_text(''.join(name+'\tPASS\t0s\n'for name in codec.CASES));summary.chmod(0o644)
        decoded=codec.decode_phase(catalogue_phase,'0123456789abcdef0123456789abcdef','warmup',codec_deadline,suite_pid,os.getuid(),os.getgid())
        check('synthetic codec complete117 and fixed11 metric catalogue',len(decoded['cases'])==117 and set(decoded['metrics'])==set(codec.METRICS)and len(decoded['metrics'])==11)
        extra=catalogue_phase/'timing/extra.begin';extra.write_bytes(b'x')
        expect_refusal('codec extra record refuses',lambda:codec.decode_phase(catalogue_phase,'0123456789abcdef0123456789abcdef','warmup',codec_deadline,suite_pid,os.getuid(),os.getgid()));extra.unlink()
        original_summary=summary.read_bytes();summary.write_bytes(original_summary.replace(b'PASS',b'SKIP',1))
        expect_refusal('codec SKIP refuses',lambda:codec.decode_phase(catalogue_phase,'0123456789abcdef0123456789abcdef','warmup',codec_deadline,suite_pid,os.getuid(),os.getgid()));summary.write_bytes(original_summary)
        measure_phase=catalogue/'measure';shutil.copytree(catalogue_phase,measure_phase)
        shift=117*(interval+1)+1
        for path in (measure_phase/'timing').iterdir():
            data=path.read_bytes().replace(b'phase=warmup',b'phase=measure')
            for field in (b'start',b'end'):
                data=re.sub(rb' '+field+rb'=([0-9]+)',lambda m:b' '+field+b'='+str(int(m[1])+shift).encode(),data)
            path.write_bytes(data)
        arm=codec.decode_arm(catalogue,'0123456789abcdef0123456789abcdef',codec_deadline,suite_pid,suite_pid,os.getuid(),os.getgid())
        check('synthetic full-arm timing enforces warm-before-measure and22 metric rows',len(arm['warmup']['metrics'])+len(arm['measure']['metrics'])==22)
        budget_files=[]
        for ph in (catalogue_phase,measure_phase):
            for index in range(2):
                path=ph/'logs'/('budget-'+str(index)+'.log')
                with path.open('wb')as stream:stream.truncate(16*1024*1024)
                budget_files.append(path)
        expect_refusal('whole-arm64MiB aggregate refuses while each phase fits',lambda:codec.decode_arm(catalogue,'0123456789abcdef0123456789abcdef',codec_deadline,suite_pid,suite_pid,os.getuid(),os.getgid()))
        for path in budget_files:path.unlink()
        oversize=catalogue_phase/'logs/oversize.log'
        with oversize.open('wb')as stream:stream.truncate(16777217)
        expect_refusal('codec retained-output cap refuses',lambda:codec.decode_phase(catalogue_phase,'0123456789abcdef0123456789abcdef','warmup',codec_deadline,suite_pid,os.getuid(),os.getgid()))
        # Real file/control mutations of saved bytes must not become qualification.
        for kind in codec.LIMITS:
            for separator in(b'\r\n',b'\r',b'\xc2\x85',b'\xe2\x80\xa8',b'\xe2\x80\xa9'):
                changed=dict(blobs);changed[kind]=blobs[kind][:-1]+separator
                expect_refusal('LF-only '+kind+' '+separator.hex(),lambda c=changed:codec.decode_case(c,'0123456789abcdef0123456789abcdef','warmup','testprog_smoke',deadline,suite_pid))
        expect_refusal('wrong actual suite pid',lambda:codec.decode_case(blobs,'0123456789abcdef0123456789abcdef','warmup','testprog_smoke',deadline,suite_pid+1))
        for kind in codec.LIMITS:
            changed=dict(blobs);del changed[kind]
            expect_refusal('missing '+kind,lambda c=changed:codec.decode_case(c,'0123456789abcdef0123456789abcdef','warmup','testprog_smoke',deadline,suite_pid))
        failed,sp,dl=bridge(binary,'actual-body-failure',status=7,expect=1)
        bad={k:(failed/'timing'/('testprog_smoke.'+k)).read_bytes()for k in codec.LIMITS}
        check('body failure really exits effective7 and consuming wait7',b'body=7 effective=7'in bad['end']and b'marker=7 decoded=7'in bad['wait'])
        expect_refusal('functional nonzero refuses metric',lambda:codec.decode_case(bad,'0123456789abcdef0123456789abcdef','warmup','testprog_smoke',dl,sp))
        # Genuine old default child leaves printf's status0 despite body7.
        old=extracted
        old=re.sub(r'^[ \t]*# UMA_TIMING_BEGIN [a-z_]+\n.*?^[ \t]*# UMA_TIMING_END [a-z_]+\n','',old,flags=re.M|re.S)
        control=scratch/'old-printf.sh';control.write_text('''#!/bin/sh
body() { return 7; }
( body; printf '%s\\n' "$?" >/dev/null ) &
pid=$!
wait "$pid"
printf 'OLD_DECODED=%s\\n' "$?"
''')
        check('genuine old printf child masks body7 as decoded0','OLD_DECODED=0'in command('old-printf-control',['/bin/sh',control]))
        # Actual BEGIN/END helper sees real subshell PID; $$ inside a subshell is the suite PID.
        check('actual PPID differs from actual subshell dollar-dollar',row['child']!=suite_pid and ('BODY_DOLLAR='+str(suite_pid))in(phase/'logs/testprog_smoke.log').read_text())
        bridge(binary,'wrong-dollar-controller',expect=1,mutate=lambda body:body.replace('\"$case_pid\" \"$$\"','\"$$\" \"$$\"'))
        mutations=[('missing-END','save_record(dir, c, "end", bytes.data(), static_cast<size_t>(n), END_LIMIT)','true'),
                   ('wrong-parent','static_cast<uint64_t>(::getppid()), 0, 0, c.deadline','static_cast<uint64_t>(::getppid())+1, 0, 0, c.deadline'),
                   ('body-status','static_cast<uint64_t>(::getppid()), 0, 0, 0, body','static_cast<uint64_t>(::getppid()), 0, 0, 0, 0')]
        for label,needle,replacement in mutations:
            check(label+' unique actual mutation',source.count(needle)==1)
            changed=scratch/(label+'.cpp');changed.write_text(source.replace(needle,replacement))
            mutated=scratch/(label+'-exe');command('compile-'+label,common+[str(changed),'-o',str(mutated)])
            changed_phase,_,_=bridge(mutated,label,status=7 if label=='body-status'else 0,expect=1)
            if label=='body-status':
                check('actual forged body0 rejects actual marker7/wait7',b'body=0 effective=0'in(changed_phase/'timing/testprog_smoke.end').read_bytes()and b'marker=7 decoded=7 timeout=0 joined=0'in(changed_phase/'timing/testprog_smoke.wait').read_bytes())
        # Real malformed CLI/file cases before any complete record can be credited.
        base,ph=tree('bad-context');environment=dict(env,WOS_UMA_TEST_ROOT=str(base))
        for label,nonce,dl in [('nonce','BAD',str(deadline)),('expired','0123456789abcdef0123456789abcdef','1')]:
            command('refuse-'+label,[str(binary),'--begin','warmup','testprog_smoke',nonce,dl],expected=126,environment=environment)
        (ph/'timing'/'testprog_smoke.begin').unlink(missing_ok=True)
        (ph/'timing'/'testprog_smoke.begin').symlink_to(ph/'work'/'outside')
        command('refuse-symlink',[str(binary),'--begin','warmup','testprog_smoke','0123456789abcdef0123456789abcdef',str(deadline)],expected=126,environment=environment)
        check('symlink target not created',not(ph/'work'/'outside').exists())
        # Actual FIFO/incomplete input and sparse oversized output fail without waiting.
        malformed,malformed_phase=tree('partial-record')
        malformed_env=dict(env,WOS_UMA_TEST_ROOT=str(malformed))
        good_deadline=time.monotonic_ns()+30_000_000_000
        prefix=[str(binary),'--begin','warmup','testprog_smoke','0123456789abcdef0123456789abcdef',str(good_deadline)]
        command('partial-BEGIN-create',prefix,environment=malformed_env)
        original=(malformed_phase/'timing/testprog_smoke.begin').read_bytes()
        (malformed_phase/'timing/testprog_smoke.begin').write_bytes(original[:-1])
        (malformed_phase/'logs/testprog_smoke.log').write_text('body')
        command('partial-BEGIN-refuse',[str(binary),'--end',*prefix[2:],'0'],expected=126,environment=malformed_env)
        (malformed_phase/'timing/testprog_smoke.begin').unlink()
        os.mkfifo(malformed_phase/'timing/testprog_smoke.begin',0o600)
        command('FIFO-BEGIN-refuse',[str(binary),'--end',*prefix[2:],'0'],expected=126,environment=malformed_env)
        large=scratch/'large-body.cpp';large.write_text('#include <unistd.h>\nint main(){return ftruncate(1,16777217)!=0;}\n')
        large_binary=scratch/'large-body'
        command('compile-large-body',[args.cxx,str(large),'-o',str(large_binary)])
        large_phase,_,_=bridge(binary,'output-too-large',expect=1,body_command=large_binary)
        check('real oversized log rejected before END',not(large_phase/'timing/testprog_smoke.end').exists()and(large_phase/'logs/testprog_smoke.log').stat().st_size==16777217)
        # Literal maxima use actual C++ formatter functions, not a Python protocol model.
        maximum=scratch/'maximum.cpp'
        maximum.write_text('#define main stamp_main\n#include "'+str(fixture)+'"\n#undef main\nint main(){using namespace uma_reference;Context c{"measure","testprog_perf_suite","ffffffffffffffffffffffffffffffff",UINT64_MAX};char b[1024];Begin x{UINT64_MAX,UINT64_MAX,UINT64_MAX,UINT64_MAX,UINT64_MAX};End y{UINT64_MAX,UINT64_MAX,UINT64_MAX,UINT64_MAX,UINT64_MAX,255};Parent z{UINT64_MAX,UINT64_MAX,UINT64_MAX};int a=format_begin(b,sizeof(b),c,x),d=format_end(b,sizeof(b),c,y),e=format_parent(b,sizeof(b),c,z),f=format_wait(b,sizeof(b),c,UINT64_MAX,UINT64_MAX,255,255,1,true);std::printf("MAX_BEGIN=%d MAX_END=%d MAX_PARENT=%d MAX_WAIT=%d\\n",a,d,e,f);return !(a<512&&d<768&&e<256&&f<256);}\n')
        command('compile-maxima',common+[str(maximum),'-o',str(scratch/'maximum')])
        command('literal-maxima',[str(scratch/'maximum')])
        command('shell-syntax',['/bin/sh','-n',str(suite)])
    except BaseException as error:
        errors.append(type(error).__name__+': '+str(error))
        if output_owned:
            try:(out/'failure.txt').write_text(traceback.format_exc())
            except BaseException as save_error:errors.append('failure evidence '+repr(save_error))
    finally:
        # No cancellation can bypass owned-child settlement once cleanup starts.
        for number in handled:signal.signal(number,signal.SIG_IGN)
        try:
            after=[pin(p['path'])for p in before]
            if before!=after:errors.append('source drift')
        except BaseException as error:errors.append('source recheck '+repr(error));after=[]
        for pid,process in list(active.items()):
            try:
                if process.returncode is not None:
                    active.pop(pid);uncertain.append(pid)
                    continue
                process_helper.reap_group(process,time.monotonic()+5)
                active.pop(pid)
            except BaseException as error:
                if process.returncode is not None:
                    active.pop(pid,None);uncertain.append(pid)
                errors.append('cleanup '+repr(error))
        removed=scratch is None
        if not active and not uncertain:
            if scratch is not None:
                try:shutil.rmtree(scratch);removed=not scratch.exists()
                except BaseException as error:errors.append('scratch '+repr(error))
        else:errors.append('owned groups uncertain; preserve scratch')
        receipt=dict(qualification='PASS_ACTUAL_HOST_CASE_STAMP_AND_SHELL_BRIDGE_ONLY'if not errors else'FAIL_HOST_CASE_TIMING',errors=errors,checks=checks,commands=commands,source_before=before,source_after=after,scratch_removed=removed,owned_groups_remaining=list(active),uncertain_consumed_sessions=uncertain,seconds=time.monotonic()-started,limits=['No WOS/kernel/controller/init/witness/runtime qualification.','Actual C++ helper and extracted run_case execute; Host prefix/UID are explicit compilation seams.','Abort relay/controller and whole117+117 phase execution remain unimplemented prerequisites.'])
        try:
            if output_owned:(out/'result.json').write_text(json.dumps(receipt,indent=2)+'\n')
        except BaseException as error:errors.append('receipt '+repr(error))
        finally:
            for number,handler in handlers.items():signal.signal(number,handler)
        print(json.dumps({k:receipt[k]for k in('qualification','errors','seconds','scratch_removed','owned_groups_remaining','uncertain_consumed_sessions')}))
        if not args.evidence_dir and not errors and output_owned:shutil.rmtree(out)
    return bool(errors)

if __name__=='__main__':raise SystemExit(main())
