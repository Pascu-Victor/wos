#!/usr/bin/env python3
"""Strict saved timing decoder. Caller must separately supply real controller/init joins."""
from pathlib import Path
import os
import re
import stat

CASES = (
    'shell_validate',
    'command_mix_001',
    'command_mix_002',
    'command_mix_003',
    'command_mix_004',
    'command_mix_005',
    'command_mix_006',
    'command_mix_007',
    'command_mix_008',
    'command_mix_009',
    'command_mix_010',
    'command_mix_011',
    'command_mix_012',
    'command_mix_013',
    'command_mix_014',
    'command_mix_015',
    'command_mix_016',
    'command_mix_017',
    'command_mix_018',
    'command_mix_019',
    'command_mix_020',
    'command_mix_021',
    'command_mix_022',
    'command_mix_023',
    'command_mix_024',
    'command_mix_025',
    'command_mix_026',
    'command_mix_027',
    'command_mix_028',
    'command_mix_029',
    'command_mix_030',
    'command_mix_031',
    'command_mix_032',
    'command_mix_033',
    'command_mix_034',
    'command_mix_035',
    'command_mix_036',
    'command_mix_037',
    'command_mix_038',
    'command_mix_039',
    'command_mix_040',
    'command_mix_041',
    'command_mix_042',
    'command_mix_043',
    'command_mix_044',
    'command_mix_045',
    'command_mix_046',
    'command_mix_047',
    'command_mix_048',
    'command_mix_049',
    'command_mix_050',
    'command_mix_051',
    'command_mix_052',
    'command_mix_053',
    'command_mix_054',
    'command_mix_055',
    'command_mix_056',
    'command_mix_057',
    'command_mix_058',
    'command_mix_059',
    'command_mix_060',
    'command_mix_061',
    'command_mix_062',
    'command_mix_063',
    'command_mix_064',
    'command_mix_065',
    'command_mix_066',
    'command_mix_067',
    'command_mix_068',
    'command_mix_069',
    'command_mix_070',
    'command_mix_071',
    'command_mix_072',
    'command_mix_073',
    'command_mix_074',
    'command_mix_075',
    'command_mix_076',
    'command_mix_077',
    'command_mix_078',
    'command_mix_079',
    'command_mix_080',
    'command_mix_081',
    'command_mix_082',
    'command_mix_083',
    'command_mix_084',
    'command_mix_085',
    'command_mix_086',
    'command_mix_087',
    'command_mix_088',
    'command_mix_089',
    'command_mix_090',
    'command_mix_091',
    'command_mix_092',
    'command_mix_093',
    'command_mix_094',
    'command_mix_095',
    'command_mix_096',
    'command_mix_097',
    'command_mix_098',
    'command_mix_099',
    'command_mix_100',
    'testd',
    'cat_bee_ipc',
    'testprog_smoke',
    'make_large_file',
    'vfsbench_read',
    'vfsbench_stat',
    'cp_large_file',
    'dd_large_file',
    'testprog_perf_suite',
    'mandelbench_large',
    'netbench_loopback',
    'renderbench_duck',
    'strace_vfsbench',
    'perf_stat',
    'perf_trace_vfs',
    'fork_cow',
)
METRICS = (
    'cat_bee_ipc',
    'make_large_file',
    'vfsbench_read',
    'vfsbench_stat',
    'cp_large_file',
    'dd_large_file',
    'testprog_perf_suite',
    'mandelbench_large',
    'netbench_loopback',
    'renderbench_duck',
    'fork_cow',
)
LIMITS = {"begin":512,"end":768,"parent":256,"wait":256}
PREFIX = {"begin":"UMA_BEGIN","end":"UMA_END","parent":"UMA_PARENT","wait":"UMA_WAIT"}
FIELDS = {
    "begin":("helper","child","start","res","deadline"),
    "end":("helper","child","start","end","res","body","effective","complete"),
    "parent":("helper","child","suite"),
    "wait":("child","suite","marker","decoded","timeout","joined"),
}

def record(data, kind, nonce, phase, case):
    if not isinstance(data,bytes) or not 0 < len(data) <= LIMITS[kind]:
        raise ValueError("record extent")
    text=data.decode("ascii",errors="strict")
    if "\r" in text or text.count("\n") != 1 or not text.endswith("\n"):
        raise ValueError("LF-only complete record required")
    prefix=f"{PREFIX[kind]} v=1 nonce={nonce} phase={phase} case={case}"
    pattern=re.escape(prefix)+"".join(r" "+name+r"=(0|[1-9][0-9]{0,19})" for name in FIELDS[kind])+r"\n"
    match=re.fullmatch(pattern,text,flags=re.ASCII)
    if not match: raise ValueError("record schema/identity")
    values={name:int(value) for name,value in zip(FIELDS[kind],match.groups())}
    if any(value > (1<<64)-1 for value in values.values()): raise ValueError("integer overflow")
    return values

def regular(path,limit,uid=0,gid=0,mode=0o600):
    def held(st):
        return (st.st_dev,st.st_ino,st.st_mode,st.st_uid,st.st_gid,st.st_nlink,st.st_size,st.st_mtime_ns,st.st_ctime_ns)
    before=path.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_uid!=uid or before.st_gid!=gid or before.st_nlink!=1 or stat.S_IMODE(before.st_mode)!=mode or before.st_size > limit:
        raise ValueError("file metadata/extent")
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC)
    with os.fdopen(fd,"rb") as stream:
        if held(os.fstat(stream.fileno())) != held(before): raise ValueError("file changed during open")
        data=stream.read(limit+1)
        if held(os.fstat(stream.fileno())) != held(before): raise ValueError("file changed during read")
    if held(path.lstat())!=held(before) or len(data)!=before.st_size: raise ValueError("file changed")
    return data

def decode_case(blobs,nonce,phase,case,deadline,suite):
    if not re.fullmatch(r"[0-9a-f]{32}",nonce) or phase not in ("warmup","measure") or case not in CASES or not 1<suite<1<<63 or not 0<deadline<1<<64:
        raise ValueError("admitted identity")
    if set(blobs)!=set(LIMITS): raise ValueError("missing/extra record")
    b,e,p,w=(record(blobs[k],k,nonce,phase,case) for k in ("begin","end","parent","wait"))
    child=b["child"]
    if not 1<child<1<<63 or child==suite or e["child"]!=child or p["child"]!=child or w["child"]!=child or p["suite"]!=suite or w["suite"]!=suite:
        raise ValueError("actual child/parent join")
    if any(not 1<r["helper"]<1<<63 or r["helper"] in (child,suite) for r in (b,e,p)):
        raise ValueError("helper pid")
    if not 0<b["start"]==e["start"]<e["end"]<b["deadline"]==deadline or not 0<b["res"]==e["res"]:
        raise ValueError("clock interval/deadline")
    if any(e[k]!=0 for k in ("body","effective")) or e["complete"]!=1 or any(w[k]!=0 for k in ("marker","decoded","timeout")) or w["joined"]!=1:
        raise ValueError("body/effective/decoded wait failure")
    return dict(case=case,child=child,suite=suite,start_ns=b["start"],end_ns=e["end"],elapsed_ns=e["end"]-b["start"],resolution_ns=b["res"],body_status=0,decoded_wait_status=0)

def decode_phase(root,nonce,phase,deadline,suite,uid=0,gid=0):
    root=Path(root);timing=root/"timing"
    for path in (root,timing,root/"logs"):
        info=path.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid!=uid or info.st_gid!=gid or stat.S_IMODE(info.st_mode)!=0o700: raise ValueError("directory ownership/type")
    expected={case+"."+kind for case in CASES for kind in LIMITS}
    if {p.name for p in timing.iterdir()}!=expected: raise ValueError("timing catalogue")
    rows=[];previous=0;transport=0
    for case in CASES:
        blobs={kind:regular(timing/(case+"."+kind),limit,uid,gid) for kind,limit in LIMITS.items()}
        row=decode_case(blobs,nonce,phase,case,deadline,suite)
        if row["start_ns"] <= previous: raise ValueError("case chronology")
        previous=row["end_ns"];rows.append(row);transport+=sum(map(len,blobs.values()))
    if transport+32768 > 512*1024: raise ValueError("transport budget")
    outputs=0
    for path in root.rglob("*"):
        parts=path.relative_to(root).parts
        if parts[0] in ("timing","work"): continue
        info=path.lstat()
        if stat.S_ISDIR(info.st_mode): continue
        if not stat.S_ISREG(info.st_mode) or info.st_uid!=uid or info.st_gid!=gid or info.st_nlink!=1 or info.st_size>16*1024*1024: raise ValueError("retained output metadata/budget")
        outputs+=info.st_size
    if outputs>64*1024*1024: raise ValueError("retained output aggregate")
    summary=regular(root/"summary.tsv",64*1024,uid,gid,0o644).decode("ascii")
    expected_summary=[case+"\tPASS\t" for case in CASES]
    lines=summary.split("\n")
    if lines[-1]!="" or len(lines)!=118 or any(not re.fullmatch(re.escape(prefix)+r"[0-9]+s",line) for prefix,line in zip(expected_summary,lines[:-1])):
        raise ValueError("full117/noSKIP summary")
    return dict(phase=phase,cases=rows,metrics={r["case"]:r["elapsed_ns"] for r in rows if r["case"] in METRICS},transport_bytes=transport,retained_output_bytes=outputs,
                qualification="CASE_TIMING_ONLY_REQUIRES_SEPARATE_CONTROLLER_INIT_RUNTIME_CLOSURE")

def decode_arm(root, nonce, deadline, warmup_suite, measure_suite, uid=0, gid=0):
    """Timing/output envelope only; actual raw suite/init waits remain caller gates."""
    root=Path(root)
    warmup=decode_phase(root/"warmup",nonce,"warmup",deadline,warmup_suite,uid,gid)
    measure=decode_phase(root/"measure",nonce,"measure",deadline,measure_suite,uid,gid)
    if warmup["cases"][-1]["end_ns"] >= measure["cases"][0]["start_ns"]:
        raise ValueError("whole warmup must precede measurement")
    if warmup["transport_bytes"]+measure["transport_bytes"]+32768 > 512*1024:
        raise ValueError("whole-arm transport budget")
    if warmup["retained_output_bytes"]+measure["retained_output_bytes"] > 64*1024*1024:
        raise ValueError("whole-arm retained output budget")
    return dict(warmup=warmup,measure=measure,qualification="TIMING_ONLY_REQUIRES_ACTUAL_CONTROLLER_INIT_AND_RUNTIME_CLOSURE")


# Whole-arm successor. Timing-only entrypoints above stay byte-exact. A result
# here qualifies saved guest evidence only: kernel/bundle identity, natural OS
# stop, Linux readback, image pruning and physical-zero remain outer gates.
SERVICES = ('journald','dropbear-keygen','netd','httpd','dropbear','testd',
            'uma-reference','uma-reference-exit-witness')
COMPLETE_NUMBERS = ('controller','warm','measure','init_deadline','work_stop',
                    'warm_first','warm_last','measure_first','measure_last','res',
                    'warm_transport','measure_transport','warm_output','measure_output')
META = {'init-start.bin':4160,'init-exit.bin':4160,'clock-before.txt':4095,
        'clock-after.txt':4095,'completion.txt':1535,'init-exit.txt':511}

def strict_meta(path,limit,uid,gid):
    # Reading may legitimately update atime. Every ownership/identity/content
    # field below remains held; atime is not an ownership or evidence mutation.
    def identity(st):
        return (st.st_dev,st.st_ino,st.st_mode,st.st_uid,st.st_gid,st.st_nlink,
                st.st_size,st.st_mtime_ns,st.st_ctime_ns)
    before=path.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_uid!=uid or before.st_gid!=gid or before.st_nlink!=1 or stat.S_IMODE(before.st_mode)!=0o600 or not 0<before.st_size<=limit:
        raise ValueError('whole-arm metadata file')
    fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_CLOEXEC)
    with os.fdopen(fd,'rb')as stream:
        if identity(os.fstat(stream.fileno()))!=identity(before):raise ValueError('metadata open race')
        raw=stream.read(limit+1)
        if identity(os.fstat(stream.fileno()))!=identity(before):raise ValueError('metadata read race')
    if len(raw)!=before.st_size or identity(path.lstat())!=identity(before):raise ValueError('metadata changed')
    return raw

def init_wire(data,final=False):
    import struct
    if len(data)!=4160:raise ValueError('complete4160 wire')
    h=struct.unpack_from('<4I2Q2I3Q',data)
    if h[0:2]!=(4160,1)or h[2]not in(1,2)or h[3]or not h[4]or not h[5]or h[6]!=8 or any(h[7:]):raise ValueError('eight-service init header')
    seen=set();selected=None;services={}
    for i in range(16):
        raw=data[64+i*256:64+(i+1)*256]
        if i>=8:
            if any(raw):raise ValueError('unused init row')
            continue
        f=struct.unpack_from('<64s4IqqQQiiQQII2Q',raw)
        if b'\0'not in f[0]:raise ValueError('name termination')
        name,padding=f[0].split(b'\0',1)
        if any(padding):raise ValueError('name padding')
        name=name.decode('ascii')
        if name not in SERVICES or name in seen:raise ValueError('service catalogue')
        seen.add(name)
        state,ready,flags,r0,pid,pgid,generation,restarts,wait,error,since,deadline,count,r1,r2,r3=f[1:]
        if not(1<=state<=8 and 1<=ready<=3 and not flags&~7 and not(r0 or r1 or r2 or r3)and 0<since<=h[5]and count<=4):raise ValueError('service wire fields')
        history=[];previous=0
        for j in range(4):
            t=struct.unpack_from('<Q4I',raw,160+24*j)
            if j>=count:
                if any(t):raise ValueError('unused history')
            else:
                stamp,before,after,reason,reserved=t
                if not(0<stamp<=h[5]and stamp>=previous and before<=8 and after<=8 and 1<=reason<=9 and reserved==0):raise ValueError('transition wire')
                previous=stamp;history.append(t)
        services[name]=dict(state=state,ready=ready,flags=flags,pid=pid,pgid=pgid,generation=generation,restarts=restarts,wait=wait,error=error,since=since,deadline=deadline,history=history)
        if name=='uma-reference':
            if final:
                if h[2]!=1 or(state,ready,flags,pid,pgid,generation,restarts,wait,error,deadline)!=(6,2,3,-1,-1,1,0,0,0,0)or not any(t[3]==5 and t[2]==6 and t[0]>=since for t in history):raise ValueError('actual consumed EXITED/READY/raw0')
            else:
                if(state,ready,flags,generation,restarts,wait,error)!=(3,1,3,1,0,0,0)or pid<=1 or pgid!=pid or deadline-since!=600_000_000_000 or not any(t[3]==3 and t[2]==3 and t[0]==since for t in history):raise ValueError('actual EXEC600s authority')
            selected=dict(pid=pid,deadline=deadline,since=since)
    if seen!=set(SERVICES)or selected is None:raise ValueError('complete init services')
    if final:
        # The live exit-witness is the sole unfinished oneshot behind STARTING.
        # All other rows must match the actual manifest's settled dispositions.
        def history(row,reason,state):
            return any(t[3]==reason and t[2]==state and t[0]==row['since']for t in row['history'])
        observer=services['uma-reference-exit-witness']
        observer_pid=observer['pid']
        if not 1<observer_pid<1<<63:raise ValueError('actual live observer PID')
        for name,row in services.items():
            if row['restarts']or row['wait']or row['error']or any(t[3]in(6,7,8,9)for t in row['history']):raise ValueError('unrestarted successful final services')
            if name=='uma-reference':continue
            if name=='uma-reference-exit-witness':
                if(row['state'],row['ready'],row['flags'],row['generation'],row['pgid'])!=(3,1,3,1,observer_pid)or row['since']<selected['since']or row['deadline']<=row['since']or row['deadline']-row['since']!=30_000_000_000 or h[5]>=row['deadline']or not any(t[3]==3 and t[1:3]==(2,3)and t[0]==row['since']for t in row['history']):raise ValueError('actual observer EXEC30s authority')
            elif name=='dropbear-keygen':
                if(row['state'],row['ready'],row['pid'],row['pgid'],row['deadline'])!=(6,2,-1,-1,0)or not((row['generation'],row['flags'])==(0,0)and history(row,2,6)or(row['generation'],row['flags'])==(1,1)and history(row,5,6)):raise ValueError('actual successful or skipped keygen')
            elif name=='testd':
                if(row['state'],row['ready'],row['flags'],row['generation'],row['pid'],row['pgid'],row['deadline'])!=(8,1,0,0,-1,-1,0)or not history(row,2,8):raise ValueError('actual disabled testd')
            elif(row['state'],row['ready'],row['flags'],row['generation'],row['deadline'])!=(3,2,3,1,0)or row['pid']<=1 or row['pgid']!=row['pid']or row['pid']==observer_pid or not(history(row,3,3)or history(row,4,3)):raise ValueError('actual ready daemon')
        selected['observer']=observer_pid
    return dict(sequence=h[4],published=h[5],**selected)

CLOCK_KEYS=('version','source','invariant_tsc','frequency_hz','resolution_ns','conversion_multiplier','conversion_divisor','calibration_spread_ppm','watchdog_deviation_ns','kvm_registered_cpus','kvm_expected_cpus','kvm_watchdog_checks','rtc_valid','wall_steps','wall_slews','wall_rejected','wall_pending_ns','slew_limit_ppm','process_timer_real_depth','process_timer_wake_depth','process_timer_expirations','process_timer_late_us_max','process_timer_work_budget','clockevent')
def clock_wire(raw,resolution):
    text=raw.decode('ascii',errors='strict')
    if '\r'in text or not text.endswith('\n'):raise ValueError('raw LF clock')
    lines=text[:-1].split('\n')
    if len(lines)!=len(CLOCK_KEYS):raise ValueError('clock catalogue')
    out={}
    for key,line in zip(CLOCK_KEYS,lines):
        if not line.startswith(key+'='):raise ValueError('clock field')
        value=line[len(key)+1:]
        if key not in('source','clockevent'):
            if not re.fullmatch(r'-?(0|[1-9][0-9]{0,19})'if key=='wall_pending_ns'else r'(0|[1-9][0-9]{0,19})',value):raise ValueError('clock decimal')
            value=int(value)
            if abs(value)>(1<<64)-1:raise ValueError('clock overflow')
        out[key]=value
    if out['version']!=1 or not out['frequency_hz']or out['resolution_ns']!=resolution or not resolution or not out['conversion_multiplier']or not out['conversion_divisor']or out['invariant_tsc']not in(0,1)or out['rtc_valid']not in(0,1)or out['slew_limit_ppm']!=500 or out['clockevent']!='x2apic-oneshot':raise ValueError('clock health/resolution')
    source=out['source']
    if source=='kvm-clock':
        if(out['kvm_registered_cpus'],out['kvm_expected_cpus'])!=(8,8):raise ValueError('KVM CPU registration')
    elif source=='tsc':
        if out['invariant_tsc']!=1:raise ValueError('TSC invariance')
    elif source not in('hpet:missing-tsc','hpet:noninvariant-tsc','hpet:calibration-failed','hpet:smp-unqualified'):raise ValueError('unhealthy/degraded clock')
    return out

def decode_complete_arm(root,nonce,*,audit_source,suite_source,uid=0,gid=0):
    import importlib.util
    import sys
    root=Path(root)
    metadata=root.lstat()
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid!=uid or metadata.st_gid!=gid or stat.S_IMODE(metadata.st_mode)!=0o700:raise ValueError('whole-arm root directory')
    if set(p.name for p in root.iterdir())!=set(META)|{'warmup','measure'}:raise ValueError('whole-arm root catalogue')
    blobs={name:strict_meta(root/name,limit,uid,gid)for name,limit in META.items()}
    if sum(map(len,blobs.values()))>32768:raise ValueError('metadata reserve')
    prefix='UMA_ARM v=1 nonce='+nonce
    pattern=re.escape(prefix)+''.join(r' '+name+r'=(0|[1-9][0-9]{0,19})'for name in COMPLETE_NUMBERS)+r' cases=234 metrics=11 warm_peek=0 warm_wait=0 measure_peek=0 measure_wait=0\n'
    m=re.fullmatch(pattern,blobs['completion.txt'].decode('ascii'),re.ASCII)
    if not m:raise ValueError('raw complete record')
    c=dict(zip(COMPLETE_NUMBERS,map(int,m.groups())))
    if any(v>(1<<64)-1 for v in c.values())or not 1<c['controller']<1<<63 or not 1<c['warm']<1<<63 or not 1<c['measure']<1<<63 or len({c[k]for k in('controller','warm','measure')})!=3 or c['init_deadline']-c['work_stop']!=30_000_000_000:raise ValueError('complete identities/deadline')
    initial=init_wire(blobs['init-start.bin']);final=init_wire(blobs['init-exit.bin'],True)
    if initial['pid']!=c['controller']or initial['deadline']!=c['init_deadline']or final['sequence']<=initial['sequence']or final['published']<c['measure_last']:raise ValueError('initial/consumed identity chronology')
    result=decode_arm(root,nonce,c['work_stop'],c['warm'],c['measure'],uid,gid)
    spec=importlib.util.spec_from_file_location('uma_actual_versioned_audit',audit_source)
    audit=importlib.util.module_from_spec(spec);sys.modules[spec.name]=audit;spec.loader.exec_module(audit)
    audit.USERLAND_SUITE=Path(suite_source)
    if audit.userland_expected_cases(userland_timing_version=1)!=set(CASES):raise ValueError('versioned source catalogue')
    for name in('warmup','measure'):
        row=result[name];short='warm'if name=='warmup'else'measure'
        if(c[short+'_first'],c[short+'_last'],c['res'],c[short+'_transport'],c[short+'_output'])!=(row['cases'][0]['start_ns'],row['cases'][-1]['end_ns'],row['cases'][0]['resolution_ns'],row['transport_bytes'],row['retained_output_bytes']):raise ValueError('completion must equal all actual raw rows/totals')
        if any(r['resolution_ns']!=c['res']for r in row['cases']):raise ValueError('all234 stable resolution')
        raw=regular(root/name/'phase.log',16<<20,uid,gid)
        audited=audit.audit_userland_text(raw.decode('ascii'),userland_timing_version=1)
        if(audited.passed,audited.failed)!=(117,0):raise ValueError('actual full117 phase log')
        work=regular(root/name/'logs/make_large_file.log',16<<20,uid,gid,0o644).decode('ascii')
        expected=f'created 67108864 bytes at /var/lib/uma-reference/{name}/work/large-input.bin using /dev/zero\n'
        if work.count(expected)!=1 or 'falling back'in work or 'copies of /srv/bee.txt'in work:raise ValueError('original64MiB zero work')
    clocks=[clock_wire(blobs[name],c['res'])for name in('clock-before.txt','clock-after.txt')]
    stable=('source','invariant_tsc','frequency_hz','resolution_ns','conversion_multiplier','conversion_divisor','calibration_spread_ppm','kvm_registered_cpus','kvm_expected_cpus')
    if any(clocks[0][key]!=clocks[1][key]for key in stable):raise ValueError('clock source/scale transition')
    status=re.fullmatch(re.escape(f'UMA_INIT_EXIT v=1 nonce={nonce} controller={c["controller"]}')+r' observer=([1-9][0-9]{0,18}) sequence='+str(final['sequence'])+r' state=6 readiness=2 wait=0 error=0 generation=1 restarts=0 history_exit=1 snapshot_bytes=4160\n',blobs['init-exit.txt'].decode('ascii'),re.ASCII)
    if not status or not 1<int(status.group(1))<1<<63 or int(status.group(1))!=final['observer']or int(status.group(1))in(c['controller'],c['warm'],c['measure']):raise ValueError('independent observer raw join')
    return dict(**result,controller=c,init_start=initial,init_exit=final,clock_before=clocks[0],clock_after=clocks[1],
                saved_guest_evidence='COMPLETE_REQUIRES_SEPARATE_BUNDLE_NATURAL_STOP_LINUX_READBACK_AND_PHYSICAL_ZERO')
