#!/usr/bin/env python3
"""Fixed private49a sources using actual current Normal flags; never builds WOS."""
import argparse
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shlex
import shutil
import signal
import struct
import time
import traceback


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root',required=True,type=Path)
    parser.add_argument('--source-root',type=Path,default=Path(__file__).resolve().parents[2])
    parser.add_argument('--output-dir',required=True,type=Path)
    args=parser.parse_args();R=args.root.resolve();S=args.source_root.resolve();O=args.output_dir.resolve()
    hp=R/'tests/host/unit/file_mmap_registry_runtime_test.py'
    spec=importlib.util.spec_from_file_location('uma_build_owned_process',hp)
    helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper) # Before any owned resource.
    source=S/'tests/fixtures'
    db=R/'build/compile_commands.json';ninja=R/'build/build.ninja'
    units=[('owner','uma_reference_owner.cpp',True),('arm','uma_reference_arm.cpp',True),
           ('audit','uma_reference_stamp.cpp',True),('stamp','uma_reference_stamp.cpp',False),
           ('control','uma_reference_control.cpp',True),('witness','uma_reference_exit_witness.cpp',True)]
    source_paths=[source/name for _,name,_ in units]+[source/'uma_reference_arm.hpp',source/'uma_reference_owner.hpp',source/'uma_reference_catalog.hpp',Path(__file__)]
    def pin(path):
        path=Path(path).resolve();st=path.stat();raw=path.read_bytes();after=path.stat()
        if(st.st_dev,st.st_ino,st.st_size,st.st_mtime_ns,st.st_ctime_ns)!=(after.st_dev,after.st_ino,after.st_size,after.st_mtime_ns,after.st_ctime_ns):raise ValueError('input changed during hash')
        return dict(path=str(path),bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest())
    started=time.monotonic();commands=[];groups=[];errors=[];before=[];after=[];emitted=[];emitted_after=[];T=None;owned=False;artifacts=[]
    handled=(signal.SIGTERM,signal.SIGINT,signal.SIGHUP);old={sig:signal.getsignal(sig)for sig in handled}
    def interrupted(sig,_):raise KeyboardInterrupt('signal '+str(sig))
    def run(label,argv):
        previous=signal.pthread_sigmask(signal.SIG_BLOCK,handled)
        try:
            result=helper.command(list(map(str,argv)),O/(label+'.log'),T,started+60,groups)
        finally:signal.pthread_sigmask(signal.SIG_SETMASK,previous)
        result['label']=label;commands.append(result)
        if result['exit']!=0:raise RuntimeError(label+' returned '+str(result['exit']))
    try:
        for sig in handled:signal.signal(sig,interrupted)
        helper.subreaper()
        mask=signal.pthread_sigmask(signal.SIG_BLOCK,handled)
        try:O.mkdir(parents=True,exist_ok=False);owned=True;T=O/'tmp';T.mkdir()
        finally:signal.pthread_sigmask(signal.SIG_SETMASK,mask)
        row=next(r for r in json.loads(db.read_text())if r['file'].endswith('/modules/testprog/src/main.cpp'))
        words=shlex.split(row['command'])
        if words[0].endswith('/ccache'):words.pop(0)
        compile_flags=[];at=0
        while at<len(words):
            if words[at]in('-o','-MF','-MT','-MQ'):at+=2
            elif words[at]in('-c','-MD','-MMD','-MP')or words[at]==row['file']:at+=1
            else:compile_flags.append(words[at]);at+=1
        # Copy only this target's actual current linker variables, substituting
        # its dependency/output files into this fresh private directory.
        text=ninja.read_text();start=text.index('build modules/testprog/testprog:');block=text[start:text.index('\n# ===',start)]
        variables={line.strip().split(' = ',1)[0]:line.strip().split(' = ',1)[1]for line in block.splitlines()if' = 'in line}
        flags=shlex.split(variables['FLAGS'])
        link_flags=[v for v in shlex.split(variables['LINK_FLAGS'])if not v.startswith('--dependency-file=')]
        # -Xlinker belongs solely to that removed dependency-file operand.
        if link_flags and link_flags[-1]=='-Xlinker':link_flags.pop()
        libraries=shlex.split(variables['LINK_LIBRARIES'])
        if libraries!=['-lc++abi','-lc++','-lc','-lunwind']:raise ValueError('unexpected Normal testprog library profile')
        lib=R/'toolchain/sysroot/lib'
        prelink=[p for p in lib.iterdir()if p.name.startswith('crt')or p.name in('Scrt1.o','libc.so','libc++.so','libc++abi.so','libunwind.so','libm.so')]
        run('compiler-runtime',[compile_flags[0],'--sysroot='+str(R/'toolchain/sysroot'),'--print-libgcc-file-name'])
        runtime=Path((O/'compiler-runtime.log').read_text().strip()).resolve()
        if not runtime.is_relative_to((R/'toolchain/host').resolve()):raise ValueError('unexpected compiler runtime path')
        prelink.append(runtime)
        linker=R/'toolchain/host/bin/ld.lld'
        inputs=set(source_paths+[db,ninja,R/'build/CMakeCache.txt',hp,Path(compile_flags[0]),linker]+prelink)
        before=[pin(p)for p in sorted(inputs)]
        (O/'actual-normal-profile.json').write_text(json.dumps(dict(compile_row=row,link_variables=variables),indent=2)+'\n')
        closures={}
        for label,name,linked in units:
            extra=['-DWOS_UMA_LINKED_AUDIT=1']if linked else[]
            dep=O/(label+'-before.d')
            run(label+'-dependencies',compile_flags+extra+['-M','-MF',dep,'-MT','private-'+label,source/name])
            closures[label]=helper.dependency_pins(dep)
        flat={p['path']:p for rows in closures.values()for p in rows}
        emitted=[pin(p)for p in sorted(flat)]
        objects={}
        for label,name,linked in units:
            extra=['-DWOS_UMA_LINKED_AUDIT=1']if linked else[]
            obj=T/(label+'.o');objects[label]=obj
            run(label+'-compile',compile_flags+extra+['-MD','-MF',O/(label+'-actual.d'),'-c',source/name,'-o',obj])
            actual=helper.dependency_pins(O/(label+'-actual.d'))
            if actual!=closures[label]:raise ValueError('compile dependency change '+label)
        for name,labels in [('uma-reference-stamp',['stamp']),('uma-reference-control',['control','owner','arm','audit']),
                           ('uma-reference-exit-witness',['witness','owner','arm','audit'])]:
            out=O/name;dep=O/(name+'-link.d')
            argv=[compile_flags[0],'--sysroot='+str(R/'toolchain/sysroot')]+flags+link_flags+[objects[label]for label in labels]+[
                '-Xlinker','--dependency-file='+str(dep),'-o',out]+libraries
            run(name+'-link',argv)
            raw=dep.read_text().replace('\\\n','')
            rules=[line for line in raw.splitlines()if line.strip()]
            first=rules[0].split(':',1)
            if len(first)!=2 or first[0]!=str(out):raise ValueError('link dependency target')
            named=shlex.split(first[1])
            # lld emits empty phony rules after the one real target rule.
            if any(not line.endswith(':')or line[:-1]not in named for line in rules[1:]):raise ValueError('unexpected link dep rule')
            linked=[dict(path=str(Path(value).resolve()))for value in named]
            external=[]
            for item in linked:
                path=Path(item['path']).resolve()
                if path.parent==T:continue
                actual=pin(path)
                allowed={p['path']:p for p in before}
                if str(path)not in allowed or actual!=allowed[str(path)]:raise ValueError('unexpected or changed external link input '+str(path))
                external.append(actual)
            data=out.read_bytes()
            if data[:7]!=b'\x7fELF\x02\x01\x01':raise ValueError('ELF64 little-endian')
            e=struct.unpack_from('<HHIQQQIHHHHHH',data,16)
            if e[0]!=3 or e[1]!=62 or e[8]!=56 or not 1<=e[9]<=32:raise ValueError('x86_64 PIE')
            if e[4]>len(data)or e[9]*56>len(data)-e[4]:raise ValueError('program headers')
            ph=[struct.unpack_from('<IIQQQQQQ',data,e[4]+56*i)for i in range(e[9])]
            for p in ph:
                if p[2]>len(data)or p[5]>len(data)-p[2]or p[5]>p[6]or p[0]==1 and p[1]&3==3:raise ValueError('bounded nonWX LOAD')
            stacks=[p for p in ph if p[0]==0x6474e551]
            interp=[p for p in ph if p[0]==3]
            if len(stacks)!=1 or stacks[0][1]&1 or len(interp)!=1 or data[interp[0][2]:interp[0][2]+interp[0][5]]!=b'/lib/ld.so\0':raise ValueError('NX stack/canonical interpreter')
            dyn=[p for p in ph if p[0]==2]
            if len(dyn)!=1 or dyn[0][5]%16:raise ValueError('one aligned dynamic table')
            tags=[]
            for offset in range(dyn[0][2],dyn[0][2]+dyn[0][5],16):
                tag,value=struct.unpack_from('<QQ',data,offset)
                if tag==0:break
                tags.append((tag,value))
            else:raise ValueError('unterminated dynamic table')
            strings=[v for t,v in tags if t==5];sizes=[v for t,v in tags if t==10]
            if len(strings)!=1 or len(sizes)!=1 or not sizes[0]:raise ValueError('dynamic string table')
            mapped=[p for p in ph if p[0]==1 and p[3]<=strings[0] and strings[0]-p[3]<=p[5]and sizes[0]<=p[5]-(strings[0]-p[3])]
            if len(mapped)!=1:raise ValueError('unambiguous file-backed strings')
            offset=mapped[0][2]+strings[0]-mapped[0][3];table=data[offset:offset+sizes[0]];needed=[]
            for tag,value in tags:
                if tag!=1:continue
                if value>=len(table)or table.find(b'\0',value)<0:raise ValueError('bounded NEEDED')
                name=table[value:table.index(0,value)].decode('ascii')
                if not name or '/'in name:raise ValueError('plain dependency basename')
                needed.append(name)
            artifacts.append(dict(**pin(out),elf='ET_DYN_x86_64_NX_CANONICAL_INTERP',needed=needed,actual_external_link_inputs=external))
        emitted_after=[pin(row['path'])for row in emitted]
        if emitted_after!=emitted:raise ValueError('emitted header/source drift')
    except BaseException as error:
        errors.append(type(error).__name__+': '+str(error))
        if owned:
            try:(O/'failure.txt').write_text(traceback.format_exc())
            except BaseException as e:errors.append('failure evidence '+repr(e))
    finally:
        for sig in handled:signal.signal(sig,signal.SIG_IGN)
        try:
            after=[pin(p['path'])for p in before]
            if before!=after:errors.append('selected source/Normal/sysroot/compiler drift')
        except BaseException as e:errors.append('source recheck '+repr(e))
        removed=T is None
        if groups:errors.append('unproven owned groups; preserve scratch')
        elif T is not None:
            try:shutil.rmtree(T);removed=not T.exists()
            except BaseException as e:errors.append('scratch cleanup '+repr(e))
        result=dict(qualification='PASS_PRIVATE_ACTUAL_NORMAL_FLAGS_THREE_ELFS'if not errors else'FAIL_PRIVATE_ARM_BUILD',
            errors=errors,commands=commands,input_before=before,input_after=after,emitted_inputs=emitted,emitted_inputs_after=emitted_after,
            artifacts=artifacts,scratch_removed=removed,groups=groups,seconds=time.monotonic()-started,
            limits=['Fixed sources/current actual Normal flags and link variables only; no shared build or module registration.',
                    'No pre40 ABI smoke, actual full117 work, init consumption, natural shutdown or runtime qualification.',
                    'Private link objects removed after exact-owned child settlement; resulting smallELFs/commands/dependencies retained.'])
        try:
            if owned:(O/'result.json').write_text(json.dumps(result,indent=2)+'\n')
        finally:
            for sig,handler in old.items():signal.signal(sig,handler)
        print(json.dumps({k:result[k]for k in('qualification','errors','scratch_removed','groups','seconds')}))
    return bool(errors)

if __name__=='__main__':raise SystemExit(main())
