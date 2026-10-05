#!/usr/bin/env python3
"""Compile actual launcher sources; adapt only WOS ABI/proc endpoints on Linux."""
import argparse
import json
import os
from pathlib import Path
import re
import shutil
import signal
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[3]
SRC = ROOT / 'modules/wkictl/src'
SOURCES = ('launch_main.cpp', 'launch_dispatch.cpp', 'launch.cpp', 'forward.cpp', 'cli.cpp', 'wosid.cpp')


def function(source, name):
    start = source.index('inline int64_t ' + name + '(')
    brace = source.index('{', start)
    end, depth = brace + 1, 1
    while depth:
        depth += (source[end] == '{') - (source[end] == '}')
        end += 1
    return source[start:end]


def prepare(folder):
    include = folder / 'include'
    (include / 'sys').mkdir(parents=True)
    (include / 'abi-bits').mkdir()
    (include / 'bits').mkdir()
    process = (ROOT / 'toolchain/sysroot/usr/include/sys/process.h').read_text()
    flags = re.findall(r'^constexpr uint32_t WKI_TARGET_FLAG_\w+ = .*?;', process, re.M)
    assert len(flags) == 6
    header = '#pragma once\n#include <cstdint>\n#include <fcntl.h>\n#include <unistd.h>\n'
    header += 'int record(const char*, const char*, uint64_t, uint32_t);\nnamespace ker::process {\n'
    header += '\n'.join(flags)
    header += '\ninline int64_t setwkitarget(const char* h,uint64_t n,uint32_t f){return record("policy",h,n,f);}\n'
    header += 'inline uint64_t getpid(){return static_cast<uint64_t>(::getpid());}\n'
    header += function(process, 'wki_launcher_node') + '\n}\n'
    (include / 'sys/process.h').write_text(header)
    (include / 'sys/vfs.h').write_text('''#pragma once
#include <cstdint>
int record(const char*, const char*, uint64_t, uint32_t);
namespace ker::abi::vfs {
constexpr uint32_t WKI_VFS_ROUTE_LOCAL=0;
constexpr uint32_t WKI_VFS_ROUTE_HOST=1;
inline int wki_rule_add_vfs(const char* p,uint32_t r){return record("rule",p,0,r);}
inline int wki_rule_clear_vfs(){return record("clear",nullptr,0,0);}
}
''')
    real_vfs = (ROOT / 'toolchain/sysroot/usr/include/sys/vfs.h').read_text()
    assert 'constexpr uint32_t WKI_VFS_ROUTE_LOCAL = 0' in real_vfs
    assert 'constexpr uint32_t WKI_VFS_ROUTE_HOST = 1' in real_vfs
    (include / 'abi-bits/fcntl.h').write_text('#pragma once\n#include <fcntl.h>\n')
    (include / 'bits/ssize_t.h').write_text('#pragma once\n#include <sys/types.h>\n')
    seams = folder / 'seams.cpp'
    seams.write_text(r'''
#include <cerrno>
#include <cstdarg>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <string>
extern "C" int __real_open(const char*,int,...);
extern "C" int __real_execvp(const char*,char* const[]);
int record(const char* kind,const char* text,uint64_t length,uint32_t flags) {
    const char* path=std::getenv("WKI_TEST_LOG");
    if(path) {
        FILE* out=std::fopen(path,"a");
        if(!out) std::abort();
        std::fprintf(out,"%s\t%s\t%llu\t%u\n",kind,text?text:"<null>",
                     static_cast<unsigned long long>(length),flags);
        std::fclose(out);
    }
    static unsigned int calls=0;
    if(std::strcmp(kind,"exec")==0) return 0;
    ++calls;
    const char* fail=std::getenv("WKI_TEST_FAIL_AT");
    return fail && calls==std::strtoul(fail,nullptr,10) ? -EPERM : 0;
}
extern "C" int __wrap_execvp(const char* file,char* const argv[]) {
    record("exec",file,0,0);
    return __real_execvp(file,argv);
}
extern "C" int __wrap_open(const char* path,int flags,...) {
    mode_t mode=0;
    if(flags&O_CREAT) {va_list args;va_start(args,flags);mode=va_arg(args,int);va_end(args);}
    const char* dir=std::getenv("WKI_TEST_PROC");
    if(dir && std::strncmp(path,"/proc/self/wki_",15)==0) {
        std::string mapped=std::string(dir)+"/"+path+11;
        return __real_open(mapped.c_str(),flags,mode);
    }
    return __real_open(path,flags,mode);
}
'''.replace('std::string(dir)+"/"+path+11', 'std::string(dir)+"/"+(path+11)'))
    child = folder / 'child'
    child.write_text(f'''#!{sys.executable}
import json,os,signal,sys
print(json.dumps(dict(args=sys.argv[1:],env=os.environ.get('WKI_TEST_PAYLOAD'),pid=os.getpid(),blocked=int(signal.SIGUSR1) in signal.pthread_sigmask(signal.SIG_BLOCK,[]))),flush=True)
print(sys.stdin.read(),end='',flush=True)
print('child-stderr',file=sys.stderr)
if os.environ.get('WKI_TEST_SIGNAL'):
 os.kill(os.getpid(),signal.SIGTERM)
raise SystemExit(23)
''')
    child.chmod(0o755)
    proc = folder / 'proc'
    proc.mkdir()
    for name, value in [('wki_launcher', 'launcher-node\n'), ('wki_runner', 'runner-node\r\n'), ('wki_remote_pid', '910\n')]:
        (proc / name).write_text(value)
    return include, seams, child, proc


def compile_binary(folder, include, seams, overrides=None, name='actual'):
    compiler = os.environ.get('WOS_HOST_CXX') or shutil.which('g++', path='/usr/bin:/bin')
    assert compiler
    inputs = [str((overrides or {}).get(s, SRC / s)) for s in SOURCES]
    binary = folder / name
    command = [compiler, '-std=c++23', '-O1', '-g', '-Wall', '-Wextra', '-Werror',
               '-fsanitize=address,undefined', '-fno-omit-frame-pointer', '-fno-pie', '-no-pie',
               '-I' + str(include), '-I' + str(ROOT / 'modules/wkictl/include'),
               *inputs, str(seams), '-Wl,--wrap=open', '-Wl,--wrap=execvp', '-o', str(binary)]
    result = subprocess.run(command, capture_output=True, text=True)
    (folder / (name + '-compile.log')).write_text(result.stdout + result.stderr)
    assert result.returncode == 0, result.stderr
    return binary


def suite(binary, folder, child, proc, only=None):
    count = 0

    def run(persona, args, effects, status=23, error=None, child_expected=True, fail=None, blocked=False, term=False):
        nonlocal count
        log = folder / 'effects.log'
        log.unlink(missing_ok=True)
        env = dict(os.environ, WKI_TEST_LOG=str(log), WKI_TEST_PROC=str(proc),
                   WKI_TEST_PAYLOAD='spaces ; $() preserved', PATH=str(folder) + ':' + os.environ.get('PATH', ''))
        env.pop('WKI_TEST_FAIL_AT', None)
        env.pop('WKI_TEST_SIGNAL', None)
        if fail:
            env['WKI_TEST_FAIL_AT'] = str(fail)
        if term:
            env['WKI_TEST_SIGNAL'] = '1'
        preexec = (lambda: signal.pthread_sigmask(signal.SIG_BLOCK, [signal.SIGUSR1])) if blocked else None
        p = subprocess.Popen(['/usr/bin/' + persona, *map(str, args)], executable=str(binary),
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                             env=env, text=True, preexec_fn=preexec)
        out, err = p.communicate('stdin preserved\n', timeout=10)
        actual_effects = [line.split('\t') for line in log.read_text().splitlines()] if log.exists() else []
        assert actual_effects == [[str(x) for x in row] for row in effects], (persona, args, actual_effects, effects)
        assert p.returncode == status, (persona, p.returncode, status, err)
        assert 'AddressSanitizer' not in err and 'runtime error:' not in err, err
        if child_expected:
            lines = out.splitlines()
            data = json.loads(lines[0])
            assert data == dict(args=['space value', 'semi;colon', '$(unchanged)'],
                                env='spaces ; $() preserved', pid=p.pid, blocked=blocked), data
            assert lines[1:] == ['stdin preserved'] and err == 'child-stderr\n', (out, err)
        else:
            if error == 'usage':
                assert not out and err.startswith('usage:\n  locally <command>') and err.endswith('wkictl wosid\n'), (out, err)
            elif error is not None:
                assert not out and err == error, (out, err)
        count += 1
        return out, err, p.pid

    payload = [str(child), 'space value', 'semi;colon', '$(unchanged)']
    ex = ('exec', str(child), 0, 0)
    cases = [('locally', [], '<null>', 0, 2), ('remotely', [], '<null>', 0, 8),
             ('anywhere', [], '<null>', 0, 16), ('homeward', [], 'launcher-node', 13, 1),
             ('on', ['target-host'], 'target-host', 11, 1)]
    for persona, prefix, host, length, flags in cases:
        if only and persona != only:
            continue
        policy = ('policy', host, length, flags)
        run(persona, prefix + payload, [policy, ex])
        message = {'locally': 'locally: failed to set local policy: -1\n',
                   'remotely': 'remotely: failed to set remote-preferred policy: -1\n',
                   'anywhere': 'anywhere: failed to set balanced policy: -1\n',
                   'homeward': "homeward: failed to target launcher 'launcher-node': -1\n",
                   'on': "on: failed to target 'target-host': -1\n"}[persona]
        run(persona, prefix + payload, [policy], 1, message, False, fail=1)
        run(persona, prefix + ['missing-v52-command'], [policy, ('exec', 'missing-v52-command', 0, 0)],
            127, 'missing-v52-command: exec failed: No such file or directory\n', False)
        run(persona, prefix + ['child', *payload[1:]], [policy, ('exec', 'child', 0, 0)])
        run(persona, prefix + payload, [policy, ex], blocked=True)
        run(persona, prefix + payload, [policy, ex], -signal.SIGTERM, term=True)
        run(persona, [], [], 1, 'usage', False)
    if only:
        return count
    run('on', ['only-host'], [], 1, 'usage', False)
    for text in ['', '\n']:
        (proc / 'wki_launcher').write_text(text)
        run('homeward', payload, [], 1, 'homeward: failed to resolve launcher node\n', False)
    (proc / 'wki_launcher').unlink()
    run('homeward', payload, [], 1, 'homeward: failed to resolve launcher node\n', False)
    (proc / 'wki_launcher').write_text('launcher-node\n')
    run('forward', payload, [ex])
    for policy, host, length, flags in [('local', '<null>', 0, 2), ('remote', '<null>', 0, 8),
                                      ('remotely', '<null>', 0, 8), ('balanced', '<null>', 0, 16),
                                      ('anywhere', '<null>', 0, 16), ('auto', '<null>', 0, 0),
                                      ('target-host', 'target-host', 11, 1)]:
        run('forward', ['--target', policy, *payload], [('policy', host, length, flags), ex])
    run('forward', ['--target', 'local', '--one-shot', *payload], [('policy', '<null>', 0, 34), ex])
    run('forward', ['--target', 'auto', '--one-shot', *payload], [], 1,
        "forward: failed to set 'auto' placement policy: -22\n", False)
    run('forward', ['--one-shot', *payload], [], 1, 'forward: --one-shot requires --target\n', False)
    run('forward', ['--target'], [], 1, 'forward: --target requires a placement policy\n', False)
    plus, minus = str(folder / 'include-path'), str(folder / 'exclude-path')
    clear = ('clear', '<null>', 0, 0)
    rules = [('rule', plus, 0, 1), ('rule', minus, 0, 0)]
    forward_args = ['--clear', '--target', 'local', '+' + plus, '-' + minus, '--', *payload]
    run('forward', forward_args, [clear, *rules, ('policy', '<null>', 0, 2), ex])
    run('forward', forward_args, [clear], 1, 'forward: failed to clear inherited VFS rules: -1\n', False, fail=1)
    run('forward', forward_args, [clear, rules[0]], 1,
        f"forward: failed to add host rule for '{plus}': -1\n", False, fail=2)
    run('forward', forward_args, [clear, *rules, ('policy', '<null>', 0, 2)], 1,
        "forward: failed to set 'local' placement policy: -1\n", False, fail=4)
    files = [folder / 'glob-a', folder / 'glob-b']
    for p in files:
        p.touch()
    run('forward', ['+' + str(folder / 'glob-*'), *payload],
        [('rule', str(p), 0, 1) for p in files] + [ex])
    run('forward', ['+' + str(folder / 'missing-*'), *payload], [], 1,
        f"forward: no matches for '{folder / 'missing-*'}'\n", False)
    run('forward', [], [], 1, 'usage', False)
    out, err, pid = run('wosid', [], [], 0, child_expected=False)
    assert not err and out == f'spawner=launcher-node host=runner-node pid={pid} remote_pid=910\n', out
    run('wki-run', [], [], 1, 'usage', False)
    return count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--out-dir', type=Path)
    args = parser.parse_args()
    temp = None
    if args.out_dir:
        folder = args.out_dir.resolve()
        folder.mkdir(parents=True, exist_ok=True)
    else:
        temp = tempfile.TemporaryDirectory(prefix='wki-launcher-test-')
        folder = Path(temp.name)
    include, seams, child, proc = prepare(folder)
    binary = compile_binary(folder, include, seams)
    cases = suite(binary, folder, child, proc)
    source = (SRC / 'launch.cpp').read_text()
    mutations = {
        'wrong_local_flag': source.replace('setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_LOCAL)',
                                           'setwkitarget(nullptr, 0, ker::process::WKI_TARGET_FLAG_BALANCED)'),
        'shift_command_argv': source.replace('return exec_command(argv + 1);', 'return exec_command(argv + 2);', 1),
        'ignore_policy_failure': source.replace('return 1;\n    }\n    return exec_command(argv + 1);',
                                                'return exec_command(argv + 1);\n    }\n    return exec_command(argv + 1);', 1),
    }
    rejected = []
    for name, changed in mutations.items():
        assert changed != source
        path = folder / (name + '.cpp')
        path.write_text(changed)
        mutant = compile_binary(folder, include, seams, {'launch.cpp': path}, name)
        try:
            suite(mutant, folder, child, proc, only='locally')
        except (AssertionError, ValueError, IndexError) as failure:
            rejected.append(name)
            (folder / (name + '-rejected.txt')).write_text(repr(failure) + '\n')
        else:
            raise AssertionError('semantic mutant survived: ' + name)
    proof = dict(status='ACTUAL_SHARED_LAUNCHER_RUNTIME_PASS', cases=cases, semantic_mutants_rejected=rejected,
                 sources=list(SOURCES), scope='Actual launcher production files compiled under Linux ASan/UBSan. WOS policy/VFS syscall endpoints are explicit recording seams. Actual installed launcher-node helper uses real bounded read/close via proc path adapter. Live exec exercises host PATH/argv/env/stdio/PID/blocked-mask/exit/signal semantics; WOS runtime and latency are not inferred.')
    (folder / 'proof.json').write_text(json.dumps(proof, indent=2) + '\n')
    print(json.dumps(proof))
    if temp:
        temp.cleanup()


if __name__ == '__main__':
    main()
