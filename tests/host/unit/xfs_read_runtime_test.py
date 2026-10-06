#!/usr/bin/env python3
"""Run complete extracted XFS read and real cache; only boundary types are mocked."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[3]
UNIT = Path(__file__).resolve().parent


def function(source, signature):
    start = source.index(signature)
    opening = source.index('{', start)
    # Removing comments/quoted strings preserves character positions. Numeric
    # separators remain code; the read bodies use no multi-character literals.
    clean = re.sub(r'//[^\n]*|/\*[\s\S]*?\*/|"(?:\\.|[^"\\])*"',
                   lambda match:' '*len(match[0]), source)
    depth = 0
    for index in range(opening, len(source)):
        if clean[index] == '{':
            depth += 1
        elif clean[index] == '}':
            depth -= 1
            if depth == 0:
                return source[start:index+1]
    raise ValueError(f'unterminated actual function: {signature}')


def write_generated(work, constants, extracted):
    """Emit identical boundary inputs for the Python and compiled CMake tests."""
    work.mkdir(parents=True, exist_ok=True)
    (work/'actual_xfs_read.inc').write_text('\n'.join(constants)+'\n'+extracted+'\n')
    perf = (ROOT/'tests/host/kern_shim/platform/perf/perf_events.hpp').read_text()
    actual_perf = (ROOT/'modules/kern/src/platform/perf/perf_events.hpp').read_text()
    enum = re.search(r'enum class WkiPerfLocalXfsOp[^}]*};', actual_perf)[0]
    perf = re.sub(r'enum class WkiPerfLocalXfsOp[^}]*};', lambda _:enum, perf)
    perf_path = work/'platform/perf/perf_events.hpp'
    perf_path.parent.mkdir(parents=True, exist_ok=True)
    perf_path.write_text(perf)
    # Add allocation rejection at the hosted physical boundary only.
    phys = (UNIT/'buffer_cache_async_shim/platform/mm/phys.hpp').read_text()
    phys = phys.replace('inline void (*allocation_probe)(size_t){};',
                        'inline void (*allocation_probe)(size_t){}; inline size_t reject_size{};')
    phys = phys.replace('if (size == 0) size = 1;',
                        'if (size != 0 && size == cache_test::reject_size) return nullptr; if (size == 0) size = 1;')
    phys_path = work/'platform/mm/phys.hpp'
    phys_path.parent.mkdir(parents=True, exist_ok=True)
    phys_path.write_text(phys)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--xfs-source', type=Path, default=ROOT/'modules/kern/src/vfs/fs/xfs/xfs_vfs.cpp')
    parser.add_argument('--cache-source', type=Path, default=ROOT/'modules/kern/src/vfs/buffer_cache.cpp')
    parser.add_argument('--cxx', default=os.environ.get('WOS_HOST_CXX', '/usr/bin/clang++'))
    parser.add_argument('--host-build', type=Path, default=ROOT/'build/goal30-host')
    parser.add_argument('--gtest-include', type=Path)
    parser.add_argument('--gtest-main', type=Path)
    parser.add_argument('--gtest-lib', type=Path)
    parser.add_argument('--original', action='store_true')
    parser.add_argument('--require-no-allocation', action='store_true',
                        help='assert new warm-copy behavior even when compiling the original control')
    parser.add_argument('--disabled', action='store_true', help='compile policy OFF and verify exact compatibility reads')
    parser.add_argument('--filter')
    parser.add_argument('--generate-only', type=Path,
                        help='emit actual read/perf/allocator inputs in DIR without compiling or running')
    args = parser.parse_args()
    selected_filter = args.filter or ('XfsDisabled.*' if args.disabled else 'XfsRead.*')
    source = args.xfs_source.read_text()
    signatures = ['auto xfs_read_batch_max_bytes(', 'auto xfs_fsblock_to_dev_block(', 'auto xfs_fsb_to_dev_count(']
    for signature in ['auto xfs_read_kernel_destination_eligible(', 'auto xfs_read_cluster_context_eligible(', 'auto xfs_read_cluster_bytes(']:
        if signature in source:
            signatures.append(signature)
    signatures.append('auto xfs_vfs_read(File*')
    extracted = '\n\n'.join(function(source, signature) for signature in signatures)
    constants = re.findall(r'^constexpr size_t XFS_READ_(?:BATCH_MAX|CACHE_COPY_MAX|CLUSTER_DEMAND_MAX|CLUSTER_MAX|CLUSTER_MIN)_BYTES[^;]*;', source, re.M)
    if not constants:
        raise ValueError('actual read constants missing')
    if args.generate_only is not None:
        write_generated(args.generate_only, constants, extracted)
        outputs = ['actual_xfs_read.inc', 'platform/perf/perf_events.hpp', 'platform/mm/phys.hpp']
        print(json.dumps({'generation_only':True, 'compiled':False, 'executed':False,
                          'full_read_source_sha256':hashlib.sha256(args.xfs_source.read_bytes()).hexdigest(),
                          'outputs':{path:hashlib.sha256((args.generate_only/path).read_bytes()).hexdigest()
                                     for path in outputs}}))
        return
    include = args.gtest_include
    if include is None:
        cache = (args.host_build/'CMakeCache.txt').read_text()
        match = re.search(r'^gtest_SOURCE_DIR:[^=]*=(.*)$', cache, re.M)
        if match is None:
            raise ValueError('supply actual googletest include path')
        include = Path(match[1])/'include'
    main_lib = args.gtest_main or args.host_build/'lib/libgtest_main.a'
    gtest_lib = args.gtest_lib or args.host_build/'lib/libgtest.a'
    with tempfile.TemporaryDirectory(prefix='xfs-read-runtime-', dir=os.environ.get('TMPDIR')) as directory:
        work = Path(directory)
        write_generated(work, constants, extracted)
        command = [args.cxx, '-std=c++23', '-O1', '-g', '-fsanitize=address,undefined', '-fno-sanitize=vptr',
                   '-fno-omit-frame-pointer', '-ffunction-sections', '-fdata-sections', '-Wl,--gc-sections',
                   '-pthread', '-DWOS_HOST_TEST=1']
        if not args.disabled:
            command.append('-DWOS_ASYNC_BLOCK_IO=1')
        if args.original or args.disabled:
            command.append('-DXFS_EXPECT_ORIGINAL=1')
        if args.require_no_allocation:
            command.append('-DXFS_EXPECT_NO_ALLOCATION=1')
        if args.disabled:
            command.append('-DXFS_EXPECT_DISABLED=1')
        for path in [work, UNIT/'xfs_read_runtime_shim', UNIT/'buffer_cache_async_shim', ROOT/'tests/host/kern_shim',
                     ROOT/'modules/kern/src', ROOT/'modules/kern/src/std', ROOT/'modules']:
            command.extend(['-I', str(path)])
        command.extend(['-isystem', str(include), str(UNIT/'xfs_read_runtime_test.cpp'), str(args.cache_source),
                        str(main_lib), str(gtest_lib), '-o', str(work/'test')])
        subprocess.run(command, check=True)
        env = dict(os.environ, ASAN_OPTIONS='detect_leaks=0', LSAN_OPTIONS='detect_leaks=0')
        subprocess.run([str(work/'test'), '--gtest_filter='+selected_filter], env=env, check=True)
        print(json.dumps({'full_read_source_sha256':hashlib.sha256(args.xfs_source.read_bytes()).hexdigest(),
                          'cache_source_sha256':hashlib.sha256(args.cache_source.read_bytes()).hexdigest(),
                          'test_source_sha256':hashlib.sha256((UNIT/'xfs_read_runtime_test.cpp').read_bytes()).hexdigest(),
                          'runner_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
                          'whole_read_extracted':True,'original_control':args.original,'policy_disabled':args.disabled,
                          'leak_detection':'disabled: kernel cache indices are permanent', 'filter':selected_filter}))


if __name__ == '__main__':
    main()
