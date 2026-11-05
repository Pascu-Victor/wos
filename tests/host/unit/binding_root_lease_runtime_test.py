#!/usr/bin/env python3
"""Compile unchanged root-lease/PUB/gate bodies with the actual binding TU.

The scheduler, IF/panic/access-window flags, mutex and pointer-key root-control
lookup are finite Host ports. The complete production family/binding/index TU
creates every snapshot; no raw snapshot adoption or mirrored admission predicate
is used. PROT_NONE roots check ordering without granting root-storage authority.
This qualifies Host metadata/control ownership, not native SMP, PTE, TLB ACK,
physical root storage, provider revoke, or executor callback-return observations.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import time

sys.dont_write_bytecode = True
from file_mmap_registry_runtime_test import command, pin, subreaper
from native_lazy_file_fault_runtime_test import definition

ROOT = next(path for path in Path(__file__).resolve().parents
            if (path / 'modules/kern/src').is_dir() and (path / 'tests/host').is_dir())
KERN = ROOT / 'modules/kern/src'
UNIT = Path(__file__).resolve().parent
SHIM = UNIT / 'file_binding_snapshot_shim'
PRELUDE = UNIT / 'binding_root_lease_prelude.inc'
CASES = UNIT / 'binding_root_lease_cases.inc'


def generate(virt: str, header: str, vmem: str, vmem_header: str, phys: str) -> tuple[str, list[dict]]:
    actual = []

    def take(source: str, anchor: str, declaration: bool = False) -> str:
        body, row = definition(source, anchor, declaration)
        actual.append(row)
        return body

    def between(source: str, begin: str, end: str) -> str:
        first = source.index(begin)
        last = source.index(end, first)
        body = source[first:last]
        # Use the same immutable-byte metadata format as complete definitions.
        import hashlib
        actual.append(dict(anchor=begin, bytes=len(body.encode()),
                           sha256=hashlib.sha256(body.encode()).hexdigest(),
                           line=source.count('\n', 0, first) + 1))
        return body

    sections = {
        'PUB_DECL': take(vmem_header, 'class SharedVmemPublicationGuard {', True),
        'PUB_STORAGE': between(vmem, 'ker::mod::sys::Mutex g_shared_vmem_publication_lock;',
                               '// Protected by blocking PUB;'),
        'PUB_MATCH': take(vmem, '[[nodiscard]] auto blocking_publication_owner_matches('),
        'PUB_CTOR': take(vmem, 'SharedVmemPublicationGuard::SharedVmemPublicationGuard()'),
        'PUB_DTOR': take(vmem, 'SharedVmemPublicationGuard::~SharedVmemPublicationGuard()'),
        'PUB_ACQUIRED': take(vmem, 'auto SharedVmemPublicationGuard::acquired()'),
        'PUB_EXECUTOR': take(vmem, 'auto SharedVmemPublicationGuard::acquired_by_current_executor()'),
        'CAN_WAIT_IMPL': take(phys, 'auto can_wait_for_reclaim_impl()'),
        'CAN_WAIT': take(phys, 'auto can_wait_for_reclaim()'),
        'LEASE_ENUM': take(header, 'enum class BindingRootAdmission :', True),
        'LEASE_DECL': take(header, 'class BindingRootMutationLease {', True),
        'GATE_STORAGE': between(virt, 'constexpr size_t USER_PAGEMAP_ACCESS_GATE_COUNT =',
                                '[[nodiscard]] auto user_pagemap_access_gate_index('),
        'GATE_INDEX': take(virt, '[[nodiscard]] auto user_pagemap_access_gate_index('),
        'GATE_SERVICE': take(virt, 'void service_user_pagemap_gate_wait('),
        'READER_ADMIT': take(virt, 'template <typename WaitForTeardown>\n[[nodiscard]] auto admit_user_pagemap_reader('),
        'READER_ACQUIRE': take(virt, '[[nodiscard]] auto acquire_user_pagemap_access('),
        'READER_RELEASE': take(virt, 'void release_user_pagemap_access('),
        'GATE_TRY': take(virt, '[[nodiscard]] auto try_begin_user_pagemap_exclusive('),
        'GATE_END': take(virt, 'void end_user_pagemap_exclusive('),
        'CONTEXT': take(virt, 'auto binding_root_context()'),
        'LEASE_ACQUIRED': take(virt, 'auto BindingRootMutationLease::acquired()'),
        'LEASE_FINISH': take(virt, 'void BindingRootMutationLease::finish()'),
        'LEASE_ACQUIRE': take(virt, 'auto BindingRootMutationLease::acquire('),
        'LEASE_FACTORY': take(virt, 'auto try_acquire_binding_root(const file_mapping::FileBindingSnapshot&'),
    }
    text = PRELUDE.read_text()
    for name, body in sections.items():
        marker = '@' + name + '@'
        if text.count(marker) != 1:
            raise ValueError('expected one source splice: ' + marker)
        text = text.replace(marker, body)
    if re.search(r'@[A-Z_]+@', text):
        raise ValueError('unresolved actual-source splice')
    return text + '\n' + CASES.read_text(), actual


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--virt-source', type=Path, default=KERN / 'platform/mm/virt.opt.cpp')
    parser.add_argument('--virt-header', type=Path, default=KERN / 'platform/mm/virt.hpp')
    parser.add_argument('--vmem-source', type=Path, default=KERN / 'syscalls_impl/vmem/sys_vmem.cpp')
    parser.add_argument('--vmem-header', type=Path, default=KERN / 'syscalls_impl/vmem/sys_vmem.hpp')
    parser.add_argument('--phys-source', type=Path, default=KERN / 'platform/mm/phys.opt.cpp')
    parser.add_argument('--cxx', default=os.environ.get('CXX', os.environ.get('WOS_HOST_CXX', '/usr/bin/clang++')))
    args = parser.parse_args()
    deadline = time.monotonic() + 90
    scratch = None
    groups = []
    watched, before = [], []
    result = dict(pass_=False, commands=[], scratch_removed=False,
                  host_metadata_control_ports=True, native_smp_storage_qualified=False)
    try:
        sources = [path.resolve(strict=True) for path in
                   (args.virt_source, args.virt_header, args.vmem_source, args.vmem_header, args.phys_source)]
        # Keep clang++'s multicall spelling while validating its actual ELF.
        compiler_driver = Path(args.cxx).absolute()
        compiler = compiler_driver.resolve(strict=True)
        if not compiler.is_file() or not os.access(compiler, os.X_OK):
            raise ValueError('--cxx must name one executable compiler ELF')
        with compiler.open('rb') as executable:
            if executable.read(4) != b'\x7fELF':
                raise ValueError('--cxx must name one executable compiler ELF')
        subreaper()
        watched = sources + [Path(__file__).resolve(), PRELUDE, CASES, compiler,
            UNIT / 'file_mmap_registry_runtime_test.py', UNIT / 'native_lazy_file_fault_runtime_test.py',
            KERN / 'platform/mm/file_mapping_lifetime.cpp', KERN / 'platform/mm/file_mapping_lifetime.hpp',
            KERN / 'platform/mm/lifetime_retirement_executor.hpp', KERN / 'platform/mm/paging.hpp',
            KERN / 'platform/sched/shutdown_ownership.hpp',
            ROOT / 'tests/host/kern_shim/defines/defines.hpp']
        watched += sorted(path for path in SHIM.rglob('*') if path.is_file())
        before = [pin(path) for path in watched]
        scratch = Path(tempfile.mkdtemp(prefix='wos-binding-root-lease-', dir='/tmp'))
        source, result['actual_definitions'] = generate(*(path.read_text() for path in sources))
        translation, binary = scratch / 'lease.cpp', scratch / 'lease'
        translation.write_text(source)
        argv = [str(compiler_driver), '-std=c++23', '-O1', '-g0', '-pipe', '-pthread', '-Wall', '-Wextra', '-Werror',
                '-fsanitize=address,undefined', '-fno-sanitize-recover=all', '-fno-omit-frame-pointer',
                '-I', str(SHIM), '-I', str(ROOT / 'tests/host/kern_shim'), '-I', str(KERN), '-I', str(ROOT / 'modules'),
                str(translation), str(KERN / 'platform/mm/file_mapping_lifetime.cpp'),
                str(SHIM / 'retirement_port.cpp'), '-o', str(binary)]
        row = command(argv, scratch / 'compile.log', scratch, deadline, groups)
        result['commands'].append(row)
        if row['exit'] != 0:
            print((scratch / 'compile.log').read_text(errors='replace')[-32768:])
            raise ValueError('actual-source compile/link failed')
        log_path = scratch / 'run.log'
        row = command([str(binary)], log_path, scratch, deadline, groups)
        result['commands'].append(row)
        log = log_path.read_text(errors='replace')
        print(log[-32768:], end='')
        if row['exit'] != 0 or not re.search(
                r'^PASS binding_root_lease cases=\d+ assertions=\d+ deaths=4 failures=0 host_metadata_control_ports=1$', log, re.M):
            raise ValueError('actual-source ownership/admission oracle did not pass')
        result['pass_'] = True
    except (OSError, ValueError, TimeoutError, KeyboardInterrupt) as error:
        result['error'] = f'{type(error).__name__}: {error}'
    finally:
        if scratch is not None and not groups:
            shutil.rmtree(scratch)
            result['scratch_removed'] = not scratch.exists()
        if groups:
            result['pass_'] = False
            result['cleanup_error'] = f'owned groups {groups}; scratch preserved'
        if before:
            try:
                result['sources_held'] = [pin(path) for path in watched] == before
            except OSError:
                result['sources_held'] = False
            if not result['sources_held']:
                result['pass_'] = False
        if time.monotonic() > deadline:
            result['pass_'] = False
            result['deadline_error'] = 'whole90-second invocation including cleanup exceeded'
        print(json.dumps(result))
    return 0 if result['pass_'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
