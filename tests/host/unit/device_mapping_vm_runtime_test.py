#!/usr/bin/env python3
"""Actual device VM/Object/family source against finite Host physical/control ports.

The unchanged prepared-plan, Object, family/index, revoke and paging TUs are
linked. Privileged VM/gate/root-lease/PUB definitions are extracted unchanged.
Task/PMM/table storage/root registry/IRQ/TLB endpoints are explicit finite ports;
this does not qualify real process syscalls, SMP, physical storage or TLB ACK.
Native maintained userspace tests cover the genuine PROCESS/fork entrypoints.
"""
from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
import time

sys.dont_write_bytecode = True
from file_mmap_registry_runtime_test import command, dependency_pins, pin, subreaper
from native_lazy_file_fault_runtime_test import definition

ROOT = next(p for p in Path(__file__).resolve().parents if (p / 'modules/kern/src').is_dir())
KERN = ROOT / 'modules/kern/src'
UNIT = Path(__file__).resolve().parent
OBJECT_SHIM = UNIT / 'device_mapping_object_shim'


def generate(scratch: Path, control: str) -> list[dict]:
    virt = (KERN / 'platform/mm/virt.opt.cpp').read_text()
    edits = {
        'ignore-mremap-policy': (' || control->ps_admission_suspensions != 0', ''),
        'ignore-fork-count': (' || found != s.count', ''),
        'ignore-lazy': ('if (overlap) return -EOPNOTSUPP;', 'if (overlap) {}'),
    }
    if control in edits:
        old, new = edits[control]
        if virt.count(old) != 1:
            raise ValueError('counterexample source anchor changed: ' + control)
        virt = virt.replace(old, new)
    if control == 'restore-fork-reclaim-check':
        body, _ = definition(virt, 'auto device_fork_match_context(')
        old = 'return preparation.ready()'
        if body.count(old) != 1: raise ValueError('fork-only reclaim control anchor changed')
        virt = virt.replace(body, body.replace(old, 'return phys::can_wait_for_reclaim() && preparation.ready()', 1), 1)
    plan_source = (KERN / 'platform/mm/device_mapping_vm.cpp').read_text()
    if control == 'no-sort':
        plan_source, changed = re.subn(r'    std::sort\(s\.old\.get\(\),[\s\S]*?\}\);\n', '', plan_source)
        if changed != 1:
            raise ValueError('counterexample sort source anchor changed')
    (scratch / 'plans.cpp').write_text(plan_source)
    header = (KERN / 'platform/mm/virt.hpp').read_text()
    vmem = (KERN / 'syscalls_impl/vmem/sys_vmem.cpp').read_text()
    vmem_header = (KERN / 'syscalls_impl/vmem/sys_vmem.hpp').read_text()
    actual = []

    def take(source: str, anchor: str, declaration: bool = False) -> str:
        body, row = definition(source, anchor, declaration)
        actual.append(row)
        return body

    def section(source: str, begin: str, end: str) -> str:
        a = source.index(begin)
        body = source[a:source.index(end, a)]
        actual.append(dict(anchor=begin, bytes=len(body.encode()), sha256=hashlib.sha256(body.encode()).hexdigest()))
        return body

    physical = (KERN / 'platform/mm/phys.opt.cpp').read_text()
    physical_context = '#include <vm_fixture.hpp>\nnamespace ker::mod::mm::phys {\nnamespace {\n'
    physical_context += take(physical, 'auto can_wait_for_reclaim_impl() -> bool {')
    physical_context += '\n}\n' + take(physical, 'auto can_wait_for_reclaim() -> bool {') + '\n}\n'
    (scratch / 'phys-context.cpp').write_text(physical_context)

    include = scratch / 'include'

    def write(name: str, text: str) -> None:
        path = include / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)

    write('vm_fixture.hpp', (UNIT / 'device_mapping_vm_common.inc').read_text())
    for name in ('platform/sys/spinlock.hpp', 'platform/sys/mutex.hpp', 'platform/sched/task.hpp',
                 'platform/sched/scheduler.hpp', 'platform/sys/usercopy.hpp', 'platform/mm/phys.hpp',
                 'vfs/file.hpp', 'vfs/vfs.hpp'):
        write(name, '#pragma once\n#include <vm_fixture.hpp>\n')
    tmpfs = (UNIT / 'file_binding_snapshot_shim/vfs/fs/tmpfs.hpp').read_text().replace('snapshot_fixture.hpp', 'vm_fixture.hpp')
    write('vfs/fs/tmpfs.hpp', tmpfs)
    pub_decl = take(vmem_header, 'class SharedVmemPublicationGuard {', True)
    write('syscalls_impl/vmem/sys_vmem.hpp', '#pragma once\n#include <vm_fixture.hpp>\nnamespace ker::syscall::vmem {\n' + pub_decl + '\n}\n')
    prep = take((KERN / 'platform/sys/syscall_preparation.hpp').read_text(), 'class SyscallPreparationScope {', True)
    write('platform/sys/syscall_preparation.hpp', '#pragma once\n#include <vm_fixture.hpp>\nnamespace ker::mod::sys {\n' + prep + '\n}\n')
    managed = (KERN / 'platform/mm/managed_huge.hpp').read_text()
    a = managed.index('[[nodiscard]] auto copy_current_user_pagemap_exclusive(')
    copier_decl = managed[a:managed.index(';', a) + 1]
    write('platform/mm/managed_huge.hpp', '''#pragma once
#include <vm_fixture.hpp>
namespace ker::mod::sys { class SyscallPreparationScope; }
namespace ker::syscall::vmem { class SharedVmemPublicationGuard; }
namespace ker::mod::mm::device_vm { class PreparedFork; }
namespace ker::mod::mm::huge {
class RangeSplitSurvey; class PreparedTables;
namespace detail {
auto allocate_table() noexcept -> paging::PageTable*;
void release_table(paging::PageTable*) noexcept;
''' + copier_decl + '\n}}\n')
    lease_decl = take(header, 'enum class BindingRootAdmission :', True) + '\n' + take(header, 'class BindingRootMutationLease {', True)
    a = header.index('[[nodiscard]] auto try_acquire_binding_root(')
    lease_factory_decl = header[a:header.index(';', a) + 1]
    write('platform/mm/virt.hpp', '''#pragma once
#include <vm_fixture.hpp>
#include <platform/mm/addr.hpp>
#include <platform/mm/file_mapping_lifetime.hpp>
#include <platform/mm/device_mapping_vm.hpp>
#include <syscalls_impl/vmem/sys_vmem.hpp>
namespace ker::mod::mm::virt { using namespace paging; using namespace addr;
''' + lease_decl + '\n' + lease_factory_decl + '\n}\n')
    pub = '#include <vm_fixture.hpp>\n#include <platform/mm/virt.hpp>\n#include <platform/mm/lifetime_retirement_executor.hpp>\n#include <platform/sys/syscall_preparation.hpp>\n'
    pub += 'namespace ker::syscall::vmem {\nnamespace {\n'
    pub += 'inline auto get_current_task()->ker::mod::sched::task::Task* {return ker::mod::sched::get_current_task();}\n'
    pub += section(vmem, 'ker::mod::sys::Mutex g_shared_vmem_publication_lock;', '// Protected by blocking PUB;')
    pub += take(vmem, '[[nodiscard]] auto blocking_publication_owner_matches(') + '\n}\n'
    for anchor in ('SharedVmemPublicationGuard::SharedVmemPublicationGuard()', 'SharedVmemPublicationGuard::~SharedVmemPublicationGuard()',
                   'auto SharedVmemPublicationGuard::acquired()', 'auto SharedVmemPublicationGuard::acquired_by_current_executor()'):
        pub += take(vmem, anchor) + '\n'
    pub += '}\nnamespace ker::mod::mm::virt {\n'
    pub += '''
using PageTable=paging::PageTable;
struct HostControl {PageTable* root{}; uint64_t serial{}, ps_admission_suspensions{};};
std::array<HostControl,8> controls{};
sys::Spinlock user_mapping_pin_lock, cow_pte_lock;
file_mapping::FileMappingIndex file_mapping_index;
PageTable* kernel_pagemap{};
bool kernel_nx_supported=true;
auto find_user_root_control_locked(PageTable* root)->HostControl* {
 object_fixture::check(object_fixture::lock_depth!=0);
 for(auto& control:controls)if(control.root==root && root)return &control;
 return nullptr;
}
void service_pending_tlb_shootdowns(){std::abort();}
auto find_anonymous_swap_mapping_locked(PageTable*,uint64_t)->void* {return nullptr;}
void owned_frame_untrack_leaf(PageTable*,uint64_t,const paging::PageTableEntry&) {}
void flush_pagemap_after_update(PageTable* root,uint64_t,bool) {
 vm_fixture::expect(object_fixture::lock_depth==0 && object_fixture::irq_depth==0,"actual CPU barrier outside IRQ/Object/PTE locks");
 vm_fixture::expect(object_fixture::pub_depth==1,"actual CPU barrier retains PUB");
 ++vm_fixture::flushes; (void)root;
}
'''
    pub += take(virt, 'auto pte_raw(const paging::PageTableEntry& e)') + '\n'
    pub += take(virt, 'auto index_of(') + '\n'
    pub += take(virt, 'auto entry_at(PageTable* table,') + '\n'
    pub += take(virt, 'auto entry_at(const PageTable* table,') + '\n'
    pub += section(virt, 'constexpr size_t USER_PAGEMAP_ACCESS_GATE_COUNT =', '[[nodiscard]] auto user_pagemap_access_gate_index(')
    for anchor in ('[[nodiscard]] auto user_pagemap_access_gate_index(', 'void service_user_pagemap_gate_wait(',
                   'template <typename WaitForTeardown>\n[[nodiscard]] auto admit_user_pagemap_reader(',
                   '[[nodiscard]] auto acquire_user_pagemap_access(', 'void release_user_pagemap_access(',
                   '[[nodiscard]] auto try_begin_user_pagemap_exclusive(', 'void end_user_pagemap_exclusive(',
                   'auto binding_root_context()', 'auto BindingRootMutationLease::acquired()',
                   'void BindingRootMutationLease::finish()', 'auto BindingRootMutationLease::acquire(',
                   'auto try_acquire_binding_root(const file_mapping::FileBindingSnapshot&', 'auto current_process_root('):
        pub += take(virt, anchor) + '\n'
    pub += section(virt, 'constexpr uint64_t DEVICE_VM_PAGE =', 'class DeviceCurrentAccess {')
    for anchor, declaration in (('class DeviceCurrentAccess {', True), ('class DevicePteLocks {', True),
                                ('auto device_control_matches(', False), ('auto device_leaf_locked(', False),
                                ('auto device_existing_leaf_locked(', False), ('auto device_install_leaf_locked(', False),
                                ('auto device_leaf_flags(', False)):
        pub += take(virt, anchor, declaration) + '\n'
    pub += '}\nnamespace ker::mod::mm::device_vm {\n' + take(virt, 'auto current_device_overlap(') + '\nnamespace detail {\n'
    for anchor in ('auto count_current_bindings(', 'auto capture_current_bindings(', 'auto VmAccess::publish(',
                   'auto VmAccess::change(', 'auto VmAccess::revoke(', 'auto VmAccess::match_fork(', 'auto VmAccess::attach_fork('):
        pub += take(virt, anchor) + '\n'
    pub += '}}\n' + (UNIT / 'device_mapping_vm_cases.inc').read_text()
    (scratch / 'vm.cpp').write_text(pub)
    (scratch / 'ports.cpp').write_text((UNIT / 'device_mapping_vm_ports.inc').read_text())
    # The actual family release submits under its owner IRQ lock. This finite
    # queue endpoint, like the real port contract, must permit that noninline
    # submission; callbacks still run only after all owner/IRQ/PUB locks end.
    retirement = (OBJECT_SHIM / 'retirement_port.cpp').read_text()
    retirement = retirement.replace('check(lock_depth == 0 && mutex_depth == 0);', 'check(mutex_depth == 0);')
    (scratch / 'retirement.cpp').write_text(retirement)
    return actual


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cxx', default='/usr/bin/clang++')
    parser.add_argument('--evidence-dir', type=Path)
    parser.add_argument('--control', choices=('current', 'no-sort', 'ignore-lazy', 'ignore-mremap-policy', 'ignore-fork-count',
                                             'restore-fork-reclaim-check'),
                        default='current')
    args = parser.parse_args()
    deadline = time.monotonic() + 90
    scratch = None
    groups = []
    watched, before = [], []
    result = dict(pass_=False, control=args.control, commands=[], scratch_removed=False, host_metadata_ports=True,
                  native_smp_tlb_qualified=False)
    try:
        compiler_driver = Path(args.cxx).absolute()
        compiler = compiler_driver.resolve(strict=True)
        if not os.access(compiler, os.X_OK) or compiler.read_bytes()[:4] != b'\x7fELF':
            raise ValueError('--cxx must name an executable compiler ELF')
        subreaper()
        paths = ('platform/mm/virt.opt.cpp', 'platform/mm/virt.hpp', 'platform/mm/device_mapping_vm.cpp',
                 'platform/mm/device_mapping_vm.hpp', 'platform/mm/device_mapping_revoke.cpp', 'platform/mm/device_mapping_revoke.hpp',
                 'platform/mm/file_mapping_lifetime.cpp', 'platform/mm/file_mapping_lifetime.hpp',
                 'platform/mm/paging.opt.cpp', 'platform/mm/paging.hpp', 'platform/mm/managed_huge.hpp',
                 'dev/device_mapping_object.cpp', 'dev/device_mapping_object.hpp',
                 'syscalls_impl/vmem/sys_vmem.cpp', 'syscalls_impl/vmem/sys_vmem.hpp', 'platform/sys/syscall_preparation.hpp',
                 'platform/mm/phys.opt.cpp')
        watched = [KERN / p for p in paths] + [Path(__file__).resolve(), compiler,
            UNIT / 'mmio_disabled_virt_ports.inc',
            UNIT / 'device_mapping_vm_common.inc', UNIT / 'device_mapping_vm_ports.inc', UNIT / 'device_mapping_vm_cases.inc',
            UNIT / 'file_mmap_registry_runtime_test.py', UNIT / 'native_lazy_file_fault_runtime_test.py']
        watched += sorted(p for p in OBJECT_SHIM.rglob('*') if p.is_file())
        before = [pin(p) for p in watched]
        scratch = Path(tempfile.mkdtemp(prefix='wos-device-mapping-vm-', dir='/tmp'))
        result['actual_definitions'] = generate(scratch, args.control)
        result['compiled_plan_source'] = pin(scratch / 'plans.cpp')
        binary = scratch / 'vm'
        flags = [str(compiler_driver), '-include', str(UNIT / 'mmio_disabled_virt_ports.inc'),
                '-std=c++23', '-O1', '-g0', '-pipe', '-pthread', '-Wall', '-Wextra', '-Werror',
                '-fsanitize=address,undefined', '-fno-sanitize-recover=all', '-fno-omit-frame-pointer',
                '-I', str(scratch / 'include'), '-I', str(OBJECT_SHIM), '-I', str(ROOT / 'tests/host/kern_shim'),
                '-I', str(KERN), '-I', str(ROOT / 'modules')]
        units = [scratch / name for name in ('vm.cpp', 'ports.cpp', 'phys-context.cpp', 'retirement.cpp', 'plans.cpp')]
        units += [KERN / name for name in ('platform/mm/device_mapping_revoke.cpp', 'platform/mm/file_mapping_lifetime.cpp',
                                          'platform/mm/paging.opt.cpp', 'dev/device_mapping_object.cpp')]
        dependencies = {}
        for i, source in enumerate(units):
            dep = scratch / f'{i}-discover.d'
            row = command(flags + ['-M', '-MF', str(dep), '-MT', str(i), str(source)],
                          scratch / f'{i}-discover.log', scratch, deadline, groups)
            result['commands'].append(row)
            if row['exit'] != 0: raise ValueError(f'dependency discovery failed: {source.name}')
            for item in dependency_pins(dep): dependencies[item['path']] = Path(item['path'])
        if [pin(p) for p in watched] != before: raise ValueError('source changed during discovery')
        watched = sorted(set(watched) | set(dependencies.values()))
        before = [pin(p) for p in watched]
        result['compiled_input_pins'] = before
        objects = []
        for i, source in enumerate(units):
            obj, dep = scratch / f'{i}.o', scratch / f'{i}-actual.d'
            row = command(flags + ['-MD', '-MF', str(dep), '-c', str(source), '-o', str(obj)],
                          scratch / f'{i}-compile.log', scratch, deadline, groups)
            result['commands'].append(row)
            if row['exit'] != 0: raise ValueError(f'actual source compile failed: {source.name}')
            if any(item['path'] not in dependencies for item in dependency_pins(dep)):
                raise ValueError(f'undeclared compiled input: {source.name}')
            objects.append(obj)
        row = command([str(compiler_driver), '-pthread', '-fsanitize=address,undefined', '-fno-sanitize-recover=all',
                       *map(str, objects), '-o', str(binary)], scratch / 'link.log', scratch, deadline, groups)
        result['commands'].append(row)
        if row['exit'] != 0: raise ValueError('actual VM/Object/family source did not link')
        result['binary'] = pin(binary)
        row = command([str(binary)], scratch / 'run.log', scratch, deadline, groups)
        result['commands'].append(row)
        result['run_exit'] = row['exit']
        log = (scratch / 'run.log').read_text(errors='replace')
        print(log[-32768:], end='')
        if re.search(r'AddressSanitizer|runtime error:|UndefinedBehaviorSanitizer', log):
            raise ValueError('sanitizer failure is not a qualified device/fork oracle')
        if args.control == 'current':
            if row['exit'] != 0 or not re.search(r'^PASS device_mapping_vm cases=\d+ assertions=\d+ failures=0 host_metadata_ports=1$', log, re.M):
                raise ValueError('actual VM ownership/transaction oracle failed')
        else:
            marker = {
                'no-sort': 'FAIL actual sorted multi-binding partial protect',
                'ignore-lazy': 'FAIL sparse device+lazy remove refuses before effects',
                'ignore-mremap-policy': 'FAIL actual mremap suspension excludes late device admission',
                'ignore-fork-count': 'FAIL new actual device binding invalidates complete fork plan',
                'restore-fork-reclaim-check': 'FAIL allocation-free empty fork match permits deferred syscall reschedule',
            }[args.control]
            if row['exit'] not in (-6, 3) or marker not in log.splitlines():
                raise ValueError('compiled counterexample did not fail its intended connected oracle')
            if args.control == 'ignore-fork-count' and not re.search(
                    r'^FAIL device_mapping_vm cases=8 assertions=\d+ failures=[1-9]\d* host_metadata_ports=1$', log, re.M):
                raise ValueError('fork counterexample did not complete all unchanged scenario groups')
            if args.control == 'restore-fork-reclaim-check':
                exact = {'FAIL allocation-free empty fork match permits deferred syscall reschedule',
                         'FAIL allocation-free nonempty fork match permits deferred syscall reschedule'}
                failures = {line for line in log.splitlines() if line.startswith('FAIL ') and ' device_mapping_vm ' not in line}
                if row['exit'] != 3 or failures != exact or not re.search(
                        r'^FAIL device_mapping_vm cases=8 assertions=\d+ failures=2 host_metadata_ports=1$', log, re.M):
                    raise ValueError('restored old check did not produce only its exact two deferred-match failures')
            result['negative_control_observed'] = True
        result['pass_'] = True
    except (OSError, ValueError, TimeoutError, KeyboardInterrupt) as error:
        result['error'] = f'{type(error).__name__}: {error}'
    finally:
        if before:
            after = [pin(p) for p in watched]
            result['changed_sources'] = [old['path'] for old, new in zip(before, after) if old != new]
            result['sources_held'] = not result['changed_sources']
            result['compiled_inputs_held'] = result['sources_held'] and 'compiled_input_pins' in result
            result['pass_'] &= result['sources_held']
        if args.evidence_dir is not None:
            try:
                args.evidence_dir.mkdir(parents=True, exist_ok=True)
                if scratch is not None:
                    for log in scratch.glob('*.log'):
                        target = args.evidence_dir / log.name
                        if target.exists(): raise ValueError(f'evidence exists: {target}')
                        shutil.copyfile(log, target)
            except (OSError, ValueError) as error:
                result['pass_'] = False
                result['evidence_error'] = str(error)
        if scratch is not None and not groups:
            shutil.rmtree(scratch)
            result['scratch_removed'] = not scratch.exists()
        if groups:
            result['pass_'] = False
            result['cleanup_error'] = f'owned groups {groups}; scratch preserved'
        if time.monotonic() > deadline:
            result['pass_'] = False
            result['error'] = 'whole90-second deadline exceeded'
        if args.evidence_dir is not None:
            target = args.evidence_dir / 'result.json'
            if target.exists():
                result['pass_'] = False
                result['evidence_error'] = f'evidence exists: {target}'
            else: target.write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps({key: value for key, value in result.items() if key not in ('commands', 'compiled_input_pins')}))
    return 0 if result['pass_'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
