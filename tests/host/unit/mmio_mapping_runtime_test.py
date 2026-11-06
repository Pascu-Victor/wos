#!/usr/bin/env python3
"""Run actual MM/PCI/CPU/DMA engines and the complete actual e1000 consumer.

Only finite firmware/configuration, CPU instruction, memory/allocator, scheduler,
network and IRQ effects are ports. Actual issuers create every Controller, actual
PCI creates every physical capability, and actual driver retirement is the sole
issuer of a QuiescedMapping. Host ports do not qualify native hardware or SMP.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time

sys.dont_write_bytecode = True
from cpu_maintenance_runtime_test import COMMON, actual_definition, controller_definitions, gtest_context
from e1000_mmio_consumer_fixture import consumer_definitions
from mmio_mapping_writer_fixture import writer_definitions
from file_mmap_registry_runtime_test import command, dependency_pins, pin, subreaper

ROOT = next(p for p in Path(__file__).resolve().parents if (p / 'modules/kern/src').is_dir())
KERN = ROOT / 'modules/kern/src'
UNIT = Path(__file__).resolve().parent
SHIM = UNIT / 'mmio_mapping_shim'
CASES = UNIT / 'mmio_mapping_cases.cpp'
PORTS = UNIT / 'mmio_mapping_ports.inc'
NAMES = ['engine_private_fences', 'engine_budget_refusal', 'engine_native_writers', 'engine_bad_table_alias',
         'engine_view_bounds', 'engine_invalid_view_plans', 'engine_legacy_view_default', 'engine_cross_bar_views', 'engine_view_prepare_resume',
         'consumer_success', 'consumer_held_callback', 'consumer_probe_packet_failure',
         'consumer_probe_irq_failure', 'consumer_stop_failure', 'consumer_mask_failure',
         'consumer_initial_bme_refusal', 'consumer_withdrawal_retry', 'consumer_observed_profile', 'consumer_second_view_retry',
         'consumer_completed_va_reuse', 'engine_large_sparse', 'engine_large_fragmented', 'engine_large_late_alias',
         'engine_large_transient_alias', 'engine_large_device_history', 'engine_large_transient_device',
         'engine_large_history_overflow', 'engine_large_root_drift', 'consumer_admission_steps',
         'consumer_admission_fragmented', 'consumer_admission_deadline', 'consumer_admission_cpu_retry',
         'consumer_admission_clock_unavailable', 'consumer_admission_clock_overflow', 'consumer_admission_clock_regression']
FINITE_NAMES = ['engine_large_epoch_saturation']
SOURCES = COMMON + [KERN / relative for relative in
    ('platform/mm/paging.opt.cpp', 'platform/mm/cache_policy.cpp', 'platform/mm/cpu_maintenance.cpp', 'platform/mm/mmio_mapping.cpp', 'dev/pci.cpp', 'dev/pci_extended.cpp',
     'dev/pci_platform_apertures.cpp', 'dev/dma/dma.cpp')]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--cxx', default='/usr/bin/clang++')
    parser.add_argument('--host-build', type=Path, default=ROOT / 'build_tests')
    parser.add_argument('--evidence-dir', type=Path)
    parser.add_argument('--case', action='append', choices=NAMES + FINITE_NAMES)
    parser.add_argument('--finite-epoch-limit', action='store_true', help='compile-copy actual saturation helper limit128; no epoch setter')
    parser.add_argument('--control', choices=['remove-writer-preflight', 'remove-bank-table-veto', 'remove-table-supervisor-check',
                                            'remove-completed-child-skip', 'remove-leaf-publication-epoch'])
    args = parser.parse_args()
    if args.finite_epoch_limit != (args.case == FINITE_NAMES) or (args.finite_epoch_limit and args.control):
        parser.error('--finite-epoch-limit requires exactly --case engine_large_epoch_saturation and no negative control')
    deadline = time.monotonic() + 180
    scratch = None
    groups = []
    watched, before = [], []
    result = dict(pass_=False, commands=[], runs=[], sources_held=False, compiled_inputs_held=False,
                  scratch_removed=False, real_mm_pci_cpu_dma_consumer=True, host_effect_ports_only=True,
                  native_hardware_smp_qualified=False, no_capability_or_witness_factory=True,
                  dma_finite_address_port_macro="-Dget_hhdm_offset=dma_host_hhdm_offset",
                  native_dma_address_arithmetic_qualified=False)
    try:
        driver = Path(args.cxx).absolute()
        compiler = driver.resolve(strict=True)
        if not compiler.is_file() or not os.access(compiler, os.X_OK):
            raise ValueError('--cxx must name an executable compiler')
        gtest_include, cmake_pins, _ = gtest_context(args.host_build)
        if not (SHIM / 'platform/asm/msr.hpp').is_file():
            raise ValueError('finite inherited-PAT MSR input header is not ready; native RDMSR is forbidden')
        controllers, controller_inputs = controller_definitions()
        consumer, actual_definitions, consumer_inputs = consumer_definitions()
        writer, writer_actual, writer_inputs = writer_definitions()
        result['actual_writer_definitions'] = writer_actual
        if args.control == 'remove-writer-preflight':
            body = actual_definition(writer, 'void require_ordinary_mapping(')
            old = 'if (typed_mmio_guards.load(std::memory_order_relaxed) &&'
            if body.count(old) != 1: raise ValueError('nonunique native mapping preflight control anchor')
            changed = body.replace(old, 'if (false && typed_mmio_guards.load(std::memory_order_relaxed) &&', 1)
            writer = writer.replace(body, changed, 1)
            result['negative_control'] = args.control
            result['compile_copy_native_mapping_preflight_removed'] = True
        result['actual_consumer_definitions'] = actual_definitions
        watched = [Path(__file__).resolve(), CASES, PORTS, *SOURCES, *controller_inputs, *consumer_inputs, *writer_inputs, UNIT / 'mmio_mapping_writer_fixture.py',
                   UNIT / 'cpu_maintenance_runtime_test.py', UNIT / 'cpu_maintenance_ports.inc',
                   UNIT / 'file_mmap_registry_runtime_test.py',
                   UNIT / 'cpu_maintenance_controller_ports.inc', UNIT / 'mmio_cache_runtime_test.cpp',
                   UNIT / 'pci_cold_fixture.hpp', UNIT / 'e1000_mmio_consumer_fixture.py',
                   UNIT / 'e1000_mmio_consumer_ports.inc', UNIT / 'e1000_mmio_consumer_cases.inc', compiler,
                   *(Path(row['path']) for row in cmake_pins)]
        watched += [p for p in SHIM.rglob('*') if p.is_file()]
        watched = sorted(set(watched))
        before = [pin(p) for p in watched]
        subreaper()
        scratch = Path(tempfile.mkdtemp(prefix='wos-mmio-engine-', dir='/tmp'))
        (scratch / 'cpu_maintenance_controllers_generated.hpp').write_text(controllers)
        (scratch / 'e1000_consumer_generated.hpp').write_text(consumer)
        (scratch / 'mmio_writer_generated.hpp').write_text(writer)
        token_source = (UNIT / 'mmio_cache_runtime_test.cpp').read_text()
        boundary = 'namespace {\nnamespace cache = ker::mod::mm::mmio_cache;'
        if token_source.count(boundary) != 1:
            raise ValueError('immutable CPU hardware-port boundary changed')
        cpu_ports = token_source[:token_source.index(boundary)]
        begin = cpu_ports.index('namespace ker::mod::mm::cache {')
        end_marker = '}  // namespace ker::mod::mm::cache'
        end = cpu_ports.index(end_marker, begin) + len(end_marker)
        # That original file's finite inherited-PAT lookup is not needed here:
        # this combined driver links/captures the complete actual PAT registry.
        cpu_ports = cpu_ports[:begin] + cpu_ports[end:]
        result['actual_inherited_pat_registry_replaces_finite_lookup'] = True
        (scratch / 'mmio_cache_runtime_ports_generated.hpp').write_text(cpu_ports)
        flags = [str(driver), '-std=c++23', '-O1', '-g0', '-pipe', '-pthread', '-Wall', '-Wextra', '-Werror', '-UNDEBUG',
                 '-DWOS_SELFTEST=1', '-DWOS_HOST_ACTUAL_E1000=1', '-fsanitize=address,undefined',
                 '-fsanitize-address-use-after-return=never', '-fno-sanitize-recover=all', '-fno-omit-frame-pointer',
                 '-I', str(SHIM), '-I', str(ROOT / 'tests/host/kern_shim'), '-I', str(scratch), '-I', str(UNIT),
                 '-isystem', str(gtest_include), '-I', str(KERN), '-I', str(KERN / 'platform/mm'), '-I', str(ROOT / 'modules')]
        units = SOURCES + [CASES]
        if args.finite_epoch_limit or args.control in ('remove-bank-table-veto', 'remove-table-supervisor-check', 'remove-completed-child-skip',
                                                      'remove-leaf-publication-epoch'):
            original = KERN / 'platform/mm/mmio_mapping.cpp'
            text = original.read_text()
            if args.finite_epoch_limit:
                anchor = 'void advance_epoch_locked()'
                old, replacement = 'UINT64_MAX', 'uint64_t{128}'
            elif args.control == 'remove-leaf-publication-epoch':
                anchor = 'auto generic_leaf(const Table* table,'
                old, replacement = 'advance_epoch_locked();', '(void)0;'
            elif args.control == 'remove-completed-child-skip':
                anchor = 'auto withdraw(uint16_t slot, uint64_t id, const Controller* scope, const QuiescedMapping& proof)'
                old = 'if (view.phase.load(std::memory_order_acquire) == Phase::complete) continue;'
                replacement = 'if (false && view.phase.load(std::memory_order_acquire) == Phase::complete) continue;'
            elif args.control == 'remove-bank-table-veto':
                anchor = 'auto generic_table(const Table* table)'
                old = 'if (ADDRESS >= BANK_BASE && ADDRESS < BANK_END)'
                replacement = 'if (false && ADDRESS >= BANK_BASE && ADDRESS < BANK_END)'
            else:
                anchor = 'auto register_table_locked(Table* table, uint64_t physical)'
                old, replacement = 'paging::PAGE_USER | ', ''
            body = actual_definition(text, anchor)
            expected_count = 2 if args.finite_epoch_limit else 1
            if body.count(old) != expected_count: raise ValueError('nonunique actual Runtime compile-copy anchor')
            changed = body.replace(old, replacement, expected_count)
            copy = scratch / 'control-mmio_mapping.cpp'
            copy.write_text(text.replace(body, changed, 1))
            if args.finite_epoch_limit:
                result['finite_actual_epoch_saturation_limit'] = 128
                result['epoch_value_setter'] = False
            else:
                result['negative_control'] = args.control
            result['actual_runtime_control_copy'] = pin(copy)
            units = [copy if source == original else source for source in units]
        deps = {}
        for i, source in enumerate(units):
            dep = scratch / f'{i}-discover.d'
            unit_flags = flags + (['-Dget_hhdm_offset=dma_host_hhdm_offset'] if source == KERN / 'dev/dma/dma.cpp' else [])
            row = command(unit_flags + ['-M', '-MF', str(dep), '-MT', str(i), str(source)],
                          scratch / f'{i}-discover.log', scratch, deadline, groups)
            result['commands'].append(row)
            if row['exit'] != 0:
                print((scratch / f'{i}-discover.log').read_text(errors='replace')[-18000:])
                raise ValueError(f'dependency discovery failed: {source.name}')
            for item in dependency_pins(dep): deps[item['path']] = Path(item['path'])
        if [pin(p) for p in watched] != before:
            raise ValueError('original source changed during dependency discovery')
        watched = sorted(set(watched) | set(deps.values()))
        before = [pin(p) for p in watched]
        result['compiled_input_pins'] = before
        objects = []
        for i, source in enumerate(units):
            obj, dep = scratch / f'{i}.o', scratch / f'{i}-actual.d'
            unit_flags = flags + (['-Dget_hhdm_offset=dma_host_hhdm_offset'] if source == KERN / 'dev/dma/dma.cpp' else [])
            row = command(unit_flags + ['-MD', '-MF', str(dep), '-c', str(source), '-o', str(obj)],
                          scratch / f'{i}-compile.log', scratch, deadline, groups)
            result['commands'].append(row)
            if row['exit'] != 0:
                print((scratch / f'{i}-compile.log').read_text(errors='replace')[-24000:])
                raise ValueError(f'actual TU compile failed: {source.name}')
            if any(item['path'] not in deps for item in dependency_pins(dep)):
                raise ValueError(f'undeclared compiled dependency: {source.name}')
            objects.append(obj)
        binary = scratch / 'mmio-engine'
        row = command([str(driver), '-pthread', '-fsanitize=address,undefined', '-fno-sanitize-recover=all',
                       *map(str, objects), '-o', str(binary)], scratch / 'link.log', scratch, deadline, groups)
        result['commands'].append(row)
        if row['exit'] != 0:
            print((scratch / 'link.log').read_text(errors='replace')[-18000:])
            raise ValueError('actual-engine link failed')
        result['binary'] = pin(binary)
        controls = {
            'remove-writer-preflight': ('engine_native_writers', 'actual map entrypoint refuses before allocation, owner tracking, refs or TLB work'),
            'remove-bank-table-veto': ('engine_private_fences', 'typed device VA cannot masquerade as an arithmetically plausible HHDM page-table pointer'),
            'remove-table-supervisor-check': ('engine_bad_table_alias', 'actual unsafe HHDM leaf cannot register the permanent private bank'),
            'remove-completed-child-skip': ('consumer_completed_va_reuse', 'completed-child retry preserves the actual newer function mapping'),
            'remove-leaf-publication-epoch': ('engine_large_late_alias',
                                              'late actual HHVA effect invalidates previously scanned full-resource absence'),
        }
        for name in ([controls[args.control][0]] if args.control else args.case or NAMES):
            row = command([str(binary), name], scratch / f'{name}.log', scratch, deadline, groups)
            result['commands'].append(row)
            output = (scratch / f'{name}.log').read_text(errors='replace')
            result['runs'].append(dict(case=name, exit=row['exit'], receipt=pin(scratch / f'{name}.log')))
            print(f'[{name}]\n{output[-16000:]}', end='')
            if args.control:
                prefix = 'e1000 consumer assertion: ' if args.control == 'remove-completed-child-skip' else 'actual MM engine check failed: '
                exact = prefix + controls[args.control][1]
                if row['exit'] != -6 or exact not in output:
                    raise ValueError('compile-success control did not fail its exact oracle')
                if any(token in output for token in ('AddressSanitizer', 'runtime error:', 'UndefinedBehaviorSanitizer')):
                    raise ValueError('control sanitizer failure is not a qualified oracle')
                result['control_detected'] = True
                continue
            if name == 'engine_bad_table_alias' and 'cold UC probe retained id=0x10d3 status=arena_unavailable' not in output:
                raise ValueError('actual malformed HHDM leaf did not leave the typed arena unavailable')
            if name == 'engine_budget_refusal' and 'MMIO_VIEW_BUDGET_REFUSAL status=capacity bytes=33554432' not in output:
                raise ValueError('actual materialized-view budget did not report capacity')
            if name == 'consumer_admission_deadline' and ('MMIO_BOOT_PREPARE attempts=1 status=pending_retained exhausted=1' not in output or
                                                        'MMIO_ADMISSION_DEADLINE_HOST next_va=' not in output):
                raise ValueError('actual driver deadline/private-VA proof missing')
            if name == 'consumer_completed_va_reuse' and 'MMIO_VA_REUSE_HOST old_mapping=' not in output:
                raise ValueError('actual second-function generation/VA reuse proof missing')
            if name == 'engine_large_epoch_saturation' and 'MMIO_EPOCH_SATURATION_HOST finite_limit=128 sticky=1 ordinary_ram_usable=1' not in output:
                raise ValueError('finite actual saturation helper proof missing')
            if row['exit'] != 0 or f'MMIO_MAPPING_HOST CASE_PASS name={name} ' not in output:
                raise ValueError(f'actual engine case failed: {name}')
            if any(token in output for token in ('AddressSanitizer', 'runtime error:', 'UndefinedBehaviorSanitizer')):
                raise ValueError(f'sanitizer failure: {name}')
        result['pass_'] = True
    except (OSError, ValueError, TimeoutError, KeyboardInterrupt) as error:
        result['error'] = f'{type(error).__name__}: {error}'
    finally:
        if before:
            try:
                result['sources_held'] = [pin(p) for p in watched] == before
                result['compiled_inputs_held'] = result['sources_held'] and 'compiled_input_pins' in result
            except OSError:
                result['sources_held'] = False
            if not result['sources_held']: result['pass_'] = False
        if args.evidence_dir is not None:
            try:
                args.evidence_dir.mkdir(parents=True, exist_ok=True)
                if scratch is not None:
                    for path in scratch.glob('*.log'):
                        target = args.evidence_dir / path.name
                        if target.exists(): raise ValueError(f'evidence exists: {target}')
                        shutil.copyfile(path, target)
            except (OSError, ValueError) as error:
                result['pass_'] = False
                result['evidence_error'] = str(error)
        if scratch is not None and not groups:
            shutil.rmtree(scratch)
            result['scratch_removed'] = not scratch.exists()
        if groups:
            result['pass_'] = False
            result['cleanup_error'] = f'owned sessions remain: {groups}'
        if time.monotonic() > deadline:
            result['pass_'] = False
            result['deadline_error'] = 'bounded invocation/cleanup deadline exceeded'
        if args.evidence_dir is not None:
            target = args.evidence_dir / 'result.json'
            if target.exists():
                result['pass_'] = False
                result['evidence_error'] = f'evidence exists: {target}'
            else:
                target.write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps({key: value for key, value in result.items() if key not in ('commands', 'compiled_input_pins')}))
    return 0 if result['pass_'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
