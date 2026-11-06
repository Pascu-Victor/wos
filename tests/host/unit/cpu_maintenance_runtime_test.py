#!/usr/bin/env python3
"""Qualify actual CPU maintenance/18b TUs with finite Host ports.

No host CR3/MSR/cache instruction executes. Original production sources are
pinned and unchanged. Explicit compile copies add two approved pause calls,
narrow private limits, or inject a stale ACK for paired negative controls.
Native issuer bodies use finite scheduler/device endpoints and the real host
stack: ASan fake use-after-return stacks are disabled, while ASan bounds/heap
checks and UBSan remain enabled. No kernel Task or controller setter is added.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import shutil
import sys
import tempfile
import time

sys.dont_write_bytecode = True
from file_mmap_registry_runtime_test import command, dependency_pins, pin, subreaper

ROOT = next(p for p in Path(__file__).resolve().parents if (p / 'modules/kern/src').is_dir())
KERN = ROOT / 'modules/kern/src'
UNIT = Path(__file__).resolve().parent
CORE = KERN / 'platform/mm/cpu_maintenance.cpp'
CASES = UNIT / 'cpu_maintenance_cases.cpp'
PORTS = UNIT / 'cpu_maintenance_ports.inc'
TOKEN_PORTS = UNIT / 'mmio_cache_runtime_test.cpp'
COMMON = [KERN / 'platform/mm' / (name + '.cpp') for name in
          ('mmio_cache', 'mtrr_policy', 'mmio_cache_model', 'mmio_cache_amd', 'mmio_cache_protected')]
OVERALL_SECONDS = 155


def replace_once(source: str, old: str, new: str) -> str:
    if source.count(old) != 1:
        raise ValueError(f'production anchor must occur exactly once: {old!r}')
    return source.replace(old, new, 1)


def actual_definition(source: str, signature: str, semicolon: bool = False) -> str:
    if source.count(signature) != 1:
        raise ValueError(f'actual controller definition must be unique: {signature}')
    start = source.index(signature)
    brace = source.index('{', start)
    depth = 1
    end = brace + 1
    while depth and end < len(source):
        depth += (source[end] == '{') - (source[end] == '}')
        end += 1
    if depth or (semicolon and source[end:end + 1] != ';'):
        raise ValueError(f'unterminated actual controller definition: {signature}')
    return source[start:end + int(semicolon)] + '\n'


def controller_definitions() -> tuple[str, list[Path]]:
    platform = KERN / 'platform/mm/cpu_maintenance_platform.cpp'
    wrappers = KERN / 'platform/init/init_wrappers.cpp'
    power = KERN / 'platform/power/power.cpp'
    registry = KERN / 'platform/init/init_registry.hpp'
    host_scope = KERN / 'platform/security/compartment_host.hpp'
    native, boot, finalizer = platform.read_text(), wrappers.read_text(), power.read_text()
    fragments = ['#pragma once\n#include <abi/callnums/power.h>\n',
                 'namespace ker::mod::mm::cpu_maintenance::detail {\n']
    for signature in ('Controller::~Controller()', 'auto Controller::context_status() const noexcept -> Status',
                      'Controller::Controller(const ker::init::InitExecutor& issuer) noexcept',
                      'auto Controller::validate_boot() const noexcept -> Status'):
        fragments.append(actual_definition(native if signature in native else boot, signature))
    fragments += ['}\nnamespace ker::mod::power {\n',
                  '[[noreturn]] void run_finalizer(ShutdownAction action);\n',
                  actual_definition(finalizer, 'class FinalizerExecution final', True),
                  'namespace {\n']
    for declaration in ('std::atomic<ShutdownPhase> g_phase', 'std::atomic<ShutdownAction> g_action',
                        'std::atomic<bool> g_finalizer_issued'):
        start = finalizer.index(declaration)
        fragments.append(finalizer[start:finalizer.index(';', start) + 1] + '\n')
    fragments += [actual_definition(finalizer, 'auto command_to_action('), '}\n',
                  '}\nnamespace ker::mod::security::compartment {\n',
                  actual_definition(host_scope.read_text(), '[[nodiscard]] inline auto host_namespace_admission('),
                  '}\nnamespace ker::mod::power {\nnamespace {\n',
                  actual_definition(finalizer, 'auto authorize_current_power('), '}\n']
    for signature in ('FinalizerExecution::FinalizerExecution(ShutdownAction action)',
                      '[[noreturn]] void run_finalizer(ShutdownAction action) {',
                      '[[noreturn]] void FinalizerExecution::run()'):
        value = actual_definition(finalizer, signature)
        if signature.endswith('::run()'):
            value = replace_once(value, 'asm volatile("sti" ::: "memory");', 'controller_host::enable_irq();')
            call = 'E1000_RET = ker::dev::e1000e::e1000e_shutdown(controller);'
            value = replace_once(value, call, call + '\n#ifdef WOS_HOST_ACTUAL_E1000\n'
                                 'controller_host::after_actual_retirement(controller, E1000_RET);\n#endif\n')
        fragments.append(value)
    for signature in ('auto shutdown_in_progress()', 'auto shutdown_phase()'):
        fragments.append(actual_definition(finalizer, signature))
    fragments.append('namespace {\n')
    for signature in ('auto begin_finalization(', 'auto begin_reboot_command(sched::task::Task* subject,'):
        fragments.append(actual_definition(finalizer, signature))
    fragments.append('}\n')
    for signature in ('auto begin_reboot_command(uint64_t cmd)', 'auto selftest_begin_reboot_for_subject(',
                      '[[noreturn]] void selftest_shutdown()'):
        fragments.append(actual_definition(finalizer, signature))
    fragments.append('}\nnamespace ker::mod::mm::cpu_maintenance::detail {\n')
    for signature in ('Controller::Controller(const ker::mod::power::FinalizerExecution& issuer) noexcept',
                      'auto Controller::validate_finalizer() const noexcept -> Status'):
        fragments.append(actual_definition(finalizer, signature))
    fragments += ['}\nnamespace ker::mod::mm::cpu_maintenance::detail::platform {\n',
                  'namespace observed = mmio_cache::detail::platform;\n',
                  actual_definition(native, 'auto private_protection_status() noexcept -> mmio_cache::Status'),
                  '}\nnamespace ker::init::fns {\n']
    for name in re.findall(r'^void ([a-zA-Z0-9_]+)\(\);', registry.read_text(), re.M):
        body = 'controller_host::finished();' if name == 'kernel_start' else f'controller_host::module("{name}");'
        fragments.append(f'void {name}() {{ {body} }}\n')
    wrapper = actual_definition(boot, 'void e1000e_init(const mod::mm::cpu_maintenance::detail::Controller& controller)')
    call = 'dev::e1000e::e1000e_init(controller);'
    wrapper = replace_once(wrapper, call, '\n#ifdef WOS_HOST_ACTUAL_E1000\n'
                           'controller_host::before_actual_boot_driver(controller);\n#endif\n' + call + '\n#ifdef WOS_HOST_ACTUAL_E1000\n'
                           'controller_host::after_actual_boot_driver(controller);\n#endif\n')
    fragments += [wrapper, '}\n']
    return ''.join(fragments), [platform, wrappers, power, registry, host_scope,
                               UNIT / 'cpu_maintenance_controller_ports.inc']


def controller_controls(source: str) -> dict[str, str]:
    stack = 'if (!contains(this, sizeof(*this)) || !contains(issuer, sizeof(*issuer)))'
    boot = 'mmio_cache::detail::platform::verify_boot_cpu(0, apic) != mmio_cache::Status::ready'
    phase_start = source.index('    if (phase != power::ShutdownPhase::FINALIZING')
    phase_end = source.index('return Status::invalid_context;', phase_start)
    phase = source[phase_start:phase_end]
    bad_phase = replace_once(phase, 'if (', 'if (false && (')
    bad_phase = replace_once(bad_phase, 'power::ShutdownPhase::ROOT_UNMOUNT)', 'power::ShutdownPhase::ROOT_UNMOUNT))')
    return {
        'control-controller-stack': replace_once(source, stack, 'if (false && (!contains(this, sizeof(*this)) || !contains(issuer, sizeof(*issuer))))'),
        'control-controller-boot-identity': replace_once(source, boot, '((void)apic, false)'),
        'control-controller-phase': replace_once(source, phase, bad_phase),
    }


def copies(source: str) -> dict[str, tuple[str, str, bool]]:
    finite = replace_once(source, "constexpr size_t WAIT_STEPS = 1'000'000;", 'constexpr size_t WAIT_STEPS = 4096;')
    finite = replace_once(finite, 'constexpr uint64_t MAX_READERS = CLOSED - 1;', 'constexpr uint64_t MAX_READERS = 1;')
    before = '// Join precedes every ID/payload load; a pre-CAS scanner owns no old ID.\n'
    finite = replace_once(finite, before, before + '        maintenance_host::before_join(&slot);\n')
    ack = 'row.completed[logical].store(id, std::memory_order_release);'
    finite = replace_once(finite, ack, ack + '\n        maintenance_host::after_ack(&slot, id, logical);')
    generation = replace_once(source, 'constexpr uint64_t REQUEST_ID_LIMIT = UINT64_MAX;', 'constexpr uint64_t REQUEST_ID_LIMIT = 4;')
    stale = replace_once(finite, ack, 'row.completed[logical].store(id - 1, std::memory_order_release);')
    no_reader = replace_once(finite, 'slot_ = &slot;', 'slot.gate.fetch_sub(1, std::memory_order_release);\n        slot_ = &slot;')
    no_reader = replace_once(no_reader, 'if (slot_ != nullptr) slot_->gate.fetch_sub(1, std::memory_order_release);', '')
    no_phase = replace_once(finite, 'if (phase == Phase::translations && all_completed(*slot, 0)) {',
                           'if (phase == Phase::translations) {')
    begin = stale.index('auto all_completed(')
    end = stale.index('\n}\n\nclass Reader', begin) + 2
    weakened = replace_once(stale[begin:end], '!= slot.id', '== 0')
    no_id = stale[:begin] + weakened + stale[end:]
    holder = actual_definition(source, '[[nodiscard]] auto revalidate() const -> CacheStatus {')
    wc_arm = 'mmio_cache::revalidate_current_cpu(wc_)'
    no_wc_revalidation = replace_once(source, holder, replace_once(holder, wc_arm, 'CacheStatus::ready'))
    return {
        'pristine': (source, 'CpuMaintenance.General*', False),
        'finite-pauses-and-reader-limit': (finite, 'CpuMaintenance.Hook*:CpuMaintenance.FiniteFrozen*', False),
        'finite-id-exhaustion': (generation, 'CpuMaintenance.FiniteGeneration*', False),
        'injected-stale-ack': (stale, 'CpuMaintenance.InjectedStale*', False),
        'control-reader-hold-removed': (no_reader, 'CpuMaintenance.HookAfterAckReaderTailBlocksCompletionAndSlotReuse', True),
        'control-phase-barrier-removed': (no_phase, 'CpuMaintenance.GeneralTlbDeferralCannotStartCacheAndExplicitSameIdRetryPreservesTimeoutHistory', True),
        'control-exact-id-removed': (no_id, 'CpuMaintenance.InjectedStaleCompletedIdCannotAdvanceTheRealRequest', True),
        'control-controller-stack': (source, 'CpuMaintenance.GeneralPrivateActualProcessFinalizerSealsDrainsAndRevalidatesCurrentStackAndPhase', True),
        'control-controller-phase': (source, 'CpuMaintenance.GeneralPrivateActualProcessFinalizerSealsDrainsAndRevalidatesCurrentStackAndPhase', True),
        'control-controller-boot-identity': (source, 'CpuMaintenance.GeneralPrivateActualBootIssuerKeepsBothIncomingIrqStatesAndSameTicketRetry', True),
        'control-wc-revalidation-removed': (no_wc_revalidation,
                                            'CpuMaintenance.GeneralWcActualRevalidationRefusesPatDriftBeforeAnyLocalAction', True),
    }


def gtest_context(host_build: Path) -> tuple[Path, list[dict], dict]:
    """Use the exact maintained CMake target's headers with its GTest archives."""
    database = (host_build / 'compile_commands.json').resolve(strict=True)
    cache = (host_build / 'CMakeCache.txt').resolve(strict=True)
    context_pins = [pin(database), pin(cache)]
    rows = [row for row in json.loads(database.read_text())
            if (Path(row['directory']) / row['file']).resolve() == TOKEN_PORTS]
    if len(rows) != 1:
        raise ValueError('CMake must contain exactly one maintained mmio_cache_runtime_test compile row')
    row = rows[0]
    arguments = row.get('arguments')
    if arguments is None:
        arguments = shlex.split(row['command'])
    includes = []
    index = 0
    while index < len(arguments):
        argument = arguments[index]
        value = None
        if argument in ('-I', '-isystem', '-iquote'):
            index += 1
            if index == len(arguments):
                raise ValueError('incomplete CMake include argument')
            value = arguments[index]
        elif argument.startswith('-isystem'):
            value = argument[len('-isystem'):]
        elif argument.startswith('-I'):
            value = argument[2:]
        if value:
            directory = (Path(row['directory']) / value).resolve()
            if (directory / 'gtest/gtest.h').is_file():
                includes.append(directory)
        index += 1
    roots = sorted(set(includes))
    if len(roots) != 1:
        raise ValueError('CMake compile row must select exactly one explicit GTest header root')
    header = roots[0] / 'gtest/gtest.h'
    context_pins.append(pin(header))
    if [pin(Path(item['path'])) for item in context_pins] != context_pins:
        raise ValueError('CMake/GTest context changed while selecting matching headers')
    return roots[0], context_pins, row


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--compiler', '--cxx', dest='compiler', default='/usr/bin/clang++')
    parser.add_argument('--host-build', type=Path, default=ROOT / 'build_tests')
    parser.add_argument('--evidence-dir', type=Path)
    args = parser.parse_args()
    deadline = time.monotonic() + OVERALL_SECONDS
    result = dict(pass_=False, commands=[], variants=[], sources_held=False,
                  scratch_removed=False, real_protocol_and_18b_tus=True,
                  host_instruction_ports_only=True, native_cpu_cache_proof=False, asan_fake_stack=False,
                  whole_budget_seconds=OVERALL_SECONDS)
    scratch = None
    groups: list[int] = []
    watched: list[Path] = []
    before: list[dict] = []
    try:
        driver = Path(args.compiler).absolute()
        compiler = driver.resolve(strict=True)
        if not compiler.is_file() or not os.access(compiler, os.X_OK):
            raise ValueError('--compiler must name an executable compiler ELF')
        with compiler.open('rb') as executable:
            if executable.read(4) != b'\x7fELF':
                raise ValueError('--compiler must name an executable compiler ELF')
        libs = [args.host_build / 'lib/libgtest_main.a', args.host_build / 'lib/libgtest.a']
        gtest_include, cmake_pins, compile_row = gtest_context(args.host_build)
        result['gtest_context'] = dict(include=str(gtest_include), compile_row=compile_row, pins=cmake_pins)
        controllers, controller_inputs = controller_definitions()
        watched = [*controller_inputs, Path(__file__).resolve(), CORE, CASES, PORTS, TOKEN_PORTS, compiler,
                   UNIT / 'file_mmap_registry_runtime_test.py', *COMMON, *libs,
                   *(Path(item['path']) for item in cmake_pins)]
        before = [pin(p) for p in watched]
        if [pin(Path(item['path'])) for item in cmake_pins] != cmake_pins:
            raise ValueError('CMake/GTest context changed before input pinning')
        subreaper()
        scratch = Path(tempfile.mkdtemp(prefix='wos-cpu-maintenance-', dir='/tmp'))
        prefix_source = TOKEN_PORTS.read_text()
        boundary = 'namespace {\nnamespace cache = ker::mod::mm::mmio_cache;'
        if prefix_source.count(boundary) != 1:
            raise ValueError('maintained finite18b port prefix boundary changed')
        (scratch / 'cpu_maintenance_controllers_generated.hpp').write_text(controllers)
        controller_case_sources = {}
        for name, value in controller_controls(controllers).items():
            header = scratch / (name + '-controllers.hpp')
            header.write_text(value)
            cases = scratch / (name + '-cases.cpp')
            cases.write_text('#define WOS_MAINTENANCE_CONTROLLERS_HEADER "' + header.name + '"\n'
                             '#include "' + str(CASES) + '"\n')
            controller_case_sources[name + '-cases'] = cases
        prefix = scratch / 'mmio_cache_runtime_ports_generated.hpp'
        prefix.write_text(prefix_source[:prefix_source.index(boundary)])
        hooks = scratch / 'host_hook_declarations.hpp'
        hooks.write_text('#include <cstddef>\n#include <cstdint>\nnamespace maintenance_host {\n'
                         'void before_join(void*);\nvoid after_ack(void*, uint64_t, size_t);\n}\n')
        flags = [str(driver), '-std=c++23', '-O1', '-g0', '-pipe', '-pthread', '-Wall', '-Wextra', '-Werror',
                 '-DWOS_SELFTEST=1', '-fsanitize=address,undefined', '-fsanitize-address-use-after-return=never', '-fno-sanitize-recover=all',
                 '-isystem', str(gtest_include),
                 '-fno-omit-frame-pointer', '-I', str(scratch), '-I', str(UNIT), '-I', str(KERN),
                 '-I', str(KERN / 'platform/mm'), '-I', str(ROOT / 'modules'), '-include', str(hooks)]
        variants = copies(CORE.read_text())
        sources: dict[str, Path] = {}
        for name, (source, _, _) in variants.items():
            p = scratch / (name + '.cpp')
            p.write_text(source)
            sources[name] = p
            result['variants'].append(dict(name=name, source=pin(p), compile_copy=source != CORE.read_text(),
                                           injected_stale_ack='stale' in name or 'exact-id' in name,
                                           negative_control=variants[name][2], actual_controller_fault=name.startswith('control-controller-')))
        units = [(p.stem, p) for p in COMMON] + [('cases', CASES)] + list(controller_case_sources.items()) + list(sources.items())
        dependencies: dict[str, Path] = {}
        for name, source in units:
            dep = scratch / (name + '.discovered.d')
            row = command(flags + ['-M', '-MF', str(dep), '-MT', name, str(source)],
                          scratch / (name + '-discover.log'), scratch, deadline, groups)
            result['commands'].append(row)
            if row['exit'] != 0:
                print((scratch / (name + '-discover.log')).read_text(errors='replace')[-16000:])
                raise ValueError(f'dependency discovery failed: {name}')
            for item in dependency_pins(dep): dependencies[item['path']] = Path(item['path'])
        # Discover then pin the COMPLETE compiled header/input closure before
        # compilation; compare actual compile depfiles to this immutable set.
        if [pin(p) for p in watched] != before:
            raise ValueError('original inputs changed during dependency discovery')
        watched = sorted(set(watched) | set(dependencies.values()))
        before = [pin(p) for p in watched]
        result['compiled_input_pins'] = before
        objects: dict[str, Path] = {}
        for name, source in units:
            obj = scratch / (name + '.o')
            dep = scratch / (name + '.actual.d')
            row = command(flags + ['-MD', '-MF', str(dep), '-c', str(source), '-o', str(obj)],
                          scratch / (name + '-compile.log'), scratch, deadline, groups)
            result['commands'].append(row)
            if row['exit'] != 0:
                print((scratch / (name + '-compile.log')).read_text(errors='replace')[-24000:])
                raise ValueError(f'actual full-TU compile failed: {name}')
            actual = dependency_pins(dep)
            if any(item['path'] not in dependencies for item in actual):
                raise ValueError(f'compiled undeclared input: {name}')
            objects[name] = obj
        common_objects = [str(objects[p.stem]) for p in COMMON]
        for name, (_, test_filter, control) in variants.items():
            binary = scratch / (name + '.bin')
            row = command([str(driver), '-pthread', '-fsanitize=address,undefined', '-fno-sanitize-recover=all',
                           *common_objects, str(objects.get(name + '-cases', objects['cases'])), str(objects[name]), *map(str, libs), '-o', str(binary)],
                          scratch / (name + '-link.log'), scratch, deadline, groups)
            result['commands'].append(row)
            if row['exit'] != 0:
                print((scratch / (name + '-link.log')).read_text(errors='replace')[-16000:])
                raise ValueError(f'actual full-TU link failed: {name}')
            log_path = scratch / (name + '-run.log')
            row = command([str(binary), '--gtest_filter=' + test_filter, '--gtest_color=no'],
                          log_path, scratch, deadline, groups)
            result['commands'].append(row)
            variant_receipt = next(item for item in result['variants'] if item['name'] == name)
            variant_receipt.update(binary=pin(binary), run_exit=row['exit'], test_filter=test_filter)
            log = log_path.read_text(errors='replace')
            print(f'[{name}]\n{log[-20000:]}', end='')
            if re.search(r'AddressSanitizer|runtime error:|UndefinedBehaviorSanitizer', log):
                raise ValueError(f'sanitizer failure, not a qualified oracle: {name}')
            if control:
                exact = test_filter.removeprefix('CpuMaintenance.')
                if row['exit'] != 1 or f'[  FAILED  ] CpuMaintenance.{exact}' not in log:
                    raise ValueError(f'compile-success control did not fail its exact oracle: {name}')
            elif row['exit'] != 0 or not re.search(r'\[  PASSED  \] [1-9][0-9]* tests?\.', log):
                raise ValueError(f'actual protocol oracle failed: {name}')
            # command() has reaped the exact owned session before disposable
            # compiler outputs are removed; source copies/logs stay for pins.
            binary.unlink()
            objects[name].unlink()
        result['pass_'] = True
    except (OSError, ValueError, TimeoutError, KeyboardInterrupt) as error:
        result['error'] = f'{type(error).__name__}: {error}'
    finally:
        if before:
            try: result['sources_held'] = [pin(p) for p in watched] == before
            except OSError: result['sources_held'] = False
            if not result['sources_held']: result['pass_'] = False
        if args.evidence_dir is not None:
            try:
                args.evidence_dir.mkdir(parents=True, exist_ok=True)
                if scratch is not None:
                    for p in scratch.glob('*.log'):
                        destination = args.evidence_dir / p.name
                        if destination.exists(): raise ValueError(f'evidence exists: {destination}')
                        shutil.copyfile(p, destination)
            except (OSError, ValueError) as error:
                result['pass_'] = False
                result['evidence_error'] = str(error)
        if scratch is not None and not groups:
            shutil.rmtree(scratch)
            result['scratch_removed'] = not scratch.exists()
        if groups:
            result['pass_'] = False
            result['cleanup_error'] = f'owned sessions remain: {groups}; scratch preserved'
        if time.monotonic() > deadline:
            result['pass_'] = False
            result['deadline_error'] = 'whole invocation/cleanup deadline exceeded'
        if args.evidence_dir is not None:
            try:
                destination = args.evidence_dir / 'result.json'
                if destination.exists(): raise ValueError('result evidence already exists')
                destination.write_text(json.dumps(result, indent=2) + '\n')
            except (OSError, ValueError) as error:
                result['pass_'] = False
                result['evidence_error'] = str(error)
        print(json.dumps(result))
    return 0 if result['pass_'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
