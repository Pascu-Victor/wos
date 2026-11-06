#!/usr/bin/env python3
"""Link the actual optional-MSR TU/NASM and execute its real recovery hook.

CPUID and SIDT are finite host ports. No RDMSR executes on the host. The NASM
hook is copied byte-for-byte from gates.asm into a host continuation wrapper;
the only substituted instruction in actual IDTR validation is the SIDT port.
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
from file_mmap_registry_runtime_test import command, pin, subreaper
from native_lazy_file_fault_runtime_test import definition

ROOT = next(p for p in Path(__file__).resolve().parents if (p / 'modules/kern/src').is_dir())
KERN = ROOT / 'modules/kern/src'
UNIT = Path(__file__).resolve().parent


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument('--cxx', default=os.environ.get('WOS_HOST_CXX', '/usr/bin/clang++'))
    parser.add_argument('--nasm', default=shutil.which('nasm'))
    args = parser.parse_args()
    result = dict(pass_=False, commands=[], scratch_removed=False, host_msr_executed=False)
    groups: list[int] = []
    scratch = None
    watched = [KERN / path for path in (
        'platform/asm/cpu_safe_msr.hpp', 'platform/asm/cpu_safe_msr.cpp',
        'platform/asm/cpu_safe_msr.asm', 'platform/asm/cpu.hpp', 'platform/asm/msr.hpp',
        'platform/interrupt/idt.hpp', 'platform/interrupt/idt.cpp',
        'platform/interrupt/gates.hpp', 'platform/interrupt/gates.asm')]
    watched += [Path(__file__).resolve(), UNIT / 'safe_msr_runtime_cases.inc',
                UNIT / 'file_mmap_registry_runtime_test.py', UNIT / 'native_lazy_file_fault_runtime_test.py']
    before = []
    deadline = time.monotonic() + 75
    try:
        if not args.nasm:
            raise ValueError('NASM is required; the real production instruction labels must be linked')
        compiler = Path(args.cxx).absolute()
        assembler = Path(args.nasm).absolute()
        watched += [compiler.resolve(strict=True), assembler.resolve(strict=True)]
        before = [pin(p) for p in watched]
        subreaper()
        scratch = Path(tempfile.mkdtemp(prefix='wos-safe-msr-', dir='/tmp'))
        shim = scratch / 'include'
        asm_dir, irq_dir = shim / 'platform/asm', shim / 'platform/interrupt'
        asm_dir.mkdir(parents=True)
        irq_dir.mkdir(parents=True)
        cpu = (KERN / 'platform/asm/cpu.hpp').read_text()
        gates = (KERN / 'platform/interrupt/gates.hpp').read_text()
        idt = (KERN / 'platform/interrupt/idt.hpp').read_text()
        idt_cpp = (KERN / 'platform/interrupt/idt.cpp').read_text()
        actual = []

        def take(source: str, anchor: str, declaration: bool = False) -> str:
            body, row = definition(source, anchor, declaration)
            actual.append(row)
            return body

        def packed(source: str, name: str) -> str:
            match = re.search(r'struct ' + name + r' \{[\s\S]*?\} __attribute__\(\(packed\)\);', source)
            if not match:
                raise ValueError(f'actual packed {name} declaration missing')
            actual.append(dict(anchor=name, bytes=len(match[0].encode()),
                               sha256=hashlib.sha256(match[0].encode()).hexdigest()))
            return match[0]

        feature = re.search(r'^inline constexpr uint32_t CPUID_FEAT_EDX_MSR = .*;',
                            (KERN / 'platform/asm/msr.hpp').read_text(), re.M)
        if not feature:
            raise ValueError('actual CPUID.MSR definition missing')
        (asm_dir / 'cpu.hpp').write_text('#pragma once\n#include <cstdint>\n' + feature[0] +
            '\nnamespace ker::mod::cpu {\n' + take(cpu, 'struct CpuidContext {', True) + '\n' +
            packed(cpu, 'GPRegs') + '\nvoid cpuid(CpuidContext*);\n}\n')
        (irq_dir / 'gates.hpp').write_text('#pragma once\n#include <cstdint>\nnamespace ker::mod::gates {\n' +
                                          packed(gates, 'InterruptFrame') + '\n}\n')
        (irq_dir / 'idt.hpp').write_text('#pragma once\n#include <cstdint>\nnamespace ker::mod::desc::idt {\n' +
            packed(idt, 'IdtPtr') + '\nbool is_idt_ready();\nbool kernel_idt_loaded_on_current_cpu();\n}\n')
        ready = re.search(r'^constexpr uint8_t IDT_READY = .*;', idt_cpp, re.M)
        if not ready:
            raise ValueError('actual ready-state value missing')
        local = take(idt_cpp, 'auto kernel_idt_loaded_on_current_cpu()')
        instruction = '__asm__ volatile("sidt %0" : "=m"(current) : : "memory");'
        if local.count(instruction) != 1:
            raise ValueError('actual SIDT instruction port drifted')
        local = local.replace(instruction, 'host_sidt(current);')
        port = '#include <platform/interrupt/idt.hpp>\nnamespace ker::mod::desc::idt {\n' + ready[0] + \
            '\nuint8_t idt_ready_state{};\nIdtPtr idt_ptr{};\nvoid host_sidt(IdtPtr&);\n' + \
            take(idt_cpp, 'auto is_idt_ready()') + '\n' + local + '\n}\n'
        (scratch / 'cases.cpp').write_text(port + (UNIT / 'safe_msr_runtime_cases.inc').read_text())
        source = (KERN / 'platform/interrupt/gates.asm').read_text()
        begin = source.index('    ; WOS_SAFE_MSR_KERNEL_HOOK_BEGIN')
        end = source.index('    ; WOS_SAFE_MSR_KERNEL_HOOK_END', begin)
        hook = source[begin:end]
        actual.append(dict(anchor='actual NASM kernel hook', bytes=len(hook.encode()),
                           sha256=hashlib.sha256(hook.encode()).hexdigest()))
        prefix = source[:begin]
        if not prefix.endswith('    pushq\n') or 'test qword [rsp + 24], 0x3\n    jnz .outer_entry' not in prefix:
            raise ValueError('actual CPL0 saved-register hook placement drifted')
        (scratch / 'hook.asm').write_text('bits 64\nsection .text\nextern wos_try_recover_msr_frame\n'
            'global host_safe_msr_hook:function\nhost_safe_msr_hook:\n'
            '    push r12\n    push r13\n    mov r13, rsp\n    mov rsp, rdi\n' + hook +
            '    xor eax, eax\n    jmp .host_return\n.kernel_return:\n    mov eax, 1\n.host_return:\n'
            '    mov rsp, r13\n    pop r13\n    pop r12\n    ret\n'
            'section .note.GNU-stack noalloc noexec nowrite progbits\n')
        common = ['-std=c++23', '-O1', '-g0', '-Wall', '-Wextra', '-Werror',
                  '-I', str(shim), '-I', str(KERN), '-I', str(ROOT / 'modules')]
        plans = [
            [str(assembler), '-f', 'elf64', str(KERN / 'platform/asm/cpu_safe_msr.asm'), '-o', str(scratch / 'probe.o')],
            [str(assembler), '-f', 'elf64', str(scratch / 'hook.asm'), '-o', str(scratch / 'hook.o')],
            [str(compiler), *common, '-fno-stack-protector', '-c', str(KERN / 'platform/asm/cpu_safe_msr.cpp'),
             '-o', str(scratch / 'actual.o')],
            [str(compiler), *common, '-fsanitize=address,undefined', '-fno-sanitize-recover=all',
             '-fno-omit-frame-pointer', str(scratch / 'cases.cpp'), str(scratch / 'actual.o'),
             str(scratch / 'probe.o'), str(scratch / 'hook.o'), '-Wl,--wrap=wos_try_recover_msr_frame',
             '-o', str(scratch / 'safe-msr')],
            [str(scratch / 'safe-msr')],
        ]
        for index, argv in enumerate(plans):
            log = scratch / f'command-{index}.log'
            row = command(argv, log, scratch, deadline, groups)
            result['commands'].append(row)
            output = log.read_text(errors='replace')
            if row['exit'] != 0:
                print(output[-32768:])
                raise ValueError(f'command {index} failed')
            if index == len(plans) - 1:
                print(output, end='')
                if not re.search(r'^PASS safe_msr cases=4 assertions=\d+ failures=0 host_msr_executed=0$', output, re.M):
                    raise ValueError('actual-source oracle did not report complete success')
        result['actual_definitions'] = actual
        result['source_pins'] = before
        result['pass_'] = True
    except (OSError, ValueError, TimeoutError, KeyboardInterrupt) as error:
        result['error'] = f'{type(error).__name__}: {error}'
    finally:
        if scratch is not None and not groups:
            shutil.rmtree(scratch)
            result['scratch_removed'] = not scratch.exists()
        if before:
            result['sources_held'] = [pin(p) for p in watched] == before
            result['pass_'] &= result['sources_held']
        if groups:
            result['pass_'] = False
            result['cleanup_error'] = f'owned groups remain {groups}; scratch preserved'
        if time.monotonic() > deadline:
            result['pass_'] = False
            result['deadline_error'] = 'whole75-second invocation including cleanup exceeded'
        print(json.dumps(result))
    return 0 if result['pass_'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
