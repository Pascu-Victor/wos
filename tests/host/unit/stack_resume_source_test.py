#!/usr/bin/env python3
"""Keep cleanup on the incoming stack, before publication, in KASan only."""
from pathlib import Path

root = Path(__file__).resolve().parents[3]
asm = (root / 'modules/kern/src/platform/sys/context_switch.asm').read_text()
cpp = (root / 'modules/kern/src/platform/sys/context_switch.cpp').read_text()
for name in ('build_kernel_return_from_stack', 'build_kernel_return_from_ptrs'):
    body = asm.split(f'%macro {name} 0', 1)[1].split('%endmacro', 1)[0]
    tokens = ('mov rsp, r10', '%ifdef WOS_KASAN',
              'lea rdi, [rsp + 8 + KERNEL_RETURN_BLOCK_SIZE]',
              'call wos_kasan_prepare_kernel_resume', '%endif',
              'call wos_commit_handoff_task', 'popq', 'iretq')
    offset = 0
    for token in tokens:
        offset = body.index(token, offset) + len(token)
helper = cpp.split('void wos_kasan_prepare_kernel_resume', 1)[1].split('#endif', 1)[0]
assert 'no_sanitize("address", "undefined", "coverage")' in cpp.split('void wos_kasan_prepare_kernel_resume', 1)[0][-150:]
assert 'sched::get_return_task()' in helper
assert 'TASK->context.syscall_kernel_stack, mm::KERNEL_STACK_SIZE, resume_rsp' in helper
assert helper.index('stack_resume_range') < helper.index('kasan::unpoison_range')
print('KASan incoming-stack resume boundary is source covered')
