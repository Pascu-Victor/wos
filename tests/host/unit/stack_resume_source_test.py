#!/usr/bin/env python3
"""Keep cleanup on the incoming stack, before publication, in KASan only."""
from pathlib import Path

root = Path(__file__).resolve().parents[3]
asm = (root / 'modules/kern/src/platform/sys/context_switch.asm').read_text()
cpp = (root / 'modules/kern/src/platform/sys/context_switch.cpp').read_text()
wrapper = asm.split('%macro build_kernel_return_from_stack 0', 1)[1].split('%endmacro', 1)[0]
instructions = [line.split(';', 1)[0].strip() for line in wrapper.splitlines()]
assert [line for line in instructions if line] == [
    'lea rsi, [rsp + GPREGS_SIZE]', 'mov rdi, rsp', 'build_kernel_return_from_ptrs']
body = asm.split('%macro build_kernel_return_from_ptrs 0', 1)[1].split('%endmacro', 1)[0]
body = '\n'.join(line.split(';', 1)[0] for line in body.splitlines())
tokens = ('mov r15, r10', 'mov rsp, r10', 'and rsp, -16', '%ifdef WOS_KASAN',
          'lea rdi, [r15 + KERNEL_RETURN_BLOCK_SIZE]',
          'call wos_kasan_prepare_kernel_resume', '%endif',
          'call wos_commit_handoff_task', 'mov rsp, r15', 'popq',
          'add rsp, INTERRUPT_FRAME_SIZE', 'iretq')
offset = 0
for token in tokens:
    offset = body.index(token, offset) + len(token)
assert body.count('call wos_kasan_prepare_kernel_resume') == 1
user = asm.split('%macro build_user_return_from_ptrs_late_commit 0', 1)[1].split('%endmacro', 1)[0]
incoming, same = user.split('%%same_stack_return:', 1)
first = asm.split('wos_asm_enter_usermode:', 1)[1].split(';clear registers', 1)[0]
for body, publication in ((incoming, True), (same, True), (first, False)):
    tokens = ['mov r14, rsp', 'and rsp, -16', '%ifdef WOS_KASAN',
              'mov rdi, rsp', 'call wos_kasan_prepare_kernel_resume', '%endif']
    if publication:
        tokens.append('call wos_commit_handoff_task')
    tokens += ['call wos_restore_return_task_fpu', 'call wos_record_exec_user_return', 'mov rsp, r14']
    offset = body.index('mov rsp, r10') if body is incoming else 0
    for token in tokens:
        offset = body.index(token, offset) + len(token)
    assert body.count('call wos_kasan_prepare_kernel_resume') == 1
helper = cpp.split('void wos_kasan_prepare_kernel_resume', 1)[1].split('#endif', 1)[0]
assert 'no_sanitize("address", "undefined", "coverage")' in cpp.split('void wos_kasan_prepare_kernel_resume', 1)[0][-150:]
assert 'sched::get_return_task()' in helper
assert 'TASK->context.syscall_kernel_stack, mm::KERNEL_STACK_SIZE, resume_rsp' in helper
assert helper.index('stack_resume_range') < helper.index('kasan::unpoison_range')
print('KASan incoming-stack resume boundary is source covered')
