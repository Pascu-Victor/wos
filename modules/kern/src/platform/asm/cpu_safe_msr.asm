bits 64
section .text

global wos_optional_msr_read:function hidden
global wos_optional_msr_fault:hidden
global wos_optional_msr_recovery:hidden

; SysV: EDI=index, RSI=kernel output pointer. No callee-saved register is used.
wos_optional_msr_read:
    mov ecx, edi
wos_optional_msr_fault:
    rdmsr
    shl rdx, 32
    or rax, rdx
    mov [rsi], rax
    mov eax, 1
    ret
wos_optional_msr_recovery:
    xor eax, eax
    ret

%ifdef WOS_SELFTEST
global wos_selftest_unmatched_msr_fault:function hidden
global wos_selftest_unmatched_msr_fault_site:hidden
wos_selftest_unmatched_msr_fault:
    ; Caller proves x2APIC mode. SELF_IPI is architecturally write-only:
    ; this genuine read must fault, with no APIC write or interrupt side effect.
    mov ecx, 0x83f
wos_selftest_unmatched_msr_fault_site:
    rdmsr
    ud2
%endif

section .note.GNU-stack noalloc noexec nowrite progbits
