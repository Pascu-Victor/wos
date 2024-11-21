bits 64
section .text
%include "platform/asm/helpers.asm"
extern syscallHandler
global _wOS_asm_syscallHandler

_wOS_asm_syscallHandler:
    swapgs

    mov qword gs:0x16, qword 1
    mov qword gs:0x08, rsp ; usermode stack
    mov rsp, qword gs:0x0

    pushl ; TODO: user has to do this
    mov rbp, qword 0
    call syscallHandler
    popl

    mov rsp, qword gs:0x08 ; restore usermode stack
    mov qword gs:0x16, qword 0
    swapgs

    o64 sysret
