bits 64
section .text
%include "platform/asm/helpers.asm"
extern syscallHandler
global _wOS_asm_syscallHandler

_wOS_asm_syscallHandler:
    swapgs
    ; ffff800000000000
    mov [gs:0x08], rsp ; save usermode stack
    mov rsp, [gs:0x0] ; switch to kernel stack

    ;syscall return value
    sub rsp, 8
    pushq
    cld

    ; save usermode segment ds and es
    mov [gs:0x18], ds
    mov [gs:0x20], es
    mov ax, 0x10
    mov ds, ax
    mov es, ax

    ; push qword 0x1b       ; usermode data segment
    ; push qword [gs:0x08]  ; usermode stack
    ; push r11              ; usermode rflags
    ; push qword 0x23       ; usermode code segment
    ; push rcx              ; usermode rip

    ; Pass pointer to saved registers (GPRegs) as first argument
    ; Skip the 8-byte return value slot
    lea rdi, [rsp+8]
    xor rbp, rbp

    call syscallHandler
    ; save return value
    mov [rsp+0x78], rax

    ; restore usermode segment ds and es
    mov ds, [gs:0x18]
    mov es, [gs:0x20]

    popq
    pop rax
    mov rsp, [gs:0x08] ; restore usermode stack
    swapgs
    sti
    o64 sysret
