bits 64

%include "platform/asm/helpers.asm"

%macro isr_swapgs 1
    cmp [rsp + 24], dword 8 ; Check if we're in userspace
    je .%1
    swapgs
    .%1:
%endmacro

global _wOS_asm_enterUsermode
_wOS_asm_enterUsermode:
    ;clear registers
    xor rax, rax
    xor rdx, rdx
    ; stack remains
    xor r8, r8
    xor r9, r9
    xor r10, r10
    xor r11, r11
    xor r12, r12
    xor r13, r13
    xor r14, r14
    xor r15, r15

    ; save kernel stack pointer to local cpu data
    mov [gs:0x0], rsp

    swapgs
    ; init usermode stack on rsi
    mov rsp, rsi

    ; According to x86-64 System V ABI, RBP should be 0 at program entry
    xor rbp, rbp

    ; set segment selectors
    mov ax, 0x1B ; RPL 11 -> RING 3 AND THIRD GDT SELECTOR
    mov ds, ax
    mov es, ax

    rdfsbase rbx
    rdgsbase rcx

    mov fs, ax
    mov gs, ax

    wrfsbase rbx
    wrgsbase rcx

    xor rbx, rbx
    xor rcx, rcx

    ; sysret params
    mov rcx, rdi   ; set RIP
    mov r11, 0x202 ; RFLAGS IF=1 and RESERVED=1
    o64 sysret

extern _wOS_schedTimer
global task_switch_handler
task_switch_handler:
    mov rdi, rsp
    call _wOS_schedTimer

    popq
    cmp qword [rsp + 32], qword 0x1b
    jne .no_swapgs_exit
    swapgs
    .no_swapgs_exit:
    add rsp, 16  ; Skip intNum and errCode
    sti
    iretq

; Jump to next task without saving current task state
; Used when a task is exiting and doesn't need its context preserved
extern _wOS_jumpToNextTaskNoSave
global jump_to_next_task_no_save
jump_to_next_task_no_save:
    ; Push dummy GPRegs structure
    push 0  ; rax
    push 0  ; rbx
    push 0  ; rcx
    push 0  ; rdx
    push 0  ; rsi
    push 0  ; rdi
    push 0  ; rbp
    push 0  ; r8
    push 0  ; r9
    push 0  ; r10
    push 0  ; r11
    push 0  ; r12
    push 0  ; r13
    push 0  ; r14
    push 0  ; r15

    ; Push dummy interrupt frame
    push 0  ; SS
    push 0  ; RSP
    push 0  ; RFLAGS
    push 0  ; CS
    push 0  ; RIP
    push 0  ; errCode
    push 0  ; intNum

    mov rdi, rsp
    call _wOS_jumpToNextTaskNoSave

    popq
    cmp qword [rsp + 32], qword 0x1b
    jne .no_swapgs_exit
    swapgs
    .no_swapgs_exit:
    add rsp, 16  ; Skip intNum and errCode
    sti
    iretq
