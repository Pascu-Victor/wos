bits 64
section .text
%include "platform/asm/helpers.asm"
extern syscallHandler
global _wOS_asm_syscallHandler

_wOS_asm_syscallHandler:
    swapgs
    ; Save RCX and R11 IMMEDIATELY - they hold return RIP and RFLAGS
    ; These must be saved before any code can clobber them
    mov [gs:0x28], rcx ; save return RIP (syscall stores it in RCX)
    mov [gs:0x30], r11 ; save RFLAGS (syscall stores it in R11)
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

    ; Check if we need to perform a deferred task switch
    ; Get current task from scheduler
    extern _wOS_getCurrentTask
    extern _wOS_DEFERRED_TASK_SWITCH_OFFSET
    sub rsp, 8  ; Align stack for call
    call _wOS_getCurrentTask
    add rsp, 8  ; Restore stack

    ; rax now contains current task pointer
    ; Check if deferredTaskSwitch flag is set at the calculated offset
    mov r8, [rel _wOS_DEFERRED_TASK_SWITCH_OFFSET]  ; Load offset value
    movzx edx, byte [rax + r8]                      ; Load flag from task at offset
    cmp edx, 0
    je .no_deferred_switch

    ; Call deferredTaskSwitch with GPRegs and frame pointers
    extern deferredTaskSwitch
    lea rdi, [rsp+8]       ; GPRegs pointer
    lea rsi, [rsp+136]     ; Frame pointer (after GPRegs)
    sub rsp, 8  ; Align stack for call
    call deferredTaskSwitch
    ; deferredTaskSwitch will not return - it switches tasks

.no_deferred_switch:
    ; restore usermode segment ds and es
    mov ds, [gs:0x18]
    mov es, [gs:0x20]

    popq
    pop rax

    ; DIAGNOSTIC: Validate RCX before sysret.
    ; RCX should match the user return RIP saved at syscall entry (gs:0x28).
    ; If it doesn't, the kernel stack was corrupted during the syscall handler.
    ; Must check BEFORE swapgs since we need kernel GS to read the scratch area.
    cmp rcx, [gs:0x28]
    jne .sysret_rcx_corrupt

    mov rsp, [gs:0x08] ; restore usermode stack
    swapgs
    sti
    o64 sysret

.sysret_rcx_corrupt:
    ; RCX was corrupted — don't sysret or we'll execute at a wrong/kernel address.
    ; Call C diagnostic function. Stack is at kernel stack top, safe for calls.
    ; Save rax (syscall return value) so the panic handler can report it.
    push rax
    extern _wOS_sysret_corrupt_panic
    mov rdi, rcx         ; arg1 = actual RCX (corrupted value)
    mov rsi, [gs:0x28]   ; arg2 = expected RCX (saved at entry)
    mov rdx, [gs:0x08]   ; arg3 = user RSP
    sub rsp, 8           ; align stack for call
    call _wOS_sysret_corrupt_panic
    ; should not return, but halt just in case
.sysret_halt:
    hlt
    jmp .sysret_halt
