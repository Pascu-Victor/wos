bits 64

extern iterrupt_handler
extern wos_user_interrupt_save_fpu
extern wos_user_interrupt_restore_fpu
extern wos_smap_active

%include "platform/asm/helpers.asm"

%macro isr_swapgs 1
    test qword [rsp + 24], 0x3 ; swap only when the interrupted frame came from user CPL
    jz .%1
    swapgs
    .%1:
%endmacro

__idt_isr_handler:
    cmp byte [rel wos_smap_active], 0
    je .access_closed
    clac
.access_closed:
    cld
    clear_live_tf

    ; Long-mode hardware saves SS:RSP even when CPL does not change. With
    ; the vector/error shim, both origins already provide InterruptFrame.
    test qword [rsp + 24], 0x3
    jnz .outer_entry

    pushq
    mov rdi, rsp
    call iterrupt_handler

    popq
    add rsp, 16
    iretq

.outer_entry:
    isr_swapgs entry
    pushq
    call wos_user_interrupt_save_fpu
    mov r12, rax
    mov rdi, rsp
    call iterrupt_handler
    mov rdi, r12
    call wos_user_interrupt_restore_fpu
    popq
    isr_swapgs exit
    add rsp, 16
    ; Don't manually enable interrupts - iretq will restore them from saved RFLAGS
    ; Having sti here can cause race conditions if another interrupt arrives between sti and iretq
    iretq

global load_idt
load_idt:
    lidt [rdi]

    push 0x08
    lea rax, [rel .reload]
    push rax
    retfq
    .reload:
        mov ax, 0x10
        mov ds, ax
        mov es, ax
        mov fs, ax
        ; NOTE: Do NOT load GS here - we use GS.base for per-CPU data
        ; and loading a selector into GS zeroes GS.base on x86-64
        mov ss, ax
        sti
        ret

%macro isr 1
global isr%1
isr%1:
    push -1  ; No-error code
    push %1 ; INT number
    jmp __idt_isr_handler
%endmacro

%macro isr_except 1
global isr_except%1
isr_except%1:
    ; code pushed by the CPU
    push %1 ; INT number
    jmp __idt_isr_handler
%endmacro

; interupt service routines
%assign i 0
%rep 8
    isr i
%assign i i + 1
%endrep

isr_except 8
isr 9
isr_except 10
isr_except 11
isr_except 12
isr_except 13
isr_except 14

%assign i 15
%rep 17
    isr i
%assign i i + 1
%endrep

; IRQ32 (timer) needs special handling for context switching
; We can't go through iterrupt_handler because it receives the frame by value (copy),
; so modifications to the frame don't affect the actual stack
global isr32
extern task_switch_handler
isr32:
    cmp byte [rel wos_smap_active], 0
    je .access_closed
    clac
.access_closed:
    cld
    clear_live_tf

    ; The native five-word frame includes the exact interrupted RSP and SS
    ; for kernel and user origins. Only the vector/error shim is missing.
    push qword 0   ; err_code (none for IRQ)
    push qword 32  ; int_num
    isr_swapgs isr32_entry
    pushq
    jmp task_switch_handler

%assign i 33
%rep 15
    isr i
%assign i i + 1
%endrep

%assign i 48
%rep 208
    isr i
%assign i i + 1
%endrep
