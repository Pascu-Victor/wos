bits 64

extern iterrupt_handler

%include "platform/asm/helpers.asm"

%assign GPREGS_SIZE 120
%assign INTERRUPT_FRAME_SIZE 56

%macro isr_swapgs 1
    cmp [rsp + 24], dword 8 ; Check if we're in userspace
    je .%1
    swapgs
    .%1:
%endmacro

__idt_isr_handler:
    cld
    clear_live_tf

    ; Same-CPL kernel interrupts/exceptions only provide RIP, CS, and RFLAGS
    ; after our int/error-code shim. Build a by-value copy with synthetic
    ; RSP/SS for C++ handlers, then return through the original hardware frame.
    cmp qword [rsp + 24], qword 0x08
    jne .outer_entry

    pushq
    mov r10, rsp
    sub rsp, GPREGS_SIZE + INTERRUPT_FRAME_SIZE

    %assign j 0
    %rep 15
        mov r11, [r10 + j * 8]
        mov [rsp + j * 8], r11
        %assign j j + 1
    %endrep

    mov r11, [r10 + GPREGS_SIZE + 0x00] ; int_num
    mov [rsp + GPREGS_SIZE + 0x00], r11
    mov r11, [r10 + GPREGS_SIZE + 0x08] ; err_code
    mov [rsp + GPREGS_SIZE + 0x08], r11
    mov r11, [r10 + GPREGS_SIZE + 0x10] ; RIP
    mov [rsp + GPREGS_SIZE + 0x10], r11
    mov r11, [r10 + GPREGS_SIZE + 0x18] ; CS
    mov [rsp + GPREGS_SIZE + 0x18], r11
    mov r11, [r10 + GPREGS_SIZE + 0x20] ; RFLAGS
    mov [rsp + GPREGS_SIZE + 0x20], r11
    lea r11, [r10 + GPREGS_SIZE + 0x28] ; interrupted RSP
    mov [rsp + GPREGS_SIZE + 0x28], r11
    mov qword [rsp + GPREGS_SIZE + 0x30], 0x10

    mov rdi, rsp
    call iterrupt_handler

    mov r10, [rsp + GPREGS_SIZE + 0x28]
    sub r10, 24
    mov r11, [rsp + GPREGS_SIZE + 0x10]
    mov [r10 + 0x00], r11
    mov r11, [rsp + GPREGS_SIZE + 0x18]
    mov [r10 + 0x08], r11
    mov r11, [rsp + GPREGS_SIZE + 0x20]
    mov [r10 + 0x10], r11

    popq
    mov rsp, [rsp + 40]
    sub rsp, 24
    iretq

.outer_entry:
    isr_swapgs entry
    pushq
    mov rdi, rsp
    call iterrupt_handler
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
    cld
    clear_live_tf

    ; Same-CPL kernel interrupts only push RIP, CS, and RFLAGS. The scheduler
    ; consumes a uniform InterruptFrame with RSP and SS as well, so build that
    ; full frame explicitly below the hardware frame instead of letting C++
    ; write synthetic RSP/SS into stack slots that do not exist.
    cmp qword [rsp + 8], qword 0x08
    jne .user_or_outer

    sub rsp, GPREGS_SIZE + INTERRUPT_FRAME_SIZE

    mov [rsp + 0x00], r15
    mov [rsp + 0x08], r14
    mov [rsp + 0x10], r13
    mov [rsp + 0x18], r12
    mov [rsp + 0x20], r11
    mov [rsp + 0x28], r10
    mov [rsp + 0x30], r9
    mov [rsp + 0x38], r8
    mov [rsp + 0x40], rbp
    mov [rsp + 0x48], rdi
    mov [rsp + 0x50], rsi
    mov [rsp + 0x58], rdx
    mov [rsp + 0x60], rcx
    mov [rsp + 0x68], rbx
    mov [rsp + 0x70], rax

    mov qword [rsp + GPREGS_SIZE + 0x00], 32
    mov qword [rsp + GPREGS_SIZE + 0x08], 0

    mov rax, [rsp + GPREGS_SIZE + INTERRUPT_FRAME_SIZE + 0x00] ; RIP from hardware frame
    mov [rsp + GPREGS_SIZE + 0x10], rax
    mov rax, [rsp + GPREGS_SIZE + INTERRUPT_FRAME_SIZE + 0x08] ; CS from hardware frame
    mov [rsp + GPREGS_SIZE + 0x18], rax
    mov rax, [rsp + GPREGS_SIZE + INTERRUPT_FRAME_SIZE + 0x10] ; RFLAGS from hardware frame
    mov [rsp + GPREGS_SIZE + 0x20], rax

    lea rax, [rsp + GPREGS_SIZE + INTERRUPT_FRAME_SIZE + 0x18] ; interrupted RSP
    mov [rsp + GPREGS_SIZE + 0x28], rax
    mov qword [rsp + GPREGS_SIZE + 0x30], 0x10

    jmp task_switch_handler

.user_or_outer:
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
