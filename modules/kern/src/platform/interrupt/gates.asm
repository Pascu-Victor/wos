bits 64

extern iterrupt_handler

%macro pushl 0
    push rax
    push rbx
    push rcx
    push rdx
    push rsi
    push rdi
    push rbp
    push r8
    push r9
    push r10
    push r11
    push r12
    push r13
    push r14
    push r15
%endmacro

%macro popl 0
    pop r15
    pop r14
    pop r13
    pop r12
    pop r11
    pop r10
    pop r9
    pop r8
    pop rbp
    pop rdi
    pop rsi
    pop rdx
    pop rcx
    pop rbx
    pop rax
%endmacro

%macro isr_swapgs 1
    cmp [rsp + 24], dword 8 ; Check if we're in userspace
    je .%1
    swapgs
    .%1:
%endmacro

__idt_isr_handler:
    cld
    isr_swapgs entry
    pushl
    mov rdi, rsp
    call iterrupt_handler
    popl
    isr_swapgs exit
    add rsp, 16
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
        mov gs, ax
        mov ss, ax
        sti
        ret

%macro isr 1
global isr%1
isr%1:
    push 0  ; No-error code
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

global isr32
extern task_switch_handler
isr32:
    push 0
    push 32
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
