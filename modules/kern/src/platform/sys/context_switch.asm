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
    ; Check if returning to userspace (CS at offset 24 == 0x23)
    cmp qword [rsp + 24], qword 0x23
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
    ; Check if returning to userspace (CS at offset 24 == 0x23)
    cmp qword [rsp + 24], qword 0x23
    jne .no_swapgs_exit_jump
    swapgs
    .no_swapgs_exit_jump:
    add rsp, 16  ; Skip intNum and errCode
    sti
    iretq

; Return from deferred task switch
; rdi = pointer to GPRegs structure in memory
; rsi = pointer to interruptFrame structure in memory
;
; GPRegs layout (120 bytes): r15, r14, r13, r12, r11, r10, r9, r8, rbp, rdi, rsi, rdx, rcx, rbx, rax
; interruptFrame layout (56 bytes): intNum, errCode, rip, cs, flags, rsp, ss
;
; We need to:
; 1. Build a proper stack with interruptFrame for iretq
; 2. Restore all GPRegs
; 3. Check if we need swapgs (if returning to userspace)
; 4. Execute iretq
global _wOS_deferredTaskSwitchReturn
_wOS_deferredTaskSwitchReturn:
    ; rdi = GPRegs*, rsi = interruptFrame*

    ; First, build the interrupt frame on the stack for iretq
    ; iretq expects (from bottom to top): ss, rsp, flags, cs, rip
    ; We push in reverse order
    push qword [rsi + 48]    ; ss
    push qword [rsi + 40]    ; rsp
    push qword [rsi + 32]    ; flags
    push qword [rsi + 24]    ; cs
    push qword [rsi + 16]    ; rip

    ; Save cs value for swapgs check later (use stack space)
    mov rax, [rsi + 24]
    push rax                 ; save cs for later check

    ; Now restore all GPRegs from memory
    ; We need to be careful about order - restore rdi last since we need it
    mov r15, [rdi + 0]
    mov r14, [rdi + 8]
    mov r13, [rdi + 16]
    mov r12, [rdi + 24]
    mov r11, [rdi + 32]
    mov r10, [rdi + 40]
    mov r9,  [rdi + 48]
    mov r8,  [rdi + 56]
    mov rbp, [rdi + 64]
    ; skip rdi (offset 72) for now
    mov rsi, [rdi + 80]
    mov rdx, [rdi + 88]
    mov rcx, [rdi + 96]
    mov rbx, [rdi + 104]
    mov rax, [rdi + 112]

    ; Now restore rdi
    mov rdi, [rdi + 72]

    ; Check if we need swapgs (cs == 0x1b means userspace)
    cmp qword [rsp], 0x1b
    jne .no_swapgs_deferred
    swapgs
.no_swapgs_deferred:
    ; Remove the saved cs from stack
    add rsp, 8

    ; Enable interrupts and return
    sti
    iretq



