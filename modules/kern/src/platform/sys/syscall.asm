bits 64
section .text
%include "platform/asm/helpers.asm"
extern syscall_handler
global wos_asm_syscall_handler

wos_asm_syscall_handler:
    swapgs
    ; Validate the kernel scratch stack before writing any syscall-entry state
    ; through %gs. If KERNEL_GS_BASE is stale, those writes corrupt whichever
    ; object %gs happens to point at and make the later fault look unrelated.
    cmp dword [gs:0x04], 0xffff8000
    jb .bad_syscall_stack
    cmp dword [gs:0x04], 0xffff9000
    jae .bad_syscall_stack

    ; Save RCX and R11 IMMEDIATELY - they hold return RIP and RFLAGS
    ; These must be saved before any code can clobber them
    mov [gs:0x28], rcx ; save return RIP (syscall stores it in RCX)
    mov [gs:0x30], r11 ; save RFLAGS (syscall stores it in R11)
    ; ffff800000000000
    mov [gs:0x08], rsp ; save usermode stack
    mov [gs:0x38], rax ; save syscall number while validating the stack
    mov rax, [gs:0x0] ; load kernel stack
    cmp rax, [rel syscall_kernel_stack_min]
    jb .bad_syscall_stack
    cmp rax, [rel syscall_kernel_stack_max]
    jae .bad_syscall_stack
    mov rsp, rax ; switch to kernel stack
    mov rax, [gs:0x38] ; restore syscall number
    clear_live_tf

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

    ; Pass pointer to saved registers (GPRegs) as first argument.
    ; The separate syscall return-value slot is after the GPRegs block.
    lea rdi, [rsp]
    xor rbp, rbp

    call syscall_handler
    cli
    ; save return value
    mov [rsp+0x78], rax

    ; Check if we need to perform a deferred task switch before delivering
    ; pending signals.  Blocking syscalls such as waitpid use a zero syscall
    ; return only as the deferred-switch placeholder; SIGCHLD delivery must not
    ; bypass the handoff and leak that placeholder to userspace.
    ; Get current task from scheduler.
    extern wos_get_current_task
    extern WOS_DEFERRED_TASK_SWITCH_OFFSET
    call wos_get_current_task
    cli

    ; rax now contains current task pointer
    ; Check if deferred_task_switch flag is set at the calculated offset
    mov r8, [rel WOS_DEFERRED_TASK_SWITCH_OFFSET]  ; Load offset value
    movzx edx, byte [rax + r8]                      ; Load flag from task at offset
    cmp edx, 0
    je .no_deferred_switch

    ; Call deferred_task_switch with GPRegs and frame pointers
    extern deferred_task_switch
    lea rdi, [rsp]         ; GPRegs pointer
    lea rsi, [rsp+128]     ; Frame pointer (after GPRegs and return slot)
    call deferred_task_switch
    ; deferred_task_switch returns only if it decided to resume this task via
    ; the normal syscall exit path.
    cli

.no_deferred_switch:
    ; Check for pending signals before returning to userspace
    extern check_pending_signals
    mov rdi, rsp            ; raw stack pointer (bottom of pushed GPRegs)
    call check_pending_signals
    cli
    test rax, rax
    setnz r12b
.signal_checked:
    ; Validate the userspace return target before either SYSRET or the signal
    ; IRET path consumes the live per-CPU scratch fields.  SYSRET in particular
    ; faults badly if handed a kernel-half RIP or nonsense RSP.
    mov r10, [gs:0x28]
    test r10, r10
    jz .sysret_bad_target_rip
    cmp r10, [rel syscall_user_addr_limit]
    jae .sysret_bad_target_rip
    mov r10, [gs:0x08]
    test r10, r10
    jz .sysret_bad_target_rsp
    cmp r10, [rel syscall_user_addr_limit]
    jae .sysret_bad_target_rsp
    mov r10, [gs:0x30]
    test r10, 0x2
    jz .sysret_bad_target_flags
    test r12b, r12b
    jne .signal_iret_return

    ; restore usermode segment ds and es
    mov ds, [gs:0x18]
    mov es, [gs:0x20]

    ; The saved RCX slot is the normal SYSRET return RIP.  Keep this check
    ; before popq so diagnostics can still read the complete GPRegs block
    ; without clobbering userspace-visible registers.
    mov r10, [rsp + 96]
    cmp r10, [gs:0x28]
    jne .sysret_rcx_corrupt_prepop

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
    o64 sysret

.sysret_rcx_corrupt:
    ; RCX was corrupted - don't sysret or we'll execute at a wrong/kernel address.
    ; Call C diagnostic function. Stack is at kernel stack top, safe for calls.
    ; Save rax (syscall return value) so the panic handler can report it.
    push rax
    extern wos_sysret_corrupt_panic
    mov rdi, rcx         ; arg1 = actual RCX (corrupted value)
    mov rsi, [gs:0x28]   ; arg2 = expected RCX (saved at entry)
    mov rdx, [gs:0x08]   ; arg3 = user RSP
    sub rsp, 8           ; align stack for call
    call wos_sysret_corrupt_panic
    ; should not return, but halt just in case
.sysret_halt:
    hlt
    jmp .sysret_halt

.sysret_rcx_corrupt_prepop:
    mov r8, 1
    jmp .sysret_bad_return

.sysret_bad_target_rip:
    mov r8, 2
    jmp .sysret_bad_return

.sysret_bad_target_rsp:
    mov r8, 3
    jmp .sysret_bad_return

.sysret_bad_target_flags:
    mov r8, 4
    jmp .sysret_bad_return

.sysret_bad_return:
    extern wos_sysret_bad_return_panic
    mov rdi, [rsp + 96]  ; arg1 = saved RCX/user RCX from GPRegs block
    mov rsi, [gs:0x28]   ; arg2 = scratch return RIP
    mov rdx, [gs:0x08]   ; arg3 = scratch user RSP
    mov rcx, [gs:0x30]   ; arg4 = scratch user RFLAGS
    ; arg5 = reason code already in r8
    call wos_sysret_bad_return_panic
    jmp .sysret_halt

.signal_iret_return:
    ; Signal delivery and sigreturn need the complete saved GP register block
    ; restored before userspace runs. SYSRET consumes RCX/R11 for the return
    ; RIP/RFLAGS, so build an IRETQ frame below the saved GPRegs block and
    ; restore the registers manually.
    mov r10, [gs:0x28] ; restored user RIP
    mov r11, [gs:0x30] ; restored user RFLAGS
    mov r12, [gs:0x08] ; restored user RSP
    sub rsp, 40
    mov [rsp + 0], r10
    mov qword [rsp + 8], 0x23
    mov [rsp + 16], r11
    mov [rsp + 24], r12
    mov qword [rsp + 32], 0x1b

    lea rdi, [rsp + 40] ; saved GPRegs block from syscall entry
    mov r15, [rdi + 0]
    mov r14, [rdi + 8]
    mov r13, [rdi + 16]
    mov r12, [rdi + 24]
    mov r11, [rdi + 32]
    mov r10, [rdi + 40]
    mov r9,  [rdi + 48]
    mov r8,  [rdi + 56]
    mov rbp, [rdi + 64]
    mov rsi, [rdi + 80]
    mov rdx, [rdi + 88]
    mov rcx, [rdi + 96]
    mov rbx, [rdi + 104]
    mov rax, [rdi + 120] ; saved syscall return value
    mov rdi, [rdi + 72]

    push rax
    push r8
    push r9
    mov ax, 0x1b
    mov ds, ax
    mov es, ax
    rdfsbase r8
    rdgsbase r9
    mov fs, ax
    mov gs, ax
    wrfsbase r8
    wrgsbase r9
    pop r9
    pop r8
    pop rax

    swapgs
    iretq

.bad_syscall_stack:
    ; gs:0x0 did not contain a kernel-half stack pointer.  Use an emergency
    ; stack before calling C so this path cannot corrupt the user red zone.
    mov rsi, rsp
    lea rsp, [rel syscall_bad_stack_panic_stack_end]
    mov rdi, [gs:0x00]
    mov rdx, [gs:0x10]
    extern wos_syscall_bad_stack_panic
    call wos_syscall_bad_stack_panic
    hlt
    jmp .bad_syscall_stack

section .bss
align 16
syscall_bad_stack_panic_stack:
    resb 4096
syscall_bad_stack_panic_stack_end:

section .rodata
align 8
syscall_kernel_stack_min:
    dq 0xffff800000000000
syscall_kernel_stack_max:
    dq 0xffff900000000000
syscall_user_addr_limit:
    dq 0x0000800000000000
