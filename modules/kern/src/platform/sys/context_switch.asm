bits 64

%include "platform/asm/helpers.asm"

%assign GPREGS_SIZE 120
%assign INTERRUPT_FRAME_SIZE 56
%assign USER_IRET_FRAME_SIZE 40
%assign IRETQ_KERNEL_FRAME_SIZE 24
%assign IRETQ_KERNEL_FRAME_OFFSET GPREGS_SIZE + INTERRUPT_FRAME_SIZE
%assign USER_RETURN_BLOCK_SIZE GPREGS_SIZE + USER_IRET_FRAME_SIZE
%assign KERNEL_RETURN_BLOCK_SIZE GPREGS_SIZE + INTERRUPT_FRAME_SIZE + IRETQ_KERNEL_FRAME_SIZE

extern wos_commit_handoff_task
extern wos_user_handoff_stack_top
extern wos_restore_return_task_fpu

; Same-CPL iretq consumes only RIP/CS/RFLAGS, but the scheduler and panic
; diagnostics treat the memory below the saved RSP as GPRegs + InterruptFrame.
; Keep that normalized shadow intact, then skip it immediately before iretq.
%macro build_kernel_return_from_stack 0
    mov r10, [rsp + GPREGS_SIZE + 40]  ; saved kernel RSP
    mov r12, [rsp + GPREGS_SIZE + 16]  ; RIP
    mov r13, [rsp + GPREGS_SIZE + 24]  ; CS
    mov r14, [rsp + GPREGS_SIZE + 32]  ; RFLAGS
    sub r10, KERNEL_RETURN_BLOCK_SIZE

    %assign i 14
    %rep 15
        mov r11, [rsp + i * 8]
        mov [r10 + i * 8], r11
        %assign i i - 1
    %endrep

    %assign j 6
    %rep 7
        mov r11, [rsp + GPREGS_SIZE + j * 8]
        mov [r10 + GPREGS_SIZE + j * 8], r11
        %assign j j - 1
    %endrep

    mov [r10 + IRETQ_KERNEL_FRAME_OFFSET], r12
    mov [r10 + IRETQ_KERNEL_FRAME_OFFSET + 8], r13
    mov [r10 + IRETQ_KERNEL_FRAME_OFFSET + 16], r14

    mov rsp, r10
    sub rsp, 8
    call wos_commit_handoff_task
    add rsp, 8
    popq
    add rsp, INTERRUPT_FRAME_SIZE
    iretq
%endmacro

%macro build_kernel_return_from_ptrs 0
    mov r10, [rsi + 40]  ; saved kernel RSP
    mov r12, [rsi + 16]  ; RIP
    mov r13, [rsi + 24]  ; CS
    mov r14, [rsi + 32]  ; RFLAGS
    sub r10, KERNEL_RETURN_BLOCK_SIZE

    %assign i 14
    %rep 15
        mov r11, [rdi + i * 8]
        mov [r10 + i * 8], r11
        %assign i i - 1
    %endrep

    %assign j 6
    %rep 7
        mov r11, [rsi + j * 8]
        mov [r10 + GPREGS_SIZE + j * 8], r11
        %assign j j - 1
    %endrep

    mov [r10 + IRETQ_KERNEL_FRAME_OFFSET], r12
    mov [r10 + IRETQ_KERNEL_FRAME_OFFSET + 8], r13
    mov [r10 + IRETQ_KERNEL_FRAME_OFFSET + 16], r14

    mov rsp, r10
    sub rsp, 8
    call wos_commit_handoff_task
    add rsp, 8
    popq
    add rsp, INTERRUPT_FRAME_SIZE
    iretq
%endmacro

%macro build_user_return_from_ptrs 0
    ; rdi = GPRegs*, rsi = InterruptFrame*
    ;
    ; The timer path may enter from either an outer-privilege hardware frame or
    ; a same-CPL kernel frame that we normalized in isr32. Always build the
    ; outgoing userspace iret frame from the normalized InterruptFrame instead
    ; of depending on which physical stack shape reached task_switch_handler.
    mov r12, rdi
    mov r13, rsi
    mov r14, rsp
    and rsp, -16
    call wos_commit_handoff_task
    mov rsp, r14
    mov rdi, r12
    mov rsi, r13

    push qword [rsi + 48]    ; SS
    push qword [rsi + 40]    ; RSP
    push qword [rsi + 32]    ; RFLAGS
    push qword [rsi + 24]    ; CS
    push qword [rsi + 16]    ; RIP

    ; Returning to userspace - set up data segment selectors.  The base values
    ; were installed by switch_to(); preserve them across selector loads.
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

    ; Swap from kernel GS base to the user's GS base prepared by switch_to().
    swapgs

    ; Restore GP registers from the normalized snapshot.  Keep the pointer in
    ; r11 until every other register has been restored, then restore r11 last.
    mov r11, rdi
    mov r15, [r11 + 0]
    mov r14, [r11 + 8]
    mov r13, [r11 + 16]
    mov r12, [r11 + 24]
    mov r10, [r11 + 40]
    mov r9,  [r11 + 48]
    mov r8,  [r11 + 56]
    mov rbp, [r11 + 64]
    mov rdi, [r11 + 72]
    mov rsi, [r11 + 80]
    mov rdx, [r11 + 88]
    mov rcx, [r11 + 96]
    mov rbx, [r11 + 104]
    mov rax, [r11 + 112]
    mov r11, [r11 + 32]

    iretq
%endmacro

%macro build_user_return_from_ptrs_late_commit 0
    ; rdi = GPRegs*, rsi = InterruptFrame*
    ;
    ; Keep the scheduler's current_task pointer on the outgoing task until RSP
    ; is already on the incoming task's kernel stack. Exiting tasks can have
    ; their kernel stacks moved to the dead list before this iret tail finishes;
    ; publishing the handoff while still on that dead stack makes later faults
    ; look like the incoming task is executing on freed outgoing storage.
    mov r12, rdi
    mov r13, rsi

    mov r14, rsp
    and rsp, -16
    call wos_user_handoff_stack_top
    mov rsp, r14
    test rax, rax
    jz %%same_stack_return

    ; Handoff to another user task: build the complete register/iret return
    ; block on the incoming task's syscall kernel stack, switch RSP there, and
    ; only then publish current_task.
    sub rax, USER_RETURN_BLOCK_SIZE
    mov r10, rax

    %assign i 14
    %rep 15
        mov r11, [r12 + i * 8]
        mov [r10 + i * 8], r11
        %assign i i - 1
    %endrep

    mov r11, [r13 + 16]    ; RIP
    mov [r10 + GPREGS_SIZE], r11
    mov r11, [r13 + 24]    ; CS
    mov [r10 + GPREGS_SIZE + 8], r11
    mov r11, [r13 + 32]    ; RFLAGS
    mov [r10 + GPREGS_SIZE + 16], r11
    mov r11, [r13 + 40]    ; RSP
    mov [r10 + GPREGS_SIZE + 24], r11
    mov r11, [r13 + 48]    ; SS
    mov [r10 + GPREGS_SIZE + 32], r11

    mov rsp, r10

    mov r14, rsp
    and rsp, -16
    call wos_commit_handoff_task
    call wos_restore_return_task_fpu
    mov rsp, r14

    ; Returning to userspace - set up data segment selectors. The base values
    ; were installed by switch_to(); preserve them across selector loads.
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

    ; Swap from kernel GS base to the user's GS base prepared by switch_to().
    swapgs

    ; Restore GP registers from the copied block on the incoming stack, then
    ; advance RSP to the iret frame that follows it.
    mov r11, rsp
    mov r15, [r11 + 0]
    mov r14, [r11 + 8]
    mov r13, [r11 + 16]
    mov r12, [r11 + 24]
    mov r10, [r11 + 40]
    mov r9,  [r11 + 48]
    mov r8,  [r11 + 56]
    mov rbp, [r11 + 64]
    mov rdi, [r11 + 72]
    mov rsi, [r11 + 80]
    mov rdx, [r11 + 88]
    mov rcx, [r11 + 96]
    mov rbx, [r11 + 104]
    mov rax, [r11 + 112]
    lea rsp, [r11 + GPREGS_SIZE]
    mov r11, [r11 + 32]

    iretq

%%same_stack_return:
    push qword [r13 + 48]    ; SS
    push qword [r13 + 40]    ; RSP
    push qword [r13 + 32]    ; RFLAGS
    push qword [r13 + 24]    ; CS
    push qword [r13 + 16]    ; RIP

    ; Returning to userspace - set up data segment selectors. The base values
    ; were installed by switch_to(); preserve them across selector loads.
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

    ; Commit immediately before the final swapgs/register restore/iret tail.
    ; The stack below the prepared iret frame is scratch for the call.
    mov r14, rsp
    and rsp, -16
    call wos_commit_handoff_task
    call wos_restore_return_task_fpu
    mov rsp, r14

    ; Swap from kernel GS base to the user's GS base prepared by switch_to().
    swapgs

    ; Restore GP registers from the normalized snapshot. Keep the pointer in
    ; r11 until every other register has been restored, then restore r11 last.
    mov r11, r12
    mov r15, [r11 + 0]
    mov r14, [r11 + 8]
    mov r13, [r11 + 16]
    mov r12, [r11 + 24]
    mov r10, [r11 + 40]
    mov r9,  [r11 + 48]
    mov r8,  [r11 + 56]
    mov rbp, [r11 + 64]
    mov rdi, [r11 + 72]
    mov rsi, [r11 + 80]
    mov rdx, [r11 + 88]
    mov rcx, [r11 + 96]
    mov rbx, [r11 + 104]
    mov rax, [r11 + 112]
    mov r11, [r11 + 32]

    iretq
%endmacro

%macro isr_swapgs 1
    cmp [rsp + 24], dword 8 ; Check if we're in userspace
    je .%1
    swapgs
    .%1:
%endmacro

; Kernel-mode idle loop - used when there are no user tasks to run
; This runs in ring 0 and just halts, waiting for interrupts
global wos_kernel_idle_loop
wos_kernel_idle_loop:
    sti         ; Enable interrupts
    hlt         ; Halt until interrupt
    jmp wos_kernel_idle_loop  ; Loop forever

extern wos_kernel_thread_returned
global wos_kernel_thread_trampoline
wos_kernel_thread_trampoline:
    ; Kernel thread tasks enter here via iretq with rdi = entry function and
    ; rsp 16-byte aligned. Calling the entry gives C++ the normal SysV stack
    ; shape: callee entry sees rsp % 16 == 8 and has a real return address.
    call rdi
    call wos_kernel_thread_returned
.kernel_thread_return_halt:
    cli
    hlt
    jmp .kernel_thread_return_halt

global wos_start_kernel_thread
wos_start_kernel_thread:
    ; rdi = initial kernel stack top
    ; rsi = kernel thread entry function
    ;
    ; Brand-new kernel threads do not have a previously interrupted kernel
    ; frame to resume. Start them by installing the stack directly, then jump
    ; into the normal trampoline with the entry function in rdi.
    push rdi
    push rsi
    sub rsp, 8
    call wos_validate_kernel_thread_start
    add rsp, 8
    pop rsi
    pop rdi

    mov r12, rsi
    mov rsp, rdi
    call wos_commit_handoff_task
    mov rdi, r12
    sti
    jmp wos_kernel_thread_trampoline

global wos_enterIdleStack
wos_enterIdleStack:
    ; rdi = idle kernel stack top
    ;
    ; Idle has no meaningful continuation. Re-entering it directly avoids
    ; constructing a same-CPL iret frame at the very top of the idle stack.
    mov rsp, rdi
    call wos_commit_handoff_task
    sti
    jmp wos_kernel_idle_loop

global wos_asm_enter_usermode
wos_asm_enter_usermode:
    mov r12, rdi
    mov r13, rsi
    mov r14, rsp
    and rsp, -16
    call wos_restore_return_task_fpu
    mov rsp, r14
    mov rdi, r12
    mov rsi, r13

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

extern wos_sched_timer
extern wos_repair_timer_return_frame
extern wos_validate_deferred_return_frame
extern wos_validate_kernel_thread_start
global task_switch_handler
task_switch_handler:
    mov rdi, rsp
    call wos_sched_timer
    cli

    ; The C++ scheduler mutates the frame in-place.  If a bad frame selector
    ; reaches iretq, the CPU raises #GP with little context; repair or panic
    ; while the saved register/frame block is still easy to inspect.
    mov rax, [rsp + GPREGS_SIZE + 24]
    cmp rax, 0x23
    je .return_frame_ready
    cmp rax, 0x08
    je .return_frame_ready
    mov rdi, rsp
    call wos_repair_timer_return_frame
    cli

.return_frame_ready:
    lea rsi, [rsp + GPREGS_SIZE]
    mov rdi, rsp
    cmp qword [rsi + 24], qword 0x23
    jne .kernel_return

    build_user_return_from_ptrs_late_commit

.kernel_return:
    build_kernel_return_from_ptrs

; Jump to next task without saving current task state
; Used when a task is exiting and doesn't need its context preserved
extern wos_jump_to_next_task_no_save
global jump_to_next_task_no_save
jump_to_next_task_no_save:
    cli

    ; Push dummy interrupt frame FIRST (will be at higher addresses)
    ; Layout expected by C++: GPRegs at stack_ptr, InterruptFrame at stack_ptr + sizeof(GPRegs)
    push 0  ; SS
    push 0  ; RSP
    push 0  ; RFLAGS
    push 0  ; CS
    push 0  ; RIP
    push 0  ; err_code
    push 0  ; int_num

    ; Push dummy GPRegs structure (will be at lower addresses = RSP after this)
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

    mov rdi, rsp
    call wos_jump_to_next_task_no_save
    cli

    ; Same-CPL iretq does not restore RSP; kernel targets need a frame on
    ; their own stack.
    lea rsi, [rsp + GPREGS_SIZE]
    mov rdi, rsp
    cmp qword [rsi + 24], qword 0x23
    jne .kernel_return_jump

    build_user_return_from_ptrs_late_commit

.kernel_return_jump:
    build_kernel_return_from_stack

; Return from deferred task switch
; rdi = pointer to GPRegs structure in memory
; rsi = pointer to InterruptFrame structure in memory
;
; GPRegs layout (120 bytes): r15, r14, r13, r12, r11, r10, r9, r8, rbp, rdi, rsi, rdx, rcx, rbx, rax
; InterruptFrame layout (56 bytes): int_num, err_code, rip, cs, flags, rsp, ss
;
; We need to:
; 1. Build a proper stack with InterruptFrame for iretq
; 2. Restore all GPRegs
; 3. Check if we need swapgs (if returning to userspace)
; 4. Execute iretq
global wos_deferred_task_switch_return
wos_deferred_task_switch_return:
    ; rdi = GPRegs*, rsi = InterruptFrame*
    cli

    ; This is the last boundary before consuming the saved frame to build an
    ; iretq target. Keep rdi/rsi intact for the return macros.
    push rdi
    push rsi
    sub rsp, 8
    call wos_validate_deferred_return_frame
    add rsp, 8
    pop rsi
    pop rdi

    ; Returning to a kernel-mode task is a same-CPL iretq. Build the return
    ; frame on the saved kernel stack so RSP is restored correctly.
    cmp qword [rsi + 24], qword 0x23
    jne .kernel_return_deferred

    build_user_return_from_ptrs_late_commit

.kernel_return_deferred:
    build_kernel_return_from_ptrs
