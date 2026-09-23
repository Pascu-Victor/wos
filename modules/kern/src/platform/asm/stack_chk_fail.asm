bits 64
default rel

section .text
global __stack_chk_fail
global __stack_chk_fail_local

; Stack corruption means neither the current stack nor the ordinary panic
; machinery is trustworthy. Switch to private storage, emit only a fixed
; non-secret diagnostic, and halt without allocation or lock acquisition.
__stack_chk_fail:
__stack_chk_fail_local:
    cli
    lea rsp, [__stack_chk_emergency_stack_end]
    and rsp, -16
    lea rsi, [stack_chk_message]

.next_character:
    mov al, [rsi]
    test al, al
    jz .halt
    inc rsi
    mov r8b, al
    mov ecx, 65536

.wait_for_uart:
    mov dx, 0x3fd
    in al, dx
    test al, 0x20
    jnz .write_character
    loop .wait_for_uart

.write_character:
    mov dx, 0x3f8
    mov al, r8b
    out dx, al
    jmp .next_character

.halt:
    hlt
    jmp .halt

section .rodata
stack_chk_message: db "FATAL: kernel stack canary corrupted", 10, 0

section .bss.stack_chk_stack nobits alloc write align=4096
global __stack_chk_emergency_guard_low
global __stack_chk_emergency_stack
global __stack_chk_emergency_stack_end
global __stack_chk_emergency_guard_high_end
__stack_chk_emergency_guard_low:
    resb 4096
__stack_chk_emergency_stack:
    resb 16384
__stack_chk_emergency_stack_end:
    resb 4096
__stack_chk_emergency_guard_high_end:
