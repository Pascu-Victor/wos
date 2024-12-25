bits 64

; extern uint64_t _wOS_asm_syscall(uint64_t callnum, uint64_t a1, uint64_t a2, uint64_t a3, uint64_t a4, uint64_t a5, uint64_t a6);
global _wOS_asm_syscall
_wOS_asm_syscall:
    mov rax, [rsp+0x38]   ; move callnum to rax
    mov rdi, [rsp+0x30]   ; move a1 to rdi
    mov rsi, [rsp+0x28]   ; move a2 to rsi
    mov rdx, [rsp+0x20]   ; move a3 to rdx
    mov r8,  [rsp+0x18]   ; move a4 to r8
    mov r9,  [rsp+0x10]   ; move a5 to r9
    mov r10, [rsp+0x08]   ; move a6 to r10
    syscall             ; make the syscall
    ret                 ; return to caller
