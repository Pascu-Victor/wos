bits 64

global _wOS_asm_switchTo

; extern "C" void _wOS_asm_switchTo(TaskRegisters regs);

_wOS_asm_switchTo:
    ; Save callee-saved registers
    push rbp
    push rbx
    push r12
    push r13
    push r14
    push r15

    ; switch stack
    movq rsp, rdi
    movq rsi, rsp

    ; TODO: add stack protection
    ; TODO: speculative execution protection

    ; Restore callee-saved registers
    pop r15
    pop r14
    pop r13
    pop r12
    pop rbx
    pop rbp

    jmp switchTo