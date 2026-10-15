/* RSP is authoritative; successful interpreter handoff precedes all C code. */
#ifndef STARTUP_MODE
#error STARTUP_MODE must be explicitly compiled as 1 or 2
#endif
#if STARTUP_MODE != 1 && STARTUP_MODE != 2
#error unsupported STARTUP_MODE
#endif
.text
.globl _start
.hidden _start
.hidden lifecycle_raw_main
.type _start,@function
_start:
    testq $15, %rsp
    jnz .Lstartup_fail
    cmpq $4096, %rsp
    jb .Lstartup_fail
    movabsq $0x00007ffffffffc00, %rax
    cmpq %rax, %rsp
    jae .Lstartup_fail
#if STARTUP_MODE == 2
    movabsq $0x54494e593431484f, %rax
    cmpq %rax, %r12
    jne .Lstartup_fail
    cmpq %rsp, %r15
    jne .Lstartup_fail
#endif
    subq $208, %rsp
    movq $26, %r10
.Lclear_startup:
    movq $0, -8(%rsp,%r10,8)
    decq %r10
    jnz .Lclear_startup
    leaq 208(%rsp), %rax
    movq %rax, 0(%rsp)
    leaq _start(%rip), %r11
    movq %r11, 8(%rsp)
    movq %r12, 40(%rsp)
    movq %rdi, 48(%rsp)
    movq %rsi, 56(%rsp)
    movq %rdx, 64(%rsp)
    movq $STARTUP_MODE, 200(%rsp)
    movabsq $0x0000800000000000, %r11
    cmpq %r11, %rax
    jae .Lstartup_fail
    testq %rax, %rax
    jz .Lstartup_fail
    cmpq $1, (%rax)
    jne .Lstartup_fail
    cmpq $0, 8(%rax)
    je .Lstartup_fail
    cmpq $0, 16(%rax)
    jne .Lstartup_fail
    cmpq $0, 24(%rax)
    jne .Lstartup_fail
    movq $1, 72(%rsp)
    leaq 32(%rax), %rbx
    xorq %r10, %r10
    movq $32, %rcx
.Laux:
    movq (%rbx), %rax
    movq 8(%rbx), %rdx
    testq %rax, %rax
    jz .Laux_done
    cmpq $3, %rax
    je .Lphdr
    cmpq $4, %rax
    je .Lphent
    cmpq $5, %rax
    je .Lphnum
    cmpq $6, %rax
    je .Lpagesz
    cmpq $7, %rax
    je .Lbase
    cmpq $9, %rax
    je .Lentry
    cmpq $11, %rax
    je .Luid
    cmpq $12, %rax
    je .Leuid
    cmpq $13, %rax
    je .Lgid
    cmpq $14, %rax
    je .Legid
    cmpq $23, %rax
    je .Lsecure
    cmpq $25, %rax
    je .Lrandom
    cmpq $31, %rax
    je .Lexecfn
    jmp .Lstartup_fail
.macro save_aux bit, offset
    btsq $\bit, %r10
    jc .Lstartup_fail
    movq %rdx, \offset(%rsp)
    jmp .Lnext
.endm
.Lphdr:
    save_aux 0, 88
.Lphent:
    save_aux 1, 96
.Lphnum:
    save_aux 2, 104
.Lpagesz:
    save_aux 3, 112
.Lbase:
    save_aux 4, 24
.Lentry:
    save_aux 5, 16
.Luid:
    save_aux 6, 128
.Leuid:
    save_aux 7, 136
.Lgid:
    save_aux 8, 144
.Legid:
    save_aux 9, 152
.Lsecure:
    save_aux 10, 120
.Lrandom:
    save_aux 11, 160
.Lexecfn:
    save_aux 12, 168
.Lnext:
    incq 176(%rsp)
    addq $16, %rbx
    decq %rcx
    jnz .Laux
    jmp .Lstartup_fail
.Laux_done:
    testq %rdx, %rdx
    jnz .Lstartup_fail
#if STARTUP_MODE == 1
    cmpq $0x1fef, %r10
    jne .Lstartup_fail
    cmpq $12, 176(%rsp)
    jne .Lstartup_fail
#else
    cmpq $0x1fff, %r10
    jne .Lstartup_fail
    cmpq $13, 176(%rsp)
    jne .Lstartup_fail
#endif
    movq 8(%rsp), %rax
    cmpq %rax, 16(%rsp)
    jne .Lstartup_fail
    cmpq $0x400000, %rax
    jb .Lstartup_fail
    cmpq $0x800000, %rax
    jae .Lstartup_fail
    cmpq $4096, 112(%rsp)
    jne .Lstartup_fail
    cmpq $56, 96(%rsp)
    jne .Lstartup_fail
    movq 104(%rsp), %rax
    testq %rax, %rax
    jz .Lstartup_fail
    cmpq $32, %rax
    ja .Lstartup_fail
    imulq $56, %rax, %rax
    movq 88(%rsp), %rdx
    cmpq $0x1040, %rdx
    jne .Lstartup_fail
    addq %rdx, %rax
    jc .Lstartup_fail
    cmpq $0x2000, %rax
    ja .Lstartup_fail
    cmpq $100, 128(%rsp)
    jne .Lstartup_fail
    cmpq $100, 136(%rsp)
    jne .Lstartup_fail
    cmpq $100, 144(%rsp)
    jne .Lstartup_fail
    cmpq $100, 152(%rsp)
    jne .Lstartup_fail
    cmpq $0, 120(%rsp)
    jne .Lstartup_fail
    movq 160(%rsp), %rax
    testq %rax, %rax
    jz .Lstartup_fail
    movabsq $0x0000800000000000, %r11
    cmpq %r11, %rax
    jae .Lstartup_fail
    movq 168(%rsp), %rax
    testq %rax, %rax
    jz .Lstartup_fail
    cmpq %r11, %rax
    jae .Lstartup_fail
    movq $0x7f, 184(%rsp)
#if STARTUP_MODE == 2
    movq 24(%rsp), %r9
    cmpq %r14, %r9
    jne .Lstartup_fail
    cmpq $0x30000000, %r9
    jb .Lstartup_fail
    cmpq $0x70000000, %r9
    jae .Lstartup_fail
    testq $0x1fffff, %r9
    jnz .Lstartup_fail
    cmpl $0x464c457f, (%r9)
    jne .Lstartup_fail
    cmpb $2, 4(%r9)
    jne .Lstartup_fail
    cmpb $1, 5(%r9)
    jne .Lstartup_fail
    cmpb $1, 6(%r9)
    jne .Lstartup_fail
    cmpw $3, 16(%r9)
    jne .Lstartup_fail
    cmpw $62, 18(%r9)
    jne .Lstartup_fail
    cmpl $1, 20(%r9)
    jne .Lstartup_fail
    movq 24(%r9), %rax
    testq %rax, %rax
    jz .Lstartup_fail
    cmpq $65536, %rax
    ja .Lstartup_fail
    movq %rax, 192(%rsp)
    addq %r9, %rax
    jc .Lstartup_fail
    cmpq %r13, %rax
    jne .Lstartup_fail
    movq %r13, 32(%rsp)
    orq $128, 184(%rsp)
#endif
    cld
    xorq %rbp, %rbp
    movq %rsp, %rdi
    call lifecycle_raw_main
    movq $114, %rsi
    jmp .Lexit
.Lstartup_fail:
    movq $104, %rsi
.Lexit:
    movq $3, %rax
    xorq %rdi, %rdi
    xorq %rdx, %rdx
    xorq %r8, %r8
    xorq %r9, %r9
    xorq %r10, %r10
    syscall
    ud2
.size _start, .-_start
.section .note.GNU-stack,"",@progbits
