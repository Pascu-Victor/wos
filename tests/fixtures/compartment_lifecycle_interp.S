/* Genuine WOS ET_DYN startup adapter; no successful-path syscall or stack write. */
.text
.globl _start
.hidden _start
.type _start,@function
_start:
    testq $15, %rsp
    jnz .Lbad_stack
    movabsq $0x00007fffffffe000, %rax
    cmpq %rax, %rsp
    jae .Lbad_stack
    cmpq $8192, %rsp
    jb .Lbad_stack
    movq (%rsp), %rcx
    cmpq $256, %rcx
    ja .Lbad_count
    leaq 8(%rsp), %rbx
    leaq (%rbx,%rcx,8), %rbx
    cmpq $0, (%rbx)
    jne .Lbad_count
    addq $8, %rbx
    movq $257, %rcx
.Lenv:
    cmpq $0, (%rbx)
    je .Lenv_done
    addq $8, %rbx
    decq %rcx
    jnz .Lenv
    jmp .Lbad_count
.Lenv_done:
    addq $8, %rbx
    xorq %rbp, %rbp
    xorq %r8, %r8
    xorq %r9, %r9
    movq $32, %rcx
.Laux:
    movq (%rbx), %rax
    testq %rax, %rax
    jz .Laux_done
    cmpq $9, %rax
    je .Lentry
    cmpq $7, %rax
    je .Lbase
    cmpq $6, %rax
    je .Lpagesz
    cmpq $3, %rax
    je .Lphdr
    cmpq $4, %rax
    je .Lphent
    cmpq $5, %rax
    je .Lphnum
.Lnext:
    addq $16, %rbx
    decq %rcx
    jnz .Laux
    jmp .Lbad_aux
.Lentry:
    btsq $0, %rbp
    jc .Lbad_aux
    movq 8(%rbx), %r8
    cmpq $0x400000, %r8
    jb .Lbad_aux
    cmpq $0x800000, %r8
    jae .Lbad_aux
    jmp .Lnext
.Lbase:
    btsq $1, %rbp
    jc .Lbad_aux
    movq 8(%rbx), %r9
    cmpq $0x30000000, %r9
    jb .Lbad_aux
    cmpq $0x70000000, %r9
    jae .Lbad_aux
    testq $0x1fffff, %r9
    jnz .Lbad_aux
    jmp .Lnext
.Lpagesz:
    btsq $2, %rbp
    jc .Lbad_aux
    cmpq $4096, 8(%rbx)
    jne .Lbad_aux
    jmp .Lnext
.Lphdr:
    btsq $3, %rbp
    jc .Lbad_aux
    movq 8(%rbx), %rax
    cmpq $0x1040, %rax
    jne .Lbad_aux
    jmp .Lnext
.Lphent:
    btsq $4, %rbp
    jc .Lbad_aux
    cmpq $56, 8(%rbx)
    jne .Lbad_aux
    jmp .Lnext
.Lphnum:
    btsq $5, %rbp
    jc .Lbad_aux
    movq 8(%rbx), %rax
    testq %rax, %rax
    jz .Lbad_aux
    cmpq $32, %rax
    ja .Lbad_aux
    jmp .Lnext
.Laux_done:
    cmpq $0, 8(%rbx)
    jne .Lbad_aux
    cmpq $63, %rbp
    jne .Lbad_aux
    cmpl $0x464c457f, (%r9)
    jne .Lbad_header
    cmpb $2, 4(%r9)
    jne .Lbad_header
    cmpb $1, 5(%r9)
    jne .Lbad_header
    cmpb $1, 6(%r9)
    jne .Lbad_header
    cmpw $3, 16(%r9)
    jne .Lbad_header
    cmpw $62, 18(%r9)
    jne .Lbad_header
    cmpl $1, 20(%r9)
    jne .Lbad_header
    movq 24(%r9), %rax
    testq %rax, %rax
    jz .Lbad_header
    cmpq $65536, %rax
    ja .Lbad_header
    addq %r9, %rax
    jc .Lbad_header
    leaq _start(%rip), %r13
    cmpq %r13, %rax
    jne .Lbad_header
    movabsq $0x54494e593431484f, %r12
    movq %r9, %r14
    movq %rsp, %r15
    jmp *%r8
.Lbad_stack:
    movq $100, %rsi
    jmp .Lexit
.Lbad_count:
    movq $101, %rsi
    jmp .Lexit
.Lbad_aux:
    movq $102, %rsi
    jmp .Lexit
.Lbad_header:
    movq $103, %rsi
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
