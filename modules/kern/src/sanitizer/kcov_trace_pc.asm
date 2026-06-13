bits 64

%assign KCOV_BUFFER_PCS_OFFSET 0
%assign KCOV_BUFFER_COUNT_OFFSET 8
%assign KCOV_BUFFER_CAPACITY_OFFSET 16
%assign KCOV_BUFFER_ENABLED_OFFSET 24
%assign KCOV_BUFFER_TRUNCATED_OFFSET 25

%assign KCOV_PANIC_TRACE_CPU_COUNT 256
%assign KCOV_PANIC_TRACE_ENTRIES 128
%assign KCOV_PANIC_TRACE_ENTRY_MASK 127
%assign KCOV_PANIC_TRACE_RING_WRITE_SEQ_OFFSET 1024

global __sanitizer_cov_trace_pc:function

extern wos_per_cpu_ready_state
extern wos_kcov_current_buffer
%ifdef WOS_KCOV_PANIC_TRACE
extern wos_kcov_panic_rings
%endif

section .text

%ifdef WOS_KCOV
__sanitizer_cov_trace_pc:
    mov r11, [rsp]

%ifdef WOS_KCOV_PANIC_TRACE
    movzx eax, byte [rel wos_per_cpu_ready_state]
    test al, al
    jz .check_buf

    mov rcx, qword gs:0x10
    cmp rcx, KCOV_PANIC_TRACE_CPU_COUNT
    jae .check_buf

    mov rax, rcx
    shl rax, 10
    lea rax, [rax + rcx * 8]
    lea rdx, [rel wos_kcov_panic_rings]
    add rdx, rax

    mov eax, 1
    lock xadd dword [rdx + KCOV_PANIC_TRACE_RING_WRITE_SEQ_OFFSET], eax
    and eax, KCOV_PANIC_TRACE_ENTRY_MASK
    mov [rdx + rax * 8], r11
%endif

.check_buf:
    mov rax, [rel wos_kcov_current_buffer]
    test rax, rax
    jz .ret

    cmp byte [rax + KCOV_BUFFER_ENABLED_OFFSET], 0
    je .ret

    mov rcx, [rax + KCOV_BUFFER_COUNT_OFFSET]
    cmp rcx, [rax + KCOV_BUFFER_CAPACITY_OFFSET]
    jae .truncated

    mov rdx, [rax + KCOV_BUFFER_PCS_OFFSET]
    mov [rdx + rcx * 8], r11
    inc rcx
    mov [rax + KCOV_BUFFER_COUNT_OFFSET], rcx

.ret:
    ret

.truncated:
    mov byte [rax + KCOV_BUFFER_TRUNCATED_OFFSET], 1
    ret
%endif
