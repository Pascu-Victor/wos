#pragma once

#include <QByteArray>
#include <QString>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace wosdbg {

// Magic number: "WOSCODMP" as little-endian uint64
static constexpr uint64_t COREDUMP_MAGIC = 0x504D55444F43534FULL;

// Segment types
enum class SegmentType : uint32_t {
    ZERO_UNMAPPED = 0,
    STACK_PAGE = 1,
    FAULT_PAGE = 2,
    MEMORY_PAGE = 3,
};

QString segment_type_name(uint32_t type);

// x86-64 interrupt frame - matches WOS kernel InterruptFrame layout
struct InterruptFrame {
    uint64_t int_num;
    uint64_t err_code;
    uint64_t rip;
    uint64_t cs;
    uint64_t rflags;
    uint64_t rsp;
    uint64_t ss;
};

// x86-64 general purpose registers - matches WOS kernel GPRegs layout
// Order: r15, r14, ..., r8, rbp, rdi, rsi, rdx, rcx, rbx, rax
struct GPRegs {
    uint64_t r15;
    uint64_t r14;
    uint64_t r13;
    uint64_t r12;
    uint64_t r11;
    uint64_t r10;
    uint64_t r9;
    uint64_t r8;
    uint64_t rbp;
    uint64_t rdi;
    uint64_t rsi;
    uint64_t rdx;
    uint64_t rcx;
    uint64_t rbx;
    uint64_t rax;
};

// A single memory segment in the coredump
struct CoreDumpSegment {
    uint64_t vaddr{};
    uint64_t size{};
    uint64_t file_offset{};
    uint32_t type{};
    uint32_t present{};
    uint64_t pte_flags = 0;
    uint64_t phys_addr = 0;

    [[nodiscard]] uint64_t vaddr_end() const { return vaddr + size; }
    [[nodiscard]] QString type_name() const { return segment_type_name(type); }
    [[nodiscard]] bool is_present() const { return present != 0; }
};

// Full parsed coredump
struct CoreDump {
    // Header fields
    uint64_t magic;
    uint32_t version;
    uint32_t header_size;
    uint64_t timestamp;
    uint64_t pid;
    uint64_t cpu;
    uint64_t int_num;
    uint64_t err_code;
    uint64_t cr2;
    uint64_t cr3;

    // CPU state at trap
    InterruptFrame trap_frame;
    GPRegs trap_regs;

    // Saved CPU state (before trap)
    InterruptFrame saved_frame;
    GPRegs saved_regs;

    // Task metadata
    uint64_t task_entry;
    uint64_t task_pagemap;
    uint64_t elf_header_addr;
    uint64_t program_header_addr;
    uint64_t segment_count;
    uint64_t segment_table_offset;
    uint64_t elf_size;
    uint64_t elf_offset;
    uint64_t segment_entry_size = 32;
    uint64_t page_size = 4096;
    uint64_t snapshot_flags = 0;
    uint64_t interp_base = 0;
    uint64_t program_header_count = 0;
    uint64_t program_header_ent_size = 0;
    uint64_t thread_fs_base = 0;
    uint64_t thread_gs_base = 0;
    uint64_t thread_stack_base = 0;
    uint64_t thread_stack_size = 0;
    uint64_t thread_tls_base = 0;
    uint64_t thread_tls_size = 0;
    uint64_t thread_safe_stack = 0;
    uint64_t task_ptr = 0;
    uint64_t thread_ptr = 0;
    uint64_t parent_pid = 0;
    uint64_t owner_pid = 0;
    uint64_t wki_remote_pid = 0;
    uint64_t session_id = 0;
    uint64_t pgid = 0;
    uint64_t task_type = 0;
    uint64_t task_state = 0;
    uint64_t sched_queue = 0;
    uint64_t current_cpu = 0;
    uint64_t domain_id = 0;
    uint64_t domain_mask = 0;
    uint64_t wki_target_flags = 0;
    uint64_t wki_proxy_task_id = 0;
    uint64_t elf_buffer_addr = 0;
    uint64_t captured_elf_buffer_size = 0;
    bool elf_buffer_shared = false;
    uint64_t start_time_us = 0;
    uint64_t user_time_us = 0;
    uint64_t system_time_us = 0;
    uint64_t vruntime = 0;
    uint64_t vdeadline = 0;
    uint64_t wake_at_us = 0;
    uint64_t wait_channel_addr = 0;
    uint64_t waiting_for_pid = 0;
    uint64_t wait_status_user_addr = 0;
    uint64_t wait_rusage_user_addr = 0;
    uint64_t sig_pending = 0;
    uint64_t sig_mask = 0;
    uint64_t ptrace_tracer_pid = 0;
    uint64_t uid = 0;
    uint64_t gid = 0;
    uint64_t euid = 0;
    uint64_t egid = 0;
    uint64_t task_flags = 0;
    QString exe_path;
    QString cwd;
    QString root;
    QString wait_channel;
    QString wki_target_hostname;
    QString wki_submitter_hostname;

    // Segment table
    std::vector<CoreDumpSegment> segments;

    // Raw file bytes (segments reference file offsets into this)
    QByteArray raw;

    // Source file path (set after parsing for symbol resolution)
    QString source_filename;

    // Get the embedded ELF bytes, if present
    [[nodiscard]] QByteArray embedded_elf() const;

    // Check if magic is valid
    [[nodiscard]] bool is_valid() const { return magic == COREDUMP_MAGIC; }
};

// Parse a coredump from raw binary data.
// Returns std::nullopt on parse failure.
std::optional<CoreDump> parse_core_dump(const QByteArray& data);

/// Load and parse a coredump from a file path. Sets sourceFilename on success.
std::unique_ptr<CoreDump> parse_core_dump(const QString& file_path);

// Get human-readable interrupt/exception name
QString interrupt_name(uint64_t num);

// Format a uint64 as "0x%016llx"
QString format_u64(uint64_t val);

// Parse binary name from coredump filename pattern: {binary}_{timestamp}_coredump.bin
QString parse_binary_name_from_filename(const QString& filename);

}  // namespace wosdbg
