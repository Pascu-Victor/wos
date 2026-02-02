_start#!/usr/bin/gdb -x
# Enhanced GDB script for debugging WOS userspace processes

# Connect to QEMU
target remote localhost:1234

# Set up architecture
set architecture i386:x86-64:intel
set disassembly-flavor intel

# Disable pagination to avoid interruptions
set pagination off

# Function to load kernel symbols
define load_kernel_symbols
    file build/modules/kern/wos
    printf "Kernel symbols loaded\n"
end

# Function to load init process symbols with correct base address
define load_init_symbols
    # The init process is mapped at 0x400000 (4MB) as specified in service.ld
    # We need to load the symbols at this address
    add-symbol-file build/modules/init/init 0x400000
    printf "Init process symbols loaded at 0x400000\n"
end

# Function to examine memory layout
define examine_memory_layout
    printf "Examining memory layout...\n"

    # Check if we can access the init process memory
    x/10i 0x400000

    # Check for debug sections in high memory
    printf "Checking for debug sections...\n"
    x/4x 0x600000000000
    x/4x 0x700000000000
end

# Function to set up userspace debugging
define setup_userspace_debug
    # Load kernel symbols first
    load_kernel_symbols

    # Load init process symbols
    load_init_symbols

    # Set breakpoints in kernel before userspace starts
    break ker::mod::smt::runHandoverTasks
    break ker::loader::elf::loadElf

    printf "Userspace debugging setup complete\n"
    printf "Breakpoints set in kernel before userspace execution\n"
end

# Function to debug the init process specifically
define debug_init_process
    # Set breakpoint at init process entry point
    break *0x400000

    # Also set breakpoint at _start function
    break _start

    printf "Breakpoints set for init process\n"
    printf "Entry point: 0x400000\n"
    printf "_start function breakpoint also set\n"
end

# Function to show process information
define show_process_info
    printf "Current process information:\n"
    info registers

    # Show memory mappings if available
    printf "Memory at entry point:\n"
    x/20i $pc

    # Show stack
    printf "Stack:\n"
    x/10x $rsp
end

# Function to trace execution
define trace_execution
    printf "Starting execution trace...\n"
    set logging on
    set logging file execution_trace.log

    # Single step through code
    while $pc != 0
        stepi
        printf "PC: 0x%x, Instruction: ", $pc
        x/i $pc

        # Stop if we hit a breakpoint or error
        if $pc == 0x400000
            break
        end
    end

    set logging off
    printf "Execution trace complete\n"
end

# Function to examine debug sections
define examine_debug_sections
    printf "Examining debug sections...\n"

    # Look for debug info in allocated memory
    printf "Checking debug section locations:\n"

    # Check the high memory areas where debug sections should be
    printf "Debug sections at 0x600000000000:\n"
    x/32x 0x600000000000

    printf "ELF headers at 0x700000000000:\n"
    x/32x 0x700000000000
end

# Advanced debugging function
define advanced_debug
    printf "Advanced debugging mode activated\n"

    # Set up comprehensive breakpoints
    break ker::mod::sched::task::Task::Task
    break ker::loader::elf::loadElf
    break ker::loader::elf::loadSectionHeaders
    break ker::loader::elf::loadSegment

    # Enable verbose output
    set verbose on

    printf "Advanced breakpoints set\n"
    printf "Use 'continue' to start debugging\n"
end

# Function to help with symbol loading issues
define fix_symbols
    # Remove all symbol files
    file

    # Reload kernel symbols
    file build/modules/kern/wos

    # Reload init symbols with correct address
    add-symbol-file build/modules/init/init 0x400000

    # Verify symbols are loaded
    info files

    printf "Symbols reloaded\n"
end

# Function to analyze ELF structure
define analyze_elf_structure
    printf "Analyzing ELF structure of init process...\n"

    # Check ELF header
    printf "ELF header (should be at file offset 0):\n"
    x/16x 0x400000

    # Look for program headers
    printf "Program headers:\n"
    x/32x 0x400000+0x40

    # Check if we can find section headers
    printf "Section headers (if mapped):\n"
    x/32x 0x700000000000
end

# Usage function
define usage
    printf "\nWOS Enhanced Userspace Debugging Commands:\n"
    printf "  setup_userspace_debug - Complete setup for userspace debugging\n"
    printf "  debug_init_process     - Set breakpoints specifically for init process\n"
    printf "  show_process_info      - Show current process information\n"
    printf "  trace_execution        - Trace program execution\n"
    printf "  examine_debug_sections - Examine debug section locations\n"
    printf "  examine_memory_layout  - Examine memory layout\n"
    printf "  advanced_debug         - Enable advanced debugging mode\n"
    printf "  fix_symbols            - Reload symbols if they're not working\n"
    printf "  analyze_elf_structure  - Analyze ELF structure in memory\n"
    printf "  usage                  - Show this help\n"
    printf "\nQuick start:\n"
    printf "  1. Run 'setup_userspace_debug'\n"
    printf "  2. Run 'debug_init_process'\n"
    printf "  3. Run 'continue' to start the kernel\n"
    printf "  4. When you hit a breakpoint, you can debug normally\n"
end

# Initialize
printf "WOS Enhanced Userspace Debugger loaded\n"
printf "=======================================\n"

# Automatic setup
setup_userspace_debug

printf "\nReady! Type 'debug_init_process' then 'continue' to start debugging\n"
printf "Type 'usage' for all available commands\n"
