#!/usr/bin/gdb -x
# VS Code GDB script for WOS userspace debugging

# Function to determine the correct PIE base address
define detect_pie_base
    # Our loader currently uses loadBase = 0 for ET_DYN; .text is linked at 0x400000.
    set $pie_base = 0x0
    set $text_section_offset = 0x400000
    set $final_text_addr = $pie_base + $text_section_offset

    printf "PIE Detection:\n"
    printf "  Base address: 0x%x\n", $pie_base
    printf "  .text section offset: 0x%x\n", $text_section_offset
    printf "  Final .text address: 0x%x\n", $final_text_addr
    printf "  Expected entry point: 0x%x\n", $pie_base + 0x400030
end

# Function to load userspace symbols with correct PIE relocation
define load_userspace_symbols_pie
    detect_pie_base
    add-symbol-file build/modules/init/init 0x400000
    printf "Loaded userspace symbols with PIE relocation\n"
end

# Function to load userspace symbols at base address (alternative approach)
define load_userspace_symbols_base
    # Load init symbols at the base address
    add-symbol-file build/modules/init/init 0x400000

    printf "Loaded userspace symbols at base address\n"
end

# Function to set comprehensive breakpoints
define set_userspace_breakpoints
    # Kernel breakpoints
    break ker::mod::smt::runHandoverTasks
    break ker::loader::elf::loadElf

    # Function breakpoints
    break _start
    break main

    printf "Set comprehensive breakpoints for userspace debugging\n"
end

# Function to examine userspace memory
define examine_userspace_memory
    # Check current PC
    printf "Current PC (0x%x):\n", $pc
    if $pc != 0
        x/10i $pc
    end
end

# Function for step-by-step debugging
define debug_userspace_step
    printf "Starting userspace debugging session...\n"

    # Set breakpoints
    set_userspace_breakpoints

    # Continue to kernel breakpoint
    printf "Continuing to kernel breakpoint...\n"
    continue

    # Load symbols after kernel setup
    load_userspace_symbols_pie

    # Continue to userspace
    printf "Continuing to userspace...\n"
    continue

    # Examine memory
    examine_userspace_memory

    printf "Ready for userspace debugging\n"
end

# Function to show debug information
define show_debug_info
    printf "Debug Information:\n"
    printf "  PC: 0x%x\n", $pc
    printf "  SP: 0x%x\n", $rsp

    # Show loaded files
    printf "Loaded files:\n"
    info files

    # Show breakpoints
    printf "Breakpoints:\n"
    info breakpoints

    # Show current location
    printf "Current location:\n"
    where
end

# Function for VS Code specific debugging
define vscode_debug_init
    printf "VS Code debugging initialized\n"
    set pagination off
    set confirm off
    # Connect to QEMU gdbstub
    target remote :1234
    # Load kernel symbols (PIE unknown, but text likely at link addr)
    symbol-file build/modules/kern/wos
    # Set initial breakpoints and load init symbols
    set_userspace_breakpoints
    load_userspace_symbols_pie
    printf "VS Code userspace debugging ready\n"
end

# Convenience functions
define b_userspace
    break _start
end

define examine_current
    examine_userspace_memory
    show_debug_info
end

# Auto-initialization for VS Code
vscode_debug_init

printf "VS Code GDB script loaded\n"
printf "Available commands:\n"
printf "  detect_pie_base            - Detect PIE base address\n"
printf "  load_userspace_symbols_pie - Load symbols with PIE relocation\n"
printf "  load_userspace_symbols_base- Load symbols at base address\n"
printf "  set_userspace_breakpoints  - Set comprehensive breakpoints\n"
printf "  examine_userspace_memory   - Examine memory layout\n"
printf "  debug_userspace_step       - Step-by-step debugging\n"
printf "  show_debug_info            - Show debug information\n"
printf "  vscode_debug_init          - Initialize VS Code debugging\n"
printf "  b_userspace                - Set userspace breakpoints\n"
printf "  examine_current            - Examine current state\n"
