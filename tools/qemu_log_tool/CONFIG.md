# QEMU Log Viewer Configuration

## Configuration File: logview.json

The QEMU Log Viewer supports a configuration file called `logview.json` in the current working directory. This file allows you to map address ranges to symbol files for enhanced debugging information.

### Configuration Format

```json
{
    "lookups": [
        {
            "from": "0x400000",
            "to": "0x800000",
            "path": "./build/modules/init/init"
        },
        {
            "from": "0xffffffff80000000",
            "to": "0xffffffffffffffff",
            "path": "./build/kernel/kernel.elf"
        }
    ]
}
```

### Fields

-   **lookups**: Array of address range mappings
-   **from**: Starting address of the range (hex string with 0x prefix)
-   **to**: Ending address of the range (hex string with 0x prefix)
-   **path**: Path to the symbol file (executable, ELF file, etc.)

### Address Ranges

Common address ranges for different types of binaries:

-   **User space applications**: `0x400000` to `0x800000`
-   **Kernel space (x86_64)**: `0xffffffff80000000` to `0xffffffffffffffff`
-   **Shared libraries**: `0x7f0000000000` to `0x7fffffffffff`

### Default Configuration

If no `logview.json` file is found, the application will use these default mappings:

1. `0x400000-0x800000` → `./build/modules/init/init`
2. `0xffffffff80000000-0xffffffffffffffff` → `./build/kernel/kernel.elf`
3. `0x7f0000000000-0x7fffffffffff` → `./build/lib/libc.so`

### Usage

1. Create a `logview.json` file in the directory where you run the log viewer
2. Configure the address ranges and paths according to your build system
3. The application will automatically load the configuration on startup
4. When viewing log entries, the details pane will show symbol file information for addresses that fall within the configured ranges

### Future Enhancements

The configuration system is designed to be extensible. Future versions may include:

-   Automatic symbol lookup and resolution
-   Integration with objdump/nm for symbol information
-   Support for multiple symbol file formats
-   Runtime configuration editing through the GUI
