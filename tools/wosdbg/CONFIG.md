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
            "path": "./build/modules/kern/wos"
        },
        {
            "from": "0x7f0000000000",
            "to": "0x7fffffffffff",
            "path": "./toolchain/mlibc-build/libc.so",
            "offset": "0x7f0000000000"
        }
    ]
}
```

### Fields

-   **lookups**: Array of address range mappings
-   **from**: Starting address of the range (hex string with 0x prefix)
-   **to**: Ending address of the range (hex string with 0x prefix)
-   **path**: Path to the symbol file (executable, ELF file, etc.)
-   **offset**: (Optional) Runtime load offset for shared libraries. This value is subtracted from the runtime address to get the file-relative address for symbol lookups. For shared libraries loaded at a specific base address, set this to the base address.

### Address Ranges

Common address ranges for different types of binaries:

-   **User space applications**: `0x400000` to `0x800000`
-   **Kernel space (x86_64)**: `0xffffffff80000000` to `0xffffffffffffffff`
-   **Shared libraries**: `0x7f0000000000` to `0x7fffffffffff`

### Load Offset Explained

For position-independent executables (PIE) and shared libraries, the addresses in the QEMU log are runtime virtual addresses where the binary was loaded. The ELF file contains symbols at file-relative addresses (typically starting from 0 for shared objects).

To correctly resolve symbols:
- **Kernel**: Usually linked at a fixed address (`0xffffffff80000000`), no offset needed
- **Init/user programs**: Linked at a fixed address (`0x400000`), no offset needed  
- **Shared libraries (libc.so, etc.)**: Loaded at runtime base address. Set `offset` to the load base address so runtime addresses can be converted to file-relative addresses.

### Default Configuration

If no `logview.json` file is found, the application will use these default mappings:

1. `0x400000-0x800000` → `./build/modules/init/init`
2. `0xffffffff80000000-0xffffffffffffffff` → `./build/modules/kern/wos`
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
