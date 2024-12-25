# WOS

Welcome to WOS, a multi-system, multi-tasking operating system.

## Overview

WOS aims to create a modern, efficient, and seamless multitasking system that scales across multiple networked computers

## Features

- [In Development]
- More details coming soon

## Getting Started

Clone the repo
```
git clone --recurse-submodules
```

### Building

Build llvm
```
cd tools
./build_llvm.sh
```

Build wos
```
cd ..
cmake -GNinja -B build .
cmake --build build
```

### Running

start via terminal
- will start a qemu instance with debugging enabled on port 1234 attach gdb to debug
```
./scripts/run_kern.sh
```

- in a separate terminal
```
gdb build/modules/kern/wos
target remote localhost:1234
continue
```


## License
See LICENSE.md

## Contact

For inquiries, please open an issue in this repository.
