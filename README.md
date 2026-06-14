# WOS

Welcome to WOS, a multi-system, multi-tasking operating system.

**Warning** here be daemons! this is an in-development project you will need to compile the toolchain using the instructions in build-llvm.sh this project for now is in a "works on my system" basis.

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
- will set up the configured WOS cluster topology and launch the VMs
```
wos-cluster --launch
```

- to start VM0 paused with a GDB stub on port 1234
```
wos-cluster --launch --debug-node 0
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
