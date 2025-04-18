#!/bin/bash

if [ -d toolchain ]; then
    rm -fr toolchain
fi

mkdir -p toolchain

git clone --branch release/20.x --depth 1 https://github.com/llvm/llvm-project.git

mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE:STRING=Release ../llvm/ -G Ninja
ninja

git clone http://llvm.org/git/libcxx.git
export TRIPLE=x86_64-pc-linux-musl
cd libcxx/lib
./buildit
