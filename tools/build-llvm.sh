#!/bin/bash
cmake  -S llvm-project/llvm \
       -B llvm-build \
       -GNinja \
       -DLLVM_USE_LINKER=lld \
       -DLLVM_TARGETS_TO_BUILD=X86 \
       -DLLVM_DEFAULT_TARGET_TRIPLE=x86_64-wos-elf \
       -DLLVM_ENABLE_PROJECTS="clang;lld;" \
       -DLIBCXX_ENABLE_THREADS=OFF \
       -DLIBCXX_ENABLE_LOCALIZATION=OFF \
       -DLIBCXX_ENABLE_WIDE_CHARACTERS=OFF \
       -DLIBCXXABI_ENABLE_THREADS=OFF \
       -DLIBCXXABI_HAS_NO_THREADS=ON \
       -DLIBCXXABI_USE_LLVM_UNWINDER=OFF \
       -DCMAKE_BUILD_TYPE=Release
cmake --build build
