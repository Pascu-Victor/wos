#pragma once

// Host shim: stub out the kernel's mini_malloc internal header.
// The shim kmalloc.hpp uses libc malloc directly, so this is never called.
