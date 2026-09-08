// Static fault fixture: its complete coredump is small and has no dynamic
// loader/runtime dependency. Resource policy is inherited from the test child.
extern "C" [[noreturn]] void _start() {
    asm volatile("ud2");
    __builtin_unreachable();
}
