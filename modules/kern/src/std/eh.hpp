#include <cstddef>
#include <defines/defines.hpp>
#include <exception>
#include <std/hcf.hpp>

struct __cxa_eh_globals {
    void* caughtExceptions;
    unsigned int uncaughtExceptions;
};

enum _Unwind_Action { _UA_SEARCH_PHASE = 1, _UA_CLEANUP_PHASE = 2, _UA_HANDLER_FRAME = 4, _UA_FORCE_UNWIND = 8 };

enum _Unwind_Reason_Code {
    _URC_NO_REASON = 0,
    _URC_FOREIGN_EXCEPTION_CAUGHT = 1,
    _URC_FATAL_PHASE2_ERROR = 2,
    _URC_FATAL_PHASE1_ERROR = 3,
    _URC_NORMAL_STOP = 4,
    _URC_END_OF_STACK = 5,
    _URC_HANDLER_FOUND = 6,
    _URC_INSTALL_CONTEXT = 7,
    _URC_CONTINUE_UNWIND = 8
};

struct _Unwind_Context;

struct _Unwind_Exception {
    uint64_t exception_class;
    void (*exception_cleanup)(_Unwind_Reason_Code reason, struct _Unwind_Exception* exc);
    uint64_t private_1;
    uint64_t private_2;
};

extern "C" __cxa_eh_globals* __cxa_get_globals();
extern "C" void* __cxa_begin_catch(void* exceptionObject);
extern "C" _Unwind_Reason_Code __gxx_personality_v0(int version, _Unwind_Action actions, uint64_t exceptionClass,
                                                    struct _Unwind_Exception* exceptionObject, struct _Unwind_Context* context);

namespace std {
void terminate() noexcept { hcf(); }
}  // namespace std
