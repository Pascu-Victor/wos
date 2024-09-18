#include "eh.hpp"

extern "C" __cxa_eh_globals* __cxa_get_globals() {
    static __cxa_eh_globals globals;
    return &globals;
}

extern "C" void* __cxa_begin_catch(void* exceptionObject) {
    __cxa_eh_globals* globals = __cxa_get_globals();
    globals->caughtExceptions = exceptionObject;
    globals->uncaughtExceptions--;
    return exceptionObject;
}

extern "C" _Unwind_Reason_Code __gxx_personality_v0(int version, _Unwind_Action actions, uint64_t exceptionClass,
                                                    struct _Unwind_Exception* exceptionObject, struct _Unwind_Context* context) {
    (void)version;
    (void)exceptionClass;
    (void)exceptionObject;
    (void)context;

    if (actions & _UA_SEARCH_PHASE) {
        // TODO: Check if this frame can handle the exception
        return _URC_CONTINUE_UNWIND;
    }

    if (actions & _UA_CLEANUP_PHASE) {
        // TODO: Perform any necessary cleanup
        return _URC_CONTINUE_UNWIND;
    }

    return _URC_FATAL_PHASE1_ERROR;
}
