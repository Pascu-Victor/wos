/* -*- mode: c; tab-width: 2; indent-tabs-mode: nil; -*-
Copyright (c) 2012 Marcus Geelnard
Copyright (c) 2013-2014 Evan Nemerson

This software is provided 'as-is', without any express or implied
warranty. In no event will the authors be held liable for any damages
arising from the use of this software.

Permission is granted to anyone to use this software for any purpose,
including commercial applications, and to alter it and redistribute it
freely, subject to the following restrictions:

    1. The origin of this software must not be misrepresented; you must not
    claim that you wrote the original software. If you use this software
    in a product, an acknowledgment in the product documentation would be
    appreciated but is not required.

    2. Altered source versions must be plainly marked as such, and must not be
    misrepresented as being the original software.

    3. This notice may not be removed or altered from any source
    distribution.
*/

#include "tinycthread.hpp"

#include <cstdlib>
#include <ctime>

/* Platform specific includes */
#if defined(_TTHREAD_POSIX_)
#include <sched.h>
#include <sys/time.h>
#include <unistd.h>

#include <cerrno>
#elif defined(_TTHREAD_WIN32_)
#include <process.h>
#include <sys/timeb.h>
#endif

/* Standard, good-to-have defines */
#ifndef TRUE
#define TRUE 1
#endif
#ifndef FALSE
#define FALSE 0
#endif

#ifdef __cplusplus
extern "C" {
#endif

int mtx_init(mtx_t* mtx, int type) {
#if defined(_TTHREAD_WIN32_)
    mtx->mAlreadyLocked = FALSE;
    mtx->mRecursive = type & MTX_RECURSIVE;
    mtx->mTimed = type & MTX_TIMED;
    if (!mtx->mTimed) {
        InitializeCriticalSection(&(mtx->mHandle.cs));
    } else {
        mtx->mHandle.mut = CreateMutex(nullptr, FALSE, nullptr);
        if (mtx->mHandle.mut == nullptr) {
            return THRD_ERROR;
        }
    }
    return THRD_SUCCESS;
#else
    int ret = 0;
    pthread_mutexattr_t attr;
    pthread_mutexattr_init(&attr);
    if ((type & MTX_RECURSIVE) != 0) {
        pthread_mutexattr_settype(&attr, PTHREAD_MUTEX_RECURSIVE);
    }
    ret = pthread_mutex_init(mtx, &attr);
    pthread_mutexattr_destroy(&attr);
    return ret == 0 ? THRD_SUCCESS : THRD_ERROR;
#endif
}

void mtx_destroy(mtx_t* mtx) {
#if defined(_TTHREAD_WIN32_)
    if (!mtx->mTimed) {
        DeleteCriticalSection(&(mtx->mHandle.cs));
    } else {
        CloseHandle(mtx->mHandle.mut);
    }
#else
    pthread_mutex_destroy(mtx);
#endif
}

int mtx_lock(mtx_t* mtx) {
#if defined(_TTHREAD_WIN32_)
    if (!mtx->mTimed) {
        EnterCriticalSection(&(mtx->mHandle.cs));
    } else {
        switch (WaitForSingleObject(mtx->mHandle.mut, INFINITE)) {
            case WAIT_OBJECT_0:
                break;
            case WAIT_ABANDONED:
            default:
                return THRD_ERROR;
        }
    }

    if (!mtx->mRecursive) {
        while (mtx->mAlreadyLocked) Sleep(1); /* Simulate deadlock... */
        mtx->mAlreadyLocked = TRUE;
    }
    return THRD_SUCCESS;
#else
    return pthread_mutex_lock(mtx) == 0 ? THRD_SUCCESS : THRD_ERROR;
#endif
}

int mtx_timedlock(mtx_t* mtx, const timespec* ts) {
#if defined(_TTHREAD_WIN32_)
    timespec current_ts;
    DWORD timeoutMs;

    if (!mtx->mTimed) {
        return THRD_ERROR;
    }

    timespec_get(&current_ts, TIME_UTC);

    if ((current_ts.tv_sec > ts->tv_sec) || ((current_ts.tv_sec == ts->tv_sec) && (current_ts.tv_nsec >= ts->tv_nsec))) {
        timeoutMs = 0;
    } else {
        timeoutMs = static_cast<DWORD>(ts->tv_sec - current_ts.tv_sec) * 1000;
        timeoutMs += (ts->tv_nsec - current_ts.tv_nsec) / 1000000;
        timeoutMs += 1;
    }

    /* TODO: the timeout for WaitForSingleObject doesn't include time
       while the computer is asleep. */
    switch (WaitForSingleObject(mtx->mHandle.mut, timeoutMs)) {
        case WAIT_OBJECT_0:
            break;
        case WAIT_TIMEOUT:
            return thrd_timedout;
        case WAIT_ABANDONED:
        default:
            return THRD_ERROR;
    }

    if (!mtx->mRecursive) {
        while (mtx->mAlreadyLocked) Sleep(1); /* Simulate deadlock... */
        mtx->mAlreadyLocked = TRUE;
    }

    return THRD_SUCCESS;
#elif defined(_POSIX_TIMEOUTS) && (_POSIX_TIMEOUTS >= 200112L) && defined(_POSIX_THREADS) && (_POSIX_THREADS >= 200112L)
    switch (pthread_mutex_timedlock(mtx, ts)) {
        case 0:
            return THRD_SUCCESS;
        case ETIMEDOUT:
            return THRD_TIMEDOUT;
        default:
            return THRD_ERROR;
    }
#else
    int rc;
    timespec cur, dur;

    /* Try to acquire the lock and, if we fail, sleep for 5ms. */
    while ((rc = pthread_mutex_trylock(mtx)) == EBUSY) {
        timespec_get(&cur, TIME_UTC);

        if ((cur.tv_sec > ts->tv_sec) || ((cur.tv_sec == ts->tv_sec) && (cur.tv_nsec >= ts->tv_nsec))) {
            break;
        }

        dur.tv_sec = ts->tv_sec - cur.tv_sec;
        dur.tv_nsec = ts->tv_nsec - cur.tv_nsec;
        if (dur.tv_nsec < 0) {
            dur.tv_sec--;
            dur.tv_nsec += 1000000000;
        }

        if ((dur.tv_sec != 0) || (dur.tv_nsec > 5000000)) {
            dur.tv_sec = 0;
            dur.tv_nsec = 5000000;
        }

        nanosleep(&dur, nullptr);
    }

    switch (rc) {
        case 0:
            return THRD_SUCCESS;
        case ETIMEDOUT:
        case EBUSY:
            return thrd_timedout;
        default:
            return THRD_ERROR;
    }
#endif
}

int mtx_trylock(mtx_t* mtx) {
#if defined(_TTHREAD_WIN32_)
    int ret;

    if (!mtx->mTimed) {
        ret = TryEnterCriticalSection(&(mtx->mHandle.cs)) ? THRD_SUCCESS : THRD_BUSY;
    } else {
        ret = (WaitForSingleObject(mtx->mHandle.mut, 0) == WAIT_OBJECT_0) ? THRD_SUCCESS : THRD_BUSY;
    }

    if ((!mtx->mRecursive) && (ret == THRD_SUCCESS)) {
        if (mtx->mAlreadyLocked) {
            LeaveCriticalSection(&(mtx->mHandle.cs));
            ret = THRD_BUSY;
        } else {
            mtx->mAlreadyLocked = TRUE;
        }
    }
    return ret;
#else
    return (pthread_mutex_trylock(mtx) == 0) ? THRD_SUCCESS : THRD_BUSY;
#endif
}

int mtx_unlock(mtx_t* mtx) {
#if defined(_TTHREAD_WIN32_)
    mtx->mAlreadyLocked = FALSE;
    if (!mtx->mTimed) {
        LeaveCriticalSection(&(mtx->mHandle.cs));
    } else {
        if (!ReleaseMutex(mtx->mHandle.mut)) {
            return THRD_ERROR;
        }
    }
    return THRD_SUCCESS;
#else
    return pthread_mutex_unlock(mtx) == 0 ? THRD_SUCCESS : THRD_ERROR;
    ;
#endif
}

#if defined(_TTHREAD_WIN32_)
#define _CONDITION_EVENT_ONE 0
#define _CONDITION_EVENT_ALL 1
#endif

int cnd_init(cnd_t* cond) {
#if defined(_TTHREAD_WIN32_)
    cond->mWaitersCount = 0;

    /* Init critical section */
    InitializeCriticalSection(&cond->mWaitersCountLock);

    /* Init events */
    cond->mEvents[_CONDITION_EVENT_ONE] = CreateEvent(nullptr, FALSE, FALSE, nullptr);
    if (cond->mEvents[_CONDITION_EVENT_ONE] == nullptr) {
        cond->mEvents[_CONDITION_EVENT_ALL] = nullptr;
        return THRD_ERROR;
    }
    cond->mEvents[_CONDITION_EVENT_ALL] = CreateEvent(nullptr, TRUE, FALSE, nullptr);
    if (cond->mEvents[_CONDITION_EVENT_ALL] == nullptr) {
        CloseHandle(cond->mEvents[_CONDITION_EVENT_ONE]);
        cond->mEvents[_CONDITION_EVENT_ONE] = nullptr;
        return THRD_ERROR;
    }

    return THRD_SUCCESS;
#else
    return pthread_cond_init(cond, nullptr) == 0 ? THRD_SUCCESS : THRD_ERROR;
#endif
}

void cnd_destroy(cnd_t* cond) {
#if defined(_TTHREAD_WIN32_)
    if (cond->mEvents[_CONDITION_EVENT_ONE] != nullptr) {
        CloseHandle(cond->mEvents[_CONDITION_EVENT_ONE]);
    }
    if (cond->mEvents[_CONDITION_EVENT_ALL] != nullptr) {
        CloseHandle(cond->mEvents[_CONDITION_EVENT_ALL]);
    }
    DeleteCriticalSection(&cond->mWaitersCountLock);
#else
    pthread_cond_destroy(cond);
#endif
}

int cnd_signal(cnd_t* cond) {
#if defined(_TTHREAD_WIN32_)
    int haveWaiters;

    /* Are there any waiters? */
    EnterCriticalSection(&cond->mWaitersCountLock);
    haveWaiters = (cond->mWaitersCount > 0);
    LeaveCriticalSection(&cond->mWaitersCountLock);

    /* If we have any waiting threads, send them a signal */
    if (haveWaiters) {
        if (SetEvent(cond->mEvents[_CONDITION_EVENT_ONE]) == 0) {
            return THRD_ERROR;
        }
    }

    return THRD_SUCCESS;
#else
    return pthread_cond_signal(cond) == 0 ? THRD_SUCCESS : THRD_ERROR;
#endif
}

int cnd_broadcast(cnd_t* cond) {
#if defined(_TTHREAD_WIN32_)
    int haveWaiters;

    /* Are there any waiters? */
    EnterCriticalSection(&cond->mWaitersCountLock);
    haveWaiters = (cond->mWaitersCount > 0);
    LeaveCriticalSection(&cond->mWaitersCountLock);

    /* If we have any waiting threads, send them a signal */
    if (haveWaiters) {
        if (SetEvent(cond->mEvents[_CONDITION_EVENT_ALL]) == 0) {
            return THRD_ERROR;
        }
    }

    return THRD_SUCCESS;
#else
    return pthread_cond_broadcast(cond) == 0 ? THRD_SUCCESS : THRD_ERROR;
#endif
}

#if defined(_TTHREAD_WIN32_)
static int _cnd_timedwait_win32(cnd_t* cond, mtx_t* mtx, DWORD timeout) {
    int result, lastWaiter;

    /* Increment number of waiters */
    EnterCriticalSection(&cond->mWaitersCountLock);
    ++cond->mWaitersCount;
    LeaveCriticalSection(&cond->mWaitersCountLock);

    /* Release the mutex while waiting for the condition (will decrease
       the number of waiters when done)... */
    mtx_unlock(mtx);

    /* Wait for either event to become signaled due to cnd_signal() or
       cnd_broadcast() being called */
    result = WaitForMultipleObjects(2, cond->mEvents, FALSE, timeout);
    if (result == WAIT_TIMEOUT) {
        return thrd_timedout;
    } else if (result == static_cast<int>(WAIT_FAILED)) {
        return THRD_ERROR;
    }

    /* Check if we are the last waiter */
    EnterCriticalSection(&cond->mWaitersCountLock);
    --cond->mWaitersCount;
    lastWaiter = (result == (WAIT_OBJECT_0 + _CONDITION_EVENT_ALL)) && (cond->mWaitersCount == 0);
    LeaveCriticalSection(&cond->mWaitersCountLock);

    /* If we are the last waiter to be notified to stop waiting, reset the event */
    if (lastWaiter) {
        if (ResetEvent(cond->mEvents[_CONDITION_EVENT_ALL]) == 0) {
            return THRD_ERROR;
        }
    }

    /* Re-acquire the mutex */
    mtx_lock(mtx);

    return THRD_SUCCESS;
}
#endif

int cnd_wait(cnd_t* cond, mtx_t* mtx) {
#if defined(_TTHREAD_WIN32_)
    return _cnd_timedwait_win32(cond, mtx, INFINITE);
#else
    return pthread_cond_wait(cond, mtx) == 0 ? THRD_SUCCESS : THRD_ERROR;
#endif
}

auto cnd_timedwait(cnd_t* cond, mtx_t* mtx, const timespec* ts) -> int {
#if defined(_TTHREAD_WIN32_)
    timespec now;
    if (timespec_get(&now, TIME_UTC) == TIME_UTC) {
        DWORD delta = static_cast<DWORD>((ts->tv_sec - now.tv_sec) * 1000 + (ts->tv_nsec - now.tv_nsec + 500000) / 1000000);
        return _cnd_timedwait_win32(cond, mtx, delta);
    } else
        return THRD_ERROR;
#else
    int ret = 0;
    ret = pthread_cond_timedwait(cond, mtx, ts);
    if (ret == ETIMEDOUT) {
        return THRD_TIMEDOUT;
    }
    return ret == 0 ? THRD_SUCCESS : THRD_ERROR;
#endif
}

#if defined(_TTHREAD_WIN32_)
struct TinyCThreadTSSData {
    void* value;
    tss_t key;
    struct TinyCThreadTSSData* next;
};

static tss_dtor_t _tinycthread_tss_dtors[1088] = {
    nullptr,
};

static _Thread_local struct TinyCThreadTSSData* _tinycthread_tss_head = nullptr;
static _Thread_local struct TinyCThreadTSSData* _tinycthread_tss_tail = nullptr;

static void _tinycthread_tss_cleanup() {
    struct TinyCThreadTSSData* data;
    int iteration;
    unsigned int again = 1;
    void* value;

    for (iteration = 0; iteration < TSS_DTOR_ITERATIONS && again > 0; iteration++) {
        again = 0;
        for (data = _tinycthread_tss_head; data != nullptr; data = data->next) {
            if (data->value != nullptr) {
                value = data->value;
                data->value = nullptr;

                if (_tinycthread_tss_dtors[data->key] != nullptr) {
                    again = 1;
                    _tinycthread_tss_dtors[data->key](value);
                }
            }
        }
    }

    while (_tinycthread_tss_head != nullptr) {
        data = _tinycthread_tss_head->next;
        std::free(_tinycthread_tss_head);
        _tinycthread_tss_head = data;
    }
    _tinycthread_tss_head = nullptr;
    _tinycthread_tss_tail = nullptr;
}

static void NTAPI _tinycthread_tss_callback(PVOID h, DWORD dwReason, PVOID pv) {
    (void)h;
    (void)pv;

    if (_tinycthread_tss_head != nullptr && (dwReason == DLL_THREAD_DETACH || dwReason == DLL_PROCESS_DETACH)) {
        _tinycthread_tss_cleanup();
    }
}

#if defined(_MSC_VER)
#pragma data_seg(".CRT$XLB")
PIMAGE_TLS_CALLBACK p_thread_callback = _tinycthread_tss_callback;
#pragma data_seg()
#else
PIMAGE_TLS_CALLBACK p_thread_callback __attribute__((section(".CRT$XLB"))) = _tinycthread_tss_callback;
#endif
#endif /* defined(_TTHREAD_WIN32_) */

/** Information to pass to the new thread (what to run). */
using thread_start_info = struct {
    thrd_start_t m_function; /**< Pointer to the function to be executed. */
    void* m_arg;             /**< Function argument for the thread function. */
};

/* Thread wrapper function. */
namespace {
#if defined(_TTHREAD_WIN32_)
DWORD WINAPI thrd_wrapper_function(LPVOID a_arg)
#elif defined(_TTHREAD_POSIX_)
auto thrd_wrapper_function(void* a_arg) -> void*
#endif
{
    thrd_start_t fun = nullptr;
    void* arg = nullptr;
    int res = 0;
#if defined(_TTHREAD_POSIX_)
    void* pres = nullptr;
#endif

    /* Get thread startup information */
    auto* ti = static_cast<thread_start_info*>(a_arg);
    fun = ti->m_function;
    arg = ti->m_arg;

    /* The thread is responsible for freeing the startup information */
    delete ti;

    /* Call the actual client thread function */
    res = fun(arg);

#if defined(_TTHREAD_WIN32_)
    if (_tinycthread_tss_head != nullptr) {
        _tinycthread_tss_cleanup();
    }

    return res;
#else
    pres = new int(res);
    if (pres != nullptr) {
        *static_cast<int*>(pres) = res;
    }
    return pres;
#endif
}
}  // namespace

auto thrd_create(thrd_t* thr, thrd_start_t func, void* arg) -> int {
    /* Fill out the thread startup information (passed to the thread wrapper,
       which will eventually free it) */
    auto* ti = new thread_start_info;
    if (ti == nullptr) {
        return THRD_NOMEM;
    }
    ti->m_function = func;
    ti->m_arg = arg;

    /* Create the thread */
#if defined(_TTHREAD_WIN32_)
    *thr = CreateThread(nullptr, 0, thrd_wrapper_function, static_cast<LPVOID>(ti), 0, nullptr);
    bool const CREATED = (*thr) != nullptr;
#elif defined(_TTHREAD_POSIX_)
    bool const CREATED = pthread_create(thr, nullptr, thrd_wrapper_function, static_cast<void*>(ti)) == 0;
#endif

    /* Did we fail to create the thread? */
    if (!CREATED) {
        delete ti;
        return THRD_ERROR;
    }

    return THRD_SUCCESS;
}

thrd_t thrd_current(void) {
#if defined(_TTHREAD_WIN32_)
    return GetCurrentThread();
#else
    return pthread_self();
#endif
}

int thrd_detach(thrd_t thr) {
#if defined(_TTHREAD_WIN32_)
    /* https://stackoverflow.com/questions/12744324/how-to-detach-a-thread-on-windows-c#answer-12746081 */
    return CloseHandle(thr) != 0 ? THRD_SUCCESS : THRD_ERROR;
#else
    return pthread_detach(thr) == 0 ? THRD_SUCCESS : THRD_ERROR;
#endif
}

int thrd_equal(thrd_t thr0, thrd_t thr1) {
#if defined(_TTHREAD_WIN32_)
    return thr0 == thr1;
#else
    return pthread_equal(thr0, thr1);
#endif
}

void thrd_exit(int res) {
#if defined(_TTHREAD_WIN32_)
    if (_tinycthread_tss_head != nullptr) {
        _tinycthread_tss_cleanup();
    }

    ExitThread(res);
#else
    void* pres = new int(res);
    if (pres != nullptr) {
        *static_cast<int*>(pres) = res;
    }
    pthread_exit(pres);
#endif
}

int thrd_join(thrd_t thr, int* res) {
#if defined(_TTHREAD_WIN32_)
    DWORD dwRes;

    if (WaitForSingleObject(thr, INFINITE) == WAIT_FAILED) {
        return THRD_ERROR;
    }
    if (res != nullptr) {
        if (GetExitCodeThread(thr, &dwRes) != 0) {
            *res = dwRes;
        } else {
            return THRD_ERROR;
        }
    }
    CloseHandle(thr);
#elif defined(_TTHREAD_POSIX_)
    void* pres = nullptr;
    int ires = 0;
    if (pthread_join(thr, &pres) != 0) {
        return THRD_ERROR;
    }
    if (pres != nullptr) {
        ires = *static_cast<int*>(pres);
        delete static_cast<int*>(pres);
    }
    if (res != nullptr) {
        *res = ires;
    }
#endif
    return THRD_SUCCESS;
}

int thrd_sleep(const timespec* duration, timespec* remaining) {
#if !defined(_TTHREAD_WIN32_)
    return nanosleep(duration, remaining);
#else
    timespec start;
    DWORD t;

    timespec_get(&start, TIME_UTC);

    t = SleepEx(static_cast<DWORD>(duration->tv_sec * 1000 + duration->tv_nsec / 1000000 + (((duration->tv_nsec % 1000000) == 0) ? 0 : 1)),
                TRUE);

    if (t == 0) {
        return 0;
    } else if (remaining != nullptr) {
        timespec_get(remaining, TIME_UTC);
        remaining->tv_sec -= start.tv_sec;
        remaining->tv_nsec -= start.tv_nsec;
        if (remaining->tv_nsec < 0) {
            remaining->tv_nsec += 1000000000;
            remaining->tv_sec -= 1;
        }
    } else {
        return -1;
    }

    return 0;
#endif
}

void thrd_yield(void) {
#if defined(_TTHREAD_WIN32_)
    Sleep(0);
#else
    sched_yield();
#endif
}

int tss_create(tss_t* key, tss_dtor_t dtor) {
#if defined(_TTHREAD_WIN32_)
    *key = TlsAlloc();
    if (*key == TLS_OUT_OF_INDEXES) {
        return THRD_ERROR;
    }
    _tinycthread_tss_dtors[*key] = dtor;
#else
    if (pthread_key_create(key, dtor) != 0) {
        return THRD_ERROR;
    }
#endif
    return THRD_SUCCESS;
}

void tss_delete(tss_t key) {
#if defined(_TTHREAD_WIN32_)
    auto* data = static_cast<TinyCThreadTSSData*>(TlsGetValue(key));
    TinyCThreadTSSData* prev = nullptr;
    if (data != nullptr) {
        if (data == _tinycthread_tss_head) {
            _tinycthread_tss_head = data->next;
        } else {
            prev = _tinycthread_tss_head;
            if (prev != nullptr) {
                while (prev->next != data) {
                    prev = prev->next;
                }
            }
        }

        if (data == _tinycthread_tss_tail) {
            _tinycthread_tss_tail = prev;
        }

        std::free(data);
    }
    _tinycthread_tss_dtors[key] = nullptr;
    TlsFree(key);
#else
    pthread_key_delete(key);
#endif
}

void* tss_get(tss_t key) {
#if defined(_TTHREAD_WIN32_)
    auto* data = static_cast<TinyCThreadTSSData*>(TlsGetValue(key));
    if (data == nullptr) {
        return nullptr;
    }
    return data->value;
#else
    return pthread_getspecific(key);
#endif
}

int tss_set(tss_t key, void* val) {
#if defined(_TTHREAD_WIN32_)
    auto* data = static_cast<TinyCThreadTSSData*>(TlsGetValue(key));
    if (data == nullptr) {
        data = static_cast<TinyCThreadTSSData*>(std::malloc(sizeof(TinyCThreadTSSData)));
        if (data == nullptr) {
            return THRD_ERROR;
        }

        data->value = nullptr;
        data->key = key;
        data->next = nullptr;

        if (_tinycthread_tss_tail != nullptr) {
            _tinycthread_tss_tail->next = data;
        } else {
            _tinycthread_tss_tail = data;
        }

        if (_tinycthread_tss_head == nullptr) {
            _tinycthread_tss_head = data;
        }

        if (!TlsSetValue(key, data)) {
            std::free(data);
            return THRD_ERROR;
        }
    }
    data->value = val;
#else
    if (pthread_setspecific(key, val) != 0) {
        return THRD_ERROR;
    }
#endif
    return THRD_SUCCESS;
}

#if defined(_TTHREAD_EMULATE_TIMESPEC_GET_)
int _tthread_timespec_get(timespec* ts, int base) {
#if defined(_TTHREAD_WIN32_)
    struct _timeb tb;
#elif !defined(CLOCK_REALTIME)
    struct timeval tv;
#endif

    if (base != TIME_UTC) {
        return 0;
    }

#if defined(_TTHREAD_WIN32_)
    _ftime64_s(&tb);
    ts->tv_sec = static_cast<time_t>(tb.time);
    ts->tv_nsec = 1000000L * static_cast<long>(tb.millitm);
#elif defined(CLOCK_REALTIME)
    base = (clock_gettime(CLOCK_REALTIME, ts) == 0) ? base : 0;
#else
    gettimeofday(&tv, nullptr);
    ts->tv_sec = static_cast<time_t>(tv.tv_sec);
    ts->tv_nsec = 1000L * static_cast<long>(tv.tv_usec);
#endif

    return base;
}
#endif /* _TTHREAD_EMULATE_TIMESPEC_GET_ */

#if defined(_TTHREAD_WIN32_)
void call_once(once_flag* flag, void (*func)(void)) {
    /* The idea here is that we use a spin lock (via the
       InterlockedCompareExchange function) to restrict access to the
       critical section until we have initialized it, then we use the
       critical section to block until the callback has completed
       execution. */
    while (flag->status < 3) {
        switch (flag->status) {
            case 0:
                if (InterlockedCompareExchange(&(flag->status), 1, 0) == 0) {
                    InitializeCriticalSection(&(flag->lock));
                    EnterCriticalSection(&(flag->lock));
                    flag->status = 2;
                    func();
                    flag->status = 3;
                    LeaveCriticalSection(&(flag->lock));
                    return;
                }
                break;
            case 1:
                break;
            case 2:
                EnterCriticalSection(&(flag->lock));
                LeaveCriticalSection(&(flag->lock));
                break;
        }
    }
}
#endif /* defined(_TTHREAD_WIN32_) */

#ifdef __cplusplus
}
#endif
