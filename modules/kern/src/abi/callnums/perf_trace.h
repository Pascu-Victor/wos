#pragma once

// Versioned optional native-image tracing. Values are shared with pre-relocation
// assembly; keep this header free of C/C++ declarations and runtime dependencies.
// NOLINTBEGIN(cppcoreguidelines-macro-usage)
#define WOS_AT_PERF_TRACE_VERSION 0x57505301
#define WOS_AT_PERF_TRACE_SESSION 0x57505302
#define WOS_AT_PERF_TRACE_CORRELATION 0x57505303
#define WOS_PERF_TRACE_VERSION 1
#define WOS_PERF_TRACE_SYS_LOG 0
#define WOS_PERF_TRACE_LOG_OP 5
#define WOS_PERF_STAGE_IMAGE_ENTRY 1
#define WOS_PERF_STAGE_RTLD_READY 2
#define WOS_PERF_STAGE_CRT_ENTRY 3
#define WOS_PERF_STAGE_MAIN_BEGIN 4
#define WOS_PERF_STAGE_MAIN_END 5
// Optional intermediate milestones. IDs are append-only, not time ordered.
// Older version-1 kernels may reject these; the caller ignores that result.
#define WOS_PERF_STAGE_RTLD_DEPENDENCIES_READY 6
#define WOS_PERF_STAGE_RTLD_LINK_READY 7
#define WOS_PERF_STAGE_RTLD_TCB_READY 8
// NOLINTEND(cppcoreguidelines-macro-usage)
