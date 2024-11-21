#pragma once

#define SIZE_MAX __SIZE_MAX__
#define SSIZE_MAX __SSIZE_MAX__
#define SSIZE_MIN __SSIZE_MIN__
#define INTMAX_MAX __INTMAX_MAX__
#define INTMAX_MIN __INTMAX_MIN__
#define UINTMAX_MAX __UINTMAX_MAX__
#define INT8_MAX __INT8_MAX__
#define INT8_MIN __INT8_MIN__
#define UINT8_MAX __UINT8_MAX__
#define INT16_MAX __INT16_MAX__
#define INT16_MIN __INT16_MIN__
#define UINT16_MAX __UINT16_MAX__
#define INT32_MAX __INT32_MAX__
#define INT32_MIN __INT32_MIN__
#define UINT32_MAX __UINT32_MAX__
#define INT64_MAX __INT64_MAX__
#define INT64_MIN __INT64_MIN__
#define UINT64_MAX __UINT64_MAX__
#define INT_LEAST8_MAX __INT_LEAST8_MAX__
#define INT_LEAST8_MIN __INT_LEAST8_MIN__
#define UINT_LEAST8_MAX __UINT_LEAST8_MAX__
#define INT_LEAST16_MAX __INT_LEAST16_MAX__
#define INT_LEAST16_MIN __INT_LEAST16_MIN__
#define UINT_LEAST16_MAX __UINT_LEAST16_MAX__
#define INT_LEAST32_MAX __INT_LEAST32_MAX__
#define INT_LEAST32_MIN __INT_LEAST32_MIN__
#define UINT_LEAST32_MAX __UINT_LEAST32_MAX__
#define INT_LEAST64_MAX __INT_LEAST64_MAX__
#define INT_LEAST64_MIN __INT_LEAST64_MIN__
#define UINT_LEAST64_MAX __UINT_LEAST64_MAX__
#define INT_FAST8_MAX __INT_FAST8_MAX__
#define INT_FAST8_MIN __INT_FAST8_MIN__
#define UINT_FAST8_MAX __UINT_FAST8_MAX__
#define INT_FAST16_MAX __INT_FAST16_MAX__
#define INT_FAST16_MIN __INT_FAST16_MIN__
#define UINT_FAST16_MAX __UINT_FAST16_MAX__
#define INT_FAST32_MAX __INT_FAST32_MAX__
#define INT_FAST32_MIN __INT_FAST32_MIN__
#define UINT_FAST32_MAX __UINT_FAST32_MAX__
#define INT_FAST64_MAX __INT_FAST64_MAX__
#define INT_FAST64_MIN __INT_FAST64_MIN__
#define UINT_FAST64_MAX __UINT_FAST64_MAX__
#define INTPTR_MAX __INTPTR_MAX__
#define INTPTR_MIN __INTPTR_MIN__
#define UINTPTR_MAX __UINTPTR_MAX__

using intmax_t = __INTMAX_TYPE__;
using uintmax_t = __UINTMAX_TYPE__;
using int8_t = __INT8_TYPE__;
using int16_t = __INT16_TYPE__;
using int32_t = __INT32_TYPE__;
using int64_t = __INT64_TYPE__;
using int_least8_t = __INT_LEAST8_TYPE__;
using int_least16_t = __INT_LEAST16_TYPE__;
using int_least32_t = __INT_LEAST32_TYPE__;
using int_least64_t = __INT_LEAST64_TYPE__;
using int_fast8_t = __INT_FAST8_TYPE__;
using int_fast16_t = __INT_FAST16_TYPE__;
using int_fast32_t = __INT_FAST32_TYPE__;
using int_fast64_t = __INT_FAST64_TYPE__;
using intptr_t = __INTPTR_TYPE__;
using uint8_t = __UINT8_TYPE__;
using uint16_t = __UINT16_TYPE__;
using uint32_t = __UINT32_TYPE__;
using uint64_t = __UINT64_TYPE__;
using uint_least8_t = __UINT_LEAST8_TYPE__;
using uint_least16_t = __UINT_LEAST16_TYPE__;
using uint_least32_t = __UINT_LEAST32_TYPE__;
using uint_least64_t = __UINT_LEAST64_TYPE__;
using uint_fast8_t = __UINT_FAST8_TYPE__;
using uint_fast16_t = __UINT_FAST16_TYPE__;
using uint_fast32_t = __UINT_FAST32_TYPE__;
using uint_fast64_t = __UINT_FAST64_TYPE__;
using uintptr_t = __UINTPTR_TYPE__;

typedef __SIZE_TYPE__ size_t;

typedef signed long long ssize_t;

typedef __PTRDIFF_TYPE__ ptrdiff_t;

typedef void (*funcpointer_t)(void);
