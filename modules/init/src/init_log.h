#pragma once

#include <callnums/sys_log.h>

void init_log(ker::abi::sys_log::sys_log_level level, const char* fmt, ...);
void init_info(const char* fmt, ...);
void init_warn(const char* fmt, ...);
void init_error(const char* fmt, ...);
