#pragma once

#include <platform/mm/paging.hpp>

extern "C" auto _wOS_getCurrentPagemap() -> ker::mod::mm::paging::PageTable*;