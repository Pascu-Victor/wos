#pragma once

#include <platform/mm/paging.hpp>

extern "C" auto wos_get_current_pagemap() -> ker::mod::mm::paging::PageTable*;
