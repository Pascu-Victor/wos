#pragma once

auto mandelbench_wki(int width, int height, int max_iteration, int workers, int repeat, const char* nodes) -> int;
auto mandelbench_worker(int argc, char** argv) -> int;
