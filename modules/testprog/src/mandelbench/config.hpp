#pragma once

#ifndef MANDELBENCH_DEBUG
#define MANDELBENCH_DEBUG 0
#endif

inline constexpr bool MANDELBENCH_DEBUG_ENABLED = MANDELBENCH_DEBUG != 0;

inline constexpr int WIDTH = 2048;
inline constexpr int HEIGHT = 2048;

inline constexpr int MAX_ITERATION = 5000;

inline constexpr int THREADS = 8;
inline constexpr int REPEAT = 5;

inline constexpr const char* IMAGE = "./%s_%02d.png";
inline constexpr const char* REPORT = "./report.txt";
