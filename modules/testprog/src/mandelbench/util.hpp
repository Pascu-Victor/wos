#pragma once
#include <sys/timeb.h>

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <vector>

#ifndef _UTIL_H
#define _UTIL_H

struct arg {
    unsigned char* image;
    unsigned char* colormap;
    int width;
    int height;
    int max;
    int id;
    int threads;
    // timing filled in by each thread
    uint64_t thread_start_ns;
    uint64_t thread_end_ns;
    uint64_t thread_cpu_ns;
    uint64_t cpu_mask;
    uint64_t total_iterations;
    uint64_t rows_completed;
    int cpu_id;  // which CPU this thread ran on
    int cpu_end_id;
    uint32_t cpu_changes;
};

unsigned char color2byte(float v);

void hsv2rgb(float h, float s, float v, unsigned char* rgb);

void init_colormap(int len, unsigned char* map);

void set_pixel(unsigned char* image, int width, int x, int y, unsigned char* c);

void save_image(const char* filename, const unsigned char* image, unsigned width, unsigned height);

void description(const char* name, int width, int height, int max_iteration, int threads, char* desc);

void progress(const char* name, int width, int height, int max_iteration, int threads, int repeat, int r, double time);

void report(const char* name, int width, int height, int max_iteration, int threads, int repeat, const std::vector<double>& times);

#endif
