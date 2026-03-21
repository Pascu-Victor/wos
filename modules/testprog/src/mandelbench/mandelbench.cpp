#define _DEFAULT_SOURCE 1
#include <sys/select.h>
#include <sys/time.h>

#include <algorithm>
#include <cstddef>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <vector>

#include "tinycthread.h"
#include "util.hpp"

/* This should be conveted into a GPU kernel */
int generate_image(void* param) {
    int row = 0;
    int col = 0;
    int index = 0;
    int iteration = 0;
    double c_re = 0.0;
    double c_im = 0.0;
    double x = 0.0;
    double y = 0.0;
    double x_new = 0.0;

    auto* a = static_cast<struct arg*>(param);

    unsigned char* image = a->image;
    unsigned char* colormap = a->colormap;
    int width = a->width;
    int height = a->height;
    int max = a->max;

    for (row = 0; row < height; row++) {
        for (col = 0; col < width; col++) {
            index = (row * height) + col;
            if (index % a->threads != a->id) {
                continue;
            }
            c_re = (col - (width / 2.0)) * 4.0 / width;
            c_im = (row - (height / 2.0)) * 4.0 / width;
            x = 0, y = 0;
            iteration = 0;
            while ((x * x) + (y * y) <= 4 && iteration < max) {
                x_new = (x * x) - (y * y) + c_re;
                y = (2 * x * y) + c_im;
                x = x_new;
                iteration++;
            }
            iteration = std::min(iteration, max);
            set_pixel(image, width, col, row, &colormap[static_cast<ptrdiff_t>(iteration * 3)]);
        }
    }
    return 0;
}

uint64_t get_time() {
    timeval tv{};
    gettimeofday(&tv, nullptr);
    return (static_cast<uint64_t>(tv.tv_sec) * 1000) + (tv.tv_usec / 1000);
}

constexpr auto DEVICE_NAME = "cpu";

int mandelbench(int width, int height, int max_iteration, int threads, int repeat, unsigned char* image, unsigned char* colormap) {
    std::vector<struct arg> a(threads);
    std::vector<thrd_t> t(threads);
    std::vector<double> times(repeat);
    uint64_t start_time = 0;
    uint64_t end_time = 0;
    int i, r;
    char path[255];

    for (r = 0; r < repeat; r++) {
        memset(image, 0, width * height * 4);

        start_time = get_time();

        for (i = 0; i < threads; i++) {
            a[i].image = image;
            a[i].colormap = colormap;
            a[i].width = width;
            a[i].height = height;
            a[i].max = max_iteration;
            a[i].id = i;
            a[i].threads = threads;

            thrd_create(&t[i], generate_image, &a[i]);
        }

        for (i = 0; i < threads; i++) {
            thrd_join(t[i], NULL);
        }

        end_time = get_time();
        times[r] = (double)(end_time - start_time) / 1000.0;

        sprintf(path, IMAGE, DEVICE_NAME, r);
        save_image(path, image, width, height);
        progress(DEVICE_NAME, r, times[r]);
    }
    report(DEVICE_NAME, times);

    return 0;
}
