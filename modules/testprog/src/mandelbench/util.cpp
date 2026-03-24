#include "util.hpp"

#include <algorithm>
#include <cmath>
#include <print>

#include "config.hpp"
#include "lodepng.hpp"

unsigned char color2byte(float v) {
    float c = v * 255;
    c = std::max<float>(c, 0);
    c = std::min<float>(c, 255);
    return (unsigned char)c;
}

void hsv2rgb(float h, float s, float v, unsigned char* rgb) {
    int i;
    float f, p, q, t, r, g, b;

    if (s == 0) {
        r = g = b = v;
        return;
    }

    h /= 60;
    i = (int)floor(h);
    f = h - i;
    p = v * (1 - s);
    q = v * (1 - s * f);
    t = v * (1 - s * (1 - f));

    switch (i) {
        case 0:
            r = v;
            g = t;
            b = p;
            break;
        case 1:
            r = q;
            g = v;
            b = p;
            break;
        case 2:
            r = p;
            g = v;
            b = t;
            break;
        case 3:
            r = p;
            g = q;
            b = v;
            break;
        case 4:
            r = t;
            g = p;
            b = v;
            break;
        default:
            r = v;
            g = p;
            b = q;
            break;
    }

    rgb[0] = color2byte(r);
    rgb[1] = color2byte(g);
    rgb[2] = color2byte(b);
}

void init_colormap(int len, unsigned char* map) {
    int i;
    for (i = 0; i < len; i++) {
        hsv2rgb(i / 4.0f, 1.0f, i / (i + 8.0f), &map[i * 3]);
    }
    map[3 * len + 0] = 0;
    map[3 * len + 1] = 0;
    map[3 * len + 2] = 0;
}

void set_pixel(unsigned char* image, int width, int x, int y, unsigned char* c) {
    image[4 * width * y + 4 * x + 0] = c[0];
    image[4 * width * y + 4 * x + 1] = c[1];
    image[4 * width * y + 4 * x + 2] = c[2];
    image[4 * width * y + 4 * x + 3] = 255;
}

void save_image(const char* filename, const unsigned char* image, unsigned width, unsigned height) {
    unsigned error;
    unsigned char* png;
    size_t pngsize;
    LodePNGState state;

    lodepng_state_init(&state);

    error = lodepng_encode(&png, &pngsize, image, width, height, &state);
    if (!error) {
        lodepng_save_file(png, pngsize, filename);
    }
    if (error) {
        std::println(stderr, "ERROR: {}: {}", error, lodepng_error_text(error));
    }
    lodepng_state_cleanup(&state);
    free(png);
}

void description(const char* name, int width, int height, int max_iteration, int threads, char* desc) {
    if (strcmp("cpu", name) == 0) {
        sprintf(desc, "width=%d height=%d iterations=%d threads=%d", width, height, max_iteration, threads);
    } else {
        sprintf(desc, "-");
    }
}

void progress(const char* name, int width, int height, int max_iteration, int threads, int repeat, int r, double time) {
    char desc[100];
    description(name, width, height, max_iteration, threads, desc);
    std::println(stderr, "name={} {} repeat={}/{} duration={:.2f}", name, desc, r + 1, repeat, time);
}

void report(const char* name, int width, int height, int max_iteration, int threads, int repeat, const std::vector<double>& times) {
    int r;
    double avg, stdev, min, max, mean;
    char desc[100];
    FILE* f = nullptr;

    description(name, width, height, max_iteration, threads, desc);

    avg = 0;
    min = times[0];
    max = times[0];
    for (r = 0; r < repeat; r++) {
        avg += times[r];
        min = std::min(min, times[r]);
        max = std::max(max, times[r]);
    }
    avg /= repeat;

    stdev = 0;
    for (r = 0; r < repeat; r++) {
        stdev += (times[r] - avg) * (times[r] - avg);
    }
    stdev = sqrt(stdev / repeat);

    mean = times[repeat / 2];

    std::string rep =
        std::format("name={} {} min={:.2f} max={:.2f} mean={:.2f} avg={:.2f} stdev={:.2f}", name, desc, min, max, mean, avg, stdev);

    f = fopen(REPORT, "a");
    fputs(rep.c_str(), f);
    fclose(f);
    std::println(stderr, "{}", rep);
}
