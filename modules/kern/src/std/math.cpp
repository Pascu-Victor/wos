int abs(int x) { return x & 0x7FFFFFFF; }

int max(int x, int y) { return x > y ? x : y; }

int min(int x, int y) { return x < y ? x : y; }

int pow(int x, int y) {
    int res = 1;
    for (int i = 0; i < y; i++) {
        res *= x;
    }
    return res;
}
