// Functional batch credentials, per-item effects and preflight checks in disposable WOS VMs.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <sys/wait.h>
#include <unistd.h>

#include <initializer_list>
namespace {
namespace abi = ker::abi::vfs;
char base[512];
constexpr unsigned BATCHES = 192;
unsigned uid_for(unsigned n) { return (n / 16) % 4 == 0 ? 0 : 1000 + (n / 16) % 4; }
void path(char* out, const char* part) { snprintf(out, 512, "%s/%s", base, part); }
void names(unsigned n, char src[3][512], char dst[3][512], bool raw = false) {
    snprintf(src[0], 512, "%s/%sleft/a%u", base, raw ? "private/../" : "", n);
    snprintf(dst[0], 512, "%s/%sright/b%u", base, raw ? "private/../" : "", n);
    snprintf(src[1], 512, "%s/public/a%u", base, n);
    snprintf(dst[1], 512, "%s/public/b%u", base, n);
    snprintf(src[2], 512, "%s/public/c%u", base, n);
    snprintf(dst[2], 512, "%s/public/d%u", base, n);
}
bool create(const char* name, unsigned uid) {
    int f = open(name, O_CREAT | O_EXCL | O_WRONLY, 0600);
    bool ok = f >= 0 && write(f, "old", 3) == 3;
    if (f >= 0) close(f);
    return chown(name, uid, 2001) == 0 && ok;
}
bool same_time(timespec a, timespec b) { return a.tv_sec == b.tv_sec && a.tv_nsec == b.tv_nsec; }
bool unchanged(const struct stat& a, const struct stat& b) {
    return a.st_ino == b.st_ino && a.st_nlink == b.st_nlink && a.st_mode == b.st_mode && a.st_uid == b.st_uid && a.st_gid == b.st_gid &&
           a.st_size == b.st_size && same_time(a.st_mtim, b.st_mtim) && same_time(a.st_ctim, b.st_ctim);
}
bool absent(const char* name) {
    struct stat s{};
    errno = 0;
    return lstat(name, &s) == -1 && errno == ENOENT;
}
abi::metadata_batch_operation operation(unsigned op) {
    return op == 0   ? abi::metadata_batch_operation::CREATE_CLOSE
           : op == 1 ? abi::metadata_batch_operation::UNLINK
                     : abi::metadata_batch_operation::RENAME;
}
int setup() {
    char left[512], right[512], pub[512], priv[512], src[3][512], dst[3][512];
    path(left, "left");
    path(right, "right");
    path(pub, "public");
    path(priv, "private");
    bool ok = mkdir(base, 0755) == 0 && mkdir(left, 0770) == 0 && mkdir(right, 0770) == 0 && mkdir(pub, 0777) == 0 &&
              mkdir(priv, 0710) == 0 && chown(left, 1001, 2002) == 0 && chown(right, 1001, 2002) == 0 && chown(priv, 0, 2000) == 0;
    for (unsigned n = 0; n < BATCHES && ok; ++n) {
        names(n, src, dst);
        unsigned op = n / 64;
        if (op == 0)
            ok = create(src[2], 1003);
        else {
            ok = create(src[0], 1002) && create(src[1], uid_for(n));
            if (op == 2) ok = create(dst[0], 1003) && create(dst[1], 1003) && ok;
        }
    }
    printf("%s setup\n", ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}
int cleanup() {
    bool ok = true;
    char src[3][512], dst[3][512], p[512];
    for (unsigned n = 0; n < BATCHES; ++n) {
        names(n, src, dst);
        for (unsigned i = 0; i < 3; ++i)
            for (auto name : {src[i], dst[i]})
                if (unlink(name) && errno != ENOENT) ok = false;
    }
    for (unsigned i = 0; i < 64; ++i) {
        snprintf(p, 512, "%s/public/bulk%u", base, i);
        if (unlink(p) && errno != ENOENT) ok = false;
        snprintf(p, 512, "%s/public/bound%u", base, i);
        if (unlink(p) && errno != ENOENT) ok = false;
    }
    for (auto part : {"left", "right", "public", "private"}) {
        path(p, part);
        if (rmdir(p) && errno != ENOENT) ok = false;
    }
    if (rmdir(base) && errno != ENOENT) ok = false;
    printf("%s cleanup\n", ok ? "PASS" : "FAIL");
    return ok ? 0 : 1;
}
int expected_first(unsigned n) {
    unsigned op = n / 64, uid = uid_for(n), group = (n / 8) % 2, sticky = (n / 2) % 4;
    bool raw = n % 2;
    if (uid != 0 && ((uid != 1001 && !group) || (raw && !group))) return EACCES;
    if (op != 0 && (sticky & 1) && uid != 0 && uid != 1001 && uid != 1002) return EPERM;
    if (op == 2 && (sticky & 2) && uid != 0 && uid != 1001 && uid != 1003) return EPERM;
    return 0;
}
int check() {
    unsigned failed = 0;
    char src[3][512], dst[3][512], rawsrc[3][512], rawdst[3][512], left[512], right[512];
    path(left, "left");
    path(right, "right");
    for (unsigned n = 0; n < BATCHES; ++n) {
        unsigned op = n / 64, uid = uid_for(n), group = (n / 8) % 2, sticky = (n / 2) % 4;
        names(n, src, dst);
        names(n, rawsrc, rawdst, n % 2);
        if (chmod(left, 02770 | ((sticky & 1) ? 01000 : 0)) || chmod(right, 0770 | ((sticky & 2) ? 01000 : 0))) return 90;
        struct stat before[3]{}, displaced[3]{}, pa{}, pb{};
        if (stat(left, &pa) || stat(right, &pb)) return 91;
        for (unsigned i = 0; i < 3; ++i) {
            bool exists = op == 0 ? i == 2 : i < 2;
            if (exists && lstat(src[i], &before[i])) return 92;
            if (op == 2 && i < 2 && lstat(dst[i], &displaced[i])) return 93;
        }
        int expected[3] = {expected_first(n), 0, op == 0 ? EEXIST : ENOENT};
        pid_t child = fork();
        if (child == 0) {
            gid_t groups[] = {2000, 2002};
            if (setgroups(group ? 2 : 0, group ? groups : nullptr) || setgid(2001) || setuid(uid)) _exit(94);
            umask(0037);
            abi::metadata_batch_entry entries[3]{};
            abi::metadata_batch_result results[3]{};
            for (unsigned i = 0; i < 3; ++i) entries[i] = {rawsrc[i], op == 2 ? rawdst[i] : nullptr};
            abi::metadata_batch_header header{
                .version = abi::METADATA_BATCH_VERSION, .operation = operation(op), .count = 3, .mode = op == 0 ? 0666U : 0U};
            int r = abi::metadata_batch(&header, entries, results);
            bool ok = r == 0;
            for (unsigned i = 0; i < 3; ++i) ok = results[i].status == -expected[i] && ok;
            if (!ok && failed < 12)
                printf("batch=%u uid=%u group=%u sticky=%u raw=%u call=%d results=%d,%d,%d expected=%d,%d,%d\n", n, uid, group, sticky,
                       n % 2, r, results[0].status, results[1].status, results[2].status, -expected[0], -expected[1], -expected[2]);
            _exit(ok ? 0 : 1);
        }
        int status = 0;
        bool batch_ok = child > 0 && waitpid(child, &status, 0) == child && WIFEXITED(status) && WEXITSTATUS(status) == 0;
        for (unsigned i = 0; i < 3; ++i) {
            struct stat after{}, target{};
            bool ok = batch_ok;
            if (expected[i]) {
                bool existed = op == 0 ? i == 2 : i < 2;
                ok = (existed ? lstat(src[i], &after) == 0 && unchanged(before[i], after) : absent(src[i])) && ok;
                if (op == 2) ok = (i < 2 ? lstat(dst[i], &target) == 0 && unchanged(displaced[i], target) : absent(dst[i])) && ok;
                if (i == 0) {
                    struct stat paa{}, pbb{};
                    ok = stat(left, &paa) == 0 && stat(right, &pbb) == 0 && unchanged(pa, paa) && unchanged(pb, pbb) && ok;
                }
            } else if (op == 0)
                ok = lstat(src[i], &after) == 0 && S_ISREG(after.st_mode) && after.st_uid == uid &&
                     after.st_gid == (i == 0 ? 2002 : 2001) && (after.st_mode & 0777) == 0640 && after.st_size == 0 && ok;
            else if (op == 1)
                ok = absent(src[i]) && ok;
            else
                ok = absent(src[i]) && lstat(dst[i], &target) == 0 && target.st_ino == before[i].st_ino &&
                     target.st_uid == before[i].st_uid && target.st_gid == before[i].st_gid && ok;
            if (!ok) {
                if (failed < 12) printf("FAIL item batch=%u item=%u child=%d\n", n, i, status);
                ++failed;
            }
        }
    }
    printf("%s batch checks=576 failed=%u\n", failed ? "FAIL" : "PASS", failed);
    return failed ? 1 : 0;
}
int bounds() {
    char paths[65][512], pub[512], longpart[181];
    memset(longpart, 'q', 180);
    longpart[180] = 0;
    path(pub, "public");
    abi::metadata_batch_entry entries[65]{};
    abi::metadata_batch_result results[65]{};
    abi::metadata_batch_header header{
        .version = abi::METADATA_BATCH_VERSION, .operation = abi::metadata_batch_operation::CREATE_CLOSE, .count = 64, .mode = 0600};
    for (unsigned i = 0; i < 64; ++i) {
        snprintf(paths[i], 512, "%s/public/bulk%u", base, i);
        entries[i] = {paths[i], nullptr};
    }
    int r = abi::metadata_batch(&header, entries, results);
    bool ok = r == 0;
    for (unsigned i = 0; i < 64; ++i) {
        struct stat s{};
        ok = results[i].status == 0 && stat(paths[i], &s) == 0 && s.st_uid == 0 && s.st_gid == 0 && (s.st_mode & 0777) == 0600 &&
             s.st_size == 0 && ok;
    }
    struct stat before{}, after{};
    if (stat(pub, &before)) return 95;
    for (unsigned i = 0; i < 65; ++i) {
        snprintf(paths[i], 512, "%s/public/bound%u", base, i);
        entries[i] = {paths[i], nullptr};
        results[i].status = 123456;
    }
    header.count = 65;
    r = abi::metadata_batch(&header, entries, results);
    ok = r == -EINVAL && results[0].status == 123456 && ok;
    header.count = 64;
    entries[63].path = "";
    r = abi::metadata_batch(&header, entries, results);
    ok = r == -ENOENT && results[0].status == 123456 && ok;
    for (unsigned i = 0; i < 64; ++i) {
        snprintf(paths[i], 512, "%s/public/%s/bound%u", base, longpart, i);
        entries[i].path = paths[i];
    }
    r = abi::metadata_batch(&header, entries, results);
    ok = r == -EOPNOTSUPP && ok;
    for (unsigned i = 0; i < 64; ++i) ok = results[i].status == -EINPROGRESS && ok;
    ok = stat(pub, &after) == 0 && unchanged(before, after) && ok;
    for (unsigned i = 0; i < 64; ++i) {
        snprintf(paths[i], 512, "%s/public/bound%u", base, i);
        ok = absent(paths[i]) && ok;
    }
    printf("%s bounds checks=67 last=%d\n", ok ? "PASS" : "FAIL", r);
    return ok ? 0 : 1;
}
int unsupported() {
    char src[3][512], dst[3][512], left[512], right[512];
    path(left, "left");
    path(right, "right");
    bool ok = true;
    for (unsigned op = 0; op < 3; ++op) {
        names(op * 64, src, dst);
        struct stat a{}, b{}, pa{}, pb{}, aa{}, bb{}, paa{}, pbb{};
        if (stat(left, &pa) || stat(right, &pb) || (op && lstat(src[0], &a)) || (op == 2 && lstat(dst[0], &b))) return 90;
        abi::metadata_batch_header header{
            .version = abi::METADATA_BATCH_VERSION, .operation = operation(op), .count = 1, .mode = op == 0 ? 0600U : 0U};
        abi::metadata_batch_entry entry{src[0], op == 2 ? dst[0] : nullptr};
        abi::metadata_batch_result result{};
        int r = abi::metadata_batch(&header, &entry, &result);
        bool unchanged_state = (op ? lstat(src[0], &aa) == 0 && unchanged(a, aa) : absent(src[0])) && stat(left, &paa) == 0 &&
                               stat(right, &pbb) == 0 && unchanged(pa, paa) && unchanged(pb, pbb);
        if (op == 2) unchanged_state = lstat(dst[0], &bb) == 0 && unchanged(b, bb) && unchanged_state;
        bool pass = r == -EOPNOTSUPP && result.status == -EINPROGRESS && unchanged_state;
        ok = pass && ok;
        printf("%s unsupported operation=%u call=%d item=%d unchanged=%d\n", pass ? "PASS" : "FAIL", op, r, result.status, unchanged_state);
    }
    return ok ? 0 : 1;
}
}  // namespace
int main(int argc, char** argv) {
    if (argc != 3 || strlen(argv[2]) >= sizeof(base)) return 2;
    strcpy(base, argv[2]);
    setvbuf(stdout, nullptr, _IOLBF, 0);
    umask(0);
    if ((!strcmp(argv[1], "setup") || !strcmp(argv[1], "cleanup")) && abi::wki_rule_add_vfs("/", abi::WKI_VFS_ROUTE_LOCAL)) return 3;
    if (!strcmp(argv[1], "setup")) return setup();
    if (!strcmp(argv[1], "cleanup")) return cleanup();
    if (!strcmp(argv[1], "check")) return check();
    if (!strcmp(argv[1], "bounds")) return bounds();
    if (!strcmp(argv[1], "unsupported")) return unsupported();
    return 2;
}
