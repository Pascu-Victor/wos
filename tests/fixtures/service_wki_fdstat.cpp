// Metadata must follow an admitted remote description through pathname changes.
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/process.h>
#include <sys/stat.h>
#include <unistd.h>

namespace {
struct Files {
    char original[1024]{};
    char moved[1024]{};
    int first = -1;
    int replacement = -1;
    bool original_created = false;
    bool moved_created = false;
    ~Files() {
        if (replacement >= 0) close(replacement);
        if (first >= 0) close(first);
        if (original_created) unlink(original);
        if (moved_created) unlink(moved);
    }
};
auto drop() -> bool {
    using Control = ker::abi::process::CredentialControl;
    gid_t groups[]{2000, 3000};
    return setgroups(2, groups) == 0 && ker::process::prctl(static_cast<int>(Control::SET_NO_NEW_PRIVILEGES), 1, 0, 0, 0) == 0 &&
           ker::process::prctl(static_cast<int>(Control::SET_SERVICE_PRIVILEGES), 0, 0, 0, 0) == 0 && setgid(2000) == 0 &&
           setuid(1000) == 0 && ker::process::prctl(static_cast<int>(Control::DROP_AUTHORITY_CEILING), 0, 0, 0, 0) == 0;
}
}  // namespace

auto main(int argc, char** argv) -> int {
    if (argc != 4 || geteuid() != 0) return 2;
    bool const BORROWED = strcmp(argv[2], "drop") == 0;
    bool const WRITE_ONLY = strcmp(argv[2], "writeonly") == 0;
    bool const CAP_OFF = strcmp(argv[3], "off") == 0;
    if (!BORROWED && !WRITE_ONLY && strcmp(argv[2], "owned") != 0) return 2;
    Files files;
    if (snprintf(files.original, sizeof(files.original), "%s/original", argv[1]) >= static_cast<int>(sizeof(files.original)) ||
        snprintf(files.moved, sizeof(files.moved), "%s/moved", argv[1]) >= static_cast<int>(sizeof(files.moved)))
        return 2;
    unsigned checks = 0;
    auto check = [&](bool condition, const char* name) {
        ++checks;
        if (!condition) printf("FAIL %s errno=%d\n", name, errno);
        return condition;
    };
    umask(0);
    if (!BORROWED && !check(drop(), "initial identity drop")) return 1;
    files.first = open(files.original, O_CREAT | O_EXCL | (WRITE_ONLY ? O_WRONLY : O_RDWR), 0600);
    if (!check(files.first >= 0, "open description")) return 1;
    files.original_created = true;
    if (!check(write(files.first, "retained", 8) == 8, "buffered write")) return 1;
    struct stat initial{};
    auto metadata = [&](struct stat& output) {
        if (!CAP_OFF) return check(fstat(files.first, &output) == 0, "descriptor stat");
        memset(&output, 0x5a, sizeof(output));
        struct stat const BEFORE = output;
        errno = 0;
        return check(fstat(files.first, &output) == -1 && errno == ENOTSUP && memcmp(&output, &BEFORE, sizeof(output)) == 0,
                     "capability denial preserves output");
    };
    if (!metadata(initial)) return 1;
    if (!CAP_OFF && !check(initial.st_uid == (BORROWED ? 0U : 1000U) && initial.st_gid == (BORROWED ? 0U : 2000U) && initial.st_size == 8 &&
                               (initial.st_mode & 0777) == 0600,
                           "metadata reflects grant and pending writes"))
        return 1;
    if (!check(lseek(files.first, 0, SEEK_CUR) == 8, "stat preserves position")) return 1;
    if (BORROWED) {
        if (!check(drop(), "drop after open")) return 1;
        int denied = open(files.original, O_RDONLY);
        int const ERROR = errno;
        if (denied >= 0) close(denied);
        if (!check(denied < 0 && ERROR == EACCES, "new open denied after drop")) return 1;
        if (!metadata(initial)) return 1;
    }
    if (!check(rename(files.original, files.moved) == 0, "rename path")) return 1;
    files.original_created = false;
    files.moved_created = true;
    files.replacement = open(files.original, O_CREAT | O_EXCL | O_RDWR, 0600);
    if (!check(files.replacement >= 0, "create replacement")) return 1;
    files.original_created = true;
    if (!check(write(files.replacement, "replacement", 11) == 11, "write replacement")) return 1;
    struct stat after{};
    if (!metadata(after)) return 1;
    if (!CAP_OFF &&
        !check(after.st_ino == initial.st_ino && after.st_dev == initial.st_dev && after.st_size == 8 && after.st_uid == initial.st_uid,
               "replacement cannot redirect descriptor stat"))
        return 1;
    if (!check(unlink(files.moved) == 0, "unlink retained file")) return 1;
    files.moved_created = false;
    if (!metadata(after)) return 1;
    if (!CAP_OFF && !check(after.st_ino == initial.st_ino && after.st_size == 8 && after.st_nlink == 0, "unlinked inode metadata survives"))
        return 1;
    if (!WRITE_ONLY) {
        char bytes[9]{};
        if (!check(
                lseek(files.first, 0, SEEK_SET) == 0 && read(files.first, bytes, sizeof(bytes)) == 8 && memcmp(bytes, "retained", 8) == 0,
                "retained contents survive"))
            return 1;
    }
    if (!check(close(files.first) == 0, "close retained file")) return 1;
    files.first = -1;
    if (!check(close(files.replacement) == 0, "close replacement")) return 1;
    files.replacement = -1;
    printf("PASS scenario=%s capability=%s checks=%u\n", argv[2], argv[3], checks);
    return 0;
}
