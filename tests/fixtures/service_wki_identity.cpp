// Disposable WOS cluster: exercise strict remote exec and expose its exact
// errno. Receivers need explicit peer_identity policy; see wki_security.md.
#include <errno.h>
#include <grp.h>
#include <stdio.h>
#include <string.h>
#include <sys/process.h>
#include <unistd.h>

auto main(int argc, char** argv) -> int {
    if (argc < 3 || geteuid() != 0) {
        fprintf(stderr, "usage (root): service_wki_identity <host> <command> [args...] | <host> --fragment\n");
        return 2;
    }
    // The legacy wire format cannot encode supplementary groups. Exercise
    // receiver trust using a representable root snapshot, not sender rejection.
    if (setgroups(0, nullptr) != 0) {
        perror("setgroups");
        return 2;
    }
    auto const RESULT = ker::process::setwkitarget(argv[1], strlen(argv[1]), ker::process::WKI_TARGET_FLAG_STRICT);
    if (RESULT != 0) {
        fprintf(stderr, "target setup failed: %lld\n", static_cast<long long>(RESULT));
        return 2;
    }
    if (argc == 3 && strcmp(argv[2], "--fragment") == 0) {
        // Larger than one WKI Ethernet payload, with a visible receiver result.
        char padding[16385];
        memset(padding, 'x', sizeof(padding) - 1);
        padding[sizeof(padding) - 1] = '\0';
        char shell[] = "/usr/bin/sh";
        char option[] = "-c";
        char command[] = "/usr/bin/uname -n";
        char name[] = "identity-fragment";
        char* arguments[] = {shell, option, command, name, padding, nullptr};
        execv(shell, arguments);
    } else {
        execv(argv[2], argv + 2);
    }
    int const ERROR = errno;
    printf("remote exec rejected: errno=%d (%s)\n", ERROR, strerror(ERROR));
    return ERROR;
}
