/* Owned disposable-VM processes only. Checks terminal allocation authority. */
#include <errno.h>
#include <fcntl.h>
#include <grp.h>
#include <pty.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/wait.h>
#include <unistd.h>

static volatile sig_atomic_t received;
static void observe(int signal) {
    (void)signal;
    received++;
}
static int transfer(int fd, void* data, size_t length, int writing) {
    char* cursor = data;
    while (length) {
        ssize_t n = writing ? write(fd, cursor, length) : read(fd, cursor, length);
        if (n < 0 && errno == EINTR) continue;
        if (n <= 0) return -1;
        cursor += n;
        length -= (size_t)n;
    }
    return 0;
}
static int probe(int command, int response, int expected) {
    char byte = 'p';
    int count = -1;
    return transfer(command, &byte, 1, 1) || transfer(response, &count, sizeof(count), 0) || count != expected;
}
static int install_handlers(void) {
    struct sigaction action = {0};
    action.sa_handler = observe;
    sigemptyset(&action.sa_mask);
    return sigaction(SIGINT, &action, NULL) || sigaction(SIGQUIT, &action, NULL) || sigaction(SIGTSTP, &action, NULL);
}
static int alias_number(int expected) {
    int fd = open("/dev/tty", O_RDWR), number = -1;
    if (fd < 0) return 1;
    int failed = ioctl(fd, TIOCGPTN, &number) || number != expected;
    close(fd);
    return failed;
}
int main(int argc, char** argv) {
    // This allocation test retires its own masters; lifetime tests observe HUP.
    if (signal(SIGHUP, SIG_IGN) == SIG_ERR) return 2;
    if (argc == 3 && !strcmp(argv[1], "--alias")) return alias_number(atoi(argv[2]));
    int command[2], response[2], victim_master = -1, victim_slave = -1;
    int master = -1, slave = -1, result = 1, status = 0;
    pid_t victim = -1;
    if (install_handlers() || pipe(command) || pipe(response) || openpty(&victim_master, &victim_slave, NULL, NULL, NULL)) return 2;
    victim = fork();
    if (victim < 0) return 3;
    if (victim == 0) {
        close(command[1]);
        close(response[0]);
        close(victim_master);
        int count = setsid() < 0 || ioctl(victim_slave, TIOCSCTTY, 0) ? -1 : 0;
        if (transfer(response[1], &count, sizeof(count), 1) || count < 0) _exit(4);
        char byte;
        while (!transfer(command[0], &byte, 1, 0) && byte != 'q') {
            usleep(50000);
            count = received;
            if (transfer(response[1], &count, sizeof(count), 1)) _exit(5);
        }
        _exit(0);
    }
    close(command[0]);
    close(response[1]);
    close(victim_slave);
    victim_slave = -1;
    int ready = -1;
    if (transfer(response[0], &ready, sizeof(ready), 0) || ready) goto cleanup;
    if (getsid(0) != getpid() && setsid() < 0) goto cleanup;
    if (argc == 2 && !strcmp(argv[1], "--drop")) {
        if (setgroups(0, NULL) || setgid(104) || setuid(104) || getuid() != 104 || geteuid() != 104) goto cleanup;
        errno = 0;
        if (kill(victim, 0) != -1 || errno != EPERM) goto cleanup;
        puts("PASS UID104 has no ordinary signal authority over root victim");
    }
    if (openpty(&master, &slave, NULL, NULL, NULL) || ioctl(slave, TIOCSCTTY, 0)) goto cleanup;
    int number = -1;
    if (ioctl(slave, TIOCGPTN, &number) || alias_number(number)) goto cleanup;

    /* Current admission accepts this number; delivery must still stay inside
       the originating terminal. Future admission may reject it with EPERM. */
    errno = 0;
    if (tcsetpgrp(slave, victim) && errno != EPERM) goto cleanup;
    char controls[] = {3, 28, 26};
    for (size_t i = 0; i < sizeof(controls); ++i) {
        if (transfer(master, &controls[i], 1, 1) || probe(command[1], response[0], 0)) goto cleanup;
    }
    puts("PASS foreign foreground group receives no INT/QUIT/TSTP");
    /* Positive controls prove that handlers and terminal input actually work. */
    for (size_t i = 0; i < sizeof(controls); ++i) {
        if (transfer(victim_master, &controls[i], 1, 1) || probe(command[1], response[0], (int)i + 1)) goto cleanup;
    }
    puts("PASS owned foreground group receives INT/QUIT/TSTP");

    close(slave);
    slave = -1;
    close(master);
    master = -1;
    if (openpty(&master, &slave, NULL, NULL, NULL)) goto cleanup;
    int replacement = -1;
    if (ioctl(slave, TIOCGPTN, &replacement) || replacement != number) goto cleanup;
    errno = 0;
    int alias = open("/dev/tty", O_RDWR);
    if (alias >= 0) {
        close(alias);
        goto cleanup;
    }
    if (errno != ENXIO) goto cleanup;
    puts("PASS reused PTY slot does not reopen through stale /dev/tty");
    if (ioctl(slave, TIOCSCTTY, 0) || alias_number(number)) goto cleanup;
    pid_t child = fork();
    if (child < 0) goto cleanup;
    if (child == 0) {
        char text[24];
        snprintf(text, sizeof(text), "%d", number);
        execl(argv[0], argv[0], "--alias", text, (char*)NULL);
        _exit(6);
    }
    if (waitpid(child, &status, 0) != child || !WIFEXITED(status) || WEXITSTATUS(status)) goto cleanup;
    puts("PASS fresh controlling terminal survives fork and exec");
    result = 0;
cleanup:
    if (result) fprintf(stderr, "terminal identity probe failed: errno=%d\n", errno);
    char quit = 'q';
    (void)transfer(command[1], &quit, 1, 1);
    if (waitpid(victim, &status, 0) != victim || !WIFEXITED(status) || WEXITSTATUS(status)) result = 1;
    if (master >= 0) close(master);
    if (slave >= 0) close(slave);
    close(victim_master);
    close(command[1]);
    close(response[0]);
    return result;
}
