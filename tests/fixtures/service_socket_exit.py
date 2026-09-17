#!/usr/bin/env python3
"""Run as root inside a disposable WOS VM: python3 service_socket_exit.py.

A listener must release its binding before waitpid reports process death, even
when a fatal signal arrives during an accept syscall that retains the File.
"""
import json
import os
import signal
import socket
import time


def wait_for_accept(pid):
    for _ in range(500):
        with open('/proc/' + str(pid) + '/status') as status:
            rows = dict(line.split(':', 1) for line in status if ':' in line)
        if rows.get('Wchan', '').strip() == 'tcp_wait':
            return
        time.sleep(.01)
    raise AssertionError('child never parked in tcp_wait')


def wait_for_exit(pid):
    for _ in range(500):
        found, status = os.waitpid(pid, os.WNOHANG)
        if found:
            return status
        time.sleep(.01)
    raise AssertionError('child did not exit within five seconds')


def run_case(mode):
    reader, writer = os.pipe()
    pid = os.fork()
    if pid == 0:
        try:
            os.close(reader)
            signal.alarm(10)
            os.setgid(100)
            os.setuid(100)
            listener = socket.socket()
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(('0.0.0.0', 0))
            listener.listen(4)
            os.write(writer, str(listener.getsockname()[1]).encode())
            os.close(writer)
            if mode == 'close':
                listener.close()
                os._exit(0)
            if mode == 'exit':
                os._exit(0)
            if mode == 'term-idle':
                while True:
                    time.sleep(1)
            listener.accept()
            os._exit(2)
        except BaseException as error:
            print('child failure', repr(error), flush=True)
            os._exit(3)
    os.close(writer)
    port = int(os.read(reader, 32))
    os.close(reader)
    if mode not in ('close', 'exit'):
        if mode.endswith('accept'):
            wait_for_accept(pid)
        os.kill(pid, signal.SIGKILL if mode == 'kill-accept' else signal.SIGTERM)
    status = wait_for_exit(pid)
    expected_signal = signal.SIGKILL if mode == 'kill-accept' else signal.SIGTERM
    assert (os.WIFEXITED(status) and os.WEXITSTATUS(status) == 0) if mode in ('close', 'exit') else (
        os.WIFSIGNALED(status) and os.WTERMSIG(status) == expected_signal)
    with socket.socket() as probe:
        probe.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            probe.bind(('0.0.0.0', port))
            error = 0
        except OSError as exc:
            error = exc.errno
    row = {'mode': mode, 'pid': pid, 'port': port, 'wait': status, 'rebind_errno': error}
    print(json.dumps(row), flush=True)
    return error == 0


def main():
    if os.geteuid() != 0:
        raise SystemExit('run as root in a disposable WOS VM')
    results = [run_case(mode) for _ in range(3) for mode in
               ('close', 'exit', 'term-idle', 'term-accept', 'kill-accept')]
    print('socket exit cases:', len(results), 'failed:', results.count(False), flush=True)
    raise SystemExit(0 if all(results) else 1)


if __name__ == '__main__':
    main()
