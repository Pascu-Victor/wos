#!/usr/bin/env python3
"""Private live KTEST fixture for suppressed virtio IRQ schedule publication.

Build the diagnostic kernel first. This runner packages a private boot disk,
verifies its payload, and uses the shared KTEST node/socket backend. The caller
must stop other WOS VMs first. It never changes the normal topology or images.
"""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / 'ktest-data/napi-irq'
CMDLINE = '--napi-irq-test net.napi_diag'
MAGIC = b'\x88\xb5WOSIRQ49'


def digest(path):
    with path.open('rb') as stream:
        return hashlib.file_digest(stream, 'sha256').hexdigest()


def save(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')


def frame(cookie, reply=False):
    return (b'\xff' * 6 + bytes([2, 0, 0, 0x49, 0, 1]) + MAGIC + bytes([2 if reply else 1, 0]) +
            cookie.to_bytes(8, 'little') + bytes((i * 17 + cookie) & 255 for i in range(32, 64)))


def prepare_config(run):
    config = json.loads((ROOT / 'configs/node_ktest.json').read_text())
    # Preserve all isolated build roots; use shared node layout for the VM.
    config['package'] = {'kernel_cmdline': CMDLINE}
    config['node'] = {
        'id': 0, 'hostname': 'wos-napi-irq', 'debug': False, 'mtu': 9000,
        'vm': {'memory': '8G', 'cpus': 4, 'disk0': str(run / 'boot.qcow2'),
               'disk1': str(ROOT / 'ktest-data/mountfs.qcow2'), 'overlay_dir': str(run / 'overlays'),
               'serial_log': str(run / 'serial.log'), 'qemu_log': str(run / 'qemu.log'),
               'usb_hotplug': {'enabled': True, 'controller_id': 'xhci', 'usb2_ports': 4, 'usb3_ports': 4,
                               'qmp_socket': str(run / 'qmp.sock')}, 'iommu': True},
        'nics': [{'zone_id': 0, 'name': 'napi-irq-private', 'model': 'virtio-net-pci,mq=on',
                  'queues': 1, 'vhost': False, 'driver': 'unmanaged',
                  'qemu_netdev': {'type': 'socket-mcast', 'address': '239.192.49.1',
                                  'port': 49000 + int.from_bytes(os.urandom(2), 'little') % 10000,
                                  'localaddr': '127.0.0.1'}}],
    }
    save(run / 'node.json', config)
    return config


def package(run):
    env = dict(os.environ, WOS_BUILD_DIR=str(ROOT / 'build-ktest'),
               WOS_SYSROOT_PATH=str(ROOT / 'ktest-data/sysroot'),
               WOS_BUSYBOX_INSTALL_DIR=str(ROOT / 'ktest-data/busybox-install'),
               WOS_BOOT_DISK=str(run / 'boot.qcow2'), WOS_ROOTFS_DISK=str(ROOT / 'ktest-data/mountfs.qcow2'),
               WOS_BOOT_IMAGE_MANIFEST=str(run / 'boot.manifest'), WOS_KERNEL_CMDLINE=CMDLINE,
               WOS_SYNC_ROOTFS_ETC_TABLES='0')
    with (run / 'package.log').open('w') as log:
        subprocess.run([str(ROOT / 'scripts/build/make_image.sh')], cwd=ROOT, env=env,
                       stdout=log, stderr=subprocess.STDOUT, check=True)


def verify_package(run):
    # Keep extracted qualification payloads; discard only this runner's raw temp.
    with tempfile.TemporaryDirectory(prefix='extract-', dir=run) as tmp:
        raw = Path(tmp) / 'boot.raw'
        subprocess.run(['qemu-img', 'convert', '-f', 'qcow2', '-O', 'raw', str(run / 'boot.qcow2'), str(raw)], check=True)
        extracted = {}
        for remote, local in [('wos', 'packaged-wos'), ('limine/limine.conf', 'packaged-limine.conf'),
                              ('EFI/BOOT/BOOTX64.EFI', 'packaged-BOOTX64.EFI'), ('initramfs.cpio', 'packaged-initramfs.cpio')]:
            target = run / local
            subprocess.run(['mcopy', '-o', '-i', f'{raw}@@1048576', f'::/{remote}', str(target)], check=True)
            extracted[remote] = digest(target)
    kernel = digest(ROOT / 'build-ktest/modules/kern/wos')
    assert kernel == extracted['wos'], 'packaged kernel differs from current diagnostic build'
    limine = (run / 'packaged-limine.conf').read_text()
    assert re.search(r'^\s*cmdline: ' + re.escape(CMDLINE) + r'\s*$', limine, re.M), limine
    proof = {'kernel_sha256': kernel, 'boot_sha256': digest(run / 'boot.qcow2'),
             'cmdline': CMDLINE, 'extracted': extracted}
    save(run / 'package-proof.json', proof)
    return proof


def open_backend(backend):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    try:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((backend['address'], backend['port']))
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP,
                        socket.inet_aton(backend['address']) + socket.inet_aton('127.0.0.1'))
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton('127.0.0.1'))
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 0)
        sock.settimeout(0.01)
        return sock
    except BaseException:
        sock.close()
        raise


def quit_owned(path):
    # This private QMP socket belongs only to the fixture launched below.
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as conn:
        conn.settimeout(10)
        conn.connect(str(path))
        with conn.makefile('rwb', buffering=0) as stream:
            json.loads(stream.readline())
            for command in ('qmp_capabilities', 'quit'):
                stream.write((json.dumps({'execute': command}) + '\n').encode())
                while True:
                    result = json.loads(stream.readline())
                    if 'error' in result:
                        raise RuntimeError(result)
                    if 'return' in result:
                        break


def check_kernel(text):
    rows = []
    for line in text.splitlines():
        if 'napi irq CASE ' in line:
            fields = dict(re.findall(r'(\w+)=([\w/]+)', line))
            rows.append(fields)
    assert [int(row['cookie']) for row in rows] == list(range(1, 17)), rows
    for row in rows:
        cookie = int(row['cookie'])
        fault = 5 <= cookie <= 12
        assert int(row['fault']) == fault and row['ok'] == '1', row
        assert all(row[key] == '0' for key in ('bad', 'duplicate', 'foreign')), row
        if fault:
            assert row['source'] == '2' and row['suppressed'] == '1', row
            assert len(row['idx'].split('/')) == 2 and len(set(row['idx'].split('/'))) == 1, row
            assert len(row['desc'].split('/')) == 2 and len(set(row['desc'].split('/'))) == 1, row
            assert row['len'] in ('74', '76') and 0 <= int(row['dt_us']) <= 150000, row
            assert row['schedule'] == '0/0/0/0', row
        else:
            assert row['suppressed'] == '0', row
    summaries = [line for line in text.splitlines() if 'napi irq recovery PASS ' in line]
    assert len(summaries) == 1, summaries
    summary = dict(re.findall(r'(\w+)=([\w/]+)', summaries[0]))
    assert summary['cases'] == '16' and summary['suppressed'] == '8' and summary['bound_us'] == '150000', summary
    assert int(summary['max_dt_us']) <= 150000, summary
    assert all(int(count) > 0 for key in ('ordinary_irq', 'ordinary_worker_rx') for count in summary[key].split('/')), summary
    assert 'system shutdown finalizer starting action=2' in text, 'missing ordinary shutdown finalizer'
    assert not re.search(r'KASAN:|KUBSAN:|Kernel panic|PANIC', text), 'kernel diagnostic failure'
    return {'rows': rows, 'summary': summary}


def launch(run, config, proof):
    serial = run / 'serial.log'
    assert not serial.exists(), 'reuse would mix runtime evidence; prepare a new run directory'
    backend = config['node']['nics'][0]['qemu_netdev']
    sock = open_backend(backend)
    requests, replies, ignored_replies, foreign, errors = [], [], 0, 0, []
    armed, pending, frames, arm_times = set(), {}, [], {}
    timed_out, process = False, None
    deadline = time.monotonic() + 180
    try:
        with (run / 'launch.log').open('w') as log:
            process = subprocess.Popen([str(ROOT / 'bin/wos-ktest'), '--config', str(run / 'node.json'),
                                        '--no-build', '--no-package', '--no-setup'], cwd=ROOT,
                                       stdout=log, stderr=subprocess.STDOUT)
            while process.poll() is None:
                if time.monotonic() >= deadline:
                    timed_out = True
                    break
                text = serial.read_text(errors='replace') if serial.exists() else ''
                for value in re.findall(r'napi irq ARM cookie=(\d+)\b', text):
                    cookie = int(value)
                    if cookie not in armed:
                        arm_times[cookie] = time.monotonic_ns()
                        armed.add(cookie)
                try:
                    data, _ = sock.recvfrom(65536)
                except socket.timeout:
                    data = None
                if data is not None:
                    if len(data) < 22 or data[12:22] != MAGIC:
                        foreign += 1
                    elif len(data) == 64 and data[22] == 2:
                        cookie = int.from_bytes(data[24:32], 'little')
                        if not 1 <= cookie <= 16 or data != frame(cookie, True):
                            errors.append({'reason': 'bad reflected reply', 'hex': data.hex()})
                        ignored_replies += 1  # Host multicast loopback, never re-echo.
                    else:
                        cookie = int.from_bytes(data[24:32], 'little') if len(data) >= 32 else 0
                        if data != frame(cookie) or not 1 <= cookie <= 16 or cookie in requests:
                            errors.append({'reason': 'bad/duplicate request', 'cookie': cookie, 'bytes': len(data), 'hex': data.hex()})
                        else:
                            requests.append(cookie)
                            frames.append({'cookie': cookie, 'received_ns': time.monotonic_ns(),
                                           'request_sha256': hashlib.sha256(data).hexdigest(), 'request_hex': data.hex()})
                            pending[cookie] = data
                for cookie in list(pending):
                    if not 5 <= cookie <= 12 or cookie in armed:
                        sent = sock.sendto(frame(cookie, True), (backend['address'], backend['port']))
                        if sent != 64:
                            errors.append({'reason': 'short send', 'cookie': cookie, 'bytes': sent})
                        replies.append(cookie)
                        next(item for item in frames if item['cookie'] == cookie)['sent_ns'] = time.monotonic_ns()
                        del pending[cookie]
            if process.poll() is None:
                quit_owned(run / 'qmp.sock')
                process.wait(timeout=30)
    finally:
        sock.close()
        if process is not None and process.poll() is None:
            # Exception cleanup is retained as failure, never qualification.
            try:
                quit_owned(run / 'qmp.sock')
            finally:
                process.wait(timeout=30)
        save(run / 'backend-receipt.json', {'requests': requests, 'replies': replies, 'armed': sorted(armed),
                                          'pending': sorted(pending), 'frames': frames, 'arm_observed_ns': arm_times,
                                          'ignored_reply_loopbacks': ignored_replies,
                                          'foreign_frames': foreign, 'errors': errors, 'timeout': timed_out,
                                          'launcher_exit': process.returncode if process is not None else None})
    text = serial.read_text(errors='replace')
    save(run / 'runtime-hashes.json', {name: digest(run / name) for name in ('serial.log', 'qemu.log', 'launch.log', 'backend-receipt.json')})
    assert not timed_out and process.returncode == 0, (timed_out, process.returncode)
    assert not errors and requests == replies == list(range(1, 17)), (errors, requests, replies)
    assert armed == set(range(5, 13)) and not pending, (armed, pending)
    result = check_kernel(text)
    result.update({'package': proof, 'launcher_exit': process.returncode, 'serial_sha256': digest(serial),
                   'scope': 'software IRQ schedule publication loss on a real single-pair virtio NIC'})
    save(run / 'validation.json', result)
    print(json.dumps({'result': 'PASS', 'run': str(run), 'kernel': proof['kernel_sha256'], 'summary': result['summary']}, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--prepare-only', action='store_true', help='Package and verify, without launching')
    parser.add_argument('--no-package', action='store_true', help='Verify and launch an already prepared fresh directory')
    parser.add_argument('--run-dir', type=Path, help='Private directory below ktest-data/napi-irq (default: unique new directory)')
    args = parser.parse_args()
    STATE.mkdir(parents=True, exist_ok=True)
    run = args.run_dir.resolve() if args.run_dir else Path(tempfile.mkdtemp(prefix='run-', dir=STATE))
    if run == STATE or not run.is_relative_to(STATE):
        parser.error('--run-dir must be below ktest-data/napi-irq')
    run.mkdir(parents=True, exist_ok=True)
    print(f'Artifacts: {run}', flush=True)
    if args.no_package:
        config = json.loads((run / 'node.json').read_text())
    else:
        assert not (run / 'node.json').exists(), 'use a fresh run directory'
        config = prepare_config(run)
        package(run)
    proof = verify_package(run)
    if not args.prepare_only:
        launch(run, config, proof)


if __name__ == '__main__':
    main()
