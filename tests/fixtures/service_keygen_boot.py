"""Disposable WOS VM boot orchestration; private-key bytes/digests stay in guest.
Run `python service_keygen_boot.py stage`, reboot, then `restore`, reboot,
then `verify`. Install the set-ID service_keygen_policy probe first.
"""
import hashlib
import os
import stat
import sys

BACKUP = "/root/goal19-keygen-backup"
DIRECTORY = "/etc/dropbear"
KEY = "dropbear_rsa_host_key"
MANIFEST = "/etc/wos-services.conf"


def digest(directory):
    with open(directory + "/" + KEY, "rb") as stream:
        return hashlib.sha256(stream.read()).digest()


def metadata(directory):
    state = os.lstat(directory)
    assert stat.S_ISDIR(state.st_mode) and stat.S_IMODE(state.st_mode) == 0o700
    assert (state.st_uid, state.st_gid) == (103, 103)
    names = os.listdir(directory)
    assert KEY in names and set(names) <= {KEY, KEY + ".pub"}
    for name in names:
        state = os.lstat(directory + "/" + name)
        assert stat.S_ISREG(state.st_mode) and stat.S_IMODE(state.st_mode) == 0o600
        assert (state.st_uid, state.st_gid, state.st_nlink) == (103, 103, 1)
        assert state.st_size > 0
    return len(names)


def main():
    assert os.geteuid() == 0
    os.umask(0o077)
    operation = sys.argv[1]
    if operation == "stage":
        metadata(DIRECTORY)
        with open(MANIFEST) as stream:
            original = stream.read()
        start = original.index("[service dropbear-keygen]")
        end = original.index("[service netd]", start)
        profile = original[start:end]
        for expected in ("user=keygen\n", "group=keygen\n", "authority-ceiling=0\n", "no-new-privileges=yes\n",
                         "private-state=keygen\n", "environment=empty\n"):
            assert expected in profile
        probe = os.stat("/usr/bin/goal19-keygen-policy")
        assert probe.st_uid == 0 and probe.st_gid == 0 and stat.S_IMODE(probe.st_mode) == 0o6755
        os.mkdir(BACKUP, 0o700)
        with open(BACKUP + "/manifest", "x") as stream:
            stream.write(original)
        with open(BACKUP + "/digest", "xb") as stream:
            stream.write(digest(DIRECTORY))
        os.rename(DIRECTORY, BACKUP + "/original")
        os.mkdir(DIRECTORY, 0o700)
        profile = profile.replace("exec=/bin/dropbearkey\n", "exec=/usr/bin/goal19-keygen-policy\n", 1)
        profile = profile.replace("arg=/bin/dropbearkey\n", "arg=/usr/bin/goal19-keygen-policy\n", 1)
        with open(MANIFEST, "w") as stream:
            stream.write(original[:start] + profile + original[end:])
        os.chmod(MANIFEST, 0o644)
        print("keygen boot: missing-key profile staged; original key retained privately")
    elif operation == "restore":
        count = metadata(DIRECTORY)
        os.rename(DIRECTORY, BACKUP + "/generated")
        os.rename(BACKUP + "/original", DIRECTORY)
        with open(BACKUP + "/manifest") as stream:
            original = stream.read()
        with open(MANIFEST, "w") as stream:
            stream.write(original)
        os.chmod(MANIFEST, 0o644)
        with open(BACKUP + "/digest", "rb") as stream:
            assert stream.read() == digest(DIRECTORY)
        print("keygen boot: generated files=%d; original key and production manifest restored" % count)
    elif operation == "verify":
        metadata(DIRECTORY)
        with open(BACKUP + "/digest", "rb") as stream:
            assert stream.read() == digest(DIRECTORY)
        with open(MANIFEST) as stream, open(BACKUP + "/manifest") as original:
            assert stream.read() == original.read()
        print("keygen boot: existing key unchanged across production restart")
    else:
        raise ValueError(operation)
    os.sync()


if __name__ == "__main__":
    main()
