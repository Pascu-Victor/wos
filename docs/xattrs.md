# Extended attributes

WOS exposes the standard `setxattr`, `getxattr`, `listxattr`, and
`removexattr` path, no-follow path (`l*`), and file-descriptor (`f*`) APIs from
`<sys/xattr.h>`.

## Limits and namespaces

- Qualified names are at most 255 bytes, excluding the terminating NUL.
- Values and packed name lists are at most 64 KiB.
- `user.*` attributes are available to ordinary callers.
- `trusted.*` and `security.*` require an effective user ID of zero.
- Private XFS namespaces, including parent-pointer records, are neither
  addressable nor visible through the public APIs.
- Filesystems without xattr support return `EOPNOTSUPP`.

Names returned by `listxattr` are qualified, NUL-terminated, and packed without
an extra terminator. Passing a null or zero-sized output buffer performs a size
query. A nonzero buffer smaller than the complete value or list fails with
`ERANGE` and does not return a partial result.

`XATTR_CREATE` fails with `EEXIST` when the name is already present;
`XATTR_REPLACE` fails with `ENODATA` when it is absent. Combining those flags or
passing unknown flags fails with `EINVAL`.

## XFS persistence

XFS stores attributes in standard v5 shortform, leaf, remote-value, and DA-tree
formats. Attribute metadata changes run under the mount metadata lock and join
the mount-scoped write-ahead transaction. A failed mutation leaves the old
attribute fork reachable; newly allocated or retired blocks become visible or
reusable only with the corresponding committed transaction.

## Remote VFS

Remote xattrs are available only after both peers negotiate the additive WKI
xattr capability. Large requests use bounded, versioned staging rather than
generic packet fragmentation. Every mutation carries a session and operation
identity; retrying the same identity returns the recorded result and never
applies the mutation twice. Older peers fail with `EOPNOTSUPP` before a request
is sent.

Remote requests expose only `user.*`. WKI does not currently propagate caller
credentials, so accepting `trusted.*` or `security.*` on behalf of the remote
worker would incorrectly apply that worker's authority. Local XFS retains the
namespace policy above.

Successful and completion-ambiguous remote mutations invalidate the proxy's
metadata caches. WKI RX only validates and queues bounded work; filesystem I/O,
allocation, staging, and cleanup run in worker context.

## Verification

Inside WOS, run the same matrix against a local XFS path and a remote path:

```sh
testprog xattr-matrix /var/tmp/wos-xattr-test
testprog xattr-matrix /wki/<peer>/var/tmp/wos-xattr-test
```

`/var/tmp` is on the XFS root filesystem. WOS mounts `/tmp` as tmpfs, so xattr
operations there intentionally return `EOPNOTSUPP`.

For negative-path checks on an already existing file, the same command can
assert the exact set error by number. For example, `EOPNOTSUPP` is 95:

```sh
touch /tmp/wos-xattr-unsupported
testprog xattr-matrix --expect-set-error 95 /tmp/wos-xattr-unsupported
```

This probe is also useful while a WKI peer is disconnected; it reports the
actual error and fails unless it matches the expected value, without silently
retrying a mutation.

The matrix covers path/lpath/fd operations, size queries, `ERANGE`, empty and
64-KiB values, create/replace semantics, list packing, symlinks, internal
namespace denial, enough attributes to force tree growth and shrink, and
explicit retirement of a remote value before inode teardown. Two workers also
race replace against remove/create and then verify a deterministic final value.

The isolated diagnostic VM and multi-node cluster can run without setup/root
steps by appending `--no-setup`:

```sh
bin/wos-ktest --no-setup
bin/wos-cluster --launch --no-setup
```
