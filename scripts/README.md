# WOS Scripts

Public commands live in `../bin` and are added to `PATH` by `.vscode/wos-env.sh`.
Use lowercase hyphenated command names such as `wos-cluster`, `wos-ssh`,
`wos-test`, and `wos-format`.

The implementation scripts are grouped by role:

- `build/`: build, image, initramfs, and rootfs staging helpers.
- `cluster/`: WOS and Linux cluster topology helpers.
- `test/`: host-side test and kernel coverage tools.
- `bench/`: benchmark orchestration, provisioning, and benchmark runners.
- `remote/`: WOS and Linux SSH/SFTP/SCP helpers.
- `debug/`: host-side debug artifact extraction.
- `dev/`: developer maintenance tools.
- `lib/`: reserved for shared script helpers.

Do not add new public scripts at the top level of this directory. Add the
implementation under the appropriate category and expose it with a `bin/`
symlink only if it is meant to be part of the user-facing command surface.
