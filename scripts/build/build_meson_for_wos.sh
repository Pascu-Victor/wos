#!/bin/bash
# Stage Meson so it can run inside WOS using the native WOS Python.
# Meson is pure Python; this installs its package into the target sysroot's
# site-packages and writes a /usr/bin/meson launcher.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

B="$WORKSPACE_ROOT/toolchain"
TARGET_SYSROOT="${WOS_SYSROOT_PATH:-$B/sysroot}"
MESON_SRC="${WOS_MESON_SOURCE_DIR:-$B/src/meson}"
MESON_BUILD="${WOS_MESON_BUILD_DIR:-$B/meson-build}"
MESON_WORK="$MESON_BUILD/work"
MESON_COMMIT="${WOS_MESON_COMMIT:-b300d9578fe62c721afbf4e5c4672ad0c94cb96c}"
MESON_TARBALL_URL="${WOS_MESON_TARBALL_URL:-https://github.com/Pascu-Victor/meson/archive/$MESON_COMMIT.tar.gz}"
MESON_TARBALL_SHA256="${WOS_MESON_TARBALL_SHA256:-afd1b4b8debff255cdf793f46a059559d9f1c3d14549371f34a9c008bf797620}"
MESON_VERSION=""

require_file() {
    local path="$1"
    local hint="$2"

    if [ ! -e "$path" ]; then
        echo "ERROR: missing $path" >&2
        echo "$hint" >&2
        exit 1
    fi
}

download_meson_source() {
    local dest="$1"
    local archive_dir="$MESON_BUILD/src"
    local archive="$archive_dir/meson-$MESON_COMMIT.tar.gz"
    local tmp_dest="$dest.tmp"

    mkdir -p "$archive_dir"
    if [ ! -f "$archive" ]; then
        if ! command -v curl >/dev/null 2>&1; then
            echo "ERROR: Meson source not found at $MESON_SRC and curl is unavailable." >&2
            echo "Populate $MESON_SRC from https://github.com/Pascu-Victor/meson." >&2
            exit 1
        fi
        echo "Downloading Meson source from Pascu-Victor/meson..." >&2
        if ! curl -fL "$MESON_TARBALL_URL" -o "$archive.tmp"; then
            rm -f "$archive.tmp"
            echo "ERROR: failed to download $MESON_TARBALL_URL" >&2
            exit 1
        fi
        mv "$archive.tmp" "$archive"
    fi

    echo "$MESON_TARBALL_SHA256  $archive" | sha256sum -c - >&2
    rm -rf "$tmp_dest" "$dest"
    mkdir -p "$tmp_dest"
    tar -xzf "$archive" -C "$tmp_dest" --strip-components=1
    mv "$tmp_dest" "$dest"
}

resolve_meson_source() {
    local fallback_src="$MESON_BUILD/src/meson-$MESON_COMMIT"

    if [ -f "$MESON_SRC/meson.py" ] && [ -d "$MESON_SRC/mesonbuild" ]; then
        printf '%s\n' "$MESON_SRC"
        return 0
    fi

    if [ -f "$fallback_src/meson.py" ] && [ -d "$fallback_src/mesonbuild" ]; then
        printf '%s\n' "$fallback_src"
        return 0
    fi

    if [ -d "$MESON_SRC" ] && [ -n "$(find "$MESON_SRC" -mindepth 1 -maxdepth 1 -print -quit)" ]; then
        echo "ERROR: Meson source at $MESON_SRC does not contain meson.py and mesonbuild/." >&2
        echo "Use the Pascu-Victor/meson source tree or clear the directory so the pinned snapshot can be downloaded." >&2
        exit 1
    fi

    download_meson_source "$fallback_src"
    printf '%s\n' "$fallback_src"
}

copy_source_to_workdir() {
    local source_dir="$1"

    rm -rf "$MESON_WORK"
    mkdir -p "$MESON_WORK"
    (
        cd "$source_dir"
        tar \
            --exclude='./.git' \
            --exclude='./.github' \
            --exclude='./ci' \
            --exclude='./docs' \
            --exclude='./manual tests' \
            --exclude='./test cases' \
            --exclude='./unittests' \
            -cf - .
    ) | (
        cd "$MESON_WORK"
        tar -xf -
    )
}

patch_meson_source_for_wos() {
    local work_dir="$1"

    python3 - "$work_dir" <<'PY'
from pathlib import Path
import sys

root = Path(sys.argv[1])
envconfig = root / 'mesonbuild' / 'envconfig.py'
universal = root / 'mesonbuild' / 'utils' / 'universal.py'

text = envconfig.read_text()
if "def is_wos(self) -> bool:" not in text:
    marker = """    def is_linux(self) -> bool:\n        \"\"\"\n        Machine is linux?\n        \"\"\"\n        return self.system == 'linux'\n"""
    replacement = marker + """\n    def is_wos(self) -> bool:\n        \"\"\"\n        Machine is WOS?\n        \"\"\"\n        return self.system == 'wos'\n"""
    if marker not in text:
        raise SystemExit(f'unable to patch MachineInfo.is_wos into {envconfig}')
    text = text.replace(marker, replacement)

if "'wos': 'wos'," not in text:
    marker = """                                        'linux': 'linux',\n"""
    replacement = marker + """                                        'wos': 'wos',\n"""
    if marker not in text:
        raise SystemExit(f'unable to patch WOS kernel mapping into {envconfig}')
    text = text.replace(marker, replacement)

envconfig.write_text(text)

text = universal.read_text()
if "    'is_wos'," not in text:
    marker = """    'is_windows',\n"""
    replacement = marker + """    'is_wos',\n"""
    if marker not in text:
        raise SystemExit(f'unable to export is_wos from {universal}')
    text = text.replace(marker, replacement)

if "def is_wos() -> bool:" not in text:
    marker = """def is_linux() -> bool:\n    return _PLATFORM_SYSTEM_LOWER == 'linux'\n\n\n"""
    replacement = marker + """def is_wos() -> bool:\n    return _PLATFORM_SYSTEM_LOWER == 'wos'\n\n\n"""
    if marker not in text:
        raise SystemExit(f'unable to patch is_wos into {universal}')
    text = text.replace(marker, replacement)

universal.write_text(text)
PY
}

find_python_site_packages() {
    local site_packages=""
    local python_lib=""
    local entry

    if [ -n "${WOS_MESON_SITE_PACKAGES:-}" ]; then
        mkdir -p "$WOS_MESON_SITE_PACKAGES"
        printf '%s\n' "$WOS_MESON_SITE_PACKAGES"
        return 0
    fi

    for entry in "$TARGET_SYSROOT"/lib/python[0-9]*/site-packages; do
        [ -d "$entry" ] || continue
        site_packages="$entry"
    done
    if [ -n "$site_packages" ]; then
        printf '%s\n' "$site_packages"
        return 0
    fi

    for entry in "$TARGET_SYSROOT"/lib/python[0-9]*; do
        [ -d "$entry" ] || continue
        python_lib="$entry"
    done
    if [ -z "$python_lib" ]; then
        echo "ERROR: could not find a Python stdlib directory under $TARGET_SYSROOT/lib." >&2
        echo "Run scripts/build/build_python_for_wos.sh before building Meson." >&2
        exit 1
    fi

    site_packages="$python_lib/site-packages"
    mkdir -p "$site_packages"
    printf '%s\n' "$site_packages"
}

install_meson_package() {
    local site_packages="$1"
    local dist_info="$site_packages/meson-$MESON_VERSION.dist-info"

    rm -rf "$site_packages/mesonbuild" "$site_packages"/meson-*.dist-info
    cp -a "$MESON_WORK/mesonbuild" "$site_packages/mesonbuild"

    mkdir -p "$dist_info"
    cat > "$dist_info/METADATA" <<EOF
Metadata-Version: 2.1
Name: meson
Version: $MESON_VERSION
Summary: A high performance build system
Requires-Python: >=3.10
EOF
    printf 'wos\n' > "$dist_info/INSTALLER"
    : > "$dist_info/RECORD"

    mkdir -p "$TARGET_SYSROOT/bin"
    cat > "$TARGET_SYSROOT/bin/meson" <<'EOF'
#!/usr/bin/python3
import sys
from mesonbuild import mesonmain

if __name__ == '__main__':
    sys.exit(mesonmain.main())
EOF
    chmod 755 "$TARGET_SYSROOT/bin/meson"
}

require_file "$TARGET_SYSROOT/bin/python3" "Build native WOS Python before building Meson."

MESON_SOURCE_DIR="$(resolve_meson_source)"
copy_source_to_workdir "$MESON_SOURCE_DIR"
patch_meson_source_for_wos "$MESON_WORK"
MESON_VERSION="$(
    python3 - "$MESON_WORK/mesonbuild/coredata.py" <<'PY'
from pathlib import Path
import ast
import sys

tree = ast.parse(Path(sys.argv[1]).read_text())
for node in tree.body:
    if isinstance(node, ast.Assign):
        for target in node.targets:
            if isinstance(target, ast.Name) and target.id == 'version':
                print(ast.literal_eval(node.value))
                raise SystemExit(0)
raise SystemExit('unable to find Meson version in coredata.py')
PY
)"

SITE_PACKAGES="$(find_python_site_packages)"
install_meson_package "$SITE_PACKAGES"

python3 -m compileall -q "$MESON_WORK/mesonbuild"
PYTHONPATH="$SITE_PACKAGES${PYTHONPATH:+:$PYTHONPATH}" python3 -m mesonbuild.mesonmain --version >/dev/null

require_file "$TARGET_SYSROOT/bin/meson" "Meson install did not produce $TARGET_SYSROOT/bin/meson."
require_file "$SITE_PACKAGES/mesonbuild/mesonmain.py" "Meson install did not produce mesonbuild in site-packages."

echo "Native WOS Meson installed to $TARGET_SYSROOT/bin/meson"
