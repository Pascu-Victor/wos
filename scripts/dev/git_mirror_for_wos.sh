#!/bin/bash
# Build and serve a local GitHub mirror for WOS development VMs.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

MIRROR_ROOT="${WOS_GIT_MIRROR_ROOT:-${XDG_CACHE_HOME:-$HOME/.cache}/wos/git-mirror}"
REPOS_ROOT="${WOS_GIT_REPOS_ROOT:-$MIRROR_ROOT/repos}"
HTTP_ROOT="${WOS_GIT_HTTP_ROOT:-$MIRROR_ROOT/http}"
DEFAULT_BIND="${WOS_GIT_HTTP_BIND:-0.0.0.0}"
DEFAULT_PORT="${WOS_GIT_HTTP_PORT:-8019}"
DEFAULT_WOS_HOST="${WOS_GIT_WOS_HOST:-root@wos-0.wos}"
DEFAULT_WOS_PATH="${WOS_GIT_WOS_PATH:-/tmp/wos-git-repos}"

usage() {
    cat <<EOF
Usage:
  scripts/dev/git_mirror_for_wos.sh list
  scripts/dev/git_mirror_for_wos.sh mirror [--remote-only]
  scripts/dev/git_mirror_for_wos.sh serve-http [--bind ADDR] [--port PORT]
  scripts/dev/git_mirror_for_wos.sh print-wos-config http HOST [PORT]
  scripts/dev/git_mirror_for_wos.sh print-wos-config file PATH
  scripts/dev/git_mirror_for_wos.sh sync-file-mirror [WOS_HOST] [REMOTE_PATH]

Defaults:
  mirror root   $MIRROR_ROOT
  repo root     $REPOS_ROOT
  HTTP bind     $DEFAULT_BIND
  HTTP port     $DEFAULT_PORT
  WOS host      $DEFAULT_WOS_HOST
  WOS path      $DEFAULT_WOS_PATH

Environment:
  WOS_GIT_MIRROR_ROOT  Persistent host-side mirror root.
  WOS_GIT_REPOS_ROOT   Bare repository root served by smart HTTP.
  WOS_GIT_HTTP_ROOT    Generated Python CGI document root.
  WOS_GIT_HTTP_BIND    Address for serve-http.
  WOS_GIT_HTTP_PORT    Port for serve-http.
  WOS_GIT_WOS_HOST     Default host for sync-file-mirror.
  WOS_GIT_WOS_PATH     Default remote mirror path for sync-file-mirror.

Notes:
  mirror uses local checkout/submodule repositories first when present, so a
  prepared host checkout does not immediately re-download everything from
  GitHub. Pass --remote-only to force GitHub remotes.

  serve-http exposes repositories as:
    http://HOST:PORT/cgi-bin/git/OWNER/REPO.git

  If WOS cannot reach the host HTTP port, use sync-file-mirror and the file
  rewrite printed at the end. That still avoids GitHub completely.
EOF
}

die() {
    echo "error: $*" >&2
    exit 1
}

need_cmd() {
    command -v "$1" >/dev/null 2>&1 || die "$1 is not installed or not in PATH"
}

normalize_github_path() {
    local url="$1"
    local path

    case "$url" in
        https://github.com/*)
            path="${url#https://github.com/}"
            ;;
        git@github.com:*)
            path="${url#git@github.com:}"
            ;;
        ssh://git@github.com/*)
            path="${url#ssh://git@github.com/}"
            ;;
        *)
            die "unsupported non-GitHub URL: $url"
            ;;
    esac

    path="${path%.git}.git"
    printf '%s\n' "$path"
}

submodule_urls() {
    git -C "$WORKSPACE_ROOT" config -f .gitmodules --get-regexp '^submodule\..*\.url$' |
        awk '{ print $2 }'
}

top_level_url() {
    local url
    url="$(git -C "$WORKSPACE_ROOT" config --get remote.origin.url || true)"
    if [ -z "$url" ]; then
        url="https://github.com/Pascu-Victor/wos.git"
    fi
    printf '%s\n' "${url%.git}.git"
}

repo_urls() {
    {
        top_level_url
        submodule_urls
    } | awk '!seen[$0]++'
}

local_source_for_url() {
    local url="$1"
    local top_url

    top_url="$(top_level_url)"
    if [ "$url" = "$top_url" ]; then
        printf '%s\n' "$WORKSPACE_ROOT"
        return 0
    fi

    while IFS= read -r name; do
        local sub_url path gitdir

        sub_url="$(git -C "$WORKSPACE_ROOT" config -f .gitmodules --get "submodule.$name.url")"
        if [ "${sub_url%.git}.git" != "$url" ]; then
            continue
        fi

        path="$(git -C "$WORKSPACE_ROOT" config -f .gitmodules --get "submodule.$name.path")"
        if [ ! -d "$WORKSPACE_ROOT/$path" ]; then
            return 1
        fi

        gitdir="$(git -C "$WORKSPACE_ROOT/$path" rev-parse --git-dir 2>/dev/null || true)"
        if [ -n "$gitdir" ]; then
            printf '%s\n' "$WORKSPACE_ROOT/$path"
            return 0
        fi

        return 1
    done < <(git -C "$WORKSPACE_ROOT" config -f .gitmodules --get-regexp '^submodule\..*\.path$' |
        sed -E 's/^submodule\.([^.]*)\.path .*/\1/')

    return 1
}

mirror_one() {
    local url="$1"
    local prefer_local="$2"
    local github_path target source

    github_path="$(normalize_github_path "$url")"
    target="$REPOS_ROOT/$github_path"
    mkdir -p "$(dirname "$target")"

    if [ -d "$target" ]; then
        echo "Updating $github_path"
        git -C "$target" remote update --prune
        git -C "$target" update-server-info
        return
    fi

    source="$url"
    if [ "$prefer_local" -eq 1 ] && source="$(local_source_for_url "$url")"; then
        echo "Mirroring $github_path from local checkout"
    else
        source="$url"
        echo "Mirroring $github_path from remote"
    fi

    git clone --mirror "$source" "$target"
    git -C "$target" update-server-info
}

cmd_list() {
    repo_urls
}

cmd_mirror() {
    local prefer_local=1

    while (($# > 0)); do
        case "$1" in
            --remote-only)
                prefer_local=0
                ;;
            -h|--help)
                usage
                return 0
                ;;
            *)
                die "unknown mirror option: $1"
                ;;
        esac
        shift
    done

    need_cmd git
    mkdir -p "$REPOS_ROOT"

    while IFS= read -r url; do
        mirror_one "$url" "$prefer_local"
    done < <(repo_urls)
}

write_cgi_wrapper() {
    local cgi_dir="$HTTP_ROOT/cgi-bin"
    mkdir -p "$cgi_dir"

    cat >"$cgi_dir/git" <<EOF
#!/bin/sh
export GIT_PROJECT_ROOT='$REPOS_ROOT'
export GIT_HTTP_EXPORT_ALL=1
exec git http-backend
EOF
    chmod +x "$cgi_dir/git"
}

cmd_serve_http() {
    local bind="$DEFAULT_BIND"
    local port="$DEFAULT_PORT"

    while (($# > 0)); do
        case "$1" in
            --bind)
                bind="${2:-}"
                [ -n "$bind" ] || die "--bind requires an address"
                shift
                ;;
            --port)
                port="${2:-}"
                [[ "$port" =~ ^[0-9]+$ ]] || die "--port requires a decimal port"
                shift
                ;;
            -h|--help)
                usage
                return 0
                ;;
            *)
                die "unknown serve-http option: $1"
                ;;
        esac
        shift
    done

    need_cmd git
    need_cmd python3
    [ -d "$REPOS_ROOT" ] || die "repo root does not exist: $REPOS_ROOT; run mirror first"

    write_cgi_wrapper
    echo "Serving $REPOS_ROOT"
    echo "Rewrite WOS with:"
    echo "  scripts/dev/git_mirror_for_wos.sh print-wos-config http HOST $port"
    exec python3 -m http.server --cgi "$port" --bind "$bind" --directory "$HTTP_ROOT"
}

cmd_print_wos_config() {
    local mode="${1:-}"
    shift || true

    case "$mode" in
        http)
            local host="${1:-}"
            local port="${2:-$DEFAULT_PORT}"
            [ -n "$host" ] || die "print-wos-config http requires HOST"
            cat <<EOF
git config --global url.http://$host:$port/cgi-bin/git/.insteadOf https://github.com/
EOF
            ;;
        file)
            local path="${1:-}"
            [ -n "$path" ] || die "print-wos-config file requires PATH"
            cat <<EOF
git config --global protocol.file.allow always
git config --global url.file://$path/.insteadOf https://github.com/
EOF
            ;;
        *)
            die "usage: print-wos-config http HOST [PORT] | file PATH"
            ;;
    esac
}

cmd_sync_file_mirror() {
    local wos_host="${1:-$DEFAULT_WOS_HOST}"
    local wos_path="${2:-$DEFAULT_WOS_PATH}"
    local remote_parent
    local remote_copied_path

    case "$wos_path" in
        /tmp/*|/var/tmp/*)
            ;;
        *)
            die "refusing to replace non-temporary WOS path: $wos_path"
            ;;
    esac

    need_cmd ssh
    need_cmd scp
    [ -d "$REPOS_ROOT" ] || die "repo root does not exist: $REPOS_ROOT; run mirror first"

    remote_parent="$(dirname "$wos_path")"
    remote_copied_path="$remote_parent/$(basename "$REPOS_ROOT")"
    echo "Copying $REPOS_ROOT to $wos_host:$wos_path"
    ssh "$wos_host" sh -s -- "$wos_path" "$remote_copied_path" <<'EOF'
set -e
wos_path="$1"
remote_copied_path="$2"
rm -rf -- "$wos_path"
if [ "$remote_copied_path" != "$wos_path" ]; then
    rm -rf -- "$remote_copied_path"
fi
mkdir -p -- "$(dirname "$wos_path")"
EOF
    scp -O -r "$REPOS_ROOT" "$wos_host:$remote_parent/"
    ssh "$wos_host" sh -s -- "$wos_path" "$remote_copied_path" <<'EOF'
set -e
wos_path="$1"
remote_copied_path="$2"
if [ "$remote_copied_path" != "$wos_path" ]; then
    mv -- "$remote_copied_path" "$wos_path"
fi
EOF
    echo
    echo "Run this inside WOS:"
    cmd_print_wos_config file "$wos_path"
}

main() {
    local command="${1:-}"
    if [ -n "$command" ]; then
        shift
    fi

    case "$command" in
        list)
            cmd_list "$@"
            ;;
        mirror)
            cmd_mirror "$@"
            ;;
        serve-http)
            cmd_serve_http "$@"
            ;;
        print-wos-config)
            cmd_print_wos_config "$@"
            ;;
        sync-file-mirror)
            cmd_sync_file_mirror "$@"
            ;;
        -h|--help|"")
            usage
            ;;
        *)
            die "unknown command: $command"
            ;;
    esac
}

main "$@"
