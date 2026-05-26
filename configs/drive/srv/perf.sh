#!/bin/sh
if [ -x /usr/bin/wos-userland-suite ]; then
    exec /usr/bin/wos-userland-suite "$@"
fi

exec /srv/wos_userland_suite.sh "$@"
