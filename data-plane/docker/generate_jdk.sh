#!/usr/bin/env sh

if [ $# -eq 0 ]
  then
    echo "No arguments supplied, You must provide the jar"
fi

echo "Computing mods for $1"
MODS=$(jdeps -q --print-module-deps --ignore-missing-deps "$1" | sed -e 's/^[ \t]*//')

echo "Computed mods = '$MODS'"

# Patch adding the dns
MODS="$MODS,jdk.naming.dns"

echo "Patched modules shipped with the generated jdk = '$MODS'"
jlink --verbose --no-header-files --no-man-pages --compress=2 --strip-debug --add-modules "$MODS" --output /app/jdk

