#!/usr/bin/env sh

if [ $# -eq 0 ]
  then
    echo "No arguments supplied, You must provide the jar"
fi

echo "Computing mods for $1"
MODS=$(jdeps -q --print-module-deps --ignore-missing-deps "$1" | sed -e 's/^[ \t]*//' | sed -z 's/\n/,/g;s/,$/\n/')
echo "Modules = '$MODS'"
jlink --verbose --no-header-files --no-man-pages --compress=2 --strip-debug --add-modules "$MODS" --output /app/jdk

