#!/usr/bin/env sh

if [ $# -eq 0 ]
  then
    echo "No arguments supplied, You must provide the jar"
fi

echo "Computing mods for $1"
MODS=$(jdeps -q --print-module-deps --ignore-missing-deps "$1")
echo "Computed mods = '$MODS'"

# Remove compiler, sql, management modules
MODS=$(echo $MODS | tr , '\n' | sed '/^java.compiler/d' | sed '/^jdk.management/d' | sed -z 's/\n/,/g;s/,$/\n/')
# Patch adding the dns
MODS="$MODS,jdk.naming.dns"

echo "Patched modules shipped with the generated jdk = '$MODS'"
jlink --verbose --no-header-files --no-man-pages --compress=2 --strip-debug --add-modules "$MODS" --output /app/jdk

