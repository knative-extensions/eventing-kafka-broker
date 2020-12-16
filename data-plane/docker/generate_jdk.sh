#!/usr/bin/env sh
#
# Copyright 2020 The Knative Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

if [ $# -eq 0 ]; then
  echo "No arguments supplied, You must provide the jar"
fi

echo "Computing mods for $1"
MODS=$(jdeps -q --print-module-deps --ignore-missing-deps "$1")
echo "Computed mods = '$MODS'"

# Remove compiler, sql, management modules
MODS=$(echo $MODS | tr , '\n' | sed '/^java.compiler/d' | sed -z 's/\n/,/g;s/,$/\n/')
# Patch adding the dns
MODS="$MODS,jdk.naming.dns"

echo "Patched modules shipped with the generated jdk = '$MODS'"
jlink --verbose --no-header-files --no-man-pages --compress=2 --strip-debug --add-modules "$MODS" --output /app/jdk
