#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

mvn -q clean compile exec:java formatter:format impsort:sort "$@"

echo
echo "Generated + formatted sources under: $(pwd)/target/generated"
