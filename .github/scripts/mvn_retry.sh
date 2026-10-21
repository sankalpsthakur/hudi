#!/usr/bin/env bash
# Run mvn once, and once more after a pause if it failed. Used for the build steps only: a cold dependency
# cache makes every job hit Maven Central at the same moment, and a throttled download is the usual failure.
set -u
mvn "$@" && exit 0
code=$?
echo "::warning::mvn exited with $code, retrying once after 90 seconds"
sleep 90
exec mvn "$@"
