#!/usr/bin/env bash
# Select the sources a validation job runs on.
#   fix  = the branch as pushed (base commit + one fix commit + this workflow commit)
#   base = the same tree with hudi-common put back to the base commit, i.e. the base commit itself
# The fix touches hudi-common only, so restoring that directory is enough; the check below proves it.
set -euo pipefail
variant="${1:?variant (fix|base)}"
base="${2:?base sha}"

git log --oneline -3
test "$(git rev-parse HEAD~2)" = "$base" || { echo "HEAD~2 is not the expected base commit"; exit 1; }

if [ "$variant" = "base" ]; then
  git checkout "$base" -- hudi-common
fi

echo "== files that differ from the base commit outside .github (variant=$variant)"
git diff --stat "$base" -- . ':(exclude).github'
count="$(git diff --name-only "$base" -- . ':(exclude).github' | wc -l | tr -d ' ')"
if [ "$variant" = "base" ]; then
  test "$count" = "0" || { echo "base variant must equal the base commit, found $count differing files"; exit 1; }
else
  test "$count" = "5" || { echo "fix variant must differ in exactly 5 files, found $count"; exit 1; }
fi
echo "variant=$variant ok ($count differing files)"
