#!/usr/bin/env bash
# The pushed branch must be: base <- fix <- one test commit <- this workflow commit.
# Needs BASE_SHA, FIX_SHA and TEST_FILE in the environment and a checkout of depth 4 or more.
set -eu

git log --format='%h %an <%ae> | %cn <%ce> | %s' -5

test "$(git rev-parse --short=11 HEAD~3)" = "${BASE_SHA}"
test "$(git rev-parse --short=11 HEAD~2)" = "${FIX_SHA}"

echo "files changed by the test commit:"
git diff --name-only HEAD~2 HEAD~1
test "$(git diff --name-only HEAD~2 HEAD~1)" = "${TEST_FILE}"
git diff --stat HEAD~2 HEAD~1

echo "files changed by the workflow commit:"
git diff --name-only HEAD~1 HEAD
test -z "$(git diff --name-only HEAD~1 HEAD | grep -v '^\.github/' || true)"

echo "trailers on the test commit:"
trailers="$(git log -1 --format=%B HEAD~1 | git interpret-trailers --parse)"
echo "[${trailers}]"
test -z "${trailers}"

echo "branch shape OK"
