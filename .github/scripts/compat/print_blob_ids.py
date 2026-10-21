#!/usr/bin/env python3
"""Print the ids of the four BLOB reference fields persisted in a table's InternalSchema history.

usage: print_blob_ids.py TABLE_ROOT [--expect legacy|current|any]
Looks at TABLE_ROOT/*/.hoodie/.schema/* (the history files written by schema-on-read) and prints, per file and
per schema, the ids of external_path / offset / length / managed. Exit code 1 when nothing is found or the
ids do not match --expect.
"""
import glob
import json
import os
import sys

LEGACY = [0, 1, 2, 3]
CURRENT = [-13, -14, -15, -16]


def walk(node, found):
    if isinstance(node, dict):
        fields = node.get("fields")
        if isinstance(fields, list) and len(fields) == 4 and [f.get("name") for f in fields] == ["external_path", "offset", "length", "managed"]:
            found.append([f.get("id") for f in fields])
        for value in node.values():
            walk(value, found)
    elif isinstance(node, list):
        for value in node:
            walk(value, found)


def main():
    root = sys.argv[1]
    expect = "any"
    if "--expect" in sys.argv:
        expect = sys.argv[sys.argv.index("--expect") + 1]
    files = sorted(glob.glob(os.path.join(root, "*", ".hoodie", ".schema", "*")))
    print("history files:", len(files))
    all_ids = []
    for path in files:
        try:
            with open(path, encoding="utf-8") as fh:
                data = json.load(fh)
        except Exception as exc:  # not a json history file
            print("  skipped %s (%s)" % (path, exc))
            continue
        found = []
        walk(data, found)
        print("  %s: reference ids per persisted schema = %s" % (os.path.relpath(path, root), found))
        all_ids.extend(found)
    if not all_ids:
        print("FAIL: no BLOB reference record found in the persisted history")
        return 1
    if expect == "legacy" and any(ids != LEGACY for ids in all_ids):
        print("FAIL: expected only the legacy ids", LEGACY)
        return 1
    if expect == "current" and any(ids != CURRENT for ids in all_ids):
        print("FAIL: expected only the current ids", CURRENT)
        return 1
    print("OK (%s)" % expect)
    return 0


if __name__ == "__main__":
    sys.exit(main())
