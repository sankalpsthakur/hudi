#!/usr/bin/env python3
"""Summarise and compare surefire / failsafe / scalatest XML reports (validation runs on a fork).

  summarize --root DIR --label TEXT [--expect A,B#m*,C] [--collect OUT]
      One table per job: tests, failures, skipped, flaky per test class. Exit 1 on any failure or when an
      expected class did not run (or ran only skipped tests), so a filter that selected nothing cannot
      look green.

  compare --root DIR
      DIR holds one directory per uploaded artifact, named reports-<job>-<variant> (variant fix | base).
      Prints per-job totals for both variants and the tests that fail with the fix but not on the base.
"""
import argparse
import os
import shutil
import sys
import xml.etree.ElementTree as ET
from collections import OrderedDict, defaultdict

RANK = {"pass": 0, "flaky": 1, "skip": 2, "fail": 3}


def report_files(root):
    for dirpath, _dirs, files in os.walk(root):
        if os.path.basename(dirpath) in ("surefire-reports", "failsafe-reports"):
            for name in sorted(files):
                if name.startswith("TEST-") and name.endswith(".xml"):
                    yield os.path.join(dirpath, name)


def module_of(path, root):
    parent = os.path.dirname(os.path.dirname(path))
    if os.path.basename(parent) == "target":
        parent = os.path.dirname(parent)
    rel = os.path.relpath(parent, root).replace(os.sep, "/")
    return rel if rel != "." else "(root)"


def first_line(text, limit=300):
    text = (text or "").strip().splitlines()
    return (text[0] if text else "")[:limit]


def parse_cases(path, root):
    """Yield (module, classname, name, status, detail) for every testcase in one report file."""
    module = module_of(path, root)
    try:
        tree = ET.parse(path)
    except ET.ParseError as exc:
        yield module, "<unparseable:%s>" % os.path.basename(path), "<file>", "fail", str(exc)
        return
    top = tree.getroot()
    suites = [top] if top.tag == "testsuite" else list(top.iter("testsuite"))
    seen = set()
    for suite in suites:
        for case in suite.findall("testcase"):
            if id(case) in seen:
                continue
            seen.add(id(case))
            classname = case.get("classname") or suite.get("name") or "?"
            name = case.get("name") or "?"
            status, detail = "pass", ""
            for child in case:
                tag = child.tag
                if tag in ("failure", "error"):
                    status = "fail"
                    detail = first_line(child.get("message") or child.text or tag)
                    break
                if tag == "skipped" and status != "fail":
                    status = "skip"
                elif tag in ("flakyFailure", "flakyError") and status == "pass":
                    status = "flaky"
                    detail = first_line(child.get("message") or child.text or tag)
            yield module, classname, name, status, detail


def load(root):
    cases = []
    for path in report_files(root):
        cases.extend(parse_cases(path, root))
    return cases


def simple(classname):
    return classname.replace("$", ".").split(".")[-1] if "$" not in classname else classname.split("$")[0].split(".")[-1]


def parse_expect(text):
    wanted = []
    for item in [x.strip() for x in (text or "").split(",") if x.strip()]:
        cls, _, method = item.partition("#")
        wanted.append((cls.strip().split(".")[-1], method.strip().rstrip("*")))
    return wanted


def check_expected(cases, wanted):
    """Returns (problems, notes, rows). A class that did not run at all is a problem (the filter selected
    nothing); a class whose tests were all skipped by design (for example Spark 4 only suites on Spark 3.5)
    is only a note."""
    problems, notes, rows = [], [], []
    for cls, method in wanted:
        mine = [c for c in cases if simple(c[1]) == cls and (not method or c[2].startswith(method) or method in c[2])]
        if not mine:
            problems.append("NOT RUN: %s%s" % (cls, "#" + method + "*" if method else ""))
            rows.append((cls, method, 0, 0))
            continue
        ran = [c for c in mine if c[3] != "skip"]
        rows.append((cls, method, len(mine), len(ran)))
        if not ran:
            notes.append("ONLY SKIPPED (by the test itself): %s" % cls)
    return problems, notes, rows


def cmd_summarize(args):
    root = os.path.abspath(args.root)
    cases = load(root)
    per_class = OrderedDict()
    for module, cls, name, status, detail in cases:
        row = per_class.setdefault((module, cls), defaultdict(int))
        row["tests"] += 1
        row[status] += 1
    total = defaultdict(int)
    lines = ["### %s" % args.label, "", "| module | class | tests | failed | skipped | flaky |", "|---|---|---:|---:|---:|---:|"]
    for (module, cls), row in sorted(per_class.items()):
        lines.append("| %s | %s | %d | %d | %d | %d |" % (module, cls, row["tests"], row["fail"], row["skip"], row["flaky"]))
        for key in ("tests", "fail", "skip", "flaky"):
            total[key] += row[key]
    passed = total["tests"] - total["fail"] - total["skip"]
    lines += ["", "TOTAL tests=%d passed=%d failed=%d skipped=%d flaky=%d classes=%d" % (
        total["tests"], passed, total["fail"], total["skip"], total["flaky"], len(per_class))]
    failed = [c for c in cases if c[3] == "fail"]
    if failed:
        lines += ["", "Failed tests:"]
        for module, cls, name, _status, detail in failed[:60]:
            lines.append("- %s :: %s :: %s -- %s" % (module, cls, name, detail))
    flaky = [c for c in cases if c[3] == "flaky"]
    if flaky:
        lines += ["", "Flaky (failed, then passed on rerun):"]
        for module, cls, name, _status, detail in flaky[:40]:
            lines.append("- %s :: %s :: %s -- %s" % (module, cls, name, detail))
    problems = []
    if args.expect:
        problems, notes, rows = check_expected(cases, parse_expect(args.expect))
        lines += ["", "Expected classes (class, method filter, testcases found, testcases that actually ran):"]
        for cls, method, found, ran in rows:
            lines.append("- %s%s: found=%d ran=%d" % (cls, "#" + method + "*" if method else "", found, ran))
        if notes:
            lines += ["", "NOTES:"] + ["- " + n for n in notes]
        if problems:
            lines += ["", "PROBLEMS:"] + ["- " + p for p in problems]
    if not cases and not args.allow_empty:
        problems.append("no testcase found under %s" % root)
        lines += ["", "PROBLEMS:", "- no testcase found under %s" % root]
    text = "\n".join(lines)
    print("=== SUMMARY BEGIN ===")
    print(text)
    print("=== SUMMARY END ===")
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write(text + "\n\n")
    if args.collect:
        out = os.path.abspath(args.collect)
        for path in report_files(root):
            report_dir = os.path.dirname(path)
            module = module_of(path, root).replace("/", "__")
            dest = os.path.join(out, module, os.path.basename(report_dir))
            os.makedirs(dest, exist_ok=True)
            shutil.copy2(path, dest)
            txt = os.path.join(report_dir, os.path.basename(path)[len("TEST-"):-len(".xml")] + ".txt")
            if os.path.exists(txt):
                shutil.copy2(txt, dest)
    return 1 if (failed or problems) else 0


def is_probe(cls):
    return "probe" in cls.lower()


def cmd_compare(args):
    root = os.path.abspath(args.root)
    jobs = defaultdict(dict)
    for entry in sorted(os.listdir(root)):
        full = os.path.join(root, entry)
        if not os.path.isdir(full) or not entry.startswith("reports-"):
            continue
        job, _, variant = entry[len("reports-"):].rpartition("-")
        table = OrderedDict()
        for module, cls, name, status, detail in load(full):
            key = (module, cls, name)
            old = table.get(key)
            if old is None or RANK[status] > RANK[old[0]]:
                table[key] = (status, detail)
        jobs[job][variant] = table

    def totals(table, probe=False):
        t = defaultdict(int)
        for (module, cls, _name), (status, _d) in table.items():
            if is_probe(cls) != probe:
                continue
            t["tests"] += 1
            t[status] += 1
        t["passed"] = t["tests"] - t["fail"] - t["skip"]
        return t

    out = ["# Fix vs baseline (existing tests only; probes are listed separately)", "",
           "| job | variant | tests | passed | failed | skipped | flaky |", "|---|---|---:|---:|---:|---:|---:|"]
    grand = {"fix": defaultdict(int), "base": defaultdict(int)}
    regressions, both, fixed_by_change, only_in_one = [], [], [], []
    probe_rows = []
    for job in sorted(jobs):
        for variant in ("fix", "base"):
            table = jobs[job].get(variant)
            if table is None:
                out.append("| %s | %s | (no report) | | | | |" % (job, variant))
                continue
            t = totals(table)
            out.append("| %s | %s | %d | %d | %d | %d | %d |" % (job, variant, t["tests"], t["passed"], t["fail"], t["skip"], t["flaky"]))
            for key in ("tests", "passed", "fail", "skip", "flaky"):
                grand[variant][key] += t[key]
            p = totals(table, probe=True)
            if p["tests"]:
                probe_rows.append((job, variant, p))
        fix, base = jobs[job].get("fix"), jobs[job].get("base")
        if fix is None or base is None:
            continue
        for key, (status, detail) in fix.items():
            module, cls, name = key
            other = base.get(key)
            if is_probe(cls):
                continue
            if other is None:
                only_in_one.append((job, "fix only", key))
            if status == "fail" and (other is None or other[0] != "fail"):
                regressions.append((job, key, detail, other[0] if other else "not run"))
            elif status == "fail" and other and other[0] == "fail":
                both.append((job, key, detail))
            elif status != "fail" and other and other[0] == "fail":
                fixed_by_change.append((job, key, other[1]))
        for key in base:
            if key not in fix and not is_probe(key[1]):
                only_in_one.append((job, "base only", key))
    out += ["", "TOTAL fix : tests=%d passed=%d failed=%d skipped=%d flaky=%d" % tuple(grand["fix"][k] for k in ("tests", "passed", "fail", "skip", "flaky")),
            "TOTAL base: tests=%d passed=%d failed=%d skipped=%d flaky=%d" % tuple(grand["base"][k] for k in ("tests", "passed", "fail", "skip", "flaky")),
            "", "## REGRESSIONS (fail with the fix, not failing on the base): %d" % len(regressions)]
    out += ["- [%s] %s :: %s :: %s -- %s (base: %s)" % (job, k[0], k[1], k[2], d, b) for job, k, d, b in regressions]
    out += ["", "## Failing on both (pre-existing / environment): %d" % len(both)]
    out += ["- [%s] %s :: %s :: %s -- %s" % (job, k[0], k[1], k[2], d) for job, k, d in both]
    out += ["", "## Failing on the base only: %d" % len(fixed_by_change)]
    out += ["- [%s] %s :: %s :: %s -- %s" % (job, k[0], k[1], k[2], d) for job, k, d in fixed_by_change]
    out += ["", "## Present in only one variant (should be empty): %d" % len(only_in_one)]
    out += ["- [%s] %s: %s :: %s :: %s" % (job, which, k[0], k[1], k[2]) for job, which, k in only_in_one[:50]]
    out += ["", "## Probes (new, not part of the repo; fix should pass, base is expected to fail)"]
    for job in sorted(jobs):
        for variant in ("fix", "base"):
            table = jobs[job].get(variant) or {}
            for (module, cls, name), (status, detail) in table.items():
                if is_probe(cls):
                    out.append("- [%s/%s] %s :: %s -> %s %s" % (job, variant, cls, name, status.upper(), ("-- " + detail) if detail else ""))
    text = "\n".join(out)
    print(text)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as fh:
            fh.write(text + "\n")
    return 0


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("summarize")
    s.add_argument("--root", default=".")
    s.add_argument("--label", required=True)
    s.add_argument("--expect", default="")
    s.add_argument("--collect", default="")
    s.add_argument("--allow-empty", action="store_true")
    s.set_defaults(func=cmd_summarize)
    c = sub.add_parser("compare")
    c.add_argument("--root", required=True)
    c.set_defaults(func=cmd_compare)
    args = parser.parse_args()
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
