#!/usr/bin/env python3
"""Reads the surefire XML reports of hudi-common and checks the outcome of the id-uniqueness test.

usage: idtest_report.py pass
       idtest_report.py fail <text the assertion message must contain>
       idtest_report.py module
"""
import glob
import os
import sys
import xml.etree.ElementTree as ET

REPORTS = "hudi-common/target/surefire-reports"
CLASS = "org.apache.hudi.common.schema.internal.convert.TestInternalSchemaConverter"
METHOD = "testNonNegativeFieldIdsAreUniqueWithSeveralBlobAndVariantColumns"
OUTCOME_TAGS = ("failure", "error", "skipped", "rerunFailure", "rerunError", "flakyFailure", "flakyError")


def fail(message):
    print("CHECK FAILED: " + message)
    sys.exit(1)


def load(path):
    if not os.path.exists(path):
        fail("no report at " + path)
    return ET.parse(path).getroot()


def cases_of(root):
    found = []
    for case in root.iter("testcase"):
        name = case.get("name", "")
        if name.endswith("()"):
            name = name[:-2]
        if name == METHOD:
            found.append(case)
    return found


def main():
    mode = sys.argv[1]
    root = load(os.path.join(REPORTS, "TEST-" + CLASS + ".xml"))
    cases = cases_of(root)
    if len(cases) != 1:
        fail("expected exactly one testcase named %s in the report of %s, found %d" % (METHOD, CLASS, len(cases)))
    case = cases[0]
    outcomes = [child for child in case if child.tag in OUTCOME_TAGS]
    print("report of %s: tests=%s failures=%s errors=%s skipped=%s" % (
        CLASS, root.get("tests"), root.get("failures"), root.get("errors"), root.get("skipped")))
    print("testcase %s: elements %s" % (case.get("name"), [o.tag for o in outcomes]))

    if mode == "pass":
        if outcomes:
            fail("the new test did not pass cleanly")
        if root.get("tests") != "1":
            fail("expected the method filter to select exactly one test, the report has %s" % root.get("tests"))
        print("OK: %s ran alone and passed" % METHOD)

    elif mode == "fail":
        expected = sys.argv[2]
        failures = [o for o in outcomes if o.tag == "failure"]
        errors = [o for o in outcomes if o.tag == "error"]
        if errors:
            fail("the test ended in an error (exception), not an assertion failure: %s" % errors[0].get("message"))
        if not failures:
            fail("the test did not fail")
        failure = failures[0]
        message = failure.get("message") or ""
        print("FAILURE TYPE: " + str(failure.get("type")))
        print("FAILURE MESSAGE: " + message)
        print("FAILURE STACK (first lines):")
        print("\n".join((failure.text or "").strip().splitlines()[:6]))
        if not str(failure.get("type")).endswith("AssertionFailedError"):
            fail("not an assertion failure: %s" % failure.get("type"))
        if expected not in message:
            fail("the assertion message does not contain %r" % expected)
        if root.get("tests") != "1":
            fail("expected the method filter to select exactly one test, the report has %s" % root.get("tests"))
        print("OK: %s fails with an assertion message containing %r" % (METHOD, expected))

    elif mode == "module":
        totals = {"tests": 0, "failures": 0, "errors": 0, "skipped": 0}
        reports = sorted(glob.glob(os.path.join(REPORTS, "TEST-*.xml")))
        for path in reports:
            suite = ET.parse(path).getroot()
            for key in totals:
                totals[key] += int(suite.get(key, "0"))
        print("hudi-common: %d report files, tests=%d failures=%d errors=%d skipped=%d" % (
            len(reports), totals["tests"], totals["failures"], totals["errors"], totals["skipped"]))
        if totals["failures"] or totals["errors"]:
            fail("the module has failing tests")
        if outcomes:
            fail("the new test did not pass cleanly in the module run")
        print("OK: no failures or errors; %s passed among %d tests" % (METHOD, totals["tests"]))

    else:
        fail("unknown mode " + mode)


if __name__ == "__main__":
    main()
