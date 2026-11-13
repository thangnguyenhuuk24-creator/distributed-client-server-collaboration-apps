"""Compact PASS/FAIL output for individually run step test modules."""

import os
import sys
import unittest


class StepTestResult(unittest.TextTestResult):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.passed = 0
        self.current_module = None

    def startTest(self, test):
        module = test.__class__.__module__.rsplit(".", 1)[-1]
        if module != self.current_module:
            self.stream.writeln(f"\n[{module}]")
            self.current_module = module
        super().startTest(test)

    @staticmethod
    def _label(test):
        name = test.id().rsplit(".", 1)[-1]
        parts = name.split("_")
        return "_".join(parts[:2]) if len(parts) >= 2 else name

    def addSuccess(self, test):
        super().addSuccess(test)
        self.passed += 1
        self.stream.writeln(f"{self._label(test)} ... PASS")

    def addFailure(self, test, err):
        super().addFailure(test, err)
        self.stream.writeln(f"{self._label(test)} ... FAIL")

    def addError(self, test, err):
        super().addError(test, err)
        self.stream.writeln(f"{self._label(test)} ... FAIL")

    def addSkip(self, test, reason):
        super().addSkip(test, reason)
        self.stream.writeln(f"{self._label(test)} ... SKIP")


class StepTestRunner(unittest.TextTestRunner):
    def __init__(self):
        super().__init__(
            stream=sys.stdout,
            descriptions=False,
            verbosity=0,
            resultclass=StepTestResult,
        )

    def run(self, test):
        result = self._makeResult()
        result.startTestRun()
        try:
            test(result)
        finally:
            result.stopTestRun()
        if not result.wasSuccessful():
            result.printErrors()
        self.stream.writeln(
            f"Result: {result.passed}/{result.testsRun} tests passed"
        )
        return result


def run_test_module(module_name):
    module = sys.modules[module_name]
    suite = unittest.defaultTestLoader.loadTestsFromModule(module)
    result = StepTestRunner().run(suite)
    if not result.wasSuccessful():
        raise SystemExit(1)


def run_step_tests(module_name):
    run_test_module(module_name)


def run_all_tests():
    test_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(test_dir)
    suite = unittest.defaultTestLoader.discover(
        start_dir=test_dir,
        pattern="test*.py",
        top_level_dir=project_dir,
    )
    result = StepTestRunner().run(suite)
    if not result.wasSuccessful():
        raise SystemExit(1)
