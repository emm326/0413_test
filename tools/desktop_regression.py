"""Run desktop regressions and save a clearly marked simulation-only report."""
import json
import sys
import time
import unittest
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))


def main():
    suite=unittest.defaultTestLoader.discover(str(ROOT/'tests'))
    start=time.monotonic()
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    report={'mode':'software_and_simulation_only','tests_run':result.testsRun,
            'failures':len(result.failures),'errors':len(result.errors),
            'passed':result.wasSuccessful(),'elapsed_s':round(time.monotonic()-start,3),
            'generated_utc':time.time(),'node_hardware_verified':False,'real_gateway_verified':False}
    target=ROOT/'tmp/e870_desktop_build/software_test_report.json'
    target.parent.mkdir(parents=True,exist_ok=True)
    target.write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    print('Report:',target)
    return 0 if result.wasSuccessful() else 1


if __name__=='__main__':
    raise SystemExit(main())
