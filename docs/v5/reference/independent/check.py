#!/usr/bin/env python3
"""Validate the independent reference packages from any working directory."""
import argparse
import hashlib
import json
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parent


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def run(folder, args, expected=0):
    result = subprocess.run([sys.executable, '-B', *args], cwd=folder,
                            capture_output=True, text=True)
    if result.returncode != expected:
        raise AssertionError((args, expected, result.returncode, result.stdout, result.stderr))
    return result


def validate():
    checks = []
    counts = {}
    for package, expected in [('temporal', 20), ('protocol', 18)]:
        result = run(ROOT / package, ['-m', 'unittest', '-v'])
        matched = re.search(r'Ran (\d+) tests', result.stderr)
        assert matched and int(matched[1]) == expected, result.stderr
        checks.append({'cwd': package, 'command': ['python3', '-B', '-m', 'unittest', '-v'],
                       'exit': result.returncode, 'expected_exit': 0, 'tests': expected, 'result': 'passed'})
        counts[package + '_unit_tests'] = expected
    result = run(ROOT / 'temporal', ['generate_corpus.py', '--check'])
    counts.update(json.loads(result.stdout))
    counts['corpus_revision'] = json.loads((ROOT / 'temporal/revised-acceptance-v1.json').read_text())['corpus_revision']
    checks.append({'cwd': 'temporal', 'command': ['python3', '-B', 'generate_corpus.py', '--check'],
                   'exit': 0, 'expected_exit': 0, 'result': 'fresh'})
    # Generate twice in isolated directories; never overwrite the reviewed corpus.
    generated_count = 0
    for package, generator in [('temporal', 'generate_corpus.py'), ('protocol', 'corpus.py')]:
        with tempfile.TemporaryDirectory() as scratch:
            folder = Path(scratch) / 'independent' / package
            folder.mkdir(parents=True)
            for source in (ROOT / package).glob('*.py'):
                shutil.copyfile(source, folder / source.name)
            shutil.copyfile(ROOT.parent / 'temporal-fixtures.json', Path(scratch) / 'temporal-fixtures.json')
            run(folder, [generator])
            first = {str(p.relative_to(folder)): p.read_bytes() for p in folder.rglob('*.json')}
            run(folder, [generator])
            second = {str(p.relative_to(folder)): p.read_bytes() for p in folder.rglob('*.json')}
            expected_files = {str(p.relative_to(ROOT / package)): p.read_bytes()
                              for p in (ROOT / package).rglob('*.json')}
            assert first == second == expected_files, 'stale or nondeterministic ' + package
            generated_count += len(first)
        checks.append({'cwd': package, 'command': ['python3', '-B', generator],
                       'exit': 0, 'expected_exit': 0, 'result': 'two isolated generations byte-identical to corpus',
                       'generated_files': len(first)})
    reports = json.loads((ROOT / 'protocol/results.json').read_text())
    counts.update({'interval_pairs': {'Q': 4096, 'Z': 4096, 'QN': 10000, 'total': 18192},
                   'generated_component_mutations': 600,
                   'protocol_positive_named_histories': reports['positive_named_histories'],
                   'protocol_positive_interleavings': reports['positive_interleavings'],
                   'protocol_expected_refusals': reports['weakened_variants'],
                   'generated_json_files': generated_count})
    assert reports['all_expected_outcomes_matched']
    for report in reports['reports']:
        history = 'histories/' + report['name'] + '.json'
        expected = 0 if report['expected_accept'] else 1
        result = run(ROOT / 'protocol', ['oracle.py', history], expected)
        output = json.loads(result.stdout)
        assert output == report['result'], history
        required = report['required_counterexample']
        assert not required or required in {v['code'] for v in output['violations']}, history
        checks.append({'cwd': 'protocol', 'command': ['python3', '-B', 'oracle.py', history],
                       'exit': result.returncode, 'expected_exit': expected,
                       'required_counterexample': required, 'result': 'expected outcome matched'})
    files = sorted(p for p in ROOT.rglob('*') if p.is_file() and p.name != 'validation.json')
    for path in files:
        content = path.read_text()
        assert not re.search(r'/(?:Users|home|private/var|tmp)/', content), path.relative_to(ROOT)
    return {'schema_version': 1, 'python_version': platform.python_version(),
            'scope': 'independent finite mathematical/declarative reference validation; no production, performance, process-kill or full V0/V1/V2 claims',
            'source_contract_sha256': {'../../PLAN.md': sha(ROOT.parent.parent / 'PLAN.md'),
                                       '../temporal-fixtures.json': sha(ROOT.parent / 'temporal-fixtures.json'),
                                       '../substrate-evaluation.json': sha(ROOT.parent / 'substrate-evaluation.json')},
            'files_sha256': {str(p.relative_to(ROOT)): sha(p) for p in files},
            'counts': counts, 'checks': checks,
            'private_path_scrub': 'all text artifacts checked; no developer absolute paths or recovery-directory references',
            'all_expected_outcomes_matched': True}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--write-validation', action='store_true', help='refresh portable validation.json after reviewing changes')
    args = parser.parse_args()
    report = validate()
    target = ROOT / 'validation.json'
    if args.write_validation:
        target.write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
    else:
        recorded = json.loads(target.read_text())
        recorded.pop('python_version')
        comparable = dict(report)
        comparable.pop('python_version')
        assert comparable == recorded, 'validation.json is stale; review changes before refreshing'
    print(json.dumps({'python_version': report['python_version'], 'counts': report['counts'],
                      'all_expected_outcomes_matched': True, 'validation': 'written' if args.write_validation else 'fresh'}, indent=2, sort_keys=True))


if __name__ == '__main__':
    main()
