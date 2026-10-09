#!/usr/bin/env python3
"""Read-only portable entity-declaration review-data validation."""
from pathlib import Path
import hashlib
import json

ROOT = Path(__file__).resolve().parent


def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def content_sha(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def main():
    validation = json.loads((ROOT / 'entity-declarations-validation.json').read_text())
    for name, expected in validation['unchanged_patch_sha256'].items():
        assert sha(ROOT / name) == expected, name
    baseline = validation['baseline']
    assert baseline['stock_source_files'] == len(baseline['files_sha256']) == 102
    assert baseline['all_102_files_match_git_blobs']
    assert baseline['all_102_blob_hashes_rechecked_during_packaging']
    assert not baseline['uncommitted_overlay_applied']
    assert not any(Path(name).name.startswith('coverage.') for name in baseline['files_sha256'])
    assert validation['canonical_absent_field_red']['failed_leaf_count'] == 3
    assert validation['dto_only_sensitivity_red']['failed_leaf_count'] == 44
    extraction = json.loads((ROOT / 'full-goldens.json').read_text())
    canonical = (ROOT / extraction['source']).resolve()
    assert sha(canonical) == extraction['source_sha256']
    source = {case['id']: case for case in json.loads(canonical.read_text())['cases']}
    extracted = {case['id']: case for case in extraction['cases']}
    expected = validation['three_full_goldens']['whole_record_content_sha256']
    assert len(extracted) == 3 and extracted.keys() == expected.keys()
    for name, value in extracted.items():
        assert value == source[name]
        assert content_sha(value) == expected[name]
    lifecycle_path = ROOT / 'declaration-lifecycle-v1.json'
    assert sha(lifecycle_path) == validation['lifecycle']['fixture_sha256']
    cases = json.loads(lifecycle_path.read_text())['cases']
    assert sum(len(case['reads']) for case in cases) == validation['lifecycle']['complete_answers'] == 32
    assert validation['historical_full_42_ledger']['not_current_owner_api_status']
    assert validation['historical_full_42_ledger']['counts'] == {
        'supported/pass': 25, 'missing_required_integration': 13,
        'model_only_policy_or_declarative_obligation': 4}
    assert validation['parent_independent_focused_race']['exit_code'] == 0
    assert not validation['canonical_go_patch_applied']
    assert not validation['whole_V1_or_production_graph_acceptance']
    print(json.dumps({'patches': 'unchanged', 'baseline_files': 102,
                      'full_goldens': 3, 'dto_only_negative_leaf_checks': 44,
                      'lifecycle_answers': 32, 'source_status': 'historical candidate',
                      'canonical_go_applied': False, 'whole_V1_claim': False}, indent=2))


if __name__ == '__main__':
    main()
