# E17 evidence packaging

This packaging revision changes storage only. `FINAL_INDEX.md`, `FINAL_RECEIPT.json`, the historical checkpoint files, and their original hash seal retain their exact bytes and claims. Raw artifacts referenced by those files are stored under their original relative names inside `E17_RAW_EVIDENCE.tar.gz`.

The archive contains 338 original artifacts (44,447,588 bytes), plus an internal copy of `ARCHIVE_PATH_HASHES.json`. Its compressed size is 2,770,968 bytes; SHA-256 is `e30e8d43016dd0a2d22060749a98b7b6c8712822206bae5d2c95e8ef9f6b90dd`. The archive was built twice with identical bytes: sorted USTAR entries, zero timestamps/owners, mode 0644, and gzip timestamp zero with no filename.

From the repository root, extract into a new directory and overlay the directly readable artifacts to reconstruct every path referenced by the original seal:

```sh
evidence=tasks/evidence/v2-range-e17
extracted=$(mktemp -d)
tar -xzf "$evidence/E17_RAW_EVIDENCE.tar.gz" -C "$extracted"
python3 - "$evidence" "$extracted" <<'PYVERIFY'
import hashlib, json, pathlib, shutil, sys
source, output = map(pathlib.Path, sys.argv[1:])
archive = source / 'E17_RAW_EVIDENCE.tar.gz'
assert hashlib.sha256(archive.read_bytes()).hexdigest() == 'e30e8d43016dd0a2d22060749a98b7b6c8712822206bae5d2c95e8ef9f6b90dd'
manifest = json.loads((source / 'ARCHIVE_PATH_HASHES.json').read_text())
assert (output / 'ARCHIVE_PATH_HASHES.json').read_bytes() == (source / 'ARCHIVE_PATH_HASHES.json').read_bytes()
for name, expected in manifest.items():
    data = (output / name).read_bytes()
    assert len(data) == expected['bytes'], name
    assert hashlib.sha256(data).hexdigest() == expected['sha256'], name
seal = json.loads((source / 'FINAL_EVIDENCE_SEAL.json').read_text())
for name, expected in seal.items():
    if name not in manifest:
        shutil.copyfile(source / name, output / name)
    assert hashlib.sha256((output / name).read_bytes()).hexdigest() == expected, name
shutil.copyfile(source / 'FINAL_EVIDENCE_SEAL.json', output / 'FINAL_EVIDENCE_SEAL.json')
print(f'Verified {len(manifest)} archived artifacts and all {len(seal)} original selected hashes in {output}')
PYVERIFY
```

`ARCHIVE_PATH_HASHES.json` maps each archived path to its original size and hash. `PACKAGING_RECEIPT.json` records packaging provenance. The original uncompressed commit `f470dbf69a7aee7f22e3b371871ec40c1ef75d06` remains locally at `refs/archive/e17-evidence-uncompressed`; it is not needed to extract or verify this package. No runtime, source, test, or acceptance changes accompany this revision.
