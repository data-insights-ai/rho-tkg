# tkg-v3 — Current Tasks

## Completed: Pre-release review fixes (v3.0.3)

- [x] Step 1: Recursive property value validation (BLOCKER)
- [x] Step 2: Fix reflect.SetMapIndex nil value data loss (BLOCKER)
- [x] Step 3: Fix log spam with sync.Once (MAJOR)
- [x] Step 4: Fill temporal and integrity struct fields (MINOR)
- [x] Step 5: Introduce unexported nodeID/relID types (MAJOR)
- [x] Step 6: Initialize snowflake generators (BLOCKER)
- [x] make ci green, make cover 93.4%, zero 0% public methods

## Next up

- Phase 2 continued: msgpack wire formats (nodeWire/relWire), Badger persistence
- Registry persistence to Badger (meta/label_tokens, meta/reltype_tokens)
