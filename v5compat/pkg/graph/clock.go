package graph

import (
	"bufio"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	v5 "github.com/data-insights-ai/rho-tkg/v5/pkg/graph"
)

// nowMs is the wall clock (a variable so tests can step it back).
var nowMs = wallMs

func wallMs() int64 { return time.Now().UnixMilli() }

// maxSkewMs bounds how far ahead of the wall clock a caller instant may be.
const maxSkewMs = 60 * 60 * 1000

// ledger maps the compat transaction instant (one monotonic millisecond per
// commit, or the caller's stamp on the backfill doors) to the v5 cut of that
// commit. v5 cuts are opaque, so every v4 "pin" is resolved here: the pin reads
// the newest cut whose instant is <= pin. The first entry is the genesis cut at
// instant 0 (the empty graph). A directory graph persists the ledger next to the
// store (v5compat.ledger, one "instant hexcut" line per commit, synced); v5 has
// no metadata door in pkg/graph, so this file is the compat-only home of the
// commit clock. A crash between a v5 commit and its ledger line leaves the commit
// without an entry; the next open adds one at last+1.
type ledger struct {
	mu      sync.Mutex
	entries []ledgerEntry
	max     int64 // newest instant of any entry: CommittedTx
	f       *os.File
}

type ledgerEntry struct {
	tx  int64
	cut v5.Cut
}

const ledgerFile = "v5compat.ledger"

func openLedger(dir string) (*ledger, error) {
	l := &ledger{}
	if dir == "" {
		return l, nil
	}
	path := filepath.Join(dir, ledgerFile)
	if b, err := os.ReadFile(path); err == nil {
		sc := bufio.NewScanner(strings.NewReader(string(b)))
		sc.Buffer(make([]byte, 0, 1<<16), v5.MaxTokenBytes*3)
		for sc.Scan() {
			tx, cutHex, ok := strings.Cut(sc.Text(), " ")
			if !ok {
				break // a torn last line
			}
			n, err := strconv.ParseInt(tx, 10, 64)
			raw, err2 := hex.DecodeString(cutHex)
			if err != nil || err2 != nil {
				break
			}
			cut, err := v5.ParseCut(raw)
			if err != nil {
				break
			}
			l.entries = append(l.entries, ledgerEntry{n, cut})
			l.max = max(l.max, n)
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	l.f = f
	return l, nil
}

func (l *ledger) close() error {
	if l.f == nil {
		return nil
	}
	return l.f.Close()
}

// last is the newest commit instant (0 for the genesis entry or none).
func (l *ledger) last() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.max
}

func (l *ledger) empty() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries) == 0
}

func (l *ledger) lastCut() (v5.Cut, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.entries) == 0 {
		return v5.Cut{}, false
	}
	return l.entries[len(l.entries)-1].cut, true
}

// next is the instant a clock write would take: the wall clock, but strictly
// above every instant committed so far.
func (l *ledger) next() int64 { return max(nowMs(), l.last()+1) }

// cutFor returns the newest cut (by commit order) whose own instant is <= pin.
// Caller stamps may step back across entities; a pin then sees every earlier
// commit too (the cut is a prefix of the commit order), never a later one.
func (l *ledger) cutFor(pin int64) (v5.Cut, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := len(l.entries) - 1; i >= 0; i-- {
		if l.entries[i].tx <= pin {
			return l.entries[i].cut, true
		}
	}
	return v5.Cut{}, false
}

// add records a commit at instant tx as given.
func (l *ledger) add(tx int64, cut v5.Cut) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.f != nil {
		if _, err := fmt.Fprintf(l.f, "%d %x\n", tx, cut.Bytes()); err != nil {
			return err
		}
		if err := l.f.Sync(); err != nil {
			return err
		}
	}
	l.entries = append(l.entries, ledgerEntry{tx, cut})
	l.max = max(l.max, tx)
	return nil
}
