package core

import (
	"sync"
	"time"

	snowflake "github.com/bds421/rho-snowflake-2026"
)

// Layout of a minted ID: 48 bits of microseconds since the epoch, a 5-bit slot, a 10-bit step.
const (
	idNodeShift = 10
	idTimeShift = 15
	idStepMask  = 1<<10 - 1
)

// idMinter mints the IDs of one slot, from the wall clock or from Config.IDClock.
type idMinter struct {
	wall  *snowflake.Node
	clock func() time.Time
	slot  int64
	mu    sync.Mutex
	last  int64
	step  int64
}

func newIDMinter(slot int64, clock func() time.Time) (*idMinter, error) {
	if clock != nil {
		return &idMinter{clock: clock, slot: slot, last: -1}, nil
	}
	gen, err := snowflake.NewNode(slot,
		snowflake.WithEpoch(snowflakeEpoch),
		snowflake.WithMicroseconds(),
		snowflake.WithNodeBits(5),
		snowflake.WithStepBits(10),
	)
	if err != nil {
		return nil, err
	}
	return &idMinter{wall: gen, slot: slot}, nil
}

// Generate returns a new ID above every earlier one of this slot; under a clock that
// stands still it advances one microsecond per 1024 IDs instead of waiting.
func (m *idMinter) Generate() snowflake.ID {
	if m.wall != nil {
		return m.wall.Generate()
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	now := m.clock().Sub(snowflakeEpoch).Microseconds()
	if now > m.last {
		m.step = 0
	} else {
		now = m.last
		m.step = (m.step + 1) & idStepMask
		if m.step == 0 {
			now++
		}
	}
	m.last = now
	return snowflake.ID(now<<idTimeShift | m.slot<<idNodeShift | m.step)
}
