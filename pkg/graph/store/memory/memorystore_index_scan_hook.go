package memory

// phase2ScanHook is invoked once per scanned entity inside the three-phase index
// builders, at the exact point BETWEEN releasing one per-row read lock and taking
// the next — so ms.mu is provably unheld when it runs.
//
// It exists so a test can observe the property that BACKLOG 17h guards ("Phase 2
// never holds the lock across the scan") DETERMINISTICALLY. The previous test raced
// a goroutine polling TryLock and required it to win at least 100 times, which
// measures the SCHEDULER: on a loaded CI runner the poller was starved to zero
// successes and the test failed while the code was correct. Running the probe on
// the scanning goroutine itself removes every race — a TryLock here MUST succeed if
// and only if the lock really was released.
//
// nil in production, so the call is a predictable never-taken branch. Set only from
// tests, which must restore it (t.Cleanup) and must not run in parallel with each
// other while it is set.
var phase2ScanHook func()

// phase2Yield calls the hook if a test installed one.
func phase2Yield() {
	if phase2ScanHook != nil {
		phase2ScanHook()
	}
}
