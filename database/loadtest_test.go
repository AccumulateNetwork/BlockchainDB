package blockchainDB

import (
	"flag"
	"testing"
)

// The load tests in this package build databases measured in tens of
// gigabytes and run for tens of minutes to hours: TestBuildBig alone
// writes 200 million keys.  They are opt-in, so that `go test ./...`
// stays a suite someone can actually wait for.
//
// A flag rather than an environment variable or a config file, because
// a misspelled flag is a hard error before any test runs -- `go test
// -lod ./database/` exits 2 -- while a misspelled variable or config
// key silently does not opt in, and the run looks like a pass of tests
// that never executed.
var runLoadTests = flag.Bool("load", false, "run the multi-GB load tests (hours; tens of GB in TMPDIR)")

// skipUnlessLoad
// Skip a load test unless -load was passed.  The skip message names
// the flag, so the suite's own output says how to run what it skipped.
func skipUnlessLoad(t *testing.T) {
	t.Helper()
	if !*runLoadTests {
		t.Skip("load test; pass -load to run (hours; tens of GB in TMPDIR)")
	}
}
