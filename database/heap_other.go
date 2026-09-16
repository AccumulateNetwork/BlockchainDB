//go:build !linux

package blockchainDB

import "os"

// punchHole is a no-op where the filesystem cannot release a range;
// the cleaned head then costs disk until the file is rewritten.
func punchHole(*os.File, int64, int64) error { return nil }
