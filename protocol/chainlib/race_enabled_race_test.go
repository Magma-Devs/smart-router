//go:build race

package chainlib

// raceEnabled reports whether the tests run under the race detector, where testing.AllocsPerRun,
// which counts every allocation in the process, does not read a stable count.
const raceEnabled = true
