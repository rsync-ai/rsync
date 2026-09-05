package kafkaclient

import (
	"log"
	"sync"
)

// Warnings() returns strings for the caller to log, which works right up until
// a caller does not log them. Turning off broker certificate verification is
// the one setting where that is not acceptable, so this package also says it
// itself, through a logger the service can redirect but not silence.
var (
	warnMu     sync.Mutex
	warnLogf   = log.Printf
	warnedOnce = map[string]bool{}
)

// SetWarnLogger routes this package's own warnings into the service's logger.
//
// Call it once during startup, before the first FromEnv. A nil function is
// ignored: there is no supported way to turn these off, because every one of
// them describes a configuration that is quietly less safe than it looks.
func SetWarnLogger(f func(format string, args ...any)) {
	if f == nil {
		return
	}
	warnMu.Lock()
	defer warnMu.Unlock()
	warnLogf = f
}

// warnOnce emits a warning the first time it is reached for a given key.
//
// FromEnv is called per client in some services and inside a reconnect path in
// others, so an unconditional log would turn a security warning into scroll and
// bury the thing it is warning about. Once per key is loud enough to be seen and
// quiet enough to stay seen.
func warnOnce(key, format string, args ...any) {
	warnMu.Lock()
	if warnedOnce[key] {
		warnMu.Unlock()
		return
	}
	warnedOnce[key] = true
	f := warnLogf
	warnMu.Unlock()
	f(format, args...)
}

// resetWarnOnce lets a test observe a warning that another test already
// consumed. Unexported: production code has no reason to re-arm a warning.
func resetWarnOnce() {
	warnMu.Lock()
	defer warnMu.Unlock()
	warnedOnce = map[string]bool{}
}
