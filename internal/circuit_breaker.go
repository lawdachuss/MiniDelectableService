package internal

import (
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// State represents the circuit breaker state.
type State int32

const (
	StateClosed   State = iota // Normal operation
	StateOpen                  // Failing — reject requests
	StateHalfOpen              // Testing if recovered
)

// CircuitBreaker protects Chaturbate API calls from cascading failure.
// When the error rate exceeds the threshold, it breaks the circuit and
// rejects all requests for a cooldown period, giving the upstream time
// to recover.
type CircuitBreaker struct {
	state           atomic.Int32
	failures        atomic.Int64
	successes       atomic.Int64
	lastStateChange atomic.Int64 // unix nanos

	threshold     float64       // error ratio that triggers open (e.g. 0.2 = 20%)
	minSamples    int64         // minimum requests before evaluating
	cooldown      time.Duration // base time to stay open before half-open
	maxCooldown   time.Duration // ceiling for escalated cooldown
	halfOpenMax   int64         // max half-open probes before re-evaluating
	halfOpenCount atomic.Int64

	// curCooldown is the current (possibly escalated) cooldown in nanoseconds.
	// It starts at cooldown and doubles on each consecutive reopen until it
	// reaches maxCooldown, so a sustained upstream outage (e.g. a Cloudflare
	// block) backs off progressively instead of probing every base interval.
	curCooldown atomic.Int64

	mu sync.Mutex
}

// BreakerConfig configures the circuit breaker.
type BreakerConfig struct {
	Threshold   float64       // error ratio threshold (default 0.2)
	MinSamples  int64         // min requests before evaluating (default 20)
	Cooldown    time.Duration // base time to stay open (default 15s)
	MaxCooldown time.Duration // escalated cooldown ceiling (default 5m)
	HalfOpenMax int64         // max half-open probes (default 3)
}

// envFloat parses a float from the environment with a fallback.
func envFloat(name string, fallback float64) float64 {
	v := os.Getenv(name)
	if v == "" {
		return fallback
	}
	parsed, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return fallback
	}
	return parsed
}

// DefaultBreakerConfig returns defaults for Chaturbate API calls, tunable via
// CHATURBATE_BREAKER_* env vars. The base cooldown is short so brief blips
// recover fast; repeated opens escalate toward maxCooldown so a real upstream
// block (invalid cf_clearance, Cloudflare challenge) backs the whole fleet off
// instead of re-probing every few seconds.
func DefaultBreakerConfig() BreakerConfig {
	cfg := BreakerConfig{
		Threshold:   envFloat("CHATURBATE_BREAKER_THRESHOLD", 0.2),
		MinSamples:  int64(envInt("CHATURBATE_BREAKER_MIN_SAMPLES", 20)),
		Cooldown:    time.Duration(envInt("CHATURBATE_BREAKER_COOLDOWN_MS", 15000)) * time.Millisecond,
		MaxCooldown: time.Duration(envInt("CHATURBATE_BREAKER_MAX_COOLDOWN_MS", 300000)) * time.Millisecond,
		HalfOpenMax: int64(envInt("CHATURBATE_BREAKER_HALF_OPEN_MAX", 3)),
	}
	if cfg.Cooldown < time.Second {
		cfg.Cooldown = time.Second
	}
	if cfg.MaxCooldown < cfg.Cooldown {
		cfg.MaxCooldown = cfg.Cooldown
	}
	if cfg.HalfOpenMax < 1 {
		cfg.HalfOpenMax = 1
	}
	return cfg
}

// NewCircuitBreaker creates a circuit breaker with the given config.
func NewCircuitBreaker(cfg BreakerConfig) *CircuitBreaker {
	cb := &CircuitBreaker{
		threshold:   cfg.Threshold,
		minSamples:  cfg.MinSamples,
		cooldown:    cfg.Cooldown,
		maxCooldown: cfg.MaxCooldown,
		halfOpenMax: cfg.HalfOpenMax,
	}
	if cb.maxCooldown < cb.cooldown {
		cb.maxCooldown = cb.cooldown
	}
	cb.curCooldown.Store(int64(cb.cooldown))
	return cb
}

// Allow returns true if the request should proceed.
func (cb *CircuitBreaker) Allow() bool {
	state := State(cb.state.Load())
	switch state {
	case StateClosed:
		return true
	case StateOpen:
		// Check if the (possibly escalated) cooldown elapsed → half-open
		changed := cb.lastStateChange.Load()
		if time.Since(time.Unix(0, changed)) >= cb.Cooldown() {
			if cb.state.CompareAndSwap(int32(StateOpen), int32(StateHalfOpen)) {
				cb.halfOpenCount.Store(0)
				return true
			}
		}
		return false
	case StateHalfOpen:
		// Allow up to halfOpenMax probes
		count := cb.halfOpenCount.Add(1)
		if count <= cb.halfOpenMax {
			return true
		}
		return false
	default:
		return true
	}
}

// Cooldown returns the current cooldown, which grows exponentially across
// consecutive reopens (up to maxCooldown) and resets after a successful probe.
func (cb *CircuitBreaker) Cooldown() time.Duration {
	cur := cb.curCooldown.Load()
	if cur <= 0 {
		return cb.cooldown
	}
	return time.Duration(cur)
}

// escalate doubles the cooldown for the next open cycle, up to maxCooldown.
func (cb *CircuitBreaker) escalate() {
	next := cb.Cooldown() * 2
	if next > cb.maxCooldown {
		next = cb.maxCooldown
	}
	cb.curCooldown.Store(int64(next))
}

// Success records a successful request.
func (cb *CircuitBreaker) Success() {
	state := State(cb.state.Load())
	if state == StateHalfOpen {
		// Single success in half-open → close the circuit
		cb.state.Store(int32(StateClosed))
		cb.halfOpenCount.Store(0)
		cb.curCooldown.Store(int64(cb.cooldown)) // reset escalation
		cb.resetCounters()
		return
	}
	cb.successes.Add(1)
	cb.evaluate()
}

// Failure records a failed request.
func (cb *CircuitBreaker) Failure() {
	state := State(cb.state.Load())
	if state == StateHalfOpen {
		// Failure in half-open → back to open, escalate the next cooldown
		cb.state.Store(int32(StateOpen))
		cb.escalate()
		cb.lastStateChange.Store(time.Now().UnixNano())
		cb.halfOpenCount.Store(0)
		return
	}
	cb.failures.Add(1)
	cb.evaluate()
}

// evaluate checks if the error rate exceeds the threshold and opens if so.
func (cb *CircuitBreaker) evaluate() {
	fail := cb.failures.Load()
	succ := cb.successes.Load()
	total := fail + succ

	if total < cb.minSamples {
		return
	}

	rate := float64(fail) / float64(total)
	if rate >= cb.threshold {
		if cb.state.CompareAndSwap(int32(StateClosed), int32(StateOpen)) {
			cb.curCooldown.Store(int64(cb.cooldown)) // fresh backoff on first open
			cb.lastStateChange.Store(time.Now().UnixNano())
		}
	}
}

func (cb *CircuitBreaker) resetCounters() {
	cb.failures.Store(0)
	cb.successes.Store(0)
}

// CircuitBreakerCooldown reports the global breaker's current cooldown, so
// callers can pace retries instead of spinning against an open circuit.
func CircuitBreakerCooldown() time.Duration {
	return chaturbateBreaker.Cooldown()
}

// chaturbateBreaker is the global circuit breaker for Chaturbate API calls.
var chaturbateBreaker = NewCircuitBreaker(DefaultBreakerConfig())
