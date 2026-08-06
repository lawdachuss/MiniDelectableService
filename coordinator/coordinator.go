package coordinator

import (
	"context"
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/teacat/chaturbate-dvr/database"
	"github.com/teacat/chaturbate-dvr/entity"
	"github.com/teacat/chaturbate-dvr/server"
)

// cycleGuard prevents overlapping coordinator cycles. When the DB is slow,
// a cycle (e.g. reaper running every 120s) could still be in progress when
// the next tick fires, launching a second concurrent cycle that increases
// DB load and makes things worse. cycleGuard skips a cycle tick if the
// previous one hasn't finished, acting as a circuit breaker.
//
// IMPORTANT: cycleGuard only prevents concurrent EXECUTION of the same
// cycle. It does NOT bound the duration of a single cycle — the database
// client's 60s HTTP timeout handles that at a finer granularity, and the
// health check warns if a cycle stops running entirely.
type cycleGuard struct {
	mu        sync.Mutex
	running   bool
	lastRun   time.Time
	lastRunMu sync.Mutex
}

// tryRun attempts to execute fn under the cycle guard, preventing overlapping
// runs of the same cycle. Returns true if the function was executed, false if
// a previous run is still in progress (the cycle tick is silently skipped).
//
// Panic recovery: any panic from fn is caught, logged, and the guard is
// released so the next tick can proceed. Without this recovery, a single bad
// cycle would crash the entire node, dropping every recording and stopping
// all coordinator loops.
func (g *cycleGuard) tryRun(name string, fn func()) bool {
	g.mu.Lock()
	if g.running {
		g.mu.Unlock()
		return false // skip silently — the ticker will retry
	}
	g.running = true
	g.mu.Unlock()

	defer func() {
		if r := recover(); r != nil {
			log.Printf("[coordinator] %s cycle panicked (recovered): %v", name, r)
		}
		g.mu.Lock()
		g.running = false
		g.mu.Unlock()
		g.lastRunMu.Lock()
		g.lastRun = time.Now()
		g.lastRunMu.Unlock()
	}()

	fn()
	return true
}

// lastRunTime returns the last execution time of this guard.
func (g *cycleGuard) lastRunTime() time.Time {
	g.lastRunMu.Lock()
	defer g.lastRunMu.Unlock()
	return g.lastRun
}

// runLoopWithRestart runs a background loop inside a goroutine with automatic
// restart if the loop exits unexpectedly. This is the core watchdog mechanism:
// if a coordinator loop goroutine crashes (unrecovered panic, unexpected return
// from the select, etc.), it is automatically restarted after a 5-second delay
// instead of silently dying and leaving the node without coordinator cycles.
//
// The loopFn receives the ticker channel and should block on it, returning only
// when the context is done or stop is received. If loopFn returns for any other
// reason, it is restarted.
//
// The loopFn sub-goroutine is deliberately NOT tracked by c.wg: the outer
// wrapper waits on loopDone before returning, so Stop() still blocks until an
// in-flight cycle finishes (bounded by the database client's 60s HTTP timeout).
func (c *Coordinator) runLoopWithRestart(ctx context.Context, name string, interval time.Duration, loopFn func(stopCh <-chan struct{}, tickerC <-chan time.Time)) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		for {
			ticker := time.NewTicker(interval)
			loopDone := make(chan struct{}, 1)

			go func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[coordinator] %s loop panicked (will restart): %v", name, r)
					}
					close(loopDone)
				}()
				loopFn(c.stopCh, ticker.C)
			}()

			select {
			case <-ctx.Done():
				ticker.Stop()
				<-loopDone // wait for the in-flight cycle to wind down before exiting
				return
			case <-c.stopCh:
				ticker.Stop()
				<-loopDone // graceful: Stop() blocks until the in-flight cycle finishes
				return
			case <-loopDone:
				// Loop exited unexpectedly — restart after delay, unless we should
				// actually stop first.
				select {
				case <-ctx.Done():
					return
				case <-c.stopCh:
					return
				default:
				}
				log.Printf("[coordinator] %s loop exited unexpectedly — restarting in 5s", name)
				ticker.Stop()
				select {
				case <-time.After(5 * time.Second):
				case <-ctx.Done():
					return
				case <-c.stopCh:
					return
				}
				continue // restart the loop
			}
		}
	}()
}

// maxCycleStall is how long a coordinator cycle may go without running before
// the health check warns.  All cycles run on multi-minute tickers, so a stall
// of 5 minutes means the loop is stuck or dead.
const maxCycleStall = 5 * time.Minute

// startHealthCheckLoop runs periodically and logs warnings if any coordinator
// cycle has not run recently (indicating a stuck or dead cycle). This provides
// observability into the health of all background cycles.
func (c *Coordinator) startHealthCheckLoop(ctx context.Context) {
	c.wg.Add(1)
	go func() {
		defer c.wg.Done()

		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-c.stopCh:
				return
		case <-ticker.C:
			c.checkCycleHealth("claim", &c.cycleGuardClaim)
			c.checkCycleHealth("live-check", &c.cycleGuardLiveCheck)
			c.checkCycleHealth("reaper", &c.cycleGuardReaper)
			c.checkCycleHealth("offline-shuffle", &c.cycleGuardShuffle)
			c.checkCycleHealth("deadline-migration", &c.cycleGuardDeadline)
			c.checkCycleHealth("reconcile", &c.cycleGuardReconcile)
		}
		}
	}()
}

// checkCycleHealth logs a warning if the given cycle has not run within
// maxCycleStall.  Guards that have never run (zero timestamp) are skipped so
// startup produces no noise.
func (c *Coordinator) checkCycleHealth(name string, guard *cycleGuard) {
	last := guard.lastRunTime()
	if !last.IsZero() && time.Since(last) > maxCycleStall {
		log.Printf("[coordinator] HEALTH WARNING: %s cycle last ran %v ago (> %v)", name, time.Since(last).Round(time.Second), maxCycleStall)
	}
}

// detectTailscaleIP attempts to get the Tailscale IPv4 address of this node.
func detectTailscaleIP() string {
	var cmd *exec.Cmd
	switch runtime.GOOS {
	case "windows":
		cmd = exec.Command("tailscale.exe", "ip", "-4")
	default:
		cmd = exec.Command("tailscale", "ip", "-4")
	}
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	ip := strings.TrimSpace(string(out))
	if ip == "" || !strings.Contains(ip, ".") {
		return ""
	}
	return ip
}

// ChannelManager is the interface the coordinator uses to create/release channels.
// Implemented by manager.Manager in pooled mode.
type ChannelManager interface {
	CreateChannelFromAssignment(ca *database.ChannelAssignment) error
	RemoveChannelForReassignment(username string) error
	GetLocalChannels() []string
}

// LivenessChecker is the interface for checking if a channel is currently live.
// Implemented by main.go wiring using the site adapters.
type LivenessChecker interface {
	IsLive(ctx context.Context, siteName, username string) bool
}

// Coordinator manages the distributed node lifecycle: registration, heartbeat,
// channel claiming, liveness checking, and orphan reclamation.
type Coordinator struct {
	NodeID    string
	Mode      string
	Client    *database.Client
	Manager   ChannelManager
	LiveCheck LivenessChecker

	stopCh   chan struct{}
	wg       sync.WaitGroup
	started  bool
	draining bool // set during graceful shutdown; prevents heartbeat from clobbering status
	fenced   bool // set when DB is unreachable; stops local recording to avoid duplicate capture
	mu       sync.Mutex

	// cycle guards prevent overlapping background operations when the DB
	// is slow — a cycle that hasn't finished when the next tick fires will
	// skip its tick instead of launching a concurrent cycle that piles on
	// more DB load.  lastRun timestamps feed the health-check watchdog.
	cycleGuardClaim         cycleGuard
	cycleGuardLiveCheck     cycleGuard
	cycleGuardReaper        cycleGuard
	cycleGuardShuffle       cycleGuard
	cycleGuardDeadline      cycleGuard
	cycleGuardReconcile     cycleGuard
}

// New creates a new Coordinator. If CHANNEL_POOL_MODE=pooled, Start() must
// be called to begin background goroutines.
func New(client *database.Client, mgr ChannelManager) *Coordinator {
	return &Coordinator{
		NodeID:  detectNodeID(),
		Mode:    channelPoolMode(),
		Client:  client,
		Manager: mgr,
		stopCh:  make(chan struct{}),
	}
}

func (c *Coordinator) IsPooled() bool { return c.Mode == entity.PoolModePooled }

// Start begins all background goroutines: heartbeat, claim, live check, reaper.
// Only starts them if mode is "pooled".
func (c *Coordinator) Start(ctx context.Context) {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return
	}
	c.started = true
	c.mu.Unlock()

	if !c.IsPooled() {
		return
	}

	log.Printf("[coordinator] starting node %q in pooled mode", c.NodeID)
	c.Register()
	c.StartHeartbeatLoop(ctx)
	c.StartClaimLoop(ctx)
	c.StartLiveCheckLoop(ctx)
	c.StartReaperLoop(ctx)
	c.StartOfflineShuffleLoop(ctx)
	c.StartDeadlineMigrationLoop(ctx)
	c.StartReconcileLoop(ctx)

	// Start the health check watchdog that detects stalled cycles
	c.startHealthCheckLoop(ctx)
}

// Stop gracefully shuts down all coordinator loops and deregisters the node.
func (c *Coordinator) Stop() {
	if !c.IsPooled() {
		return
	}

	// Guard against double-close panic — Stop() is safe to call multiple times.
	c.mu.Lock()
	select {
	case <-c.stopCh:
		c.mu.Unlock()
		return // already closed
	default:
		close(c.stopCh)
	}
	c.mu.Unlock()

	log.Printf("[coordinator] stopping node %q", c.NodeID)

	// Wait for goroutines to finish
	c.wg.Wait()

	if c.Client == nil {
		return
	}

	// Release all channel assignments
	if err := c.Client.ReleaseNodeChannels(c.NodeID); err != nil {
		log.Printf("[coordinator] error releasing channels: %v", err)
	}

	// Mark node as offline
	if err := c.Client.UpdateNodeStatus(c.NodeID, "offline"); err != nil {
		log.Printf("[coordinator] error deregistering node: %v", err)
	}

	// Zero our own load: all assignments were released above, so a stale
	// current_load would otherwise linger on the row and inflate Total Load.
	if err := c.Client.ResetNodeLoad(c.NodeID); err != nil {
		log.Printf("[coordinator] error resetting node load: %v", err)
	}

	log.Printf("[coordinator] node %q stopped cleanly", c.NodeID)
}

// Register upserts this node in the nodes table.
func (c *Coordinator) Register() {
	if c.Client == nil {
		log.Printf("[coordinator] WARNING: no database client — skipping node registration")
		return
	}
	host, _ := os.Hostname()
	version := os.Getenv("SOFTWARE_VERSION")
	if version == "" {
		version = "dev"
	}

	webURL := os.Getenv("NODE_WEB_URL")
	if webURL == "" {
		if tsIP := detectTailscaleIP(); tsIP != "" {
			webURL = fmt.Sprintf("http://%s:8080", tsIP)
			log.Printf("[coordinator] auto-detected Tailscale IP: %s", tsIP)
		}
	}

	node := &database.Node{
		NodeID:          c.NodeID,
		Hostname:        host,
		InstanceLabel:   os.Getenv("INSTANCE_LABEL"),
		SoftwareVersion: version,
		Status:          "online",
		CurrentLoad:     0,
		WebURL:          webURL,
		SessionDeadline: computeSessionDeadline(),
	}

	if err := c.Client.UpsertNode(node); err != nil {
		log.Printf("[coordinator] WARNING: failed to register node: %v", err)
	} else {
		log.Printf("[coordinator] registered as node %q on %s", c.NodeID, host)
	}
}

// StartDraining sets the node status to "draining" so other nodes know not to
// assign new channels to this node. Call during graceful shutdown BEFORE stopping
// channels, so new claims go elsewhere.
func (c *Coordinator) StartDraining() {
	if !c.IsPooled() || c.Client == nil {
		return
	}
	c.mu.Lock()
	c.draining = true
	c.mu.Unlock()
	if err := c.Client.UpdateNodeStatus(c.NodeID, "draining"); err != nil {
		log.Printf("[coordinator] error setting draining: %v", err)
	}
}

// isActive reports whether this node is currently able to own/claim channels.
// A draining or fenced (partitioned) node must not claim or migrate channels.
func (c *Coordinator) isActive() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return !c.draining && !c.fenced
}

// isFenced reports whether the node is currently fenced due to a partition.
func (c *Coordinator) isFenced() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.fenced
}

// currentLoad returns the count of channels this node owns.
func (c *Coordinator) currentLoad() int {
	if c.Client == nil {
		return 0
	}
	count, err := c.Client.CountMyAssignments(c.NodeID)
	if err != nil {
		return 0
	}
	return count
}

// detectNodeID auto-detects the node identity using a priority chain:
// 1. NODE_ID env var (explicit)
// 2. GITHUB_REPOSITORY env var — splits by "-" and takes the last segment
//    so "owner/MiniDelectableService-node-a" yields "a"
// 3. os.Hostname() (VPS / local)
// 4. Random fallback (defensive)
//
// IMPORTANT: this must stay in sync with server/db.go:detectNodeID().
func detectNodeID() string {
	// Ignore placeholder values (empty/whitespace/"-") so a "-" NODE_ID secret
	// can never register a bogus "-" row in the Supabase nodes table.
	if id := strings.TrimSpace(os.Getenv("NODE_ID")); id != "" && id != "-" {
		return id
	}
	if repo := os.Getenv("GITHUB_REPOSITORY"); repo != "" {
		parts := strings.Split(repo, "-")
		if len(parts) > 1 {
			return parts[len(parts)-1]
		}
		return strings.ReplaceAll(repo, "/", "-")
	}
	if host, err := os.Hostname(); err == nil && host != "" {
		return host
	}
	n, _ := rand.Int(rand.Reader, big.NewInt(1<<48))
	return fmt.Sprintf("node-%x", n)
}

// channelPoolMode returns the pool mode from env var, falling back to
// auto-detection for node-* repos (GITHUB_REPOSITORY). MUST stay in sync with
// server/db.go:detectPoolMode() — a mismatch silently runs the coordinator
// isolated while the web UI thinks the node is pooled.
func channelPoolMode() string {
	if mode := os.Getenv("CHANNEL_POOL_MODE"); mode != "" {
		return mode
	}
	if repo := os.Getenv("GITHUB_REPOSITORY"); repo != "" {
		// Auto-enable pooled mode for repos named node-*
		if strings.Contains(repo, "node-") {
			return entity.PoolModePooled
		}
	}
	return entity.PoolModeIsolated
}

// computeSessionDeadline determines when this node will be forcibly killed so
// the coordinator can migrate its channels away beforehand.  The value is
// persisted on the node row (session_deadline) at registration; the deadline
// migration loop reads it.  Priority:
//  1. The effective session length on server.Config (env/flag, central
//     Supabase value, or CI fallback — see ApplyCentralSessionDuration), so the
//     recorder stop and the coordinator deadline always agree.
//  2. SESSION_DURATION env (Go duration string, e.g. "5h20m") — defensive
//     fallback if Config is unavailable.
//  3. GITHUB_RUN_ID present (CI runner, hard 6h cap) — use a buffer BEFORE the
//     workflow's 348-minute self-cancel so migration fires while we're still up.
//  4. None of the above — nil (permanent node, no deadline).
func computeSessionDeadline() *time.Time {
	if server.Config != nil && server.Config.SessionDurationParsed > 0 {
		t := time.Now().Add(server.Config.SessionDurationParsed)
		return &t
	}
	if d := os.Getenv("SESSION_DURATION"); d != "" {
		if dur, err := time.ParseDuration(d); err == nil && dur > 0 {
			t := time.Now().Add(dur)
			return &t
		}
	}
	if os.Getenv("GITHUB_RUN_ID") != "" {
		// 335m leaves a ~13m buffer before the 348m self-cancel / 360m hard kill.
		t := time.Now().Add(335 * time.Minute)
		return &t
	}
	return nil
}
