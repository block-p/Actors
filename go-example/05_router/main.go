// 05_router — Router Actor Pattern
//
// A RouterActor sits in front of a pool of WorkerActors and decides which
// worker should handle each incoming message.  Three strategies are shown:
//
//   Round-Robin  — strict rotation, great for evenly-distributed load
//   Random       — simple but uneven; good when messages are already balanced
//   Hash         — same key ALWAYS maps to the same worker
//
// WHY HASH ROUTING MATTERS IN TRADE ENGINES
// ------------------------------------------
// Consider two orders for BTC-USD arriving 1 ms apart:
//   Order A: BUY  0.5 BTC
//   Order B: SELL 0.5 BTC  (this cancels A if processed first)
//
// With round-robin those two orders land on different workers and race.
// With hash routing (key = symbol) they always land on Worker-2 (or whichever),
// so they are processed sequentially, preserving order and preventing phantom
// fills. This is how real matching engines achieve lock-free ordering per symbol.

package main

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

// WorkMsg is the unit of work the router distributes to a worker.
type WorkMsg struct {
	ID     int    // unique message identifier
	Symbol string // trading symbol — used as hash key
	Data   string // arbitrary payload
}

// WorkDone is sent back by a worker to acknowledge completion.
type WorkDone struct {
	WorkerID int
	MsgID    int
}

// GetStats requests a snapshot of routing statistics.
type GetStats struct {
	Reply chan RouterStats
}

// RouterStats holds counters accumulated by the router.
type RouterStats struct {
	TotalRouted int
	PerWorker   map[int]int // workerID → messages handled
}

// ---------------------------------------------------------------------------
// Routing strategies
// ---------------------------------------------------------------------------

// RoutingStrategy is the policy the router uses to pick a worker.
type RoutingStrategy int

const (
	RoundRobin RoutingStrategy = iota // sequential rotation
	Random                            // random pick
	Hash                              // deterministic by key
)

func (s RoutingStrategy) String() string {
	switch s {
	case RoundRobin:
		return "RoundRobin"
	case Random:
		return "Random"
	case Hash:
		return "Hash"
	}
	return "Unknown"
}

// ---------------------------------------------------------------------------
// WorkerActor
// ---------------------------------------------------------------------------

// WorkerActor represents one worker in the pool.  Each worker has its own
// goroutine and its own unbuffered channel — the classic actor mailbox.
type WorkerActor struct {
	id      int
	mailbox chan WorkMsg
	done    chan struct{} // closed by the router when it wants the worker to stop
	results chan WorkDone // shared result channel back to the router/main
}

// newWorkerActor constructs and starts a worker.
func newWorkerActor(id int, results chan WorkDone) *WorkerActor {
	w := &WorkerActor{
		id:      id,
		mailbox: make(chan WorkMsg, 16), // small buffer so router never blocks
		done:    make(chan struct{}),
		results: results,
	}
	go w.run()
	return w
}

// run is the worker's event loop — process one message at a time.
func (w *WorkerActor) run() {
	for {
		select {
		case msg := <-w.mailbox:
			// Simulate a tiny amount of work so output is visible.
			time.Sleep(5 * time.Millisecond)
			fmt.Printf("  [Worker-%d] processed msg #%d  symbol=%-8s  data=%q\n",
				w.id, msg.ID, msg.Symbol, msg.Data)
			// Notify whoever is collecting results.
			w.results <- WorkDone{WorkerID: w.id, MsgID: msg.ID}
		case <-w.done:
			fmt.Printf("  [Worker-%d] shutting down\n", w.id)
			return
		}
	}
}

// ---------------------------------------------------------------------------
// RouterActor
// ---------------------------------------------------------------------------

// routerCmd is the internal union type for the router's mailbox.
// Using a struct with optional fields is idiomatic Go for a "tagged union".
type routerCmd struct {
	work  *WorkMsg  // non-nil → route this message
	stats *GetStats // non-nil → reply with current stats
	stop  bool      // true → shut everything down
}

// RouterActor owns the pool of workers and applies the routing strategy.
type RouterActor struct {
	strategy  RoutingStrategy
	workers   []*WorkerActor
	mailbox   chan routerCmd
	results   chan WorkDone // workers post completions here
	rrCounter int           // cursor for round-robin
	stats     RouterStats   // live counters
}

// newRouterActor creates the router and its worker pool, then starts both.
func newRouterActor(strategy RoutingStrategy, numWorkers int) *RouterActor {
	results := make(chan WorkDone, 64)
	r := &RouterActor{
		strategy: strategy,
		mailbox:  make(chan routerCmd, 64),
		results:  results,
		stats:    RouterStats{PerWorker: make(map[int]int)},
	}
	for i := 1; i <= numWorkers; i++ {
		r.workers = append(r.workers, newWorkerActor(i, results))
	}
	go r.run()
	return r
}

// run is the router's event loop.
func (r *RouterActor) run() {
	for cmd := range r.mailbox {
		switch {
		case cmd.stop:
			// Signal every worker to stop, then drain their results.
			for _, w := range r.workers {
				close(w.done)
			}
			return

		case cmd.stats != nil:
			// Copy stats and send — never share mutable state directly.
			snap := RouterStats{
				TotalRouted: r.stats.TotalRouted,
				PerWorker:   make(map[int]int, len(r.stats.PerWorker)),
			}
			for k, v := range r.stats.PerWorker {
				snap.PerWorker[k] = v
			}
			cmd.stats.Reply <- snap

		case cmd.work != nil:
			workerIdx := r.pick(cmd.work)
			r.workers[workerIdx].mailbox <- *cmd.work
			r.stats.TotalRouted++
			r.stats.PerWorker[r.workers[workerIdx].id]++
		}
	}
}

// pick selects the index (into r.workers) for this message based on strategy.
func (r *RouterActor) pick(msg *WorkMsg) int {
	n := len(r.workers)
	switch r.strategy {
	case RoundRobin:
		// Increment the cursor modulo worker count — simple and fair.
		idx := r.rrCounter % n
		r.rrCounter++
		return idx

	case Random:
		// Pure random — cheap, but over short runs some workers get more load.
		return rand.Intn(n)

	case Hash:
		// FNV-32 hash on the symbol string.  The same symbol always hashes to
		// the same bucket, so all BTC-USD messages go to one worker, all
		// ETH-USD to another, etc.  This guarantees per-symbol ordering.
		h := fnv.New32a()
		h.Write([]byte(msg.Symbol))
		return int(h.Sum32()) % n
	}
	return 0
}

// ---------------------------------------------------------------------------
// Public API helpers (so main doesn't touch channels directly)
// ---------------------------------------------------------------------------

// Send routes a WorkMsg through the router.
func (r *RouterActor) Send(msg WorkMsg) {
	r.mailbox <- routerCmd{work: &msg}
}

// Stats requests and waits for current routing statistics.
func (r *RouterActor) Stats() RouterStats {
	reply := make(chan RouterStats, 1)
	r.mailbox <- routerCmd{stats: &GetStats{Reply: reply}}
	return <-reply
}

// Stop shuts down the router and waits for all workers to drain.
func (r *RouterActor) Stop(wg *sync.WaitGroup) {
	// Wait until all sent messages are acknowledged before stopping.
	// (In main we already wait on wg before calling Stop.)
	r.mailbox <- routerCmd{stop: true}
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	// Seed the PRNG so the Random strategy produces visible variation.
	rand.Seed(time.Now().UnixNano()) //nolint:staticcheck

	symbols := []string{
		"BTC-USD", "ETH-USD", "SOL-USD", "BTC-USD",
		"ETH-USD", "BTC-USD", "AVAX-USD", "SOL-USD",
		"BTC-USD", "ETH-USD", "BTC-USD", "AVAX-USD",
	}

	strategies := []RoutingStrategy{RoundRobin, Random, Hash}

	for _, strategy := range strategies {
		fmt.Printf("\n════════════════════════════════════════\n")
		fmt.Printf("  Strategy: %s\n", strategy)
		fmt.Printf("════════════════════════════════════════\n")

		const numWorkers = 4
		router := newRouterActor(strategy, numWorkers)

		// We'll track completions with a WaitGroup to know when all work is done.
		var wg sync.WaitGroup
		total := len(symbols)
		wg.Add(total)

		// Drain the results channel in a separate goroutine.
		go func() {
			received := 0
			for range router.results {
				wg.Done()
				received++
				if received == total {
					return // all done; let the goroutine exit naturally
				}
			}
		}()

		// Send 12 messages with varying symbols.
		for i, sym := range symbols {
			router.Send(WorkMsg{
				ID:     i + 1,
				Symbol: sym,
				Data:   fmt.Sprintf("tick-%d", i+1),
			})
		}

		// Wait for every message to be processed before printing stats.
		wg.Wait()

		stats := router.Stats()
		fmt.Printf("\n  ── Stats ──────────────────────────────\n")
		fmt.Printf("  Total routed: %d\n", stats.TotalRouted)
		for wid := 1; wid <= numWorkers; wid++ {
			bar := ""
			for j := 0; j < stats.PerWorker[wid]; j++ {
				bar += "█"
			}
			fmt.Printf("  Worker-%d: %2d  %s\n", wid, stats.PerWorker[wid], bar)
		}

		// Hash strategy: show that all BTC-USD messages landed on the same worker.
		if strategy == Hash {
			fmt.Println()
			fmt.Println("  ── Hash routing verification ──────────")
			fmt.Println("  (all messages for the same symbol must go to the same worker)")
			fmt.Println("  Check output above: every BTC-USD line should share one Worker-N.")
		}

		router.Stop(&wg)
		// Give workers a moment to print their shutdown messages.
		time.Sleep(20 * time.Millisecond)
	}

	fmt.Println("\nDone.")
}
