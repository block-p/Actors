// Package main demonstrates the full actor lifecycle:
//
//   - PreStart: actor initialises itself via an Init message
//   - Running: the receive loop — never block, offload slow work to goroutines
//   - PostStop: graceful cleanup when a StopActor message arrives
//   - Supervision: a supervisor that restarts crashed actors
//   - Max restarts: detecting when an actor is crashing too often
//
// This program models a small trade system:
//
//	Supervisor
//	  └── PriceActor   (simulates a WebSocket price feed, can crash)
//	  └── StrategyActor (reads prices, places fake orders, can crash)
//	  └── LoggerActor   (receives events, always healthy)
//
// Run this program:
//
//	cd go-example
//	go run ./04_lifecycle/main.go
package main

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

// ═══════════════════════════════════════════════════════════════════════════════
// MESSAGES
// ═══════════════════════════════════════════════════════════════════════════════

// Init is sent to an actor as its very first message.
// This is the "Init Message Pattern" for PreStart:
// the spawner doesn't block, the actor initialises itself at its own pace.
type Init struct{}

// PriceUpdate is an Event — the price actor publishes this to all subscribers.
// Past tense, no reply, always has a timestamp.
type PriceUpdate struct {
	Symbol    string
	Price     float64
	Timestamp time.Time
}

// GetLatestPrice is a Query — includes a buffered reply channel.
type GetLatestPrice struct {
	Symbol string
	Reply  chan PriceResult
}

// PriceResult is the reply payload for GetLatestPrice.
type PriceResult struct {
	Symbol string
	Price  float64
	Found  bool
}

// PlaceOrder is a Command — fire and forget.
type PlaceOrder struct {
	Symbol string
	Side   string
	Qty    float64
	Price  float64
}

// LogEvent is a Command — fire and forget, sent to the logger actor.
type LogEvent struct {
	Level   string
	Actor   string
	Message string
}

// ActorCrashed is sent by an actor to its supervisor just before dying.
type ActorCrashed struct {
	ActorName string
	Err       error
}

// StopActor is the Poison Pill — graceful shutdown.
// Done is closed by the actor to signal "I am fully stopped."
type StopActor struct {
	Done chan struct{}
}

// ═══════════════════════════════════════════════════════════════════════════════
// LOGGER ACTOR — simple, never crashes, demonstrates PostStop flush
// ═══════════════════════════════════════════════════════════════════════════════

type loggerActor struct {
	mailbox chan any
	log     []string // buffered log entries (normally you'd write to a file/DB)
}

func newLoggerActor() *loggerActor {
	a := &loggerActor{
		mailbox: make(chan any, 500),
		log:     make([]string, 0, 100),
	}
	go a.run()
	// Send the Init message to self — actor initialises asynchronously.
	// The spawner (main) doesn't block; it can continue wiring up other actors.
	a.mailbox <- Init{}
	return a
}

func (a *loggerActor) Send(msg any) { a.mailbox <- msg }

func (a *loggerActor) run() {
	for msg := range a.mailbox {
		switch m := msg.(type) {

		// ── PreStart via Init message ────────────────────────────────────────
		case Init:
			// In a real logger you'd open the log file, connect to a log aggregator,
			// set up structured logging, etc. Here we just print.
			fmt.Println("[Logger] PreStart: log file opened, ready to receive events")

		// ── Normal operation ─────────────────────────────────────────────────
		case LogEvent:
			entry := fmt.Sprintf("[%s] [%s] %s: %s",
				time.Now().Format("15:04:05.000"), m.Level, m.Actor, m.Message)
			a.log = append(a.log, entry)
			fmt.Println(entry)

		// ── PostStop ─────────────────────────────────────────────────────────
		case StopActor:
			// Flush all buffered log entries before dying.
			// This is PostStop: cleanup work before returning.
			fmt.Printf("[Logger] PostStop: flushing %d buffered entries to disk...\n", len(a.log))
			// (In a real system: write a.log to disk/DB)
			fmt.Println("[Logger] PostStop: flush complete, closing log file")
			close(m.Done) // signal: I am fully stopped
			return
		}
	}
}

// ═══════════════════════════════════════════════════════════════════════════════
// PRICE ACTOR — demonstrates PreStart, offloading blocking work, and crashing
// ═══════════════════════════════════════════════════════════════════════════════

type priceActor struct {
	mailbox    chan any
	supervisor chan any
	logger     *loggerActor
	prices     map[string]float64
	ready      bool
	// crashAfter: if > 0, the actor will panic after processing this many price updates.
	// This demonstrates the supervisor's restart behaviour.
	crashAfter  int
	updateCount int
}

func newPriceActor(supervisor chan any, logger *loggerActor, crashAfter int) *priceActor {
	a := &priceActor{
		mailbox:    make(chan any, 200),
		supervisor: supervisor,
		logger:     logger,
		prices:     make(map[string]float64),
		crashAfter: crashAfter,
	}
	go a.run()
	a.mailbox <- Init{} // kick off PreStart
	return a
}

func (a *priceActor) Send(msg any) { a.mailbox <- msg }

func (a *priceActor) run() {
	// The defer+recover is the supervision hook.
	// If any message handler panics, recover() catches it and sends
	// ActorCrashed to the supervisor. The goroutine then exits cleanly —
	// the supervisor decides whether to restart.
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("panic: %v", r)
			fmt.Printf("[PriceActor] CRASHED: %v\n", err)
			// Notify supervisor — this is fire-and-forget; we're dying anyway.
			a.supervisor <- ActorCrashed{ActorName: "price-actor", Err: err}
		}
	}()

	for msg := range a.mailbox {
		switch m := msg.(type) {

		// ── PreStart ─────────────────────────────────────────────────────────
		case Init:
			// Simulate connecting to a WebSocket price feed.
			// In a real system this would be an HTTP handshake + WebSocket upgrade.
			// We do it here (synchronously, inside the actor) because Init is just
			// another message — the actor's receive loop handles it sequentially.
			// There's no risk of blocking the spawner because the spawner already moved on.
			fmt.Println("[PriceActor] PreStart: connecting to exchange WebSocket...")
			time.Sleep(20 * time.Millisecond) // simulate connection latency
			fmt.Println("[PriceActor] PreStart: loading last known prices from DB...")

			// Seed with known prices (simulating a DB load)
			a.prices["BTC/USDT"] = 68_000.0
			a.prices["ETH/USDT"] = 3_800.0
			a.prices["SOL/USDT"] = 180.0

			a.ready = true
			fmt.Println("[PriceActor] PreStart: ready, subscribed to 3 symbols")
			a.logger.Send(LogEvent{Level: "INFO", Actor: "PriceActor", Message: "started and ready"})

		// ── Offloading blocking work to a goroutine ───────────────────────────
		// In a real price actor, fetching a snapshot from REST would be slow.
		// We NEVER do slow work inside the receive loop — we kick off a goroutine
		// and send the result back as a message.
		case fetchSnapshot: // internal message type (see below)
			symbol := m.Symbol
			self := a.mailbox
			go func() {
				// Simulate a slow REST API call (100ms)
				// This goroutine runs OUTSIDE the receive loop.
				// The actor is NOT blocked — it can process other messages while
				// this goroutine is running.
				time.Sleep(100 * time.Millisecond)
				// Send result back as a message — the actor handles it normally
				self <- snapshotReady{
					Symbol: symbol,
					Price:  68_500.0, // fake snapshot data
				}
			}()
			fmt.Printf("[PriceActor] kicked off background snapshot fetch for %s\n", m.Symbol)

		case snapshotReady:
			// Result of the background REST fetch — process it instantly
			a.prices[m.Symbol] = m.Price
			fmt.Printf("[PriceActor] snapshot applied: %s = %.2f\n", m.Symbol, m.Price)

		// ── Price update (simulates WebSocket tick arriving) ─────────────────
		case PriceUpdate:
			if !a.ready {
				continue
			}
			a.prices[m.Symbol] = m.Price
			a.updateCount++

			// Deliberate crash after N updates (to demonstrate supervision)
			if a.crashAfter > 0 && a.updateCount >= a.crashAfter {
				// This panic is caught by the deferred recover() above.
				// The actor's state is lost; the supervisor will create a new instance.
				panic(fmt.Sprintf("simulated crash after %d price updates", a.updateCount))
			}

		// ── Query ─────────────────────────────────────────────────────────────
		case GetLatestPrice:
			if !a.ready {
				m.Reply <- PriceResult{Found: false}
				continue
			}
			price, ok := a.prices[m.Symbol]
			// Reply is buffered (size 1) — safe even if caller timed out.
			m.Reply <- PriceResult{Symbol: m.Symbol, Price: price, Found: ok}

		// ── PostStop ─────────────────────────────────────────────────────────
		case StopActor:
			// Close WebSocket connection, flush any pending price data.
			fmt.Println("[PriceActor] PostStop: closing WebSocket connection")
			fmt.Printf("[PriceActor] PostStop: cached %d prices\n", len(a.prices))
			a.logger.Send(LogEvent{Level: "INFO", Actor: "PriceActor", Message: "stopped cleanly"})
			close(m.Done) // broadcast "I am fully dead"
			return
		}
	}
}

// Internal messages used only by priceActor to offload slow work
type fetchSnapshot struct{ Symbol string }
type snapshotReady struct {
	Symbol string
	Price  float64
}

// ═══════════════════════════════════════════════════════════════════════════════
// STRATEGY ACTOR — demonstrates reading from another actor + crashing
// ═══════════════════════════════════════════════════════════════════════════════

type strategyActor struct {
	mailbox    chan any
	priceActor *priceActor
	supervisor chan any
	logger     *loggerActor
	ready      bool
}

func newStrategyActor(price *priceActor, supervisor chan any, logger *loggerActor) *strategyActor {
	a := &strategyActor{
		mailbox:    make(chan any, 200),
		priceActor: price,
		supervisor: supervisor,
		logger:     logger,
	}
	go a.run()
	a.mailbox <- Init{}
	return a
}

func (a *strategyActor) Send(msg any) { a.mailbox <- msg }

func (a *strategyActor) run() {
	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("panic: %v", r)
			fmt.Printf("[StrategyActor] CRASHED: %v\n", err)
			a.supervisor <- ActorCrashed{ActorName: "strategy-actor", Err: err}
		}
	}()

	for msg := range a.mailbox {
		switch m := msg.(type) {

		case Init:
			fmt.Println("[StrategyActor] PreStart: loading strategy parameters...")
			time.Sleep(10 * time.Millisecond)
			a.ready = true
			fmt.Println("[StrategyActor] PreStart: ready")
			a.logger.Send(LogEvent{Level: "INFO", Actor: "StrategyActor", Message: "started and ready"})

		case runStrategy: // tick message — "evaluate strategy now"
			if !a.ready {
				continue
			}

			// KEY PATTERN: querying another actor from INSIDE a receive case.
			//
			// The WRONG way would be:
			//   reply := make(chan PriceResult, 1)
			//   a.priceActor.Send(GetLatestPrice{Reply: reply})
			//   price := <-reply   // ← BLOCKS the strategy actor!
			//
			// The RIGHT way: offload to a goroutine, receive result as a message.
			// This keeps the strategy actor's loop non-blocking.
			symbol := m.Symbol
			self := a.mailbox
			priceActor := a.priceActor
			go func() {
				reply := make(chan PriceResult, 1) // buffered — always
				priceActor.Send(GetLatestPrice{Symbol: symbol, Reply: reply})

				select {
				case result := <-reply:
					// Got the price — send it back to ourselves as a message
					self <- priceReceived{Symbol: symbol, Price: result.Price, Found: result.Found}
				case <-time.After(500 * time.Millisecond):
					// Price actor didn't respond — don't block, report the failure
					self <- priceReceived{Symbol: symbol, Found: false}
				}
			}()

		case priceReceived: // result of the async price query
			if !m.Found {
				fmt.Printf("[StrategyActor] no price for %s, skipping\n", m.Symbol)
				continue
			}
			// Simple strategy: if price is below a threshold, generate a buy signal
			if m.Price < 69_000 && m.Symbol == "BTC/USDT" {
				fmt.Printf("[StrategyActor] BUY signal: %s @ %.2f\n", m.Symbol, m.Price)
				a.logger.Send(LogEvent{
					Level:   "INFO",
					Actor:   "StrategyActor",
					Message: fmt.Sprintf("BUY signal generated for %s @ %.2f", m.Symbol, m.Price),
				})
			} else {
				fmt.Printf("[StrategyActor] HOLD: %s @ %.2f\n", m.Symbol, m.Price)
			}

		case StopActor:
			fmt.Println("[StrategyActor] PostStop: cancelling open orders, flushing state")
			a.logger.Send(LogEvent{Level: "INFO", Actor: "StrategyActor", Message: "stopped cleanly"})
			close(m.Done)
			return
		}
	}
}

// Internal messages for strategyActor
type runStrategy struct{ Symbol string }
type priceReceived struct {
	Symbol string
	Price  float64
	Found  bool
}

// ═══════════════════════════════════════════════════════════════════════════════
// SUPERVISOR — restarts crashed actors, tracks restart counts
// ═══════════════════════════════════════════════════════════════════════════════

// supervisorState tracks restart counts per actor name.
type supervisorState struct {
	restarts    map[string]int
	maxRestarts int
}

// supervisor runs as a goroutine (it IS an actor, just simpler).
// It listens for ActorCrashed messages and decides what to do.
//
// Supervision strategy here: restart up to maxRestarts times.
// After that, log a critical alert and give up (escalate to operator).
//
// In production you'd escalate to a parent supervisor or trigger a system shutdown.
func runSupervisor(
	mailbox chan any,
	maxRestarts int,
	onRestart func(actorName string),
	logger *loggerActor,
) {
	state := &supervisorState{
		restarts:    make(map[string]int),
		maxRestarts: maxRestarts,
	}

	go func() {
		fmt.Println("[Supervisor] started")

		for msg := range mailbox {
			switch m := msg.(type) {

			case ActorCrashed:
				state.restarts[m.ActorName]++
				count := state.restarts[m.ActorName]

				logger.Send(LogEvent{
					Level: "WARN",
					Actor: "Supervisor",
					Message: fmt.Sprintf("actor %q crashed (restart %d/%d): %v",
						m.ActorName, count, maxRestarts, m.Err),
				})

				if count > maxRestarts {
					// Max restarts exceeded — this actor is in a crash loop.
					// Escalate: in a real system you'd alert the operator and
					// consider shutting down the entire system.
					logger.Send(LogEvent{
						Level: "CRITICAL",
						Actor: "Supervisor",
						Message: fmt.Sprintf("actor %q exceeded max restarts (%d) — NOT restarting, needs operator attention",
							m.ActorName, maxRestarts),
					})
					fmt.Printf("[Supervisor] CRITICAL: %s exceeded max restarts, giving up\n", m.ActorName)
					continue
				}

				// Restart the actor — call the provided callback
				fmt.Printf("[Supervisor] restarting %q (attempt %d/%d)\n",
					m.ActorName, count, maxRestarts)
				onRestart(m.ActorName)

			case StopActor:
				fmt.Println("[Supervisor] stopping")
				close(m.Done)
				return
			}
		}
	}()
}

// ═══════════════════════════════════════════════════════════════════════════════
// CRASH LOOP DEMO — isolated mini-system to show max-restarts-exceeded
// ═══════════════════════════════════════════════════════════════════════════════

// demonstrateMaxRestarts creates a fresh, isolated supervisor and a price actor
// that is ALWAYS crashy (crashAfter=1). The supervisor is configured to allow
// only 1 restart. After the second crash the supervisor gives up and logs CRITICAL.
//
// We use a separate supervisor here so the restart count starts at 0 and we
// can clearly see it count up to the limit in a controlled way.
func demonstrateMaxRestarts(logger *loggerActor) {
	fmt.Println("\n── Part 5: Max restarts exceeded (isolated demo) ────────────────")

	// Fresh supervisor with maxRestarts=1 (allows only 1 restart)
	superMailbox := make(chan any, 20)

	// We track the current crashy price actor behind an atomic pointer
	// so the onRestart callback can swap it.
	var ref atomic.Pointer[priceActor]

	// onRestart always creates a new crashy actor (crashAfter=1).
	// This means every restart will also crash, driving the count up to the limit.
	onRestart := func(actorName string) {
		if actorName == "price-actor" {
			newA := newPriceActor(superMailbox, logger, 1) // still crashy!
			ref.Store(newA)
			time.Sleep(50 * time.Millisecond)
			fmt.Println("[DemoSupervisor] price-actor restarted (crashAfter=1 again)")
		}
	}

	runSupervisor(superMailbox, 1, onRestart, logger) // maxRestarts = 1

	// Spawn the first (crashy) price actor
	a := newPriceActor(superMailbox, logger, 1)
	ref.Store(a)
	time.Sleep(50 * time.Millisecond)

	// Crash 1 → supervisor allows it (restart 1/1)
	fmt.Println("  Sending update #1 → first crash expected")
	ref.Load().Send(PriceUpdate{Symbol: "BTC/USDT", Price: 68_000.0, Timestamp: time.Now()})
	time.Sleep(200 * time.Millisecond) // wait for crash + restart

	// Crash 2 → supervisor has already used its 1 allowed restart
	//            restart count (2) > maxRestarts (1) → CRITICAL, no restart
	fmt.Println("  Sending update #2 → second crash, max restarts exceeded")
	ref.Load().Send(PriceUpdate{Symbol: "BTC/USDT", Price: 69_000.0, Timestamp: time.Now()})
	time.Sleep(200 * time.Millisecond) // wait for CRITICAL log

	// Stop the demo supervisor (the actor is already dead by this point)
	done := make(chan struct{})
	superMailbox <- StopActor{Done: done}
	<-done
	fmt.Println("  Demo supervisor stopped")
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN — wire everything up and demonstrate the full lifecycle
// ═══════════════════════════════════════════════════════════════════════════════

func main() {
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println(" Actor Lifecycle: PreStart, Running, PostStop, Supervision")
	fmt.Println("═══════════════════════════════════════════════════════════════")

	// ── Part 1: Spawn actors (PreStart via Init message) ─────────────────────
	fmt.Println("\n── Part 1: Spawning actors (PreStart via Init message) ──────────")

	// Logger first — it has no dependencies
	logger := newLoggerActor()

	// Supervisor mailbox — created before the actors so they can send to it
	supervisorMailbox := make(chan any, 50)

	// We need to be able to restart actors by name, so we hold references
	// behind a pointer that can be swapped on restart.
	// atomic.Pointer gives us lock-free swapping.
	var priceRef atomic.Pointer[priceActor]
	var strategyRef atomic.Pointer[strategyActor]

	// crashAfter=3 means the price actor will crash after 3 PriceUpdate messages.
	// This demonstrates the supervisor restarting it.
	price := newPriceActor(supervisorMailbox, logger, 3)
	priceRef.Store(price)

	strategy := newStrategyActor(price, supervisorMailbox, logger)
	strategyRef.Store(strategy)

	// Give actors time to finish their PreStart (the Init message is processed async)
	time.Sleep(60 * time.Millisecond)

	// ── Part 2: Set up the supervisor ────────────────────────────────────────
	fmt.Println("\n── Part 2: Starting supervisor (max 2 restarts per actor) ──────")

	// The restart callback — creates a fresh actor and updates the shared reference.
	// In a real system you'd use a proper actor registry instead of atomic pointers.
	var restartMu sync.Mutex // protect against concurrent restarts
	onRestart := func(actorName string) {
		restartMu.Lock()
		defer restartMu.Unlock()

		switch actorName {
		case "price-actor":
			// Spawn a new price actor — no crash limit this time (crashAfter=0)
			newPrice := newPriceActor(supervisorMailbox, logger, 0)
			priceRef.Store(newPrice)
			time.Sleep(50 * time.Millisecond) // wait for PreStart
			fmt.Printf("[Supervisor] price-actor restarted successfully\n")

		case "strategy-actor":
			// Restart strategy actor pointing at the current price actor
			currentPrice := priceRef.Load()
			newStrategy := newStrategyActor(currentPrice, supervisorMailbox, logger)
			strategyRef.Store(newStrategy)
			time.Sleep(30 * time.Millisecond) // wait for PreStart
			fmt.Printf("[Supervisor] strategy-actor restarted successfully\n")
		}
	}

	runSupervisor(supervisorMailbox, 2, onRestart, logger)

	// ── Part 3: Normal operation ──────────────────────────────────────────────
	fmt.Println("\n── Part 3: Normal operation — sending price updates ─────────────")

	// Send price updates. The price actor will crash on the 3rd one (crashAfter=3).
	// The supervisor will catch it and restart.
	symbols := []string{"BTC/USDT", "ETH/USDT", "SOL/USDT"}
	prices := []float64{68_500.0, 3_850.0, 182.0}

	for i := 0; i < 2; i++ {
		sym := symbols[i%len(symbols)]
		p := prices[i%len(prices)]
		update := PriceUpdate{Symbol: sym, Price: p, Timestamp: time.Now()}

		// Always use the current price actor reference (may have been swapped by supervisor)
		priceRef.Load().Send(update)

		// Also run the strategy to demonstrate cross-actor queries
		strategyRef.Load().Send(runStrategy{Symbol: "BTC/USDT"})
		time.Sleep(30 * time.Millisecond)
	}

	// Trigger the crash — this is the 3rd price update
	fmt.Println("\n── Part 3b: Triggering deliberate crash (3rd price update) ─────")
	priceRef.Load().Send(PriceUpdate{Symbol: "BTC/USDT", Price: 69_100.0, Timestamp: time.Now()})

	// Give supervisor time to detect crash and restart the actor
	time.Sleep(150 * time.Millisecond)

	// ── Part 4: Demonstrate offloading blocking work ──────────────────────────
	fmt.Println("\n── Part 4: Offloading blocking work to a goroutine ─────────────")
	fmt.Println("  (Fetching price snapshot in background — actor is NOT blocked)")

	// Tell the restarted price actor to fetch a snapshot in the background
	priceRef.Load().Send(fetchSnapshot{Symbol: "SOL/USDT"})

	// The actor can process OTHER messages while the snapshot goroutine runs.
	// Here we immediately ask for a price — the actor handles this WHILE the
	// background fetch is running.
	reply := make(chan PriceResult, 1) // buffered — always
	priceRef.Load().Send(GetLatestPrice{Symbol: "BTC/USDT", Reply: reply})

	select {
	case result := <-reply:
		fmt.Printf("  Price query while snapshot running: BTC/USDT = %.2f (found=%v)\n",
			result.Price, result.Found)
	case <-time.After(500 * time.Millisecond):
		fmt.Println("  Price query timed out")
	}

	// Wait for the snapshot goroutine to finish and deliver its result
	time.Sleep(150 * time.Millisecond)

	// ── Part 5: Demonstrate max restarts exceeded ─────────────────────────────
	demonstrateMaxRestarts(logger)

	// ── Part 6: Graceful shutdown in dependency order ─────────────────────────
	fmt.Println("\n── Part 6: Graceful shutdown (reverse dependency order) ─────────")
	fmt.Println("  Order: strategy → price → supervisor → logger")
	fmt.Println("  (Stop dependents first, dependencies last)")

	// Step 1: Stop the strategy actor (it depends on price, stop it first)
	fmt.Println("  Stopping strategy actor...")
	d1 := make(chan struct{})
	strategyRef.Load().Send(StopActor{Done: d1})
	<-d1
	fmt.Println("  Strategy actor confirmed stopped")

	// Step 2: Stop the price actor (strategy is already stopped, safe now)
	fmt.Println("  Stopping price actor...")
	d2 := make(chan struct{})
	priceRef.Load().Send(StopActor{Done: d2})
	<-d2
	fmt.Println("  Price actor confirmed stopped")

	// Step 3: Stop the supervisor (no more actors to supervise)
	fmt.Println("  Stopping supervisor...")
	d3 := make(chan struct{})
	supervisorMailbox <- StopActor{Done: d3}
	<-d3
	fmt.Println("  Supervisor confirmed stopped")

	// Step 4: Stop the logger last (everything else may have sent final log events)
	fmt.Println("  Stopping logger (final flush)...")
	d4 := make(chan struct{})
	logger.Send(StopActor{Done: d4})
	<-d4
	fmt.Println("  Logger confirmed stopped")

	// ── Summary ───────────────────────────────────────────────────────────────
	fmt.Println("\n═══════════════════════════════════════════════════════════════")
	fmt.Println(" All actors stopped. Clean shutdown complete.")
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println()
	fmt.Println("Patterns demonstrated:")
	fmt.Println("  1. PreStart via Init message — actor initialises itself asynchronously")
	fmt.Println("  2. Blocking work offloaded to goroutine — receive loop stays fast")
	fmt.Println("  3. Cross-actor query from inside receive — use goroutine, never block")
	fmt.Println("  4. StopActor{Done} — graceful shutdown with confirmation")
	fmt.Println("  5. Shutdown in dependency order — stop dependents before dependencies")
	fmt.Println("  6. Supervisor with restart count — max restarts before escalation")
	fmt.Println("  7. defer+recover — crash isolated to one actor, supervisor notified")
}
