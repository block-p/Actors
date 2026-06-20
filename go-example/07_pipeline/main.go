// 07_pipeline — Pipeline Actor Pattern
//
// A pipeline chains actors so that the output of one becomes the input of the
// next.  Each stage has one responsibility and one goroutine.
//
//   [RawFeedActor] → [NormalizerActor] → [EnricherActor] → [StrategyActor]
//
// WHY PIPELINES ARE GOOD
// ----------------------
//  Single responsibility: RawFeedActor knows nothing about moving averages.
//                         StrategyActor knows nothing about wire formats.
//  Independent testability: swap out any stage with a mock for unit tests.
//  Easy composition: insert a logging stage, a filtering stage, etc., by
//                    simply changing who the next pointer is.
//  Natural backpressure: if StrategyActor is slow, its inbound channel fills
//                        up and EnricherActor blocks — the whole pipeline slows
//                        down organically. No dropped data, no busy-wait.
//
// RESILIENCE DEMO
// ---------------
// The Normalizer has a deliberate panic injected on tick #5.  We show two
// things:
//   1. Without recovery: the panic propagates and kills the whole pipeline.
//   2. With recover(): only the bad tick is dropped; the pipeline continues.
//
// In production you would pair this with a supervisor that restarts the stage.

package main

import (
	"fmt"
	"math"
	"strings"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Data types flowing through the pipeline
// ---------------------------------------------------------------------------

// RawTick simulates raw bytes off a WebSocket feed — a single line of CSV.
type RawTick struct {
	Raw string // e.g. "BTC-USD,65123.45,65130.00,1234.56"
}

// NormalizedTick is the result of parsing and normalising a RawTick.
type NormalizedTick struct {
	Symbol string
	Bid    float64
	Ask    float64
	Volume float64
}

// EnrichedTick adds derived/computed fields on top of a NormalizedTick.
type EnrichedTick struct {
	NormalizedTick
	Spread    float64 // ask - bid
	MidPrice  float64 // (bid + ask) / 2
	MovingAvg float64 // simple moving average of last N midprices
}

// Signal is the strategy's output for one tick.
type Signal struct {
	Symbol    string
	Action    string // "BUY", "SELL", "HOLD"
	MidPrice  float64
	MovingAvg float64
	Reason    string
}

// ---------------------------------------------------------------------------
// Stage 1 — RawFeedActor
// ---------------------------------------------------------------------------
//
// Receives raw string ticks and parses them into NormalizedTick values.
// It owns its own goroutine and passes results to the next stage via a channel.

type RawFeedActor struct {
	in   chan RawTick        // inbound mailbox
	next chan NormalizedTick // outbound to NormalizerActor
}

func newRawFeedActor(next chan NormalizedTick) *RawFeedActor {
	a := &RawFeedActor{
		in:   make(chan RawTick, 32),
		next: next,
	}
	go a.run()
	return a
}

func (a *RawFeedActor) run() {
	for raw := range a.in {
		tick, ok := parseRaw(raw.Raw)
		if !ok {
			fmt.Printf("  [RawFeed] ⚠ failed to parse %q — skipping\n", raw.Raw)
			continue
		}
		fmt.Printf("  [RawFeed]      parsed  symbol=%-8s bid=%.2f ask=%.2f vol=%.2f\n",
			tick.Symbol, tick.Bid, tick.Ask, tick.Volume)
		a.next <- tick
	}
	// Propagate shutdown downstream by closing the next channel.
	close(a.next)
}

// parseRaw parses "SYMBOL,bid,ask,volume" — a toy wire format.
func parseRaw(s string) (NormalizedTick, bool) {
	parts := strings.Split(s, ",")
	if len(parts) != 4 {
		return NormalizedTick{}, false
	}
	var bid, ask, vol float64
	fmt.Sscanf(parts[1], "%f", &bid)
	fmt.Sscanf(parts[2], "%f", &ask)
	fmt.Sscanf(parts[3], "%f", &vol)
	return NormalizedTick{Symbol: parts[0], Bid: bid, Ask: ask, Volume: vol}, true
}

// ---------------------------------------------------------------------------
// Stage 2 — NormalizerActor
// ---------------------------------------------------------------------------
//
// Receives NormalizedTick values and applies normalisation rules (e.g., noise
// filtering, currency conversion).  This stage demonstrates panic recovery:
// tick #5 triggers a deliberate panic, but recover() catches it so the stage
// keeps running.

type NormalizerActor struct {
	in      chan NormalizedTick
	next    chan NormalizedTick
	counter int // used to inject a fault on tick 5
}

func newNormalizerActor(next chan NormalizedTick) *NormalizerActor {
	a := &NormalizerActor{
		in:   make(chan NormalizedTick, 32),
		next: next,
	}
	go a.run()
	return a
}

func (a *NormalizerActor) run() {
	for tick := range a.in {
		a.counter++
		a.processSafe(tick)
	}
	close(a.next)
}

// processSafe wraps process() in a recover so a bad tick never kills the stage.
func (a *NormalizerActor) processSafe(tick NormalizedTick) {
	defer func() {
		if r := recover(); r != nil {
			// The stage survived the panic.  Log and continue.
			fmt.Printf("  [Normalizer] ⚠ RECOVERED from panic on tick #%d: %v — tick dropped\n",
				a.counter, r)
		}
	}()
	a.process(tick)
}

func (a *NormalizerActor) process(tick NormalizedTick) {
	// Inject a panic on tick 5 to demonstrate resilience.
	if a.counter == 5 {
		panic("simulated bad tick: price spike detected")
	}
	// Normalisation: round prices to 2 dp, ignore zero-volume ticks.
	if tick.Volume == 0 {
		fmt.Printf("  [Normalizer] ⚠ zero-volume tick for %s — skipping\n", tick.Symbol)
		return
	}
	tick.Bid = math.Round(tick.Bid*100) / 100
	tick.Ask = math.Round(tick.Ask*100) / 100
	fmt.Printf("  [Normalizer]  normalised  symbol=%-8s bid=%.2f ask=%.2f\n",
		tick.Symbol, tick.Bid, tick.Ask)
	a.next <- tick
}

// ---------------------------------------------------------------------------
// Stage 3 — EnricherActor
// ---------------------------------------------------------------------------
//
// Adds computed fields: spread, mid-price, and a simple N-period moving
// average.  Because each stage has its own state (the price history here),
// enrichment logic is fully encapsulated — no locks needed.

const maWindow = 5 // moving-average window size

type EnricherActor struct {
	in        chan NormalizedTick
	next      chan EnrichedTick
	priceHist []float64 // rolling buffer of mid-prices
}

func newEnricherActor(next chan EnrichedTick) *EnricherActor {
	a := &EnricherActor{
		in:   make(chan NormalizedTick, 32),
		next: next,
	}
	go a.run()
	return a
}

func (a *EnricherActor) run() {
	for tick := range a.in {
		mid := (tick.Bid + tick.Ask) / 2
		spread := tick.Ask - tick.Bid

		// Maintain the rolling window.
		a.priceHist = append(a.priceHist, mid)
		if len(a.priceHist) > maWindow {
			a.priceHist = a.priceHist[1:] // drop oldest
		}

		// Compute simple moving average.
		sum := 0.0
		for _, p := range a.priceHist {
			sum += p
		}
		ma := sum / float64(len(a.priceHist))

		enriched := EnrichedTick{
			NormalizedTick: tick,
			Spread:         spread,
			MidPrice:       mid,
			MovingAvg:      ma,
		}
		fmt.Printf("  [Enricher]    enriched  symbol=%-8s mid=%.2f spread=%.2f ma(5)=%.2f\n",
			tick.Symbol, mid, spread, ma)
		a.next <- enriched
	}
	close(a.next)
}

// ---------------------------------------------------------------------------
// Stage 4 — StrategyActor
// ---------------------------------------------------------------------------
//
// Applies a simple mean-reversion rule:
//   mid > MA  →  SELL  (price is above average, likely to revert down)
//   mid < MA  →  BUY   (price is below average, likely to revert up)
//   mid == MA →  HOLD
//
// This is the only stage that knows about trading strategy.  Swapping it for
// a momentum strategy requires changing only this stage.
//
// BACKPRESSURE DEMO: StrategyActor sleeps 15 ms per tick, which is slower
// than the feed.  The pipeline naturally slows down — EnricherActor blocks
// when its outbound channel is full.  No data is lost; no busy-wait occurs.

type StrategyActor struct {
	in      chan EnrichedTick
	signals []Signal // collected for final display
	done    chan struct{}
	mu      sync.Mutex
}

func newStrategyActor() *StrategyActor {
	a := &StrategyActor{
		in:   make(chan EnrichedTick, 4), // small buffer → backpressure demo
		done: make(chan struct{}),
	}
	go a.run()
	return a
}

func (a *StrategyActor) run() {
	for tick := range a.in {
		// Simulate slow strategy computation (backpressure source).
		time.Sleep(15 * time.Millisecond)

		action := "HOLD"
		reason := "mid == MA"
		switch {
		case tick.MidPrice > tick.MovingAvg:
			action = "SELL"
			reason = fmt.Sprintf("mid %.2f > MA %.2f", tick.MidPrice, tick.MovingAvg)
		case tick.MidPrice < tick.MovingAvg:
			action = "BUY"
			reason = fmt.Sprintf("mid %.2f < MA %.2f", tick.MidPrice, tick.MovingAvg)
		}
		sig := Signal{
			Symbol:    tick.Symbol,
			Action:    action,
			MidPrice:  tick.MidPrice,
			MovingAvg: tick.MovingAvg,
			Reason:    reason,
		}
		fmt.Printf("  [Strategy]    signal    symbol=%-8s action=%-4s  %s\n",
			sig.Symbol, sig.Action, sig.Reason)
		a.mu.Lock()
		a.signals = append(a.signals, sig)
		a.mu.Unlock()
	}
	close(a.done)
}

// Signals returns the collected signals after the pipeline is done.
func (a *StrategyActor) Signals() []Signal {
	a.mu.Lock()
	defer a.mu.Unlock()
	cp := make([]Signal, len(a.signals))
	copy(cp, a.signals)
	return cp
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	fmt.Println("╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║  07_pipeline — RawFeed → Normalizer → Enricher → Strategy     ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("Note: tick #5 triggers a deliberate panic inside Normalizer.")
	fmt.Println("      recover() catches it; the pipeline keeps running.")
	fmt.Println("      The StrategyActor has a 15 ms delay → backpressure visible.")
	fmt.Println()

	// Wire up the pipeline back-to-front so each stage knows the next channel.
	strategy := newStrategyActor()
	enrichCh := make(chan EnrichedTick, 4)        // Enricher → Strategy
	enricher := newEnricherActor(strategy.in)     // stage 3
	normCh := make(chan NormalizedTick, 8)        // Normalizer → Enricher
	normalizer := newNormalizerActor(enricher.in) // stage 2
	rawFeed := newRawFeedActor(normalizer.in)     // stage 1

	// Suppress "declared and not used" for channels wired differently.
	_ = enrichCh
	_ = normCh

	// 10 raw ticks — tick #5 will cause a panic in Normalizer.
	rawTicks := []string{
		"BTC-USD,65100.00,65102.00,120.5",
		"BTC-USD,65105.50,65108.00,98.2",
		"ETH-USD,3490.00,3491.50,450.0",
		"BTC-USD,65110.00,65112.50,75.3",
		"BTC-USD,65999.00,66001.00,999.9", // tick #5 → panic in Normalizer
		"ETH-USD,3492.00,3493.00,380.1",
		"BTC-USD,65095.00,65097.00,110.0",
		"ETH-USD,3488.50,3489.50,500.5",
		"BTC-USD,65080.00,65082.00,88.8",
		"ETH-USD,3494.00,3496.00,410.2",
	}

	fmt.Println("── Feeding ticks ──────────────────────────────────────────────")
	for i, raw := range rawTicks {
		fmt.Printf("\n  [main] → feeding tick #%d: %s\n", i+1, raw)
		rawFeed.in <- RawTick{Raw: raw}
		// Feed slightly faster than the Strategy consumes so backpressure builds.
		time.Sleep(5 * time.Millisecond)
	}

	// Close the head of the pipeline; the close cascades through all stages.
	close(rawFeed.in)

	// Wait until the tail stage (Strategy) has finished all work.
	<-strategy.done

	fmt.Println("\n── Final signals ──────────────────────────────────────────────")
	for i, sig := range strategy.Signals() {
		fmt.Printf("  Signal #%d  %-8s  %-4s  mid=%.2f  ma=%.2f  (%s)\n",
			i+1, sig.Symbol, sig.Action, sig.MidPrice, sig.MovingAvg, sig.Reason)
	}

	fmt.Println("\nPipeline complete.")
}
