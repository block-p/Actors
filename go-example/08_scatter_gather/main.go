// 08_scatter_gather — Scatter-Gather (Fan-Out / Fan-In) Pattern
//
// A PriceAggregatorActor fans out a price query to N exchange actors
// simultaneously, then fans in all responses within a deadline.
//
// WHY THIS PATTERN IS ESSENTIAL FOR BEST EXECUTION
// --------------------------------------------------
// In trading, "best execution" means buying at the lowest ask and selling at
// the highest bid across all available venues.  Querying exchanges sequentially
// would be too slow (500 ms × 4 exchanges = 2 s per query).
//
// With scatter-gather:
//   1. SCATTER — send GetPrice to all 4 exchanges at the same time (goroutines).
//   2. GATHER  — collect responses into a single channel with a timeout.
//   3. DECIDE  — pick the best bid (highest) and best ask (lowest) from whoever
//                responded in time.
//
// If Kraken is slow today, we still get best execution from Binance, Coinbase,
// and OKX.  The timeout is a first-class citizen of the design.
//
// This is identical to what real Smart Order Routers (SORs) do.

package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

// GetBestPrice is sent by a client to the aggregator.
type GetBestPrice struct {
	Symbol string
	Reply  chan BestPriceResult
}

// ExchangePrice is the response an exchange actor sends back.
type ExchangePrice struct {
	Exchange string
	Symbol   string
	Bid      float64
	Ask      float64
}

// BestPriceResult is what the aggregator returns to the client.
type BestPriceResult struct {
	BestBid         float64
	BestBidExchange string
	BestAsk         float64
	BestAskExchange string
	Responded       []string // exchanges that replied in time
	TimedOut        []string // exchanges that did not reply in time
}

// internalGetPrice is the message the aggregator sends to each exchange.
// The reply channel is per-query so there is no cross-query interference.
type internalGetPrice struct {
	Symbol string
	Reply  chan ExchangePrice
}

// ---------------------------------------------------------------------------
// Exchange actors
// ---------------------------------------------------------------------------

// exchangeActor simulates one exchange's price feed.
// Each exchange has:
//   - a base bid/ask that drifts slightly each call
//   - a simulated latency
//   - a timeout probability (Kraken sometimes doesn't reply in time)

type exchangeActor struct {
	name        string
	baseBid     float64
	baseAsk     float64
	latency     time.Duration // simulated network round-trip
	timeoutProb float64       // probability [0,1] of not replying at all
	mailbox     chan internalGetPrice
	stop        chan struct{}
}

func newExchangeActor(name string, baseBid, baseAsk float64, latency time.Duration, timeoutProb float64) *exchangeActor {
	e := &exchangeActor{
		name:        name,
		baseBid:     baseBid,
		baseAsk:     baseAsk,
		latency:     latency,
		timeoutProb: timeoutProb,
		mailbox:     make(chan internalGetPrice, 16),
		stop:        make(chan struct{}),
	}
	go e.run()
	return e
}

func (e *exchangeActor) run() {
	for {
		select {
		case req := <-e.mailbox:
			// Simulate whether this exchange "times out" on this request.
			if rand.Float64() < e.timeoutProb {
				// Don't reply — the aggregator will time out waiting for us.
				fmt.Printf("  [%s] ⚠ simulating timeout for %s (no reply sent)\n", e.name, req.Symbol)
				continue
			}
			// Simulate network latency before responding.
			go func(r internalGetPrice) {
				time.Sleep(e.latency)
				// Add a small random drift to prices (±0.5%).
				drift := (rand.Float64()*2 - 1) * 0.005
				bid := e.baseBid * (1 + drift)
				ask := e.baseAsk * (1 + drift)
				r.Reply <- ExchangePrice{
					Exchange: e.name,
					Symbol:   r.Symbol,
					Bid:      bid,
					Ask:      ask,
				}
			}(req)
		case <-e.stop:
			return
		}
	}
}

// ---------------------------------------------------------------------------
// PriceAggregatorActor
// ---------------------------------------------------------------------------

// PriceAggregatorActor is the scatter-gather coordinator.
// It holds references to all exchange actors and fans out price queries.

type PriceAggregatorActor struct {
	exchanges []*exchangeActor
	mailbox   chan GetBestPrice
	stop      chan struct{}
	timeout   time.Duration // how long to wait for exchange replies
}

func newPriceAggregatorActor(exchanges []*exchangeActor, timeout time.Duration) *PriceAggregatorActor {
	a := &PriceAggregatorActor{
		exchanges: exchanges,
		mailbox:   make(chan GetBestPrice, 16),
		stop:      make(chan struct{}),
		timeout:   timeout,
	}
	go a.run()
	return a
}

func (a *PriceAggregatorActor) run() {
	for {
		select {
		case req := <-a.mailbox:
			result := a.scatterGather(req.Symbol)
			req.Reply <- result
		case <-a.stop:
			return
		}
	}
}

// scatterGather is the core of the pattern.
func (a *PriceAggregatorActor) scatterGather(symbol string) BestPriceResult {
	// One shared reply channel for all exchange responses to this query.
	// Buffer = number of exchanges so no goroutine ever blocks trying to reply.
	replyCh := make(chan ExchangePrice, len(a.exchanges))

	// ── SCATTER ─────────────────────────────────────────────────────────────
	// Send the query to all exchanges simultaneously.
	fmt.Printf("  [Aggregator] scatter → querying %d exchanges for %s\n",
		len(a.exchanges), symbol)
	for _, ex := range a.exchanges {
		ex.mailbox <- internalGetPrice{Symbol: symbol, Reply: replyCh}
	}

	// ── GATHER ──────────────────────────────────────────────────────────────
	// Collect responses until the deadline or all exchanges have replied.
	deadline := time.NewTimer(a.timeout)
	defer deadline.Stop()

	var (
		responded []string
		timedOut  []string
		prices    []ExchangePrice
		received  = make(map[string]bool)
	)

	// We expect at most len(exchanges) replies.
	for len(responded)+len(timedOut) < len(a.exchanges) {
		select {
		case price := <-replyCh:
			if !received[price.Exchange] { // deduplicate (shouldn't happen, but safe)
				received[price.Exchange] = true
				responded = append(responded, price.Exchange)
				prices = append(prices, price)
				fmt.Printf("  [Aggregator] ← got reply from %-10s  bid=%.2f  ask=%.2f\n",
					price.Exchange, price.Bid, price.Ask)
			}
		case <-deadline.C:
			// Timeout fired — any exchange that hasn't replied yet is "timed out".
			for _, ex := range a.exchanges {
				if !received[ex.name] {
					timedOut = append(timedOut, ex.name)
					fmt.Printf("  [Aggregator] ✗ timeout waiting for %s\n", ex.name)
				}
			}
			// Exit the gather loop.
			goto done
		}
	}

done:
	// ── DECIDE ──────────────────────────────────────────────────────────────
	// Pick the best bid (highest) and best ask (lowest) from all responses.
	result := BestPriceResult{
		Responded: responded,
		TimedOut:  timedOut,
	}
	for i, p := range prices {
		if i == 0 || p.Bid > result.BestBid {
			result.BestBid = p.Bid
			result.BestBidExchange = p.Exchange
		}
		if i == 0 || p.Ask < result.BestAsk {
			result.BestAsk = p.Ask
			result.BestAskExchange = p.Exchange
		}
	}
	return result
}

// GetBestPriceSync is the client-facing API — blocks until the result arrives.
func (a *PriceAggregatorActor) GetBestPriceSync(symbol string) BestPriceResult {
	reply := make(chan BestPriceResult, 1)
	a.mailbox <- GetBestPrice{Symbol: symbol, Reply: reply}
	return <-reply
}

// Stop shuts down the aggregator and all exchange actors.
func (a *PriceAggregatorActor) Stop() {
	close(a.stop)
	for _, ex := range a.exchanges {
		close(ex.stop)
	}
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	rand.Seed(time.Now().UnixNano()) //nolint:staticcheck

	fmt.Println("╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║  08_scatter_gather — Best Price Aggregator                   ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")
	fmt.Println()
	fmt.Println("Kraken has a 60% chance of timing out each query.")
	fmt.Println("Timeout budget per query: 500 ms.")
	fmt.Println()

	// Create four exchange actors with different latencies and reliability.
	//
	//   Exchange   Latency   Timeout prob
	//   ─────────────────────────────────
	//   Binance    80 ms     0%   (always responds)
	//   Coinbase   120 ms    0%   (always responds)
	//   Kraken     200 ms    60%  (often too slow)
	//   OKX        90 ms     0%   (always responds)
	exchanges := []*exchangeActor{
		newExchangeActor("Binance", 65100.00, 65102.00, 80*time.Millisecond, 0.00),
		newExchangeActor("Coinbase", 65098.50, 65103.50, 120*time.Millisecond, 0.00),
		newExchangeActor("Kraken", 65097.00, 65104.00, 200*time.Millisecond, 0.60),
		newExchangeActor("OKX", 65101.00, 65103.00, 90*time.Millisecond, 0.00),
	}

	aggregator := newPriceAggregatorActor(exchanges, 500*time.Millisecond)

	var wg sync.WaitGroup

	for q := 1; q <= 3; q++ {
		wg.Add(1)
		go func(queryNum int) {
			defer wg.Done()
			fmt.Printf("\n══ Query #%d ══════════════════════════════════════════════════\n", queryNum)
			result := aggregator.GetBestPriceSync("BTC-USD")
			printResult(queryNum, result)
		}(q)
		// Stagger queries slightly so output is readable.
		time.Sleep(600 * time.Millisecond)
	}

	wg.Wait()
	aggregator.Stop()
	fmt.Println("\nAll queries complete.")
}

func printResult(q int, r BestPriceResult) {
	fmt.Printf("\n  ── Query #%d result ────────────────────────────────────────\n", q)
	fmt.Printf("  Best Bid:  %.2f  (from %s)\n", r.BestBid, r.BestBidExchange)
	fmt.Printf("  Best Ask:  %.2f  (from %s)\n", r.BestAsk, r.BestAskExchange)
	fmt.Printf("  Responded: %v\n", r.Responded)
	if len(r.TimedOut) > 0 {
		fmt.Printf("  Timed out: %v  ← still got best execution from the rest\n", r.TimedOut)
	} else {
		fmt.Printf("  Timed out: none\n")
	}
}
