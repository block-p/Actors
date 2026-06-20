# Messages — The Only Interface Between Actors

> **Core principle:** If two actors never share memory and only communicate through
> messages, then the message itself IS the entire interface. There is no other way
> in or out. This is not a limitation — it is the source of all safety.

---

## 1. Messages Are the ONLY Interface

### Why This Matters

In object-oriented code you call methods on objects: `order.Fill(qty, price)`.
The object is right there in memory. You reach in and change it.

In the actor model you **cannot do that**. The actor lives inside its own goroutine.
Its state is local variables that no other code can name, let alone touch.
The *only* way to interact with it is to drop a message in its mailbox channel.

This is the guarantee that eliminates entire categories of bugs:

- No race conditions on actor state (you can't race on data you can't access)
- No deadlocks from lock ordering (there are no locks)
- No surprising state mutations (every mutation is a named, traceable message)

### Why Messages Must Be Immutable

Once a message leaves your hands and lands in a channel, you have lost control
of when it will be read. The actor might process it immediately or in 50ms.
If you hold a pointer into the message and mutate the underlying data in the
meantime, the actor reads stale or corrupted values.

**Rule: never put a pointer to mutable shared state inside a message.**

### Value Types vs Pointer Types

Go's type system gives you value semantics by default. A struct passed by value
is *copied*. The copy is independent. The actor owns its copy; you own yours.

```go
// SAFE — value type, copied on send
type PlaceOrder struct {
    OrderID string
    Symbol  string
    Qty     float64
    Price   float64
    Side    string
}

actor <- PlaceOrder{
    OrderID: "ord-001",
    Symbol:  "BTC/USDT",
    Qty:     0.5,
    Price:   68_000.00,
    Side:    "buy",
}
// After this line you cannot corrupt what the actor received.
// The struct was copied into the channel buffer.
```

```go
// DANGEROUS — pointer type
type OrderData struct {
    Symbol string
    Qty    float64
}

type PlaceOrderPtr struct {
    Data *OrderData   // ← pointer to shared memory
}

data := &OrderData{Symbol: "BTC/USDT", Qty: 0.5}
actor <- PlaceOrderPtr{Data: data}

// NOW both you and the actor hold a pointer to the same OrderData.
// If you write data.Qty = 0.0 on this goroutine while the actor
// is reading data.Qty, you have a DATA RACE.
data.Qty = 0.0  // ← you just corrupted the actor's message
```

### Race Condition Example — What Actually Goes Wrong

```go
package main

import (
    "fmt"
    "sync"
    "time"
)

// The BAD message type — contains a pointer
type BadFillMessage struct {
    Fill *FillData
}

type FillData struct {
    Symbol string
    Qty    float64
    Price  float64
}

func corruptedActorLoop(mailbox chan any) {
    for msg := range mailbox {
        switch m := msg.(type) {
        case BadFillMessage:
            // Simulate some processing time
            time.Sleep(1 * time.Millisecond)
            // By the time we read this, the sender may have mutated Fill
            fmt.Printf("Processing fill: qty=%.2f price=%.2f\n",
                m.Fill.Qty, m.Fill.Price)
        }
    }
}

func main() {
    mailbox := make(chan any, 10)
    go corruptedActorLoop(mailbox)

    fill := &FillData{Symbol: "BTC/USDT", Qty: 1.0, Price: 68_000.0}
    mailbox <- BadFillMessage{Fill: fill}

    // Sender mutates the struct after sending — actor reads garbage
    fill.Qty = 999.0    // DATA RACE: go run -race will catch this
    fill.Price = 0.01

    time.Sleep(10 * time.Millisecond)
    // Actor prints: qty=999.00 price=0.01  ← wrong!
}
```

Run this with `go run -race main.go` and the race detector fires immediately.

**The fix is trivial:** copy the data into the struct, not the pointer.

```go
// SAFE — value copy
type GoodFillMessage struct {
    Symbol string
    Qty    float64
    Price  float64
}

// No pointer. No shared memory. No race.
mailbox <- GoodFillMessage{Symbol: "BTC/USDT", Qty: 1.0, Price: 68_000.0}
```

### When Pointers Are Acceptable

There is one legitimate use of pointers in messages: **read-only, deeply immutable
data** (e.g., a large snapshot you know will never be modified after creation, or
a `*sync.WaitGroup` / reply channel for coordination). In those cases, document
the contract clearly. For normal message data, use value types.

---

## 2. Three Categories of Messages

Every message in an actor system falls into one of three roles.
Understanding which role a message plays tells you how to design it,
whether it needs a reply, and what the receiver should do with it.

### Command — "Do This Thing"

A command tells an actor to **perform an action and change its state**.
The sender does not expect a reply — it's fire and forget.

```
Sender ──── PlaceOrder ────▶ OrderActor
                              (updates internal order book)
```

Commands are named with an imperative verb: `PlaceOrder`, `CancelOrder`,
`ResetRiskLimits`, `SuspendTrading`.

```go
// Commands — imperative verbs, no ReplyTo field

type PlaceOrder struct {
    CorrelationID string
    Symbol        string
    Side          string // "buy" or "sell"
    Qty           float64
    Price         float64
    Timestamp     time.Time
}

type CancelOrder struct {
    CorrelationID string
    OrderID       string
    Reason        string
}

type ResetRiskLimits struct {
    CorrelationID  string
    MaxPositionUSD float64
}

// Sending commands — sender moves on immediately
orderActor <- PlaceOrder{
    CorrelationID: "req-abc",
    Symbol:        "ETH/USDT",
    Side:          "buy",
    Qty:           2.0,
    Price:         3_800.0,
    Timestamp:     time.Now(),
}
// We don't wait. We don't block. We move on.
```

### Query — "Tell Me Something"

A query asks an actor to **read its state and return a value**.
It always includes a `ReplyTo` channel — without it, the answer goes nowhere.

```
Caller ──── GetBalance{Reply} ────▶ AccountActor
Caller ◀──────── Balance ──────────
```

Queries are named with a noun or `Get` prefix: `GetBalance`, `GetOrderBook`,
`GetPosition`, `GetRiskMetrics`.

```go
// Queries — always include a Reply channel

type GetBalance struct {
    AccountID string
    Reply     chan BalanceResult
}

type BalanceResult struct {
    AccountID  string
    Available  float64
    Locked     float64
    TotalUSD   float64
    AsOf       time.Time
}

type GetOrderBook struct {
    Symbol string
    Depth  int
    Reply  chan OrderBookResult
}

type OrderBookResult struct {
    Symbol string
    Bids   []PriceLevel
    Asks   []PriceLevel
    AsOf   time.Time
}

// Sending a query
reply := make(chan BalanceResult, 1)  // buffered — critical!
accountActor <- GetBalance{AccountID: "acct-001", Reply: reply}

select {
case result := <-reply:
    fmt.Printf("Available: $%.2f\n", result.Available)
case <-time.After(2 * time.Second):
    fmt.Println("ERROR: account actor did not respond in time")
}
```

### Event — "This Happened"

An event **announces something that occurred**. Events are past-tense facts.
They do not have a reply channel — they are notifications, not requests.
Multiple subscribers might receive the same event (broadcast pattern).

```
ExchangeActor ──── OrderFilled ────▶ RiskActor
                                ──▶ LoggerActor
                                ──▶ StrategyActor
```

Events are named in past tense: `OrderFilled`, `PriceChanged`, `PositionUpdated`,
`ConnectionLost`.

```go
// Events — past-tense names, no Reply, always include Timestamp

type OrderFilled struct {
    OrderID       string
    Symbol        string
    Side          string
    FilledQty     float64
    FilledPrice   float64
    RemainingQty  float64
    Timestamp     time.Time
}

type PriceChanged struct {
    Symbol    string
    OldPrice  float64
    NewPrice  float64
    Timestamp time.Time
}

type ConnectionLost struct {
    Exchange  string
    Reason    string
    Timestamp time.Time
}

// Publishing an event to all interested actors
event := OrderFilled{
    OrderID:     "ord-001",
    Symbol:      "BTC/USDT",
    FilledQty:   0.5,
    FilledPrice: 68_050.0,
    Timestamp:   time.Now(),
}

riskActor     <- event  // risk actor needs to update positions
loggerActor   <- event  // logger persists the fill
strategyActor <- event  // strategy might place a follow-up order
```

### Summary Table

| Category | Named       | Has ReplyTo | Sender waits? | Example            |
|----------|-------------|-------------|---------------|--------------------|
| Command  | Imperative  | No          | Never         | `PlaceOrder`       |
| Query    | Noun/GetX   | Always      | Yes           | `GetBalance`       |
| Event    | Past tense  | Never       | Never         | `OrderFilled`      |

---

## 3. Fire and Forget vs Ask

### Fire and Forget

You send a message and immediately continue with your work. You trust that
the actor will handle it. This is the default in actor systems — it is the
pattern that gives you throughput.

```go
// Fire and forget — simple, fast, non-blocking
orderActor <- PlaceOrder{Symbol: "BTC/USDT", Side: "buy", Qty: 1.0}
cancelActor <- CancelOrder{OrderID: "ord-001"}
logActor    <- LogEvent{Level: "INFO", Message: "strategy started"}

// All three are in flight. We moved on immediately.
// We have no guarantee about WHEN they will be processed,
// only that they will be processed IN ORDER per actor.
```

The tradeoff: you never know if the actor succeeded, failed, or even received
the message (if its mailbox was full and you used a non-blocking send).

### Ask (Request/Response)

Sometimes you genuinely need a result before you can continue. Ask means:
"I send you a query AND I wait here until you answer."

```go
// Ask pattern — you block until you get the reply
reply := make(chan PositionResult, 1)
positionActor <- GetPosition{Symbol: "BTC/USDT", Reply: reply}
position := <-reply   // block here
fmt.Printf("Current position: %.4f BTC\n", position.Qty)
```

### Why Ask Is Harder Than It Looks

Fire and forget is simple because you don't care about the result.
Ask has several failure modes you must handle:

1. **The actor crashes** before processing your query — reply channel never receives
2. **The actor is overwhelmed** and processes it in 30s — you've been blocked 30s
3. **You forget the timeout** and block forever — your goroutine leaks
4. **You forget to buffer the reply channel** — if you time out and leave, the actor
   blocks forever trying to send its reply

All of these failures have solutions, but you must actively design for them.
That's why you should always prefer fire-and-forget when you can, and only use
Ask when the result is truly necessary to continue.

### Comparing Both Patterns in Code

```go
package main

import (
    "fmt"
    "time"
)

// Messages
type Increment struct{ Amount int }
type GetCount  struct{ Reply chan int }

// A simple counter actor
func newCounter() chan any {
    mailbox := make(chan any, 100)
    go func() {
        count := 0
        for msg := range mailbox {
            switch m := msg.(type) {
            case Increment:
                count += m.Amount
            case GetCount:
                m.Reply <- count
            }
        }
    }()
    return mailbox
}

func main() {
    counter := newCounter()

    // --- Pattern 1: Fire and Forget ---
    // Send these and immediately continue. No waiting.
    counter <- Increment{Amount: 10}
    counter <- Increment{Amount: 5}
    counter <- Increment{Amount: 3}
    fmt.Println("All increments sent (fire and forget)")

    // --- Pattern 2: Ask (Request/Response) ---
    // We NEED the count, so we Ask and wait.
    reply := make(chan int, 1)  // buffered size 1 — explained below
    counter <- GetCount{Reply: reply}

    // Block here until we get the answer
    count := <-reply
    fmt.Printf("Current count: %d\n", count)
}
```

---

## 4. The Ask Pattern with Timeout — CRITICAL

The single most important rule about Ask:

> **Always put a timeout. Always. No exceptions.**

Without a timeout, you are one crashed actor away from a goroutine that blocks
forever. In a trade engine that runs 24/7, a goroutine leak means you slowly
run out of memory until the process dies at 3am on a Friday.

### The Full Pattern

```go
func askWithTimeout(actor chan any, query GetCount, timeout time.Duration) (int, error) {
    // Step 1: Create a BUFFERED reply channel.
    // Buffer size 1 is the only correct size.
    reply := make(chan int, 1)

    // Step 2: Send the query with our reply channel.
    actor <- GetCount{Reply: reply}

    // Step 3: Wait for reply OR timeout — never block indefinitely.
    select {
    case result := <-reply:
        return result, nil
    case <-time.After(timeout):
        // The actor did not reply in time.
        // The buffered channel means the actor can still send its (now-ignored)
        // reply without blocking. If reply were unbuffered (size 0), the actor
        // would block on `reply <- count` forever — a goroutine leak in the ACTOR.
        return 0, fmt.Errorf("ask timed out after %s", timeout)
    }
}
```

### Why the Reply Channel MUST Be Buffered (Size 1)

This is subtle but critical. Consider what happens with an **unbuffered** reply
channel when the caller times out:

```
Time 0ms:  Caller sends GetCount{Reply: make(chan int)} to actor's mailbox
Time 0ms:  Caller enters select, waiting for reply or timeout
Time 2000ms: Timeout fires! Caller goroutine unblocks and moves on.
             The reply channel is now orphaned — nobody is reading from it.
Time 2001ms: Actor finally processes GetCount, tries to send: reply <- count
             BLOCKS FOREVER — nobody is reading the unbuffered channel.
             The actor goroutine is now LEAKED.
```

With a **buffered** reply channel of size 1:

```
Time 2001ms: Actor sends: reply <- count
             The buffer absorbs it. Actor does NOT block. Actor moves on.
             The orphaned channel gets garbage collected eventually.
```

```go
// WRONG — unbuffered reply channel
reply := make(chan int)   // size 0 = unbuffered = goroutine leak risk

// CORRECT — buffered reply channel, always size 1
reply := make(chan int, 1)  // size 1 = actor can always send without blocking
```

### A Complete Ask-with-Timeout Example

```go
func askBalance(accountActor chan any, accountID string) (float64, error) {
    reply := make(chan BalanceResult, 1)

    accountActor <- GetBalance{
        AccountID: accountID,
        Reply:     reply,
    }

    select {
    case result := <-reply:
        return result.Available, nil

    case <-time.After(2 * time.Second):
        // Don't panic. Don't crash. Return an error.
        // The buffered channel lets the actor send its (now-ignored) reply
        // without blocking, so the actor continues working normally.
        return 0, fmt.Errorf("account actor timeout: accountID=%s", accountID)
    }
}
```

### Retry Logic on Timeout

For transient failures (actor temporarily overloaded), a simple retry helps:

```go
func askWithRetry(actor chan any, q GetCount, timeout time.Duration, maxRetries int) (int, error) {
    for attempt := 1; attempt <= maxRetries; attempt++ {
        reply := make(chan int, 1)
        actor <- q
        // Note: in a real system you'd send a fresh query each time,
        // not re-send the same struct (the Reply channel reference must be fresh)
        actor <- GetCount{Reply: reply}

        select {
        case result := <-reply:
            return result, nil
        case <-time.After(timeout):
            if attempt < maxRetries {
                fmt.Printf("attempt %d/%d timed out, retrying...\n", attempt, maxRetries)
                continue
            }
        }
    }
    return 0, fmt.Errorf("all %d attempts timed out", maxRetries)
}
```

---

## 5. The Poison Pill — Graceful Shutdown via Message

An actor runs until its mailbox channel is closed, or until it explicitly
returns from its receive loop. The cleanest way to stop an actor from outside
is to send it a special **"stop" message** — the Poison Pill.

### Why Not Just Close the Channel?

You *could* close the mailbox channel to stop the actor. The `for range` loop
will exit when the channel closes. But this has problems:

1. Any sender that tries to send after close will **panic** — channel sends on
   closed channels are a runtime panic in Go.
2. If multiple goroutines send to the actor, you'd need coordination to safely
   close the channel.
3. It gives the actor no chance to do cleanup work first.

The Poison Pill solves all three: it's just another message, it arrives in order,
and it triggers whatever cleanup you want before stopping.

### The Done Channel Pattern

The actor signals "I am fully stopped and cleaned up" by closing a `Done` channel
that was passed in the stop message.

```go
// The stop message carries a Done channel
type StopActor struct {
    Done chan struct{}
}

// Actor loop handling StopActor
func NewWorkerActor() chan any {
    mailbox := make(chan any, 100)

    go func() {
        // private state
        pendingOrders := make(map[string]Order)

        for msg := range mailbox {
            switch m := msg.(type) {

            case PlaceOrder:
                pendingOrders[m.OrderID] = Order{Symbol: m.Symbol, Qty: m.Qty}

            case StopActor:
                // PostStop: flush everything before dying
                for id, order := range pendingOrders {
                    fmt.Printf("flushing pending order %s: %s %.2f\n",
                        id, order.Symbol, order.Qty)
                }
                // Signal that we are FULLY stopped.
                // Closing broadcasts to ALL waiters simultaneously.
                close(m.Done)
                return  // exit the goroutine
            }
        }
    }()

    return mailbox
}

// How to use it — clean, deterministic shutdown
func main() {
    worker := NewWorkerActor()

    worker <- PlaceOrder{OrderID: "ord-001", Symbol: "BTC/USDT", Qty: 1.0}
    worker <- PlaceOrder{OrderID: "ord-002", Symbol: "ETH/USDT", Qty: 5.0}

    // Stop the actor and wait for it to fully finish
    done := make(chan struct{})
    worker <- StopActor{Done: done}
    <-done  // blocks until the actor closes done
    fmt.Println("actor has fully stopped")
}
```

### Why `close(done)` Is Better Than Sending a Boolean

```go
// Option A: send a boolean
done <- true     // only ONE waiter can receive this

// Option B: close the channel
close(done)      // ALL waiters unblock simultaneously
```

`close` broadcasts. If your supervisor, your metrics collector, AND your test
all want to know when the actor stopped, `close` notifies all three. A send
only notifies one.

Also, you cannot "un-close" a channel, which makes it a permanent, safe signal:
any goroutine that calls `<-done` after the actor has already stopped returns
immediately with the zero value. This is idempotent — no race.

---

## 6. Message Ordering

### What Ordering Guarantees Does Go Give You?

Within a single unbuffered or buffered channel: **FIFO**. Messages arrive in
the order they were sent. This is guaranteed by the Go memory model.

This means:

```
Sender ──┬── msg1 ──▶ Actor    Actor receives: msg1, msg2, msg3 (in order)
         ├── msg2 ──▶ Actor
         └── msg3 ──▶ Actor
```

If a single sender puts `msg1`, then `msg2`, then `msg3`, the actor will
process them in that exact order. **This guarantee holds for one sender.**

### Multiple Senders — Order Is NOT Guaranteed

When two or more goroutines send to the same actor mailbox, Go gives you
no guarantee about the order their messages interleave:

```
SenderA ──── fillA ──▶ Actor     Actor might receive: fillA, fillB
SenderB ──── fillB ──▶ Actor     OR: fillB, fillA
                                 Depends on goroutine scheduling
```

### Why This Matters for a Trade Engine

Imagine two exchange connectors both sending fills to your position actor:

```
BinanceConnector ──── OrderFilled{OrderID:"x", Qty:1.0} ──▶ PositionActor
CoinbaseConnector ─── OrderFilled{OrderID:"y", Qty:0.5} ──▶ PositionActor
```

If your position actor processes fills in a different order on different runs,
and each fill triggers a risk check that depends on the running total, you get
non-deterministic behavior. Your position calculations will be correct in sum,
but events that depend on intermediate states (circuit breakers, alerts) might
fire at different times on different runs.

**Practical consequence:** never design logic that depends on the relative
ordering of messages from *different* senders. Design for commutativity
(order doesn't matter) or use a single sender to enforce ordering.

```go
// PROBLEMATIC: two senders, order-dependent logic
go binanceConnector.SendFills(positionActor)
go coinbaseConnector.SendFills(positionActor)
// PositionActor has non-deterministic fill ordering

// BETTER: single aggregator enforces ordering
go func() {
    for {
        select {
        case fill := <-binanceFills:
            positionActor <- fill
        case fill := <-coinbaseFills:
            positionActor <- fill
        }
    }
}()
// Now there is one sender. Order is stable within each exchange's stream.
```

---

## 7. Designing Your Messages for a Real System

### Use Structs, Not Primitives

A message like `chan int` tells you nothing. A message like:

```go
type GetBalance struct {
    AccountID string
    Currency  string
    Reply     chan BalanceResult
}
```

is self-documenting. When you look at the actor's message handler three months
from now, the struct tells you exactly what the query is about without needing
to trace back to the call site.

### Always Include Correlation IDs

Distributed systems and actor systems share a problem: a request passes through
many actors and you need to trace it end-to-end. A `CorrelationID` (or `TraceID`,
`RequestID`) threads through every message in a chain.

```go
type PlaceOrder struct {
    CorrelationID string  // ← trace this through the whole system
    Symbol        string
    Side          string
    Qty           float64
    Price         float64
}

// When this triggers a fill, the fill carries the same ID
type OrderFilled struct {
    CorrelationID string  // ← same ID as the PlaceOrder that caused this
    OrderID       string
    FilledQty     float64
    FilledPrice   float64
    Timestamp     time.Time
}
```

Now when something goes wrong, you can search your logs for the correlation ID
and see the entire causal chain across all actors.

### Include a Timestamp in Events

Events represent facts that happened in the real world. The timestamp of the
fact is part of the fact. Do not use the timestamp of when the message was
processed — use the timestamp of when the event occurred.

```go
type PriceChanged struct {
    Symbol     string
    NewPrice   float64
    // When the exchange reported this price change
    ExchangeTimestamp time.Time
    // When we received it (for latency measurement)
    ReceivedAt        time.Time
}
```

### A Realistic Trade Engine Message Set

```go
package messages

import "time"

// ── COMMANDS ────────────────────────────────────────────────────────────────

// PlaceOrder tells the order manager to submit an order to the exchange.
type PlaceOrder struct {
    CorrelationID string
    Symbol        string    // e.g. "BTC/USDT"
    Side          string    // "buy" or "sell"
    OrderType     string    // "limit" or "market"
    Qty           float64
    LimitPrice    float64   // 0 if market order
    TimeInForce   string    // "GTC", "IOC", "FOK"
    Timestamp     time.Time
}

// CancelOrder tells the order manager to cancel an open order.
type CancelOrder struct {
    CorrelationID string
    OrderID       string
    Symbol        string
    Reason        string
    Timestamp     time.Time
}

// UpdateRiskLimits tells the risk actor to change its limits.
type UpdateRiskLimits struct {
    CorrelationID     string
    MaxPositionSizeUSD float64
    MaxDailyLossUSD    float64
    Timestamp          time.Time
}

// ── QUERIES ─────────────────────────────────────────────────────────────────

// GetOpenOrders asks the order manager for all open orders on a symbol.
type GetOpenOrders struct {
    CorrelationID string
    Symbol        string
    Reply         chan OpenOrdersResult
}

type OpenOrdersResult struct {
    Symbol string
    Orders []OpenOrder
    AsOf   time.Time
}

type OpenOrder struct {
    OrderID   string
    Side      string
    Qty       float64
    Price     float64
    Filled    float64
}

// GetPosition asks the position tracker for the current position.
type GetPosition struct {
    CorrelationID string
    Symbol        string
    Reply         chan PositionResult
}

type PositionResult struct {
    Symbol      string
    NetQty      float64  // positive = long, negative = short
    AvgCost     float64
    UnrealizedPnL float64
    AsOf        time.Time
}

// ── EVENTS ──────────────────────────────────────────────────────────────────

// OrderFilled is published when the exchange confirms a fill.
type OrderFilled struct {
    CorrelationID     string
    OrderID           string
    Symbol            string
    Side              string
    FilledQty         float64
    FilledPrice       float64
    RemainingQty      float64
    ExchangeTimestamp time.Time
    ReceivedAt        time.Time
}

// OrderRejected is published when the exchange rejects an order.
type OrderRejected struct {
    CorrelationID string
    OrderID       string
    Symbol        string
    Reason        string
    Timestamp     time.Time
}

// PriceUpdated is published when we receive a new best bid/ask from the exchange.
type PriceUpdated struct {
    Symbol            string
    BestBid           float64
    BestAsk           float64
    ExchangeTimestamp time.Time
    ReceivedAt        time.Time
}

// RiskLimitBreached is published when a trade would exceed risk limits.
type RiskLimitBreached struct {
    CorrelationID string
    Symbol        string
    LimitType     string  // "max_position", "max_daily_loss", etc.
    CurrentValue  float64
    LimitValue    float64
    Timestamp     time.Time
}

// ── LIFECYCLE ────────────────────────────────────────────────────────────────

// StopActor is the Poison Pill — sends this to gracefully stop any actor.
type StopActor struct {
    Done chan struct{}
}
```

### The Rule of Three Questions

Before finalising any message type, answer:

1. **What category is it?** Command / Query / Event
2. **Does it have a correlation ID?** (If not, add one)
3. **If it's an event — does it have a timestamp?** (If not, add one)

If you answer these three questions for every message, your system will be
debuggable, traceable, and correct by design.

---

## Quick Reference

```
Message Type  │  Name Style    │  Has ReplyTo  │  Sender blocks?  │  Example
──────────────┼────────────────┼───────────────┼──────────────────┼──────────────────
Command       │  Imperative    │  No           │  Never           │  PlaceOrder
Query         │  Noun/GetX     │  Always       │  Yes + timeout   │  GetBalance
Event         │  Past tense    │  Never        │  Never           │  OrderFilled

Ask Pattern:
  reply := make(chan T, 1)        ← buffered, always size 1
  actor <- Query{Reply: reply}
  select {
    case r := <-reply: ...        ← got answer
    case <-time.After(2s): ...    ← actor didn't answer in time
  }

Stop Pattern:
  done := make(chan struct{})
  actor <- StopActor{Done: done}
  <-done                          ← wait for full stop
```

Next → `04-lifecycle.md` covers the full lifecycle of an actor: birth, running, and graceful death.
