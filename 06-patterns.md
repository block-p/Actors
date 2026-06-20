# 06 — Actor Patterns: The Building Blocks of a Real System

> "Individual actors are simple. The patterns they form are powerful."
> You've learned what actors *are*. Now you learn how to *combine* them to solve hard problems.

---

## Table of Contents

1. [Pattern 1: Router](#1-pattern-1-router)
2. [Pattern 2: Finite State Machine (FSM) Actor](#2-pattern-2-finite-state-machine-fsm-actor)
3. [Pattern 3: Request-Response with Correlation IDs](#3-pattern-3-request-response-with-correlation-ids)
4. [Pattern 4: Pipeline](#4-pattern-4-pipeline)
5. [Pattern 5: Scatter-Gather (Fan-Out / Fan-In)](#5-pattern-5-scatter-gather-fan-out--fan-in)
6. [Pattern 6: Dead Letter Box](#6-pattern-6-dead-letter-box)
7. [Pattern 7: Saga (Distributed Transactions)](#7-pattern-7-saga-distributed-transactions)

---

## 1. Pattern 1: Router

### Why Before How

Imagine your trade engine is receiving 50,000 order messages per second from customers. You have one `OrderWorker` actor. It can process 10,000 per second. You are dropping 40,000 messages every second.

The naive fix is "make the actor faster." But there's a physical ceiling. CPUs have cores, not magic. The correct fix is: **run more workers in parallel and distribute the load**.

A **Router** is the actor that sits in front of a pool of workers and answers one question: *which worker should handle this message?*

```
                         ┌─────────────────┐
                         │                 │
                    ┌───►│   Worker-1      │
                    │    │                 │
                    │    └─────────────────┘
                    │
┌───────────────┐   │    ┌─────────────────┐
│               │   │    │                 │
│  Router Actor ├───┼───►│   Worker-2      │
│               │   │    │                 │
└───────────────┘   │    └─────────────────┘
        ▲           │
        │           │    ┌─────────────────┐
   incoming         │    │                 │
   messages         └───►│   Worker-3      │
                         │                 │
                         └─────────────────┘
```

The router itself does **no business logic**. It only routes. This is the single responsibility principle applied to concurrency.

---

### Three Routing Strategies

#### Strategy 1: Round-Robin

Send to workers in strict rotation. Worker 1 gets message 1, Worker 2 gets message 2, Worker 3 gets message 3, Worker 1 gets message 4, and so on.

```
Message stream:   [M1] [M2] [M3] [M4] [M5] [M6] [M7] [M8] [M9]
                   │    │    │    │    │    │    │    │    │
Worker-1:         M1              M4              M7
Worker-2:              M2              M5              M8
Worker-3:                   M3              M6              M9

Index counter:     0    1    2    0    1    2    0    1    2
```

**Pro:** Perfectly even distribution when all messages take the same time.
**Con:** Makes no guarantees about ordering per logical key. M1 and M4 both go to Worker-1, but M2 and M5 (which might be for the same symbol!) go to Worker-2. If ordering per symbol matters, this breaks it.

---

#### Strategy 2: Random

Pick a worker at random from the pool on each message.

```
Message stream:   [M1] [M2] [M3] [M4] [M5] [M6]
                   │    │    │    │    │    │
Worker-1:         M1        M3        M5
Worker-2:              M2             
Worker-3:                        M4        M6

(random picks: 0, 1, 0, 2, 0, 2)
```

**Pro:** No hot spots — if one worker is temporarily busy, others absorb load naturally.
**Con:** Same as round-robin: no ordering guarantees. Slightly worse distribution than round-robin statistically for small N, equivalent for large N (law of large numbers).

**Use case:** Stateless, embarrassingly parallel work where ordering never matters. Example: sending notifications, logging events.

---

#### Strategy 3: Hash Routing (The One That Matters Most for Trading)

Hash a *field* of the message (e.g., the trading symbol) and use that hash to deterministically pick a worker. The same key **always** goes to the same worker.

```
hash("BTC/USD") % 3 = 1  → always Worker-1
hash("ETH/USD") % 3 = 2  → always Worker-2
hash("SOL/USD") % 3 = 0  → always Worker-3

Message stream:
  [BTC/USD order] → Worker-1
  [ETH/USD order] → Worker-2
  [BTC/USD order] → Worker-1  ← same symbol, same worker
  [SOL/USD order] → Worker-3
  [BTC/USD order] → Worker-1  ← still Worker-1
  [ETH/USD order] → Worker-2  ← still Worker-2
```

Worker-1 is the **sole owner** of all BTC/USD state. Worker-2 owns all ETH/USD state. They never share or conflict.

```
┌─────────────────────────────────────────────────────────────┐
│                      HASH ROUTING                           │
│                                                             │
│  Incoming:  BTC  ETH  BTC  SOL  BTC  ETH  SOL  BTC         │
│              │    │    │    │    │    │    │    │           │
│              ▼    ▼    ▼    ▼    ▼    ▼    ▼    ▼           │
│           hash  hash  ...                                   │
│              │    │    │    │    │    │    │    │           │
│         ┌────┼────┼────┤    │    │    │    │               │
│         │    │    │    │    │    │    │    │               │
│         ▼    ▼    ▼    ▼    ▼    ▼    ▼    ▼               │
│       W-1  W-2  W-1  W-3  W-1  W-2  W-3  W-1              │
│         │         │         │         │                    │
│         ▼         ▼         ▼         ▼                    │
│       BTC/USD   ETH/USD   SOL/USD   BTC/USD                │
│       queue     queue     queue     queue                   │
│                                                             │
│  W-1 processes: BTC BTC BTC BTC  (in arrival order!)       │
│  W-2 processes: ETH ETH           (in arrival order!)      │
│  W-3 processes: SOL SOL           (in arrival order!)      │
└─────────────────────────────────────────────────────────────┘
```

### Why Hash Routing Is Critical for a Trade Engine

An order book is a *stateful* data structure. When two orders arrive for BTC/USD:

```
[t=1] Buy  BTC/USD 1.0 @ $50,000
[t=2] Sell BTC/USD 1.0 @ $50,000  ← should match with t=1
```

If these go to different workers:
- Worker-1 processes the Buy and adds it to its local order book.
- Worker-2 processes the Sell and looks in *its* order book — empty! No match.

The trade never happens. Your engine has a correctness bug, not just a performance bug.

With hash routing on symbol:
- Both messages go to the same worker.
- That worker's order book sees them in sequence.
- Match happens correctly.

```
WITHOUT HASH ROUTING:
  Buy  BTC → Worker-1 (has the buy, waiting)
  Sell BTC → Worker-2 (doesn't see the buy, no match)
  
  Result: ORDER BOOK SPLIT — correctness bug!

WITH HASH ROUTING:
  Buy  BTC → Worker-1 (symbol "BTC/USD" hashes to 1)
  Sell BTC → Worker-1 (same symbol, same hash, same worker)
  
  Result: Worker-1 matches them correctly.
```

---

### Go Code: Round-Robin Router

```go
package router

import "fmt"

// WorkerRef is a reference to a worker actor's mailbox.
type WorkerRef struct {
    name    string
    mailbox chan any
}

// Send delivers a message to the worker's mailbox.
func (w *WorkerRef) Send(msg any) {
    w.mailbox <- msg
}

// RouterActor distributes messages to a pool of workers using round-robin.
type RouterActor struct {
    workers      []*WorkerRef
    currentIndex int
    mailbox      chan any
}

// NewRouterActor creates a router with the given pool of workers.
func NewRouterActor(workers []*WorkerRef) *RouterActor {
    return &RouterActor{
        workers: workers,
        mailbox: make(chan any, 1024),
    }
}

// Distribute picks the next worker and forwards the message.
func (r *RouterActor) Distribute(msg any) {
    worker := r.workers[r.currentIndex]
    worker.Send(msg)

    // Advance the index, wrapping around to 0 when we pass the last worker.
    r.currentIndex = (r.currentIndex + 1) % len(r.workers)
}

// Run starts the router's receive loop.
func (r *RouterActor) Run() {
    for msg := range r.mailbox {
        r.Distribute(msg)
    }
}

// --- Hash Router (bonus) ---

// HashRouterActor routes based on the hash of a key extracted from the message.
type HashRouterActor struct {
    workers []*WorkerRef
    mailbox chan any
    keyFn   func(msg any) string // extracts the routing key from a message
}

// NewHashRouterActor creates a hash router.
// keyFn must return the string key to hash (e.g., the trading symbol).
func NewHashRouterActor(workers []*WorkerRef, keyFn func(any) string) *HashRouterActor {
    return &HashRouterActor{
        workers: workers,
        mailbox: make(chan any, 1024),
        keyFn:   keyFn,
    }
}

// hashKey converts a string key into a stable worker index.
func (h *HashRouterActor) hashKey(key string) int {
    sum := 0
    for _, c := range key {
        sum += int(c)
    }
    return sum % len(h.workers)
}

// Distribute routes msg to the worker determined by the key hash.
func (h *HashRouterActor) Distribute(msg any) {
    key := h.keyFn(msg)
    idx := h.hashKey(key)
    h.workers[idx].Send(msg)
}

// Run starts the hash router's receive loop.
func (h *HashRouterActor) Run() {
    for msg := range h.mailbox {
        h.Distribute(msg)
    }
}

// --- Example usage ---

// OrderMessage is an example trade message that carries a symbol.
type OrderMessage struct {
    Symbol   string
    Side     string // "buy" or "sell"
    Quantity float64
    Price    float64
}

// Example: wiring up a hash router for order execution.
func ExampleHashRouter() {
    workers := []*WorkerRef{
        {name: "worker-0", mailbox: make(chan any, 256)},
        {name: "worker-1", mailbox: make(chan any, 256)},
        {name: "worker-2", mailbox: make(chan any, 256)},
    }

    router := NewHashRouterActor(workers, func(msg any) string {
        if order, ok := msg.(OrderMessage); ok {
            return order.Symbol // hash on the trading symbol
        }
        return "default"
    })

    // All BTC/USD orders will always land on the same worker.
    router.mailbox <- OrderMessage{Symbol: "BTC/USD", Side: "buy",  Quantity: 1.0, Price: 50000}
    router.mailbox <- OrderMessage{Symbol: "ETH/USD", Side: "sell", Quantity: 5.0, Price: 3000}
    router.mailbox <- OrderMessage{Symbol: "BTC/USD", Side: "sell", Quantity: 1.0, Price: 50000}

    fmt.Println("messages queued")
}
```

### Trade Engine Use Case: Order Execution Workers

In a real trade engine you'd wire up the hash router like this:

```
  Incoming orders
        │
        ▼
  ┌─────────────┐
  │ HashRouter  │  keyFn = msg.Symbol
  └──────┬──────┘
         │
    ┌────┴──────────────┐
    │                   │
    ▼                   ▼
┌────────┐         ┌────────┐
│ Exec-0 │         │ Exec-1 │
│BTC/USD │         │ETH/USD │
│SOL/USD │         │ADA/USD │
└────────┘         └────────┘
   owns these         owns these
   order books        order books
```

Each executor actor owns the order book for its assigned symbols. No locking required. Horizontal scaling is as simple as adding more workers (though you must rehash — more on consistent hashing in advanced topics).

---

## 2. Pattern 2: Finite State Machine (FSM) Actor

### Why Before How

An actor is not just a message processor. It is an entity that lives through **states**. An order starts as `New`, moves to `Submitted`, then can be `Filled`, `Cancelled`, or `Rejected`. Some transitions are legal. Others are absurd.

Can a `Filled` order go back to `Submitted`? No.
Can a `Cancelled` order be `Filled`? No.
Can a `New` order be `PartiallyFilled` before it is submitted? No.

Without an FSM, these illegal transitions can happen silently due to bugs, race conditions, or misdelivered messages. The damage is invisible until reconciliation time — when your books don't balance.

With an FSM actor, illegal transitions are **impossible by construction**. The actor simply ignores (or logs) messages that don't make sense in the current state. The state machine is the spec; the code is the spec.

```
Without FSM:                      With FSM:

  bug sends "Fill" to a           actor checks: "am I in
  Cancelled order                 PartiallyFilled? No, I'm
                                  Cancelled. Ignoring Fill."
        ▼                                 ▼
  order gets double-filled        warning logged, state unchanged
  books are wrong                 books are correct
  discovered at month-end         discovered immediately
```

### Trade Order FSM Diagram

```
                   SubmitOrder
    New ──────────────────────────► Submitted
                                         │
              ┌──────────────────────────┼─────────────────────────┐
              │                          │                         │
              ▼                          ▼                         ▼
          Rejected                   Cancelled             PartiallyFilled
          (terminal)                 (terminal)                    │
                                                         PartialFill / FullFill
                                                                    │
                                                                    ▼
                                                            FullyFilled
                                                            (terminal)
```

Terminal states cannot transition to anything. They are the end of the actor's useful life.

### State Transition Table

```
┌──────────────────┬──────────────────┬───────────────────────────┐
│   Current State  │   Message        │   Next State              │
├──────────────────┼──────────────────┼───────────────────────────┤
│ New              │ SubmitOrder      │ Submitted                 │
│ New              │ anything else    │ WARN: invalid transition   │
├──────────────────┼──────────────────┼───────────────────────────┤
│ Submitted        │ Reject           │ Rejected (terminal)       │
│ Submitted        │ Cancel           │ Cancelled (terminal)      │
│ Submitted        │ PartialFill      │ PartiallyFilled           │
│ Submitted        │ FullFill         │ FullyFilled (terminal)    │
│ Submitted        │ anything else    │ WARN: invalid transition   │
├──────────────────┼──────────────────┼───────────────────────────┤
│ PartiallyFilled  │ PartialFill      │ PartiallyFilled           │
│ PartiallyFilled  │ FullFill         │ FullyFilled (terminal)    │
│ PartiallyFilled  │ Cancel           │ Cancelled (terminal)      │
│ PartiallyFilled  │ anything else    │ WARN: invalid transition   │
├──────────────────┼──────────────────┼───────────────────────────┤
│ FullyFilled      │ anything         │ WARN: already terminal    │
│ Rejected         │ anything         │ WARN: already terminal    │
│ Cancelled        │ anything         │ WARN: already terminal    │
└──────────────────┴──────────────────┴───────────────────────────┘
```

### Go Code: FSM Order Actor

```go
package fsm

import "fmt"

// OrderState represents the lifecycle state of a trade order.
type OrderState int

const (
    StateNew             OrderState = iota
    StateSubmitted
    StatePartiallyFilled
    StateFullyFilled
    StateCancelled
    StateRejected
)

func (s OrderState) String() string {
    switch s {
    case StateNew:             return "New"
    case StateSubmitted:       return "Submitted"
    case StatePartiallyFilled: return "PartiallyFilled"
    case StateFullyFilled:     return "FullyFilled"
    case StateCancelled:       return "Cancelled"
    case StateRejected:        return "Rejected"
    default:                   return "Unknown"
    }
}

// --- Messages ---

type MsgSubmitOrder  struct{ OrderID string }
type MsgPartialFill  struct{ FilledQty float64 }
type MsgFullFill     struct{ FilledQty float64 }
type MsgCancel       struct{ Reason string }
type MsgReject       struct{ Reason string }

// --- Actor ---

// OrderActor manages the lifecycle of a single order as a finite state machine.
type OrderActor struct {
    orderID      string
    state        OrderState
    filledQty    float64
    mailbox      chan any
}

func NewOrderActor(orderID string) *OrderActor {
    return &OrderActor{
        orderID: orderID,
        state:   StateNew,
        mailbox: make(chan any, 64),
    }
}

// Receive is the core of the FSM: a two-level switch.
// Outer switch: current state. Inner switch: message type.
// Any message that doesn't match a valid transition is logged and dropped.
func (o *OrderActor) Receive(msg any) {
    switch o.state {

    // ── State: New ─────────────────────────────────────────────────────────
    case StateNew:
        switch m := msg.(type) {
        case MsgSubmitOrder:
            fmt.Printf("[%s] New → Submitted\n", o.orderID)
            o.state = StateSubmitted
            _ = m
        default:
            fmt.Printf("[%s] WARN: received %T in state %s — ignoring\n",
                o.orderID, msg, o.state)
        }

    // ── State: Submitted ───────────────────────────────────────────────────
    case StateSubmitted:
        switch m := msg.(type) {
        case MsgPartialFill:
            fmt.Printf("[%s] Submitted → PartiallyFilled (qty=%.2f)\n",
                o.orderID, m.FilledQty)
            o.filledQty += m.FilledQty
            o.state = StatePartiallyFilled
        case MsgFullFill:
            fmt.Printf("[%s] Submitted → FullyFilled (qty=%.2f)\n",
                o.orderID, m.FilledQty)
            o.filledQty += m.FilledQty
            o.state = StateFullyFilled
        case MsgCancel:
            fmt.Printf("[%s] Submitted → Cancelled (reason=%s)\n",
                o.orderID, m.Reason)
            o.state = StateCancelled
        case MsgReject:
            fmt.Printf("[%s] Submitted → Rejected (reason=%s)\n",
                o.orderID, m.Reason)
            o.state = StateRejected
        default:
            fmt.Printf("[%s] WARN: received %T in state %s — ignoring\n",
                o.orderID, msg, o.state)
        }

    // ── State: PartiallyFilled ─────────────────────────────────────────────
    case StatePartiallyFilled:
        switch m := msg.(type) {
        case MsgPartialFill:
            fmt.Printf("[%s] PartiallyFilled += %.2f\n", o.orderID, m.FilledQty)
            o.filledQty += m.FilledQty
            // stay in PartiallyFilled
        case MsgFullFill:
            fmt.Printf("[%s] PartiallyFilled → FullyFilled (final qty=%.2f)\n",
                o.orderID, o.filledQty+m.FilledQty)
            o.filledQty += m.FilledQty
            o.state = StateFullyFilled
        case MsgCancel:
            fmt.Printf("[%s] PartiallyFilled → Cancelled (reason=%s)\n",
                o.orderID, m.Reason)
            o.state = StateCancelled
        default:
            fmt.Printf("[%s] WARN: received %T in state %s — ignoring\n",
                o.orderID, msg, o.state)
        }

    // ── Terminal States ────────────────────────────────────────────────────
    case StateFullyFilled, StateCancelled, StateRejected:
        fmt.Printf("[%s] WARN: received %T in terminal state %s — ignoring\n",
            o.orderID, msg, o.state)
    }
}

// Run starts the actor's message loop.
func (o *OrderActor) Run() {
    for msg := range o.mailbox {
        o.Receive(msg)

        // Once we reach a terminal state, drain remaining messages with warnings
        // and shut down. In a real system the supervisor would be notified here.
        if o.state == StateFullyFilled ||
            o.state == StateCancelled ||
            o.state == StateRejected {
            fmt.Printf("[%s] reached terminal state %s, shutting down\n",
                o.orderID, o.state)
            return
        }
    }
}
```

### The Two-Level Switch Pattern Explained

```
Receive(msg):
  ┌─────────────────────────────────────────────────────┐
  │  switch current_state                               │
  │   ├─ StateNew          ← outer: which state am I?  │
  │   │   switch msg_type                               │
  │   │    ├─ MsgSubmit    ← inner: valid here?         │
  │   │    └─ default      ← warn and drop              │
  │   ├─ StateSubmitted                                 │
  │   │   switch msg_type                               │
  │   │    ├─ MsgFill                                   │
  │   │    ├─ MsgCancel                                 │
  │   │    └─ default      ← warn and drop              │
  │   └─ terminal states   ← warn and drop everything   │
  └─────────────────────────────────────────────────────┘
```

This structure makes the valid transitions **visible in the code**. New developers can read the FSM directly from the `Receive` method. There's no hidden global state, no boolean flags scattered across fields, no "if order.submitted && !order.cancelled && order.partialFills > 0" conditions.

---

## 3. Pattern 3: Request-Response with Correlation IDs

### Why Before How

The simplest form of ask-pattern is:

```go
replyCh := make(chan Response, 1)
actor.mailbox <- Request{ReplyTo: replyCh}
resp := <-replyCh
```

This works for one request. It breaks when you have many.

### The Problem: Which Reply Is Mine?

```
  time ──────────────────────────────────────────────────►

  Requester A:  ──── GetPrice(BTC) ──────────────────────────────
  Requester B:  ──────── GetPrice(ETH) ──────────────────────────
  Requester C:  ──────────── GetPrice(SOL) ──────────────────────

  PriceActor:   ... processing ...
                                 ──── reply(BTC=$50k) ──►
                                 ──── reply(ETH=$3k) ───►
                                 ──── reply(SOL=$150) ──►

  Who gets which reply?
  If replies arrive out of order:
    Requester A gets SOL price  ← WRONG
    Requester B gets BTC price  ← WRONG
    Requester C gets ETH price  ← WRONG
```

With a shared reply channel (or no reply channel at all), you cannot match replies to requesters.

### The Solution: Correlation IDs

Each outbound request gets a **unique ID** (UUID or timestamp). The actor echoes that ID in its reply. A dispatcher maps `correlationID → waiting channel`.

```
  Requester A sends: GetPrice{ID: "aaa", Symbol: "BTC"}
  Requester B sends: GetPrice{ID: "bbb", Symbol: "ETH"}
  Requester C sends: GetPrice{ID: "ccc", Symbol: "SOL"}

  Dispatcher stores:
    pendingRequests["aaa"] = chan_A
    pendingRequests["bbb"] = chan_B
    pendingRequests["ccc"] = chan_C

  PriceActor replies:
    PriceResponse{ID: "bbb", Price: 3000}  ← ETH came back first
    PriceResponse{ID: "aaa", Price: 50000}
    PriceResponse{ID: "ccc", Price: 150}

  Dispatcher routes:
    "bbb" → chan_B  ✓ Requester B gets ETH price
    "aaa" → chan_A  ✓ Requester A gets BTC price
    "ccc" → chan_C  ✓ Requester C gets SOL price
```

```
┌────────────┐    Request{ID:"aaa"}    ┌────────────────┐
│ Requester  ├────────────────────────►│                │
│     A      │◄────────────────────────┤  PriceActor    │
└────────────┘    Reply{ID:"aaa",      │                │
                        Price:50000}   └────────────────┘
       ▲                                        ▲
       │         ┌───────────────────────┐      │
       │         │   DispatcherActor     │      │
       │         │                       │      │
       └─────────┤  pendingRequests:     ├──────┘
                 │   "aaa" → chan_A      │
                 │   "bbb" → chan_B      │
                 │   "ccc" → chan_C      │
                 └───────────────────────────┘
```

### Go Code: Dispatcher with Correlation IDs

```go
package correlation

import (
    "fmt"
    "sync"
    "time"
)

// PriceRequest is sent to the PriceActor asking for a quote.
type PriceRequest struct {
    CorrelationID string
    Symbol        string
    ReplyTo       chan PriceResponse
}

// PriceResponse is the reply from the PriceActor.
type PriceResponse struct {
    CorrelationID string
    Symbol        string
    Price         float64
    Err           error
}

// DispatcherActor sends requests to PriceActor and routes replies to callers.
type DispatcherActor struct {
    priceActor      chan PriceRequest
    mailbox         chan PriceResponse
    mu              sync.Mutex
    pendingRequests map[string]chan PriceResponse
}

func NewDispatcherActor(priceActor chan PriceRequest) *DispatcherActor {
    return &DispatcherActor{
        priceActor:      priceActor,
        mailbox:         make(chan PriceResponse, 256),
        pendingRequests: make(map[string]chan PriceResponse),
    }
}

// newCorrelationID generates a unique ID for each request.
func newCorrelationID() string {
    return fmt.Sprintf("%d", time.Now().UnixNano())
}

// Ask sends a price request and returns a channel the caller can block on.
func (d *DispatcherActor) Ask(symbol string) chan PriceResponse {
    id := newCorrelationID()
    replyCh := make(chan PriceResponse, 1)

    d.mu.Lock()
    d.pendingRequests[id] = replyCh
    d.mu.Unlock()

    // Replies come back to our mailbox, not directly to the caller.
    // The dispatcher then routes to the correct replyCh via the map.
    d.priceActor <- PriceRequest{
        CorrelationID: id,
        Symbol:        symbol,
        ReplyTo:       d.mailbox,
    }

    return replyCh
}

// Run processes incoming replies and routes each to the correct caller.
func (d *DispatcherActor) Run() {
    for resp := range d.mailbox {
        d.mu.Lock()
        replyCh, ok := d.pendingRequests[resp.CorrelationID]
        if ok {
            delete(d.pendingRequests, resp.CorrelationID)
        }
        d.mu.Unlock()

        if ok {
            replyCh <- resp
        } else {
            fmt.Printf("WARN: reply for unknown correlationID %s\n",
                resp.CorrelationID)
        }
    }
}
```

### How the PriceActor echoes the correlation ID

```go
// PriceActor looks up a price and echoes the correlation ID back in the reply.
func PriceActorRun(mailbox chan PriceRequest, prices map[string]float64) {
    for req := range mailbox {
        price, ok := prices[req.Symbol]
        if !ok {
            req.ReplyTo <- PriceResponse{
                CorrelationID: req.CorrelationID, // echo the ID!
                Symbol:        req.Symbol,
                Err:           fmt.Errorf("unknown symbol %s", req.Symbol),
            }
            continue
        }
        req.ReplyTo <- PriceResponse{
            CorrelationID: req.CorrelationID, // echo the ID!
            Symbol:        req.Symbol,
            Price:         price,
        }
    }
}
```

### Example caller

```go
func ExampleConcurrentAsks(d *DispatcherActor) {
    // Three concurrent asks — each gets its own buffered channel.
    chBTC := d.Ask("BTC/USD")
    chETH := d.Ask("ETH/USD")
    chSOL := d.Ask("SOL/USD")

    // Replies can arrive in any order. Each lands in the right channel.
    btc := <-chBTC
    eth := <-chETH
    sol := <-chSOL

    fmt.Printf("BTC: %.2f, ETH: %.2f, SOL: %.2f\n",
        btc.Price, eth.Price, sol.Price)
}
```

### Key insight

```
Without correlation IDs:
  Reply 1 goes to... whoever is waiting. First come, first served.
  Out-of-order replies = wrong data matched to wrong caller.

With correlation IDs:
  Reply carries ID "aaa" -> map says "aaa" belongs to chan_A -> correct.
  Order of replies is irrelevant. Matching is always correct.
```

This pattern is the foundation of every RPC system ever built
(HTTP request IDs, gRPC stream IDs, TCP sequence numbers — all correlation IDs).

---

## 4. Pattern 4: Pipeline

### Why Before How

A trade engine ingests raw WebSocket frames from an exchange. That raw data needs to be parsed, normalized, enriched, evaluated by a strategy, and executed. You could put all of this in one actor. But then testing is hard, changing one step risks breaking others, profiling is impossible, and you can't scale individual steps independently.

A **pipeline** solves all of this by making each step its own actor. Data flows in one direction.

```
Raw WS Data
    |
    v
+----------+
|  Parser  |  bytes -> struct
+----+-----+
     |
     v
+------------+
| Normalizer |  exchange format -> canonical
+----+-------+
     |
     v
+----------+
| Enricher |  add VWAP, depth, volatility
+----+-----+
     |
     v
+----------+
| Strategy |  emit signal (buy/sell/hold)
+----+-----+
     |
     v
+----------+
| Executor |  place order at exchange
+----------+
```

### Natural Backpressure

If Strategy is slow, its mailbox fills. Enricher blocks trying to send. Then Normalizer blocks. Then Parser blocks. The pipeline throttles itself with zero explicit flow-control code.

```
  Parser    Normalizer   Enricher    Strategy
  [      ]  [      ]     [||||||||]  <- full!
                              |
                         Enricher blocks on next send
                              |
                    Normalizer mailbox fills
                              |
               Parser blocks on next send
                              |
         WebSocket read rate slows automatically
```

**Easy to scale.** If Enricher is the bottleneck, run three Enricher actors behind a router without touching any other stage.

**Independent supervision.** If Strategy panics, its supervisor restarts only that stage. Parser and Normalizer keep running, buffering data until Strategy recovers.

### Go Code: Three-Stage Pipeline

```go
package pipeline

import "fmt"

// Stage 1: Parser
// Input: raw bytes. Output: ParsedTick.

type RawFrame struct {
    Data []byte
}

type ParsedTick struct {
    Symbol string
    Price  float64
    Volume float64
}

type ParserActor struct {
    mailbox chan RawFrame
    next    chan ParsedTick // the next stage's mailbox
}

func NewParserActor(next chan ParsedTick) *ParserActor {
    return &ParserActor{
        mailbox: make(chan RawFrame, 256),
        next:    next,
    }
}

func (p *ParserActor) Run() {
    for frame := range p.mailbox {
        // Simplified: in reality you'd json.Unmarshal here.
        tick := ParsedTick{
            Symbol: "BTC/USD",
            Price:  50000.0,
            Volume: float64(len(frame.Data)),
        }
        p.next <- tick // forward to next stage
    }
}

// Stage 2: Calculator
// Input: ParsedTick. Output: EnrichedTick (adds a simple moving average).

type EnrichedTick struct {
    ParsedTick
    SMA20 float64 // 20-period simple moving average
}

type CalculatorActor struct {
    mailbox    chan ParsedTick
    next       chan EnrichedTick
    priceHistory []float64
}

func NewCalculatorActor(next chan EnrichedTick) *CalculatorActor {
    return &CalculatorActor{
        mailbox: make(chan ParsedTick, 256),
        next:    next,
    }
}

func (c *CalculatorActor) sma20() float64 {
    n := len(c.priceHistory)
    if n == 0 {
        return 0
    }
    window := 20
    if n < window {
        window = n
    }
    sum := 0.0
    for _, p := range c.priceHistory[n-window:] {
        sum += p
    }
    return sum / float64(window)
}

func (c *CalculatorActor) Run() {
    for tick := range c.mailbox {
        c.priceHistory = append(c.priceHistory, tick.Price)
        enriched := EnrichedTick{
            ParsedTick: tick,
            SMA20:      c.sma20(),
        }
        c.next <- enriched
    }
}

// Stage 3: Printer (stand-in for Strategy/Executor)
// Input: EnrichedTick. Output: printed to console.

type PrinterActor struct {
    mailbox chan EnrichedTick
}

func NewPrinterActor() *PrinterActor {
    return &PrinterActor{
        mailbox: make(chan EnrichedTick, 256),
    }
}

func (pr *PrinterActor) Run() {
    for tick := range pr.mailbox {
        fmt.Printf("[%s] price=%.2f sma20=%.2f\n",
            tick.Symbol, tick.Price, tick.SMA20)
    }
}

// Wire them up: Parser -> Calculator -> Printer
func WirePipeline() {
    printer    := NewPrinterActor()
    calculator := NewCalculatorActor(printer.mailbox)
    parser     := NewParserActor(calculator.mailbox)

    // Start all stages concurrently.
    go printer.Run()
    go calculator.Run()
    go parser.Run()

    // Feed raw frames into the first stage.
    parser.mailbox <- RawFrame{Data: []byte(`{"symbol":"BTC","price":50000}`)}
    parser.mailbox <- RawFrame{Data: []byte(`{"symbol":"BTC","price":50100}`)}
}
```

### Wiring Visualized

```
WirePipeline():

  printer.mailbox    <-- calculator.next
  calculator.mailbox <-- parser.next
  parser.mailbox     <-- external input

  Each actor ONLY knows about the next stage's channel.
  None of them know about the stages before or after.
  Completely decoupled.
```

---

## 5. Pattern 5: Scatter-Gather (Fan-Out / Fan-In)

### Why Before How

You want the best price to buy 1 BTC. Binance quotes $50,010. Coinbase quotes $49,990. Kraken quotes $50,005. You should buy on Coinbase. But you won't know that unless you query all three at the same time and compare.

Querying them sequentially wastes time. Each query takes ~100ms. Three sequential = 300ms. Three parallel = ~100ms (limited by the slowest).

**Scatter:** send the same query to N actors simultaneously.
**Gather:** collect responses, with a timeout for stragglers.

```
                         +---------------+
                    +--->| BinanceActor  |---+
                    |    |  $50,010      |   |
                    |    +---------------+   |
                    |                        |
  +------------+   |    +---------------+   |   +------------+
  | Aggregator +---+--->| CoinbaseActor |---+-->| Aggregator |
  |  (sender)  |   |    |  $49,990      |   |   | (receiver) |
  +------------+   |    +---------------+   |   +------------+
                    |                        |   picks best:
                    |    +---------------+   |   $49,990
                    +--->| KrakenActor   |---+
                         |  $50,005      |
                         +---------------+

  All 3 queries sent at t=0.
  All 3 replies arrive around t=100ms.
  Aggregator picks the best.
```

### Handling Partial Responses

Exchanges time out. Networks partition. You cannot wait forever.

```
  t=0ms:   Send to Binance, Coinbase, Kraken
  t=95ms:  Coinbase replies  -> record $49,990
  t=102ms: Binance replies   -> record $50,010
  t=500ms: TIMEOUT           -> Kraken didn't reply

  Result: use best of {Coinbase, Binance} = $49,990
  Proceed without Kraken. Log the timeout.
```

The `select` statement with a `time.After` case handles this cleanly.

### Go Code: Scatter-Gather Aggregator

```go
package scattergather

import (
    "fmt"
    "time"
)

// PriceQuote is a response from one exchange.
type PriceQuote struct {
    Exchange string
    Symbol   string
    Price    float64
}

// PriceQuery is sent to each exchange actor.
type PriceQuery struct {
    Symbol  string
    ReplyTo chan PriceQuote
}

// ExchangeActor simulates a single exchange feed.
type ExchangeActor struct {
    name    string
    mailbox chan PriceQuery
    price   float64
    latency time.Duration
}

func NewExchangeActor(name string, price float64, latency time.Duration) *ExchangeActor {
    return &ExchangeActor{
        name:    name,
        mailbox: make(chan PriceQuery, 64),
        price:   price,
        latency: latency,
    }
}

func (e *ExchangeActor) Run() {
    for query := range e.mailbox {
        time.Sleep(e.latency) // simulate network round-trip
        query.ReplyTo <- PriceQuote{
            Exchange: e.name,
            Symbol:   query.Symbol,
            Price:    e.price,
        }
    }
}

// BestPriceAggregator queries all exchanges and returns the best (lowest) price.
func BestPriceAggregator(symbol string, exchanges []*ExchangeActor) PriceQuote {
    replyCh := make(chan PriceQuote, len(exchanges))
    timeout := 500 * time.Millisecond

    // Fan-out: send to all exchanges simultaneously.
    for _, ex := range exchanges {
        ex.mailbox <- PriceQuery{
            Symbol:  symbol,
            ReplyTo: replyCh,
        }
    }

    // Fan-in: collect responses until timeout.
    var quotes []PriceQuote
    deadline := time.After(timeout)

collect:
    for len(quotes) < len(exchanges) {
        select {
        case q := <-replyCh:
            quotes = append(quotes, q)
            fmt.Printf("received quote from %s: %.2f\n", q.Exchange, q.Price)
        case <-deadline:
            fmt.Printf("timeout: got %d/%d responses\n", len(quotes), len(exchanges))
            break collect
        }
    }

    if len(quotes) == 0 {
        return PriceQuote{Symbol: symbol, Price: -1} // no data
    }

    // Pick the best (lowest) price.
    best := quotes[0]
    for _, q := range quotes[1:] {
        if q.Price < best.Price {
            best = q
        }
    }

    fmt.Printf("best price: %.2f on %s\n", best.Price, best.Exchange)
    return best
}

// ExampleUsage wires up three exchange actors and finds the best BTC price.
func ExampleUsage() {
    binance  := NewExchangeActor("Binance",  50010.0, 95*time.Millisecond)
    coinbase := NewExchangeActor("Coinbase", 49990.0, 80*time.Millisecond)
    kraken   := NewExchangeActor("Kraken",   50005.0, 600*time.Millisecond) // will timeout

    go binance.Run()
    go coinbase.Run()
    go kraken.Run()

    best := BestPriceAggregator("BTC/USD", []*ExchangeActor{binance, coinbase, kraken})
    fmt.Printf("placing order on %s at %.2f\n", best.Exchange, best.Price)
}
```

### The `select` Fan-In Explained

```
collect loop:
  for each iteration, select WHICHEVER happens first:

    case q := <-replyCh:          <- a quote arrived
        record it, loop again

    case <-time.After(500ms):     <- deadline hit
        stop collecting, use what we have

This is the canonical Go pattern for "collect with timeout".
The deadline fires once. Any exchange that replies after it is ignored.
```

### Trade Engine Use Case: Best Execution

```
  Strategy says: "buy 1 BTC"
  Aggregator queries all connected exchanges simultaneously
  Collects quotes within 500ms
  Places the order on the exchange with the lowest ask price
  Logs which exchanges timed out (for monitoring)
```

---

## 6. Pattern 6: Dead Letter Box

### Why Before How

In a system of dozens of actors, messages can fail to reach their destination for many reasons:

- The target actor's mailbox is full (channel buffer overflowed)
- The target actor has been shut down
- A routing error sent the message to a non-existent actor
- A channel `send` panicked because the channel was closed

In most naive implementations, the message is silently dropped. It disappears. Nobody knows. This is the worst possible failure mode: **silent data loss**.

The Dead Letter Box makes silent drops impossible. Every undeliverable message is forwarded to a single, always-available actor that logs it, counts it, and alerts on spikes.

```
Normal delivery:
  Sender --> [mailbox] --> Actor  OK

Failed delivery WITHOUT dead letters:
  Sender --> [full mailbox] --> message dropped, nobody knows

Failed delivery WITH dead letters:
  Sender --> [full mailbox] --> SafeSend --> DeadLetterActor
                                                   |
                                              logs: "actor X dropped msg Y"
                                              counter["actor X"]++
                                              alert if count > threshold
```

### Why spikes matter

If `dead_letters["OrderActor"]` jumps from 0 to 500 in one minute, something is wrong. Either:
- Orders are arriving faster than they can be processed (need more workers)
- OrderActor crashed and isn't being restarted (supervision failure)
- A router bug is sending messages to a dead mailbox

Without dead letter tracking, you'd discover this at month-end when reconciliation finds 500 missing orders.

### Go Code: Dead Letter Box

```go
package deadletter

import (
    "fmt"
    "sync"
)

// DeadLetter wraps any undeliverable message with context.
type DeadLetter struct {
    To     string // name of the intended recipient actor
    Msg    any    // the original message
    Reason string // why it could not be delivered
}

// DeadLetterActor receives all undeliverable messages and tracks them.
type DeadLetterActor struct {
    mailbox  chan DeadLetter
    mu       sync.Mutex
    counters map[string]int // counts dead letters per target actor name
}

var globalDeadLetterActor *DeadLetterActor

func NewDeadLetterActor() *DeadLetterActor {
    return &DeadLetterActor{
        mailbox:  make(chan DeadLetter, 4096),
        counters: make(map[string]int),
    }
}

func (d *DeadLetterActor) Run() {
    for dl := range d.mailbox {
        d.mu.Lock()
        d.counters[dl.To]++
        count := d.counters[dl.To]
        d.mu.Unlock()

        fmt.Printf("[DEAD LETTER] to=%s reason=%q msgType=%T count=%d\n",
            dl.To, dl.Reason, dl.Msg, count)

        // Alert if dead letters for one actor spike above a threshold.
        if count > 100 {
            fmt.Printf("[ALERT] actor %q has %d dead letters -- investigate!\n",
                dl.To, count)
        }
    }
}

// CountFor returns the current dead letter count for a given actor name.
func (d *DeadLetterActor) CountFor(actorName string) int {
    d.mu.Lock()
    defer d.mu.Unlock()
    return d.counters[actorName]
}

// SafeSend attempts to send msg to mailbox (non-blocking).
// On failure it routes the message to the dead letter box instead of dropping it.
func SafeSend(actorName string, mailbox chan any, msg any, dl *DeadLetterActor) {
    select {
    case mailbox <- msg:
        // delivered successfully
    default:
        // mailbox is full or closed; route to dead letters
        dl.mailbox <- DeadLetter{
            To:     actorName,
            Msg:    msg,
            Reason: "mailbox full",
        }
    }
}

// SafeSendWithPanic is a variant that also recovers from send-on-closed-channel panics.
func SafeSendWithPanic(actorName string, mailbox chan any, msg any, dl *DeadLetterActor) {
    defer func() {
        if r := recover(); r != nil {
            dl.mailbox <- DeadLetter{
                To:     actorName,
                Msg:    msg,
                Reason: fmt.Sprintf("panic: %v", r),
            }
        }
    }()
    select {
    case mailbox <- msg:
    default:
        dl.mailbox <- DeadLetter{
            To:     actorName,
            Msg:    msg,
            Reason: "mailbox full",
        }
    }
}
```

### Usage Pattern

```go
// Instead of this (silent drop):
orderActor.mailbox <- order // blocks or panics if full/closed

// Do this:
SafeSend("OrderActor", orderActor.mailbox, order, deadLetterActor)

// Now:
//   - Success: order reaches OrderActor
//   - Full mailbox: order goes to dead letter box with reason "mailbox full"
//   - Closed channel: panic recovered, order goes to dead letter box
```

### Dead Letter Flow Diagram

```
  Sender
    |
    | SafeSend("OrderActor", mailbox, order)
    |
    +--[ select ]--> mailbox <- order  ---> OrderActor  (success)
    |                    full?
    |
    +---------------> DeadLetterActor
                           |
                      log + count
                           |
                      counters["OrderActor"]++
                           |
                      if count > 100: ALERT
```

### In a Real System

Beyond logging, a production dead letter box might:
- Publish metrics to Prometheus (`dead_letters_total{actor="OrderActor"}`)
- Persist messages to a database for later replay
- Page on-call if the rate exceeds a threshold
- Attempt to re-deliver after a backoff window

---

## 7. Pattern 7: Saga (Distributed Transactions)

### Why Before How

A database transaction is simple: all steps succeed or all are rolled back atomically. `BEGIN ... COMMIT` or `ROLLBACK`.

A trade engine does not have a single database transaction. Placing an order spans multiple systems:
1. **Risk Service** — reserve the capital
2. **Exchange Connector** — place the order on the exchange
3. **Ledger Service** — record the fill

If step 3 fails after steps 1 and 2 have succeeded, you can't just rollback a database transaction. The exchange has already received the order. The capital is already reserved. You need to **undo** the actions you already took, in reverse order.

This is the **Saga pattern**: a sequence of steps where each step has a corresponding **compensation** that undoes it. If any step fails, all completed steps are compensated in reverse order.

```
Happy path:
  Step 1: Reserve funds       -> OK
  Step 2: Place order         -> OK
  Step 3: Confirm fill        -> OK
  Done. Ledger updated.

Failure at Step 3:
  Step 1: Reserve funds       -> OK  (completed)
  Step 2: Place order         -> OK  (completed)
  Step 3: Confirm fill        -> FAILED
  |
  v compensate backwards:
  Undo Step 2: Cancel the order    (compensation for step 2)
  Undo Step 1: Release funds       (compensation for step 1)
  Saga rolled back cleanly.
```

```
  +-------+    +-------+    +-------+    +-------+
  |Step 1 |--->|Step 2 |--->|Step 3 |--->|Step 4 |  <- happy path
  +-------+    +-------+    +---X---+    +-------+
                                 |
                               FAIL
                                 |
                                 v
  +-------+    +-------+
  |Undo 2 |<---|Undo 1 |  <- compensation runs in reverse
  +-------+    +-------+
```

### Why the Saga is an Actor

The Saga **coordinator** is itself an actor. It:
- Receives a trigger message to start the saga
- Executes steps sequentially, tracking which completed
- On failure, executes compensations in reverse
- Sends a final message (success or failure) to the caller

Because it's an actor, it has its own mailbox and state. Multiple sagas can run concurrently (each is its own actor instance). Supervisors can restart crashed sagas. Dead letter boxes catch lost messages.

### Go Code: Saga Actor

```go
package saga

import "fmt"

// Step represents one unit of work in a saga.
// Execute performs the action. Compensate undoes it.
type Step struct {
    Name       string
    Execute    func() error
    Compensate func() error
}

// SagaActor coordinates a sequence of steps with automatic compensation.
type SagaActor struct {
    name           string
    steps          []Step
    completedSteps []int // indices of steps that succeeded
    mailbox        chan any
}

func NewSagaActor(name string, steps []Step) *SagaActor {
    return &SagaActor{
        name:    name,
        steps:   steps,
        mailbox: make(chan any, 16),
    }
}

// TriggerSaga is the message that starts a saga.
type TriggerSaga struct {
    ReplyTo chan SagaResult
}

// SagaResult is sent back to the caller when the saga completes.
type SagaResult struct {
    Success bool
    Err     error
}

// Run starts the saga actor's receive loop.
func (s *SagaActor) Run() {
    for msg := range s.mailbox {
        switch m := msg.(type) {
        case TriggerSaga:
            result := s.execute()
            if m.ReplyTo != nil {
                m.ReplyTo <- result
            }
        default:
            fmt.Printf("[Saga:%s] unknown message %T\n", s.name, msg)
        }
    }
}

// execute runs all steps. On failure, compensates all completed steps in reverse.
func (s *SagaActor) execute() SagaResult {
    s.completedSteps = nil // reset state for this run

    for i, step := range s.steps {
        fmt.Printf("[Saga:%s] executing step %d: %s\n", s.name, i+1, step.Name)

        if err := step.Execute(); err != nil {
            fmt.Printf("[Saga:%s] step %d FAILED: %v\n", s.name, i+1, err)
            s.compensate()
            return SagaResult{Success: false, Err: err}
        }

        s.completedSteps = append(s.completedSteps, i)
        fmt.Printf("[Saga:%s] step %d OK\n", s.name, i+1)
    }

    fmt.Printf("[Saga:%s] all steps completed successfully\n", s.name)
    return SagaResult{Success: true}
}

// compensate runs compensations for all completed steps in reverse order.
func (s *SagaActor) compensate() {
    fmt.Printf("[Saga:%s] starting compensation for %d completed steps\n",
        s.name, len(s.completedSteps))

    for i := len(s.completedSteps) - 1; i >= 0; i-- {
        stepIdx := s.completedSteps[i]
        step := s.steps[stepIdx]

        fmt.Printf("[Saga:%s] compensating step %d: %s\n",
            s.name, stepIdx+1, step.Name)

        if err := step.Compensate(); err != nil {
            // Compensation failure is serious. Log loudly.
            // In production: alert, write to audit log, require manual intervention.
            fmt.Printf("[Saga:%s] COMPENSATION FAILED for step %d: %v -- MANUAL INTERVENTION REQUIRED\n",
                s.name, stepIdx+1, err)
        } else {
            fmt.Printf("[Saga:%s] compensation for step %d OK\n", s.name, stepIdx+1)
        }
    }
}
```

### Wiring a Trade Saga

```go
func BuildTradeSaga(orderID string) *SagaActor {
    return NewSagaActor("trade-"+orderID, []Step{
        {
            Name: "ReserveFunds",
            Execute: func() error {
                fmt.Printf("reserving funds for order %s\n", orderID)
                // call risk service
                return nil
            },
            Compensate: func() error {
                fmt.Printf("releasing funds for order %s\n", orderID)
                // call risk service to reverse reservation
                return nil
            },
        },
        {
            Name: "PlaceOrder",
            Execute: func() error {
                fmt.Printf("placing order %s on exchange\n", orderID)
                // call exchange connector
                return nil
            },
            Compensate: func() error {
                fmt.Printf("cancelling order %s on exchange\n", orderID)
                // call exchange connector to cancel
                return nil
            },
        },
        {
            Name: "ConfirmFill",
            Execute: func() error {
                fmt.Printf("waiting for fill confirmation for order %s\n", orderID)
                // wait for exchange fill message
                return fmt.Errorf("exchange timeout") // simulated failure
            },
            Compensate: func() error {
                // Fill confirmation has no compensation: if we never got the fill,
                // there is nothing to undo at this stage.
                return nil
            },
        },
    })
}

func ExampleTradeSaga() {
    saga := BuildTradeSaga("order-123")
    go saga.Run()

    replyCh := make(chan SagaResult, 1)
    saga.mailbox <- TriggerSaga{ReplyTo: replyCh}

    result := <-replyCh
    if result.Success {
        fmt.Println("trade completed successfully")
    } else {
        fmt.Printf("trade failed: %v (compensations applied)\n", result.Err)
    }
}
```

### Expected Output

```
[Saga:trade-order-123] executing step 1: ReserveFunds
reserving funds for order order-123
[Saga:trade-order-123] step 1 OK
[Saga:trade-order-123] executing step 2: PlaceOrder
placing order order-123 on exchange
[Saga:trade-order-123] step 2 OK
[Saga:trade-order-123] executing step 3: ConfirmFill
waiting for fill confirmation for order order-123
[Saga:trade-order-123] step 3 FAILED: exchange timeout
[Saga:trade-order-123] starting compensation for 2 completed steps
[Saga:trade-order-123] compensating step 2: PlaceOrder
cancelling order order-123 on exchange
[Saga:trade-order-123] compensation for step 2 OK
[Saga:trade-order-123] compensating step 1: ReserveFunds
releasing funds for order order-123
[Saga:trade-order-123] compensation for step 1 OK
trade failed: exchange timeout (compensations applied)
```

### The Saga State Machine

The saga coordinator itself is an FSM:

```
  Idle
    |
    | TriggerSaga
    |
    v
  Running
    |         |
    |success  |failure
    |         |
    v         v
  Done     Compensating
               |
               | all compensations done
               |
               v
            RolledBack
```

### Important Caveats

**Compensation failure.** What if cancelling the exchange order also fails? You need an alert and a human in the loop. Sagas cannot guarantee atomicity; they guarantee *eventual consistency with audit trail*.

**Idempotency.** Compensations may run more than once if the saga actor restarts mid-compensation (e.g., process crash). Each compensation must be idempotent: cancelling an already-cancelled order should not be an error.

**Timeout.** Steps should have timeouts. A step that waits forever will stall the entire saga. Use `context.WithTimeout` around each `Execute`.

---

## Putting It All Together

These seven patterns are not independent. A real trade engine uses all of them, composed:

```
  WebSocket feed
       |
       v
  [Pipeline: Parser -> Normalizer -> Enricher]
       |
       v
  [Hash Router: route by symbol]
       |         |         |
       v         v         v
  [FSM Actor] [FSM Actor] [FSM Actor]   <- one per order
       |         |         |
       +----+----+         |
            |              |
            v              v
  [Scatter-Gather]   [Dead Letter Box]
   query exchanges
       |         |
       v         v
  [Saga Actor]  [timeout]
   place order
   with compensation
       |
       v
  [Correlation ID dispatcher]
   notify original requester
```

Each layer handles one concern. Each concern is tested independently. Each actor is supervised independently. Failures are contained, logged, and recovered.

This is the architecture of a production trade engine.

---

## Pattern Summary

```
+---------------------+--------------------------------------------------+
| Pattern             | Primary Problem Solved                           |
+---------------------+--------------------------------------------------+
| Router              | Scale throughput; isolate state per key (hash)   |
| FSM Actor           | Prevent illegal state transitions               |
| Correlation IDs     | Match async replies to concurrent requesters     |
| Pipeline            | Separation of concerns; backpressure; testability|
| Scatter-Gather      | Parallel queries; partial response handling      |
| Dead Letter Box     | Eliminate silent message loss                    |
| Saga                | Distributed transaction with compensation        |
+---------------------+--------------------------------------------------+
```

---

*Next: `07-testing.md` — How to test actors without race conditions or flaky timeouts.*