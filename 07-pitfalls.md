# Actor Model — Pitfalls, Mistakes, and How to Fix Them

These are the bugs that will hit you in production if you don't know them upfront.
Every single one is a real category of bug. Every single one has killed real trading systems.

---

## Pitfall 1: Blocking Inside Receive — The Frozen Actor

**This is the #1 mistake beginners make.**

### What it is

You call something that takes time DIRECTLY inside your `Receive` function:
- `time.Sleep()`
- `http.Post()` (any network call)
- A database query
- Waiting for another actor's reply with `<-replyChan`

### What goes wrong

While your actor is blocked, it cannot process ANY other messages.
Its mailbox fills up. Everything upstream from it also fills up. The whole system backs up.

```
WRONG:

func (a *BinanceActor) Receive(ctx *Context, msg Message) {
    switch m := msg.(type) {
    case PlaceOrder:
        // This HTTP call takes 200ms.
        // The actor is COMPLETELY FROZEN for 200ms.
        // No other PlaceOrder, no cancellations, nothing.
        resp, err := http.Post("https://api.binance.com/order", ...)
        fmt.Println(resp, err)
    }
}
```

The cascade:
```
StrategyActor mailbox → fills up (100 msgs)
        ↓
ExecutorActor mailbox → fills up (50 msgs)
        ↓
BinanceActor mailbox → 1 msg being processed, frozen for 200ms
```

Everything waits. In a trading system during high volatility, this is catastrophic.

### The Fix: Offload to a Goroutine, Send Result Back

```go
func (a *BinanceActor) Receive(ctx *Context, msg Message) {
    switch m := msg.(type) {
    case PlaceOrder:
        self := ctx.Self
        // Spin off a goroutine. Receive returns IMMEDIATELY.
        go func() {
            resp, err := http.Post("https://api.binance.com/order", ...)
            // Send the result back to yourself as a message.
            self.Send(OrderResult{Order: m, Response: resp, Err: err})
        }()
        // Actor is free to process the next message NOW.
    
    case OrderResult:
        // Handle the result here, back in the actor's single-threaded loop.
        if m.Err != nil {
            // handle error
        }
    }
}
```

### Rule

> Never let `Receive` block. If the work takes time, spawn a goroutine and send the result back as a message.

---

## Pitfall 2: Shared Mutable State — The Invisible Race Condition

### What it is

You pass a pointer to a map, slice, or struct in a message. Both the sender and the actor now share the same memory. There is no lock. This is a data race.

### What goes wrong

```go
// WRONG:
prices := map[string]float64{
    "BTC": 50000.0,
    "ETH": 3000.0,
}

// You send this map by reference
actor.Send(PriceUpdate{Prices: prices}) // <-- passing pointer to map

// Meanwhile, in another goroutine, you update it
prices["BTC"] = 51000.0  // <-- writing concurrently with actor reading it
```

The actor is reading `prices["BTC"]` while you're writing to it. This is undefined behavior in Go. With `-race` it will be caught. In production without `-race`, it silently corrupts data. In a trading system, corrupted price data means wrong orders.

### How to Detect It

```bash
go test -race ./...
go run -race main.go
```

The race detector will show you exactly which goroutines are conflicting.

### The Fix: Copy Before Sending

```go
// CORRECT: copy the map before sending
snapshot := make(map[string]float64, len(prices))
for k, v := range prices {
    snapshot[k] = v
}
actor.Send(PriceUpdate{Prices: snapshot})

// Now the actor owns this copy. You own the original. No sharing.
```

### Better Fix: Use Value Types

Design your messages as plain structs with primitive fields where possible:

```go
// CORRECT: no pointers, pure value type
type PriceUpdate struct {
    Symbol string
    Price  float64
    Time   time.Time
}
// This is copied when sent. Completely safe.
```

### The Rule

> Messages should be values, not references. If you must send a collection, copy it first.

---

## Pitfall 3: The Ask Deadlock — Two Actors Waiting for Each Other

### What it is

Actor A is handling a message and sends an Ask to Actor B.
While A is blocking waiting for B's reply, B sends an Ask BACK to A.
A is busy waiting for B → cannot process B's ask.
B is waiting for A's reply → cannot unblock.
Both are frozen forever.

```
Actor A          Actor B
  │                 │
  │── Ask ─────────►│
  │  (blocking)     │── Ask ─────────►(A is blocked, can't respond)
  │                 │  (blocking)
  ▼ ∞               ▼ ∞
DEADLOCK          DEADLOCK
```

### Real Trade Engine Example

```
StrategyActor processes a signal:
  → sends Ask to RiskActor: "what's my current exposure?"
  → blocks waiting for answer

RiskActor handles a different message:
  → sends Ask to StrategyActor: "what's your current position?"
  → blocks waiting for answer

Both frozen. Forever.
```

### How to Detect It

Deadlocks show up as:
- System appears to be running (no panic) but no output
- Mailboxes fill up (metrics show growing backlog)
- Go's built-in deadlock detector fires if ALL goroutines are blocked, but it misses partial deadlocks

### The Fixes

**Fix 1: Never block inside Receive waiting for another actor.**
Use fire-and-forget + callback:
```go
// Instead of Ask, send a request with "who to reply to"
case NeedExposure:
    riskActor.Send(GetExposure{
        Symbol:  m.Symbol,
        ReplyTo: ctx.Self,  // "send answer back to me"
    })
    // DON'T WAIT HERE. Return. Process the reply later.

case ExposureResult:
    // This arrives later. NOW make the decision.
    a.makeDecision(m.Exposure)
```

**Fix 2: Add timeout to every Ask.**
```go
reply := make(chan Exposure, 1)
riskActor.Send(GetExposure{Reply: reply})

select {
case result := <-reply:
    // use it
case <-time.After(500 * time.Millisecond):
    // timeout — don't block forever, handle the failure
    log.Println("RiskActor didn't reply in time")
}
```

**Fix 3: Redesign to remove circular dependencies.**
If A needs data from B and B needs data from A, you have a circular dependency. Break it by introducing a third actor that holds the shared data:
```
StrategyActor → reads from → PositionStore ← writes to ← RiskActor
```

---

## Pitfall 4: Unbuffered Reply Channel — The Goroutine Leak

### What it is

You create an unbuffered channel for Ask replies. Your actor sends the reply. But you already timed out and moved on. The actor goroutine is now STUCK FOREVER trying to send to a channel nobody is reading.

### The Bug

```go
// WRONG: unbuffered reply channel
reply := make(chan int)  // size 0!

actor.Send(GetCount{Reply: reply})

select {
case result := <-reply:
    use(result)
case <-time.After(1 * time.Second):
    // You timed out. You stop listening to `reply`.
    // But the actor will try to do: reply <- count
    // Nobody is reading. The actor goroutine BLOCKS FOREVER.
    log.Println("timed out")
    return
}
// reply channel is garbage collected? No — the actor holds a reference.
// The actor goroutine leaks.
```

Over time, each timed-out Ask leaks one goroutine. You'll see memory grow slowly. In a high-throughput system this kills the process.

### The Fix: Always Buffer Reply Channels at Size 1

```go
// CORRECT: buffered reply channel, size 1
reply := make(chan int, 1)  // size 1!

// Now if you time out, the actor can still send without blocking.
// The value goes into the buffer and the channel is eventually GC'd.
```

Size 1 is always the right size for Ask replies. Never 0, never 2+.

---

## Pitfall 5: Mailbox Overflow — Silent Data Loss or Deadlock

### What it is

Your mailbox (channel) is full. What happens next depends on how you send:

- `actor.mailbox <- msg` — **sender blocks** until there's space. If the actor is busy, everything backs up.
- A `select` with a `default` case — **message is silently dropped.**

Both are bad. One freezes your system. The other loses data.

### Signs of Mailbox Overflow

- Growing latency (senders are waiting)
- Dropped messages (silent — very hard to debug without monitoring)
- In extreme cases: deadlock (everything waiting on everything)

### How to Size Your Mailbox

There's no magic number. General guidelines:
- Fast actors (pure computation): buffer = 10-50
- I/O actors (network, DB): buffer = 100-500 (they can be slow)
- Market data feeds: buffer = 1000-10000 (bursts happen)

### How to Monitor Mailbox Depth

Add a metric to your ActorRef:
```go
type ActorRef struct {
    mailbox chan Message
    name    string
}

// MailboxDepth returns the current number of pending messages.
func (r *ActorRef) MailboxDepth() int {
    return len(r.mailbox)
}

// Call this periodically to detect overflow risk:
func (r *ActorRef) LogDepthIfHigh(threshold int) {
    depth := r.MailboxDepth()
    if depth > threshold {
        log.Printf("WARNING: %s mailbox depth = %d", r.name, depth)
    }
}
```

### What to Do When Mailbox Is Full

Options (pick one based on your requirements):
1. **Block the sender** — natural backpressure, slows the whole system
2. **Drop the message** — use a non-blocking send, log the drop, alert
3. **Drop the oldest message** — ring buffer mailbox (requires custom implementation)
4. **Shed load** — stop accepting new work until mailbox clears

```go
// Option 2: non-blocking send with logging
func (r *ActorRef) TrySend(msg Message) bool {
    select {
    case r.mailbox <- msg:
        return true
    default:
        log.Printf("DROPPED message to %s: mailbox full (depth=%d)", r.name, len(r.mailbox))
        return false
    }
}
```

---

## Pitfall 6: Sending to a Closed Channel — The Panic

### What it is

In Go, sending to a closed channel panics. If an actor is stopped (its channel is closed) and someone sends it a message, your program crashes.

```go
close(actor.mailbox)
// ...
actor.mailbox <- SomeMessage{}  // PANIC: send on closed channel
```

### When This Happens

- An actor crashes and its channel is closed during cleanup
- A supervisor restarts the actor with a NEW channel, but old callers still hold the old ActorRef
- Callers send to the old (closed) channel → panic

### The Fix: Use a Safe Send Helper

```go
// SafeSend sends a message and recovers from a closed-channel panic.
// Returns false if the channel was closed.
func SafeSend(ch chan Message, msg Message) (sent bool) {
    defer func() {
        if r := recover(); r != nil {
            sent = false
        }
    }()
    ch <- msg
    return true
}
```

### Better Fix: Don't Close Channels for Shutdown

Instead of closing the channel, use a dedicated shutdown message (Poison Pill):
```go
type StopActor struct{ Done chan struct{} }

// Send this instead of closing the channel:
done := make(chan struct{})
actor.Send(StopActor{Done: done})
<-done // wait for actor to finish

// The channel stays open. No panic risk.
```

---

## Pitfall 7: The Actor That Never Stops — Resource Leak

### What it is

You spawn an actor, use it temporarily, then "forget" about it. The goroutine keeps running. The channel stays open. Memory is never released.

```go
func handleRequest(req Request) {
    // Spawn a temporary actor to handle this request
    tempActor := spawnTempActor()
    tempActor.Send(req)
    // ... use it ...
    // You forgot to stop it!
    // Now there's a goroutine + channel leaking forever.
}
```

In a trade engine that handles thousands of requests per second, this leaks thousands of goroutines per second.

### How to Detect It

```bash
# Check goroutine count at runtime
import "runtime"
fmt.Println("goroutines:", runtime.NumGoroutine())
```

If this number keeps growing without bound, you have leaks.

Also use pprof:
```go
import _ "net/http/pprof"
go http.ListenAndServe(":6060", nil)
// Then: go tool pprof http://localhost:6060/debug/pprof/goroutine
```

### The Fix

Always have a defined exit condition for every actor:
1. Poison pill message: `StopActor`
2. Context cancellation: `context.Context` passed to the actor
3. Parent stopping its children explicitly

---

## Pitfall 8: Not Handling Unknown Messages

### What it is

Your switch statement has no `default` case. An unrecognized message type is silently ignored. No error, no log, nothing.

```go
// WRONG:
func (a *MyActor) Receive(ctx *Context, msg Message) {
    switch m := msg.(type) {
    case DoSomething:
        a.doIt(m)
    // No default. Unknown messages vanish.
    }
}
```

Sometime later, you add a new message type but forget to add a handler. The messages go nowhere. You have no idea why your feature isn't working.

### The Fix: Always Log Unknown Messages

```go
// CORRECT:
func (a *MyActor) Receive(ctx *Context, msg Message) {
    switch m := msg.(type) {
    case DoSomething:
        a.doIt(m)
    default:
        log.Printf("[%T] unhandled message type: %T — %+v", a, msg, msg)
        // In strict mode, you could even panic here during development
    }
}
```

---

## Pitfall 9: Using Actors for Everything

Actors are not the right tool for every problem. Overusing them adds complexity without benefit.

### When NOT to use actors:

**CPU-bound computation with no state**
```go
// WRONG: actor just to run a calculation
calc := spawnCalculator()
calc.Send(ComputeIndicator{Data: data})
// Just call the function!

// CORRECT:
result := computeIndicator(data)
```

**Simple synchronous request/response**
```go
// WRONG: actor just to look something up
lookup := spawnLookup()
reply := make(chan float64, 1)
lookup.Send(GetPrice{Symbol: "BTC", Reply: reply})
price := <-reply
// Just use a function or a read-locked map!
```

**Short-lived one-off tasks**
```go
// WRONG: spawning an actor to do one thing and die
for _, order := range orders {
    a := spawnOrderActor()
    a.Send(order)
    // This creates and destroys a goroutine per order — overhead not worth it
}
// CORRECT: use a worker pool goroutine pattern instead
```

### When actors ARE the right tool:
- Long-lived stateful things (exchange connections, order books, sessions)
- Things that need fault isolation (if one exchange crashes, others should be fine)
- Things with complex message-driven behavior (an order lifecycle with many state transitions)
- Things that communicate with each other frequently

---

## Pitfall 10: Forgetting That Message Ordering Is Per-Sender

### What it is

Message ordering in actor systems:
- Messages from ONE sender to ONE actor → guaranteed order (they go through one channel)
- Messages from MULTIPLE senders to ONE actor → order between senders is NOT guaranteed

### The Bug in a Trade Engine

```
BinanceActor sends: OrderFilled{ID: 1}
BinanceActor sends: OrderFilled{ID: 2}
→ StrategyActor receives them in order 1, 2. SAFE.

BinanceActor  sends: OrderFilled{ID: 1}
CoinbaseActor sends: OrderFilled{ID: 2}
→ StrategyActor might receive: 2, 1. ORDER NOT GUARANTEED.
```

If your strategy assumes fills arrive in chronological order across exchanges, it may make wrong decisions.

### The Fix

- Always include a timestamp in messages
- Sort or sequence events at the point of consumption if ordering matters
- Use a single aggregator actor that sequences events from multiple sources before forwarding

```go
type OrderFilled struct {
    OrderID    string
    Exchange   string
    Price      float64
    Qty        float64
    FilledAt   time.Time  // always include this
    SequenceNo uint64     // optional: global sequence from a sequencer actor
}
```

---

## Quick Reference Cheat Sheet

| Pitfall | Symptom | Fix |
|---|---|---|
| Blocking in Receive | Actor freezes, mailbox fills | Offload to goroutine, send result back |
| Shared mutable state | Data race, corrupted data | Copy before sending, use value types |
| Ask deadlock | System frozen, no output | Never block in Receive, use callbacks |
| Unbuffered reply chan | Goroutine leak | Always `make(chan T, 1)` for replies |
| Mailbox overflow | Slowdown or silent drops | Right-size buffer, monitor depth, shed load |
| Send to closed chan | panic | Use poison pill, safe-send helper |
| Actor never stops | Goroutine leak, OOM | Always define an exit condition |
| No default case | Silent message loss | Always log unknown messages |
| Actors for everything | Complexity, overhead | Use functions for stateless work |
| Multi-sender ordering | Wrong sequence, bad decisions | Timestamp messages, use a sequencer |
