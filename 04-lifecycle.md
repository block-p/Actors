# Actor Lifecycle — Birth, Running, and Graceful Death

> **Core insight:** An actor is not just code that runs — it has a life.
> It is born, it initialises itself, it processes messages, and eventually it dies.
> Understanding each stage of that lifecycle is what separates a toy actor from
> a production actor that you can safely deploy in a trade engine.

---

## 1. The 4 Stages of an Actor's Life

```
┌──────────┐     spawn      ┌─────────┐    StopActor    ┌──────────┐    cleanup done    ┌──────┐
│  Not Yet │ ─────────────▶ │ Running │ ───────────────▶ │Stopping │ ──────────────────▶ │ Dead │
│  Exists  │                │  Loop   │                  │(PostStop)│                    │      │
└──────────┘                └─────────┘                  └──────────┘                    └──────┘
                               ▲    │
                               │    │ (crash)
                               │    ▼
                            ┌──────────┐
                            │Supervisor│ restarts it
                            └──────────┘
```

### Stage 1 — Born (Spawn)

The actor does not exist until something creates it. In Go, "spawning" an actor
means: allocating its mailbox channel, starting its goroutine, and running any
PreStart logic. The moment `go func()` is called, the actor exists.

The caller receives an address — in Go, a `chan any`. That channel IS the actor.
From this moment, the actor is running and ready to receive messages.

### Stage 2 — Running (Receive Loop)

This is the vast majority of the actor's life. It sits in its `for msg := range mailbox`
loop and processes messages one at a time. Its state changes, it sends messages to
other actors, it reacts to the world. This stage runs for as long as the system needs it.

The golden rule of the Running stage: **process each message quickly and return**.
Never block inside the receive loop (see section 4 for exactly why).

### Stage 3 — Stopping (PostStop)

Something signals the actor to stop — either a `StopActor` message (graceful shutdown)
or a panic (crash). The actor enters PostStop: it does cleanup work before dying.

For a graceful stop, this means:
- Closing open connections
- Flushing buffered data
- Sending a final event to notify downstream actors
- Signalling the done channel to confirm full stop

For a crash, the supervisor catches the panic and decides whether to restart or escalate.

### Stage 4 — Dead

The goroutine has returned. The mailbox channel may still exist in memory (if someone
holds a reference to it), but no one is reading from it. Any messages sent to a dead
actor's mailbox will accumulate unseen until garbage collected.

This is why supervision matters: you need to know when an actor dies and respond to it.

---

## 2. PreStart — What to Do When an Actor Starts

The actor has just been spawned. Before entering the main receive loop, it may need to
do initialisation work: connect to an exchange WebSocket, load its last known state from
a database, or announce itself to a registry.

### Why PreStart Is Tricky

PreStart work is often I/O-bound (network connection, database query). If you do this
synchronously before entering the receive loop, the spawner blocks waiting. If the I/O
fails, the actor never starts cleanly.

There are two good patterns.

### Pattern A — Init Function (Synchronous, Simple)

For lightweight initialisation, do it synchronously inside a setup function before
the receive loop starts. This is the simplest approach.

```go
type ExchangeConnector struct {
    mailbox  chan any
    exchange string
}

func NewExchangeConnector(exchange string) *ExchangeConnector {
    a := &ExchangeConnector{
        mailbox:  make(chan any, 200),
        exchange: exchange,
    }
    go a.run()
    return a
}

func (a *ExchangeConnector) Send(msg any) {
    a.mailbox <- msg
}

func (a *ExchangeConnector) run() {
    // ── PreStart ──────────────────────────────────────────────────────────
    // This runs ONCE before we enter the message loop.
    // If this panics, the goroutine dies — the supervisor will restart it.
    conn := a.preStart()

    // ── Receive Loop ──────────────────────────────────────────────────────
    for msg := range a.mailbox {
        switch m := msg.(type) {
        case SubscribeMarket:
            conn.Subscribe(m.Symbol)
        case StopActor:
            a.postStop(conn)
            close(m.Done)
            return
        }
    }
}

func (a *ExchangeConnector) preStart() *FakeWSConn {
    fmt.Printf("[%s] PreStart: connecting to exchange WebSocket...\n", a.exchange)
    conn := dialWebSocket(a.exchange)
    fmt.Printf("[%s] PreStart: connected, loading symbol list...\n", a.exchange)
    return conn
}
```

### Pattern B — Init Message (Asynchronous, Robust)

For heavier initialisation, send the actor an `Init` message as its first message.
This lets the spawner continue immediately and gives the actor a chance to initialise
at its own pace.

```go
type Init struct{}   // "please initialise yourself"

type PriceActor struct {
    mailbox   chan any
    prices    map[string]float64
    wsConn    *FakeWSConn
    ready     bool
}

func NewPriceActor() *PriceActor {
    a := &PriceActor{
        mailbox: make(chan any, 200),
        prices:  make(map[string]float64),
    }
    go a.run()
    // First message to self: "go initialise"
    // This does NOT block the spawner — it returns immediately.
    a.mailbox <- Init{}
    return a
}

func (a *PriceActor) run() {
    for msg := range a.mailbox {
        switch m := msg.(type) {

        case Init:
            // Heavy initialisation here — safe because we're inside the actor
            fmt.Println("[PriceActor] Init: connecting to price feed...")
            a.wsConn = dialWebSocket("price-feed")
            fmt.Println("[PriceActor] Init: loading last known prices from DB...")
            a.prices["BTC/USDT"] = 68_000.0
            a.prices["ETH/USDT"] = 3_800.0
            a.ready = true
            fmt.Println("[PriceActor] Init: ready")

        case GetPrice:
            if !a.ready {
                // Still initialising — send back an error
                m.Reply <- PriceResult{Err: errors.New("not ready yet")}
                continue
            }
            price := a.prices[m.Symbol]
            m.Reply <- PriceResult{Symbol: m.Symbol, Price: price}

        case StopActor:
            if a.wsConn != nil {
                a.wsConn.Close()
            }
            close(m.Done)
            return
        }
    }
}
```

The Init message pattern is more flexible: if initialisation fails, the actor can retry
by sending itself another `Init{}` message with a delay (using a goroutine + `time.Sleep`
outside the receive loop).

### Announcing Yourself to Other Actors

After initialisation, an actor may want to tell other actors it is alive:

```go
case Init:
    a.init()
    // Announce to the registry: "I am the ETH/USDT price feed actor"
    a.registry <- RegisterActor{
        Name:    "price-feed-eth-usdt",
        Mailbox: a.mailbox,
    }
```

---

## 3. The Receive Loop — The Heart of the Actor

```go
for msg := range mailbox {
    switch m := msg.(type) {
    case TypeA:
        // handle TypeA
    case TypeB:
        // handle TypeB
    case StopActor:
        // cleanup
        close(m.Done)
        return  // this exits the for loop AND the goroutine
    }
}
// We reach here if the mailbox channel is closed from outside.
// But prefer StopActor over closing the channel.
```

### How the `for range` Loop Works

`for msg := range mailbox` does three things:
1. Blocks until a message arrives
2. Receives the message
3. Loops back to wait for the next one

It exits cleanly when the channel is closed (receives the zero value and the
"closed" signal). This is how the Go runtime notifies range loops of channel closure.

### One Message at a Time — Why It Matters

The goroutine only reads the next message *after finishing the current one*.
This is not just a detail — it is the entire safety model. Because we process
messages sequentially, we never need locks on the actor's internal state.
No mutex, no atomic, no sync.

If you did parallel work inside the receive case, two operations could race
on the actor's state. Don't do it.

### Blocking vs Non-Blocking Operations

There are two kinds of operations:

**Non-blocking (instant):** In-memory computation, map lookups, arithmetic,
sending messages to other actors. These complete in microseconds.
Do them directly in the receive case.

**Blocking (slow):** Network I/O, database queries, HTTP calls, `time.Sleep`.
These can take milliseconds to seconds. **Never do them inside the receive loop.**

### How to Handle Long-Running Work Without Blocking

The pattern is always the same: **offload to a goroutine, send the result back
as a message**.

```go
// WRONG — blocking HTTP call inside receive loop
case FetchMarketData:
    resp, err := http.Get("https://api.exchange.com/data")  // BLOCKS the actor!
    // While this blocks, ALL OTHER MESSAGES queue up. The actor is frozen.
    data := parseResponse(resp)
    a.prices["BTC/USDT"] = data.Price

// CORRECT — offload to goroutine, send result back as message
case FetchMarketData:
    selfMailbox := a.mailbox  // capture for goroutine
    go func() {
        resp, err := http.Get("https://api.exchange.com/data")
        if err != nil {
            selfMailbox <- FetchFailed{Reason: err.Error()}
            return
        }
        data := parseResponse(resp)
        // Send the result back to ourselves as a message
        selfMailbox <- MarketDataReady{
            Symbol: "BTC/USDT",
            Price:  data.Price,
        }
    }()
    // Actor returns immediately from this case.
    // The goroutine runs in parallel.
    // When it's done, the result arrives as a message.

case MarketDataReady:
    // Process the result — this is instant (just update a map entry)
    a.prices[m.Symbol] = m.Price
```

This pattern keeps the actor's receive loop fast. The actor is never frozen.
Messages keep flowing. The goroutine does the slow work and delivers the result
as just another message — the actor processes it like any other.

---

## 4. What NEVER to Do Inside Receive — Blocking Operations

This section is critical. These mistakes will destroy your system's responsiveness.

### NEVER: `time.Sleep()`

```go
// WRONG
case RetryConnection:
    time.Sleep(5 * time.Second)   // Actor is FROZEN for 5 seconds
    a.connect()

// CORRECT
case RetryConnection:
    selfMailbox := a.mailbox
    go func() {
        time.Sleep(5 * time.Second)  // sleep in background goroutine
        selfMailbox <- Connect{}     // trigger reconnection via message
    }()
```

### NEVER: HTTP Requests Inside Receive

```go
// WRONG
case PlaceOrderOnExchange:
    resp, err := http.Post("https://exchange.com/orders", ...)  // BLOCKS!
    // Actor is frozen until HTTP responds (could be 100ms to 30s)

// CORRECT
case PlaceOrderOnExchange:
    msg := m  // capture for goroutine
    self := a.mailbox
    go func() {
        resp, err := http.Post("https://exchange.com/orders", ...)
        if err != nil {
            self <- OrderSubmitFailed{OrderID: msg.OrderID, Reason: err.Error()}
            return
        }
        self <- OrderSubmitted{OrderID: msg.OrderID, ExchangeOrderID: resp.ID}
    }()
```

### NEVER: Blocking DB Queries Inside Receive

```go
// WRONG
case LoadPositions:
    rows, err := db.Query("SELECT * FROM positions")  // BLOCKS!
    // All messages queue up during the DB round-trip

// CORRECT
case LoadPositions:
    self := a.mailbox
    go func() {
        rows, err := db.Query("SELECT * FROM positions")
        if err != nil {
            self <- LoadPositionsFailed{Reason: err.Error()}
            return
        }
        positions := scanRows(rows)
        self <- PositionsLoaded{Positions: positions}
    }()
```

### NEVER: Waiting on Another Actor's Reply Inside Receive

This is the deadlock scenario. If Actor A waits for Actor B's reply, and
Actor B is waiting for Actor A's reply, neither can proceed.

```go
// WRONG — actor A blocks waiting for actor B's reply WHILE inside its own receive
case GetCombinedData:
    // Actor A's receive loop is now BLOCKED waiting for actor B
    balanceReply := make(chan BalanceResult, 1)
    a.accountActor <- GetBalance{Reply: balanceReply}
    balance := <-balanceReply   // ← BLOCKS! Actor A cannot process other messages.

    // Meanwhile, what if accountActor sends a message to A and waits for A's reply?
    // DEADLOCK.

// CORRECT — offload the ask to a goroutine
case GetCombinedData:
    self := a.mailbox
    replyTo := m.Reply
    go func() {
        balanceReply := make(chan BalanceResult, 1)
        a.accountActor <- GetBalance{Reply: balanceReply}
        select {
        case balance := <-balanceReply:
            replyTo <- CombinedDataResult{Balance: balance.Available}
        case <-time.After(2 * time.Second):
            replyTo <- CombinedDataResult{Err: errors.New("balance actor timeout")}
        }
    }()
    // Actor A's receive loop is NOT blocked. It can process other messages
    // while the goroutine waits for the balance reply.
```

### The Mental Model

Think of the receive loop as an **event loop** (like JavaScript's).
If you block the event loop, the whole system freezes.
Every non-trivial I/O must go off the event loop (into a goroutine)
and come back as a message.

```
Receive Loop  ─────────────────────────────────────────────────▶ time
    │              │              │              │
    ▼              ▼              ▼              ▼
 Handle         Handle         Handle         Handle
 MsgA (fast)   MsgB (fast)   MsgC (fast)   MsgD (fast)
    │
    └──▶ goroutine: slow HTTP call
                    │
                    ▼
               sends MarketDataReady message back to actor
```

---

## 5. PostStop — What to Do When Stopping

PostStop is the cleanup phase. It runs once, right before the goroutine returns.
Think of it as a destructor with a guaranteed-once semantic.

What belongs in PostStop:

1. **Close connections** — WebSocket, TCP, database connections
2. **Flush buffered data** — any writes that haven't been committed yet
3. **Notify downstream actors** — "I'm going away, stop expecting my messages"
4. **Notify parent/supervisor** — "I have cleanly stopped"
5. **Signal the done channel** — the Poison Pill pattern

```go
func (a *OrderActor) postStop() {
    fmt.Printf("[OrderActor] PostStop: flushing %d pending orders\n",
        len(a.pendingOrders))

    // 1. Flush pending orders to persistent storage
    for _, order := range a.pendingOrders {
        a.db.Save(order)
    }

    // 2. Close the exchange connection
    if a.exchangeConn != nil {
        a.exchangeConn.Close()
        fmt.Println("[OrderActor] PostStop: exchange connection closed")
    }

    // 3. Notify the logger that we're done
    a.logActor <- ActorStopped{
        ActorName:  "order-actor",
        StoppedAt:  time.Now(),
        PendingMsgs: len(a.mailbox),
    }

    fmt.Println("[OrderActor] PostStop: complete")
}

// In the receive loop:
case StopActor:
    a.postStop()
    close(m.Done)  // signal full stop
    return
```

### Partial PostStop — When You Crash

If the actor panics, PostStop does not run automatically. The supervisor's
`recover()` catches the panic. If you want cleanup on crash, run PostStop
in a `defer`:

```go
func (a *OrderActor) run() {
    defer func() {
        if r := recover(); r != nil {
            fmt.Printf("[OrderActor] CRASHED: %v\n", r)
            a.crashCleanup()   // minimal crash cleanup
            a.supervisor <- ActorCrashed{Actor: a, Err: fmt.Errorf("%v", r)}
        }
    }()

    a.preStart()

    for msg := range a.mailbox {
        // ... handle messages
    }
}
```

---

## 6. Graceful Shutdown Sequence

In a real system you have a dependency graph of actors:

```
Strategy Actor
    ├── depends on → Price Actor
    ├── depends on → Order Actor
    │                   └── depends on → Exchange Connector
    └── depends on → Risk Actor
```

You must shut down in **reverse dependency order** — children before parents.
If you stop the Exchange Connector first, the Order Actor will panic trying to
send to a dead actor.

```
Stop order:
    1. Strategy Actor     (top-level, depends on everything)
    2. Risk Actor         (depends on nothing)
    3. Order Actor        (depends on Exchange Connector)
    4. Price Actor        (depends on nothing)
    5. Exchange Connector (bottom of the tree)
```

### Walk-through of the Full Sequence

```
Main → StopActor{Done} → Strategy Actor
     ← waits on done channel

Strategy Actor receives StopActor:
  → sends StopActor to Risk Actor, waits for its done
  → sends StopActor to Order Actor, waits for its done
    (Order Actor → sends StopActor to Exchange Connector, waits)
  → sends StopActor to Price Actor, waits for its done
  → all children stopped → closes own done channel
  → goroutine returns

Main ← done channel closes → continues with shutdown
```

This gives you deterministic, ordered shutdown. Every actor gets to finish its
current work and flush its state before dying.

### Complete Go Program — Graceful Shutdown

See `go-example/04_lifecycle/main.go` for the full runnable program.
Below is the shutdown sequence in isolation:

```go
// Shutdown order matters — stop dependents first, dependencies last.
func shutdownAll(strategy, risk, order, price, exchange *Actor) {
    // Layer 1: top-level actors (depend on everything)
    fmt.Println("Stopping strategy actor...")
    done1 := make(chan struct{})
    strategy.Send(StopActor{Done: done1})
    <-done1

    // Layer 2: mid-level actors
    fmt.Println("Stopping risk actor...")
    done2 := make(chan struct{})
    risk.Send(StopActor{Done: done2})
    <-done2

    fmt.Println("Stopping order actor...")
    done3 := make(chan struct{})
    order.Send(StopActor{Done: done3})
    <-done3

    fmt.Println("Stopping price actor...")
    done4 := make(chan struct{})
    price.Send(StopActor{Done: done4})
    <-done4

    // Layer 3: infrastructure actors (no dependents remain)
    fmt.Println("Stopping exchange connector...")
    done5 := make(chan struct{})
    exchange.Send(StopActor{Done: done5})
    <-done5

    fmt.Println("All actors stopped cleanly.")
}
```

---

## 7. The Done Channel Pattern

The `done chan struct{}` carried in `StopActor` is the standard Go signalling idiom.
`struct{}` uses zero bytes — it's purely a signal.

```go
// The lifecycle message
type StopActor struct {
    Done chan struct{}
}

// Inside any actor's receive loop — this is the template
case StopActor:
    // ─── PostStop work here ───────────────────────────────
    fmt.Printf("[%s] flushing state before stop...\n", actorName)
    a.flush()
    a.closeConnections()
    // ──────────────────────────────────────────────────────

    // Signal full stop — this BROADCASTS to all waiters
    close(m.Done)

    // Exit the goroutine. The actor is now Dead.
    return
```

### Waiting for a Single Actor

```go
done := make(chan struct{})
actor.Send(StopActor{Done: done})
<-done   // block until actor closes done
```

### Waiting for Multiple Actors in Parallel

If actors are independent, stop them in parallel and wait for all:

```go
// Stop price and risk in parallel (neither depends on the other)
priceDone := make(chan struct{})
riskDone  := make(chan struct{})

priceActor.Send(StopActor{Done: priceDone})
riskActor.Send(StopActor{Done: riskDone})

// Wait for BOTH
<-priceDone
<-riskDone
fmt.Println("both stopped")
```

### Using sync.WaitGroup for Many Actors

```go
var wg sync.WaitGroup
actors := []*Actor{price, risk, logger}

for _, a := range actors {
    wg.Add(1)
    done := make(chan struct{})
    a.Send(StopActor{Done: done})
    go func(d chan struct{}) {
        <-d
        wg.Done()
    }(done)
}

wg.Wait()
fmt.Println("all stopped")
```

### Why `close(done)` Beats Sending a Value

A send (`done <- struct{}{}`) can only be received once. A close broadcasts to
every goroutine waiting on `<-done`. For supervision, testing, and monitoring,
you often have multiple observers — close satisfies all of them simultaneously.

And once closed, `<-done` returns immediately for any future callers. The signal
is permanent. This means a health-check goroutine that polls `<-done` with a
non-blocking select never races with the actor's shutdown.

```go
// Any number of observers can wait — all unblock when the actor closes done
go func() { <-done; fmt.Println("supervisor sees actor stopped") }()
go func() { <-done; fmt.Println("metrics sees actor stopped") }()
go func() { <-done; fmt.Println("test sees actor stopped") }()
```

---

## Quick Reference — The Full Lifecycle

```
SPAWN
  NewActor()
    │
    ├── allocate mailbox channel
    ├── go func() { ... }()
    └── return mailbox (the actor's address)

PRESTART (inside goroutine, before message loop)
  preStart()
    ├── dial WebSocket / DB connection
    ├── load state from persistent storage
    └── send Init{} to self if async

RUNNING (the message loop)
  for msg := range mailbox {
    switch msg.(type) {
      case Command:   update state, send events
      case Query:     read state, reply to Reply channel
      case Event:     react, maybe send commands
      case StopActor: go to POSTSTOP
    }
  }
  ┌─ RULES ──────────────────────────────────────────────┐
  │ NEVER block inside receive (no sleep, no HTTP, no DB) │
  │ NEVER wait for another actor's reply in receive       │
  │ DO offload slow work to goroutines, reply by message  │
  └───────────────────────────────────────────────────────┘

POSTSTOP (runs when StopActor received)
  postStop()
    ├── close connections
    ├── flush pending data
    ├── notify downstream actors
    └── close(m.Done)  ← broadcast "I am dead"

DEAD
  goroutine has returned
  mailbox channel still exists (held in memory by senders)
  supervisor detects death and decides to restart or escalate
```

---

## Common Mistakes and How to Avoid Them

| Mistake | What Goes Wrong | Fix |
|---------|----------------|-----|
| HTTP call in receive | Actor frozen for seconds | Goroutine + reply-as-message |
| `time.Sleep` in receive | Actor frozen, messages queue up | Goroutine + `time.After` |
| Waiting on actor reply in receive | Deadlock if actors depend on each other | Goroutine + timeout |
| Unbuffered reply channel | Actor blocked if caller times out | `make(chan T, 1)` always |
| Stopping in wrong order | Panic sending to dead actor | Stop dependents first |
| Closing mailbox instead of Poison Pill | Panic on concurrent senders | Use `StopActor{Done}` |
| No timeout on Ask | Goroutine leak | `select` with `time.After` |
| Not waiting for done channel | Process exits before actors flush | `<-done` after every stop |

Next → `go-example/04_lifecycle/main.go` for a complete runnable system
demonstrating all of these patterns.
