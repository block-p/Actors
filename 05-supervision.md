# 05 — Supervision: The Backbone of Fault-Tolerant Systems

> "A system that never crashes is a system that was never stressed enough."
> The goal is not to prevent failure — it's to *contain* it and *recover* automatically.

---

## Table of Contents

1. [What Supervision IS](#1-what-supervision-is)
2. [The Supervision Tree](#2-the-supervision-tree)
3. [The Three Supervision Strategies](#3-the-three-supervision-strategies)
4. [Restart Policies](#4-restart-policies)
5. [Implementing a Supervisor in Go from Scratch](#5-implementing-a-supervisor-in-go-from-scratch)
6. [The Supervisor IS an Actor Too](#6-the-supervisor-is-an-actor-too)
7. [Error Classification — Transient vs Permanent](#7-error-classification--transient-vs-permanent)
8. [Real Trade Engine Failure Scenarios](#8-real-trade-engine-failure-scenarios)

---

## 1. What Supervision IS

### The Core Idea

In a traditional program you protect dangerous code with `try/catch` or `if err != nil`. This puts the *caller* in charge of deciding what to do when something goes wrong. That sounds responsible, but it has a deep flaw: **the caller is the wrong entity to make recovery decisions**.

Consider this: if your WebSocket connection to Binance drops inside `BinanceActor`, what should happen?

- Should the trading strategy care? No — it just wants prices.
- Should the risk checker care? No — it just checks position limits.
- Should the executor care? No — it just sends orders.

The *only* thing that should care is whatever is responsible for keeping `BinanceActor` alive. That entity is the **supervisor**.

Supervision is a *structural relationship* in which a **parent actor** is explicitly responsible for the lifecycle of its **child actors**. When a child fails, the parent — and only the parent — decides what to do.

```
      Parent Supervisor
            |
     "I am responsible
      for your lifecycle"
            |
     +------+------+
     |             |
  ChildA        ChildB
  (crashes)     (running)
     |
     +---> Parent receives "ChildA exited with error"
           Parent decides: restart? escalate? ignore?
           ChildB is completely unaffected.
```

### Why Not try/catch Everywhere?

The `try/catch`-everywhere pattern has several problems at scale:

| Problem | What Goes Wrong |
|---|---|
| **Error handling is scattered** | Recovery logic lives in dozens of places; hard to reason about |
| **Caller couples to failure modes** | StrategyActor knows about WebSocket reconnect logic — wrong layer |
| **Partial state corruption** | An exception mid-computation can leave local state half-updated |
| **No automatic recovery** | You catch the error... then what? Restart the goroutine? With what backoff? How many times? |
| **Cascade risk** | An unhandled panic propagates up the call stack, killing unrelated work |

Supervision solves all of these by making recovery a **first-class architectural concern** — not an afterthought inside business logic.

### The "Let It Crash" Philosophy

This idea originates from **Erlang/OTP**, developed at Ericsson in the 1980s for telephone switches that needed 99.9999999% uptime ("nine nines" — about 31ms of downtime per year).

The Erlang engineers discovered something counter-intuitive: **trying to handle every possible error in place made the system MORE fragile**, not less. Error-handling code is itself complex code that can have its own bugs. It is nearly impossible to reason about every failure mode in advance.

Their solution: **write only the happy path**. If something goes wrong, let the process crash cleanly, and let the supervisor restart it from a known-good initial state.

"Let it crash" does NOT mean:
- Be reckless with errors
- Ignore errors
- Let the whole system die

"Let it crash" DOES mean:
- An actor that hits an unrecoverable state should exit immediately (fail fast and clean)
- Its supervisor will restart it from a fresh, known-good state
- Other actors are completely unaffected — they keep running
- The corrupt state is *thrown away*, not patched over with increasingly complex guards

```
WITHOUT supervision (defensive programming):

  BinanceActor:
    connect()
    for {
      msg, err := read()
      if err != nil {
        // try to reconnect — but HOW MANY TIMES? what backoff?
        // notify who? strategy? risk? how?
        // what if notify itself fails?
        // what if we're in the middle of processing a message?
        // ...
      }
    }

  Result: Complex, fragile, tightly coupled error-handling at every layer.
  The actor code is polluted with recovery logic it shouldn't own.

WITH supervision (let it crash):

  BinanceActor:
    conn := connect()     // if this fails, return error, supervisor handles it
    for {
      msg := conn.read()  // if this fails, return error, supervisor handles it
      publish(msg)        // if this fails, return error, supervisor handles it
    }

  ExchangeSupervisor sees the exit error -> restarts BinanceActor -> problem solved.
  BinanceActor's code is clean. Recovery policy lives in exactly one place.
```

The key insight: a freshly started actor is always in a **known-good state**. Restarting is the simplest, most reliable recovery strategy. You don't need to "fix" the broken state — you replace it with a new clean state.

---

## 2. The Supervision Tree

A real system is not a flat list of actors. It is a **hierarchy**. Supervisors supervise children, who can themselves be supervisors of their own children. This forms a tree.

```
                              SystemSupervisor
                                     |
               +---------------------+---------------------+
               |                     |                     |
     ExchangeSupervisor      TradingSupervisor       InfraSupervisor
          |                          |                     |
   +------+------+            +------+------+       +------+------+
   |      |      |            |      |      |       |      |      |
Binance Coinbase Kraken   Strategy  Risk  Executor Logger Metrics  DB
 Actor   Actor   Actor     Actor   Actor   Actor   Actor   Actor  Actor
```

### Fault Isolation — The Blast Radius Boundary

Each subtree is an independent fault domain. A crash in one subtree cannot spontaneously propagate to a sibling subtree. The supervisor absorbs the shock.

```
ExchangeSupervisor notices BinanceActor crashed:

ExchangeSupervisor
+-- BinanceActor   [CRASH] --> will restart
+-- CoinbaseActor  [OK]    --> completely untouched
+-- KrakenActor    [OK]    --> completely untouched

TradingSupervisor   [OK] --> has no idea BinanceActor ever crashed
InfraSupervisor     [OK] --> has no idea BinanceActor ever crashed
```

`StrategyActor` never panics. It never receives an error. It simply stops getting Binance price updates for a second, then they resume. If StrategyActor is designed correctly (it should be), it handles a brief data gap gracefully or falls back to Coinbase prices.

### The Escalation Path

If a supervisor exhausts all its restart attempts for a child, it does not quietly give up — it reports the failure **up to its own parent supervisor**. Problems escalate up the tree. The higher the level, the more drastic the response.

```
BinanceActor crashes 5 times in 10 seconds:

  BinanceActor        --> exits with error (5th time)
       |
       v
  ExchangeSupervisor  --> "I've hit max restarts for BinanceActor"
       |                   calls its escalation function
       v
  SystemSupervisor    --> receives escalation from ExchangeSupervisor
       |
       +-- Option A: Restart the entire ExchangeSupervisor subtree
       +-- Option B: Switch to emergency mode (use only Coinbase + Kraken)
       +-- Option C: Page on-call engineer and pause new position opens
```

### Startup Order

The tree also encodes **startup dependencies**. Within a supervisor, children start in registration order. You should start infrastructure actors (LoggerActor, DatabaseActor) before trading actors that depend on them. The tree topology communicates intent.

---

## 3. The Three Supervision Strategies

When a child crashes, the supervisor must decide *which other children* — if any — to also restart. This choice is the **restart strategy**, and choosing the right one is critical.

---

### OneForOne — Restart Only the Crashed Child

The supervisor restarts the single child that crashed and leaves all siblings untouched.

```
Supervisor (OneForOne)
|
+-- BinanceActor   [CRASH]     --> [RESTART]
+-- CoinbaseActor  [running]   --> [NO CHANGE]
+-- KrakenActor    [running]   --> [NO CHANGE]

After restart:
+-- BinanceActor   [NEW]       (fresh goroutine, clean state)
+-- CoinbaseActor  [running]
+-- KrakenActor    [running]
```

**When to use:** Children are **completely independent** of each other. Each manages its own isolated state, its own connections, its own data. One failing does not leave siblings in a logically inconsistent state.

**Trade engine use case — ExchangeSupervisor:**

```
ExchangeSupervisor (OneForOne)
+-- BinanceActor:  owns its WebSocket to Binance. Independent.
+-- CoinbaseActor: owns its WebSocket to Coinbase. Independent.
+-- KrakenActor:   owns its WebSocket to Kraken. Independent.
```

If Binance has an outage and BinanceActor crashes, there is no reason to touch CoinbaseActor or KrakenActor. They are still publishing live prices. OneForOne is correct here.

---

### OneForAll — Restart ALL Children When One Crashes

When one child crashes, the supervisor stops ALL children and restarts ALL of them from scratch.

```
Supervisor (OneForAll)
|
+-- StrategyActor  [CRASH]     --> [STOP, then RESTART]
+-- RiskActor      [running]   --> [STOP, then RESTART]  <-- also restarted!
+-- ExecutorActor  [running]   --> [STOP, then RESTART]  <-- also restarted!

After restart (all fresh):
+-- StrategyActor  [NEW]
+-- RiskActor      [NEW]
+-- ExecutorActor  [NEW]
```

**When to use:** Children share **interdependent state** such that if one crashes mid-operation, the survivors are now in a logically inconsistent state that can't be trusted. A partial restart would leave the system more broken than a full restart.

**Trade engine use case — TradingSupervisor:**

```
TradingSupervisor (OneForAll)
+-- StrategyActor:  maintains current position targets, signal state
+-- RiskActor:      maintains approved position limits, exposure tracking
+-- ExecutorActor:  maintains pending order state, fill confirmations

Why OneForAll here?

  1. StrategyActor decides: "buy 1 BTC"
  2. StrategyActor sends intent to RiskActor
  3. StrategyActor CRASHES before RiskActor processes the intent

  Now RiskActor and ExecutorActor have no idea a buy was requested.
  StrategyActor, when restarted, may generate a *different* intent based
  on its reset state. The system state is now ambiguous.

  With OneForAll:
  - All three restart together
  - All three start from a clean, consistent state
  - The cluster is always internally consistent
```

The cost of OneForAll: more disruption per crash. The benefit: you never have an inconsistent cluster.

---

### RestForOne — Restart the Crashed Actor and All That Started After It

The supervisor restarts the crashed child AND every child that was registered after it. Children registered BEFORE the crashed child are untouched.

```
Supervisor (RestForOne) — startup order matters!

Start order:
  1. PriceNormalizerActor  (registered first)
  2. OrderBookBuilderActor (registered second, depends on #1)
  3. StrategyActor         (registered third, depends on #2)

PriceNormalizerActor crashes:

+-- PriceNormalizerActor  [CRASH]   --> [RESTART]         (the crashed one)
+-- OrderBookBuilderActor [running] --> [STOP + RESTART]   (started after crash point)
+-- StrategyActor         [running] --> [STOP + RESTART]   (started after crash point)

OrderBookBuilderActor crashes:

+-- PriceNormalizerActor  [running] --> [NO CHANGE]        (started before crash point)
+-- OrderBookBuilderActor [CRASH]   --> [RESTART]          (the crashed one)
+-- StrategyActor         [running] --> [STOP + RESTART]   (started after crash point)
```

**When to use:** Actors form a **linear dependency chain** — A produces data for B, B produces data for C. If A crashes, B's internal state is built on data from the crashed A and is now potentially stale or corrupt. C's state is built on B's now-corrupt state. B and C must restart.

But any actors before A in the chain are fine — they don't depend on A.

**Trade engine use case — data pipeline:**

```
Registration order in DataPipelineSupervisor (RestForOne):

  1. PriceNormalizerActor
     - Subscribes to raw ticks from exchanges
     - Normalizes prices across venues into a common format

  2. OrderBookBuilderActor
     - Subscribes to PriceNormalizerActor's output
     - Builds and maintains a consolidated order book
     - Its state is entirely derived from PriceNormalizerActor

  3. StrategyActor
     - Subscribes to OrderBookBuilderActor's output
     - Makes trading decisions based on the order book
     - Its signal state is derived from OrderBookBuilderActor

If PriceNormalizerActor crashes:
  - OrderBookBuilder has been consuming stale/corrupt data. Reset it.
  - StrategyActor has been consuming a stale/corrupt order book. Reset it.
  - Restart all three in order so the pipeline reconnects cleanly.

If OrderBookBuilderActor crashes:
  - PriceNormalizerActor is fine — it's the data SOURCE, not a consumer.
  - StrategyActor is downstream of the crash — reset it.
  - Restart from OrderBookBuilder onward.
```

This is the most nuanced strategy. Get the startup order right and it surgically minimizes restarts while still maintaining data consistency.

---

### Strategy Summary

```
                         Which children restart?
Strategy     Crashed   Before Crashed   After Crashed
-----------  -------   --------------   -------------
OneForOne      YES          no               no
OneForAll      YES          YES              YES
RestForOne     YES          no               YES

Choose based on:
  - Independent children?          -> OneForOne
  - Tightly coupled shared state?  -> OneForAll
  - Linear data pipeline?          -> RestForOne
```

---

## 4. Restart Policies

Knowing *which* children to restart is only half the story. The supervisor also needs to know *when* and *how many times* to restart before giving up.

### Immediate Restart vs Exponential Backoff

**Immediate restart** is the naive approach: the child crashes, restart it instantly. This works fine for transient issues like a brief network hiccup. But consider what happens when the *external service* is down:

```
09:00:00.000  BinanceActor starts, connects to Binance
09:00:00.001  Binance is down — connection refused
09:00:00.001  BinanceActor crashes
09:00:00.001  Supervisor restarts immediately
09:00:00.002  Binance is still down — connection refused
09:00:00.002  BinanceActor crashes
09:00:00.002  Supervisor restarts immediately
... (10,000 times per second)
```

You have now created a **thundering herd** problem. Your service hammers Binance's servers thousands of times per second during an outage, making it harder for them to recover. Many exchanges will rate-limit or IP-ban you for this.

**Exponential backoff** solves this: each restart waits longer than the last.

```
Restart 1:  wait  1 second   (2^0)
Restart 2:  wait  2 seconds  (2^1)
Restart 3:  wait  4 seconds  (2^2)
Restart 4:  wait  8 seconds  (2^3)
Restart 5:  wait 16 seconds  (2^4)
Restart 6:  wait 30 seconds  (capped at max, e.g. 30s)
```

This gives the remote service time to recover while still retrying automatically. It is a first-class feature of any production supervisor.

```
Time -->
|--1s--|----2s----|--------4s--------|----------------8s----------------|
  ^         ^               ^                         ^
restart1  restart2       restart3                  restart4
```

### MaxRestarts in a Window

Backoff alone isn't sufficient. What if the child successfully starts, processes a few messages, then crashes again, over and over? After enough time, the restart counter resets, and we'd retry forever.

The standard pattern is a **sliding window**: track how many times a child has been restarted within the last N seconds. If the count hits a limit, escalate.

```
Config: MaxRestarts=5, Window=30 seconds

Timeline:
  T=0s   BinanceActor crashes -> restart 1 (1 restart in window)
  T=2s   BinanceActor crashes -> restart 2 (2 restarts in window)
  T=5s   BinanceActor crashes -> restart 3 (3 restarts in window)
  T=8s   BinanceActor crashes -> restart 4 (4 restarts in window)
  T=10s  BinanceActor crashes -> restart 5 (5 restarts in window)
  T=11s  BinanceActor crashes -> MAX RESTARTS EXCEEDED -> ESCALATE

  T=45s  (window has slid past T=0s restart)
         If BinanceActor crashes here, the window only shows 4 restarts.
         Supervisor allows restart 6.
```

### What Happens When Max Restarts Is Exceeded

When the restart budget is exhausted, the supervisor does NOT silently give up. It **escalates** to its parent supervisor. The parent then makes a higher-level decision:

```
Level 1: ExchangeSupervisor hits max restarts for BinanceActor
         |
         v
         Calls escalation function -> passes to SystemSupervisor

Level 2: SystemSupervisor receives escalation
         |
         +-- Response A: Restart entire ExchangeSupervisor subtree
         |   (gives BinanceActor a completely fresh start including
         |    reset restart counters — use carefully)
         |
         +-- Response B: Mark Binance as unavailable, use Coinbase only
         |   (graceful degradation — system keeps running)
         |
         +-- Response C: Send PagerDuty alert, pause new trades
             (human intervention required — system enters safe mode)
```

### Permanent Failure Alert: When to Page a Human

Not all failures should be auto-recovered. Some failures require human judgment:

| Error Type | Example | Action |
|---|---|---|
| Transient | WebSocket timeout, connection reset | Auto-restart with backoff |
| Transient (repeated) | Service down for minutes | Auto-restart, then escalate |
| Permanent | Invalid API key | Do NOT restart — page human |
| Permanent | Account banned/suspended | Do NOT restart — page human |
| Permanent | Invalid config (bad symbol name) | Do NOT restart — fix config |
| Budget exhausted | 5 crashes in 10 seconds | Escalate, potentially page |

**Rule of thumb:**
- If restarting would make things worse (e.g., replaying bad orders), don't restart.
- If the error indicates a configuration or credentials problem, page a human.
- If the error is environmental (network, remote service), retry with backoff.

```
Auto-recover                      Page Human
    |                                  |
    v                                  v
Network drop                   Invalid API key
Rate limit hit                 Account suspended
Service restarting             Config file corrupt
Temporary DB overload          Regulatory halt
```

---

## 5. Implementing a Supervisor in Go from Scratch

Now let's build one. We'll implement a full `Supervisor` that supports all three strategies, exponential backoff, max restarts with a sliding window, generation tracking (to discard stale crash reports), and escalation.

```go
// Package supervisor implements an actor supervision tree for Go.
// It provides OneForOne, OneForAll, and RestForOne restart strategies
// with exponential backoff and max-restarts-in-window budgeting.
package supervisor

import (
    "context"
    "errors"
    "fmt"
    "log"
    "math"
    "sync"
    "time"
)

// ============================================================
// Strategy
// ============================================================

// Strategy defines which children are restarted when one child crashes.
type Strategy int

const (
    // OneForOne restarts only the crashed child. Use when children are independent.
    OneForOne Strategy = iota

    // OneForAll restarts every child. Use when children share interdependent state.
    OneForAll

    // RestForOne restarts the crashed child plus all children registered after it.
    // Use when children form a linear data pipeline.
    RestForOne
)

func (s Strategy) String() string {
    switch s {
    case OneForOne:
        return "OneForOne"
    case OneForAll:
        return "OneForAll"
    case RestForOne:
        return "RestForOne"
    default:
        return "Unknown"
    }
}

// ============================================================
// Error Classification
// ============================================================

// ActorError wraps an error with a permanence flag.
// Permanent errors cause immediate escalation without any restart attempt.
// Transient errors trigger the restart policy.
type ActorError struct {
    Cause     error
    Permanent bool
}

func (e *ActorError) Error() string {
    kind := "transient"
    if e.Permanent {
        kind = "permanent"
    }
    return fmt.Sprintf("[%s] %v", kind, e.Cause)
}

func (e *ActorError) Unwrap() error { return e.Cause }

// Perm wraps an error as permanent. The supervisor will NOT restart the child.
// Use for: invalid credentials, account banned, bad config.
func Perm(err error) *ActorError {
    return &ActorError{Cause: err, Permanent: true}
}

// Trans wraps an error as transient. The supervisor will restart with backoff.
// Use for: network drops, timeouts, rate limits, service unavailable.
func Trans(err error) *ActorError {
    return &ActorError{Cause: err, Permanent: false}
}

// ============================================================
// ChildSpec — describes a supervised child
// ============================================================

// ChildFactory starts an actor. It must:
//   - Respect the provided context (return nil error when ctx is cancelled)
//   - Return a receive-only channel that will receive exactly one value:
//       nil   = clean shutdown (no restart needed)
//       error = crash (supervisor will apply restart policy)
//   - Return an error directly if startup itself fails
type ChildFactory func(ctx context.Context) (<-chan error, error)

// ChildSpec describes a supervised actor's configuration.
type ChildSpec struct {
    Name        string        // unique name within this supervisor
    Factory     ChildFactory  // function that starts the actor
    MaxRestarts int           // max restarts within Window; default 5
    Window      time.Duration // sliding window duration; default 10s
}

// ============================================================
// Internal runtime state
// ============================================================

// childState tracks the live state of a running child.
type childState struct {
    spec         ChildSpec
    generation   uint64      // incremented on each spawn; used to discard stale reports
    errCh        <-chan error // child writes here exactly once on exit
    cancel       context.CancelFunc
    restartTimes []time.Time // timestamps of recent restarts, for budget tracking
}

// crashReport is sent from a watcher goroutine to the supervisor loop.
// The generation field lets us discard reports from old (superseded) instances.
type crashReport struct {
    name       string
    generation uint64
    err        error
}

// ============================================================
// EscalationFunc
// ============================================================

// EscalationFunc is called when a child's restart budget is exhausted,
// or when a permanent error is received. The supervisor passes this
// up to its own parent, forming the escalation chain.
//
// supervisorName: the name of the supervisor reporting the problem
// childName:      the name of the child that failed
// err:            the error that caused the failure
type EscalationFunc func(supervisorName, childName string, err error)

// ============================================================
// Supervisor
// ============================================================

// Supervisor manages a set of child actors and restarts them on failure.
// The Supervisor is itself an actor: it runs a supervision loop goroutine
// that processes crash reports from its children one at a time.
type Supervisor struct {
    name     string
    strategy Strategy
    escalate EscalationFunc // called when a child exceeds its restart budget

    mu       sync.Mutex
    nextGen  uint64
    children []*childState // ordered by registration; order matters for RestForOne

    crashCh chan crashReport // watcher goroutines report crashes here
    ctx     context.Context
    cancel  context.CancelFunc
}

// New creates a Supervisor and starts its internal supervision loop.
//
// escalate is called when a child exceeds its restart budget or has a
// permanent error. Pass nil if this is the root supervisor.
func New(name string, strategy Strategy, escalate EscalationFunc) *Supervisor {
    ctx, cancel := context.WithCancel(context.Background())
    s := &Supervisor{
        name:     name,
        strategy: strategy,
        escalate: escalate,
        crashCh:  make(chan crashReport, 64),
        ctx:      ctx,
        cancel:   cancel,
    }
    go s.supervisionLoop()
    return s
}

// AddChild registers a child spec and starts the child actor.
// Children are ordered by registration time — this matters for RestForOne.
func (s *Supervisor) AddChild(spec ChildSpec) error {
    s.mu.Lock()
    defer s.mu.Unlock()

    cs, err := s.spawnLocked(spec, nil)
    if err != nil {
        return err
    }
    s.children = append(s.children, cs)
    go s.watchChild(cs)
    return nil
}

// Stop shuts down this supervisor and all its children.
func (s *Supervisor) Stop() {
    s.cancel()
}

// ============================================================
// Internal: spawning
// ============================================================

// spawnLocked creates a new childState by calling the factory.
// prevRestarts carries over restart history from the previous instance
// of this child (so the restart budget is not reset on every restart).
// Must be called with s.mu held.
func (s *Supervisor) spawnLocked(spec ChildSpec, prevRestarts []time.Time) (*childState, error) {
    s.nextGen++
    gen := s.nextGen

    ctx, cancel := context.WithCancel(s.ctx)
    errCh, err := spec.Factory(ctx)
    if err != nil {
        cancel()
        return nil, fmt.Errorf("supervisor %s: factory %q failed: %w", s.name, spec.Name, err)
    }

    return &childState{
        spec:         spec,
        generation:   gen,
        errCh:        errCh,
        cancel:       cancel,
        restartTimes: prevRestarts,
    }, nil
}

// ============================================================
// Internal: watching
// ============================================================

// watchChild blocks until the child's errCh produces a value.
// nil means clean shutdown — do nothing.
// non-nil means crash — send a crashReport to the supervisor loop.
//
// Each child instance gets its own dedicated watcher goroutine.
// When the child is restarted, the old watcher goroutine exits
// (it reads nil from the old errCh when the context is cancelled)
// and a new watcher goroutine is spawned for the new instance.
func (s *Supervisor) watchChild(cs *childState) {
    err := <-cs.errCh
    if err == nil {
        return // clean shutdown, nothing to do
    }
    // Report the crash to the supervision loop.
    // If the supervisor itself is shutting down, drop the report.
    select {
    case s.crashCh <- crashReport{
        name:       cs.spec.Name,
        generation: cs.generation,
        err:        err,
    }:
    case <-s.ctx.Done():
    }
}

// ============================================================
// Internal: supervision loop (the supervisor's own actor loop)
// ============================================================

// supervisionLoop is the supervisor's actor loop. It processes crash
// reports serially — one at a time. This is intentional: supervision
// decisions are not concurrent within a single supervisor. This makes
// the logic simple to reason about and avoids races on s.children.
func (s *Supervisor) supervisionLoop() {
    for {
        select {
        case <-s.ctx.Done():
            // Supervisor is stopping — cancel all children.
            s.stopAllChildren()
            return

        case report := <-s.crashCh:
            s.handleCrash(report)
        }
    }
}

// handleCrash is called by the supervision loop when a crash report arrives.
// It applies error classification and the configured restart strategy.
func (s *Supervisor) handleCrash(report crashReport) {
    s.mu.Lock()
    defer s.mu.Unlock()

    idx := s.findChildLocked(report.name)
    if idx < 0 {
        return // child was removed (shouldn't happen in normal operation)
    }

    // Generation check: discard stale crash reports from old instances.
    // This can happen with OneForAll or RestForOne: we cancel a child
    // intentionally, its old watcher goroutine eventually sends nil,
    // but an earlier crash from that instance may have been enqueued
    // before the cancel took effect.
    if s.children[idx].generation != report.generation {
        log.Printf("[supervisor:%s] discarding stale crash report for %q (gen %d != current %d)",
            s.name, report.name, report.generation, s.children[idx].generation)
        return
    }

    // Check if this is a permanent error. If so, escalate immediately.
    // Do NOT restart a child with a permanent error — it would just crash again.
    var ae *ActorError
    if errors.As(report.err, &ae) && ae.Permanent {
        log.Printf("[supervisor:%s] permanent failure in %q: %v — escalating, will NOT restart",
            s.name, report.name, report.err)
        s.doEscalate(report.name, report.err)
        return
    }

    log.Printf("[supervisor:%s] child %q crashed (transient): %v — applying %s strategy",
        s.name, report.name, report.err, s.strategy)

    switch s.strategy {
    case OneForOne:
        s.restartOneLocked(idx, report.err)
    case OneForAll:
        s.restartAllLocked(report.err)
    case RestForOne:
        s.restartFromLocked(idx, report.err)
    }
}

// ============================================================
// Internal: restart strategies
// ============================================================

// restartOneLocked implements OneForOne: restart only the child at idx.
func (s *Supervisor) restartOneLocked(idx int, reason error) {
    cs := s.children[idx]
    newCS := s.attemptRestartLocked(cs, reason)
    if newCS != nil {
        s.children[idx] = newCS
        go s.watchChild(newCS)
    }
}

// restartAllLocked implements OneForAll: stop all children, restart all of them.
// Children are restarted in registration order.
func (s *Supervisor) restartAllLocked(reason error) {
    // Cancel all running children. Their watchChild goroutines will read nil
    // (clean shutdown via context cancellation) and exit without crashing.
    for _, cs := range s.children {
        cs.cancel()
    }
    // Restart all children in registration order.
    for i, cs := range s.children {
        newCS := s.attemptRestartLocked(cs, reason)
        if newCS != nil {
            s.children[i] = newCS
            go s.watchChild(newCS)
        }
    }
}

// restartFromLocked implements RestForOne: stop the crashed child and all
// children registered after it, then restart them in order.
func (s *Supervisor) restartFromLocked(fromIdx int, reason error) {
    // Cancel the crashed child and all downstream children.
    for i := fromIdx; i < len(s.children); i++ {
        s.children[i].cancel()
    }
    // Restart them in order so upstream actors are ready before downstream ones.
    for i := fromIdx; i < len(s.children); i++ {
        newCS := s.attemptRestartLocked(s.children[i], reason)
        if newCS != nil {
            s.children[i] = newCS
            go s.watchChild(newCS)
        }
    }
}

// ============================================================
// Internal: restart budget and backoff
// ============================================================

// attemptRestartLocked checks the restart budget, applies exponential backoff,
// and spawns a new child. Returns nil if the budget is exhausted (escalation
// has already been triggered).
//
// SIMPLIFICATION NOTE: This implementation sleeps while holding s.mu.
// This blocks the supervision loop from processing other crash events
// during the backoff period. In production, you would:
//   1. Release the lock
//   2. Use time.AfterFunc to schedule a deferred restart
//   3. Re-acquire the lock only when spawning the new child
// The simplified approach is used here to keep the teaching example clear.
func (s *Supervisor) attemptRestartLocked(cs *childState, reason error) *childState {
    spec := cs.spec
    now := time.Now()

    // Apply defaults.
    window := spec.Window
    if window == 0 {
        window = 10 * time.Second
    }
    maxRestarts := spec.MaxRestarts
    if maxRestarts == 0 {
        maxRestarts = 5
    }

    // Prune restart timestamps that have aged out of the sliding window.
    recent := make([]time.Time, 0, len(cs.restartTimes))
    for _, t := range cs.restartTimes {
        if now.Sub(t) <= window {
            recent = append(recent, t)
        }
    }

    // Budget check: has this child crashed too many times recently?
    if len(recent) >= maxRestarts {
        log.Printf("[supervisor:%s] child %q exceeded max restarts (%d in %v) — escalating",
            s.name, spec.Name, maxRestarts, window)
        s.doEscalate(spec.Name, reason)
        return nil
    }

    // Compute backoff: 2^n seconds where n = number of recent restarts.
    // This way: first restart = 1s, second = 2s, third = 4s, etc.
    backoff := exponentialBackoff(len(recent))
    log.Printf("[supervisor:%s] restarting %q after %v backoff (restarts in window: %d/%d)",
        s.name, spec.Name, backoff, len(recent)+1, maxRestarts)

    // Record this restart in the history BEFORE sleeping, so the timestamp
    // reflects when we DECIDED to restart, not when it completed.
    recent = append(recent, now)

    // Sleep for the backoff period. See SIMPLIFICATION NOTE above.
    time.Sleep(backoff)

    // Spawn the new child, carrying the updated restart history.
    newCS, err := s.spawnLocked(spec, recent)
    if err != nil {
        log.Printf("[supervisor:%s] factory for %q failed after restart: %v", s.name, spec.Name, err)
        s.doEscalate(spec.Name, err)
        return nil
    }

    return newCS
}

// exponentialBackoff returns a wait duration of 2^attempt seconds, capped at 30s.
//   attempt=0 ->  1s  (2^0)
//   attempt=1 ->  2s  (2^1)
//   attempt=2 ->  4s  (2^2)
//   attempt=3 ->  8s  (2^3)
//   attempt=4 -> 16s  (2^4)
//   attempt=5 -> 30s  (capped)
func exponentialBackoff(attempt int) time.Duration {
    secs := math.Pow(2, float64(attempt))
    if secs > 30 {
        secs = 30
    }
    return time.Duration(secs * float64(time.Second))
}

// ============================================================
// Internal: helpers
// ============================================================

func (s *Supervisor) doEscalate(childName string, err error) {
    if s.escalate != nil {
        s.escalate(s.name, childName, err)
    }
}

func (s *Supervisor) findChildLocked(name string) int {
    for i, cs := range s.children {
        if cs.spec.Name == name {
            return i
        }
    }
    return -1
}

func (s *Supervisor) stopAllChildren() {
    s.mu.Lock()
    defer s.mu.Unlock()
    for _, cs := range s.children {
        cs.cancel()
    }
}
```

### Using the Supervisor

Here is how you wire up the trade engine supervision tree:

```go
func BuildTradeEngine() *Supervisor {
    // Root supervisor: escalation goes nowhere (it IS the root).
    // If SystemSupervisor escalates, we enter emergency mode.
    system := New("SystemSupervisor", OneForOne, func(supName, childName string, err error) {
        log.Printf("CRITICAL: SystemSupervisor escalation from %s/%s: %v", supName, childName, err)
        // In production: send PagerDuty alert, switch to safe mode, etc.
    })

    // ExchangeSupervisor: OneForOne because each exchange is independent.
    exchangeEscalate := func(supName, childName string, err error) {
        // Escalate to system supervisor by calling its crashCh indirectly.
        // In a full implementation, sub-supervisors are children of the system supervisor.
        log.Printf("ESCALATE from %s: %s failed: %v", supName, childName, err)
    }
    exchange := New("ExchangeSupervisor", OneForOne, exchangeEscalate)

    _ = exchange.AddChild(ChildSpec{
        Name:        "BinanceActor",
        Factory:     newBinanceActor,
        MaxRestarts: 5,
        Window:      30 * time.Second,
    })
    _ = exchange.AddChild(ChildSpec{
        Name:        "CoinbaseActor",
        Factory:     newCoinbaseActor,
        MaxRestarts: 5,
        Window:      30 * time.Second,
    })
    _ = exchange.AddChild(ChildSpec{
        Name:        "KrakenActor",
        Factory:     newKrakenActor,
        MaxRestarts: 5,
        Window:      30 * time.Second,
    })

    // TradingSupervisor: OneForAll because strategy/risk/executor share state.
    trading := New("TradingSupervisor", OneForAll, exchangeEscalate)

    _ = trading.AddChild(ChildSpec{
        Name:        "StrategyActor",
        Factory:     newStrategyActor,
        MaxRestarts: 3,
        Window:      60 * time.Second,
    })
    _ = trading.AddChild(ChildSpec{
        Name:        "RiskActor",
        Factory:     newRiskActor,
        MaxRestarts: 3,
        Window:      60 * time.Second,
    })
    _ = trading.AddChild(ChildSpec{
        Name:        "ExecutorActor",
        Factory:     newExecutorActor,
        MaxRestarts: 3,
        Window:      60 * time.Second,
    })

    _ = system
    _ = exchange
    _ = trading

    return system
}

// Example actor factory: a minimal BinanceActor.
func newBinanceActor(ctx context.Context) (<-chan error, error) {
    exitCh := make(chan error, 1)

    // Startup validation happens here. If it fails, return error directly
    // (supervisor treats this as a crash during startup).
    conn, err := connectToBinance()
    if err != nil {
        return nil, Trans(fmt.Errorf("binance connect: %w", err))
    }

    go func() {
        defer close(exitCh)
        exitCh <- runBinanceLoop(ctx, conn)
    }()

    return exitCh, nil
}

func connectToBinance() (interface{}, error) { return struct{}{}, nil } // placeholder

// Placeholder factories for the other actors referenced in BuildTradeEngine above.
// In a real implementation each would establish its own connection and run its own loop.
func newCoinbaseActor(ctx context.Context) (<-chan error, error) {
    exitCh := make(chan error, 1)
    go func() {
        defer close(exitCh)
        <-ctx.Done() // wait for supervisor to cancel us
    }()
    return exitCh, nil
}

func newKrakenActor(ctx context.Context) (<-chan error, error) {
    exitCh := make(chan error, 1)
    go func() {
        defer close(exitCh)
        <-ctx.Done()
    }()
    return exitCh, nil
}

func newStrategyActor(ctx context.Context) (<-chan error, error) {
    exitCh := make(chan error, 1)
    go func() {
        defer close(exitCh)
        <-ctx.Done()
    }()
    return exitCh, nil
}

func newRiskActor(ctx context.Context) (<-chan error, error) {
    exitCh := make(chan error, 1)
    go func() {
        defer close(exitCh)
        <-ctx.Done()
    }()
    return exitCh, nil
}

func newExecutorActor(ctx context.Context) (<-chan error, error) {
    exitCh := make(chan error, 1)
    go func() {
        defer close(exitCh)
        <-ctx.Done()
    }()
    return exitCh, nil
}

func runBinanceLoop(ctx context.Context, conn interface{}) error {
    for {
        select {
        case <-ctx.Done():
            return nil // clean shutdown — supervisor will NOT restart
        default:
            // read from WebSocket, publish prices...
            // if WebSocket drops:
            //   return Trans(fmt.Errorf("binance ws dropped: %w", err))
            //   -> supervisor will restart with backoff
            //
            // if API key revoked:
            //   return Perm(fmt.Errorf("binance: API key invalid"))
            //   -> supervisor will escalate, NOT restart
        }
    }
}
```

---

## 6. The Supervisor IS an Actor Too

This is a subtle but important point that beginners often miss.

The supervisor is not a special god-object that sits outside the actor system. **The supervisor IS an actor.** It has a mailbox (the `crashCh` channel). It processes messages (crash reports) one at a time in a loop. It holds state (the list of children). It runs in its own goroutine.

```
  Child Goroutine            Supervisor Goroutine
  ───────────────            ───────────────────
  [running...]
  [crash happens]
       |
       | watchChild goroutine
       | sends crashReport
       |
       +───────────────────> crashCh (buffered channel = mailbox)
                                         |
                                         | supervisor loop
                                         | reads from crashCh
                                         |
                                    handleCrash()
                                         |
                                    apply strategy
                                    spawn new child
                                    start new watcher
```

Why does this matter?

1. **Non-blocking**: The crashed child does NOT block waiting for the supervisor to restart it. It crashes and exits. The supervisor processes the crash asynchronously. The rest of the system keeps running.

2. **Concurrency-safe**: All supervision decisions are serialized through the supervision loop. There are no races between simultaneous crash reports from different children — they are processed one at a time. The `sync.Mutex` on `s.children` protects against races between AddChild calls and the loop.

3. **Composable**: Because the supervisor is itself an actor, it can itself be a child of another supervisor. This is exactly how the supervision tree is built — `ExchangeSupervisor` is a child actor inside `SystemSupervisor`.

### The Channel Pattern for Crash Reporting

Here is the core pattern distilled to its essence. Every actor must be able to report its exit status to its supervisor. The cleanest Go way to do this is a dedicated exit channel:

```go
// The contract every actor must follow:
//
//   - Accept a context. When the context is cancelled, shut down cleanly
//     and send nil to the exit channel.
//
//   - If the actor crashes due to an error, send that error to the exit channel.
//
//   - Send EXACTLY ONE value. Never send more than once. Close the channel
//     after sending (or use a buffered channel of size 1).

func startMyActor(ctx context.Context) (<-chan error, error) {
    exitCh := make(chan error, 1) // buffered: actor writes without blocking

    go func() {
        defer func() {
            // Recover from panics so the goroutine doesn't silently vanish.
            // Convert the panic into a crash report for the supervisor.
            if r := recover(); r != nil {
                exitCh <- fmt.Errorf("actor panicked: %v", r)
            }
            close(exitCh)
        }()

        err := runActorLoop(ctx)
        if err != nil {
            exitCh <- err
        }
        // nil error: the channel closes without a send,
        // which causes the receive in watchChild to return (nil, false) -> nil.
        // Actually: send nil explicitly for clarity.
        // exitCh <- nil  (but then don't close? use buffered size 1)
    }()

    return exitCh, nil
}
```

The `watchChild` goroutine in the supervisor simply waits on this channel:

```go
func (s *Supervisor) watchChild(cs *childState) {
    err := <-cs.errCh  // blocks here until the child exits
    if err == nil {
        return  // clean shutdown requested by supervisor via context cancel
    }
    // Forward the crash report to the supervision loop's mailbox.
    select {
    case s.crashCh <- crashReport{name: cs.spec.Name, generation: cs.generation, err: err}:
    case <-s.ctx.Done():
        // Supervisor itself is shutting down. Discard the report.
    }
}
```

This is the bridge between the child's goroutine and the supervisor's actor loop. One goroutine exits, writes to a channel, another goroutine reads from that channel and responds. Purely channel-driven. No shared mutable state between child and supervisor.

---

## 7. Error Classification — Transient vs Permanent

One of the most important decisions in a supervision system is: **should this error trigger a restart, or should it trigger escalation?**

Not all errors are created equal. A network timeout is a completely different kind of failure than an invalid API key. The supervisor needs to know the difference.

### The Two Error Classes

```
TRANSIENT errors:                    PERMANENT errors:
- Network connection dropped         - Invalid API key
- WebSocket timeout                  - Account suspended / banned
- Rate limit exceeded (429)          - Symbol not found (bad config)
- Service temporarily unavailable    - Certificate pinning mismatch
- DB connection pool exhausted       - Illegal trade (regulatory halt)
- Parsing error on one message       - Out of memory (unrecoverable)

Action: RESTART with backoff         Action: ESCALATE, do NOT restart
```

### The Go Pattern

```go
// ActorError is the error type that actors return to communicate
// permanence to the supervisor.
type ActorError struct {
    Cause     error
    Permanent bool // true = don't restart, escalate immediately
}

func (e *ActorError) Error() string {
    kind := "transient"
    if e.Permanent {
        kind = "permanent"
    }
    return fmt.Sprintf("[%s] %v", kind, e.Cause)
}

func (e *ActorError) Unwrap() error { return e.Cause }

// Actors use these constructors to signal their intent:
func Perm(err error) *ActorError  { return &ActorError{Cause: err, Permanent: true} }
func Trans(err error) *ActorError { return &ActorError{Cause: err, Permanent: false} }
```

### How the Supervisor Inspects the Error

```go
// In handleCrash, BEFORE applying any restart strategy:
var ae *ActorError
if errors.As(report.err, &ae) && ae.Permanent {
    // Permanent: skip the restart strategy entirely.
    // This error will recur if we restart. Don't restart.
    log.Printf("[supervisor:%s] permanent failure in %q: %v",
        s.name, report.name, report.err)
    s.doEscalate(report.name, report.err)
    return
}
// Transient: proceed with the restart strategy (OneForOne, etc.)
```

`errors.As` is used rather than a type assertion because the error may be wrapped in another error by the time it reaches the supervisor. `errors.As` unwraps the chain to find the `*ActorError`.

### Real Usage in a BinanceActor

```go
func runBinanceLoop(ctx context.Context, client *BinanceClient) error {
    for {
        select {
        case <-ctx.Done():
            return nil // context cancelled = clean shutdown, no restart
        default:
        }

        msg, err := client.ReadMessage()
        if err != nil {
            // Is this a permanent or transient failure?
            switch {
            case isAuthError(err):
                // API key revoked. Restarting won't help. Escalate.
                return Perm(fmt.Errorf("binance auth failed: %w", err))

            case isRateLimitError(err):
                // Temporary. Back off and retry by letting the supervisor restart us.
                return Trans(fmt.Errorf("binance rate limited: %w", err))

            case isNetworkError(err):
                // Connection dropped. Supervisor will reconnect.
                return Trans(fmt.Errorf("binance ws error: %w", err))

            default:
                // Unknown error — treat as transient (safer than hanging forever).
                return Trans(fmt.Errorf("binance unknown error: %w", err))
            }
        }

        if err := processMessage(msg); err != nil {
            // A bad message: transient. Log it and let the supervisor restart.
            return Trans(fmt.Errorf("binance message processing: %w", err))
        }
    }
}

func isAuthError(err error) bool {
    // Check for specific error codes from the exchange API.
    var apiErr *BinanceAPIError
    return errors.As(err, &apiErr) && (apiErr.Code == 401 || apiErr.Code == 403)
}

func isRateLimitError(err error) bool {
    var apiErr *BinanceAPIError
    return errors.As(err, &apiErr) && apiErr.Code == 429
}

func isNetworkError(err error) bool {
    var netErr interface{ Timeout() bool }
    return errors.As(err, &netErr)
}

// Placeholder types for illustration:
type BinanceClient struct{}
type BinanceAPIError struct{ Code int }

func (e *BinanceAPIError) Error() string { return fmt.Sprintf("binance API error %d", e.Code) }
func (c *BinanceClient) ReadMessage() ([]byte, error) { return nil, nil }
func processMessage(_ []byte) error { return nil }
```

### Unclassified Errors and Panics

What if an actor returns a plain `error` that is NOT wrapped in `ActorError`? The supervisor should treat it as transient by default — a restart is always safer than permanently halting a critical component without a human decision.

What about panics? An actor goroutine that panics will crash the entire program by default. You must recover from panics in every actor's goroutine and convert them to errors:

```go
go func() {
    defer func() {
        if r := recover(); r != nil {
            // Convert panic to a transient error. The supervisor restarts us.
            // If the panic recurs on every restart, the budget will exhaust
            // and escalate — bringing it to human attention.
            exitCh <- Trans(fmt.Errorf("actor panic: %v", r))
        }
        close(exitCh)
    }()
    exitCh <- runActorLoop(ctx)
}()
```

---

## 8. Real Trade Engine Failure Scenarios

Let's walk through three real scenarios and trace exactly what happens in the supervision tree.

---

### Scenario A — Binance WebSocket Drops

This is the most common failure in a live trade engine. WebSocket connections drop all the time.

```
What the system looks like at T=0:

SystemSupervisor [OK]
+-- ExchangeSupervisor [OK]    (OneForOne)
|   +-- BinanceActor   [OK]   <-- WebSocket connected, streaming prices
|   +-- CoinbaseActor  [OK]
|   +-- KrakenActor    [OK]
+-- TradingSupervisor  [OK]
|   +-- StrategyActor  [OK]   <-- Receiving prices from all 3 exchanges
|   +-- RiskActor      [OK]
|   +-- ExecutorActor  [OK]
+-- InfraSupervisor    [OK]
    +-- LoggerActor    [OK]
    +-- MetricsActor   [OK]
    +-- DatabaseActor  [OK]
```

**T=1s: Binance drops the WebSocket connection.**

```
BinanceActor:
  ws.ReadMessage() -> returns EOF error
  return Trans(fmt.Errorf("binance ws: %w", err))
  -> BinanceActor goroutine exits
  -> watchChild goroutine reads the error
  -> sends crashReport{name:"BinanceActor", generation:1, err:Trans("ws EOF")}
     to ExchangeSupervisor.crashCh
```

**T=1s + epsilon: ExchangeSupervisor processes the crash report.**

```
ExchangeSupervisor supervision loop:
  handleCrash({name:"BinanceActor", generation:1, err:Trans("ws EOF")})
  - errors.As(err, &ae) -> ae.Permanent = false -> will restart
  - strategy is OneForOne -> restartOneLocked(idx=0, reason)
  - attemptRestartLocked:
      recent restarts in 30s window: 0
      backoff for attempt 0: 2^0 = 1 second
      sleep 1s...
      spawnLocked("BinanceActor", recent=[T=1s])
      -> calls newBinanceActor(ctx)
      -> new WebSocket connection established
      -> returns new childState with generation=2
  - s.children[0] = newCS
  - go watchChild(newCS)
```

**T=2s: BinanceActor is back online.**

```
SystemSupervisor [OK] -- never knew anything happened
+-- ExchangeSupervisor [OK]
|   +-- BinanceActor   [NEW gen=2]  <-- reconnected, streaming again
|   +-- CoinbaseActor  [OK]         <-- was NEVER interrupted
|   +-- KrakenActor    [OK]         <-- was NEVER interrupted
+-- TradingSupervisor  [OK]         <-- never knew anything happened
...
```

**What StrategyActor experienced:** A 1-second gap in Binance price data. It was still receiving data from Coinbase and Kraken. If it is designed to handle single-source gaps (it should be), it never even noticed.

---

### Scenario B — Database Connection Dies and Stays Dead

This scenario tests the escalation path.

```
T=0s:  All systems nominal.

T=30s: DatabaseActor's connection pool times out.
       DatabaseActor returns Trans(fmt.Errorf("db: connection timeout"))
       InfraSupervisor: restart 1 after 1s backoff.

T=32s: DatabaseActor starts, immediately fails again (DB is down).
       InfraSupervisor: restart 2 after 2s backoff.

T=35s: Fails again. Restart 3 after 4s backoff.
T=40s: Fails again. Restart 4 after 8s backoff.
T=49s: Fails again. Restart 5 after 16s backoff.

       [restart history: [32s, 35s, 40s, 49s, 65s]]
       [5 restarts within 30s window? Let's check:]

       Window=30s. Now at T=65s.
       Restarts within window (T=35s to T=65s): [35s, 40s, 49s, 65s] = 4
       Not yet exceeded. One more try.

T=66s: Fails again. Now 5 restarts in window. BUDGET EXHAUSTED.

InfraSupervisor:
  "DatabaseActor exceeded 5 restarts in 30s — escalating"
  calls escalate("InfraSupervisor", "DatabaseActor", err)
```

**SystemSupervisor receives the escalation:**

```
SystemSupervisor:
  "InfraSupervisor/DatabaseActor has exceeded restart budget"
  
  Decision tree:
  
  Is this the Logger? -> No, we can still log to stderr
  Is this the DB?     -> YES
  
  Action: Switch to emergency mode
    1. Signal all actors to use in-memory storage instead of DB
    2. Send alert: POST to PagerDuty API with "DatabaseActor dead"
    3. Set a flag: acceptNewTrades = false (don't open new positions)
    4. Keep existing positions running (don't force-close them)
    5. Attempt manual DB restart after 5 minutes (longer window)

SystemSupervisor state: DEGRADED — trading read-only, alerting human
```

This is **graceful degradation**: the system is not dead, but it is running in a reduced capacity and has alerted a human.

---

### Scenario C — Strategy Bug Causes Repeated Panics

This is the most dangerous scenario: a bug in your own code, not an external service.

```
T=0s: Developer deploys new StrategyActor code with a nil pointer bug.
      The bug triggers when a specific market condition occurs.

T=5s: Market condition occurs.
      StrategyActor's goroutine panics with: "runtime error: invalid memory address"
      The recover() wrapper catches it:
        exitCh <- Trans(fmt.Errorf("actor panic: runtime error: invalid memory address"))
      watchChild sends crashReport to TradingSupervisor.

TradingSupervisor (OneForAll):
  handleCrash -> transient error -> restart ALL (strategy+risk+executor)
  backoff 1s, restart all three.

T=6s: All three restart cleanly.

T=6s: Market condition occurs again immediately.
      StrategyActor panics again.
      TradingSupervisor: restart 2 after 2s backoff.

T=9s: Panics again. Restart 3 after 4s backoff.
T=14s: Panics again. Restart 4 after 8s backoff.
T=23s: Panics again. Restart 5 after 16s backoff.

      5 restarts in window. BUDGET EXHAUSTED.
```

**TradingSupervisor escalates to SystemSupervisor:**

```
TradingSupervisor:
  "StrategyActor (triggered OneForAll) exceeded restart budget — escalating"
  calls escalate("TradingSupervisor", "StrategyActor", err)
```

**SystemSupervisor's response — this is where safe mode matters:**

```
SystemSupervisor receives escalation from TradingSupervisor:

  The trading cluster has destabilized. We have a bug.
  
  Critical question: are there open positions?
  
  Action A — Safe Mode:
    1. DO NOT restart TradingSupervisor (it would just crash again)
    2. Send immediate cancel-all-orders to ExchangeActors
       (close open positions at market, or move stops to breakeven)
    3. Pause all new trade signals
    4. Send CRITICAL alert: "StrategyActor repeatedly panicking, trading halted"
    5. Log the panic stack trace to permanent storage
    6. Wait for human to deploy fix and manually restart

  The system state:
  
  SystemSupervisor [OK]
  +-- ExchangeSupervisor [OK]    <-- still connected, ready to execute cancel orders
  +-- TradingSupervisor  [HALTED] <-- children stopped, not restarting
  +-- InfraSupervisor    [OK]    <-- logging everything, DB intact

  Humans are paged. System is safe. Positions are protected.
```

**Why OneForAll was the right choice for TradingSupervisor:**

If we had used OneForOne, StrategyActor would restart while RiskActor and ExecutorActor continued running with their old state. StrategyActor in its fresh state might send orders that conflict with pending orders ExecutorActor still thinks are live. OneForAll prevents this inconsistency.

**Why the panic was treated as transient (not permanent):**

The panic happens at a specific market condition — it's a bug, but we don't know if it's permanent from the actor's perspective. We give it a few tries. After repeated failures, the budget exhausts and escalates. The escalation handler then makes a high-level decision (safe mode). This is the right distribution of concerns:
- The actor: detects failure, exits cleanly
- The local supervisor: attempts recovery
- The root supervisor: makes business decisions under uncertainty

---

## Summary

```
Concept                    Key Takeaway
─────────────────────────  ────────────────────────────────────────────────────
Supervision                Parent is responsible for child lifecycle
Let it crash               Crash fast + restart clean > complex error handling
Supervision tree           Hierarchy of fault domains and escalation paths
OneForOne                  Independent children (exchange connectors)
OneForAll                  Interdependent children (strategy+risk+executor)
RestForOne                 Linear pipeline (normalizer->orderbook->strategy)
Exponential backoff        Don't hammer a dead service; give it time to recover
Sliding window             Detect repeated crashes, not just isolated ones
Permanent vs transient     Classification drives restart vs immediate escalation
Supervisor as actor        Crash reports are messages; decisions are serialized
```

The supervision tree is not an add-on — it is the **skeleton** of a fault-tolerant system. Every actor you write should be designed with its supervisor in mind. Every failure path should end with a clean exit and a classified error. The supervisor handles the rest.

---

*Next: `06-messaging-patterns.md` — Request/Reply, Publish/Subscribe, and Pipeline patterns in Go actors.*
