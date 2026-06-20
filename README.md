# Learn Actors — Complete Guide

Read the docs in order. Then run the Go examples. Each example is a standalone program.

---

## The Docs

| File | What it covers |
|------|----------------|
| `01-philosophy.md` | WHY actors exist. The problem with locks. Mental model. The 5 rules. When NOT to use actors. |
| `02-actor-from-scratch.md` | HOW to think about building one. Channel = mailbox. Goroutine = actor. Local var = private state. |
| `03-messages.md` | Everything about messages. Commands vs Events vs Queries. Fire-and-forget. Ask. Ask with timeout. Poison Pill. Message ordering. |
| `04-lifecycle.md` | Actor birth, life, and death. PreStart. Never blocking in Receive. Offloading work to goroutines. Graceful shutdown. Done channel pattern. |
| `05-supervision.md` | Let it crash. Supervision trees. OneForOne vs OneForAll vs RestForOne. Backoff. Max restarts. Error classification. Real trade engine failure scenarios. |
| `06-patterns.md` | Router. FSM Actor. Correlation IDs. Pipeline. Scatter-Gather. Dead Letter Box. Saga. |
| `07-pitfalls.md` | Every mistake that will kill your system in production. With wrong code, right code, and how to detect each bug. |

---

## The Go Examples

Each directory is a self-contained runnable program. Run any of them with `go run main.go`.

```
go-example/
├── main.go                  ← Start here. The full system: primitives + counter + supervision + trade system
├── 02_supervision/          ← OneForOne, OneForAll, max restarts, exponential backoff
├── 03_ask_timeout/          ← Ask pattern, timeouts, retry logic, goroutine leak prevention
├── 04_lifecycle/            ← PreStart, graceful shutdown, blocking work offload, crash recovery
├── 05_router/               ← Round-Robin, Random, Hash routing with stats
├── 06_fsm/                  ← Trade order as FSM: New→Submitted→Filled/Cancelled/Rejected
├── 07_pipeline/             ← 4-stage pipeline: RawFeed→Normalizer→Enricher→Strategy
└── 08_scatter_gather/       ← Best price aggregation across 4 exchanges with timeout handling
```

### Run them all at once:
```sh
cd go-example
for dir in . 02_supervision 03_ask_timeout 04_lifecycle 05_router 06_fsm 07_pipeline 08_scatter_gather; do
    echo "=== $dir ===" && (cd $dir && go run main.go)
done
```

### Or run individually:
```sh
cd go-example/02_supervision && go run main.go
cd go-example/06_fsm         && go run main.go
# etc.
```

---

## The Rust Example

```sh
cd rust-example && cargo run
```

Shows the same concepts as Go but with Rust's enum-per-actor pattern — the compiler catches wrong message types at compile time instead of runtime.

---

## The 5 Rules (memorize these)

```
1. An actor OWNS its state — nobody else can touch it
2. ONLY messages — no shared memory, no locks
3. ONE message at a time — no concurrency inside an actor
4. LET IT CRASH — supervisors restart, actors don't defend themselves
5. MAILBOX = backpressure — full mailbox slows the sender naturally
```

## Go Actor Cheat Sheet

```go
// Mailbox
mailbox := make(chan Message, 100)

// Actor running (goroutine + loop = actor)
go func() {
    state := MyState{}                // private state
    for msg := range mailbox {        // one message at a time
        switch m := msg.(type) {
        case DoThing:   state.doThing(m)
        case GetValue:  m.Reply <- state.value
        default:        log.Printf("unknown: %T", msg)
        }
    }
}()

// Send (fire and forget)
mailbox <- DoThing{Arg: "x"}

// Ask (request/response) — ALWAYS buffer the reply channel
reply := make(chan int, 1)
mailbox <- GetValue{Reply: reply}
select {
case v := <-reply:
    // use v
case <-time.After(500 * time.Millisecond):
    // timeout — never block forever
}

// Graceful shutdown — poison pill
done := make(chan struct{})
mailbox <- StopActor{Done: done}
<-done
```

## Go vs Rust Quick Reference

| | Go | Rust |
|---|---|---|
| Mailbox | `chan any` | `mpsc::Receiver<MyMsg>` |
| Actor address (ActorRef) | the channel `chan any` | `mpsc::Sender<MyMsg>` |
| Private state | local vars inside goroutine | local vars inside tokio task |
| Actor loop | `for msg := range ch {}` | `while let Some(msg) = rx.recv().await {}` |
| Message dispatch | `switch msg.(type)` | `match msg {}` |
| Reply channel | `make(chan T, 1)` (buffered!) | `oneshot::channel()` |
| Supervision | `recover()` + restart loop | task restart loop |
| Message type safety | runtime — can fail in prod | compile time — caught by compiler |
