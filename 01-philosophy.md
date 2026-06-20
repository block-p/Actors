# The Actor Model — Philosophy & Mental Model

## 1. The Problem Actors Solve

Before actors, concurrent programming looked like this:

```
Thread A        Thread B
   |               |
   |-- lock(x) --> |
   |               |-- lock(x) --> BLOCKED...
   | writes x      |
   |-- unlock(x)-->|
                   |-- lock(x) --> OK
                   | writes x
                   |-- unlock(x)
```

You have **shared memory** + **locks** to protect it.
This leads to:

- Deadlocks (A waits for B, B waits for A → forever)
- Race conditions (you forgot a lock somewhere)
- Hard to reason about (who owns this data right now?)
- Hard to scale (one big lock = bottleneck)

## 2. The Core Insight of Actors

> "Instead of sharing memory and protecting it with locks,
>  give each piece of state its own private owner,
>  and let them talk only by sending messages."

This is it. This is the whole philosophy.

No shared memory.
No locks.
Only messages.

## 3. What IS an Actor?

An actor is the **smallest unit of computation** in this model.

Every actor has exactly three things:

```
┌─────────────────────────────┐
│           ACTOR             │
│                             │
│  📬 Mailbox (message queue) │  ← messages arrive here
│                             │
│  🧠 Behavior (your logic)   │  ← processes one message at a time
│                             │
│  📦 Private State           │  ← nobody else can touch this
└─────────────────────────────┘
```

Key rule: **an actor processes ONE message at a time.**
No concurrency INSIDE an actor. Concurrency happens BETWEEN actors.

## 4. What Can an Actor DO When It Receives a Message?

Only three things. Nothing more:

```
Receive a message
      │
      ├── 1. Change its own state
      │         (update a counter, store a price, etc.)
      │
      ├── 2. Send messages to other actors
      │         (tell executor to place an order)
      │
      └── 3. Create new actors
                (spawn a new connection handler)
```

That's the entire programming model.

## 5. The Mailbox Mental Model

Think of actors like people in an office:

```
                    📬 Mailbox
                   ┌──────────┐
  [msg1] [msg2] → │ pending  │
  [msg3]           └──────────┘
                        │
                        ↓ (one at a time)
                   ┌──────────┐
                   │  Person  │ ← doing ONE thing at a time
                   │ (Actor)  │   no interruptions
                   └──────────┘
```

The person:
- Finishes what they're doing before reading the next message
- Their desk (state) is ONLY accessible to them
- To ask someone else something, they write a note (send a message)
- They never walk over and grab something off someone else's desk

## 6. Location Transparency

One beautiful property of actors:

> You don't know WHERE an actor is. You only hold a reference to its mailbox.

```
actorRef.Send(message)
   │
   └─→ could be:
         - same goroutine
         - different goroutine
         - different process
         - different machine
```

Your code looks the same regardless.
This is how Erlang runs the same code on 1 machine or 1000 machines.

## 7. Supervision — "Let It Crash"

Traditional programming:
```
error happens → catch it → try to recover → complicated cleanup code
```

Actor philosophy (from Erlang):
```
error happens → actor crashes → supervisor restarts it fresh
```

This is called **"let it crash"**.

Instead of writing defensive code inside every actor,
you build a **supervision tree**:

```
          SupervisorActor
         /       |        \
    Binance  Coinbase   Strategy
    Actor    Actor      Actor
                           |
                       Risk Actor
```

Rules:
- If a child crashes → supervisor decides: restart it? stop it? restart all siblings?
- Supervisors can have supervisors
- System stays alive even when parts crash

For a trade engine this is CRITICAL.
Binance connection drops → only the Binance actor restarts.
Your strategy keeps running. Your other exchanges keep running.

## 8. Backpressure — The Mailbox as a Buffer

When one actor is slow and another is fast:

```
Fast Producer Actor  →→→→→→→→→→  📬 [msg][msg][msg][msg][msg]...
                                         Slow Consumer Actor
```

The mailbox fills up.
You can:
- Set a max mailbox size → sender blocks or gets an error
- Monitor mailbox depth → alert when it grows too big
- Drop old messages → use a ring buffer mailbox

This is built-in flow control. No special code needed.

## 9. Actors vs Threads vs Goroutines

| Concept       | Thread          | Goroutine       | Actor           |
|---------------|-----------------|-----------------|-----------------|
| Weight        | Heavy (1MB+)    | Light (2KB)     | Conceptual      |
| Communication | Shared memory   | Channels        | Messages        |
| State         | Shared (risky)  | Shared (risky)  | Private (safe)  |
| Count         | Hundreds        | Millions        | Millions        |
| Failure       | Kills process   | panic()         | Supervisor      |

Goroutines are the IMPLEMENTATION.
Actors are the PATTERN on top.

In Go: goroutine + channel = actor (conceptually)
In Rust: tokio task + channel = actor (conceptually)

## 10. When NOT to Use Actors

Actors are NOT always the answer.

Avoid actors when:
- Simple sequential logic — just use a function
- CPU-bound number crunching — use a worker pool, not actors
- You need strict call/response — actors are async, it gets awkward
- Tiny scripts — the overhead isn't worth it

Use actors when:
- Multiple independent things happening concurrently
- Need fault isolation (one crash shouldn't kill everything)
- Long-lived stateful processes (connections, sessions, order books)
- Natural "entities" in your domain (each exchange IS an actor)

## Summary

| Principle            | What it means                                      |
|----------------------|----------------------------------------------------|
| No shared state      | Each actor owns its data privately                 |
| Message passing only | The only way to communicate                        |
| One message at a time| No internal concurrency, no locks needed           |
| Location transparent | You don't care where the actor runs                |
| Let it crash         | Supervisors handle failure, not defensive catch    |
| Backpressure         | Mailbox size controls flow naturally               |

Next → Read `02-actor-from-scratch.md` to see the minimal implementation.
