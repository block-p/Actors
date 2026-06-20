# Building an Actor From Scratch — The Thinking Process

## Before Writing Code — Ask These Questions

When you build an actor system, you need to answer:

1. **What IS an actor in my language?**
   - It needs: a mailbox, a loop, private state, a behavior

2. **How does one actor find another?**
   - You need some kind of "address" or "reference"

3. **How do actors start and stop?**
   - Something needs to manage their lifecycle

4. **What happens when one crashes?**
   - Supervision strategy

Let's build the simplest possible actor system step by step.
The goal is to understand — not to build a production framework.

---

## The Absolute Minimum Actor

Forget frameworks. Strip it down.

What is the MINIMUM you need?

```
1. A channel        → the mailbox
2. A goroutine      → the actor "running"
3. A struct         → the private state
4. A for loop       → "listen forever for messages"
```

That's it. 4 things.

---

## Step 1 — A Channel Is Already a Mailbox

```
ch := make(chan Message, 100)
```

- `ch` is the mailbox
- `100` is the mailbox capacity (backpressure)
- Anyone who has `ch` can send to this actor
- But only THIS actor reads from it

The channel reference IS the actor's address.

---

## Step 2 — A Goroutine Is the Actor Running

```
go func() {
    for msg := range ch {
        // handle msg
    }
}()
```

- This goroutine runs forever
- It processes ONE message at a time (the `range` loop)
- No other code touches this goroutine's local variables

---

## Step 3 — Local Variables Are the Private State

```
go func() {
    count := 0          // <-- private state, nobody else can access this
    name  := "counter"  // <-- private state

    for msg := range ch {
        // handle msg, can read/write count and name freely
        // NO LOCKS NEEDED because only this goroutine runs here
    }
}()
```

This is the key insight:
**Local variables inside the goroutine = private actor state.**
No mutex. No sync. No race condition. It's just a local variable.

---

## Step 4 — The Behavior Is the Switch Statement

```
for msg := range ch {
    switch m := msg.(type) {
    case Increment:
        count += m.Amount
    case GetCount:
        m.ReplyTo <- count   // send back the answer
    case Reset:
        count = 0
    }
}
```

Each `case` is how the actor REACTS to a message.
This is the behavior. This is where your logic lives.

---

## Putting It All Together — Counter Actor

Read this slowly. Every line matters.

```go
package main

import "fmt"

// --- Messages ---
// Messages are just types. Any type can be a message.

type Increment struct{ Amount int }
type Reset     struct{}
type GetCount  struct{ ReplyTo chan int }

// --- The Actor ---
// Returns a channel — that channel IS the actor's address.

func NewCounterActor() chan any {
    mailbox := make(chan any, 10)

    go func() {
        // This is the PRIVATE STATE.
        // Only this goroutine can touch it. No locks needed.
        count := 0

        // This loop IS the actor running.
        // It processes one message at a time, forever.
        for msg := range mailbox {
            switch m := msg.(type) {

            case Increment:
                // Rule 1: change own state
                count += m.Amount
                fmt.Printf("Counter is now: %d\n", count)

            case Reset:
                // Rule 1: change own state
                count = 0
                fmt.Println("Counter reset to 0")

            case GetCount:
                // Rule 2: send a message to another actor (or caller)
                m.ReplyTo <- count
            }
        }
    }()

    return mailbox // this IS the actor's address
}

func main() {
    counter := NewCounterActor()

    // Send messages — we never touch the state directly
    counter <- Increment{Amount: 5}
    counter <- Increment{Amount: 3}
    counter <- Increment{Amount: 2}

    // Ask for the count
    reply := make(chan int, 1)
    counter <- GetCount{ReplyTo: reply}
    fmt.Printf("Final count: %d\n", <-reply)

    counter <- Reset{}

    counter <- GetCount{ReplyTo: reply}
    fmt.Printf("After reset: %d\n", <-reply)
}
```

Output:
```
Counter is now: 5
Counter is now: 8
Counter is now: 10
Final count: 10
Counter reset to 0
After reset: 0
```

Notice:
- You never called a method on the actor directly
- You never accessed `count` from outside
- Everything went through the channel (mailbox)

---

## The Ask Pattern (Request/Response)

Sometimes you need an answer back. This is called "Ask".

```
Caller                    Actor
  |                         |
  |-- GetCount{ReplyTo} --> |
  |                         | processes message
  |                         | sends count to ReplyTo
  | <------ count ----------|
  |                         |
```

The trick: create a **temporary reply channel** just for this one response.

```go
// Create a one-time reply channel
reply := make(chan int, 1)  // buffered so actor never blocks

// Ask the actor
counter <- GetCount{ReplyTo: reply}

// Wait for the answer
count := <-reply
```

Why buffer size 1? So if you time out and abandon the reply,
the actor can still send without blocking forever.

---

## Two Actors Talking to Each Other

This is where it gets interesting.
Two independent goroutines, each with their own state,
communicating only through messages.

```go
package main

import "fmt"

// --- Messages ---
type Ping struct{ ReplyTo chan any }
type Pong struct{}
type Stop struct{}

// --- Ping Actor ---
func NewPingActor(pong chan any) chan any {
    mailbox := make(chan any, 10)

    go func() {
        count := 0 // private state

        for msg := range mailbox {
            switch msg.(type) {
            case Ping:
                count++
                fmt.Printf("Ping! (sent %d times)\n", count)
                // send Pong back to the pong actor
                pong <- Pong{}

                if count >= 3 {
                    pong <- Stop{}
                    return // actor stops itself
                }
            }
        }
    }()

    return mailbox
}

// --- Pong Actor ---
func NewPongActor(ping chan any) chan any {
    mailbox := make(chan any, 10)

    go func() {
        for msg := range mailbox {
            switch msg.(type) {
            case Pong:
                fmt.Println("Pong!")
                // reply back to ping
                ping <- Ping{}

            case Stop:
                fmt.Println("Pong actor stopping")
                return
            }
        }
    }()

    return mailbox
}

func main() {
    // Wire them together
    // Note: we need to break the circular dependency
    // So we create pong first, then ping with pong's address

    pongMailbox := make(chan any, 10)
    pingMailbox := make(chan any, 10)

    // Start pong actor, it knows ping's address
    go func() {
        for msg := range pongMailbox {
            switch msg.(type) {
            case Pong:
                fmt.Println("Pong!")
                pingMailbox <- Ping{}
            case Stop:
                fmt.Println("Pong actor stopping")
                return
            }
        }
    }()

    // Start ping actor, it knows pong's address
    done := make(chan struct{})
    go func() {
        count := 0
        for msg := range pingMailbox {
            switch msg.(type) {
            case Ping:
                count++
                fmt.Printf("Ping! (%d/3)\n", count)
                pongMailbox <- Pong{}
                if count >= 3 {
                    pongMailbox <- Stop{}
                    close(done)
                    return
                }
            }
        }
    }()

    // Start the conversation
    pingMailbox <- Ping{}

    <-done // wait for it to finish
}
```

Output:
```
Ping! (1/3)
Pong!
Ping! (2/3)
Pong!
Ping! (3/3)
Pong actor stopping
```

---

## What You Just Learned

| Concept              | How it maps in Go                        |
|----------------------|------------------------------------------|
| Mailbox              | `chan any` (buffered)                    |
| Actor running        | `go func() { for msg := range ch {} }()`|
| Private state        | Local variables inside the goroutine     |
| Send message         | `ch <- msg`                              |
| Actor address        | The channel itself                       |
| Ask (request/reply)  | Reply channel inside the message         |
| Actor stops          | `return` from the goroutine loop         |

---

## What's Missing in These Examples

These examples are intentionally simple. A real actor system also needs:

1. **Supervision** — restart crashed actors
2. **Named actors** — look up actor by name, not just channel variable
3. **Typed messages** — right now we use `any`, could be more type-safe
4. **Lifecycle hooks** — `OnStart`, `OnStop`, `OnCrash`
5. **Timeouts on Ask** — what if the actor never replies?

Next → See the full Go example in `go-example/` and Rust example in `rust-example/`
