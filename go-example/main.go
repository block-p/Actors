// Actor Model from Scratch in Go
//
// This file builds the actor system piece by piece:
//   Part 1 — The primitives (what an actor IS)
//   Part 2 — A counter actor (simplest possible actor)
//   Part 3 — A supervisor (what happens when actors crash)
//   Part 4 — A mini trade system (3 actors talking)
//
// Run with: go run main.go

package main

import (
	"fmt"
	"log"
	"sync"
	"time"
)

// =============================================================================
// PART 1 — PRIMITIVES
// What is an actor in Go?
// =============================================================================

// Message is any value. We use `any` so any type can be a message.
// In a real system you might use a typed interface instead.
type Message any

// ActorRef is an actor's "address". It's how you send messages to an actor.
// You never touch the actor's internals — only this handle.
//
// Think of it like a mailbox slot on a door.
// You can push letters in. You never go inside the house.
type ActorRef struct {
	mailbox chan Message
}

// Send puts a message into the actor's mailbox.
// Non-blocking if mailbox has capacity, blocks if full (backpressure).
func (r *ActorRef) Send(msg Message) {
	r.mailbox <- msg
}

// TrySend sends without blocking. Returns false if mailbox is full.
func (r *ActorRef) TrySend(msg Message) bool {
	select {
	case r.mailbox <- msg:
		return true
	default:
		return false
	}
}

// Context is given to an actor when it handles a message.
// It lets the actor know who it is and talk to the system.
type Context struct {
	Self   *ActorRef
	System *System
}

// Spawn creates a new actor and returns its address.
// Usage: ref := ctx.Spawn("child-name", &MyActor{}, 100)
func (c *Context) Spawn(name string, a Actor, mailboxSize int) *ActorRef {
	return c.System.Spawn(name, a, mailboxSize)
}

// Actor is the behavior interface. You implement Receive.
// Receive is called for each message, one at a time.
// Your state lives inside the struct that implements this.
type Actor interface {
	Receive(ctx *Context, msg Message)
}

// System manages all actors.
// It's the registry — you can look up actors by name.
type System struct {
	mu     sync.RWMutex
	actors map[string]*ActorRef
}

func NewSystem() *System {
	return &System{actors: make(map[string]*ActorRef)}
}

// Spawn creates an actor goroutine and registers it.
func (s *System) Spawn(name string, a Actor, mailboxSize int) *ActorRef {
	ref := &ActorRef{mailbox: make(chan Message, mailboxSize)}
	ctx := &Context{Self: ref, System: s}

	s.mu.Lock()
	s.actors[name] = ref
	s.mu.Unlock()

	go func() {
		for msg := range ref.mailbox {
			a.Receive(ctx, msg)
		}
		log.Printf("[%s] stopped", name)
	}()

	log.Printf("[system] spawned actor: %s", name)
	return ref
}

// SpawnSupervised wraps the actor in a recovery loop.
// If Receive panics, the actor restarts from a fresh instance.
func (s *System) SpawnSupervised(name string, factory func() Actor, mailboxSize int) *ActorRef {
	ref := &ActorRef{mailbox: make(chan Message, mailboxSize)}

	s.mu.Lock()
	s.actors[name] = ref
	s.mu.Unlock()

	go func() {
		for {
			a := factory() // fresh actor each restart
			ctx := &Context{Self: ref, System: s}

			crashed := false
			func() {
				defer func() {
					if r := recover(); r != nil {
						log.Printf("[%s] CRASHED: %v — restarting in 100ms...", name, r)
						crashed = true
					}
				}()
				for msg := range ref.mailbox {
					a.Receive(ctx, msg)
				}
			}()

			if !crashed {
				// mailbox was closed — clean exit, don't restart
				log.Printf("[%s] stopped cleanly", name)
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	log.Printf("[system] spawned supervised actor: %s", name)
	return ref
}

// Get finds an actor by name.
func (s *System) Get(name string) (*ActorRef, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ref, ok := s.actors[name]
	return ref, ok
}

// =============================================================================
// PART 2 — COUNTER ACTOR (Simplest example)
// =============================================================================
//
// Philosophy: The counter's VALUE lives INSIDE the actor.
// Nobody outside can read or write it directly.
// You ask for it by sending a message.

// Messages for the counter actor
type (
	Increment struct{ By int }
	Decrement struct{ By int }
	GetValue  struct{ Reply chan int }
	Reset     struct{}
)

// CounterActor is the simplest actor.
// Its only state: a single integer.
type CounterActor struct {
	value int // PRIVATE — only Receive can touch this
}

func (a *CounterActor) Receive(ctx *Context, msg Message) {
	switch m := msg.(type) {

	case Increment:
		a.value += m.By
		// No need to reply — fire and forget

	case Decrement:
		a.value -= m.By

	case Reset:
		a.value = 0

	case GetValue:
		// Send the current value back through the reply channel.
		// This is the "Ask" pattern.
		m.Reply <- a.value
	}
}

func runCounterExample(sys *System) {
	fmt.Println("\n--- Part 2: Counter Actor ---")

	counter := sys.Spawn("counter", &CounterActor{}, 20)

	// Fire and forget — we don't wait for these
	counter.Send(Increment{By: 10})
	counter.Send(Increment{By: 5})
	counter.Send(Decrement{By: 3})

	// Ask — we need the answer back
	reply := make(chan int, 1) // buffered! always buffer reply channels
	counter.Send(GetValue{Reply: reply})
	fmt.Printf("Counter value: %d\n", <-reply) // expect 12

	counter.Send(Reset{})
	counter.Send(GetValue{Reply: reply})
	fmt.Printf("After reset: %d\n", <-reply) // expect 0
}

// =============================================================================
// PART 3 — SUPERVISED ACTOR (Crash and restart)
// =============================================================================
//
// Philosophy: "Let it crash."
// Don't write defensive code inside the actor.
// Let the supervisor handle restarts.

type BombMessage struct{} // this will cause a panic
type NormalMessage struct{ Text string }

type FragileActor struct {
	received int
}

func (a *FragileActor) Receive(ctx *Context, msg Message) {
	switch m := msg.(type) {
	case NormalMessage:
		a.received++
		fmt.Printf("[fragile] received: %q (count: %d)\n", m.Text, a.received)
	case BombMessage:
		// This crashes the actor!
		// With supervision, it will restart automatically.
		panic("BOOM! fragile actor crashed")
	}
}

func runSupervisionExample(sys *System) {
	fmt.Println("\n--- Part 3: Supervision ---")

	// Factory function creates a fresh actor on each restart
	fragile := sys.SpawnSupervised("fragile", func() Actor {
		return &FragileActor{}
	}, 20)

	fragile.Send(NormalMessage{Text: "hello"})
	fragile.Send(NormalMessage{Text: "world"})
	fragile.Send(BombMessage{})                    // CRASH
	time.Sleep(200 * time.Millisecond)             // wait for restart
	fragile.Send(NormalMessage{Text: "I'm back!"}) // actor restarted
	time.Sleep(50 * time.Millisecond)
}

// =============================================================================
// PART 4 — MINI TRADE SYSTEM (Multiple actors collaborating)
// =============================================================================
//
// Three actors:
//   MarketActor  — generates fake price ticks
//   StrategyActor — decides when to buy or sell
//   LoggerActor  — records everything that happens
//
// Data flow:
//   Market --PriceTick--> Strategy --TradeSignal--> Logger
//                                                ↑
//                         Strategy --PriceTick---+

// Trade messages
type (
	PriceTick struct {
		Symbol string
		Price  float64
	}
	TradeSignal struct {
		Symbol string
		Side   string // "BUY" or "SELL"
		Price  float64
		Reason string
	}
	LogEntry struct {
		Level   string
		Message string
	}
	StopActor struct{} // tells an actor to shut down
)

// LoggerActor just prints everything it receives.
// In a real system this would write to a database or file.
type LoggerActor struct{}

func (a *LoggerActor) Receive(ctx *Context, msg Message) {
	switch m := msg.(type) {
	case LogEntry:
		fmt.Printf("[%s] %s\n", m.Level, m.Message)
	case TradeSignal:
		fmt.Printf("[TRADE] %s %s @ %.2f — %s\n", m.Side, m.Symbol, m.Price, m.Reason)
	}
}

// StrategyActor watches prices and emits trade signals.
// Its private state: the last known price.
// It uses a simple strategy: buy when price drops 1%, sell when it rises 1%.
type StrategyActor struct {
	logger    *ActorRef
	lastPrice float64
}

func (a *StrategyActor) Receive(ctx *Context, msg Message) {
	switch m := msg.(type) {

	case PriceTick:
		// Log the tick
		a.logger.Send(LogEntry{
			Level:   "INFO",
			Message: fmt.Sprintf("Price tick: %s @ %.2f", m.Symbol, m.Price),
		})

		// First tick — just record the price
		if a.lastPrice == 0 {
			a.lastPrice = m.Price
			return
		}

		// Calculate change
		change := (m.Price - a.lastPrice) / a.lastPrice * 100

		// Apply strategy rules
		switch {
		case change <= -1.0:
			// Price dropped 1% — buy signal
			a.logger.Send(TradeSignal{
				Symbol: m.Symbol,
				Side:   "BUY",
				Price:  m.Price,
				Reason: fmt.Sprintf("price dropped %.2f%%", change),
			})
		case change >= 1.0:
			// Price rose 1% — sell signal
			a.logger.Send(TradeSignal{
				Symbol: m.Symbol,
				Side:   "SELL",
				Price:  m.Price,
				Reason: fmt.Sprintf("price rose +%.2f%%", change),
			})
		}

		a.lastPrice = m.Price
	}
}

// MarketActor simulates price ticks.
// In a real system this connects to an exchange WebSocket.
type MarketActor struct {
	strategy *ActorRef
}

func (a *MarketActor) Receive(ctx *Context, msg Message) {
	switch m := msg.(type) {
	case PriceTick:
		// Forward market data to the strategy
		a.strategy.Send(m)
	}
}

func runTradeExample(sys *System) {
	fmt.Println("\n--- Part 4: Mini Trade System ---")

	logger := sys.Spawn("logger", &LoggerActor{}, 50)
	strategy := sys.Spawn("strategy", &StrategyActor{logger: logger}, 50)
	market := sys.Spawn("market", &MarketActor{strategy: strategy}, 50)

	// Simulate price ticks
	prices := []float64{100.0, 99.5, 98.8, 99.9, 101.2, 100.5, 99.0}
	for _, price := range prices {
		market.Send(PriceTick{Symbol: "BTC/USD", Price: price})
		time.Sleep(50 * time.Millisecond) // simulate time between ticks
	}

	time.Sleep(100 * time.Millisecond) // let all messages flush
	_ = logger
	_ = strategy
	_ = market
}

// =============================================================================
// MAIN
// =============================================================================

func main() {
	// Silence the default logger's timestamp for cleaner output
	log.SetFlags(0)

	sys := NewSystem()

	runCounterExample(sys)
	time.Sleep(50 * time.Millisecond)

	runSupervisionExample(sys)
	time.Sleep(50 * time.Millisecond)

	runTradeExample(sys)

	fmt.Println("\nDone.")
}
