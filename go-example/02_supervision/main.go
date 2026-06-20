// Supervision in the Actor Model — Go from Scratch
//
// This file shows the full supervision system:
//   Demo 1 — OneForOne: only the crashed actor restarts
//   Demo 2 — OneForAll: all actors restart when one crashes
//   Demo 3 — Max restarts exceeded: supervisor gives up, alerts
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
// CORE TYPES
// =============================================================================

type Message any

// ActorRef is the actor's address — the only way to talk to it.
type ActorRef struct {
	mailbox chan Message
	name    string
}

func (r *ActorRef) Send(msg Message) {
	r.mailbox <- msg
}

// Actor is the behavior interface every actor implements.
type Actor interface {
	Receive(self *ActorRef, msg Message)
}

// =============================================================================
// SUPERVISION STRATEGY
// =============================================================================

// Strategy defines what the supervisor does when a child crashes.
type Strategy int

const (
	// OneForOne: only restart the actor that crashed.
	// Use when children are INDEPENDENT of each other.
	// Example: exchange connectors (Binance crashing doesn't affect Coinbase).
	OneForOne Strategy = iota

	// OneForAll: restart ALL children when any one crashes.
	// Use when children share state or form a unit that must be consistent.
	// Example: Strategy + Risk + Executor actors form a trading unit.
	// If one gets into a bad state, the whole unit needs to reset.
	OneForAll
)

// =============================================================================
// CRASH REPORT
// sent from child-watcher goroutine to supervisor's mailbox
// =============================================================================

type childCrashed struct {
	name string
	err  interface{} // whatever recover() caught
	ref  *ActorRef
}

// =============================================================================
// CHILD SPEC — blueprint for spawning (and re-spawning) an actor
// =============================================================================

type ChildSpec struct {
	Name        string
	Factory     func() Actor // called fresh on every restart
	MailboxSize int
}

// =============================================================================
// SUPERVISOR
// =============================================================================

type Supervisor struct {
	strategy    Strategy
	maxRestarts int           // max allowed restarts per child
	backoff     time.Duration // wait between restarts (first restart)

	mu           sync.RWMutex
	children     map[string]*ActorRef
	restartCount map[string]int
	specs        map[string]ChildSpec
	allSpecs     []ChildSpec // ordered, needed for OneForAll

	crashCh chan childCrashed // crash reports arrive here
	stopped chan struct{}
}

func NewSupervisor(strategy Strategy, maxRestarts int, backoff time.Duration) *Supervisor {
	return &Supervisor{
		strategy:     strategy,
		maxRestarts:  maxRestarts,
		backoff:      backoff,
		children:     make(map[string]*ActorRef),
		restartCount: make(map[string]int),
		specs:        make(map[string]ChildSpec),
		crashCh:      make(chan childCrashed, 100),
		stopped:      make(chan struct{}),
	}
}

// AddChild registers a child spec and spawns the actor immediately.
func (s *Supervisor) AddChild(spec ChildSpec) *ActorRef {
	ref := s.spawnChild(spec)
	s.allSpecs = append(s.allSpecs, spec)
	return ref
}

// spawnChild creates the actor goroutine and sets up a watcher.
func (s *Supervisor) spawnChild(spec ChildSpec) *ActorRef {
	ref := &ActorRef{
		mailbox: make(chan Message, spec.MailboxSize),
		name:    spec.Name,
	}

	s.mu.Lock()
	s.children[spec.Name] = ref
	s.specs[spec.Name] = spec
	s.mu.Unlock()

	s.watchChild(spec, ref)
	return ref
}

// watchChild runs the actor in a goroutine and reports crashes to the supervisor.
func (s *Supervisor) watchChild(spec ChildSpec, ref *ActorRef) {
	a := spec.Factory() // fresh actor instance

	go func() {
		defer func() {
			if r := recover(); r != nil {
				// Actor crashed — report to supervisor
				s.crashCh <- childCrashed{name: spec.Name, err: r, ref: ref}
			}
		}()
		for msg := range ref.mailbox {
			a.Receive(ref, msg)
		}
	}()
}

// Run starts the supervision loop. Call in a goroutine.
func (s *Supervisor) Run() {
	for {
		select {
		case crash := <-s.crashCh:
			s.handleCrash(crash)

		case <-s.stopped:
			return
		}
	}
}

func (s *Supervisor) handleCrash(crash childCrashed) {
	s.mu.Lock()
	s.restartCount[crash.name]++
	count := s.restartCount[crash.name]
	s.mu.Unlock()

	log.Printf("[supervisor] %q crashed (err: %v) — restart #%d", crash.name, crash.err, count)

	if count > s.maxRestarts {
		// Give up on this actor.
		log.Printf("[supervisor] CRITICAL: %q exceeded max restarts (%d). Giving up. Manual intervention needed.", crash.name, s.maxRestarts)
		// In a real system: page on-call, switch to safe mode, etc.
		return
	}

	// Backoff before restarting. Exponential: 100ms, 200ms, 400ms...
	wait := s.backoff * time.Duration(1<<uint(count-1))
	log.Printf("[supervisor] waiting %v before restarting %q", wait, crash.name)
	time.Sleep(wait)

	switch s.strategy {
	case OneForOne:
		s.restartOne(crash.name)

	case OneForAll:
		s.restartAll()
	}
}

// restartOne restarts only the crashed child (OneForOne strategy).
func (s *Supervisor) restartOne(name string) {
	s.mu.RLock()
	spec := s.specs[name]
	oldRef := s.children[name]
	s.mu.RUnlock()

	log.Printf("[supervisor] OneForOne — restarting only %q", name)

	// Create a NEW ActorRef (new mailbox) for the restarted actor.
	// IMPORTANT: the old mailbox still has any pending messages.
	// We keep the same channel so callers don't need to update their refs.
	// We just run a new actor goroutine reading from the same mailbox.
	_ = oldRef
	s.watchChild(spec, oldRef)
}

// restartAll restarts every child (OneForAll strategy).
func (s *Supervisor) restartAll() {
	log.Printf("[supervisor] OneForAll — restarting ALL children")

	s.mu.RLock()
	specs := make([]ChildSpec, len(s.allSpecs))
	copy(specs, s.allSpecs)
	refs := make(map[string]*ActorRef, len(s.children))
	for k, v := range s.children {
		refs[k] = v
	}
	s.mu.RUnlock()

	for _, spec := range specs {
		ref := refs[spec.Name]
		log.Printf("[supervisor] restarting child: %q", spec.Name)
		s.watchChild(spec, ref)
	}
}

// Get returns the ActorRef for a named child.
func (s *Supervisor) Get(name string) *ActorRef {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.children[name]
}

func (s *Supervisor) Stop() {
	close(s.stopped)
}

// =============================================================================
// WORKER ACTOR — used in all demos
// =============================================================================

type NormalWork struct {
	ID      int
	Payload string
}
type BombMessage struct{} // causes a panic — used to test supervision
type GetProcessed struct{ Reply chan int }

type WorkerActor struct {
	id        int
	processed int
}

func NewWorker(id int) func() Actor {
	return func() Actor {
		return &WorkerActor{id: id}
	}
}

func (a *WorkerActor) Receive(self *ActorRef, msg Message) {
	switch m := msg.(type) {
	case NormalWork:
		a.processed++
		fmt.Printf("  [Worker-%d] processed job #%d: %q (total: %d)\n", a.id, m.ID, m.Payload, a.processed)

	case BombMessage:
		// This simulates a bug — panics the actor
		panic(fmt.Sprintf("Worker-%d exploded!", a.id))

	case GetProcessed:
		m.Reply <- a.processed

	default:
		log.Printf("[Worker-%d] unknown message: %T", a.id, msg)
	}
}

// =============================================================================
// DEMO 1 — OneForOne
// Only the crashed actor restarts. Others keep running unaffected.
// =============================================================================

func demoOneForOne() {
	fmt.Println("\n╔══════════════════════════════════════════╗")
	fmt.Println("║  Demo 1: OneForOne Supervision           ║")
	fmt.Println("║  Only crashed actor restarts.            ║")
	fmt.Println("╚══════════════════════════════════════════╝")

	sup := NewSupervisor(OneForOne, 5, 100*time.Millisecond)
	go sup.Run()

	w1 := sup.AddChild(ChildSpec{Name: "worker-1", Factory: NewWorker(1), MailboxSize: 20})
	w2 := sup.AddChild(ChildSpec{Name: "worker-2", Factory: NewWorker(2), MailboxSize: 20})
	w3 := sup.AddChild(ChildSpec{Name: "worker-3", Factory: NewWorker(3), MailboxSize: 20})
	time.Sleep(20 * time.Millisecond)

	fmt.Println("\n→ Sending work to all 3 workers...")
	w1.Send(NormalWork{ID: 1, Payload: "order-A"})
	w2.Send(NormalWork{ID: 2, Payload: "order-B"})
	w3.Send(NormalWork{ID: 3, Payload: "order-C"})
	time.Sleep(50 * time.Millisecond)

	fmt.Println("\n→ CRASHING Worker-2 with a bomb...")
	w2.Send(BombMessage{})
	time.Sleep(300 * time.Millisecond) // wait for crash + restart

	fmt.Println("\n→ Sending more work. Worker-1 and Worker-3 never stopped.")
	fmt.Println("  Worker-2 was restarted and is fresh (processed count reset to 0).")
	w1.Send(NormalWork{ID: 4, Payload: "order-D"})
	w2.Send(NormalWork{ID: 5, Payload: "order-E"}) // goes to the restarted Worker-2
	w3.Send(NormalWork{ID: 6, Payload: "order-F"})
	time.Sleep(50 * time.Millisecond)

	sup.Stop()
}

// =============================================================================
// DEMO 2 — OneForAll
// Every actor restarts when one crashes.
// =============================================================================

func demoOneForAll() {
	fmt.Println("\n╔══════════════════════════════════════════╗")
	fmt.Println("║  Demo 2: OneForAll Supervision           ║")
	fmt.Println("║  ALL actors restart when one crashes.    ║")
	fmt.Println("╚══════════════════════════════════════════╝")

	sup := NewSupervisor(OneForAll, 5, 100*time.Millisecond)
	go sup.Run()

	w1 := sup.AddChild(ChildSpec{Name: "strategy", Factory: NewWorker(1), MailboxSize: 20})
	w2 := sup.AddChild(ChildSpec{Name: "risk", Factory: NewWorker(2), MailboxSize: 20})
	w3 := sup.AddChild(ChildSpec{Name: "executor", Factory: NewWorker(3), MailboxSize: 20})
	time.Sleep(20 * time.Millisecond)

	fmt.Println("\n→ Strategy + Risk + Executor all running. Sending work...")
	w1.Send(NormalWork{ID: 1, Payload: "signal-buy"})
	w2.Send(NormalWork{ID: 2, Payload: "risk-check"})
	w3.Send(NormalWork{ID: 3, Payload: "execute"})
	time.Sleep(50 * time.Millisecond)

	fmt.Println("\n→ CRASHING the risk actor...")
	fmt.Println("  With OneForAll, this restarts strategy, risk, AND executor.")
	fmt.Println("  (They form a unit — partial restart would leave inconsistent state.)")
	w2.Send(BombMessage{})
	time.Sleep(400 * time.Millisecond) // wait for all to restart

	fmt.Println("\n→ All three restarted fresh. Sending work again...")
	w1.Send(NormalWork{ID: 4, Payload: "signal-sell"})
	w2.Send(NormalWork{ID: 5, Payload: "risk-check"})
	w3.Send(NormalWork{ID: 6, Payload: "execute"})
	time.Sleep(50 * time.Millisecond)

	sup.Stop()
}

// =============================================================================
// DEMO 3 — Max Restarts Exceeded
// Supervisor gives up after too many crashes in a row.
// =============================================================================

func demoMaxRestarts() {
	fmt.Println("\n╔══════════════════════════════════════════╗")
	fmt.Println("║  Demo 3: Max Restarts Exceeded           ║")
	fmt.Println("║  Supervisor gives up, raises CRITICAL.   ║")
	fmt.Println("╚══════════════════════════════════════════╝")

	// maxRestarts=3 — after 3 crashes this actor is abandoned
	sup := NewSupervisor(OneForOne, 3, 50*time.Millisecond)
	go sup.Run()

	broken := sup.AddChild(ChildSpec{Name: "broken-connector", Factory: NewWorker(99), MailboxSize: 20})
	time.Sleep(20 * time.Millisecond)

	fmt.Println("\n→ Sending 5 bombs in a row. Supervisor will restart 3 times then give up.")
	for i := 1; i <= 5; i++ {
		time.Sleep(300 * time.Millisecond) // give time for restart + backoff
		fmt.Printf("\n→ Sending bomb #%d...\n", i)
		broken.Send(BombMessage{})
	}

	// Wait for all the backoffs and restarts to play out
	time.Sleep(1 * time.Second)
	fmt.Println("\n→ After max restarts exceeded, the CRITICAL alert fires.")
	fmt.Println("  In production: page on-call, stop accepting new trades, etc.")

	sup.Stop()
}

// =============================================================================
// MAIN
// =============================================================================

func main() {
	log.SetFlags(0) // no timestamps in log output, cleaner for demo

	demoOneForOne()
	time.Sleep(100 * time.Millisecond)

	demoOneForAll()
	time.Sleep(100 * time.Millisecond)

	demoMaxRestarts()

	fmt.Println("\n\nDone.")
}
