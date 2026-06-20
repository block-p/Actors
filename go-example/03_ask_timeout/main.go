// Package main demonstrates the Ask pattern in actor systems.
//
// The Ask pattern is how you do request/response between actors.
// It looks deceptively simple, but there are several subtle traps:
//   - You MUST use a buffered reply channel (size 1) or you'll leak goroutines.
//   - You MUST add a timeout or a crashed actor will block you forever.
//   - You MUST handle the timeout case gracefully (don't panic, return an error).
//
// Run this program:
//
//	cd go-example
//	go run ./03_ask_timeout/main.go
package main

import (
	"fmt"
	"time"
)

// ═══════════════════════════════════════════════════════════════════════════════
// MESSAGES
// ═══════════════════════════════════════════════════════════════════════════════

// Increment is a Command — fire and forget, no reply expected.
type Increment struct{ Amount int }

// Reset is a Command — fire and forget.
type Reset struct{}

// GetCount is a Query — it carries a buffered reply channel.
// The actor will send the current count into Reply.
// The Reply channel MUST be buffered (size 1) — explained at length below.
type GetCount struct {
	Reply chan int
}

// StopActor is the Poison Pill.
// The Done channel lets the sender wait for full shutdown.
type StopActor struct {
	Done chan struct{}
}

// ═══════════════════════════════════════════════════════════════════════════════
// COUNTER ACTOR — Normal (fast, always responds)
// ═══════════════════════════════════════════════════════════════════════════════

// newCounterActor creates a counter actor that responds to all queries promptly.
// Returns the mailbox channel — that IS the actor's address.
func newCounterActor(name string) chan any {
	mailbox := make(chan any, 100)

	go func() {
		count := 0 // private state — nobody outside this goroutine can touch it

		fmt.Printf("[%s] started\n", name)

		for msg := range mailbox {
			switch m := msg.(type) {

			case Increment:
				// Command: update state, no reply
				count += m.Amount

			case Reset:
				// Command: update state, no reply
				count = 0
				fmt.Printf("[%s] reset to 0\n", name)

			case GetCount:
				// Query: read state, reply via the channel in the message.
				//
				// WHY we can always send here without blocking:
				// The Reply channel has a buffer of 1. Even if the caller has already
				// timed out and moved on, this send completes immediately into the
				// buffer. The channel will be garbage collected when no references
				// remain. Without the buffer, this send would block forever if the
				// caller had timed out — leaking THIS goroutine.
				m.Reply <- count

			case StopActor:
				fmt.Printf("[%s] stopping\n", name)
				close(m.Done)
				return
			}
		}
	}()

	return mailbox
}

// ═══════════════════════════════════════════════════════════════════════════════
// SLOW COUNTER ACTOR — Deliberately slow to demonstrate timeouts
// ═══════════════════════════════════════════════════════════════════════════════

// newSlowCounterActor creates a counter that simulates a slow actor.
// It sleeps 500ms before responding to GetCount queries.
// This demonstrates what happens when the caller has a tight timeout.
func newSlowCounterActor(name string, delay time.Duration) chan any {
	mailbox := make(chan any, 100)

	go func() {
		count := 0

		fmt.Printf("[%s] started (responds after %s delay)\n", name, delay)

		for msg := range mailbox {
			switch m := msg.(type) {

			case Increment:
				count += m.Amount

			case GetCount:
				// Simulate slow processing (DB query, complex calculation, etc.)
				// In a real actor you would NEVER sleep inside the receive loop —
				// you'd offload to a goroutine. But here we're deliberately showing
				// what a SLOW actor looks like so callers can time out.
				fmt.Printf("[%s] processing GetCount (slow: %s)...\n", name, delay)
				time.Sleep(delay)

				// By the time we send this, the caller may have already timed out.
				// With a buffered Reply channel, this send still completes safely.
				// With an unbuffered channel, this would BLOCK FOREVER if the caller
				// is gone — permanently leaking this actor's goroutine.
				m.Reply <- count
				fmt.Printf("[%s] GetCount reply sent (caller may have timed out)\n", name)

			case StopActor:
				fmt.Printf("[%s] stopping\n", name)
				close(m.Done)
				return
			}
		}
	}()

	return mailbox
}

// ═══════════════════════════════════════════════════════════════════════════════
// ASK HELPERS
// ═══════════════════════════════════════════════════════════════════════════════

// ask sends a GetCount query to an actor and waits for the reply with a timeout.
//
// This is the canonical Ask pattern. Study it closely:
//
//  1. Create a BUFFERED reply channel (size 1). Not size 0. Not size 2. Always 1.
//     - Size 0 (unbuffered): if caller times out, actor blocks forever → goroutine leak.
//     - Size 2+: wastes memory; one reply is all we ever get.
//     - Size 1: actor can always send exactly one reply, whether or not caller is waiting.
//
//  2. Send the query WITH the reply channel embedded in it.
//     The actor knows where to send its answer.
//
//  3. Use `select` with `time.After` to impose a deadline.
//     NEVER do a bare `<-reply` without a timeout in a real system.
func ask(actor chan any, timeout time.Duration) (int, error) {
	// Step 1: BUFFERED reply channel — this is non-negotiable.
	reply := make(chan int, 1)

	// Step 2: Send the query. This is fire-and-forget from our perspective
	// (we don't block waiting for acknowledgement that the actor received it).
	// The actor will send its response to `reply` when it processes this message.
	actor <- GetCount{Reply: reply}

	// Step 3: Wait for the reply OR the timeout — never both, never neither.
	select {
	case result := <-reply:
		// Happy path: actor responded in time.
		return result, nil

	case <-time.After(timeout):
		// Timeout path: actor did NOT respond within our deadline.
		//
		// What happens to the actor?
		//   - The actor is still running normally. It will eventually process
		//     our GetCount message and send to reply.
		//   - Because reply is buffered (size 1), that send will NOT block.
		//     The goroutine in the actor proceeds normally.
		//   - The orphaned `reply` channel will be garbage collected when nothing
		//     holds a reference to it.
		//
		// What we return:
		//   - An error. The caller can decide to retry, use a cached value, etc.
		//   - We do NOT panic. We do NOT crash. We handle it gracefully.
		return 0, fmt.Errorf("ask timed out after %s", timeout)
	}
}

// askWithRetry wraps ask with retry logic for transient failures.
// Each attempt uses a fresh reply channel so there's no ambiguity about
// which reply belongs to which attempt.
func askWithRetry(actor chan any, timeout time.Duration, maxRetries int) (int, error) {
	var lastErr error

	for attempt := 1; attempt <= maxRetries; attempt++ {
		result, err := ask(actor, timeout)
		if err == nil {
			if attempt > 1 {
				fmt.Printf("  [retry] succeeded on attempt %d/%d\n", attempt, maxRetries)
			}
			return result, nil
		}

		lastErr = err
		fmt.Printf("  [retry] attempt %d/%d failed: %v\n", attempt, maxRetries, err)

		// Small backoff between retries (not exponential here, but you should use it
		// in a real system: time.Sleep(time.Duration(attempt) * 100 * time.Millisecond))
		if attempt < maxRetries {
			time.Sleep(50 * time.Millisecond)
		}
	}

	return 0, fmt.Errorf("all %d attempts failed; last error: %w", maxRetries, lastErr)
}

// ═══════════════════════════════════════════════════════════════════════════════
// DEMONSTRATION: WHY UNBUFFERED CHANNELS LEAK GOROUTINES
// ═══════════════════════════════════════════════════════════════════════════════

// demonstrateBufferImportance shows concretely why the reply channel must be
// buffered. We create a slow actor, time out waiting for it, and then show that
// the actor is still alive and functioning (it was not leaked).
//
// With an unbuffered channel, after the caller times out, the actor goroutine
// would block on `reply <- count` forever because nobody is reading. It would
// never process another message. It would be a zombie.
func demonstrateBufferImportance() {
	fmt.Println("\n── Demo: Buffered Channel Prevents Goroutine Leak ─────────────────────")

	// Slow actor: 300ms delay per GetCount response
	slow := newSlowCounterActor("slow-counter", 300*time.Millisecond)

	// Add some state
	slow <- Increment{Amount: 42}

	// Ask with a TIGHT timeout (100ms < 300ms delay)
	// This WILL time out.
	fmt.Println("Asking with 100ms timeout (actor needs 300ms)...")
	result, err := ask(slow, 100*time.Millisecond)
	if err != nil {
		fmt.Printf("  Got expected timeout error: %v\n", err)
	} else {
		fmt.Printf("  Unexpected success: %d\n", result)
	}

	// Wait for the actor to finish its slow response and return to idle
	// (it took 300ms total, we timed out at 100ms, so wait ~250ms more)
	time.Sleep(250 * time.Millisecond)

	// NOW ask again with a generous timeout.
	// If the buffered channel did NOT save the actor, it would be frozen
	// trying to send to the abandoned reply channel, and this ask would time out too.
	// A healthy actor responds here — proof that buffering prevented a zombie.
	fmt.Println("Asking again with 500ms timeout (actor should be healthy)...")
	result, err = ask(slow, 500*time.Millisecond)
	if err != nil {
		fmt.Printf("  ERROR: actor appears stuck (goroutine leak!): %v\n", err)
	} else {
		fmt.Printf("  Actor is healthy! Count = %d (buffered channel saved it)\n", result)
	}

	// Graceful shutdown
	done := make(chan struct{})
	slow <- StopActor{Done: done}
	<-done
}

// ═══════════════════════════════════════════════════════════════════════════════
// MAIN — Walk through all Ask patterns
// ═══════════════════════════════════════════════════════════════════════════════

func main() {
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println(" Actor Model: The Ask Pattern with Timeout")
	fmt.Println("═══════════════════════════════════════════════════════════════")

	// ── Part 1: Basic Ask ────────────────────────────────────────────────────
	fmt.Println("\n── Part 1: Basic Ask (Request/Response) ────────────────────────")

	counter := newCounterActor("counter-1")

	// Fire-and-forget commands (no reply needed)
	counter <- Increment{Amount: 10}
	counter <- Increment{Amount: 5}
	counter <- Increment{Amount: 3}

	// Ask: we NEED the value before we can continue
	result, err := ask(counter, 2*time.Second)
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		fmt.Printf("  Count after 10+5+3 = %d\n", result)
	}

	// ── Part 2: Ask with Timeout (actor is healthy, responds fast) ───────────
	fmt.Println("\n── Part 2: Ask with Generous Timeout (should succeed) ──────────")

	// This is the same call, but we're being explicit about the timeout value.
	// 2 seconds is generous for an in-memory operation.
	result, err = ask(counter, 2*time.Second)
	if err != nil {
		fmt.Printf("  Unexpected timeout: %v\n", err)
	} else {
		fmt.Printf("  Got count: %d (as expected)\n", result)
	}

	// ── Part 3: Ask that Times Out ───────────────────────────────────────────
	fmt.Println("\n── Part 3: Ask that TIMES OUT (actor is too slow) ──────────────")

	// Create a slow actor (250ms delay)
	slow := newSlowCounterActor("counter-slow", 250*time.Millisecond)
	slow <- Increment{Amount: 99}

	// Ask with a tight timeout (50ms)
	fmt.Println("  Asking slow actor with 50ms timeout...")
	result, err = ask(slow, 50*time.Millisecond)
	if err != nil {
		fmt.Printf("  Correctly handled timeout: %v\n", err)
		fmt.Println("  (actor is still running, just slow — we moved on without blocking)")
	} else {
		fmt.Printf("  Got %d (this is a surprise — actor was faster than expected)\n", result)
	}

	// Give the slow actor time to finish its in-progress work
	time.Sleep(300 * time.Millisecond)

	// ── Part 4: Retry Logic on Timeout ──────────────────────────────────────
	fmt.Println("\n── Part 4: Retry Logic on Timeout ──────────────────────────────")

	// The slow actor (250ms delay) with a 50ms timeout per attempt and 2 retries.
	// ALL attempts will time out because 50ms < 250ms always.
	fmt.Println("  Retrying slow actor (50ms timeout, max 2 retries) — all will fail:")
	_, err = askWithRetry(slow, 50*time.Millisecond, 2)
	if err != nil {
		fmt.Printf("  Final error after retries: %v\n", err)
	}

	// Wait for the slow actor's in-flight processing to settle
	time.Sleep(300 * time.Millisecond)

	// Now try with a timeout that WILL succeed (350ms > 250ms delay)
	fmt.Println("  Retrying slow actor (350ms timeout, max 3 retries) — will succeed:")
	result, err = askWithRetry(slow, 350*time.Millisecond, 3)
	if err != nil {
		fmt.Printf("  Error: %v\n", err)
	} else {
		fmt.Printf("  Result: %d\n", result)
	}

	// ── Part 5: Buffer Importance Demonstration ──────────────────────────────
	demonstrateBufferImportance()

	// ── Part 6: The `select` Pattern Explained ───────────────────────────────
	fmt.Println("\n── Part 6: Manual select Pattern (inline, for clarity) ─────────")
	fmt.Println("  This is exactly what `ask()` does inside, spelled out manually:")

	counter <- Increment{Amount: 100}

	reply := make(chan int, 1) // BUFFERED — the only correct choice
	counter <- GetCount{Reply: reply}

	select {
	case count := <-reply:
		// Actor responded before our deadline
		fmt.Printf("  Got reply: %d\n", count)

	case <-time.After(2 * time.Second):
		// Actor did not respond in time.
		// Because reply is buffered, the actor can still send its reply later
		// and NOT block. Our goroutine is free; the actor is free.
		fmt.Println("  Timeout — actor did not respond. Moving on.")
	}

	// ── Cleanup: graceful shutdown ───────────────────────────────────────────
	fmt.Println("\n── Cleanup: graceful shutdown ───────────────────────────────────")

	d1 := make(chan struct{})
	d2 := make(chan struct{})
	counter <- StopActor{Done: d1}
	slow <- StopActor{Done: d2}

	// Wait for both to confirm they've fully stopped
	// We do this in order (not parallel) just for deterministic output here.
	// In a real shutdown you'd often wait in parallel.
	<-d1
	<-d2

	fmt.Println("\nAll actors stopped. Program complete.")
	fmt.Println()
	fmt.Println("Key takeaways:")
	fmt.Println("  1. Reply channels MUST be buffered (size 1) — prevents goroutine leaks")
	fmt.Println("  2. ALWAYS add a timeout to Ask — never block indefinitely")
	fmt.Println("  3. On timeout: return an error, do NOT panic or crash")
	fmt.Println("  4. The actor keeps running after your timeout — it is not killed")
	fmt.Println("  5. Retry by creating a fresh reply channel per attempt")
}
