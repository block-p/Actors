// 06_fsm — Finite State Machine Actor Pattern
//
// A TradeOrderActor models a real order's lifecycle as an explicit FSM.
//
// WHY FSM ACTORS ARE GREAT FOR TRADE ORDERS
// -------------------------------------------
// Without a state machine you might accidentally call "cancel" on an order
// that was never submitted, or try to fill an order that was already cancelled.
// In production, such bugs cause real financial losses.
//
// An FSM actor makes illegal transitions IMPOSSIBLE at the code level:
//   • The state is private — only the actor can change it.
//   • Each message handler checks the current state first.
//   • Invalid transitions are logged loudly, never silently ignored.
//
// State diagram:
//
//   New ──SubmitOrder──► Submitted ──OrderAcknowledged──► PartiallyFilled ──FullFill──► FullyFilled (terminal)
//                           │                                   │
//                           ├──OrderRejected──► Rejected        └──CancelOrder──► Cancelled (terminal)
//                           │    (terminal)
//                           └──CancelOrder──► Cancelled (terminal)

package main

import (
	"fmt"
	"sync"
	"time"
)

// ---------------------------------------------------------------------------
// FSM States
// ---------------------------------------------------------------------------

// OrderState is the type-safe state enum.
type OrderState int

const (
	StateNew             OrderState = iota // order created, not yet sent
	StateSubmitted                         // sent to exchange, waiting for ack
	StatePartiallyFilled                   // some quantity filled
	StateFullyFilled                       // completely filled — terminal
	StateCancelled                         // cancelled — terminal
	StateRejected                          // rejected by exchange — terminal
)

func (s OrderState) String() string {
	switch s {
	case StateNew:
		return "New"
	case StateSubmitted:
		return "Submitted"
	case StatePartiallyFilled:
		return "PartiallyFilled"
	case StateFullyFilled:
		return "FullyFilled"
	case StateCancelled:
		return "Cancelled"
	case StateRejected:
		return "Rejected"
	}
	return "Unknown"
}

// isTerminal returns true if no further transitions are possible.
func (s OrderState) isTerminal() bool {
	return s == StateFullyFilled || s == StateCancelled || s == StateRejected
}

// ---------------------------------------------------------------------------
// Messages (each message triggers one state transition)
// ---------------------------------------------------------------------------

// SubmitOrder transitions New → Submitted.
type SubmitOrder struct {
	OrderID string
	Symbol  string
	Side    string  // "BUY" or "SELL"
	Qty     float64 // total quantity
	Price   float64 // limit price
}

// OrderAcknowledged transitions Submitted → PartiallyFilled (qty still 0).
// In real life the exchange sends this; here the test harness sends it.
type OrderAcknowledged struct {
	ExchangeOrderID string
}

// OrderRejected transitions Submitted → Rejected.
type OrderRejected struct {
	Reason string
}

// PartialFill transitions PartiallyFilled → PartiallyFilled (more fill needed).
type PartialFill struct {
	FilledQty   float64
	FilledPrice float64
}

// FullFill transitions PartiallyFilled → FullyFilled.
type FullFill struct {
	FilledQty   float64
	FilledPrice float64
}

// CancelOrder transitions Submitted|PartiallyFilled → Cancelled.
type CancelOrder struct{}

// OrderCancelled is a notification sent back to the caller after cancel.
type OrderCancelled struct{}

// GetOrderStatus is a query — returns the current state without changing it.
type GetOrderStatus struct {
	Reply chan OrderStatus
}

// OrderStatus is the immutable snapshot returned to callers.
type OrderStatus struct {
	OrderID         string
	State           OrderState
	FilledQty       float64
	RemainingQty    float64
	AvgFilledPrice  float64
	ExchangeOrderID string
}

// ---------------------------------------------------------------------------
// Internal mailbox union type
// ---------------------------------------------------------------------------

// orderCmd wraps every possible message in one struct so the mailbox can be
// typed as chan orderCmd.  Non-nil fields indicate which variant is active.
type orderCmd struct {
	submit  *SubmitOrder
	ack     *OrderAcknowledged
	reject  *OrderRejected
	partial *PartialFill
	full    *FullFill
	cancel  *CancelOrder
	status  *GetOrderStatus
}

// ---------------------------------------------------------------------------
// TradeOrderActor
// ---------------------------------------------------------------------------

// TradeOrderActor is the FSM actor.  All mutable state lives ONLY inside this
// struct and is never exposed outside — this is the key safety guarantee.
type TradeOrderActor struct {
	// identity
	name string

	// FSM state — the heart of the pattern
	state OrderState

	// order data
	orderID         string
	symbol          string
	side            string
	totalQty        float64
	price           float64
	filledQty       float64
	totalCost       float64 // sum(filledQty * filledPrice) for avg calculation
	exchangeOrderID string

	// actor plumbing
	mailbox chan orderCmd
	wg      *sync.WaitGroup // decremented when the actor exits
}

// newTradeOrderActor creates an actor in StateNew and starts its goroutine.
func newTradeOrderActor(name string, wg *sync.WaitGroup) *TradeOrderActor {
	a := &TradeOrderActor{
		name:    name,
		state:   StateNew,
		mailbox: make(chan orderCmd, 32),
		wg:      wg,
	}
	wg.Add(1)
	go a.run()
	return a
}

// ---------------------------------------------------------------------------
// Event loop
// ---------------------------------------------------------------------------

func (a *TradeOrderActor) run() {
	defer a.wg.Done()
	// The actor runs until its mailbox is closed via Close().
	// Even in terminal states it stays alive to answer status queries;
	// this avoids a race where main calls Status() just after the last
	// transition fires.
	for cmd := range a.mailbox {
		a.handle(cmd)
	}
}

// handle dispatches one message to the correct transition handler.
func (a *TradeOrderActor) handle(cmd orderCmd) {
	// In a terminal state, only status queries are meaningful.
	// All other messages are rejected with a loud warning.
	if a.state.isTerminal() {
		if cmd.status != nil {
			a.onStatus(cmd.status)
		} else {
			fmt.Printf("  [%s] ⚠ INVALID: message received in terminal state %s — ignored\n",
				a.name, a.state)
		}
		return
	}
	switch {
	case cmd.submit != nil:
		a.onSubmit(cmd.submit)
	case cmd.ack != nil:
		a.onAck(cmd.ack)
	case cmd.reject != nil:
		a.onReject(cmd.reject)
	case cmd.partial != nil:
		a.onPartialFill(cmd.partial)
	case cmd.full != nil:
		a.onFullFill(cmd.full)
	case cmd.cancel != nil:
		a.onCancel()
	case cmd.status != nil:
		a.onStatus(cmd.status)
	}
}

// ---------------------------------------------------------------------------
// Transition handlers — each checks its precondition first
// ---------------------------------------------------------------------------

func (a *TradeOrderActor) onSubmit(msg *SubmitOrder) {
	if a.state != StateNew {
		// INVALID TRANSITION — log loudly, do not panic.
		fmt.Printf("  [%s] ⚠ INVALID: SubmitOrder received in state %s (only valid in New)\n",
			a.name, a.state)
		return
	}
	a.orderID = msg.OrderID
	a.symbol = msg.Symbol
	a.side = msg.Side
	a.totalQty = msg.Qty
	a.price = msg.Price
	a.state = StateSubmitted
	fmt.Printf("  [%s] %s → %s  order=%s %s %.4f %s @ %.2f\n",
		a.name, StateNew, a.state, a.orderID, a.side, a.totalQty, a.symbol, a.price)
}

func (a *TradeOrderActor) onAck(msg *OrderAcknowledged) {
	if a.state != StateSubmitted {
		fmt.Printf("  [%s] ⚠ INVALID: OrderAcknowledged received in state %s\n", a.name, a.state)
		return
	}
	prev := a.state
	a.exchangeOrderID = msg.ExchangeOrderID
	// Move to PartiallyFilled with 0 filled — exchange acknowledged but nothing
	// executed yet.  This is a common real-world state.
	a.state = StatePartiallyFilled
	fmt.Printf("  [%s] %s → %s  exchangeID=%s\n", a.name, prev, a.state, a.exchangeOrderID)
}

func (a *TradeOrderActor) onReject(msg *OrderRejected) {
	if a.state != StateSubmitted {
		fmt.Printf("  [%s] ⚠ INVALID: OrderRejected received in state %s\n", a.name, a.state)
		return
	}
	prev := a.state
	a.state = StateRejected
	fmt.Printf("  [%s] %s → %s  reason=%q\n", a.name, prev, a.state, msg.Reason)
}

func (a *TradeOrderActor) onPartialFill(msg *PartialFill) {
	if a.state != StatePartiallyFilled {
		fmt.Printf("  [%s] ⚠ INVALID: PartialFill received in state %s\n", a.name, a.state)
		return
	}
	a.filledQty += msg.FilledQty
	a.totalCost += msg.FilledQty * msg.FilledPrice
	avgPrice := a.totalCost / a.filledQty
	fmt.Printf("  [%s] %s → %s  +%.4f @ %.2f  (total filled=%.4f / %.4f  avg=%.2f)\n",
		a.name, a.state, a.state,
		msg.FilledQty, msg.FilledPrice,
		a.filledQty, a.totalQty, avgPrice)
}

func (a *TradeOrderActor) onFullFill(msg *FullFill) {
	if a.state != StatePartiallyFilled {
		fmt.Printf("  [%s] ⚠ INVALID: FullFill received in state %s\n", a.name, a.state)
		return
	}
	prev := a.state
	a.filledQty += msg.FilledQty
	a.totalCost += msg.FilledQty * msg.FilledPrice
	avgPrice := a.totalCost / a.filledQty
	a.state = StateFullyFilled
	fmt.Printf("  [%s] %s → %s  +%.4f @ %.2f  (total filled=%.4f  avg=%.2f)\n",
		a.name, prev, a.state,
		msg.FilledQty, msg.FilledPrice,
		a.filledQty, avgPrice)
}

func (a *TradeOrderActor) onCancel() {
	if a.state != StateSubmitted && a.state != StatePartiallyFilled {
		fmt.Printf("  [%s] ⚠ INVALID: CancelOrder received in state %s\n", a.name, a.state)
		return
	}
	prev := a.state
	a.state = StateCancelled
	fmt.Printf("  [%s] %s → %s  (filled so far: %.4f / %.4f)\n",
		a.name, prev, a.state, a.filledQty, a.totalQty)
}

func (a *TradeOrderActor) onStatus(msg *GetOrderStatus) {
	remaining := a.totalQty - a.filledQty
	avg := 0.0
	if a.filledQty > 0 {
		avg = a.totalCost / a.filledQty
	}
	msg.Reply <- OrderStatus{
		OrderID:         a.orderID,
		State:           a.state,
		FilledQty:       a.filledQty,
		RemainingQty:    remaining,
		AvgFilledPrice:  avg,
		ExchangeOrderID: a.exchangeOrderID,
	}
}

// ---------------------------------------------------------------------------
// Public send helpers — hide raw channel sends from main
// ---------------------------------------------------------------------------

func (a *TradeOrderActor) Submit(m SubmitOrder)    { a.mailbox <- orderCmd{submit: &m} }
func (a *TradeOrderActor) Ack(m OrderAcknowledged) { a.mailbox <- orderCmd{ack: &m} }
func (a *TradeOrderActor) Reject(m OrderRejected)  { a.mailbox <- orderCmd{reject: &m} }
func (a *TradeOrderActor) Partial(m PartialFill)   { a.mailbox <- orderCmd{partial: &m} }
func (a *TradeOrderActor) Fill(m FullFill)         { a.mailbox <- orderCmd{full: &m} }
func (a *TradeOrderActor) Cancel()                 { a.mailbox <- orderCmd{cancel: &CancelOrder{}} }
func (a *TradeOrderActor) Status() OrderStatus {
	reply := make(chan OrderStatus, 1)
	a.mailbox <- orderCmd{status: &GetOrderStatus{Reply: reply}}
	return <-reply
}

// Close signals the actor to stop (by closing the mailbox).
func (a *TradeOrderActor) Close() { close(a.mailbox) }

// ---------------------------------------------------------------------------
// Helper: print a status snapshot
// ---------------------------------------------------------------------------

func printStatus(label string, s OrderStatus) {
	fmt.Printf("  ── %s status: state=%-16s filled=%.4f remaining=%.4f avgPrice=%.2f\n",
		label, s.State, s.FilledQty, s.RemainingQty, s.AvgFilledPrice)
}

// ---------------------------------------------------------------------------
// main — four scenarios
// ---------------------------------------------------------------------------

func main() {
	var wg sync.WaitGroup

	// ── Scenario 1: Happy path ─────────────────────────────────────────────
	fmt.Println("\n╔══════════════════════════════════════════════════════════════╗")
	fmt.Println("║  Scenario 1: Happy Path — New → Submitted → Partial → Full  ║")
	fmt.Println("╚══════════════════════════════════════════════════════════════╝")

	o1 := newTradeOrderActor("Order-1", &wg)
	o1.Submit(SubmitOrder{OrderID: "O-001", Symbol: "BTC-USD", Side: "BUY", Qty: 1.0, Price: 65000.00})
	o1.Ack(OrderAcknowledged{ExchangeOrderID: "EX-8821"})
	o1.Partial(PartialFill{FilledQty: 0.4, FilledPrice: 64990.00})
	o1.Partial(PartialFill{FilledQty: 0.3, FilledPrice: 65010.00})
	o1.Fill(FullFill{FilledQty: 0.3, FilledPrice: 65005.00})

	time.Sleep(20 * time.Millisecond) // let the actor finish printing
	s1 := o1.Status()
	printStatus("Order-1", s1)
	o1.Close()

	// ── Scenario 2: Rejection ──────────────────────────────────────────────
	fmt.Println("\n╔════════════════════════════════════════════════════════╗")
	fmt.Println("║  Scenario 2: Rejection — New → Submitted → Rejected   ║")
	fmt.Println("╚════════════════════════════════════════════════════════╝")

	o2 := newTradeOrderActor("Order-2", &wg)
	o2.Submit(SubmitOrder{OrderID: "O-002", Symbol: "ETH-USD", Side: "SELL", Qty: 10.0, Price: 3500.00})
	o2.Reject(OrderRejected{Reason: "insufficient margin"})

	time.Sleep(20 * time.Millisecond)
	s2 := o2.Status()
	printStatus("Order-2", s2)
	o2.Close()

	// ── Scenario 3: Cancellation mid-fill ─────────────────────────────────
	fmt.Println("\n╔═══════════════════════════════════════════════════════════════╗")
	fmt.Println("║  Scenario 3: Cancel mid-fill — Partial → Cancelled            ║")
	fmt.Println("╚═══════════════════════════════════════════════════════════════╝")

	o3 := newTradeOrderActor("Order-3", &wg)
	o3.Submit(SubmitOrder{OrderID: "O-003", Symbol: "SOL-USD", Side: "BUY", Qty: 100.0, Price: 150.00})
	o3.Ack(OrderAcknowledged{ExchangeOrderID: "EX-9943"})
	o3.Partial(PartialFill{FilledQty: 30.0, FilledPrice: 149.95})
	o3.Cancel() // user changed their mind

	time.Sleep(20 * time.Millisecond)
	s3 := o3.Status()
	printStatus("Order-3", s3)
	o3.Close()

	// ── Scenario 4: Invalid message in wrong state ─────────────────────────
	fmt.Println("\n╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║  Scenario 4: Invalid transition — SubmitOrder when Submitted   ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
	fmt.Println("  (Actor should log a warning, not crash)")

	o4 := newTradeOrderActor("Order-4", &wg)
	o4.Submit(SubmitOrder{OrderID: "O-004", Symbol: "BTC-USD", Side: "BUY", Qty: 0.5, Price: 64000.00})
	// Send SubmitOrder again — this is the invalid transition.
	o4.Submit(SubmitOrder{OrderID: "O-004b", Symbol: "BTC-USD", Side: "SELL", Qty: 0.5, Price: 64500.00})

	time.Sleep(20 * time.Millisecond)
	s4 := o4.Status()
	printStatus("Order-4", s4)
	o4.Cancel() // valid: Submitted → Cancelled
	time.Sleep(10 * time.Millisecond)
	o4.Close()

	wg.Wait()
	fmt.Println("\nAll scenarios complete.")
}
