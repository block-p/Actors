// Actor Model from Scratch in Rust
//
// Rust is different from Go in one important way:
// Go uses interfaces (dynamic dispatch) → easy to send "any" message
// Rust uses enums (static dispatch)    → each actor has its OWN message type
//
// This is actually BETTER for a trade engine:
// - The compiler tells you if you send the wrong message type
// - No runtime type-checking overhead
// - Exhaustive pattern matching → you can't forget a message case
//
// The pattern:
//   1. Define an enum of all messages an actor can receive
//   2. Spawn a tokio task (the actor goroutine equivalent)
//   3. Give the task an mpsc::Receiver (the mailbox)
//   4. Give callers an mpsc::Sender (the ActorRef equivalent)
//
// Run with: cargo run

use tokio::sync::{mpsc, oneshot};

// =============================================================================
// PART 1 — THE CORE PATTERN
//
// In Rust, we don't need a framework struct.
// The pattern is:
//
//   let (tx, mut rx) = mpsc::channel(mailbox_size);
//
//   tokio::spawn(async move {
//       // private state lives here
//       let mut state = MyState::new();
//
//       while let Some(msg) = rx.recv().await {
//           match msg {
//               MyMessage::Foo => { state.handle_foo() }
//               MyMessage::Bar => { state.handle_bar() }
//           }
//       }
//   });
//
//   // tx is the ActorRef — hand it out to whoever needs to send messages
//
// That's it. The task IS the actor.
// The rx (receiver) IS the mailbox.
// The tx (sender) IS the actor's address.
// Local variables inside the task ARE the private state.
// =============================================================================


// =============================================================================
// PART 2 — COUNTER ACTOR
//
// Same idea as Go, but:
// - Messages are an enum (not `any`)
// - Reply uses oneshot channel (not a buffered channel)
//   oneshot = single-use channel, perfect for request/response
// =============================================================================

// Every message the counter can receive
enum CounterMsg {
    Increment(i64),
    Decrement(i64),
    Reset,
    // oneshot::Sender is a one-time reply channel
    // It's like "here's an envelope, put the answer in it"
    GetValue(oneshot::Sender<i64>),
}

// ActorRef for the counter — just a channel sender
// Cloning it gives you another handle to the same actor
type CounterRef = mpsc::Sender<CounterMsg>;

fn spawn_counter() -> CounterRef {
    let (tx, mut rx) = mpsc::channel(20);

    tokio::spawn(async move {
        // PRIVATE STATE — only this task can touch it
        let mut value: i64 = 0;

        // The actor loop — process one message at a time
        while let Some(msg) = rx.recv().await {
            match msg {
                CounterMsg::Increment(n) => {
                    value += n;
                    // fire and forget — no reply needed
                }
                CounterMsg::Decrement(n) => {
                    value -= n;
                }
                CounterMsg::Reset => {
                    value = 0;
                }
                CounterMsg::GetValue(reply) => {
                    // Send the answer back through the oneshot channel
                    // .send() consumes the sender — it can only be used once
                    let _ = reply.send(value);
                }
            }
        }
    });

    tx // return the sender = the actor's address
}

async fn run_counter_example() {
    println!("\n--- Part 2: Counter Actor ---");

    let counter = spawn_counter();

    // Fire and forget
    counter.send(CounterMsg::Increment(10)).await.unwrap();
    counter.send(CounterMsg::Increment(5)).await.unwrap();
    counter.send(CounterMsg::Decrement(3)).await.unwrap();

    // Ask pattern with oneshot
    let (reply_tx, reply_rx) = oneshot::channel();
    counter.send(CounterMsg::GetValue(reply_tx)).await.unwrap();
    let value = reply_rx.await.unwrap();
    println!("Counter value: {}", value); // expect 12

    counter.send(CounterMsg::Reset).await.unwrap();

    let (reply_tx, reply_rx) = oneshot::channel();
    counter.send(CounterMsg::GetValue(reply_tx)).await.unwrap();
    let value = reply_rx.await.unwrap();
    println!("After reset: {}", value); // expect 0
}


// =============================================================================
// PART 3 — WHY RUST'S ENUM MESSAGES ARE BETTER
//
// In Go:  msg.(type) — runtime check, compiler can't help you
// In Rust: match msg  — compile time, compiler FORCES exhaustive handling
//
// Example: if you add a new message type in Rust and forget to handle it:
//   error[E0004]: non-exhaustive patterns: `NewMessage` not covered
//
// This is caught at COMPILE TIME, not in production at 3am.
// =============================================================================


// =============================================================================
// PART 4 — MINI TRADE SYSTEM
//
// Three actors:
//   LoggerActor  — receives and prints all log/trade events
//   StrategyActor — watches prices, emits trade signals
//   MarketActor  — simulates incoming price ticks
//
// Notice: each actor has its OWN message enum.
// They are completely independent types.
// The compiler won't let you send a MarketMsg to the LoggerActor.
// =============================================================================

// --- Logger Actor ---

enum LoggerMsg {
    Info(String),
    TradeSignal { side: String, symbol: String, price: f64, reason: String },
}

type LoggerRef = mpsc::Sender<LoggerMsg>;

fn spawn_logger() -> LoggerRef {
    let (tx, mut rx) = mpsc::channel(50);

    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                LoggerMsg::Info(text) => {
                    println!("[INFO] {}", text);
                }
                LoggerMsg::TradeSignal { side, symbol, price, reason } => {
                    println!("[TRADE] {} {} @ {:.2} — {}", side, symbol, price, reason);
                }
            }
        }
    });

    tx
}

// --- Strategy Actor ---

enum StrategyMsg {
    PriceTick { symbol: String, price: f64 },
}

type StrategyRef = mpsc::Sender<StrategyMsg>;

fn spawn_strategy(logger: LoggerRef) -> StrategyRef {
    let (tx, mut rx) = mpsc::channel(50);

    tokio::spawn(async move {
        // PRIVATE STATE
        let mut last_price: f64 = 0.0;

        while let Some(msg) = rx.recv().await {
            match msg {
                StrategyMsg::PriceTick { symbol, price } => {
                    logger
                        .send(LoggerMsg::Info(format!("Price tick: {} @ {:.2}", symbol, price)))
                        .await
                        .unwrap();

                    // First tick
                    if last_price == 0.0 {
                        last_price = price;
                        continue;
                    }

                    let change = (price - last_price) / last_price * 100.0;

                    if change <= -1.0 {
                        logger
                            .send(LoggerMsg::TradeSignal {
                                side: "BUY".into(),
                                symbol: symbol.clone(),
                                price,
                                reason: format!("price dropped {:.2}%", change),
                            })
                            .await
                            .unwrap();
                    } else if change >= 1.0 {
                        logger
                            .send(LoggerMsg::TradeSignal {
                                side: "SELL".into(),
                                symbol: symbol.clone(),
                                price,
                                reason: format!("price rose +{:.2}%", change),
                            })
                            .await
                            .unwrap();
                    }

                    last_price = price;
                }
            }
        }
    });

    tx
}

// --- Market Actor ---
// In a real system this would open a WebSocket and stream real data.
// Here we just forward simulated ticks.

enum MarketMsg {
    Tick { symbol: String, price: f64 },
}

type MarketRef = mpsc::Sender<MarketMsg>;

fn spawn_market(strategy: StrategyRef) -> MarketRef {
    let (tx, mut rx) = mpsc::channel(50);

    tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            match msg {
                MarketMsg::Tick { symbol, price } => {
                    // Forward to strategy
                    strategy
                        .send(StrategyMsg::PriceTick { symbol, price })
                        .await
                        .unwrap();
                }
            }
        }
    });

    tx
}

async fn run_trade_example() {
    println!("\n--- Part 4: Mini Trade System ---");

    let logger   = spawn_logger();
    let strategy = spawn_strategy(logger);
    let market   = spawn_market(strategy);

    // Simulate price ticks
    let prices = vec![100.0, 99.5, 98.8, 99.9, 101.2, 100.5, 99.0];
    for price in prices {
        market
            .send(MarketMsg::Tick {
                symbol: "BTC/USD".into(),
                price,
            })
            .await
            .unwrap();

        tokio::time::sleep(tokio::time::Duration::from_millis(50)).await;
    }

    // Wait for messages to flush
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
}


// =============================================================================
// PART 5 — KEY DIFFERENCES: GO vs RUST ACTORS
//
// Go:
//   + Simpler to write (chan any, type switch)
//   - Runtime type errors possible
//   - Must use recover() for crash handling
//   + Goroutines are very cheap
//
// Rust:
//   + Compile-time message type safety
//   + No runtime panics from wrong message types
//   + Zero-cost abstractions (no GC pauses)
//   - More verbose (enum per actor)
//   - Ownership rules require more thought
//   + tokio tasks are extremely cheap (similar to goroutines)
//
// For a trade engine:
//   - Go = faster to build, easier to iterate
//   - Rust = safer, lower latency, better for HFT
// =============================================================================

#[tokio::main]
async fn main() {
    run_counter_example().await;
    run_trade_example().await;

    println!("\nDone.");
}
