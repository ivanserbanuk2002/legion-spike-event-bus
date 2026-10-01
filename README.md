# legion-spike-event-bus

A small in-memory pub/sub architecture spike. The sample payment events carry an `id`; they do not execute payments or trades.

## Layout

- `pkg/index.ts`: typed public contract and factory export.
- `internal/event-bus.ts`: native Node `EventEmitter` implementation.
- `cmd/demo.ts`: subscriber and publisher example for both topics.

## Usage

Requires Node.js 20 or newer and npm. No build step is needed.

```sh
npm ci
npm run typecheck
npm run demo
```

The demo prints `created: demo-1` and `failed: demo-1`.

```ts
import { createEventBus } from "./pkg/index.js";
const bus = createEventBus();
const unsubscribe = bus.subscribe("payment.created", ({ id }) => console.log(id));
bus.publish("payment.created", { id: "example-1" });
unsubscribe();
```

The two topics are `payment.created` and `payment.failed`; both accept `{ id: string }`. Topic and payload checks are compile-time TypeScript checks. Each factory call creates an independent bus. Subscribers run synchronously in registration order; events are neither buffered nor replayed. A thrown listener error propagates to the publisher and stops that delivery. Async listeners are not awaited.

## ADR stub

- **Context:** establish a small publisher/subscriber boundary for a future router feature.
- **Decision:** keep the contract in `pkg/` and wrap Node's `EventEmitter` in `internal/`.
- **Consequences:** no runtime broker dependency; delivery is transient and stays within one process.
- **Revisit:** define durability, retries, ordering and failure handling before selecting a production transport.

## Validation

On 2026-10-01, Node `v20.20.2` passed the no-emit typecheck, demo and temporary assertions for both topics, topic isolation, repeat-safe unsubscribe including duplicate callbacks, independent buses, no replay, synchronous order and propagated listener errors. The spike contains 25 application TypeScript lines and no retained test suite.
