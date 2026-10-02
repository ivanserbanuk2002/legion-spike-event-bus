# legion-spike-event-bus

A small in-memory pub/sub architecture spike. The sample payment events carry an `id`; they do not execute payments or trades.

## Layout

- `pkg/index.ts`: typed public contract and factory export.
- `internal/event-bus.ts`: native Node `EventEmitter` implementation.
- `cmd/demo.ts`: subscriber and publisher example for both topics.

## Usage

Requires Node.js 20.5.0 or newer and npm. No build step is needed.

```sh
npm ci
npm run typecheck
npm test
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

## Additional methods

- `subscribeAbortable(topic, listener, signal)` delivers like `subscribe` until the
  signal aborts or its repeat-safe unsubscribe handle is called. An already-aborted
  signal registers nothing. Each handle owns one subscription, including reused
  callbacks. Abort, manual unsubscribe and `clear` remove its abort handler as well
  as its bus listener. Another abort handler calling `stopImmediatePropagation()`
  cannot prevent cancellation. Abort during a publish preserves the native current-delivery
  snapshot; subsequent publishes skip the removed listener. Listener errors still
  propagate synchronously to the publisher.
- `waitForEvent(topic, { signal?, timeoutMs? } = {})` returns a promise for the
  first event delivered to its one-time subscription. The resolved value is the
  original payload object. An already-aborted signal rejects with `signal.reason`
  without subscribing; a later abort rejects with that same reason, even if another
  abort handler calls `stopImmediatePropagation()`. Timeout rejects
  with a `TimeoutError` DOMException. `timeoutMs` must be an integer from 1 through
  2147483647; invalid values reject with `RangeError` before subscribing. Event,
  abort or timeout removes the listener, abort handler and timer; the first outcome
  wins. Without a signal or timeout, the wait has no deadline. `clear` removes its
  bus listener but leaves the promise pending until abort or timeout, if provided.
  Publishing stays synchronous; promise continuations run afterward. A preceding
  listener error still propagates from publishing and can prevent this wait from
  seeing that event.
- `subscribeOnce(topic, listener)` returns an unsubscribe function and delivers at most one event. The subscription is removed before invoking the listener, including recursive publishing and thrown errors. Each unsubscribe function is repeat-safe and owns only its subscription, even when callbacks are reused.
- `publishMany(topic, events)` accepts a readonly array and publishes synchronously in array order. An empty array does nothing. The first listener error propagates immediately, skipping later listeners for that event and every remaining event; earlier deliveries are not rolled back. The bus does not mutate the array or clone payloads.
- `listenerCount(topic)` reports currently registered subscriptions for that topic on this bus. Repeated callbacks count separately, and pending one-time subscriptions count until they fire or are cancelled.
- `clear(topic?)` removes persistent and one-time subscriptions from one topic, or both topics when omitted. It is repeat-safe, leaves other buses alone and allows new subscriptions afterward. Old unsubscribe handles cannot remove newly registered subscriptions. Native `EventEmitter` delivery uses a snapshot: clearing during a callback affects subsequent publishes, while already selected callbacks still run in the current delivery.

```ts
const stopOnce = bus.subscribeOnce("payment.failed", ({ id }) => console.log(id));
console.log(bus.listenerCount("payment.failed")); // 1
bus.publish("payment.failed", { id: "failed-1" });
stopOnce(); // Also safe after delivery.
bus.publishMany("payment.created", [{ id: "created-1" }, { id: "created-2" }]);
bus.clear("payment.created");
bus.clear(); // Clear both topics on this bus.
```

```ts
const controller = new AbortController();
const stop = bus.subscribeAbortable("payment.created", ({ id }) => console.log(id), controller.signal);
bus.publish("payment.created", { id: "abortable-1" });
controller.abort();
stop(); // Safe after abort; also removes the abort handler when called first.
```

```ts
const next = bus.waitForEvent("payment.created", { timeoutMs: 1000 });
bus.publish("payment.created", { id: "waited-1" });
console.log((await next).id); // waited-1
```

## ADR stub

- **Context:** establish a small publisher/subscriber boundary for a future router feature.
- **Decision:** keep the contract in `pkg/` and wrap Node's `EventEmitter` in `internal/`.
- **Consequences:** no runtime broker dependency; delivery is transient and stays within one process.
- **Revisit:** define durability, retries, ordering and failure handling before selecting a production transport.

## Validation

The original 25-line spike was validated with temporary assertions on Node `v20.20.2`. Feature work adds a retained `node:test` suite in `tests/event-bus.test.ts` covering the original delivery contract, one-time subscriptions, ordered batches with error propagation, listener counts, scoped cleanup, abortable subscriptions and waiting with abort/time limits. `npm test` names the test file explicitly so it also runs on Windows with Node 20. The GitHub Actions workflow installs the locked dependencies with `npm ci`, then runs the no-emit typecheck and tests on Node 20.

## Design references

[Nader Dabit's a2a-x402-typescript commit 43d7294](https://github.com/dabit3/a2a-x402-typescript/commit/43d7294c4fc489d574b579d3ed2f856ebefa5f3f) provides a reference for typed event payloads, status/correlation fields and injected event dispatch. This spike keeps its existing `{ id: string }` payload. Its unsubscribe and lifecycle behavior are local extensions; the referenced commit does not establish those guarantees. No source was copied.

## Status

✅ In-memory spike with lifecycle helpers and retained tests

This status covers only the in-memory spike checks described above. Durability, retries and cross-process integration have not been validated. The companion [legion-spike-trade-router](https://github.com/ivanserbanuk2002/legion-spike-trade-router) is the future integration target; this spike does not connect to it.

`subscribeWhere(topic, predicate, listener)` filters deliveries synchronously,
returns a repeat-safe unsubscribe handle and propagates predicate/listener errors.
A nonmatching event leaves the subscription registered.

`activeTopics()` returns a new array of topics with registered listeners,
in `payment.created`, `payment.failed` order. Fired one-time listeners disappear.

`publishIfObserved(topic, event)` returns whether listeners were registered at
dispatch. A true result is not an acknowledgement of processing or persistence;
filtered listeners may skip it. Synchronous errors still propagate.
