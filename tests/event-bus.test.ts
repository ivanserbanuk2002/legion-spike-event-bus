import assert from "node:assert/strict";
import test from "node:test";
import { createEventBus, type PaymentEvent } from "../pkg/index.js";

test("publish delivers synchronously in registration order to its topic", () => {
  const bus = createEventBus();
  const seen: string[] = [];
  bus.subscribe("payment.created", ({ id }) => seen.push(`first:${id}`));
  bus.subscribe("payment.created", ({ id }) => seen.push(`second:${id}`));
  bus.subscribe("payment.failed", ({ id }) => seen.push(`failed:${id}`));
  bus.publish("payment.created", { id: "a" });
  assert.deepEqual(seen, ["first:a", "second:a"]);
  bus.publish("payment.failed", { id: "b" });
  assert.deepEqual(seen, ["first:a", "second:a", "failed:b"]);
});

test("unsubscribe is repeat-safe and owns only one duplicate callback", () => {
  const bus = createEventBus();
  let calls = 0;
  const listener = () => { calls++; };
  const stopFirst = bus.subscribe("payment.created", listener);
  const stopSecond = bus.subscribe("payment.created", listener);
  stopFirst();
  stopFirst();
  bus.publish("payment.created", { id: "a" });
  assert.equal(calls, 1);
  stopSecond();
  bus.publish("payment.created", { id: "b" });
  assert.equal(calls, 1);
});

test("buses are independent and do not replay past events", () => {
  const first = createEventBus();
  const second = createEventBus();
  const seen: string[] = [];
  first.publish("payment.created", { id: "past" });
  first.subscribe("payment.created", ({ id }) => seen.push(id));
  second.publish("payment.created", { id: "other-bus" });
  assert.deepEqual(seen, []);
  first.publish("payment.created", { id: "present" });
  assert.deepEqual(seen, ["present"]);
});

test("publish propagates the same listener error and stops delivery", () => {
  const bus = createEventBus();
  const failure = new Error("listener failed");
  let laterCalls = 0;
  bus.subscribe("payment.failed", () => { throw failure; });
  bus.subscribe("payment.failed", () => { laterCalls++; });
  assert.throws(() => bus.publish("payment.failed", { id: "a" }), (error) => error === failure);
  assert.equal(laterCalls, 0);
});

test("subscribeOnce fires only once even when its listener publishes recursively", () => {
  const bus = createEventBus();
  const seen: string[] = [];
  bus.subscribeOnce("payment.created", ({ id }: PaymentEvent) => {
    seen.push(id);
    if (id === "outer") bus.publish("payment.created", { id: "inner" });
  });
  bus.publish("payment.failed", { id: "other-topic" });
  bus.publish("payment.created", { id: "outer" });
  bus.publish("payment.created", { id: "later" });
  assert.deepEqual(seen, ["outer"]);
});

test("subscribeOnce cancellation is repeat-safe and independent of duplicate callbacks", () => {
  const bus = createEventBus();
  let calls = 0;
  const listener = () => { calls++; };
  const stopFirst = bus.subscribeOnce("payment.failed", listener);
  const stopSecond = bus.subscribeOnce("payment.failed", listener);
  const stopPersistent = bus.subscribe("payment.failed", listener);
  stopFirst();
  stopFirst();
  bus.publish("payment.failed", { id: "a" });
  assert.equal(calls, 2);
  stopSecond();
  stopSecond();
  bus.publish("payment.failed", { id: "b" });
  assert.equal(calls, 3);
  stopPersistent();
});

test("a throwing subscribeOnce listener is still removed", () => {
  const bus = createEventBus();
  const failure = new Error("once listener failed");
  bus.subscribeOnce("payment.failed", () => { throw failure; });
  assert.throws(() => bus.publish("payment.failed", { id: "a" }), (error) => error === failure);
  assert.doesNotThrow(() => bus.publish("payment.failed", { id: "b" }));
});

test("publishMany delivers a readonly batch synchronously in event order", () => {
  const bus = createEventBus();
  const seen: string[] = [];
  const events: readonly PaymentEvent[] = Object.freeze([{ id: "a" }, { id: "b" }]);
  bus.subscribe("payment.created", ({ id }) => seen.push(`first:${id}`));
  bus.subscribeOnce("payment.created", ({ id }) => seen.push(`once:${id}`));
  bus.subscribe("payment.created", ({ id }) => seen.push(`second:${id}`));
  bus.subscribe("payment.failed", () => assert.fail("wrong topic"));
  bus.publishMany("payment.created", events);
  assert.deepEqual(seen, ["first:a", "once:a", "second:a", "first:b", "second:b"]);
  assert.deepEqual(events, [{ id: "a" }, { id: "b" }]);
});

test("publishMany accepts empty batches and the failed topic", () => {
  const bus = createEventBus();
  const seen: string[] = [];
  bus.subscribe("payment.failed", ({ id }) => seen.push(id));
  bus.publishMany("payment.failed", []);
  assert.deepEqual(seen, []);
  bus.publishMany("payment.failed", [{ id: "failed" }]);
  assert.deepEqual(seen, ["failed"]);
});

test("publishMany propagates the first error and skips remaining delivery", () => {
  const bus = createEventBus();
  const seen: string[] = [];
  const failure = new Error("batch listener failed");
  bus.subscribe("payment.created", ({ id }) => {
    seen.push(`first:${id}`);
    if (id === "b") throw failure;
  });
  bus.subscribe("payment.created", ({ id }) => seen.push(`second:${id}`));
  assert.throws(
    () => bus.publishMany("payment.created", [{ id: "a" }, { id: "b" }, { id: "c" }]),
    (error) => error === failure,
  );
  assert.deepEqual(seen, ["first:a", "second:a", "first:b"]);
});

test("listenerCount tracks each subscription within its topic and bus", () => {
  const bus = createEventBus();
  const other = createEventBus();
  const listener = () => {};
  assert.equal(bus.listenerCount("payment.created"), 0);
  const stopFirst = bus.subscribe("payment.created", listener);
  const stopSecond = bus.subscribe("payment.created", listener);
  assert.equal(bus.listenerCount("payment.created"), 2);
  assert.equal(bus.listenerCount("payment.failed"), 0);
  assert.equal(other.listenerCount("payment.created"), 0);
  stopFirst();
  stopFirst();
  assert.equal(bus.listenerCount("payment.created"), 1);
  stopSecond();
  assert.equal(bus.listenerCount("payment.created"), 0);
});

test("listenerCount removes once subscriptions before delivery and on cancellation", () => {
  const bus = createEventBus();
  bus.subscribe("payment.failed", () => {});
  const stopDelivered = bus.subscribeOnce("payment.failed", () => {
    assert.equal(bus.listenerCount("payment.failed"), 1);
  });
  assert.equal(bus.listenerCount("payment.failed"), 2);
  bus.publish("payment.failed", { id: "a" });
  stopDelivered();
  stopDelivered();
  assert.equal(bus.listenerCount("payment.failed"), 1);
  const stopCancelled = bus.subscribeOnce("payment.failed", () => assert.fail("cancelled"));
  assert.equal(bus.listenerCount("payment.failed"), 2);
  stopCancelled();
  stopCancelled();
  assert.equal(bus.listenerCount("payment.failed"), 1);
  bus.publish("payment.failed", { id: "b" });
});

test("clear(topic) removes all subscriptions only from that topic", () => {
  const bus = createEventBus();
  const seen: string[] = [];
  bus.subscribe("payment.created", () => assert.fail("cleared persistent listener"));
  bus.subscribeOnce("payment.created", () => assert.fail("cleared once listener"));
  bus.subscribe("payment.failed", ({ id }) => seen.push(id));
  bus.clear("payment.created");
  bus.clear("payment.created");
  assert.equal(bus.listenerCount("payment.created"), 0);
  assert.equal(bus.listenerCount("payment.failed"), 1);
  bus.publish("payment.created", { id: "a" });
  bus.publish("payment.failed", { id: "b" });
  assert.deepEqual(seen, ["b"]);
});

test("clear() removes both topics only from its own bus", () => {
  const bus = createEventBus();
  const other = createEventBus();
  const seen: string[] = [];
  bus.subscribe("payment.created", () => assert.fail("cleared persistent listener"));
  bus.subscribeOnce("payment.failed", () => assert.fail("cleared once listener"));
  other.subscribe("payment.created", ({ id }) => seen.push(id));
  bus.clear();
  bus.clear();
  assert.equal(bus.listenerCount("payment.created"), 0);
  assert.equal(bus.listenerCount("payment.failed"), 0);
  bus.publish("payment.created", { id: "a" });
  bus.publish("payment.failed", { id: "b" });
  other.publish("payment.created", { id: "other" });
  assert.deepEqual(seen, ["other"]);
  bus.subscribe("payment.failed", () => assert.fail("explicit undefined also clears all"));
  bus.clear(undefined);
  assert.equal(bus.listenerCount("payment.failed"), 0);
});

test("old unsubscribe handles remain harmless after clearing and resubscribing", () => {
  const bus = createEventBus();
  let calls = 0;
  const listener = () => { calls++; };
  const stopPersistent = bus.subscribe("payment.created", listener);
  const stopOnce = bus.subscribeOnce("payment.created", listener);
  bus.clear();
  bus.subscribe("payment.created", listener);
  bus.subscribeOnce("payment.created", listener);
  stopPersistent();
  stopPersistent();
  stopOnce();
  stopOnce();
  assert.equal(bus.listenerCount("payment.created"), 2);
  bus.publish("payment.created", { id: "a" });
  assert.equal(calls, 2);
  bus.publish("payment.created", { id: "b" });
  assert.equal(calls, 3);
});

test("clear during delivery keeps the current snapshot but blocks later publishes", () => {
  const bus = createEventBus();
  const seen: string[] = [];
  bus.subscribe("payment.created", ({ id }) => {
    seen.push(`first:${id}`);
    bus.clear("payment.created");
    bus.publish("payment.created", { id: "nested" });
  });
  bus.subscribe("payment.created", ({ id }) => seen.push(`second:${id}`));
  bus.publish("payment.created", { id: "a" });
  bus.publish("payment.created", { id: "b" });
  assert.deepEqual(seen, ["first:a", "second:a"]);
  assert.equal(bus.listenerCount("payment.created"), 0);
});


test("subscribeWhere filters synchronously and owns its unsubscribe handle", () => {
  const bus = createEventBus();
  const seen: string[] = [];
  const stop = bus.subscribeWhere("payment.created", ({ id }) => id.startsWith("keep"),
    ({ id }) => seen.push(id));
  bus.publishMany("payment.created", [{ id: "skip" }, { id: "keep-1" }]);
  assert.deepEqual(seen, ["keep-1"]);
  stop(); stop();
  bus.publish("payment.created", { id: "keep-2" });
  assert.deepEqual(seen, ["keep-1"]);
  const failure = new Error("predicate failed");
  bus.subscribeWhere("payment.failed", () => { throw failure; }, () => assert.fail());
  assert.throws(() => bus.publish("payment.failed", { id: "x" }), (e) => e === failure);
});


test("activeTopics returns a detached deterministic list of subscribed topics", () => {
  const bus = createEventBus();
  assert.deepEqual(bus.activeTopics(), []);
  bus.subscribeOnce("payment.failed", () => {});
  const stop = bus.subscribe("payment.created", () => {});
  const topics = bus.activeTopics();
  assert.deepEqual(topics, ["payment.created", "payment.failed"]);
  topics.pop();
  assert.equal(bus.activeTopics().length, 2);
  bus.publish("payment.failed", { id: "x" });
  assert.deepEqual(bus.activeTopics(), ["payment.created"]);
  stop();
  assert.deepEqual(bus.activeTopics(), []);
});


test("publishIfObserved reports registered listeners without swallowing errors", () => {
  const bus = createEventBus();
  assert.equal(bus.publishIfObserved("payment.created", { id: "lost" }), false);
  let delivered = "";
  bus.subscribeOnce("payment.created", ({ id }) => { delivered = id; });
  assert.equal(bus.publishIfObserved("payment.created", { id: "kept" }), true);
  assert.equal(delivered, "kept");
  assert.equal(bus.publishIfObserved("payment.created", { id: "lost-again" }), false);
  const failure = new Error("listener failed");
  bus.subscribe("payment.failed", () => { throw failure; });
  assert.throws(() => bus.publishIfObserved("payment.failed", { id: "x" }), (e) => e === failure);
});
