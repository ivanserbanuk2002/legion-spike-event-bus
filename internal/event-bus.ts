import { addAbortListener, EventEmitter } from "node:events";
import type { EventBus, PaymentEvent, Topic } from "../pkg/index.js";
export function createEventBus(): EventBus {
  const emitter = new EventEmitter<Record<Topic, [PaymentEvent]>>();
  const abortable = new Map<() => void, Topic>();
  const bus: EventBus = {
    publishIfObserved: (topic, event) => emitter.emit(topic, event),
    publish: (topic, event) => { emitter.emit(topic, event); },
    publishMany: (topic, events) => {
      for (const event of events) emitter.emit(topic, event);
    },
    subscribe: (topic, listener) => {
      const subscribed = (event: PaymentEvent) => listener(event);
      emitter.on(topic, subscribed);
      return () => { emitter.off(topic, subscribed); };
    },
    subscribeAbortable: (topic, listener, signal) => {
      if (signal.aborted) return () => {};
      const subscribed = (event: PaymentEvent) => listener(event);
      let abortListener: ReturnType<typeof addAbortListener> | undefined;
      const unsubscribe = () => {
        emitter.off(topic, subscribed);
        abortListener?.[Symbol.dispose]();
        abortable.delete(unsubscribe);
      };
      emitter.on(topic, subscribed);
      abortListener = addAbortListener(signal, unsubscribe);
      abortable.set(unsubscribe, topic);
      return unsubscribe;
    },
    waitForEvent: (topic, { signal, timeoutMs } = {}) => new Promise((resolve, reject) => {
      if (timeoutMs !== undefined && (!Number.isInteger(timeoutMs) || timeoutMs <= 0 || timeoutMs > 2147483647))
        throw new RangeError("timeoutMs must be a positive integer up to 2147483647");
      if (signal?.aborted) { reject(signal.reason); return; }
      let timer: ReturnType<typeof setTimeout> | undefined;
      let abortListener: ReturnType<typeof addAbortListener> | undefined;
      const cleanup = () => {
        unsubscribe();
        if (timer !== undefined) clearTimeout(timer);
        abortListener?.[Symbol.dispose]();
      };
      const onAbort = () => { cleanup(); reject(signal?.reason); };
      const unsubscribe = bus.subscribeOnce(topic, (event) => { cleanup(); resolve(event); });
      if (signal) abortListener = addAbortListener(signal, onAbort);
      if (timeoutMs !== undefined) timer = setTimeout(() => {
        cleanup();
        reject(new DOMException("Timed out waiting for event", "TimeoutError"));
      }, timeoutMs);
    }),
    subscribeOnce: (topic, listener) => {
      const subscribed = (event: PaymentEvent) => listener(event);
      emitter.once(topic, subscribed);
      return () => { emitter.off(topic, subscribed); };
    },
    subscribeWhere: (topic, predicate, listener) => {
      const subscribed = (event: PaymentEvent) => { if (predicate(event)) listener(event); };
      emitter.on(topic, subscribed);
      return () => { emitter.off(topic, subscribed); };
    },
    listenerCount: (topic) => emitter.listenerCount(topic),
    activeTopics: () => (["payment.created", "payment.failed"] as Topic[])
      .filter((topic) => emitter.listenerCount(topic) > 0),
    clear: (topic) => {
      for (const [unsubscribe, subscribedTopic] of abortable)
        if (topic === undefined || topic === subscribedTopic) unsubscribe();
      if (topic === undefined) emitter.removeAllListeners();
      else emitter.removeAllListeners(topic);
    },
  };
  return bus;
}
