import { EventEmitter } from "node:events";
import type { EventBus, PaymentEvent, Topic } from "../pkg/index.js";
export function createEventBus(): EventBus {
  const emitter = new EventEmitter<Record<Topic, [PaymentEvent]>>();
  const abortable = new Map<() => void, Topic>();
  return {
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
      const unsubscribe = () => {
        emitter.off(topic, subscribed);
        signal.removeEventListener("abort", unsubscribe);
        abortable.delete(unsubscribe);
      };
      emitter.on(topic, subscribed);
      signal.addEventListener("abort", unsubscribe, { once: true });
      abortable.set(unsubscribe, topic);
      return unsubscribe;
    },
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
}
