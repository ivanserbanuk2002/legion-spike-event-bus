import { EventEmitter } from "node:events";
import type { EventBus, PaymentEvent, Topic } from "../pkg/index.js";
export function createEventBus(): EventBus {
  const emitter = new EventEmitter<Record<Topic, [PaymentEvent]>>();
  return {
    publish: (topic, event) => { emitter.emit(topic, event); },
    subscribe: (topic, listener) => {
      const subscribed = (event: PaymentEvent) => listener(event);
      emitter.on(topic, subscribed);
      return () => { emitter.off(topic, subscribed); };
    },
  };
}
