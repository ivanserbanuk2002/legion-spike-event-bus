export type Topic = "payment.created" | "payment.failed";
export type PaymentEvent = { id: string };
export interface EventBus {
  publish(topic: Topic, event: PaymentEvent): void;
  publishMany(topic: Topic, events: readonly PaymentEvent[]): void;
  subscribe(topic: Topic, listener: (event: PaymentEvent) => void): () => void;
  subscribeOnce(topic: Topic, listener: (event: PaymentEvent) => void): () => void;
  subscribeWhere(topic: Topic, predicate: (event: PaymentEvent) => boolean,
    listener: (event: PaymentEvent) => void): () => void;
  listenerCount(topic: Topic): number;
  clear(topic?: Topic): void;
}
export { createEventBus } from "../internal/event-bus.js";
