export type Topic = "payment.created" | "payment.failed";
export type PaymentEvent = { id: string };
export interface EventBus {
  publish(topic: Topic, event: PaymentEvent): void;
  subscribe(topic: Topic, listener: (event: PaymentEvent) => void): () => void;
}
export { createEventBus } from "../internal/event-bus.js";
