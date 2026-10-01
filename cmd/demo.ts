import { createEventBus } from "../pkg/index.js";
const bus = createEventBus();
bus.subscribe("payment.created", ({ id }) => console.log(`created: ${id}`));
bus.subscribe("payment.failed", ({ id }) => console.log(`failed: ${id}`));
for (const topic of ["payment.created", "payment.failed"] as const) bus.publish(topic, { id: "demo-1" });
