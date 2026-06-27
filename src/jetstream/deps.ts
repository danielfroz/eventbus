// Third-party dependencies for the Jetstream adapter. Kept here so only
// consumers of `@danielfroz/eventbus/jetstream` ever resolve the NATS clients.
export { ConsoleLog } from '@danielfroz/slog'
export * as NATSJ from 'jsr:@nats-io/jetstream@3.1.0'
export * as NATS from 'jsr:@nats-io/nats-core@3.1.0'
export * as NATSC from 'jsr:@nats-io/transport-deno@3.1.0'
