# EventBus

A small, opinionated **event-driven messaging library for Deno/TypeScript** with
a single broker-agnostic API and pluggable backends for
**[Apache Iggy](https://iggy.apache.org)**, **[Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/)**,
and **[NATS Jetstream](https://docs.nats.io/nats-concepts/jetstream)**.

Write your producers and consumers once against the `EventBus` interface, then
choose (or switch) the broker by swapping a single import. Each backend lives
behind its own subpath export, so an app that only uses Redis never downloads
the Iggy or NATS client, and vice-versa.

```ts
import { EventBusRedis } from '@danielfroz/eventbus/redis'
// or  @danielfroz/eventbus/jetstream
// or  @danielfroz/eventbus/iggy
```

> ⚠️ **Status:** pre-1.0. The API is small and stable in practice, but contracts
> may still change before `1.0`.

---

## Features

- **One API, many brokers** — `init` / `publish` / `destroy` + `EventHandler`s.
- **Backends:** Apache Iggy, Redis Streams, NATS Jetstream.
- **At-least-once delivery** — messages are acknowledged only after the handler
  runs; a crash mid-handle redelivers.
- **Consumer groups** — each service consumes as a named group, so events fan
  out across services and load-balance across instances.
- **Per-backend dependency isolation** — subpath exports keep each broker
  client out of the others' dependency graph.
- **Pluggable (de)serialization** — defaults to JSON; bring your own
  `encode`/`decode` (e.g. for schema validation or envelope metadata).
- **Structured error routing** — transport errors and handler failures are
  delivered to dedicated callbacks for logging / dead-letter handling.

---

## Installation

This package is published on [JSR](https://jsr.io).

```bash
# Deno
deno add jsr:@danielfroz/eventbus
```

Or import directly with a versioned specifier — no install step needed:

```ts
import type { Event, EventHandler } from 'jsr:@danielfroz/eventbus'
import { EventBusIggy } from 'jsr:@danielfroz/eventbus/iggy'
```

### Exports

| Subpath | Import | Backend / contents |
|---------|--------|--------------------|
| `.` | `@danielfroz/eventbus` | Core types: `EventBus`, `Event`, `EventHandler`, `Config`, errors |
| `./redis` | `@danielfroz/eventbus/redis` | `EventBusRedis` (Redis Streams) |
| `./jetstream` | `@danielfroz/eventbus/jetstream` | `EventBusJetstream` (NATS Jetstream) |
| `./iggy` | `@danielfroz/eventbus/iggy` | `EventBusIggy` (Apache Iggy) |

---

## Core concepts

| Concept | Description |
|---------|-------------|
| **Event** | The message envelope: `{ type, id, sid, author?, ts? }`. Extend it with your own payload fields. |
| **EventHandler** | `{ type, handle(event) }` — processes one event type. |
| **EventBus** | The broker abstraction: `init(config)`, `publish(event)`, `destroy()`. |
| **producer** | Your service's name. It is also the **consumer-group name** and the name of the stream this service publishes to. |
| **consuming** | The list of **other producers** whose streams you want to receive. Omit it for a publisher-only bus. |

**Delivery model.** A service publishes to its own stream (`producer`) and
subscribes to others by listing them in `consuming`. Every event is decoded,
dispatched to the handler registered for its `type`, then acknowledged. Events
with no matching handler are acknowledged and ignored. Handler failures and
malformed events are routed to `errorHandler` (then acknowledged so a poison
message never blocks the stream).

---

## Quick start

### 1. Define an event and a handler

```ts
import type { Event, EventHandler } from '@danielfroz/eventbus'

// A domain event — extend the Event envelope.
export interface UserCreated extends Event {
  type: 'User.Created'
  user: { id: string; name: string }
}

export class UserCreatedHandler implements EventHandler<UserCreated> {
  type = 'User.Created'

  async handle(event: UserCreated): Promise<void> {
    console.log('user created:', event.user.id, event.user.name)
    // ...do work (idempotently — delivery is at-least-once)
  }
}
```

### 2. Publish

```ts
import { EventBusRedis } from '@danielfroz/eventbus/redis'

const bus = new EventBusRedis({ uri: 'redis://127.0.0.1:6379' })

await bus.init({
  producer: 'identity',                       // this service's name
  error: async (err) => console.error(err),   // transport errors
})

await bus.publish({
  type: 'User.Created',
  id: crypto.randomUUID(),                     // unique event id
  sid: crypto.randomUUID(),                    // saga / correlation id
  author: 'identity',
  user: { id: 'u-1', name: 'Ada' },
} satisfies UserCreated)

await bus.destroy()
```

### 3. Consume

```ts
import { EventBusRedis } from '@danielfroz/eventbus/redis'

const bus = new EventBusRedis({ uri: 'redis://127.0.0.1:6379' })

await bus.init({
  producer: 'billing',                  // consumer-group name
  consuming: ['identity'],              // subscribe to the 'identity' producer
  handlers: [new UserCreatedHandler()],
  error: async (err) => console.error('transport:', err),
  errorHandler: async (err) => console.error('handler:', err.message, err.event),
})

// ...the bus now polls in the background. Keep the process alive, then:
// await bus.destroy()
```

> A bus can both publish and consume — set `producer`, `consuming`, and
> `handlers` together.

---

## Backends

All three implement the same `EventBus` interface; only construction differs.

### Apache Iggy

```ts
import { EventBusIggy } from '@danielfroz/eventbus/iggy'

const bus = new EventBusIggy({
  uri: 'tcp://iggy:iggy@127.0.0.1:8090',  // tls:// selects TLS; defaults: 8090, iggy/iggy
  // partitions: 1,      // partitions for this producer's topic
  // expiry: 86400,      // topic message expiry in seconds (default 1 day; 0 = never)
  // batch: 100,         // messages fetched per poll
  // interval: 500,      // poll interval (ms)
})

await bus.init({
  producer: 'orders',
  consuming: ['identity', 'catalog'],
  handlers: [new UserCreatedHandler()],
  error: async (e) => console.error(e),
  errorHandler: async (e) => console.error(e),
})
```

Iggy organizes data as *stream → topic → partition*. This library maps one
**service = one Iggy stream**, funnels all of that service's events through a
single `events` topic, and consumes via a consumer group named after the
subscribing service.

#### Message expiry

`expiry` sets how long the Iggy server retains messages on the topic, in
**seconds**. Messages older than `expiry` are deleted by the server, bounding
disk usage and replay history.

- **Default `86400` (1 day)** — chosen to mirror the NATS Jetstream adapter's
  1-day `max_age`, so the two persistent backends behave consistently.
- **`0` = never expire** — keep messages forever (until other retention limits
  apply).
- The value is applied as the topic's expiry on every topic the bus ensures at
  `init()` (its own producer topic and any consuming topics it creates). It is
  set **at topic-creation time**; changing `expiry` later does not retroactively
  update an existing topic.

```ts
const bus = new EventBusIggy({
  uri: 'tcp://iggy:iggy@127.0.0.1:8090',
  expiry: 7 * 24 * 3600,   // keep messages for 7 days
})
```

> Note: the underlying `apache-iggy` client (0.8.0) misreports `messageExpiry`
> when reading a topic back, so verify configured expiry via the Iggy HTTP API
> (`GET /streams/{stream}/topics/{topic}` → `message_expiry`, in microseconds),
> not the TCP client. The write path is correct (`expiry: 60` ⇒
> `message_expiry: 60000000`).

### Redis Streams

```ts
import { EventBusRedis } from '@danielfroz/eventbus/redis'

const bus = new EventBusRedis({
  uri: 'redis://127.0.0.1:6379',          // redis://[user:pass@]host[:port]; scheme ignored
})

await bus.init({
  producer: 'orders',
  consuming: ['identity'],
  handlers: [new UserCreatedHandler()],
  error: async (e) => console.error(e),
  errorHandler: async (e) => console.error(e),
})
```

Each producer is a Redis Stream key; consumers read via `XREADGROUP` consumer
groups and `XACK` after handling.

### NATS Jetstream

```ts
import { EventBusJetstream } from '@danielfroz/eventbus/jetstream'

const bus = new EventBusJetstream({
  uri: 'nats://127.0.0.1:4222',  // or just '127.0.0.1:4222'; scheme ignored, port defaults to 4222
})

await bus.init({
  producer: 'orders',
  consuming: ['identity'],
  handlers: [new UserCreatedHandler()],
  error: async (e) => console.error(e),
  errorHandler: async (e) => console.error(e),
})
```

Each producer is a Jetstream stream; consumers use a durable pull consumer named
after the subscribing service and `ack()` after handling.

---

## Handler registration

`Config.handlers` accepts three forms, mixable in one array:

```ts
handlers: [
  new OrderCreatedHandler(),                       // an instance
  () => new BillCreatedHandler(),                  // a sync factory (lazy)
  () => import('./handlers/Refund.ts')             // an async factory (lazy)
        .then((m) => new m.RefundHandler()),
]
```

### Lazy / async factories

A factory may return a `Promise`, which the bus `await`s during `init()`. This
lets you **defer loading a handler's module** with a dynamic `import()` (code
splitting) or resolve it through an async path — handy with a DI container:

```ts
import { container } from '@danielfroz/sloth'

handlers: [
  () => import('./handlers/OrderCreated.ts')
        .then((m) => container.resolve(m.OrderCreated)),
]
```

Factories resolve at `init()` (the handler map is built before consuming starts);
instances and sync factories keep working unchanged.

### The `@Consumes()` decorator

Instead of listing handlers by hand, mark each with `@Consumes(type)` and let the
bus discover them. The decorator supplies the event type (so the class needs no
`type` field) and registers the class for `consumers()`:

```ts
import { EventHandler, Consumes, consumers } from '@danielfroz/eventbus'
import { container, DI } from '@danielfroz/sloth'

@Consumes(Events.Order.CREATED)
export class OrderCreated implements EventHandler<Events.Order.Created> {
  constructor(private readonly repo = DI.inject(Types.Repos.Order)) {}
  async handle(e: Events.Order.Created) { /* ... */ }
}

// wiring — discovery + lazy resolution via your container
await bus.init({
  producer: 'order',
  consuming: ['order'],
  handlers: consumers().map((C) => () => container.resolve(C)),
  error: async (e) => console.error(e),
  errorHandler: async (e) => console.error(e),
})
```

`@Consumes(type)` requires a non-empty `type`. It's a standard TC39 decorator
(no `experimentalDecorators`, no `reflect-metadata`), and the library stays
**dependency-free of any DI container** — you map the discovered constructors to
factories with your own container. `init()` rejects any handler that ends up with
no `type` (neither a `@Consumes` type nor a `type` field).

## Integration with sloth & slog

EventBus pairs naturally with [`@danielfroz/sloth`](https://jsr.io/@danielfroz/sloth)
(DI container, lazy `DI.inject`) and [`@danielfroz/slog`](https://jsr.io/@danielfroz/slog)
(structured logging via `log` in `Config`). A complete, runnable example —
`@Consumes` discovery, lazy factories, `DI.inject` dependencies, and a wired
`bus.init({ ..., log })` — lives in [`examples/sloth-slog.ts`](./examples/sloth-slog.ts).

## Custom serialization

By default events are (de)serialized as JSON. Provide `encode`/`decode` to add
an envelope, compress, encrypt, or validate against a schema:

```ts
await bus.init({
  producer: 'orders',
  encode: async (event) => JSON.stringify({ v: 1, event }),
  decode: async (raw) => JSON.parse(raw).event,
  error: async (e) => console.error(e),
})
```

---

## Configuration reference

### `Config` (passed to `init`)

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `producer` | `string` | yes | Service name — also the consumer-group and publish-stream name. |
| `instance` | `string` | no | Unique per-process id. Defaults to `producer.<epoch>`. |
| `consuming` | `string[]` | no | Producers to subscribe to. Omit for publisher-only. |
| `handlers` | `EventHandler[]` \| `(() => EventHandler)[]` | when consuming | Handlers, as instances or zero-arg factories. |
| `encode` | `(event) => Promise<string>` | no | Custom serializer (default JSON). |
| `decode` | `(raw: string) => Promise<Event>` | no | Custom deserializer (default JSON). |
| `error` | `(err: EventBusError) => Promise<void>` | yes | Transport / connection error callback. |
| `errorHandler` | `(err: EventHandlerError) => Promise<void>` | when consuming | Failed-handler / malformed-event callback (dead-letter hook). |
| `log` | `Log` (from `@danielfroz/slog`) | no | Structured logger for trace output. |

### Backend options

| Backend | Constructor options |
|---------|---------------------|
| `EventBusRedis` | `{ uri, trace? }` — `uri`: `redis://[user:pass@]host[:port]` (port → 6379) |
| `EventBusJetstream` | `{ uri }` — `uri`: `nats://host[:port]` or `host[:port]` (port → 4222) |
| `EventBusIggy` | `{ uri, partitions?=1, expiry?=86400, batch?=100, interval?=500, trace? }` — `uri`: `tcp://[user:pass@]host[:port]` (`tls://` for TLS; port → 8090, creds → iggy/iggy). `expiry`: topic message expiry in **seconds** (0 = never) |

All connection details (host, port, credentials, and — for Iggy — the transport
via `tcp://`/`tls://`) come from the `uri`. The scheme is ignored by Redis and
Jetstream. Missing parts fall back to the defaults above.

---

## Local development & testing

A `docker-compose.yml` is included to run all three brokers locally (no UI
services, ephemeral state):

```bash
docker compose up -d        # redis:6379, nats:4222, iggy:8090
sh ./test.sh                # type-check -> lint -> test
docker compose down
```

`sh ./test.sh` is the single validation entry point. The integration tests in
`src/mod_test.ts` exercise a full publish→consume round-trip on every backend;
each test **probes its broker port and is skipped when the broker is down**, so
the suite passes with or without docker.

---

## Design notes

- **At-least-once, ack-after-handle.** Handlers must be idempotent.
- **Per-service consumer group.** Multiple instances of a service share one
  group and load-balance; different services each get their own copy of an
  event.
- **Topology is created idempotently** on `init`, so a consumer can start before
  the producer it subscribes to.
- **Graceful shutdown.** `destroy()` stops polling, waits for the in-flight
  cycle to drain, and closes connections.

---

## Contributing

Issues and pull requests are welcome.

1. Install [Deno](https://deno.com).
2. `docker compose up -d` to start the brokers.
3. `sh ./test.sh` — must pass (type-check + lint + tests) before submitting.

Please keep each backend self-contained (its broker client belongs in that
adapter's `deps.ts`) and add a test to `src/mod_test.ts` for new behavior.