# EventBus SDK

Deno/TypeScript library that gives ACTT services an opinionated, event-driven
messaging abstraction over a pluggable broker. Published to JSR as
`@danielfroz/eventbus` (see `deno.json`); CI publishes on push to `main` via
`npx jsr publish`.

> ⚠️ Pre-1.0 — interfaces and contracts may still change.

---

## Architecture

A small, broker-agnostic core (`src/`) plus one self-contained adapter per
broker, each exported as a subpath:

| Export | File | Backend |
|--------|------|---------|
| `@danielfroz/eventbus` | `src/mod.ts` | Core types only (no broker) |
| `@danielfroz/eventbus/redis` | `src/redis/mod.ts` | Redis Streams (`jsr:@db/redis`) |
| `@danielfroz/eventbus/jetstream` | `src/jetstream/mod.ts` | NATS Jetstream (`jsr:@nats-io/*`) |
| `@danielfroz/eventbus/iggy` | `src/iggy/mod.ts` | Apache Iggy (`npm:apache-iggy`) |

### Core contracts (`src/`)

- **`EventBus`** (`EventBus.ts`) — the interface every adapter implements:
  `init(config)`, `publish(event)`, `destroy()`.
- **`Event`** (`Event.ts`) — minimal envelope: `type`, `id`, `sid`, `author?`,
  `ts?`. Domain payload fields are added by extending `Event`.
- **`EventHandler<T>`** (`EventHandler.ts`) — `{ type?, handle(event) }`. `type`
  is **optional** so a `@Consumes(type)`-decorated class (which stamps `type` on
  the prototype) satisfies the interface without a field; the adapter loop
  rejects a handler that ends up with no `type`.
- **`@Consumes(type)` + `consumers()`** (`Consumes.ts`) — a TC39 class decorator
  (no `experimentalDecorators`, no `reflect-metadata`) that records the handler
  class in a module-level registry **and** stamps the event `type` onto the class
  prototype. `consumers()` reads the registry back as constructors; `type` is
  **required** and validated non-empty. The core stays DI-free — the app maps
  discovered constructors to factories with its own container:
  `handlers: consumers().map((C) => () => container.resolve(C))`. Mirrors sloth's
  `@Route`/`@Provide`.
- **`Config`** (`Config.ts`) — runtime config passed to `init()`. Key fields:
  - `producer` — this service's name; **also the consumer-group name** and the
    name of the stream/topic this service publishes to.
  - `instance` — unique per-process id (defaults to `producer.<epoch>`).
  - `consuming` — list of **other producers'** streams to subscribe to. Omit it
    for a publisher-only bus.
  - `handlers` — `EventHandler` instances, **or** zero-arg factories returning an
    instance **or a `Promise`** (`TypeOrPredicate<T> = T | (() => T | Promise<T>)`).
    The adapter loop resolves each with `await hop()` at `init()`, so async/lazy
    factories work — e.g. `() => import('./H.ts').then((m) => container.resolve(m.H))`.
  - `encode`/`decode` — optional custom (de)serialization; default is JSON.
  - `error` — required; receives transport/`NetworkError`s.
  - `errorHandler` — required **when consuming**; receives `EventHandlerError`s
    (failed handler, malformed event) for DLQ-style handling.
- **Errors** (`Errors.ts`) — `EventBusError` base; `ArgumentError`,
  `InitError`, `NetworkError`, `EventHandlerError`.

### The shared adapter shape

All three adapters follow the same lifecycle, so read one to understand the
others:

1. `init()` validates `Config`, connects, and ensures the **producer** topology
   exists (so `publish` works even with no consumers).
2. If `config.consuming` is set, it requires `errorHandler` + `handlers`,
   resolves each handler entry with `await hop()` (supports async/lazy factories),
   rejects any with no `type` (`InitError`), registers them into a
   `Map<type, handler>`, ensures each consuming source, then starts a
   `setInterval` poll loop guarded by a `running` flag (prevents overlapping
   cycles). The resolve loop is **identical across all three adapters** — keep it
   in sync when editing.
3. Per polled message: decode → validate (`type`/`sid`/`id`/`ts`) → dispatch to
   the handler by `type` → **acknowledge** (Redis `xack` / Jetstream `msg.ack`
   / Iggy `offset.store`). Validation failures and handler throws are routed to
   `errorHandler`, then acked anyway — mirroring a "finally ack" so a poison
   message never blocks the stream.
4. `destroy()` clears the interval, waits for the in-flight cycle to drain
   (`running`), then closes connections.

---

## Concept mapping across brokers

| Concept | Redis | Jetstream | Iggy |
|---------|-------|-----------|------|
| Publish target | stream key = `producer` | stream `producer`, subject `producer` | stream `producer`, topic `events` |
| Subscription unit | consumer group = `producer` on each consuming key | durable consumer = `producer` on each consuming stream | consumer group = `producer` on each consuming stream's `events` topic |
| Ack | `xack` | `msg.ack()` | `offset.store` (autocommit off) |

Iggy adds a stream→topic→partition hierarchy the others lack. We model **one
service = one stream**, funnelling all of that service's events through a single
topic named `events` (`TOPIC` in `src/iggy/mod.ts`), single partition by default
(preserves global ordering, like the other two backends).

---

## Apache Iggy adapter — critical knowledge

The Iggy adapter (`src/iggy/mod.ts`) runs the `npm:apache-iggy` client under
Deno. Several non-obvious behaviours were found the hard way and are baked into
the implementation — **do not "simplify" them away**:

1. **Never call zero-payload commands (`system.ping`, `system.getStats`).** The
   client serializes commands with `Buffer.fill(payload, 8)`; for an empty
   payload Deno's `node:Buffer` polyfill throws
   `ERR_INVALID_ARG_VALUE: ... Received <Buffer >`. Node tolerates it; Deno does
   not. Every command we use (login, stream/topic/group create+get, send, poll,
   offset.store) carries a payload and is fine. Connectivity/credentials are
   fail-fast-checked by the first real command (`stream.get`) inside `init`,
   not by `ping`.

2. **`get*` returns `null`/`undefined` for a missing entity — it does not
   throw.** Existence checks (`_exists`) must inspect the return value, not rely
   on `try/catch`. Getting this wrong silently skips creation.

3. **The server auto-assigns stream/topic/group IDs.** `create*` takes only a
   `name` (+ topic/group parents); reference everything by name afterwards
   (`Id = number | string`). Do not invent numeric IDs.

4. **At-least-once via manual offset commit.** Poll uses
   `autocommit: false` + `PollingStrategy.Next` + `Consumer.Group(producer)`,
   and stores the offset (`partitionId: null` for groups) **after** the handler
   runs. A crash mid-handle redelivers on the next poll. Verified: restarting a
   bus on the same group does not redeliver already-committed messages.

5. **Idempotent topology.** `init` creates the producer's own stream+topic, and
   for each consuming source creates the stream+topic+group then joins — so a
   consumer works even before the remote producer has started. "Already exists"
   errors are tolerated (`_alreadyExists`).

### Iggy config (`EventBusIggyConfig`)

`host` (required), `port` (8090), `transport` (`TCP`), `username`/`password`
(`iggy`/`iggy`), `partitions` (1), `batch` (100 msgs/poll), `interval` (500ms),
`trace`.

---

## Commands

```bash
sh ./test.sh                         # validate the codebase: check -> lint -> test
docker compose up -d && sh ./test.sh # full run incl. broker integration tests
docker compose down                  # tear down brokers

deno check src/iggy/mod.ts           # type-check a single module while iterating
```

**`sh ./test.sh` is the single validation entry point** (type-check → lint →
`deno test -A src/`). Run it before committing. There is no `compile.sh`; tests
import `asserts` (see `deno.json` import map).

### Tests

- **Unit:** `Errors_test.ts`, `EventHandler_test.ts`, `Consumes_test.ts`
  (decorator discovery + prototype stamping) — no broker needed.
- **Integration:** `src/mod_test.ts` runs the same publish→consume round-trip
  against all three backends (the redis case passes its handler as an **async
  factory** to exercise the `await hop()` path). It needs the brokers from the
  local `docker-compose.yml` (redis 6379 / nats 4222 / iggy 8090, no UI,
  ephemeral — iggy root password is `iggy/iggy` there). Each test **probes its
  port and is skipped when the broker is down**, so `sh ./test.sh` stays green
  without docker. Each run uses a unique alphanumeric producer name to avoid
  stream/consumer-group state collisions.
- **Examples:** `examples/sloth-slog.ts` (excluded from `test.sh`/publish) — a
  runnable sloth+slog integration; type-check with `deno check examples/*.ts`.

> **Dependency note:** each adapter's third-party imports live in a per-adapter
> `deps.ts` (`src/iggy/deps.ts`, `src/jetstream/deps.ts`, `src/redis/deps.ts`)
> that re-exports the broker client; the adapter's `mod.ts` imports from
> `./deps.ts`. These broker clients are intentionally **not** in
> `deno.json#imports`, so the core package and the other adapters carry no
> dependency on them — verified with `deno info` (the per-export graphs are
> disjoint). Two invariants keep this true: **(1)** the root `mod.ts` never
> imports an adapter (only core types/errors), and **(2)** `deps.ts` is
> **per-adapter, never a shared root `src/deps.ts`** (a shared one would merge
> all broker clients into one graph and break isolation). Bonus: `export … from
> 'npm:'/'jsr:'` re-exports are not flagged by the `no-import-prefix` lint
> (only `import` is), so this layout is fully lint-clean.

---

## Migrating 0.1.5 / 0.1.6 → 0.2.0

**0.2.0 is backward compatible** — bumping the dependency alone needs no code
changes. The only interface change is additive: `EventHandler.type` is now
optional (`type?: string`). Existing handlers (with a `type` field) and existing
`handlers: [instance]` / `handlers: [() => instance]` wiring keep working. The
items below are **opt-in**.

### New: async / lazy handler factories

`Config.handlers` now also accepts `() => Promise<EventHandler>`, awaited at
`init()`. Use it to lazily import a handler module (code splitting) or resolve it
asynchronously:

```ts
handlers: [
  () => import('@/handlers/events/order/Created.ts')
        .then((m) => container.resolve(m.Created)),
]
```

### New: `@Consumes()` auto-discovery (with `@danielfroz/sloth`)

Replace hand-maintained handler lists with decorator discovery. (Paths like
`@/handlers/...` below are **consumer-service** examples, e.g. the `order` /
`organization` services.)

**Before (0.1.x) — manual list, `type` field on each handler:**
```ts
import * as h from '@/handlers/events/index.ts'
// handler:
export class Created implements EventHandler<Events.Organization.Created> {
  type = Events.Organization.CREATED
  constructor(private readonly ro = DI.inject(Types.Repos.Organization)) {}
  async handle(event: Events.Organization.Created) { /* ... */ }
}
// wiring:
const handlers = [
  container.resolve(h.Organization.Created),
  container.resolve(h.Bill.Created),
]
await bus.init({ producer: 'organization', consuming: [/* ... */], handlers, /* ... */ })
```

**After (0.2.0) — `@Consumes`, no `type` field, `consumers()` discovery:**
```ts
import '@/handlers/events/index.ts'                 // side-effect import (see below)
import { Consumes, consumers } from '@danielfroz/eventbus'
// handler:
@Consumes(Events.Organization.CREATED)
export class Created implements EventHandler<Events.Organization.Created> {
  constructor(private readonly ro = DI.inject(Types.Repos.Organization)) {}
  async handle(event: Events.Organization.Created) { /* ... */ }
}
// wiring:
await bus.init({
  producer: 'organization',
  consuming: [/* ... */],
  handlers: consumers().map((C) => () => container.resolve(C)),
  /* ... */
})
```

**Critical — side-effect import:** `consumers()` only returns classes whose
**modules have been evaluated** (the `@Consumes` decorator runs at import time).
The manual approach imported handlers implicitly via `h.Organization.Created`;
with discovery you no longer reference `h.*`, so keep a **side-effect import** of
the handlers barrel (`import '@/handlers/events/index.ts'`) before `init()`.
Forgetting it ⇒ empty `consumers()` ⇒ no handlers registered.

### Steps

1. Bump `@danielfroz/eventbus` to `0.2.0` in **both** `deno.json` and
   `deno.local.json`.
2. (Opt-in, recommended with sloth) For each event handler: add
   `@Consumes(Events.X.TYPE)` and **remove the `type = …` field**.
3. Replace the manual `container.resolve(h.X)` list with
   `handlers: consumers().map((C) => () => container.resolve(C))`.
4. Add/keep a side-effect import of the handlers barrel so the decorators run.
5. `sh ./compile.sh` (service). `init()` throws `handler.type required` if a
   handler ends up with neither a `@Consumes` type nor a `type` field.

**Not using sloth?** Skip the decorator — the instance/factory `handlers` array
is unchanged. `@Consumes`/`consumers()` carry no DI-container dependency, but the
`consumers().map(C => () => container.resolve(C))` wiring assumes a container
(sloth's `container.resolve`, or your own).

---

## Conventions

- 2-space indent, no semicolons, single quotes, `if(...)` with no space — match
  the surrounding adapter style.
- Validate every public-method argument up front and throw `ArgumentError`.
- Wrap transport failures in `NetworkError` and route through `config.error`;
  never let a raw client error escape `publish`/the poll loop.
- Keep each adapter self-contained: a backend's quirks live in its own file.
- When adding a broker: implement `EventBus`, add the subpath to
  `deno.json#exports`, and follow the shared adapter shape above.
