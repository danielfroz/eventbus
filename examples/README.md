# Examples

Runnable examples for `@danielfroz/eventbus`. They import the library by relative
path (`../src/...`) so they type-check against the local code; a published
consumer would import from `@danielfroz/eventbus` and `@danielfroz/eventbus/<backend>`.

| File | Shows |
|------|-------|
| [`sloth-slog.ts`](./sloth-slog.ts) | Full integration with [`@danielfroz/sloth`](https://jsr.io/@danielfroz/sloth) (DI container, `DI.inject`) and [`@danielfroz/slog`](https://jsr.io/@danielfroz/slog) (structured logging): the `@Consumes(type)` decorator (no `type` field), `consumers()` discovery, lazy Promise-based handler factories, and a fully wired `bus.init({ ... })` on the Iggy backend. |

## Running

Start the local brokers from the repo root, then run an example:

```bash
docker compose up -d                       # redis 6379 / nats 4222 / iggy 8090
deno run -A examples/sloth-slog.ts
docker compose down
```

The Iggy example authenticates with `iggy/iggy` by default (matching
`docker-compose.yml`); override with `IGGY_HOST` / `IGGY_PASSWORD` env vars.

## Type-checking

Examples are excluded from the published package and from `test.sh`. Type-check
them explicitly:

```bash
deno check examples/*.ts
```
