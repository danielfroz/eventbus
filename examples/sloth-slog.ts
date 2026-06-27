// deno-lint-ignore-file no-import-prefix
/**
 * EventBus fully integrated with @danielfroz/sloth (DI) and @danielfroz/slog
 * (structured logging) — the pattern used by real ACTT services.
 *
 * Shows:
 *   - JsonLog (slog) registered as a DI value provider;
 *   - DI.Type tokens + a repository registered with the sloth container;
 *   - an EventHandler that uses DI.inject(...) and the @Consumes(type) decorator
 *     (no `type` field — the decorator supplies it);
 *   - discovery + lazy resolution: consumers().map(C => () => container.resolve(C));
 *   - a lazy, Promise-based factory via dynamic import();
 *   - bus.init({ ... }) with decode/encode/error/errorHandler/log.
 *
 * Run against the local brokers (see ../docker-compose.yml):
 *   docker compose up -d
 *   deno run -A examples/sloth-slog.ts
 *
 * NOTE: this file imports the library by relative path (../src/...) so it
 * type-checks against the local code. As a published consumer you would instead:
 *   import { Consumes, consumers, type EventHandler } from '@danielfroz/eventbus'
 *   import { EventBusIggy } from '@danielfroz/eventbus/iggy'
 */

import { ConsoleLog, type Log } from '@danielfroz/slog'
import { container, DI } from 'jsr:@danielfroz/sloth@0.3.1'
import {
  Consumes,
  consumers,
  type Event,
  type EventBus,
  type EventHandler,
  type EventHandlerError,
} from '../src/mod.ts'
import { EventBusIggy } from '../src/iggy/mod.ts'

// --- domain event ----------------------------------------------------------

const ORDER_CREATED = 'Order.Created'
interface OrderCreated extends Event {
  type: typeof ORDER_CREATED
  order: { id: string; total: number }
}

// --- a dependency (repository) ---------------------------------------------

class OrderRepo {
  private readonly db = new Map<string, number>()
  save(id: string, total: number): Promise<void> {
    this.db.set(id, total)
    return Promise.resolve()
  }
}

// --- DI tokens -------------------------------------------------------------

const Types = {
  Log: DI.Type<Log>('Log'),
  Orders: DI.Type<OrderRepo>('Repos.Orders'),
}

// --- handler: decorated, no `type` field, deps via DI.inject ----------------

@Consumes(ORDER_CREATED)
export class OrderCreatedHandler implements EventHandler<OrderCreated> {
  constructor(
    private readonly repo = DI.inject(Types.Orders),
    private readonly log = DI.inject(Types.Log),
  ) {}

  async handle(event: OrderCreated): Promise<void> {
    const log = this.log.child({ mod: 'order.created', event: event.id })
    await this.repo.save(event.order.id, event.order.total)
    log.info({ msg: 'order persisted', order: event.order.id, total: event.order.total })
  }
}

// --- bootstrap -------------------------------------------------------------

async function main() {
  // 1. logging (slog) registered as a DI value provider.
  //    On slog 0.3.x production services use `new JsonLog({ level, init: { service } })`.
  const log = new ConsoleLog({ init: { service: 'orders' } })
  container.register<Log>(Types.Log, { useValue: log })

  // 2. dependencies registered with the sloth container (lazy by default)
  container.register(Types.Orders, { useClass: OrderRepo })

  // 3. event bus (swap EventBusIggy for EventBusRedis / EventBusJetstream freely)
  const bus = new EventBusIggy({
    host: Deno.env.get('IGGY_HOST') ?? '127.0.0.1',
    port: 8090,
    username: 'iggy',
    password: Deno.env.get('IGGY_PASSWORD') ?? 'iggy',
    interval: 300,
  })
  container.register<EventBus>(DI.Type<EventBus>('EventBus'), { useValue: bus })

  // 4. handlers: discovered via @Consumes + resolved lazily through the container.
  //    Each entry is a Promise-capable factory, so you can also lazily import a
  //    handler module on demand, e.g.:
  //      () => import('./handlers/BillCreated.ts').then((m) => container.resolve(m.BillCreated))
  const handlers = consumers().map((C) => () => container.resolve(C))

  await bus.init({
    producer: 'orders',
    consuming: ['orders'],
    handlers,
    decode: (raw) => Promise.resolve(JSON.parse(raw) as Event),
    encode: (event) => Promise.resolve(JSON.stringify(event)),
    error: (err) => {
      log.error({ msg: 'eventbus transport error', err: err.message })
      return Promise.resolve()
    },
    errorHandler: (err: EventHandlerError) => {
      log.error({ msg: 'handler failed', type: err.event?.type, err: err.message })
      return Promise.resolve()
    },
    log,
  })
  log.info({ msg: 'eventbus initialized' })

  // 5. publish one event and let the handler pick it up
  const event: OrderCreated = {
    type: ORDER_CREATED,
    id: crypto.randomUUID(),
    sid: crypto.randomUUID(),
    author: 'orders',
    order: { id: 'o-1', total: 4200 },
  }
  await bus.publish(event)
  log.info({ msg: 'published', order: event.order.id })

  await new Promise((r) => setTimeout(r, 3000))
  await bus.destroy()
}

if (import.meta.main) {
  await main()
}
