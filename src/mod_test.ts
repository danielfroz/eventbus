import { assert, assertEquals } from 'asserts'
import type { Event, EventBus, EventHandler } from './mod.ts'
import { EventBusIggy } from './iggy/mod.ts'
import { EventBusJetstream } from './jetstream/mod.ts'
import { EventBusRedis } from './redis/mod.ts'

/**
 * Integration tests for every EventBus backend. They require the brokers from
 * the sibling `docker-compose.yml` to be running:
 *
 *   docker compose up -d
 *   deno test -A
 *
 * Each test probes its broker's TCP port first and is skipped (ignored) when
 * the broker is unreachable, so the suite stays green without docker.
 */

const HOST = '127.0.0.1'

const probe = async (port: number): Promise<boolean> => {
  try {
    const conn = await Deno.connect({ hostname: HOST, port })
    conn.close()
    return true
  }
  catch {
    return false
  }
}

const REDIS = await probe(6379)
const NATS = await probe(4222)
const IGGY = await probe(8090)

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

interface Created extends Event {
  type: 'Test.Created'
  value: string
}
const TYPE = 'Test.Created'

/**
 * Each run uses a unique producer name so backends never collide on stream /
 * consumer-group state across tests or repeated runs. Kept alphanumeric so it
 * is valid as a Jetstream stream name / subject and an Iggy stream name.
 */
const uniqueProducer = (prefix: string) =>
  `${prefix}${crypto.randomUUID().replace(/-/g, '')}`

/**
 * Publishes one event on a bus that also consumes its own producer stream, and
 * asserts the registered handler receives it back. Verifies init -> publish ->
 * consume -> handle -> ack for any backend.
 */
const roundtrip = async (build: (handler: EventHandler<Event>) => Promise<EventBus>) => {
  const received: Created[] = []
  const handler: EventHandler<Created> = {
    type: TYPE,
    handle: (event) => {
      received.push(event)
      return Promise.resolve()
    },
  }

  const bus = await build(handler as EventHandler<Event>)
  try {
    const id = crypto.randomUUID()
    const event: Created = {
      type: TYPE,
      id,
      sid: crypto.randomUUID(),
      author: 'mod_test',
      value: 'hello',
    }
    await bus.publish(event)

    const deadline = Date.now() + 15_000
    while (Date.now() < deadline && !received.find((e) => e.id === id)) {
      await sleep(200)
    }

    const got = received.find((e) => e.id === id)
    assert(got, 'event was not received by the handler')
    assertEquals(got.type, TYPE)
    assertEquals(got.value, 'hello')
    assert(got.ts, 'publish should stamp event.ts when missing')
  }
  finally {
    await bus.destroy()
  }
}

Deno.test({
  name: 'redis: publish -> consume round-trip',
  ignore: !REDIS,
  // redis-server tears its sockets down on close async; allow leak detection off
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    await roundtrip(async (handler) => {
      const producer = uniqueProducer('redis')
      const bus = new EventBusRedis({ uri: `redis://${HOST}:6379` })
      await bus.init({
        producer,
        consuming: [producer],
        // async (Promise-returning) lazy factory — exercises the `await hop()`
        // resolution path (e.g. dynamic import / async DI) end-to-end
        handlers: [() => Promise.resolve(handler)],
        error: () => Promise.resolve(),
        errorHandler: () => Promise.resolve(),
      })
      return bus
    })
  },
})

Deno.test({
  name: 'jetstream: publish -> consume round-trip',
  ignore: !NATS,
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    await roundtrip(async (handler) => {
      const producer = uniqueProducer('js')
      const bus = new EventBusJetstream({ uri: `nats://${HOST}:4222` })
      await bus.init({
        producer,
        consuming: [producer],
        handlers: [handler],
        error: () => Promise.resolve(),
        errorHandler: () => Promise.resolve(),
      })
      return bus
    })
  },
})

Deno.test({
  name: 'iggy: publish -> consume round-trip',
  ignore: !IGGY,
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    await roundtrip(async (handler) => {
      const producer = uniqueProducer('iggy')
      const bus = new EventBusIggy({
        uri: `tcp://iggy:iggy@${HOST}:8090`,
        interval: 300,
      })
      await bus.init({
        producer,
        consuming: [producer],
        handlers: [handler],
        error: () => Promise.resolve(),
        errorHandler: () => Promise.resolve(),
      })
      return bus
    })
  },
})
