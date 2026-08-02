import { assert, assertEquals } from 'asserts'
import type { Event, EventHandler, EventHandlerError } from '../mod.ts'
import { NetworkError } from '../mod.ts'
import { EventBusIggy } from './mod.ts'
import { Client, Partitioning } from './deps.ts'

/**
 * Regression tests for the _poll crash-proofing in ./mod.ts — a malformed
 * payload, a throwing handler, or a throwing errorHandler must never escape
 * the poll loop (an unhandled rejection there kills the whole Deno process;
 * see the CLAUDE.md "finally ack, poison message never blocks the stream"
 * contract). Requires the local docker-compose Iggy broker; skipped when
 * unreachable (mirrors ../mod_test.ts).
 */

const HOST = '127.0.0.1'
const PORT = 8090
// mirrors the private TOPIC constant in ./mod.ts (one topic per producer stream)
const TOPIC = 'events'

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

const IGGY = await probe(PORT)

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms))

const waitFor = async (deadlineMs: number, cond: () => boolean): Promise<void> => {
  const deadline = Date.now() + deadlineMs
  while (Date.now() < deadline && !cond()) {
    await sleep(200)
  }
}

const uniqueProducer = (prefix: string) =>
  `${prefix}${crypto.randomUUID().replace(/-/g, '')}`

interface Created extends Event {
  type: 'Test.Created'
  value: string
}
const TYPE = 'Test.Created'

/**
 * Publishes a raw payload directly onto `producer`'s topic, bypassing
 * EventBus.publish() (which always JSON-encodes a well-formed Event) — used
 * to simulate a poison (non-JSON) message arriving on the wire.
 */
const publishRaw = async (producer: string, payload: string): Promise<void> => {
  const client = new Client({
    transport: 'TCP',
    // deno-lint-ignore no-explicit-any
    options: { host: HOST, port: PORT } as any,
    credentials: { username: 'iggy', password: 'iggy' },
  })
  try {
    await client.message.send({
      streamId: producer,
      topicId: TOPIC,
      messages: [{ payload }],
      partition: Partitioning.Balanced,
    })
  }
  finally {
    await client.destroy()
  }
}

Deno.test({
  name: 'iggy: a poison (non-JSON) message is routed to errorHandler and does not block the stream',
  ignore: !IGGY,
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const producer = uniqueProducer('iggypoison')
    const errors: EventHandlerError[] = []
    const received: Created[] = []

    const handler: EventHandler<Created> = {
      type: TYPE,
      handle: (event) => {
        received.push(event)
        return Promise.resolve()
      },
    }

    const bus = new EventBusIggy({ uri: `tcp://iggy:iggy@${HOST}:8090`, interval: 300 })
    await bus.init({
      producer,
      consuming: [producer],
      handlers: [handler as EventHandler<Event>],
      error: () => Promise.resolve(),
      errorHandler: (err) => {
        errors.push(err)
        return Promise.resolve()
      },
    })

    try {
      // 1. a malformed payload lands on the topic first
      await publishRaw(producer, 'not-json-at-all')

      // 2. a well-formed event follows on the same (single, ordered) partition
      const id = crypto.randomUUID()
      const event: Created = {
        type: TYPE,
        id,
        sid: crypto.randomUUID(),
        author: 'mod_test',
        value: 'hello',
      }
      await bus.publish(event)

      await waitFor(15_000, () => received.some((e) => e.id === id))

      assert(received.some((e) => e.id === id), 'the valid event that followed the poison message was never delivered — the stream is stuck')
      assert(errors.length > 0, 'the poison message never reached errorHandler')
    }
    finally {
      await bus.destroy()
    }
  },
})

Deno.test({
  name: 'iggy: a synchronously-throwing or rejecting handler is routed to errorHandler and does not crash the bus',
  ignore: !IGGY,
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const producer = uniqueProducer('iggythrows')
    const errors: EventHandlerError[] = []
    const received: Created[] = []

    const throwsHandler: EventHandler<Event> = {
      type: 'Test.Throws',
      // deliberately not async — a synchronous throw bypasses .catch() entirely
      // and can only be contained by the try/catch around the dispatch call
      handle: () => {
        throw new Error('sync boom')
      },
    }
    const rejectsHandler: EventHandler<Event> = {
      type: 'Test.Rejects',
      handle: () => Promise.reject(new Error('async boom')),
    }
    const okHandler: EventHandler<Created> = {
      type: TYPE,
      handle: (event) => {
        received.push(event)
        return Promise.resolve()
      },
    }

    const bus = new EventBusIggy({ uri: `tcp://iggy:iggy@${HOST}:8090`, interval: 300 })
    await bus.init({
      producer,
      consuming: [producer],
      handlers: [throwsHandler, rejectsHandler, okHandler as EventHandler<Event>],
      error: () => Promise.resolve(),
      errorHandler: (err) => {
        errors.push(err)
        return Promise.resolve()
      },
    })

    try {
      await bus.publish({ type: 'Test.Throws', id: crypto.randomUUID(), sid: crypto.randomUUID(), author: 'mod_test' } as Event)
      await bus.publish({ type: 'Test.Rejects', id: crypto.randomUUID(), sid: crypto.randomUUID(), author: 'mod_test' } as Event)
      const id = crypto.randomUUID()
      const event: Created = { type: TYPE, id, sid: crypto.randomUUID(), author: 'mod_test', value: 'hello' }
      await bus.publish(event)

      await waitFor(15_000, () => received.some((e) => e.id === id))

      assert(received.some((e) => e.id === id), 'the valid event published after the two failing handlers was never delivered — the bus died or stalled')
      assertEquals(errors.length, 2, `expected both the throwing and the rejecting handler to reach errorHandler; got ${errors.length}`)
      assert(errors.some((e) => e.message.includes('sync boom')), 'the synchronous throw never reached errorHandler')
      assert(errors.some((e) => e.message.includes('async boom')), 'the rejected promise never reached errorHandler')
    }
    finally {
      await bus.destroy()
    }
  },
})

Deno.test({
  name: 'iggy: a throwing errorHandler (broken DLQ writer) is reported via config.error and does not crash the bus',
  ignore: !IGGY,
  sanitizeOps: false,
  sanitizeResources: false,
  fn: async () => {
    const producer = uniqueProducer('iggydlq')
    const netErrors: Error[] = []
    const received: Created[] = []

    const throwsHandler: EventHandler<Event> = {
      type: 'Test.Throws',
      handle: () => {
        throw new Error('handler boom')
      },
    }
    const okHandler: EventHandler<Created> = {
      type: TYPE,
      handle: (event) => {
        received.push(event)
        return Promise.resolve()
      },
    }

    const bus = new EventBusIggy({ uri: `tcp://iggy:iggy@${HOST}:8090`, interval: 300 })
    await bus.init({
      producer,
      consuming: [producer],
      handlers: [throwsHandler, okHandler as EventHandler<Event>],
      error: (err) => {
        netErrors.push(err)
        return Promise.resolve()
      },
      // simulates a broken DLQ writer (e.g. Mongo down) — errorHandler itself rejects
      errorHandler: () => {
        throw new Error('dlq write failed')
      },
    })

    try {
      await bus.publish({ type: 'Test.Throws', id: crypto.randomUUID(), sid: crypto.randomUUID(), author: 'mod_test' } as Event)
      const id = crypto.randomUUID()
      const event: Created = { type: TYPE, id, sid: crypto.randomUUID(), author: 'mod_test', value: 'hello' }
      await bus.publish(event)

      await waitFor(15_000, () => received.some((e) => e.id === id))

      assert(received.some((e) => e.id === id), 'the valid event published after the broken-DLQ failure was never delivered — the bus died or stalled')
      assert(netErrors.some((e) => e instanceof NetworkError), 'the broken errorHandler failure was never reported via config.error')
    }
    finally {
      await bus.destroy()
    }
  },
})
