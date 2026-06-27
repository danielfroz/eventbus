import { assert, assertEquals, assertThrows } from 'asserts'
import { ArgumentError, Consumes, clearConsumers, consumers } from './mod.ts'
import type { Event, EventHandler } from './mod.ts'

interface Created extends Event {
  type: 'Test.Created'
}
const TYPE = 'Test.Created'

Deno.test('@Consumes registers the class and stamps the type (no type field)', () => {
  clearConsumers()

  @Consumes(TYPE)
  class TestCreated implements EventHandler<Created> {
    // intentionally NO `type` field — the decorator supplies it
    handle(_event: Created): Promise<void> {
      return Promise.resolve()
    }
  }

  // (a) discovery: the constructor is registered
  const ctors = consumers()
  assertEquals(ctors.length, 1)
  assertEquals(ctors[0], TestCreated)

  // (b) prototype stamping: a fresh instance exposes `.type` (read through the
  // interface, the way an adapter does — `implements` does not add the member)
  const instance: EventHandler<Created> = new TestCreated()
  assertEquals(instance.type, TYPE)
})

Deno.test('@Consumes requires a non-empty type', () => {
  assertThrows(() => Consumes(''), ArgumentError)
  assertThrows(() => Consumes('   '), ArgumentError)
})

Deno.test('consumers() returns every decorated class in order', () => {
  clearConsumers()

  @Consumes('A.One')
  class One implements EventHandler<Event> {
    handle(): Promise<void> { return Promise.resolve() }
  }
  @Consumes('A.Two')
  class Two implements EventHandler<Event> {
    handle(): Promise<void> { return Promise.resolve() }
  }

  const ctors = consumers()
  assertEquals(ctors.length, 2)
  assert(ctors.includes(One))
  assert(ctors.includes(Two))
  // resolving each exposes its stamped type
  const one: EventHandler<Event> = new One()
  const two: EventHandler<Event> = new Two()
  assertEquals(one.type, 'A.One')
  assertEquals(two.type, 'A.Two')
})
