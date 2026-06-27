import { ArgumentError } from './mod.ts'
import type { Event, EventHandler } from './mod.ts'

// deno-lint-ignore no-explicit-any
export type Constructor<T = unknown> = new (...args: any[]) => T

interface Registration {
  ctor: Constructor<EventHandler<Event>>
  type: string
}

/** TC39 class decorator applied to an EventHandler implementation. */
export type ConsumesDecorator = <C extends Constructor<EventHandler<Event>>>(
  value: C,
  context: ClassDecoratorContext<C>,
) => void

const registry = new Array<Registration>()

/**
 * TC39 class decorator that declares an EventHandler implementation and the
 * event `type` it consumes.
 *
 * Two effects, both at class-definition time:
 *  1. records the class in a module-level registry (read back via `consumers()`);
 *  2. stamps `type` onto the class prototype so instances expose `.type` without
 *     the class declaring a `type` field.
 *
 * Mirrors sloth's `@Route`/`@Provide`: TC39 standard decorator (no
 * `experimentalDecorators`, no `reflect-metadata`).
 *
 * ```ts
 * @Consumes(Events.Order.CREATED)
 * export class OrderCreated implements EventHandler<Events.Order.Created> {
 *   async handle(e: Events.Order.Created) { ... }
 * }
 * ```
 */
export function Consumes(type: string): ConsumesDecorator {
  if (!type || !type.trim())
    throw new ArgumentError('type')
  return (value, _ctx) => {
    registry.push({ ctor: value, type })
    // class-decorator addInitializer runs with `this` = the class, not the
    // instance, so stamp the prototype to give every instance `.type`.
    ;(value.prototype as EventHandler<Event>).type = type
  }
}

/**
 * Constructors of every `@Consumes()`-decorated class, in declaration order.
 * Wire them with your DI container, e.g.:
 * `handlers: consumers().map((C) => () => container.resolve(C))`.
 */
export function consumers(): ReadonlyArray<Constructor<EventHandler<Event>>> {
  return registry.map((r) => r.ctor)
}

/** Clears the discovery registry (intended for tests). */
export function clearConsumers(): void {
  registry.length = 0
}
