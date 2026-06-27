import type { Event } from './mod.ts'

export interface EventHandler<T extends Event> {
  /**
   * Event type this handler consumes (eg.: Database.Created).
   * Optional at the type level so a class decorated with `@Consumes(type)` —
   * which stamps `type` onto the prototype — satisfies this interface without
   * declaring the field. EventBus.init() rejects any handler that ends up with
   * no resolvable type.
   */
  type?: string
  handle(event: T): Promise<void>;
}