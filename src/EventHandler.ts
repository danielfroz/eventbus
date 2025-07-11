import type { Event } from './mod.ts'

export interface EventHandler<T extends Event> {
  type: string
  handle(event: T): Promise<void>;
}