import type { Config, Event } from './mod.ts'

export type EventBusErrorListener = (error?: Error) => void

export interface EventBus {
  init(config: Config): Promise<void>
  destroy(): Promise<void>
  publish(event: Event): Promise<void>
}