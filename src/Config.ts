import type { Log } from '@danielfroz/slog'
import type { Event, EventBusError, EventHandler, EventHandlerError } from './mod.ts'

type TypeOrPredicate<T> = T | (() => T)

export interface Config {
  /**
   * Name of the service / module (aka: consumer group name)
   * Same for producer
   * name = account
   */
  producer: string,
  /**
   * different from name; this is the name of the instance
   * Usually NAME-ID; account-1
   */
  instance?: string
  /**
   * Consumer group names which this EventBus is listening for events
   */
  consuming?: Array<string>
  handlers?: Array<TypeOrPredicate<EventHandler<Event>>>
  /**
   * When specified, we utilize decode() to transform EventBus message / payload (string) to Event.
   * This gives the ability to add custom logic, such as 
   * 'checking for extra fields' or 'add metadata to event'
   */
  decode?: (content: string) => Promise<Event>
  /**
   * When specified does the opposite of decode(). Transforms Event to EventBus message / payload.
   */
  encode?: (event: Event) => Promise<string>
  /**
   * handler for EventBus errors
   */
  error: (error: EventBusError) => Promise<void>
  /**
   * handler thrown by EventHandlers
   */
  errorHandler?: (error: EventHandlerError) => Promise<void>
  /**
   * LoggerFactory from Slog
   */
  log?: Log
}