import type { Event, Errors, EventHandler } from './mod.ts'

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
  handlers?: Array<EventHandler<Event>>
  /**
   * When specified, we utilize decoder to transform the message from EventBus (string) to Event.
   * This gives the ability to add custom logic, such as 'checking for extra fields'
   */
  decoder?: (content: string) => Promise<Event>
  /**
   * When specified we utilize decoder to transform Event to message prior posting it to EventBus
   */
  encoder?: (event: Event) => Promise<string>
  /**
   * handler for EventBus errors
   */
  error: (error: Errors.EventBusError) => Promise<void>
  /**
   * handler thrown by EventHandlers
   */
  errorHandler?: (error: Errors.EventHandlerError) => Promise<void>
}