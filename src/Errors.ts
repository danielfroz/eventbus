import type { Event } from './mod.ts'

export class EventBusError extends Error {
  constructor(message: string) {
    super(message)
    this.name = 'EventBusError'
  }
}

export class ArgumentError extends EventBusError {
  constructor(message: string) {
    super(message)
    this.name = 'ArgumentError'
  }
}

/**
 * Specific to Initialization required error
 */
export class InitError extends EventBusError {
  constructor(message: string) {
    super(message)
    this.name = 'InitError'
  }
}

export type NetworkErrorArgs = {
  producer: string
  instance: string
  message: string
  stack?: string
}
export class NetworkError extends EventBusError {
  producer: string
  instance: string

  constructor(args: NetworkErrorArgs) {
    super(args.message)
    this.name = 'NetworkError'

    this.producer = args.producer
    this.instance = args.instance
    this.stack = args.stack
  }
}

export type EventHandlerErrorArgs = {
  /** basic error data */
  message: string
  stack?: string
  /** eventbus specific data */
  producer: string
  stream: string
  event?: Event
}
export class EventHandlerError extends EventBusError {
  producer: string
  stream: string
  event?: Event

  constructor(readonly args: EventHandlerErrorArgs) {
    super(args.message)
    this.name = 'EventHandlerError'
    if(args.stack)
      this.stack = args.stack

    this.producer = args.producer
    this.stream = args.stream
    this.event = args.event
  }
}