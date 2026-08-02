// deno-lint-ignore-file no-explicit-any
import type { Config, Event, EventBus, EventHandler } from '../mod.ts'
import { ArgumentError, EventHandlerError, InitError, NetworkError } from "../mod.ts"
import { parseUri } from '../uri.ts'
import { r } from './deps.ts'
import type * as i from './mod.ts'

export interface EventBusRedisConfig {
  /**
   * Connection URI, eg. `redis://[user:pass@]host[:port]`. The scheme is
   * ignored; missing port defaults to 6379.
   */
  uri: string
  trace?: boolean
}

export class EventBusRedis implements EventBus {
  private iconfig?: Config
  /**
   * Producer Redis
   */
  private predis?: r.Redis
  /**
   * Consumer Redis
   */
  private credis?: r.Redis
  /**
   * Redis configuration
   */
  private config: i.EventBusRedisConfig
  /**
   * Connection details parsed from config.uri
   */
  private hostname: string
  private port: number
  private username?: string
  private password?: string
  private handlers = new Map<string, EventHandler<Event>>()
  private interval?: ReturnType<typeof setInterval>
  /**
   * Indicates if running
   */
  private running?: boolean

  constructor(config: i.EventBusRedisConfig) {
    if(!config)
      throw new ArgumentError('config')
    if(!config.uri)
      throw new ArgumentError('config.uri')

    this.config = config
    this.config.trace = this.config.trace ?? false

    const u = parseUri(config.uri)
    this.hostname = u.hostname
    this.port = u.port ?? 6379
    this.username = u.username
    this.password = u.password
  }

  async init(config: Config): Promise<void> {
    if(config == null)
      throw new ArgumentError('config');
    if(!config.producer)
      throw new ArgumentError('config.producer')
    if(!config.error)
      throw new ArgumentError('config.error')
    if(!config.instance)
      config.instance = `${config.producer}.${Math.floor(Date.now() / 1000)}`

    // register publisher so understand correct name
    this.iconfig = config

    const { producer, instance } = config

    const connect = async (): Promise<r.Redis> => {
      try {
        const descriptor = await r.connect({ hostname: this.hostname, port: this.port })
        if(this.username != null && this.password != null)
          await descriptor.auth(this.username, this.password)
        else if(this.password != null)
          await descriptor.auth(this.password)
        return descriptor
      }
      catch(error: Error | any) {
        throw new NetworkError({
          producer,
          instance,
          message: `EventBusRedis; host: ${this.hostname}:${this.port}; connect failed: ${error}`,
          stack: `${error.stack}`
        })
      }
    }

    // this maybe a publisher only.... so 1 connection suffices
    this.predis = await connect()
    
    // the rest of the code is for consumer... if no consuming section
    // it assumes that will deal with publish() only actions...
    if(!config.consuming) {
      return
    }


    // must declare errorHandler!
    if(!config.errorHandler) {
      throw new InitError('config.errorHandler.required')
    }
    // then consumers are configured...
    // if no handler... thrown an Error
    if(!config.handlers) {
      throw new InitError('config.handlers.required')
    }

    // initializer handlers
    for(const hop of config.handlers) {
      const handler = typeof(hop) === 'function' ? await hop(): hop
      if(!handler.type)
        throw new InitError('handler.type required (declare a `type` field or use @Consumes(type))')
      this.handlers.set(handler.type, handler)
    }
  
    this.credis = await connect()
    // this is used when reading from groups...
    const streams = new Array<r.XKeyIdGroup>()
    if(config.consuming) {
      for(const stream of config.consuming) {
        try {
          await this.credis?.xgroupCreate(stream, name, '$', true);
        }
        catch(error: Error|any) {
          if(!error.message.includes('already exists')) {
            throw new NetworkError({ producer: name, instance, message: error.message })
          }
        }
        streams.push({ key: stream, xid: '>' })
      }
    }
  
    this.running = false
    // _poll must never reject the interval callback (an unhandled rejection
    // from setInterval crashes the whole Deno process) — belt-and-braces on
    // top of the guards inside _poll itself.
    this.interval = setInterval(() => {
      this._poll(config, streams, name, instance).catch(async (error: Error | any) => {
        this.running = false
        const nerror = new NetworkError({ producer: name, instance, message: error.message, stack: `${error.stack}` })
        if(config.error) {
          await config.error(nerror)
            .catch(err => {
              config.log?.error({ msg: 'config error caught', message: err.message, stack: err.stack })
            })
        }
      })
    }, 500)
  }

  private async _poll(config: Config, streams: r.XKeyIdGroup[], name: string, instance: string): Promise<void> {
    // only executes if there is streams for this EventBusRedis.
    // in other words, no need to execute when it's a publisher only
    if(!streams || streams.length == 0) {
      this.running = false
      return
    }

    if(this.running) {
      return
    }

    // indicates that it's still running
    // avoids stacking multiples runs due to setInterval
    this.running = true

    const producer = config.producer

    try {
      let result
      try {
        result = await this.credis?.xreadgroup(streams, { group: name, consumer: instance })
      }
      catch(error: Error | any) {
        const nerror = new NetworkError({ producer: name, instance, message: error.message, stack: `${error.stack}` })
        if(config.error)
          await config.error(nerror)
        return
      }

      if(!result || result.length == 0) {
        // no message to read... sleep
        return
      }

      for(const reply of result) {
        const stream = reply.key
        const messages = reply.messages

        // never throws: a broken errorHandler is reported via config.error and
        // swallowed, so a single DLQ failure can never escape _poll and crash
        // the process.
        const handleError = async (args: { message: string, producer?: string, event?: Event, stack?: string }) => {
          const { message, event, stack } = args
          try {
            if(config.errorHandler) {
              await config.errorHandler(new EventHandlerError({
                message,
                stream,
                producer,
                event,
                stack
              }))
            }
          }
          catch(error: Error | any) {
            if(config.error) {
              await config.error(new NetworkError({ producer: name, instance, message: error.message, stack: `${error.stack}` }))
                .catch(err => {
                  config.log?.error({ msg: 'config error caught', message: err.message, stack: err.stack })
                })
            }
          }
        }

        for(const message of messages) {
          // never throws: a failed ack is reported via config.error and
          // swallowed — the message simply redelivers on the next poll rather
          // than crashing the process.
          const ack = async () => {
            try {
              await this.credis?.xack(stream, name, message.xid)
            }
            catch(error: Error | any) {
              if(config.error) {
                await config.error(new NetworkError({ producer: name, instance, message: error.message, stack: `${error.stack}` }))
                  .catch(err => {
                    config.log?.error({ msg: 'errorhandler thrown error', message: err.message, stack: err.stack })
                  })
              }
            }
          }

          // decode -> validate -> dispatch as one guarded unit: any throw (a
          // malformed payload, a synchronously-throwing handler, ...) is
          // routed to handleError instead of escaping _poll — a poison
          // message must never block or kill the stream. The message is
          // always acked on the way out via `finally`.
          try {
            const content = message.fieldValues['content']
            if(content == null) {
              await handleError({ message: 'message.content.required' })
              continue
            }

            const event = config.decode ?
              await config.decode(content):
              JSON.parse(content) as Event

            if(!event) {
              await handleError({ message: 'event.required' })
              continue
            }
            if(!event.type) {
              await handleError({ message: 'event.type.required', event })
              continue
            }
            if(!event.sid) {
              await handleError({ message: 'event.sid.required'})
              continue
            }
            if(!event.id) {
              await handleError({ message: 'event.id.required', event })
              continue
            }
            if(!event.ts) {
              await handleError({ message: 'event.ts.required', event })
              continue
            }

            const handler = this.handlers.get(event.type)
            if(!handler) {
              if(config.log)
                config.log.trace({ msg: `no handler for event: ${event.type}` })
              continue
            }

            if(config.log) {
              config.log.trace({ msg: 'exec handler', stream, instance, handler: handler.constructor.name, event })
            }

            try {
              await handler.handle(event)
            }
            catch(error: Error | any) {
              await handleError({ message: `${error?.message ?? error}`, stack: `${error?.stack}`, event })
            }
          }
          catch(error: Error | any) {
            await handleError({ message: `${error?.message ?? error}`, stack: `${error?.stack}` })
          }
          finally {
            await ack()
          }
        } // !for message
      } // !for stream
    }
    finally {
      this.running = false
    }
  }

  _sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms))
  }

  async destroy(): Promise<void> {
    if(this.interval)
      clearInterval(this.interval)
    
    while(true) {
      if(!this.running)
        break
      await this._sleep(100)
    }

    this.credis?.close()
    this.predis?.close()
    return Promise.resolve()
  }

  async publish(event: Event): Promise<void> {
    // checking events...
    if(!event)
      throw new ArgumentError('event')
    if(!event.type || typeof(event.type) !== 'string')
      throw new ArgumentError('event.type')
    if(!event.id)
      throw new ArgumentError('event.id')
    if(!event.sid)
      throw new ArgumentError('event.sid')
    if(!event.author)
      throw new ArgumentError('event.author')
    if(!event.ts)
      event.ts = new Date().toISOString()

    if(!this.predis)
      throw new InitError('init required')

    const config = this.iconfig
    if(!config)
      throw new InitError('config not correctly initialized')

    const { producer, instance } = config
    if(!producer)
      throw new InitError('config.producer.required')
    if(!instance)
      throw new InitError('config.instance.required')
    
    const content = config.encode ?
      await config.encode(event):
      JSON.stringify(event)
    
    try {
      await this.predis.xadd(producer, '*', { content } as r.XAddFieldValues)
    }
    catch(error: Error|any) {
      const nerror = new NetworkError({ 
        producer,
        instance,
        message: error.message,
        stack: `${error.stack}`
      })
      if(config.error) {
        config.error(nerror)
      }
      else
        throw nerror
    }
  }
}
