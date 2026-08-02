// deno-lint-ignore-file no-explicit-any
import type { Config, Event, EventBus, EventHandler } from '../mod.ts';
import { ArgumentError, EventHandlerError, InitError, NetworkError } from "../mod.ts";
import { parseUri } from '../uri.ts';
import { ConsoleLog, NATS, NATSC, NATSJ } from './deps.ts';

export interface EventBusJetstreamConfig {
  /**
   * Connection URI, eg. `nats://host:4222` or simply `host:4222`. The scheme is
   * ignored; missing port defaults to 4222.
   */
  uri: string
}

export class EventBusJetstream implements EventBus {
  private iconfig?: Config
  private ncs?: NATS.NatsConnection
  private ncc?: NATS.NatsConnection
  private jss?: NATSJ.JetStreamClient
  private jsc?: NATSJ.JetStreamClient
  private subj?: string
  private running: boolean
  private intervals?: Array<ReturnType<typeof setInterval>>
  private handlers = new Map<string, EventHandler<Event>>()
  /**
   * NATS server (`host:port`) parsed from config.uri; protocol is ignored.
   */
  private readonly servers: string[]

  constructor(jscfg: EventBusJetstreamConfig) {
    if(!jscfg)
      throw new ArgumentError('config')
    if(!jscfg.uri)
      throw new ArgumentError('config.uri')
    const u = parseUri(jscfg.uri)
    this.servers = [ `${u.hostname}:${u.port ?? 4222}` ]
    this.running = false
  }

  async connect(): Promise<NATS.NatsConnection> {
    try {
      return await NATSC.connect({
        servers: this.servers
      })
    }
    catch(err: Error|any) {
      throw new InitError(`connect failed: ${JSON.stringify(this.servers)}; err: ${err.message}`)
    }
  }

  async _initPublisher(config: Config): Promise<void> {
    try {
      const name = config.producer
      this.ncs = await this.connect()
      this.jss = NATSJ.jetstream(this.ncs)
      this.subj = name
      const jsm = await NATSJ.jetstreamManager(this.ncs)
      const streams = await jsm.streams.list().next()
      const exists = streams.find(si => si.config.name === name)
      if(!exists) {
        await jsm.streams.add({
          name: name,
          subjects: [ name ],
          description: `stream of ${name}`,
          retention: NATSJ.RetentionPolicy.Limits,
          discard: NATSJ.DiscardPolicy.Old,
          max_age: NATS.nanos(1000 * 3600 * 24)
        })
      }
    }
    catch(err: Error|any) {
      throw new NetworkError({
        producer: config.producer,
        instance: config.instance!,
        message: `error while creating service: ${err.message}`,
        stack: `${err.stack}`
      })
    }
  }

  async _initConsumers(config: Config): Promise<void> {
    if(!config.consuming) {
      // nothing to be done here...
      return
    }

    // strict check as we must have configuration in place
    if(!config.errorHandler) {
      throw new InitError('config.errorHandler.required')
    }
    if(!config.handlers) {
      throw new InitError(`config.handlers.required`)
    }

    // initialize handlers
    for(const hop of config.handlers) {
      const handler = typeof(hop) === 'function' ? await hop(): hop
      if(!handler.type)
        throw new InitError('handler.type required (declare a `type` field or use @Consumes(type))')
      this.handlers.set(handler.type, handler)
    }
    
    try {
      const producer = config.producer
      

      this.ncc = await this.connect()
      this.jsc = NATSJ.jetstream(this.ncc)
      const jsm = await NATSJ.jetstreamManager(this.ncc) 

      const existingStreams = new Set<string>()
      const streams = await jsm.streams.list().next()
      for(const si of streams) {
        existingStreams.add(si.config.name)
      }

      // consumer setup...
      for(const stream of config.consuming) {
        if(!existingStreams.has(stream)) {
          continue
        }
        const cis = await jsm.consumers.list(stream).next()
        const existing = cis?.find(x => x.config.name === producer)
        if(!existing) {
          // create consumer for this stream
          await jsm.consumers.add(stream, {
            name: producer,
            durable_name: producer,
            filter_subject: `${stream}`,
            ack_policy: NATSJ.AckPolicy.Explicit,
            deliver_policy: NATSJ.DeliverPolicy.New,
          })
        }
      }

      const consumers = new Array<{ stream: string, consumer: NATSJ.Consumer }>()
      for(const stream of config.consuming) {
        if(!existingStreams.has(stream)) {
          continue
        }
        const consumer = await this.jsc.consumers.get(stream, producer)
        consumers.push({ stream, consumer })
      }

      this.intervals = new Array<ReturnType<typeof setInterval>>()
      // _poll must never reject the interval callback (an unhandled rejection
      // from setInterval crashes the whole Deno process) — belt-and-braces on
      // top of the guards inside _poll itself.
      this.intervals.push(setInterval(() => {
        this._poll(config, consumers, producer).catch(async (error: Error | any) => {
          this.running = false
          const nerror = new NetworkError({ producer, instance: config.instance!, message: error.message, stack: `${error.stack}` })
          if(config.error) {
            await config.error(nerror)
              .catch(err => {
                config.log?.error({ msg: 'config error caught', message: err.message, stack: err.stack })
              })
          }
        })
      }, 1000))
    }
    catch(err) {
      throw err
    }
  }

  private async _poll(
    config: Config,
    consumers: Array<{ stream: string, consumer: NATSJ.Consumer }>,
    producer: string,
  ): Promise<void> {
    this.running = true
    try {
      for(const c of consumers) {
        const { stream, consumer } = c

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
                stack,
              }))
            }
          }
          catch(error: Error | any) {
            if(config.error) {
              await config.error(new NetworkError({ producer, instance: config.instance!, message: error.message, stack: `${error.stack}` }))
                .catch(err => {
                  config.log?.error({ msg: 'errorhandler thrown error', message: err.message, stack: err.stack })
                })
            }
          }
        }

        let msgs
        try {
          msgs = await consumer.fetch({ expires: 2000 })
        }
        catch(error: Error | any) {
          const nerror = new NetworkError({ producer, instance: config.instance!, message: error.message, stack: `${error.stack}` })
          if(config.error)
            await config.error(nerror)
          continue
        }

        for await (const msg of msgs) {
          // never throws: a failed ack is reported via config.error and
          // swallowed — the message simply redelivers on the next poll rather
          // than crashing the process.
          const ack = async () => {
            try {
              msg.ack()
            }
            catch(error: Error | any) {
              if(config.error) {
                await config.error(new NetworkError({ producer, instance: config.instance!, message: error.message, stack: `${error.stack}` }))
                  .catch(err => {
                    config.log?.error({ msg: 'config error caught', message: err.message, stack: err.stack })
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
            const json = new TextDecoder().decode(msg.data)
            const event = config.decode ?
              await config.decode(json):
              JSON.parse(json) as Event

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
              config.log.trace({ msg: 'exec handler', stream, instance: config.instance, handler: handler.constructor.name, event })
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
        }
      } // !for
    }
    finally {
      this.running = false
    }
  }
  
  async init(config: Config): Promise<void> {
    if(!config)
      throw new ArgumentError('config')
    if(!config.producer)
      throw new ArgumentError('config.producer')
    if(!config.instance)
      config.instance = `${config.producer}.${Math.floor(Date.now() / 1000)}`

    this.iconfig = config
    if(!config.log)
      config.log = new ConsoleLog({ init: { service: 'eventbus.jetstream' }})
    try {
      await this._initPublisher(config)
      await this._initConsumers(config)
    }
    catch(err) {
      throw err
    }
  }

  async destroy(): Promise<void> {
    if(this.intervals) {
      for(const i of this.intervals) {
        clearInterval(i)
      }
    }
    const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms))
    if(this.ncs) {
      await this.ncs.close()
    }
    if(this.ncc) {
      while(true) {
        if(!this.running)
          break
        await sleep(100)
      }
      await this.ncc.close()
    }
  }

  async publish(event: Event): Promise<void> {
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

    if(!this.iconfig) {
      throw new InitError('eventbus not initialized')
    }
    if(!this.jss) {
      throw new InitError('not connected or initialized')
    }
    if(!this.subj) {
      throw new InitError('not initialized; this.subj')
    }

    const config = this.iconfig
    const { producer, instance } = config
    if(!producer) {
      throw new InitError('config.producer.required')
    }
    if(!instance) {
      throw new InitError('config.producer.instance')
    }

    const payload = config.encode ?
      await config.encode(event):
      JSON.stringify(event)
    try {
      await this.jss.publish(this.subj, payload)
    }
    catch(error: Error|any) {
      const nerror = new NetworkError({
        producer,
        instance,
        message: error.message,
        stack: `${error.stack}`
      })

      if(config.error)
        config.error(nerror)
      else
        throw nerror
    }
  }
}
