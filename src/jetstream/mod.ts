// deno-lint-ignore-file no-explicit-any
import { ConsoleLog } from "@danielfroz/slog";
import * as NATSJ from 'jsr:@nats-io/jetstream@3.1.0';
import * as NATS from 'jsr:@nats-io/nats-core@3.1.0';
import * as NATSC from 'jsr:@nats-io/transport-deno@3.1.0';
import type { Config, Event, EventBus, EventHandler } from '../mod.ts';
import { ArgumentError, EventHandlerError, InitError, NetworkError } from "../mod.ts";

export interface EventBusJetstreamConfig {
  servers: string[]
}

export class EventBusJetstream implements EventBus {
  private iconfig?: Config
  private ncs?: NATS.NatsConnection
  private ncc?: NATS.NatsConnection
  private jss?: NATSJ.JetStreamClient
  private jsc?: NATSJ.JetStreamClient
  private subj?: string
  private running: boolean
  private intervals?: Array<number>
  private handlers = new Map<string, EventHandler<Event>>()

  constructor(private readonly jscfg: EventBusJetstreamConfig) {
    this.running = false
  }

  async connect(): Promise<NATS.NatsConnection> {
    const CONFIG = this.jscfg
    try {
      return await NATSC.connect({
        servers: CONFIG.servers
      })
    }
    catch(err: Error|any) {
      throw new InitError(`connect failed: ${JSON.stringify(this.jscfg.servers)}; err: ${err.message}`)
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
      const handler = typeof(hop) === 'function' ? hop(): hop
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

      this.intervals = new Array<number>()
      this.intervals.push(setInterval(async () => {
        this.running = true
        try {
          for(const c of consumers) {
            const { stream, consumer } = c

            // helper
            const handleError = async (args: { message: string, producer?: string, event?: Event, stack?: string }) => {
              const { message, event, stack } = args
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

            const msgs = await consumer.fetch({ expires: 2000 })
            for await (const msg of msgs) {
              // deno-lint-ignore require-await
              const ack = async () => {
                msg.ack()
              }

              const json = new TextDecoder().decode(msg.data)
              const event = config.decode ? 
                await config.decode(json):
                JSON.parse(json) as Event

              if(!event) {
                await handleError({ message: 'event.required' })
                await ack()
                continue
              }
              if(!event.type) {
                await handleError({ message: 'event.type.required', event })
                await ack()
                continue
              }
              if(!event.sid) {
                await handleError({ message: 'event.sid.required'})
                await ack()
                continue
              }
              if(!event.id) {
                await handleError({ message: 'event.id.required', event })
                await ack()
                continue;
              }
              if(!event.ts) {
                await handleError({ message: 'event.ts.required', event })
                await ack()
                continue
              }

              const handler = this.handlers.get(event.type)
              if(!handler) {
                if(config.log)
                  config.log.trace({ msg: `no handler for event: ${event.type}` })
                await ack()
                continue
              }

              if(config.log) {
                config.log.trace({ msg: 'exec handler', stream, instance: config.instance, handler: handler.constructor.name, event })
              }

              await handler.handle(event)
                .catch(async (err: Error) => {
                  await handleError({ message: err.message, stack: `${err.stack}`, event })
                })
                .finally(async () => {
                  await ack()
                })
            }
          } // !for
        }
        finally {
          this.running = false
        }
      }, 1000))
    }
    catch(err) {
      throw err
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
