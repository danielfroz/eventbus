export interface Event {
  /** event type. eg.: Database.Created */
  type: string
  /** event id */
  id: string
  /** saga / stream id */
  sid: string
  /** author id */
  author?: string
  /** ts */
  ts?: Date|string
}