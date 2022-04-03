const {Connection} = require('./conn')

const DEFAULT_URI = 'http://transcoorditor:8000'
const V1_PREFIX_PATH = 'api/v1'

class Client {
  constructor(uri) {
    if (!uri) {
      uri = DEFAULT_URI
    }

    this.conn = new Connection(uri)

    // connect in background
    this.connecting = this.conn.connect()
  }

  async connect() {
    if (!this.connecting) {
      this.connecting = this.conn.connect()
    }

    await this.connecting

    return this
  }

  v1SessionPath() {
    return V1_PREFIX_PATH + '/sessions'
  }

  async SessionFromId(sessionId) {
    const data = await this.conn.request(
      (rest) => rest.get(this.v1SessionPath() + '/' + sessionId)
    )

    return new Session(this, data)
  }

  async StartSession() {
    const data = await this.conn.request(
      (rest) => rest.post(this.v1SessionPath())
    )

    return new Session(this, data)
  }

  async joinSession(sessionId, body, session) {
    const data = await this.conn.request(
      (rest) => rest.post(this.v1SessionPath() + '/' + sessionId + '/join', {
        json: body
      })
    )

    return new Participant(session, data)
  }

  async JoinSession(sessionId, body) {
    return this.joinSession(sessionId, body, new Session(this, { id: sessionId }))
  }

  async partialCommit(sessionId, body, participant) {
    const data = await this.conn.request(
      (rest) => rest.post(this.v1SessionPath() + '/' + sessionId + '/partial-commit', {
        json: body
      })
    )

    participant.fromData(data)

    return participant
  }

  async PartialCommit(sessionId, body) {
    return this.partialCommit(sessionId, body, new Session(this, { id: sessionId }))
  }

  async commitSession(sessionId, session) {
    const data = await this.conn.request(
      (rest) => rest.post(this.v1SessionPath() + '/' + sessionId + '/commit')
    )

    session.fromData(data)

    return session
  }

  async CommitSession(sessionId) {
    return this.commitSession(sessionId, new Session(this, {}))
  }

  async abortSession(sessionId, session) {
    const data = await this.conn.request(
      (rest) => rest.post(this.v1SessionPath() + '/' + sessionId + '/abort')
    )

    session.fromData(data)

    return session
  }

  async AbortSession(sessionId) {
    return this.abortSession(sessionId, new Session(this, {}))
  }
}

exports.Client = Client

class Session {
  constructor(client, data) {
    this.client = client

    this.fromData(data)
  }

  fromData(data) {
    this.id = data.id
    this.state = data.state
    this.timeout = data.timeout
    this.startedAt = data.startedAt
    this.updatedAt = data.updatedAt
    this.createdAt = data.createdAt
    this.errors = data.errors
    this.retries = data.retries
    this.terminateReason = data.terminateReason
  }

  async JoinSession(body) {
    return this.client.joinSession(this.id, body, this)
  }

  async CommitSession() {
    return this.client.commitSession(this.id, this)
  }

  async AbortSession() {
    return this.client.abortSession(this.id, this)
  }
}

class Participant {
  constructor(session, data) {
    this.session = session

    this.fromData(data)
  }

  fromData(data) {
    this.id = data.id
    this.sessionId = data.sessionId
    this.clientId = data.clientId
    this.requestId = data.requestId
    this.state = data.state
    this.compensateAction = data.compensateAction
    this.completeAction = data.completeAction
    this.updatedAt = data.updatedAt
    this.createdAt = data.createdAt
  }

  async PartialCommit(body) {
    const session = this.session
    body.participantId = this.id

    return session.client.partialCommit(session.id, body, this)
  }
}
