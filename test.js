const { Client } = require('./index')
const { _delay, ErrorWrapRetryable, IsRetryableErr } = require('./helper')

setImmediate(async () => {
  const client = new Client('http://localhost:8001,localhost:8002,localhost:8003')
  await client.connect()

  // console.log(client.conn.leader)
  // console.log(client.conn.rsconf)
  while (true) {
    try {
      await client.SessionFromId("55231414-3acd-4ddb-a45c-5505c056eed0")
      console.log(client.conn.leader)
    } catch (err) {
      console.log('isertry', IsRetryableErr(err))
      console.log(err)
    }

    await _delay(1000)
  }
})
