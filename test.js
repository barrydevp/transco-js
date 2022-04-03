const { Client } = require('./index')


setImmediate(async () => {
  const client = new Client('http://localhost:8001')
  await client.connect()

  // console.log(client.conn.leader)
  // console.log(client.conn.rsconf)
  console.log(await client.SessionFromId("a2037b19-3c5d-4554-ac57-cc63d77bdb2f"))
})
