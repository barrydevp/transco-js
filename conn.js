const got = require('got')
const { _delay, ErrorWrapRetryable, IsRetryableErr, ParseUri } = require('./helper')

const constants = {
  ErrNotLeader: () => ErrorWrapRetryable(new Error('not leader')),
  ErrNoNodeAvailable: () => new Error('no node available'),
  SchemeHttp: "http",
  SchemeHttps: "https",
  DefaultPort: "8000",
  SysPrefixApiPath: "api/sys",
  MaxRetry: 10,
}

class Node {
  constructor(baseUrl) {
    this.baseUrl = baseUrl
    this.rest = got.extend({
      prefixUrl: baseUrl,
      headers: {
        'Content-Type': 'application/json',
      },
      resolveBodyOnly: true,
      responseType: 'json',
      // json: {},
    })
    this.perish()
  }

  async init() {
    const nconf = await this.nconf()
    this.conf = nconf
  }

  isAvailable() {
    return !!this.conf
  }

  perish() {
    this.conf = null
  }

  async nconf() {
    const nconf = await this._request(
      (rest) => rest.get(constants.SysPrefixApiPath + "/nconf"),
    )

    return nconf
  }

  async rsconf() {
    const rsconf = await this._request(
      (rest) => rest.get(constants.SysPrefixApiPath + "/rsconf"),
    )

    return rsconf
  }

  async ping() {
    await this._request(
      (rest) => rest.get(constants.SysPrefixApiPath + "/ping"),
    )

    return true
  }

  async _request(fn) {
    try {
      const resp = await fn(this.rest)
      return resp.data
    } catch (err) {
      const resp = err.response
      if (resp) {
        if (resp.body.err && resp.body.err.includes('node is not the leader')) {
          throw constants.ErrNotLeader()
        }
        throw new Error(`non 200 status: ${resp.statusCode}, msg: ${resp.body.msg}, err: ${resp.body.err}`)
      }
      // cannot request to node, so we will make this node perished
      this.perish()
      throw ErrorWrapRetryable(err)
    }
  }
}

class ConnString {
  constructor(uri) {
    if (!uri) {
      uri = "http://localhost" + constants.DefaultPort
    }
    this.uri = uri
    const u = ParseUri(uri)
    this.url = u
    const scheme = u.scheme
    if (scheme !== constants.SchemeHttp && scheme !== constants.SchemeHttps) {
      throw new Error("scheme must be \"http\" or \"https\"")
    }

    this.scheme = scheme
    const rawHosts = u.host.split(",")
    if (!rawHosts.length) {
      throw new Error("empty host")
    }
    this.hosts = rawHosts.map(h => {
      return h + (h.includes(':') ? "" : `:${constants.DefaultPort}`)
    })
  }

  getBaseURL(host) {
    return this.scheme + "://" + host
  }
}

class Connection {
  constructor(uri) {
    this.connStr = new ConnString(uri)
    const cs = this.connStr

    const nodes = cs.hosts.map((host) => {
      const baseURL = cs.getBaseURL(host)
      const n = new Node(baseURL)
      return n
    })

    this.nodes = nodes
  }

  async connect() {
    await this.loadCluster()
  }

  // this function is not thread safe
  // (concurrent run may lead to unexpected behavior)
  async loadNodes() {
    if (!this.loadingNodes) {
      const load = async () => {
        let nodes = this.nodes

        await Promise.all(nodes.map(async (n) => {
          if (!n.isAvailable()) {
            await n.init().catch((err) => {
              console.log(`init node ${n.baseUrl} failed: ${err.message}`)
            })
          }
        }))

        return true
      }

      this.loadingNodes = load().finally(() => { this.loadingNodes = null })
    }

    return this.loadingNodes
  }

  _reset() {
    this.rsconf = null
    this.leader = null
  }

  // this function is not thread safe
  // (concurrent run may lead to unexpected behavior)
  async loadCluster() {
    // act like mutex
    if (!this.loadingCluster) {
      const load = async () => {
        this.rsconf = null
        this.leader = null
        // try to init unavailable nodes if have
        await this.loadNodes()

        let err = constants.ErrNoNodeAvailable()
        // fetch rsconf
        for (const node of this.nodes) {
          // const firstNode = this.nodes[0]
          if (node.isAvailable()) {
            try {
              const rsconf = await node.rsconf()
              this.rsconf = rsconf
              err = null
              break
            } catch (error) {
              err = error
            }
          }
        }

        if (err !== null) {
          throw err
        }

        // populate leader
        const leaderConf = this.rsconf.Leader
        if (!leaderConf) {
          throw new Error('no leader')
        }

        let leader = this._findLeaderNode(leaderConf)
        // if (!leader) {
        //   const unAvailNodes = this._unAvailableNodes()
        //
        //   if (unAvailNodes.length < this.nodes.length) {
        //     // it mean the leader is ready at this time, 
        //     // so we should try to init unavail node 
        //     // and find the leader one more time
        //     await this.loadNodes()
        //     leader = this._findLeaderNode(leaderConf)
        //   }
        // }

        if (!leader) {
          throw new Error('cannot found leader in uri')
        }

        this.leader = leader

        return true
      }

      this.loadingCluster = load().finally(() => { this.loadingCluster = null })
    }

    return this.loadingCluster
  }

  _unAvailableNodes() {
    return this.nodes.filter(n => n.isAvailable())
  }

  // find leader in nodes list by leader config
  _findLeaderNode(leaderConf) {
    return this.nodes.find((n) => {
      if (!n.conf) {
        return false
      }
      return n.conf.Host === leaderConf.Host && n.conf.ID === leaderConf.ID
    })

  }

  async request(fn) {
    let resp, err
    const exec = async () => {
      if (this.leader) {
        resp = await this.leader._request(fn).catch((error) => {
          err = error
          return null
        })
      } else {
        err = constants.ErrNotLeader()
      }
    }

    await exec()
    let retries = 0
    while (retries < constants.MaxRetry && IsRetryableErr(err)) {
      // handle change leader by reload cluster
      await this.loadCluster()
      err = null
      resp = null
      await exec()
      retries++
      const delay = 200 ** retries
      await _delay(delay)
    }

    if (err) {
      throw err
    }

    return resp
  }
}

exports.Connection = Connection
