'use strict'

const waterfall = require('async/waterfall')
const each = require('async/each')
const queue = require('async/queue')
const mh = require('multihashes')

const c = require('./constants')
const PeerQueue = require('./peer-queue')
const utils = require('./utils')

/**
 * Query peers from closest to farthest away.
 */
class Query {
  /**
   * Create a new query.
   *
   * @param {DHT} dht - DHT instance
   * @param {Buffer} key
   * @param {function(PeerId, function(Error, Object))} makePath - The query function to exectue TODO: update this
   *
   */
  constructor (dht, key, makePath) {
    this.dht = dht
    this.key = key
    this.makePath = makePath
    this.concurrency = c.ALPHA
    this._log = utils.logger(this.dht.peerInfo.id, 'query:' + mh.toB58String(key))
  }

  /**
   * Run this query, start with the given list of peers first.
   *
   * @param {Array<PeerId>} peers
   * @param {number} numPaths - Number of parallel disjoint paths to use
   * @param {function(Error, Object)} callback
   * @returns {void}
   */
  run (peers, numPaths, callback) {
    const run = {
      peersSeen: new Set(),
      errors: [],
      paths: null // array of states per disjoint path
    }

    if (peers.length === 0) {
      this._log.error('Running query with no peers')
      return callback()
    }

    numPaths = Math.min(numPaths, peers.length)

    // create correct number of paths
    const pathPeers = []
    for (let i = 0; i < numPaths; i++) {
      pathPeers.push([])
    }

    // assign peers to paths round-robin style
    peers.forEach((peer, i) => {
      pathPeers[i % numPaths].push(peer)
    })
    run.paths = pathPeers.map((peers, i) => {
      return {
        peers,
        query: this.makePath(i),
        peersToQuery: null
      }
    })

    each(run.paths, (path, cb) => {
      waterfall([
        (cb) => PeerQueue.fromKey(this.key, cb),
        (q, cb) => {
          path.peersToQuery = q
          each(path.peers, (p, cb) => addPeerToQuery(p, this.dht, path, run, cb), cb)
        },
        (cb) => workerQueue(this, path, run, cb)
      ], cb)
    }, (err, results) => {
      this._log('query:done')
      if (err) {
        return callback(err)
      }

      if (run.errors.length === run.peersSeen.size) {
        return callback(run.errors[0])
      }

      run.res = {
        finalSet: run.peersSeen,
        paths: []
      }

      run.paths.forEach((path) => {
        if (path.res && path.res.success) {
          run.res.paths.push(path.res)
        }
      })

      callback(null, run.res)
    })
  }
}

/**
 * Use the queue from async to keep `concurrency` amount items running.
 *
 * @param {Query} query
 PATH
 * @param {Object} run
 * @param {function(Error)} callback
 * @returns {void}
 */
function workerQueue (query, path, run, callback) {
  let killed = false
  const q = queue((next, cb) => {
    query._log('queue:work')
    execQuery(next, query, path, run, (err, done) => {
      // Ignore after kill
      if (killed) {
        return cb()
      }
      query._log('queue:work:done', err, done)
      if (err) {
        return cb(err)
      }
      if (done) {
        q.kill()
        killed = true
        return callback()
      }
      cb()
    })
  }, query.concurrency)

  const fill = () => {
    query._log('queue:fill')
    while (q.length() < query.concurrency &&
           path.peersToQuery.length > 0) {
      q.push(path.peersToQuery.dequeue())
    }
  }

  fill()

  // callback handling
  q.error = (err) => {
    query._log.error('queue', err)
    callback(err)
  }

  q.drain = () => {
    query._log('queue:drain')
    callback()
  }

  q.unsaturated = () => {
    query._log('queue:unsatured')
    fill()
  }

  q.buffer = 0
}

/**
 * Execute a query on the `next` peer.
 *
 * @param {PeerId} next
 * @param {Query} query
 PATH
 * @param {Object} run
 * @param {function(Error)} callback
 * @returns {void}
 * @private
 */
function execQuery (next, query, path, run, callback) {
  path.query(next, (err, res) => {
    if (err) {
      run.errors.push(err)
      callback()
    } else if (res.success) {
      path.res = res
      callback(null, true)
    } else if (res.closerPeers && res.closerPeers.length > 0) {
      each(res.closerPeers, (closer, cb) => {
        // don't add ourselves
        if (query.dht._isSelf(closer.id)) {
          return cb()
        }
        closer = query.dht.peerBook.put(closer)
        addPeerToQuery(closer.id, query.dht, path, run, cb)
      }, callback)
    } else {
      callback()
    }
  })
}

/**
 * Add a peer to the peers to be queried.
 *
 * @param {PeerId} next
 * @param {DHT} dht
 PATH
 * @param {Object} run
 * @param {function(Error)} callback
 * @returns {void}
 * @private
 */
function addPeerToQuery (next, dht, path, run, callback) {
  if (dht._isSelf(next)) {
    return callback()
  }

  if (run.peersSeen.has(next)) {
    return callback()
  }

  run.peersSeen.add(next)
  path.peersToQuery.enqueue(next, callback)
}

module.exports = Query
