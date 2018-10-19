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
   * @param {function(PeerId, function(Error, Object))} query - The query function to exectue TODO: update this
   *
   */
  constructor (dht, key, makeSlice) {
    this.dht = dht
    this.key = key
    this.makeSlice = makeSlice
    this.concurrency = c.ALPHA
    this._log = utils.logger(this.dht.peerInfo.id, 'query:' + mh.toB58String(key))
  }

  /**
   * Run this query, start with the given list of peers first.
   *
   * @param {Array<PeerId>} peers
   * TODO: numSlices
   * @param {function(Error, Object)} callback
   * @returns {void}
   */
  run (peers, numSlices, callback) {
    // console.log('query start')
    const run = {
      peersSeen: new Set(),
      errors: [],
      slices: null // array of states per disjoint slice
    }

    if (peers.length === 0) {
      this._log.error('Running query with no peers')
      return callback()
    }

    numSlices = Math.min(numSlices, peers.length)

    if (Number.isNaN(numSlices))
      throw new Error('wtf')

    // create correct number of slices
    const slicePeers = []
    for (let i = 0; i < numSlices; i++) {
      slicePeers.push([])
    }

    // assign peers to slices round-robin style
    peers.forEach((peer, i) => {
      // console.log('LENGTH:', slicePeers.length, 'i:', i, 'numSlices:', numSlices)
      slicePeers[i % numSlices].push(peer)
    })
    run.slices = slicePeers.map((peers) => {
      return {
        peers,
        query: this.makeSlice(peers),
        peersToQuery: null
      }
    })

    each(run.slices, (slice, cb) => {
      waterfall([
        (cb) => PeerQueue.fromKey(this.key, cb),
        (q, cb) => {
          slice.peersToQuery = q
          each(slice.peers, (p, cb) => addPeerToQuery(p, this.dht, slice, run, cb), cb)
        },
        (cb) => workerQueue(this, slice, run, cb)
      ], cb)
    }, (err, results) => {
      // console.log('query end')
      this._log('query:done')
      if (err) {
        return callback(err)
      }

      if (run.errors.length === run.peersSeen.size) {
        return callback(run.errors[0])
      }

      run.res = {
        finalSet: run.peersSeen,
        slices: []
      }

      run.slices.forEach((slice) => {
        if (slice.res && slice.res.success) {
          run.res.slices.push(slice.res)
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
 SLICE
 * @param {Object} run
 * @param {function(Error)} callback
 * @returns {void}
 */
function workerQueue (query, slice, run, callback) {
  let killed = false
  const q = queue((next, cb) => {
    query._log('queue:work')
    execQuery(next, query, slice, run, (err, done) => {
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
           slice.peersToQuery.length > 0) {
      q.push(slice.peersToQuery.dequeue())
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
 SLICE
 * @param {Object} run
 * @param {function(Error)} callback
 * @returns {void}
 * @private
 */
function execQuery (next, query, slice, run, callback) {
  slice.query(next, (err, res) => {
    if (err) {
      run.errors.push(err)
      callback()
    } else if (res.success) {
      slice.res = res
      callback(null, true)
    } else if (res.closerPeers && res.closerPeers.length > 0) {
      each(res.closerPeers, (closer, cb) => {
        // don't add ourselves
        if (query.dht._isSelf(closer.id)) {
          return cb()
        }
        closer = query.dht.peerBook.put(closer)
        addPeerToQuery(closer.id, query.dht, slice, run, cb)
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
 SLICE
 * @param {Object} run
 * @param {function(Error)} callback
 * @returns {void}
 * @private
 */
function addPeerToQuery (next, dht, slice, run, callback) {
  if (dht._isSelf(next)) {
    return callback()
  }

  if (run.peersSeen.has(next)) {
    return callback()
  }

  run.peersSeen.add(next)
  slice.peersToQuery.enqueue(next, callback)
}

module.exports = Query
