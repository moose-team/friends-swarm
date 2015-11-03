var events = require('events')
var subleveldown = require('subleveldown')
var webrtcSwarm = require('webrtc-swarm')
var signalhub = require('signalhub')
var hyperlog = require('hyperlog')
var through = require('through2')
var protobuf = require('protocol-buffers')
var multiline = require('multiline')
var nets = require('nets')

var messages = protobuf(multiline(function () {/*
message SignedMessage {
  optional bytes signature = 1;
  required bytes message = 2;
}

message Message {
  optional string username = 1;
  optional string channel = 2;
  optional uint64 timestamp = 3;
  optional string text = 4;
}

*/}))

module.exports = createSwarm

function createSwarm (db, defaultOpts) {
  var swarm = new events.EventEmitter()
  var logs = {}

  swarm.logs = logs

  var processor
  var sign
  var verify

  if (!defaultOpts) defaultOpts = {}

  if (!defaultOpts.hubs) {
    defaultOpts.hubs = [
      'https://signalhub.mafintosh.com' // mafintosh
      // 'https://instant.io:8080' // feross
    ]
  }

  var remoteConfigUrl = defaultOpts.remoteConfigUrl || 'https://instant.io/rtcConfig' // thanks feross

  swarm.changes = function (name) {
    return logs[name] ? logs[name].changes : 0
  }

  /**
   * fn should be a function that takes the args `data, callback`.
   * The `callback` takes the args `err, signature`.
   */
  swarm.sign = function (fn) {
    sign = fn
  }

  /**
   * fn should be a function that takes the args `username, signature, callback`.
   * The `callback` is called with `err, valid` with valid being truthy if the signature was verified.
   */
  swarm.verify = function (fn) {
    verify = fn
  }

  swarm.process = function (fn) {
    processor = fn
    Object.keys(logs).forEach(function (name) {
      startProcessor(logs[name])
    })
  }

  swarm.removeChannel = function (name) {
    var log = logs[name]
    if (!log) return
    delete logs[name]

    if (log.processing) log.processing.destroy()
    log.peers.forEach(function (p) {
      p.destroy()
    })
  }

  swarm.addChannel = function (name) {
    if (logs[name]) return

    getRemoteConfig(remoteConfigUrl, function (err, config) {
      if (err) console.error('skipping remote config', err)
      if (config) defaultOpts.config = config
      var log = logs[name] = hyperlog(subleveldown(db, name))
      var id = 'friends-' + name
      var hub = signalhub(id, defaultOpts.hubs)
      var sw = webrtcSwarm(hub, defaultOpts)

      log.peers = []

      sw.on('peer', function (p, id) {
        var stream = log.replicate({live: true})

        log.peers.push(p)
        p.on('close', function () {
          var i = log.peers.indexOf(p)
          if (i > -1) log.peers.splice(i, 1)
        })

        swarm.emit('peer', p, name, id, stream)

        stream.on('push', function () {
          swarm.emit('push', name)
        })

        stream.on('pull', function () {
          swarm.emit('pull', name)
        })

        p.pipe(stream).pipe(p)
      })

      if (processor) startProcessor(log)
    })
  }

  swarm.send = function (message, cb) {
    var addMessage = function (m, sig) {
      var ch = message.channel || 'friends'
      swarm.addChannel(ch)
      var log = logs[ch]
      log.heads(function (err, heads) {
        if (err) return cb(err)
        log.add(heads, messages.SignedMessage.encode({signature: sig, message: m}), cb)
      })
    }

    var m = messages.Message.encode(message)

    if (sign) {
      sign(m, function (err, sig) {
        if (err) return cb(err)
        addMessage(m, sig)
      })
      return
    }

    addMessage(m, null)
  }

  function startProcessor (log) {
    log.ready(function () {
      if (log.processing) return

      var rs = log.processing = log.createReadStream({
        live: true,
        since: Math.max(log.changes - 500, 0)
      })

      rs.pipe(through.obj(function (data, enc, cb) {
        if (data.value.toString()[0] === '{') return cb() // old stuff

        var val = messages.SignedMessage.decode(data.value)
        var m = messages.Message.decode(val.message)
        var u = m.username

        m.change = data.change

        if (verify) {
          verify(u, val.message, val.signature, function (_, valid) {
            m.valid = !!valid
            processor(m, cb)
          })
          return
        }

        m.valid = false
        processor(m, cb)
      }))
    })
  }

  return swarm
}

// get remote webrtc config (ice/stun/turn)
function getRemoteConfig (remoteConfigUrl, cb) {
  nets({url: remoteConfigUrl, json: true}, function gotConfig (err, resp, config) {
    if (err || resp.statusCode > 299) config = undefined // ignore errors
    cb(null, config)
  })
}
