// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import log
import monitor
import mqtt
import mqtt.transport as mqtt
import mqtt.packets as mqtt

import .transport

is-idle-packet_ activity-entry/List idle-topic/string -> bool:
  if activity-entry[0] != "read" and activity-entry[0] != "write": return false
  if activity-entry[1] is not mqtt.PublishPacket: return false
  publish := activity-entry[1] as mqtt.PublishPacket
  return publish.topic == idle-topic

filter-idle-packets_ activity/List idle-topic/string -> List:
  return activity.filter: not is-idle-packet_ it idle-topic

/**
Tests that the client and broker correctly ack packets.

Calls the block with a client, and three lambdas:
- wait_for_idle: sends a packet to the broker and waits for it to come back.
- clear: clears the activity that is logged.
- get_activity: returns the packets that were captured (see $TestTransport).
*/
with-packet-client create-transport/Lambda [block]
    --logger /log.Logger
    --client-id/string = "test-client"
    --clean-session /bool = true
    --keep-alive /Duration = (Duration --s=10_000) // Mosquitto doesn't support 0-duration keep-alives.
    --reconnection-strategy /mqtt.ReconnectionStrategy? = null
    --persistence-store /mqtt.PersistenceStore? = null
    --max-inflight /int? = null
    --on-handle-error /Lambda? = null
    --read-filter /Lambda? = null
    --write-filter /Lambda? = null:
  transport /mqtt.Transport := create-transport.call
  logging-transport := TestTransport transport --read-filter=read-filter --write-filter=write-filter
  client := mqtt.FullClient --transport=logging-transport --logger=logger

  // Mosquitto doesn't support zero-duration keep-alives.
  // Just set it to something really big.
  options := mqtt.SessionOptions
      --client-id = client-id
      --keep-alive = keep-alive
      --clean-session = clean-session
      --max-inflight = max-inflight
  client.connect --options=options
      --persistence-store = persistence-store
      --reconnection-strategy = reconnection-strategy

  // We are going to use a "idle" ping packet to know when the broker is idle.
  // It's not a guarantee as the broker is allowed to send acks whenever it wants, but
  // it should be quite stable.
  idle := monitor.Semaphore

  idle-topic := "idle-$client-id-$random"
  wait-for-idle := ::
    client.publish idle-topic #[] --qos=0
    idle.down
    client.publish idle-topic #[] --qos=0
    idle.down

  task::
    exception := catch --unwind=(on-handle-error == null):
      client.handle: | packet/mqtt.Packet |
        logger.info "received $packet"
        if packet is mqtt.PublishPacket:
          client.ack packet
          if (packet as mqtt.PublishPacket).topic == idle-topic: idle.up

      logger.info "client shut down"
    if exception:
      on-handle-error.call exception

  client.when-running:
    client.subscribe idle-topic --max-qos=0

  block.call client
      wait-for-idle
      :: logging-transport.clear
      :: filter-idle-packets_ logging-transport.activity idle-topic

  client.close
