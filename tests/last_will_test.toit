// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import expect show *
import log
import monitor
import mqtt
import mqtt.transport as mqtt
import mqtt.packets as mqtt
import net

import .broker-internal
import .broker-mosquitto
import .transport

PING-PONG-MAX ::= 100

/**
Tests the last-will message.
*/
test create-transport/Lambda --logger/log.Logger:
  // Mosquitto doesn't support zero-duration keep-alives.
  // Just set it to something really big.
  keep-alive := Duration --s=10_000

  clients := []
  transports := []

  id-counter := 0
  2.repeat: | qos |
    2.repeat: | retain-counter |
      retain := retain-counter == 0
      last-will := mqtt.LastWill --qos=qos --retain=retain
          "test/last_will$id-counter"
          "last_will_message $qos $retain".to-byte-array
      transport /mqtt.Transport := create-transport.call
      transports.add transport
      client := mqtt.FullClient --transport=transport --logger=logger
      options := mqtt.SessionOptions --client-id="test_client $(id-counter++)"
          --keep-alive=keep-alive
          --clean-session
          --last-will=last-will
      client.connect --options=options
      clients.add client

  receiver-transport /mqtt.Transport := create-transport.call
  receiver-client := mqtt.FullClient --transport=receiver-transport --logger=logger
  receiver-options := mqtt.SessionOptions --client-id="receiver" --keep-alive=keep-alive --clean-session
  receiver-client.connect --options=receiver-options

  clients.do: | client |
    task::
      catch:
        client.handle: | packet/mqtt.Packet |
          logger.info "received $packet"
          client.ack packet

        logger.info "client shut down"

  subscription-ack-semaphore := monitor.Semaphore

  received := monitor.Semaphore
  received-packets := {:}
  task::
    receiver-client.handle: | packet/mqtt.Packet |
      logger.info "received $packet"
      receiver-client.ack packet
      if packet is mqtt.PublishPacket:
        received.up
        publish := packet as mqtt.PublishPacket
        received-packets[publish.topic] = publish
      else if packet is mqtt.SubAckPacket:
        subscription-ack-semaphore.up

    logger.info "receiver client shut down"

  // Wait for all clients to be connected.
  clients.do: it.when-running: null
  receiver-client.when-running: null

  receiver-client.subscribe "test/last_will0"
  receiver-client.subscribe "test/last_will1"
  receiver-client.subscribe "test/last_will2"
  receiver-client.subscribe "test/last_will3"

  subscription-ack-semaphore.down
  subscription-ack-semaphore.down
  subscription-ack-semaphore.down
  subscription-ack-semaphore.down

  // Cut the connections of the clients, triggering a last will.
  transports.do: it.close

  4.repeat: received.down

  4.repeat:
    packet /mqtt.PublishPacket := received-packets["test/last_will$it"]
    payload := packet.payload.to-string
    parts := payload.split " "
    expected-qos := int.parse parts[1]
    if expected-qos == 0: expect-null packet.packet-id
    else: expect-not-null packet.packet-id
    // Since we were alive when the last will was sent, the retain is set to false.
    expect-not packet.retain

  // Reset.
  received-packets.clear

  // Subscribe again to get the retained messages.
  receiver-client.subscribe "test/last_will0"
  receiver-client.subscribe "test/last_will1"
  receiver-client.subscribe "test/last_will2"
  receiver-client.subscribe "test/last_will3"

  2.repeat: received.down
  received-packets.do: | topic packet/mqtt.PublishPacket |
    payload := packet.payload.to-string
    parts := payload.split " "
    expected-qos := int.parse parts[1]
    if expected-qos == 0: expect-null packet.packet-id
    else: expect-not-null packet.packet-id
    expected-retain := parts[2] == "true"
    expect expected-retain
    // Since we subscribed again, the retain flag must be set.
    expect packet.retain

  receiver-client.close

main args:
  test-with-mosquitto := args.contains "--mosquitto"
  log-level := log.ERROR-LEVEL
  logger := log.default.with-level log-level

  run-test := : | create-transport/Lambda | test create-transport --logger=logger
  if test-with-mosquitto: with-mosquitto --logger=logger run-test
  else: with-internal-broker --logger=logger run-test
