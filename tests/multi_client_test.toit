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
Tests that two clients can communicate through the broker.
*/
test create-transport/Lambda --logger/log.Logger:
  // Mosquitto doesn't support zero-duration keep-alives.
  // Just set it to something really big.
  keep-alive := Duration --s=10_000

  transport1 /mqtt.Transport := create-transport.call
  client1 := mqtt.FullClient --transport=transport1 --logger=logger
  options1 := mqtt.SessionOptions --client-id="test_client1" --keep-alive=keep-alive --clean-session
  client1.connect --options=options1

  transport2 /mqtt.Transport := create-transport.call
  client2 := mqtt.FullClient --transport=transport2 --logger=logger
  options2 := mqtt.SessionOptions --client-id="test_client2" --keep-alive=keep-alive --clean-session
  client2.connect --options=options2

  client1-callback /Lambda := :: it // Ignore the packet.
  client2-callback /Lambda := :: it // Ignore the packet.

  2.repeat: | client-index |
    client := client-index == 0 ? client1 : client2
    task::
      client.handle: | packet/mqtt.Packet |
        logger.info "received $packet"
        if client-index == 0: client1-callback.call packet
        else: client2-callback.call packet

      logger.info "client$(client-index + 1) shut down"

  // Wait for both clients to be connected.
  client1.when-running: null
  client2.when-running: null

  subscribed-semaphore := monitor.Semaphore
  client1-callback = :: | packet |
    if packet is mqtt.SubAckPacket: subscribed-semaphore.up
  client2-callback = client1-callback

  client1.subscribe "2-to-1"
  client2.subscribe "1-to-2"

  subscribed-semaphore.down
  subscribed-semaphore.down

  done := monitor.Semaphore

  qos := 0

  ping-pong-count := 0
  client1-callback = :: | packet |
    client1.ack packet
    if packet is mqtt.PublishPacket:
      ping-pong-count++
      if ping-pong-count >= PING-PONG-MAX: done.up
      if ping-pong-count <= PING-PONG-MAX:
        client1.publish "1-to-2" "ping".to-byte-array --qos=qos

  client2-callback = :: | packet |
    client2.ack packet
    if packet is mqtt.PublishPacket:
      ping-pong-count++
      if ping-pong-count >= PING-PONG-MAX: done.up
      if ping-pong-count <= PING-PONG-MAX:
        client2.publish "2-to-1" "pong".to-byte-array --qos=qos

  2.repeat:
    qos = it
    ping-pong-count = 0
    if it == 0: client1.publish "1-to-2" "ping".to-byte-array --qos=qos
    else: client2.publish "2-to-1" "ping".to-byte-array --qos=qos
    done.down
    done.down

  client1.close
  client2.close

main args:
  test-with-mosquitto := args.contains "--mosquitto"
  log-level := log.ERROR-LEVEL
  logger := log.default.with-level log-level

  run-test := : | create-transport/Lambda | test create-transport --logger=logger
  if test-with-mosquitto: with-mosquitto --logger=logger run-test
  else: with-internal-broker --logger=logger run-test
