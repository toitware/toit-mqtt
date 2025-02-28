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

test-publish client/mqtt.FullClient transport/TestTransport --auto-ack-enabled/bool --logger/log.Logger [--wait-for-idle]:
  2.repeat: | qos |
    transport.clear
    client.publish "foo/bar/gee" "bar".to-byte-array --qos=qos
    wait-for-idle.call

    activity := transport.activity
    expected-count := qos == 0 ? 3 : 4
    expect-equals expected-count activity.size
    expect-equals "write" activity[0][0]  // The publish.
    publish := activity[0][1] as mqtt.PublishPacket
    if qos == 0:
      expect-null publish.packet-id
    else:
      response := activity[1][0] == "read" ? activity[1] : activity[2]
      ack := response[1] as mqtt.PubAckPacket
      expect publish.packet-id == ack.packet-id
    // The other 2 packets are the idle packets.

test-sub-unsub client/mqtt.FullClient transport/TestTransport --logger/log.Logger [--wait-for-idle]:
  // Subscriptions always have qos=1.
  transport.clear
  client.subscribe "foo/bar"
  wait-for-idle.call

  activity := transport.activity
  expect-equals 4 activity.size
  expect-equals "write" activity[0][0]  // The subscription.
  subscribe := activity[0][1] as mqtt.SubscribePacket
  response := activity[1][0] == "read" ? activity[1] : activity[2]
  sub-ack := response[1] as mqtt.SubAckPacket
  expect subscribe.packet-id == sub-ack.packet-id
  // The other 2 packets are the idle packets.

  // Unsubscriptions always have qos=1.
  transport.clear
  client.unsubscribe "foo/bar"
  wait-for-idle.call

  activity = transport.activity
  expect-equals 4 activity.size
  expect-equals "write" activity[0][0]  // The subscription.
  unsubscribe := activity[0][1] as mqtt.UnsubscribePacket
  response = activity[1][0] == "read" ? activity[1] : activity[2]
  unsub-ack := response[1] as mqtt.UnsubAckPacket
  expect unsubscribe.packet-id == unsub-ack.packet-id
  // The other 2 packets are the idle packets.

test-max-qos client/mqtt.FullClient transport/TestTransport --logger/log.Logger [--wait-for-idle]:
  2.repeat: | max-qos |
    topic := "foo/bar$max-qos"
    client.subscribe topic --max-qos=max-qos
    wait-for-idle.call

    2.repeat: | packet-qos |
      transport.clear
      client.publish topic "bar".to-byte-array --qos=packet-qos
      wait-for-idle.call

      expect-count := 4
      if packet-qos != 0:
        // If the packet_qos is 0, then there will never be an ack.
        expect-count++
        // If the packet qos is 1, then there might be another ack to the sub.
        if max-qos != 0: expect-count++

      activity := transport.activity
      expect-equals expect-count activity.size
      reads := activity.filter: it[0] == "read"
      writes := activity.filter: it[0] == "write"

      to-broker := writes[0][1] as mqtt.PublishPacket
      expect-equals topic to-broker.topic
      if packet-qos == 0:
        expect-null to-broker.packet-id
      else:
        to-broker-ack := (reads.filter: it[1] is mqtt.PubAckPacket)[0][1]
        expect-equals to-broker.packet-id to-broker-ack.packet-id

      from-broker := (reads.filter: it[1] is mqtt.PublishPacket)[0][1]
      if packet-qos == 0 or max-qos == 0:
        expect-null from-broker.packet-id
      else:
        from-broker-ack := (writes.filter: it[1] is mqtt.PubAckPacket)[0][1]
        expect-equals from-broker.packet-id from-broker-ack.packet-id

/**
Tests that the client and broker correctly ack packets.
*/
test create-transport/Lambda --logger/log.Logger:
  transport /mqtt.Transport := create-transport.call
  logging-transport := TestTransport transport
  client := mqtt.FullClient --transport=logging-transport --logger=logger

  // Mosquitto doesn't support zero-duration keep-alives.
  // Just set it to something really big.
  options := mqtt.SessionOptions --client-id="test_client" --keep-alive=(Duration --s=10_000)
      --clean-session
  client.connect --options=options

  // We are going to use a "idle" ping packet to know when the broker is idle.
  // It's not a guarantee as the broker is allowed to send acks whenever it wants, but
  // it should be quite stable.
  idle := monitor.Semaphore

  wait-for-idle := :
    client.publish "idle" #[] --qos=0
    idle.down

  auto-ack := true

  task::
    client.handle: | packet/mqtt.Packet |
      logger.info "received $packet"
      if packet is mqtt.PublishPacket:
        if not auto-ack: client.ack packet
        if (packet as mqtt.PublishPacket).topic == "idle": idle.up

    logger.info "client shut down"

  client.when-running:
    client.subscribe "idle" --max-qos=0
    wait-for-idle.call

  2.repeat:
    test-publish client logging-transport
        --logger=logger
        --auto-ack-enabled=auto-ack
        --wait-for-idle=wait-for-idle
    // Disable auto-ack for the next iteration and the remaining tests.
    auto-ack = false


  test-sub-unsub client logging-transport --logger=logger --wait-for-idle=wait-for-idle
  test-max-qos client logging-transport --logger=logger --wait-for-idle=wait-for-idle

  client.close

main args:
  test-with-mosquitto := args.contains "--mosquitto"
  log-level := log.ERROR-LEVEL
  logger := log.default.with-level log-level

  run-test := : | create-transport/Lambda | test create-transport --logger=logger
  if test-with-mosquitto: with-mosquitto --logger=logger run-test
  else: with-internal-broker --logger=logger run-test
