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

test-pubsub client/mqtt.FullClient callbacks/Map --logger/log.Logger:
  2.repeat: | qos |
    TESTS ::= [
      "foo",
      "level1/level2",
      "level1/level2/level3",
      "/leading/empty/level",
      "with spaces",
      ["#", "foobar"],
      ["#", "/foo/bar"],
      ["+", "foo"],
      ["+/", "foo/"],
      ["foo/+/bar", "foo/x/bar"],
      ["foo/+/gee", "foo/y/gee"],
    ]

    TESTS.do: | sub-topic |
      subscription := ?
      topic := ?
      if sub-topic is string:
        subscription = sub-topic
        topic = sub-topic
      else:
        subscription = sub-topic[0]
        topic = sub-topic[1]

      logger.info "testing topic: $topic - $subscription"

      wait-for-bar := monitor.Latch
      seen-not-bar := false

      callbacks[topic] = :: | packet/mqtt.PublishPacket |
        if packet.payload.to-string == "bar": wait-for-bar.set true
        else: seen-not-bar = true

      client.subscribe subscription
      client.publish topic "not bar".to-byte-array  --qos=qos
      client.publish topic "bar".to-byte-array  --qos=qos

      wait-for-bar.get
      expect seen-not-bar
      client.unsubscribe subscription
      callbacks.remove topic

  // Test sending a message in the handle function.
  topic := "in_handle"
  response-topic := "response_topic"
  client.subscribe topic
  client.subscribe response-topic

  callbacks[topic] = :: | packet/mqtt.PublishPacket |
    client.publish response-topic "response".to-byte-array

  got-response := monitor.Latch
  callbacks[response-topic] = :: | packet/mqtt.PublishPacket |
    client.unsubscribe topic
    client.unsubscribe response-topic
    got-response.set true

  client.publish topic "message".to-byte-array
  got-response.get

test-multisub client/mqtt.FullClient callbacks/Map --logger/log.Logger:
  client.subscribe "idle"
  2.repeat: | max-qos |
    logger.info "testing multi-subscription with max-qos=$max-qos"

    TOPICS ::= [
      "foo/+/gee",
      "#",
      "foo/bar/gee",
    ]

    wait-for-bar := monitor.Latch
    idle := monitor.Semaphore
    seen-not-bar := false

    callbacks["idle"] = :: | packet/mqtt.PublishPacket |
      idle.up

    callbacks["foo/bar/gee"] = :: | packet/mqtt.PublishPacket |
      if packet.payload.to-string == "bar": wait-for-bar.set true
      else:
        expect-not seen-not-bar
        seen-not-bar = true

    client.subscribe-all
        TOPICS.map: mqtt.TopicQos it --max-qos=max-qos

    client.publish "foo/bar/gee" "not bar".to-byte-array --qos=1
    client.publish "foo/bar/gee" "bar".to-byte-array --qos=1

    client.unsubscribe-all TOPICS
    client.publish "idle" #[] --qos=0
    idle.down

test create-transport/Lambda --logger/log.Logger:
  transport /mqtt.Transport := create-transport.call
  client := mqtt.FullClient --transport=transport --logger=logger

  options := mqtt.SessionOptions --client-id="test_client"
  client.connect --options=options

  callbacks := {:}
  task::
    client.handle: | packet/mqtt.Packet |
      if packet is mqtt.PublishPacket:
        client.ack packet
        publish := packet as mqtt.PublishPacket
        callbacks[publish.topic].call publish
      else:
        logger.info "ignored $packet"
    logger.info "client shut down"

  client.when-running:
    test-pubsub client callbacks --logger=logger
    test-multisub client callbacks --logger=logger

  client.close

main args:
  test-with-mosquitto := args.contains "--mosquitto"
  log-level := log.ERROR-LEVEL
  logger := log.default.with-level log-level

  run-test := : | create-transport/Lambda | test create-transport --logger=logger
  if test-with-mosquitto: with-mosquitto --logger=logger run-test
  else: with-internal-broker --logger=logger run-test
