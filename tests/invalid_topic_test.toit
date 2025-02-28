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

SUBSCRIPTION-TESTS ::= [
  "##",
  "#/level2",
  "++/level1/level2/level3",
  "le+vel1/level2/level3",
]

PUBLISH-TESTS ::= [
  "#",
  "+/foo",
  "",
]

test-topic topic/string create-transport/Lambda --mode/string --logger/log.Logger:
  transport /mqtt.Transport := create-transport.call
  client := mqtt.FullClient --transport=transport --logger=logger

  options := mqtt.SessionOptions --client-id="test_client"
  client.connect --options=options

  done := monitor.Latch

  callbacks := {:}
  task::
    exception := catch:
      client.handle: | packet/mqtt.Packet |
        if packet is mqtt.PublishPacket:
          client.ack packet
          publish := packet as mqtt.PublishPacket
          callbacks[publish.topic].call publish
        else:
          logger.info "ignored $packet"
      logger.info "client shut down"
    expect-not-null exception
    done.set true

  exception := catch:
    if mode == "subscribe":
      client.subscribe topic
    else if mode == "unsubscribe":
      client.unsubscribe topic
    else if mode == "publish":
      client.publish topic #[]
  if exception:
    logger.info "caught on client side"
    client.close
    return
  else:
    logger.info "waiting for client to shut down"
    done.get

/**
Tests that invalid topics are correctly handled.
*/
test create-transport/Lambda --logger/log.Logger:
  SUBSCRIPTION-TESTS.do: | topic |
    test-topic topic create-transport --logger=logger --mode="subscribe"
    test-topic topic create-transport --logger=logger --mode="unsubscribe"
    test-topic topic create-transport --logger=logger --mode="publish"

  PUBLISH-TESTS.do: | topic |
    test-topic topic create-transport --logger=logger --mode="publish"

main args:
  test-with-mosquitto := args.contains "--mosquitto"
  log-level := log.ERROR-LEVEL
  logger := log.default.with-level log-level

  run-test := : | create-transport/Lambda | test create-transport --logger=logger
  if test-with-mosquitto: with-mosquitto --logger=logger run-test
  else: with-internal-broker --logger=logger run-test
