// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the tests/LICENSE file.

import expect show *
import log
import monitor
import mqtt
import mqtt.transport as mqtt
import net

import .broker-internal
import .broker-mosquitto
import .transport

test-pubsub create-transport/Lambda --logger/log.Logger:
  transport /mqtt.Transport := create-transport.call
  client := mqtt.SimpleClient --transport=transport --logger=logger
  options := mqtt.SessionOptions --client-id="test_pubsub"
  client.start --options=options

  done := monitor.Latch

  foo-sharp-counter := 0
  longer-topic-counter := 0
  task::
    while true:
      packet := client.receive
      if not packet: break
      if packet.topic == "foo/bar/gee":
        longer-topic-counter++
      else if packet.topic.starts-with "foo/":
        foo-sharp-counter++
      else if packet.topic == "done":
        done.set true

  client.subscribe "foo/#"
  client.subscribe "foo/bar/gee"
  client.subscribe "done"

  client.publish "foo/non_bar" "msg" --qos=0
  client.publish "foo/non_bar2" "msg" --qos=1

  client.publish "foo/bar/gee" "msg" --qos=0
  client.publish "foo/bar/gee" "msg2" --qos=1
  client.publish "foo/bar/gee" "msg3" --qos=1

  client.publish "done" "done" --qos=0
  done.get

  expect-equals 2 foo-sharp-counter
  expect-equals 3 longer-topic-counter

  client.unsubscribe "foo/bar/gee"
  // We still get the message through the "foo/" subscription.
  client.publish "foo/bar/gee" "msg4" --qos=1

  done = monitor.Latch
  client.publish "done" "done" --qos=0
  done.get

  expect-equals 4 longer-topic-counter

  client.close

test-unclean-session create-transport/Lambda --logger/log.Logger:
  other-transport /mqtt.Transport := create-transport.call
  other-client := mqtt.SimpleClient --transport=other-transport --logger=logger
  other-client.start --client-id="other"

  unclean-transport /mqtt.Transport := create-transport.call
  unclean-client := mqtt.SimpleClient --transport=unclean-transport --logger=logger
  unclean-client.start --client-id="unclean"

  done := monitor.Latch
  unclean-message-counter := 0
  task --background::
    while true:
      packet := unclean-client.receive
      if not packet: break
      if packet.topic.starts-with "foo/":
        unclean-message-counter++
      else if packet.topic == "done":
        done.set true
  unclean-client.subscribe "foo/#"
  unclean-client.subscribe "done"

  unclean-client.publish "done" "done" --qos=0
  done.get
  unclean-client.close

  // While the client is away, send some messages to it.
  other-client.publish "foo/bar" "msg" --qos=1
  other-client.publish "foo/gee" "msg" --qos=1

  // Connect again.
  unclean-transport = create-transport.call
  unclean-client = mqtt.SimpleClient --transport=unclean-transport --logger=logger
  catch-all-counter := 0
  unclean-client.start --client-id="unclean"

  done = monitor.Latch
  task --background::
    while true:
      packet := unclean-client.receive
      if not packet: break
      if packet.topic == "done":
        done.set true
      else:
        catch-all-counter++

  unclean-client.publish "done" "done" --qos=0
  done.get

  expect-equals 2 catch-all-counter
  unclean-client.close

  // Send more messages.
  // The client is again not connected.
  other-client.publish "foo/handled1" "msg" --qos=1
  other-client.publish "foo/handled2" "msg" --qos=1

  unclean-transport = create-transport.call
  routed-messages-counter := 0
  unclean-client = mqtt.SimpleClient --transport=unclean-transport --logger=logger

  unclean-client.start --client-id="unclean"

  done = monitor.Latch
  // Don't use the same counter as before, as the client might not have
  // shut down. Since we are reuising the same client id, the broker
  // is supposed to kick the old client, but mosquitto seems to sometimes
  // still send a message to the old client.
  catch-all-counter2 := 0

  task --background::
    while true:
      packet := unclean-client.receive
      if not packet: break
      if packet.topic == "done":
        done.set true
      else if packet.topic.starts-with "foo/handled":
          routed-messages-counter++
          expect-equals "msg" packet.payload.to-string
      else:
        catch-all-counter2++

  unclean-client.subscribe "done"
  unclean-client.publish "done" "done" --qos=0
  done.get

  expect-equals 0 catch-all-counter2
  expect-equals 2 routed-messages-counter

  unclean-client.close
  other-client.close

test-poll-receive create-transport/Lambda --logger/log.Logger:
  transport /mqtt.Transport := create-transport.call
  client := mqtt.SimpleClient --transport=transport --logger=logger
  client.start --client-id="test_poll_receive"

  client.subscribe "foo"
  client.publish "foo" "msg" --qos=0
  client.publish "foo" "msg2" --qos=1

  with-timeout (Duration --s=5):
    backup := 10
    while true:
      if client.received-count == 2: break
      sleep --ms=backup
      backup = min 100 backup * 2
  msg := client.receive
  payload := msg.payload
  expect-equals "msg" msg.payload.to-string
  expect-equals 1 client.received-count
  msg = client.receive
  expect-equals "msg2" msg.payload.to-string
  expect-equals 0 client.received-count

  client.close

test create-transport/Lambda --logger/log.Logger:
  test-pubsub create-transport --logger=logger
  test-unclean-session create-transport --logger=logger
  test-poll-receive create-transport --logger=logger

main args:
  test-with-mosquitto := args.contains "--mosquitto"
  log-level := log.ERROR-LEVEL
  logger := log.default.with-level log-level

  run-test := : | create-transport/Lambda | test create-transport --logger=logger
  if test-with-mosquitto: with-mosquitto --logger=logger run-test
  else: with-internal-broker --logger=logger run-test
