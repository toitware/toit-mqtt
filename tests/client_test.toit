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
  client := mqtt.Client --transport=transport --logger=logger
  options := mqtt.SessionOptions --client-id="test_pubsub"
  client.start --options=options

  done := monitor.Latch

  foo-sharp-counter := 0
  longer-topic-counter := 0
  client.subscribe "foo/#":: foo-sharp-counter++
  client.subscribe "foo/bar/gee":: longer-topic-counter++
  client.subscribe "done":: done.set true

  client.publish "foo/non_bar" "msg".to-byte-array --qos=0
  client.publish "foo/non_bar2" "msg".to-byte-array --qos=1

  client.publish "foo/bar/gee" "msg".to-byte-array --qos=0
  client.publish "foo/bar/gee" "msg2".to-byte-array --qos=1
  client.publish "foo/bar/gee" "msg3".to-byte-array --qos=1

  client.publish "done" "done".to-byte-array --qos=0
  done.get

  expect-equals 2 foo-sharp-counter
  expect-equals 3 longer-topic-counter

  client.unsubscribe "foo/bar/gee"
  client.publish "foo/bar/gee" "msg4".to-byte-array --qos=1

  done = monitor.Latch
  client.publish "done" "done".to-byte-array --qos=0
  done.get

  expect-equals 3 longer-topic-counter

  client.close

test-unclean-session create-transport/Lambda --logger/log.Logger:
  other-transport /mqtt.Transport := create-transport.call
  other-client := mqtt.Client --transport=other-transport --logger=logger
  other-client.start --client-id="other"

  unclean-transport /mqtt.Transport := create-transport.call
  unclean-client := mqtt.Client --transport=unclean-transport --logger=logger
  unclean-client.start --client-id="unclean"

  done := monitor.Latch
  unclean-message-counter := 0
  unclean-client.subscribe "foo/#":: unclean-message-counter++
  unclean-client.subscribe "done":: done.set true

  unclean-client.publish "done" "done".to-byte-array --qos=0
  done.get
  unclean-client.close

  // While the client is away, send some messages to it.
  other-client.publish "foo/bar" "msg".to-byte-array --qos=1
  other-client.publish "foo/gee" "msg".to-byte-array --qos=1

  // Connect again.
  // Since we don't give routes to the client, the catch-all-callback will handle the messages.
  unclean-transport = create-transport.call
  unclean-client = mqtt.Client --transport=unclean-transport --logger=logger
  catch-all-counter := 0
  unclean-client.start --client-id="unclean"
      --catch-all-callback=:: | topic/string payload/ByteArray |
        catch-all-counter++

  done = monitor.Latch
  unclean-client.subscribe "done":: done.set true
  unclean-client.publish "done" "done".to-byte-array --qos=0
  done.get

  expect-equals 2 catch-all-counter
  unclean-client.close

  // Send more messages.
  // The client is again not connected.
  other-client.publish "foo/handled1" "msg".to-byte-array --qos=1
  other-client.publish "foo/handled2" "msg".to-byte-array --qos=1

  unclean-transport = create-transport.call
  routed-messages-counter := 0
  unclean-client = mqtt.Client --transport=unclean-transport --logger=logger --routes={
    "foo/#": :: | topic/string payload/ByteArray |
      expect (topic.starts-with "foo/handled")
      routed-messages-counter++
      expect-equals "msg" payload.to-string
  }
  // Don't use the same counter as before, as the client might not have
  // shut down. Since we are reuising the same client id, the broker
  // is supposed to kick the old client, but mosquitto seems to sometimes
  // still send a message to the old client.
  catch-all-counter2 := 0
  unclean-client.start --client-id="unclean"
      --catch-all-callback=:: | topic/string payload/ByteArray |
        catch-all-counter2++

  done = monitor.Latch
  unclean-client.subscribe "done":: done.set true
  unclean-client.publish "done" "done".to-byte-array --qos=0
  done.get

  expect-equals 0 catch-all-counter2
  expect-equals 2 routed-messages-counter

  unclean-client.close
  other-client.close

test create-transport/Lambda --logger/log.Logger:
  test-pubsub create-transport --logger=logger
  test-unclean-session create-transport --logger=logger

main args:
  test-with-mosquitto := args.contains "--mosquitto"
  log-level := log.ERROR-LEVEL
  logger := log.default.with-level log-level

  run-test := : | create-transport/Lambda | test create-transport --logger=logger
  if test-with-mosquitto: with-mosquitto --logger=logger run-test
  else: with-internal-broker --logger=logger run-test
