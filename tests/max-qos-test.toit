// Copyright (C) 2025 Toitlang
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

test create-transport/Lambda --logger/log.Logger:
  other-transport /mqtt.Transport := create-transport.call
  other-client := mqtt.SimpleClient --transport=other-transport --logger=logger
  other-client.start --client-id="other"

  unclean-transport /mqtt.Transport := create-transport.call
  unclean-client := mqtt.SimpleClient --transport=unclean-transport --logger=logger
  unclean-client.start --client-id="unclean"

  unclean-client.subscribe "foo/#"
  unclean-client.subscribe "bar/#" --max-qos=0
  unclean-client.subscribe "done"
  unclean-client.close

  // There is now a session for the unclean client.

  // While the client is away, send some messages to it.

  // Since the client set a max-qos of 0 for "bar/#", the messages
  // will be discarded.
  other-client.publish "bar/1" "msg" --qos=1
  other-client.publish "bar/2" "msg" --qos=1
  other-client.publish "foo/1" "msg" --qos=1
  other-client.publish "foo/2" "msg" --qos=1

  // Connect again.
  unclean-transport = create-transport.call
  unclean-client = mqtt.SimpleClient --transport=unclean-transport --logger=logger
  unclean-client.start --client-id="unclean"

  // Receive the two packets.
  pkg1 := unclean-client.receive
  pkg2 := unclean-client.receive
  topics := {pkg1.topic, pkg2.topic}
  expect (topics.contains "foo/1")
  expect (topics.contains "foo/2")
  expect pkg1.payload == "msg".to-byte-array
  expect pkg2.payload == "msg".to-byte-array

  other-client.publish "done" "done"
  done-pkg := unclean-client.receive
  expect-equals "done" done-pkg.topic

  unclean-client.close
  other-client.close

main args:
  test-with-mosquitto := args.contains "--mosquitto"
  log-level := log.ERROR-LEVEL
  logger := log.default.with-level log-level

  run-test := : | create-transport/Lambda | test create-transport --logger=logger
  if test-with-mosquitto: with-mosquitto --logger=logger run-test
  else: with-internal-broker --logger=logger run-test
