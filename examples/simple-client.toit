// Copyright (C) 2025 Toitlang
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

import log
import monitor
import mqtt
import mqtt.packets as mqtt

/**
An example demonstrating the simple client.
*/

HOST ::= "test.mosquitto.org"
PORT ::= 1883

TOPIC-PREFIX ::= "toit/topic-$(random)"
CLIENT-ID ::= "toit-client-id-$(random)"

main:
  // The transport connects as soon as it is created.
  // Make sure to guard the creation with a `catch` if necessary.
  // If the connection configuration is given to the constructor,
  // you need to guard the SimpleClient construction.
  transport := mqtt.TcpTransport --host=HOST --port=PORT
  logger := log.default.with-level log.INFO-LEVEL
  client := mqtt.SimpleClient --transport=transport --logger=logger

  options := mqtt.SessionOptions --client-id=CLIENT-ID
  // Starting the client might throw if the connection to the broker
  // doesn't work. This can happen if the transport is not connected to
  // an MQTT broker (but, for example, an HTTP server), or if the broker
  // rejects the client.
  client.start --options=options
  // At this point the client is connected to the broker.

  // The '--background' flag depends on the use-case. If provided,
  // then the program will terminate if the there are only
  // background tasks running.
  // If not provided, then the receiver-task will keep the program
  // alive even if the rest of the program has finished.
  task::
    while true:
      // The call to `client.receive` can throw. Make sure to guard it
      // with a `catch` if necessary.
      publish := client.receive
      if not publish: break  // Closed.
      print "Incoming: $publish.topic $publish.payload.to-string"

  // All calls to the broker that expect an ack (like subscribe, unsubscribe,
  // and publish with QoS > 0) will block until the ack is received.
  client.subscribe "$TOPIC-PREFIX/#"
  client.publish "$TOPIC-PREFIX/foo" "hello_world"
  client.publish "$TOPIC-PREFIX/bar" "hello_world" --qos=1
  client.unsubscribe "$TOPIC-PREFIX/#"

  client.close
