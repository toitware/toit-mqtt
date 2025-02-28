# mqtt

An MQTT v3.1.1 Toit client, with support for QoS 0 and 1.

## Example

These simple examples show how to publish and subscribe to an MQTT broker. You
can find more examples in the [examples/](examples/) folder.

### Simple client

The simple client is useful, if advanced features, like automatic
reconnects, topic routing, parallel sending, etc., are not needed.

In many cases they are easier to use than the `Client` or `FullClient`.

```toit
import mqtt

main:
  transport := mqtt.TcpTransport --host="127.0.0.1"
  client := mqtt.SimpleClient --transport=transport

  client.start --client_id="toit-client-id"

  client.subscribe "device/events"

  // Note that we publish to a different topic than we subscribed to.
  // If you want to receive the message you published, make sure to change
  // one of the topics.
  client.publish "device/data" "Hello, world!"

  while true:
    received := client.receive
    print "Received: $received.topic: $received.payload.to_string_non_throwing"
```

### Routing client

The routing client is a wrapper around the `FullClient`. It routes incoming
messages to the appropriate handler based on the topic.

Since it is based on the `FullClient` it automatically reconnects to the broker
if the connection is lost. It also resends unacknowledged messages and has
many more features. It is heavier and more complicated than the `SimpleClient`.

```toit
import mqtt

main:
  transport := mqtt.TcpTransport --host="127.0.0.1"
  client := mqtt.Client --transport=transport

  client.start --client_id="toit-client-id"

  client.subscribe "device/events":: | topic/string payload/ByteArray |
    print "Received: $topic: $payload.to_string_non_throwing"

  // Note that we publish to a different topic than we subscribed to.
  // If you want to receive the message you published, make sure to change
  // one of the topics.
  client.publish "device/data" "Hello, world!"
```
