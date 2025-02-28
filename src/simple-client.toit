// Copyright (C) 2025 Toitlang Team.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import io
import log
import monitor
import net
import tls
import .client show Client  // For toitdoc.
import .full-client show FullClient  // For toitdoc.
import .session-options
import .last-will
import .packets
import .topic-qos
import .tcp // For toitdoc.
import .transport
import .utils_

/**
A simple MQTT client.

This client is purposefully simple and minimizes the work that is done
  in the background. As a consequence, it does not handle reconnections,
  parallel packet-sends (where a packet can be sent before the ACK of
  an earlier packet), topic routing, or other advanced features.

If you just want to send messages to a broker, this client is a good choice.
  If you need more advanced features, consider using the $Client or
  $FullClient class.

Received packets are enqueued until they are read. The client automatically
  acknowledges packets with QoS 1.
*/

/**
A simple MQTT client that can send messages to a broker.
*/
class SimpleClient:
  static PROTOCOL-ERROR_ ::= "PROTOCOL_ERROR"
  static CONNECTION-CLOSED_ ::= "CONNECTION_CLOSED"

  /**
  The client has been created. Handle has not been called yet.
  The transport is connected (to our knowledge).
  */
  static STATE-CREATED_ ::= 0
  /**
  The client is (or was) connected and the receiver task is running.
  */
  static STATE-CONNECTED_ ::= 1
  /**
  The client has disconnected.
  The packet might not be sent yet (if other packets are queued in front), but no
    calls to $publish, ... should be done.
  */
  static STATE-DISCONNECTED_ ::= 2
  /**
  The client is closed.
  If an $error_ is set, then the client is in an error state.
  */
  static STATE-CLOSED_ ::= 3

  transport_/Transport? := ?
  logger_/log.Logger
  on-error_/Lambda? := null
  state_/int := STATE-CREATED_
  reader_ /io.Reader
  writer_ /io.Writer
  reader-task_/Task? := null
  pinger-task_/Task? := null
  connection-monitor_/MutexSignal := MutexSignal
  last-received_/Packet? := null
  received-queue_/Deque := Deque
  // The error is set the first time we encounter one.
  error_/any := null

  packet-id_/int := 1

  /**
  Constructs a new sending MQTT client.

  The $transport parameter is used to send messages and is usually a TCP socket instance.
    See $(constructor --host), and $TcpTransport.
  */
  constructor --transport/Transport --logger/log.Logger=log.default:
    transport_ = transport
    logger_ = logger
    reader_ = TransportReader_ transport_
    writer_ = TransportWriter_ transport_

  /**
  Variant of $(constructor --transport) that connects to the given $host:$port over TCP.
  */
  constructor
      --host/string
      --port/int=1883
      --net-open/Lambda?=(:: net.open)
      --logger/log.Logger=log.default:
    transport := TcpTransport --host=host --port=port --net-open=net-open
    return SimpleClient --transport=transport --logger=logger

  /**
  Variant of $(constructor --host) that supports TLS.
  */
  constructor.tls
      --host/string
      --port/int=8883
      --net-open/Lambda?=(:: net.open)
      --server-name/string?=null
      --certificate/tls.Certificate?=null
      --logger/log.Logger=log.default:
    transport := TcpTransport.tls --host=host --port=port --net-open=net-open
          --server-name=server-name
          --certificate=certificate
    return SimpleClient --transport=transport --logger=logger

  /**
  Closes the client.

  Unless $force is true, just sends a disconnect packet to the broker.
    The client then shuts down gracefully once the broker has closed
    the connection. Blocks until the client is closed.

  If $force is true, shuts down the client by severing the transport.

  # Disconnect
  In the case of a graceful (non-$force close), the following applies:

  If other tasks are currently sending packets (with $publish or
    (un)$subscribe), then waits for these tasks to finish, before
    taking any action.

  If the client has already sent a disconnect packet, or is closed,
    does nothing.

  # Forced shutdown
  In the case of a forced shutdown, calls $Transport.close on the current
    transport.

  No further packet is sent or received at this point.
  */
  close --force/bool=false -> none:
    if state_ == STATE-CREATED_:
      // We are not connected yet, so we can just tear down.
      tear-down_
    else if state_ == STATE-CONNECTED_:
      if not force: disconnect_
      else: tear-down_
    else if state_ == STATE_DISCONNECTED_:
      if not force: return
      tear-down_
    else:
      assert: state_ == STATE-CLOSED_
      // Nothing to do.
      return

  disconnect_ -> none:
    state_ = STATE-DISCONNECTED_

    // Kill the ping-task.
    if pinger-task_: pinger-task_.cancel

    disconnect-packet := DisconnectPacket
    send-packet_ disconnect-packet

    connection-monitor_.wait: state_ == STATE-CLOSED_

  tear-down_:
    // catch --trace: throw "x"
    critical-do:
      state_ = STATE-CLOSED_
      if reader-task_: reader-task_.cancel
      if pinger-task_: pinger-task_.cancel
      if transport_:
        transport_.close
      transport_ = null
    // Notify any listeners. They are probably unblocked now.
    connection-monitor_.raise

  error_ reason/string --do-throw/bool=false:
    if not error_:
      error_ = reason
      if on-error_: on-error_.call reason
      tear-down_
    if do-throw: throw error_

  run-protected-task_ name/string fun/Lambda --background/bool -> Task:
    return task --background=background::
      e := catch --trace=(: state_ != STATE-CLOSED_):
        fun.call
      if e:
        logger_.error "task failed" --tags={"task": name, "error": e}
        error_ e

  /**
  Variant of $(start --options).

  Starts the client with default session options.
  If $client-id is given, uses it as the client ID.
  The $SessionOptions.clean-session flag is set if the client-id is equal to "".
  */
  start -> none
      --drop-incoming/bool=false
      --client-id/string=""
      --on-error/Lambda=(:: /* Do nothing. */):
    clean-session := client-id == ""
    options := SessionOptions --client-id=client-id --clean-session=clean-session
    start --options=options --on-error=on-error --drop-incoming=drop-incoming

  /**
  Starts the client with the given $options.

  At this point the client is connected through the transport, but no
    messages have been sent yet.

  The $on-error callback is called when an error occurs while reading
    data and no $publish is in progress. The error is passed as the first
    argument to the callback. Users don't need to handle errors this way.
    They are automatically resurfaced at the next $publish. That is, if an
    error occurred at some point, the next $publish will fail with the same
    error.

  If starting fails, it is recommended to $close the client and start over.

  If $drop-incoming is true, then incoming publish packets are dropped.
    This can be used if the client is known to only send packets. It should
    never be necessary, but may protect against bugs in the broker or
    bad session handling.
  */
  start -> none
      --options/SessionOptions
      --on-error/Lambda=(:: /* Do nothing */)
      --drop-incoming/bool=false:
    packet := ConnectPacket options.client-id
        --clean-session=options.clean-session
        --username=options.username
        --password=options.password
        --keep-alive=options.keep-alive
        --last-will=options.last-will
    writer_.write packet.serialize

    response := Packet.deserialize reader_
    if not response: throw CONNECTION-CLOSED_
    if response is not ConnAckPacket:
      logger_.error "expected ConnAckPacket" --tags={
        "response-type": response.type
      }
      error_ PROTOCOL-ERROR_ --do-throw
    ack := response as ConnAckPacket
    return-code := ack.return-code
    if return-code != 0:
      refused-reason := refused-reason-for-return-code_ return-code
      logger_.error "connection refused" --tags={
        "reason": refused-reason
      }
      error_ refused-reason --do-throw

    state_ = STATE-CONNECTED_

    reader-task_ = run-protected-task_ "incoming" --background::
      try:
        handle-incoming_ --drop-incoming=drop-incoming
      finally:
        reader-task_ = null
        // If we are not closed, we need to tear down.
        // This is the clean way of shutting down.
        tear-down_

    pinger-task_ = run-protected-task_ "pinger" --background::
      try:
        start-pings_ --keep-alive=options.keep-alive
      finally:
        pinger-task_ = null

  handle-incoming_ --drop-incoming/bool:
    while state_ == STATE-CONNECTED_ or state_ == STATE-DISCONNECTED_:
      packet := Packet.deserialize reader_
      if state_ != STATE-CONNECTED_: break
      if not packet:
        if state_ == STATE-DISCONNECTED_ or state_ == STATE-CLOSED_:
          return
        else:
          throw CONNECTION-CLOSED_

      logger_.debug "received packet" --tags={"packet": packet}
      if packet is PingRespPacket:
        continue
      else if packet is PublishPacket:
        // Load the payload into memory.
        packet.payload
        if not drop-incoming:
          receive-publish-packet_ packet as PublishPacket
      else:
        // When senders want to receive an ack, they need to start
        // listening to the received-signal. By wrapping the assignment
        // of the packet into a mutex we guarantee that they have the
        // time to do so.
        connection-monitor_.do:
          last-received_ = packet

      // Notify any listeners that a packet has been received.
      connection-monitor_.raise

  /**
  Waits for a packet to arrive.
  Does not take any mutex. Expects the caller to have taken
    $connection-monitor_.

  The $block is called whenever a new packet is received.
  */
  loock-for-received_ -> bool
      --without-mutex/True
      --kind/string
      --allow-close/bool=false
      [block]:
    // As long as the condition is met we don't look for connection
    // issues yet. This way we can dispatch received packets even
    // if the connection is dead.
    if block.call: return true
    if state_ != STATE-CONNECTED_:
      if allow-close: return true
      logger_.error "connection closed while waiting for packet"
          --tags={
            "state": state_,
            "waiting-for": kind,
          }
      error_ CONNECTION-CLOSED_ --do-throw
    return false

  start-pings_ --keep-alive/Duration:
    while state_ == STATE-CONNECTED_:
      sleep keep-alive
      send-packet_ PingReqPacket

  /**
  Sends the given packet.
  */
  send-packet_ --without-mutex/True packet/Packet:
    if state_ == STATE-CLOSED_: throw CONNECTION-CLOSED_
    try:
      catch --trace --unwind=true:
        writer_.write packet.serialize
    finally: | is-exception exception |
      if is-exception:
        logger_.debug "failed to send packet" --tags={
          "error": exception.value
        }
        error_ exception.value

  send-packet_ packet/Packet:
    connection-monitor_.do:
      check-connection_
      send-packet_ --without-mutex packet

  send-and-wait-for-ack_ packet/Packet --packet-id/int --kind/string:
    logger_.debug "sending and waiting for ack" --tags={"packet-id": packet-id}
    connection-monitor_.do-wait
        --do=: send-packet_ --without-mutex packet
        --wait=:
          loock-for-received_ --without-mutex --kind=kind:
            if last-received_ is AckPacket:
              ack := last-received_ as AckPacket
              if ack.packet-id == packet-id:
                logger_.debug "ack received" --tags={"packet-id": packet-id}
              ack.packet-id == packet-id
            else:
              false

  next-packet-id_ -> int:
    result := packet-id_++ & 0xFFFF
    if result == 0: return next-packet-id_
    return result

  check-connection_:
    if error_: throw error_
    if state_ == STATE-CLOSED_: throw CONNECTION-CLOSED_
    if state_ != STATE-CONNECTED_ and state_ != STATE-DISCONNECTED_:
      throw "NOT_CONNECTED"

  /**
  Sends the $payload to the $topic.

  If $qos is 0, the message is sent, but no acknowledgment is expected.
  if $qos is 1, then the message is sent, and the broker will acknowledge
    that it received the message. This function blocks until the acknowledgment
    is received. Consider using $with-timeout to avoid blocking indefinitely.
  */
  publish topic/string payload/io.Data --qos/int=0 --retain/bool=false:
    packet-id := next-packet-id_
    payload-bytes := payload is ByteArray ? payload : ByteArray.from payload
    packet := PublishPacket topic payload-bytes
        --qos=qos
        --retain=retain
        --packet-id=(qos != 0 ? packet-id : null)
    if qos == 1:
      send-and-wait-for-ack_ packet --packet-id=packet-id --kind="PubAck"
    else:
      send-packet_ packet

  /**
  Subscribes to the given $topic.

  The $topic might have wildcards and is thus more of a filter than a single topic.

  The $max-qos is the maximum quality of service that the client wants to receive
    messages with. The broker might send messages with a lower QoS, but never with
    a higher QoS. If the broker received a message with a higher QoS, it will send
    the message with the reduced QoS to the client.

  Blocks until the subscription is acknowledged.
  */
  subscribe topic/string --max-qos/int=1:
    packet-id := next-packet-id_
    topic-qos := [TopicQos topic --max-qos=max-qos]
    packet := SubscribePacket topic-qos --packet-id=packet-id
    send-and-wait-for-ack_ packet --packet-id=packet-id --kind="SubAck"

  /**
  Unsubscribes from the given $topic.

  Blocks until the unsubscription is acknowledged.
  */
  unsubscribe topic/string:
    packet-id := next-packet-id_
    packet := UnsubscribePacket [topic] --packet-id=packet-id
    send-and-wait-for-ack_ packet --packet-id=packet-id --kind="UnsubAck"

  /**
  The number of received packets that are currently in the queue.
  */
  received-count -> int:
    return received-queue_.size

  /**
  Receives a packet.

  Blocks until a packet is received.
  Returns null if the client is closed.
  The $PublishPacket.payload is already read into memory.
  */
  receive -> PublishPacket?:
    e := catch --unwind=(: state_ != STATE-DISCONNECTED_ or state_ == STATE-CLOSED_):
      connection-monitor_.wait:
        loock-for-received_ --allow-close --kind="receive" --without-mutex:
          not received-queue_.is-empty
      if received-queue_.is-empty:
        // We are closed.
        return null
      return received-queue_.remove-first
    return null

  receive-publish-packet_ packet/PublishPacket:
    received-queue_.add packet
    if packet.qos == 0: return
    if packet.qos == 1:
      logger_.debug "sending ack" --tags={"packet-id": packet.packet-id}
      ack := PubAckPacket --packet-id=packet.packet-id
      catch --trace: send-packet_ ack

monitor MutexSignal:
  do [block]:
    block.call

  wait [block]:
    await block

  do-wait [--do] [--wait]:
    do.call
    await wait

  raise:
