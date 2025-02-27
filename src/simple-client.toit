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
  an earlier packet), or other advanced features.

If you just want to send messages to a broker, this client is a good choice.
  If you need more advanced features, consider using the $Client or
  $FullClient class.

The client is not thread-safe. It is expected that only one task uses the client
  at a time. If multiple tasks need to use the client, they must synchronize
  access to the client.

Received packets are enqueued until they are read. The client automatically
  acknowledges packets with QoS 1.
*/

/**
A simple MQTT client that can send messages to a broker.
*/
class SimpleClient:
  static CLOSED-ERROR_ ::= "CONNECTION-CLOSED"

  /**
  The client is not connected/started.
  The underlying transport is independent and likely already connected.
  */
  static STATE-DISCONNECTED_ ::= 0
  /**
  The client established a connection to the broker, and got an acknowledgment.
  */
  static STATE-CONNECTED_ ::= 1
  /**
  The client encountered an error and is in a bad state.
  */
  static STATE-ERROR_ ::= 2
  /**
  The client is closed and cannot be used anymore.
  */
  static STATE-CLOSED_ ::= 3

  transport_/Transport
  logger_/log.Logger
  on-error_/Lambda? := null
  state_/int := STATE-DISCONNECTED_
  reader_ /io.Reader
  writer_ /io.Writer
  reader-task_/Task? := null
  pinger-task_/Task? := null
  connection-monitor_/MutexSignal := MutexSignal
  received-signal_/monitor.Signal := monitor.Signal
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
  */
  close:
    if state_ == STATE-CLOSED_: return
    state_ = STATE-CLOSED_
    critical-do:
      if reader-task_: reader-task_.cancel
      if pinger-task_: pinger-task_.cancel
      transport_.close

  error_ reason/string --tags/Map? --do-throw/bool=false:
    if state_ != STATE-ERROR_:
      error_ = reason
      logger_.error reason --tags=tags
      state_ = STATE-ERROR_
      if on-error_: on-error_.call reason
      received-signal_.raise
    if do-throw: throw reason

  run-protected-task_ name/string fun/Lambda --background/bool -> Task:
    return task --background=background::
      e := catch:
        fun.call
      if e:
        error_ "Task $name failed" --tags={"error": e}

  /**
  Variant of $(start --options).

  Starts the client with default session options.
  If $client-id is given, uses it as the client ID.
  The $SessionOptions.clean-session flag is always set to true.
  */
  start -> none
      --drop-incoming/bool=false
      --client-id/string = ""
      --on-error/Lambda=(:: /* Do nothing. */):
    options := SessionOptions --client-id=client-id --clean-session
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
    if not response: throw "CONNECTION_CLOSED"
    if response is not ConnAckPacket:
      error_ --do-throw "Expected ConnAckPacket" --tags={
        "response-type": response.type
      }
    ack := response as ConnAckPacket
    return-code := ack.return-code
    if return-code != 0:
      refused-reason := refused-reason-for-return-code_ return-code
      error_ --do-throw "Connection refused" --tags={
        "reason": refused-reason
      }

    state_ = STATE-CONNECTED_
    reader-task_ = run-protected-task_ "incoming" --background::
      try:
        handle-incoming_ --drop-incoming=drop-incoming
      finally:
        reader-task_ = null
    pinger-task_ = run-protected-task_ "pinger" --background::
      try:
        start-pings_ --keep-alive=options.keep-alive
      finally:
        pinger-task_ = null

  handle-incoming_ --drop-incoming/bool:
    while state_ == STATE-CONNECTED_:
      packet := Packet.deserialize reader_
      if state_ != STATE-CONNECTED_: break
      if not packet:
        throw "CONNECTION_CLOSED"
      if packet is PingRespPacket: continue
      if packet is PublishPacket:
        if not drop-incoming:
          receive-publish-packet_ packet as PublishPacket
      else:
        // When senders want to receive an ack, they need to start
        // listening to the received-signal. By wrapping the assignment
        // of the packet into a mutex we guarantee that they have the
        // time to do so.
        logger_.debug "received packet" --tags={"packet": packet}
        connection-monitor_.do:
          logger_.debug "received packet in mutex"
          last-received_ = packet
      logger_.debug "notifying"
      received-signal_.raise

  /**
  Waits for a packet to arrive.
  Does not take any mutex. Expects the caller to have taken
    $connection-monitor_.

  The $block is called whenever a new packet is received.
  */
  wait-for-received_ --without-mutex/True [block]:
    if block.call: return
    while true:
      received-signal_.wait
      if state_ != STATE-CONNECTED_:
        error_ "Connection closed" --do-throw --tags=null
      if block.call:
        logger_.debug "waiter is happy"
        return

  start-pings_ --keep-alive/Duration:
    while state_ == STATE-CONNECTED_:
      sleep keep-alive
      send-packet_ PingReqPacket

  /**
  Sends the given packet.
  */
  send-packet_ --without-mutex/True packet/Packet:
    if state_ != STATE-CONNECTED_:
      throw "NOT_CONNECTED"
    e := catch:
      writer_.write packet.serialize
    if e:
      error_ "Failed to send packet" --tags={"error": e} --do-throw

  send-packet_ packet/Packet:
    connection-monitor_.do:
      check-connection_
      send-packet_ --without-mutex packet

  send-and-wait-for-ack_ packet/Packet --packet-id/int:
    connection-monitor_.do-wait
        --do=: send-packet_ --without-mutex packet
        --wait=:
          logger_.debug "waiting for ack" --tags={"packet-id": packet-id}
          wait-for-received_ --without-mutex:
            if last-received_ is AckPacket:
              ack := last-received_ as AckPacket
              ack.packet-id == packet-id
            else:
              false

  next-packet-id_ -> int:
    result := packet-id_++ & 0xFFFF
    if result == 0: return next-packet-id_
    return result

  check-connection_:
    if state_ == STATE-ERROR_: throw error_
    if state_ != STATE-CONNECTED_: throw "NOT_CONNECTED"

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
        --packet-id=packet-id
    if qos == 1:
      send-and-wait-for-ack_ packet --packet-id=packet-id
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
  subscribe topic/string --max-qos/int=0:
    packet-id := next-packet-id_
    topic-qos := [TopicQos topic --max-qos=max-qos]
    packet := SubscribePacket topic-qos --packet-id=packet-id
    send-and-wait-for-ack_ packet --packet-id=packet-id

  /**
  Unsubscribes from the given $topic.

  Blocks until the unsubscription is acknowledged.
  */
  unsubscribe topic/string:
    packet-id := next-packet-id_
    packet := UnsubscribePacket [topic] --packet-id=packet-id
    send-and-wait-for-ack_ packet --packet-id=packet-id

  /**
  Receives a packet.

  Blocks until a packet is received.
  Returns null if the client is closed.
  */
  receive -> PublishPacket?:
    e := catch --unwind=(: state_ != STATE-CLOSED_):
      connection-monitor_.wait:
        wait-for-received_ --without-mutex: not received-queue_.is-empty
      return received-queue_.remove-first
    return null

  receive-publish-packet_ packet/PublishPacket:
    received-queue_.add packet
    if packet.qos == 0: return
    if packet.qos == 1:
      ack := PubAckPacket --packet-id=packet.packet-id
      send-packet_ ack

monitor MutexSignal:
  do [block]:
    try:
      print "enter"
      block.call
    finally:
      print "exit"

  wait [block]:
    print "wait-start"
    await block
    print "wait-end"

  do-wait [--do] [--wait]:
    print "enter"
    do.call
    print "now wait"
    await wait
    print "exit"
