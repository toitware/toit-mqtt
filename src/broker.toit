// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

/**
A simple MQTT broker library.

This implementation was created for testing, but is fully functional.
*/

import io
import log
import monitor
import .packets
import .last-will
import .topic-qos
import .topic-tree
import .utils_

/**
The transport interface used by the broker.
*/
interface BrokerTransport:
  write bytes/ByteArray -> int
  read -> ByteArray?
  close -> none

/**
The transport interface allowing the broker to listen to incoming connections.
*/
interface ServerTransport:
  /**
  Listens for incoming connections and calls the $callback whenever a client connects.
  Calls the $callback with a $BrokerTransport as argument.
  */
  listen callback/Lambda -> none
  close -> none

/**
A reader that can timeout.
*/
class TimeoutReader_ extends io.Reader:
  transport_ /BrokerTransport
  timeout_ /Duration? := null

  constructor .transport_:

  read_ -> ByteArray?:
    if timeout_:
      with-timeout timeout_: return transport_.read
    return transport_.read

  set-timeout timeout/Duration:
    timeout_ = timeout

class TransportWriter_ extends io.Writer:
  transport_ /BrokerTransport

  constructor .transport_:

  try-write_ data/io.Data from/int to/int -> int:
    return transport_.write (io-data-to-byte-array_ data from to)

class Connection_:
  transport_ /BrokerTransport
  reader_ /TimeoutReader_
  writer_ /io.Writer

  constructor .transport_:
    reader_ = TimeoutReader_ transport_
    writer_ = TransportWriter_ transport_

  read -> Packet?:
    return Packet.deserialize reader_

  write packet/Packet:
    writer_.write packet.serialize

  close -> none:
    transport_.close

  set-read-timeout duration/Duration:
    reader_.set-timeout duration


monitor QueuedMessages_:
  // Messages that haven't been sent yet.
  queued_ /Deque := Deque
  // Acks that haven't been sent yet.
  queued-acks_ /Deque := Deque

  next -> Packet?:
    await: not queued_.is-empty or not queued-acks_.is-empty
    if not queued-acks_.is-empty: return queued-acks_.remove-first
    return queued_.remove-first

  add-ack packet/Packet:
    queued-acks_.add packet

  add-packet packet/Packet:
    queued_.add packet

  size -> int:
    return queued_.size + queued-acks_.size

/**
A client session.

This object keeps track of all the data needed to maintain a session state for
  each client.

If the client connects with a 'clean_session' flag, then this object only stays
  alive until the client disconnects. Otherwise, it is kept forever.

If a client subscribes to messages, but never connects again, the data is just
  saved in the session. Eventually, the broker will run out of memory.
*/
class Session_:
  static STATE-CREATED_ ::= 0
  static STATE-RUNNING_ ::= 1
  static STATE-DISCONNECTED_  ::= 2
  // Note that a state can go from disconnected to running again.
  state_ /int := STATE-CREATED_

  client-id /string
  broker /Broker
  clean-session /bool
  logger_ /log.Logger

  subscription-tree_ /TopicTree ::= TopicTree
  connection_ /Connection_? := null
  reader-task_ /Task? := null
  writer-task_ /Task? := null

  queued_ /QueuedMessages_ ::= QueuedMessages_

  // Messages that have been sent but not yet acknowledged.
  waiting-for-ack_ /Map := {:}

  last-will_ /LastWill? := null

  next-packet-id_ := 0

  constructor .client-id --logger/log.Logger --.broker/Broker --.clean-session/bool:
    logger_ = logger

  run connection/Connection_ --keep-alive/Duration --last-will/LastWill?:
    // Note that there could be a race condition here:
    // If there are multiple requests for the same client, then we might be
    // in the process of closing, while another task enters here.
    if state_ == STATE-RUNNING_: disconnect --reason="already running"

    state_ = STATE-RUNNING_
    last-will_ = last-will

    connection_ = connection

    if not keep-alive.is-zero:
      connection.set-read-timeout (keep-alive * 2)

    reader-task_ = task::
      exception := catch --trace:
        while true:
          packet := connection.read
          if not packet and state_ != STATE-DISCONNECTED_:
            logger_.info "client $client-id disconnected"
            disconnect --reason="CLIENT_DISCONNECTED"
            break
          logger_.debug "received $(Packet.debug-string_ packet) from client $client-id"
          try:
            handle packet
          finally: | is-exception _ |
            if is-exception:
              logger_.error "error handling packet $(Packet.debug-string_ packet)"
      if exception: disconnect --reason=exception

    writer-task_ = task::
      exception := catch --trace:
        if not waiting-for-ack_.is-empty:
          waiting-for-ack_.do --values: | packet/PublishPacket |
            duped := packet.with --duplicate=true
            connection_.write duped

        while true:
          packet := queued_.next
          logger_.debug "writing $(Packet.debug-string_ packet)"
          if packet is PublishPacket:
            publish := packet as PublishPacket
            if publish.qos > 0:
              waiting-for-ack_[publish.packet-id] = publish

          connection_.write packet
      logger_.info "client $client-id writer task closed with $queued_.size messages pending"

  handle packet/Packet:
    if packet is SubscribePacket:
      subscribe-packet := packet as SubscribePacket
      subscribe subscribe-packet
      return

    if packet is UnsubscribePacket:
      unsubscribe-packet := packet as UnsubscribePacket
      unsubscribe unsubscribe-packet
      return

    if packet is PublishPacket:
      publish-packet := packet as PublishPacket
      publish publish-packet
      return

    if packet is PingReqPacket:
      ping
      return

    if packet is DisconnectPacket:
      disconnect
      return

    if packet is PubAckPacket:
      id := (packet as PubAckPacket).packet-id
      waiting-for-ack_.remove id
      return

    logger_.warn "unhandled packet $(Packet.debug-string_ packet)"

  subscribe packet/SubscribePacket:
    result-qos := []
    allow-plus := true
    last-was-plus := false
    packet.topics.do: | topic-qos/TopicQos |
      topic := topic-qos.topic
      for i := 0; i < topic.size; i++:
        char := topic[i]
        if not char: continue  // Unicode character.
        if last-was-plus and char != '/': throw "INVALID_SUBSCRIPTION: $topic"
        if char == '+':
          if not allow-plus: throw "INVALID_SUBSCRIPTION: $topic"
          else: last-was-plus = true
        else:
          last-was-plus = false
        allow-plus = char == '/'

        if char == '#' and i != topic.size - 1:
          throw "INVALID_SUBSCRIPTION: $topic"

      if not 0 <= topic-qos.max-qos <= 2:
        throw "INVALID_SUBSCRIPTION: $topic ($topic-qos.max-qos)"

      accepted-qos := min topic-qos.max-qos 1
      subscription-tree_.set topic accepted-qos
      result-qos.add accepted-qos
    send_ (SubAckPacket --qos=result-qos --packet-id=packet.packet-id)

    packet.topics.do: | topic-qos/TopicQos |
      topic := topic-qos.topic
      broker.retained.do topic --all: | retained/PublishPacket |
        qos := min topic-qos.max-qos  retained.qos
        packet-id := qos > 0 ? next-packet-id_++ : null
        send_ (retained.with --packet-id=packet-id --retain --qos=qos)

  unsubscribe packet/UnsubscribePacket:
    packet.topics.do: | topic |
      existed := subscription-tree_.remove topic
      if not existed:
        logger_.info "client $client-id unsubscribed from non-existent topic $topic"
    send_ (UnsubAckPacket --packet-id=packet.packet-id)

  publish packet/PublishPacket:
    topic := packet.topic
    if topic == "" or topic.contains "#" or topic.contains "+":
      throw "INVALID PUBLISH TOPIC. NO WILD CARDS ALLOWED. $packet.topic"
    needs-ack := packet.qos > 0
    if needs-ack:
      packet-id := packet.packet-id
      send-ack_ (PubAckPacket --packet-id=packet-id)

    broker.publish packet

  ping:
    send_ (PingRespPacket)

  disconnect --reason=null -> none:
    if state_ == STATE-DISCONNECTED_: return
    state_ = STATE-DISCONNECTED_
    reason-suffix := reason ? " ($reason)" :""
    logger_.info "client $client-id closing$reason-suffix"
    if connection_:
      connection_.close
      connection_ = null

    // Send the last will before we kill all tasks.
    // Otherwise we will cancel the task on which we currently run on.
    if reason and last-will_:
      packet-id := last-will_.qos > 0 ? next-packet-id_++ : null
      packet := PublishPacket last-will_.topic last-will_.payload \
          --qos=last-will_.qos --packet-id=packet-id --retain=last-will_.retain
      broker.publish packet

    if clean-session: broker.remove-session_ client-id

    // Cancel the writer_task_ first, since the reader task might be
    // the one calling the disconnect.
    assert: writer-task_ != Task.current
    if writer-task_:
      writer-task_.cancel
      writer-task_ = null
    if reader-task_:
      reader-task := reader-task_
      reader-task_ = null
      reader-task.cancel

  send_ packet/Packet:
    queued_.add-packet packet

  send-ack_ ack/Packet:
    queued_.add-ack ack

  dispatch-incoming-publish packet/PublishPacket:
    // There doesn't seem to be a rule which qos we should use if multiple
    // subscriptions match. We thus use the one from the most specialized.
    subscription-tree_.do --most-specialized packet.topic: | subscription-max-qos |
      qos := min packet.qos subscription-max-qos
      if state_ != STATE-RUNNING_ and qos == 0:
        // We don't queue qos=0 packets if the client is disconnected.
        // In theory we could/should also delete queued messages if the
        // client disconnects after we queued them.
        continue.do
      NO-PACKET-ID ::= -1  // See $PublishPacket.with.
      packet-id := qos > 0 ? next-packet-id_++ : NO-PACKET-ID
      send_ (packet.with --packet-id=packet-id --qos=qos)

/** An unbounded channel for publish messages. */
class PublishChannel_:
  // Messages that haven't been sent yet.
  queued_ /Deque := Deque
  semaphore_ /monitor.Semaphore := monitor.Semaphore

  next -> Packet:
    semaphore_.down
    return queued_.remove-first

  add packet/Packet:
    queued_.add packet
    semaphore_.up

/**
An MQTT broker.
*/
class Broker:
  sessions_ /Map ::= {:}
  server-transport_ /ServerTransport
  logger_ /log.Logger
  publish-channel_ /PublishChannel_ ::= PublishChannel_

  retained /TopicTree ::= TopicTree

  constructor .server-transport_ --logger/log.Logger=log.default:
    logger_ = logger

  start:
    logger_.info "starting broker"

    publish-task := task --background::
      while true:
        packet := publish-channel_.next
        sessions_.do  --values: | session |
          session.dispatch-incoming-publish packet

    try:
      server-transport_.listen::
        logger_.info "connection established"
        connection := Connection_ it
        exception := catch --trace:
          packet := connection.read
          if not packet:
            logger_.info "connection was closed"
            connection.close
            continue.listen

          logger_.info "read packet $(Packet.debug-string_ packet)"
          if packet is not ConnectPacket:
            logger_.error "didn't receive connect packet, but got packet of type $packet.type"
            connection.close
            continue.listen

          connect := packet as ConnectPacket

          logger_.info "new connection-request: $(Packet.debug-string_ connect)"

          client-id := connect.client-id
          connack /ConnAckPacket ::= ?

          clean-session := connect.clean-session
          if client-id == "": client-id = "unknown-$(random)"
          session-present /bool ::= ?
          session /Session_? := sessions_.get connect.client-id
          if session and (clean-session or session.clean-session):
            logger_.info "removing existing session for client $client-id"
            session.disconnect
            session = null
          if session:
            logger_.info "existing session for client $client-id"
            session-present = true
          else:
            logger_.info "new session for client $client-id"
            session = Session_ client-id --broker=this --logger=logger_ --clean-session=clean-session
            sessions_[connect.client-id] = session
            session-present = false

          session.run connection --keep-alive=connect.keep-alive --last-will=connect.last-will
          connack = ConnAckPacket --session-present=session-present --return-code=0x00

          connection.write connack

          // Currently we always succeed the connection, so the following 'if' never triggers.
          if connack.return-code != 0:
            connection.close
            continue.listen
    finally:
      publish-task.cancel

  publish packet/PublishPacket:
    logger_.info "publishing $(Packet.debug-string_ packet)"
    if packet.retain:
      if packet.payload.size == 0: retained.remove packet.topic
      else: retained.set packet.topic packet
      packet = packet.with --no-retain

    // Hand over the packet to the publish channel.
    // We can't notify the sessions ourselves as the current task might be killed soon.
    publish-channel_.add packet

  remove-session_ client-id/string:
    sessions_.remove client-id
