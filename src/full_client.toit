// Copyright (C) 2022 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import io
import monitor
import log

import .session-options
import .last-will
import .packets
import .tcp  // For toitdoc.
import .topic-qos
import .transport
import .utils_

CLIENT-CLOSED-EXCEPTION ::= "CLIENT_CLOSED"

/**
A class that ensures that the connection to the broker is kept alive.

When necessary sends ping packets to the broker.
*/
class ActivityChecker_:
  connection_ /Connection_
  keep-alive_ /Duration
  current-connection_ /Connection_? := null

  constructor .connection_ --keep-alive/Duration:
    keep-alive_ = keep-alive

  /**
  Checks for activity.

  Returns a duration for when it wants to be called again.
  */
  check -> Duration:
    // TODO(florian): we should be more clever here:
    // We should monitor when the transport starts writing, and when it gets a chunk through.
    // Also, we should monitor that we actually get something from the server.
    last-write-us := connection_.transport_.last-write-us
    remaining-keep-alive-us := keep-alive_.in-us - (Time.monotonic-us - last-write-us)
    if remaining-keep-alive-us > 0:
      remaining-keep-alive := Duration --us=remaining-keep-alive-us
      return remaining-keep-alive
    else if not connection_.is-writing:
      // TODO(florian): we need to keep track of whether we have sent a ping.
      connection_.write PingReqPacket
      return keep-alive_ / 2
    else:
      // TODO(florian): we are currently sending.
      // We should detect timeouts on the sending.
      return keep-alive_

  run:
    while not Task.current.is-canceled and not connection_.is-closed:
      catch:
        duration := check
        sleep duration

/**
A connection to the broker.

Primarily ensures that exceptions are handled correctly.
The first side (reading or writing) that detects an issue with the transport disconnects the transport.
  It throws the original exception. The other side simply throws a $CLIENT-CLOSED-EXCEPTION.

Also sends ping requests to keep the connection alive (if $keep-alive is called).
*/
class Connection_:
  /**
  The connection is considered alive.
  This could be because we haven't tried to establish a connection, but it
    could also be that we are happily running.
  */
  static STATE-ALIVE_ ::= 0
  /**
  The connection is closed or in the process of closing.
  Once connected, if there is an error during receiving or sending, the connection will
    switch to this state and call $Transport.close. This will cause the
    other side (receive or send) to shut down as well (if there is any).
  */
  static STATE-CLOSED_ ::= 1

  state_ / int := STATE-ALIVE_

  transport_ / ActivityMonitoringTransport_
  reader_ /io.Reader
  writer_ /io.Writer
  writing_ /monitor.Mutex ::= monitor.Mutex
  is-writing_ /bool := false

  closing-reason_ /any := null

  logger_ /log.Logger

  keep-alive-duration_ /Duration?
  // TODO(florian): the following field should be typed as `Task?`.
  // However, that class is only available in Toit 2.0.
  activity-task_ /any := null

  /** Constructs a new connection. */
  constructor .transport_ --keep-alive/Duration? --logger/log.Logger:
    reader_ = TransportReader_ transport_
    writer_ = TransportWriter_ transport_
    keep-alive-duration_ = keep-alive
    logger_ = logger

  is-alive -> bool: return state_ == STATE-ALIVE_
  is-closed -> bool: return state_ == STATE-CLOSED_

  /**
  Starts a task to keep this connection to the broker alive.
  Sends ping requests when necessary.
  */
  keep-alive --background/bool:
    assert: background
    if activity-task_: throw "ALREADY_RUNNING"
    if keep-alive-duration_ == Duration.ZERO: return

    activity-task_ = task --background::
      try:
        checker := ActivityChecker_ this --keep-alive=keep-alive-duration_
        checker.run
      finally:
        activity-task_ = null

  /**
  Receives an incoming packet.
  */
  read -> Packet?:
    if is-closed: throw CLIENT-CLOSED-EXCEPTION
    try:
      catch --unwind=(: not is-closed):
        return Packet.deserialize reader_
      if closing-reason_: throw closing-reason_
      return null
    finally: | is-exception exception |
      if is-exception:
        close --reason=exception.value
        state_ = STATE-CLOSED_

  /** Whether the connection is in the process of writing. */
  is-writing -> bool:
    return is-writing_

  /**
  Writes the given $packet, serializing it first.
  */
  write packet/Packet:
    // The client already serializes most sends. However, some messages are written without
    // taking the client's lock. For example, 'ack' messages, the 'disconnect' message, or pings
    // are directly written to the connection (jumping the queue).
    writing_.do:
      if is-closed: throw CLIENT-CLOSED-EXCEPTION
      try:
        is-writing_ = true
        exception := catch --unwind=(: not is-closed):
          writer_.write packet.serialize
        if exception:
          assert: is-closed
          throw CLIENT-CLOSED-EXCEPTION
      finally: | is-exception exception |
        is-writing_ = false
        if is-exception:
          close --reason=exception.value

  /**
  Closes the connection.

  If $reason is not null, the closing happens due to an error.
  Closes the underlying transport.
  */
  close --reason=null:
    if is-closed: return
    logger_.debug "closing connection" --tags={"reason": reason}
    assert: closing-reason_ == null
    critical-do:
      closing-reason_ = reason
      // By setting the state to closed we quell any error messages from disconnecting the transport.
      state_ = STATE-CLOSED_
      if activity-task_:
        activity-task_.cancel
        activity-task_ = null
      catch: transport_.close

/**
A strategy to connect to the broker.

This class deals with (temporary) disconnections and timeouts.
It keeps track of whether trying to connect again makes sense, and when it
  should try again.

It is also called for the first time the client connects to the broker.
*/
interface ReconnectionStrategy:
  /**
  Is called when the client wants to establish a connection through the given $transport.

  Returns null if the strategy has been closed.
  Otherwise, returns whether the broker had a session for this client (the result of $receive-connect-ack).

  The strategy should first call $send-connect, followed by a $receive-connect-ack. If
    the connection is unsuccessful, it may retry.

  The $disconnect-transport may be called before attempting a reconnect. It shuts down the
    network, thus increasing the chance of a successful reconnect. However, a complete
    network reconnect takes more time. We recommend to try at least once to reconnect without
    a disconnect first, and then call $disconnect-transport for later attempts.

  The $receive-connect-ack block returns whether the broker had a session for this client.

  The $is-initial-connection is true if this is the first time the client connects to the broker.
  */
  connect -> bool?
      transport/ActivityMonitoringTransport_
      --is-initial-connection /bool
      [--reconnect-transport]
      [--disconnect-transport]
      [--send-connect]
      [--receive-connect-ack]
      [--disconnect]

  /** Whether the client should even try to reconnect. */
  should-try-reconnect transport/ActivityMonitoringTransport_ -> bool

  /** Whether the strategy is closed. */
  is-closed -> bool

  /** Closes the strategy, indicating that no further reconnection attempts should be done. */
  close -> none

/**
A latch that has a timeout when getting its value.
*/
monitor WaitSignal_:
  has-triggered_ /bool := false

  /**
  Either returns true if the signal is triggered or null if the timeout is reached.
  */
  wait --timeout/Duration -> bool?:
    try-await --deadline=(Time.monotonic-us + timeout.in-us): has-triggered_
    if has-triggered_: return true
    return null

  trigger -> none:
    has-triggered_ = true

/**
A base class for reconnection strategies.
*/
abstract class ReconnectionStrategyBase implements ReconnectionStrategy:
  static DEFAULT-RECEIVE-CONNECT-TIMEOUT /Duration ::= Duration --s=5
  static DEFAULT-ATTEMPT-DELAYS /List ::= [
    Duration --s=1,
    Duration --s=5,
    Duration --s=15,
  ]
  /**
  The default duration to consider a new connection attempt to be independent
    from previous attempts.

  If there is a connection attempt within this duration, then the strategy
    assumes that we managed to connect, but got disconnected immediately after.
    It then treats this as if we didn't manage to connect at all, increasing
    the attempts and thus the timeouts.
  */
  static DEFAULT-RESET-DURATION /Duration ::= Duration --s=1

  receive-connect-timeout_ /Duration
  attempt-delays_ /List?
  attempt_/int := -1  // Incremented before each connection attempt.
  last-attempt-time_/int := -1
  reset-duration-us_/int
  delay-lambda_ /Lambda?

  is-closed_ := false

  logger_/log.Logger

  /**
  A latch that is set when the client is closed.
  */
  closed-signal_ /WaitSignal_ := WaitSignal_

  constructor
      --receive-connect-timeout /Duration = DEFAULT-RECEIVE-CONNECT-TIMEOUT
      --reset-duration /Duration = DEFAULT-RESET-DURATION
      --delay-lambda /Lambda? = null
      --attempt-delays /List? /*Duration*/ = (delay-lambda ? null : DEFAULT-ATTEMPT-DELAYS)
      --logger/log.Logger?=log.default:
    if delay-lambda and attempt-delays: throw "BOTH_DELAY_LAMBDAS_AND_DELAYS"
    receive-connect-timeout_ = receive-connect-timeout
    reset-duration-us_ = reset-duration.in-us
    delay-lambda_ = delay-lambda
    attempt-delays_ = attempt-delays
    logger_ = logger

  /**
  Tries to connect, potentially retrying with delays.

  Returns null if the strategy was closed.
  Returns whether the broker had a session for this client, otherwise.
  */
  do-connect -> bool?
      transport/ActivityMonitoringTransport_
      --reuse-connection/bool=false
      [--reconnect-transport]
      [--disconnect-transport]
      [--send-connect]
      [--receive-connect-ack]:
    current-attempt-time := Time.monotonic-us
    // If we try to do a connection rapidly after we managed to connect, then we
    // got probably disconnected. In that case, treat this as if we didn't manage to
    // connect and keep the last attempt counter to have some back-off.
    if current-attempt-time - last-attempt-time_ > reset-duration-us_:
      // Consider this a completely fresh attempt to connect.
      attempt_ = -1

    while true:
      if is-closed: return null

      attempt_++

      is-last-attempt := false
      if attempt-delays_:
        if attempt_ >= attempt-delays_.size:
          attempt_ = attempt-delays_.size
          is-last-attempt = true

      try:
        catch --unwind=(: is-closed or is-last-attempt):

          if attempt_ == 0:
            // In the first iteration we try to connect without delay.
            if not reuse-connection:
              logger_.debug "attempting to (re)connect"
              reconnect-transport.call
          else:
            sleep-duration/Duration := ?
            if attempt-delays_:
              sleep-duration = attempt-delays_[attempt_ - 1]
            else:
              sleep-duration = delay-lambda_.call attempt_
            disconnect-transport.call
            closed-signal_.wait --timeout=sleep-duration
            if is-closed: return null
            logger_.debug "attempting to (re)connect"
            reconnect-transport.call

          send-connect.call
          logger_.debug "connected to broker"
          return with-timeout receive-connect-timeout_:
            result := receive-connect-ack.call
            logger_.debug "connection established"
            result
      finally: | is-exception _ |
        last-attempt-time_ = Time.monotonic-us
        if is-exception:
          logger_.debug "attempting to (re)connect failed"

    unreachable

  abstract connect -> bool?
      transport/ActivityMonitoringTransport_
      --is-initial-connection /bool
      [--reconnect-transport]
      [--disconnect-transport]
      [--send-connect]
      [--receive-connect-ack]
      [--disconnect]

  abstract should-try-reconnect transport/ActivityMonitoringTransport_ -> bool

  is-closed -> bool:
    return is-closed_

  close -> none:
    is-closed_ = true
    closed-signal_.trigger

/**
Deprecated. Use $ReconnectionStrategyBase instead.
*/
abstract class DefaultReconnectionStrategyBase extends ReconnectionStrategyBase:
  /** Deprecated. Use $ReconnectionStrategyBase.DEFAULT-RECEIVE-CONNECT-TIMEOUT instead. */
  static DEFAULT-RECEIVE-CONNECT-TIMEOUT ::= ReconnectionStrategyBase.DEFAULT-RECEIVE-CONNECT-TIMEOUT
  /** Deprecated. Use $ReconnectionStrategyBase.DEFAULT-ATTEMPT-DELAYS instead. */
  static DEFAULT-ATTEMPT-DELAYS ::= ReconnectionStrategyBase.DEFAULT-ATTEMPT-DELAYS

  constructor
      --receive-connect-timeout /Duration = ReconnectionStrategyBase.DEFAULT-RECEIVE-CONNECT-TIMEOUT
      --delay-lambda /Lambda? = null
      --attempt-delays /List? /*Duration*/ = (delay-lambda ? null : ReconnectionStrategyBase.DEFAULT-ATTEMPT-DELAYS)
      --logger/log.Logger?=log.default:
    super
        --receive-connect-timeout=receive-connect-timeout
        --delay-lambda=delay-lambda
        --attempt-delays=attempt-delays
        --logger=logger

/**
A reconnection strategy that does not attempt to reconnect when the connection
  is interrupted.

This strategy will potentially try to connect multiple times for the initial connection.
*/
class NoReconnectionStrategy extends ReconnectionStrategyBase:

  /**
  Constructs a new reconnection strategy.

  See $ReconnectionStrategyBase for the meaning of the parameters.

  The $delay-lambda and $attempt-delays parameters are only used for the initial connection
    attempt.
  */
  constructor
      --logger/log.Logger=log.default
      --receive-connect-timeout /Duration = ReconnectionStrategyBase.DEFAULT-RECEIVE-CONNECT-TIMEOUT
      --delay-lambda /Lambda? = null
      --attempt-delays /List? /*Duration*/ = null:
    super --logger=logger
        --receive-connect-timeout=receive-connect-timeout
        --delay-lambda=delay-lambda
        --attempt-delays=attempt-delays

  connect -> bool?
      transport/ActivityMonitoringTransport_
      --is-initial-connection /bool
      [--reconnect-transport]
      [--disconnect-transport]
      [--send-connect]
      [--receive-connect-ack]
      [--disconnect]:
    if is-closed: return null
    // No reconnect.
    if not is-initial-connection: throw "NO RECONNECT STRATEGY"
    return do-connect transport
        --reuse-connection = is-initial-connection
        --reconnect-transport = reconnect-transport
        --disconnect-transport = disconnect-transport
        --send-connect = send-connect
        --receive-connect-ack = receive-connect-ack

  should-try-reconnect transport/ActivityMonitoringTransport_ -> bool:
    return false

/**
The default reconnection strategy for clients that are connected with the
  clean session bit.

Since the broker will drop the session information this strategy won't reconnect
  when the connection breaks after it has connected. However, it will potentially
  try to connect multiple times for the initial connection.

Deprecated. Use $NoReconnectionStrategy instead.
*/
class DefaultCleanSessionReconnectionStrategy extends NoReconnectionStrategy:
  constructor
      --logger/log.Logger=log.default
      --receive-connect-timeout /Duration = ReconnectionStrategyBase.DEFAULT-RECEIVE-CONNECT-TIMEOUT
      --delay-lambda /Lambda? = null
      --attempt-delays /List? /*Duration*/ = null:
    super --logger=logger
        --receive-connect-timeout=receive-connect-timeout
        --delay-lambda=delay-lambda
        --attempt-delays=attempt-delays

/**
The reconnection strategy that tries to reconnect when the connection is
  interrupted.

If the connection drops, the client tries to reconnect potentially multiple times.
*/
class RetryReconnectionStrategy extends ReconnectionStrategyBase:
  /**
  Constructs a new reconnection strategy.

  See $ReconnectionStrategyBase for the meaning of the parameters.
  */
  constructor
      --logger/log.Logger=log.default
      --receive-connect-timeout /Duration = ReconnectionStrategyBase.DEFAULT-RECEIVE-CONNECT-TIMEOUT
      --delay-lambda /Lambda? = null
      --attempt-delays /List? = null:
    super --logger=logger
        --receive-connect-timeout=receive-connect-timeout
        --delay-lambda=delay-lambda
        --attempt-delays=attempt-delays

  connect -> bool?
      transport/ActivityMonitoringTransport_
      --is-initial-connection /bool
      [--reconnect-transport]
      [--disconnect-transport]
      [--send-connect]
      [--receive-connect-ack]
      [--disconnect]:
    return do-connect transport
        --reuse-connection = is-initial-connection
        --reconnect-transport = reconnect-transport
        --disconnect-transport = disconnect-transport
        --send-connect = send-connect
        --receive-connect-ack = receive-connect-ack

  should-try-reconnect transport/ActivityMonitoringTransport_ -> bool:
    return transport.supports-reconnect

/**
Deprecated. Use $RetryReconnectionStrategy instead.
*/
class DefaultSessionReconnectionStrategy extends RetryReconnectionStrategy:
  constructor
      --logger/log.Logger=log.default
      --receive-connect-timeout /Duration = ReconnectionStrategyBase.DEFAULT-RECEIVE-CONNECT-TIMEOUT
      --delay-lambda /Lambda? = null
      --attempt-delays /List? = null:
    super --logger=logger
        --receive-connect-timeout=receive-connect-timeout
        --delay-lambda=delay-lambda
        --attempt-delays=attempt-delays

/**
A reconnection strategy that keeps reconnecting.

This strategy also ignores whether the broker did or did not have a session for
  the client. See below for the consequences of ignoring session state.

The strategy will keep trying to reconnect until the client is closed. The
  delay between each connection attempt can be configured by a lambda.

# Session state
If the client reconnects, but the broker doesn't have a session for the client,
  then this strategy continues as if nothing ever happened. This can, in
  theory, lead to a situation where the client acknowledges messages that the
  broker never sent. In rare cases, the client might even acknowledge a message
  that happens to have an ID of a packet the broker just sent, but the client
  hasn't received yet.
  Theoretically, this is a protocol violation, but most brokers just drop the packet.

Note that this can also happen to clients that don't set the client-session flag, but
  where the broker lost the session. This can happen because the session expired, the
  broker crashed, or a client with the same ID connected in the meantime with a
  clean-session flag.
*/
class TenaciousReconnectionStrategy extends ReconnectionStrategyBase:
  /**
  Creates a new tenacious reconnection strategy.

  The given $delay-lambda must return a Duration, representing the delay between
    the next connection attempt. The first reconnection attempt is always
    immediate, so this lambda is only called before the second and subsequent
    attempts.

  The $receive-connect-timeout is the timeout given for the CONNECT packet to
    arrive at the client.
  */
  constructor
      --logger/log.Logger=log.default
      --receive-connect-timeout /Duration = ReconnectionStrategyBase.DEFAULT-RECEIVE-CONNECT-TIMEOUT
      --reset-duration /Duration = ReconnectionStrategyBase.DEFAULT-RESET-DURATION
      --delay-lambda /Lambda:
    super --logger=logger
        --receive-connect-timeout=receive-connect-timeout
        --delay-lambda=delay-lambda
        --reset-duration=reset-duration

  connect -> bool?
      transport/ActivityMonitoringTransport_
      --is-initial-connection /bool
      [--reconnect-transport]
      [--disconnect-transport]
      [--send-connect]
      [--receive-connect-ack]
      [--disconnect]:
    return do-connect transport
        --reuse-connection = is-initial-connection
        --reconnect-transport = reconnect-transport
        --disconnect-transport = disconnect-transport
        --send-connect = send-connect
        --receive-connect-ack = receive-connect-ack

  should-try-reconnect transport/ActivityMonitoringTransport_ -> bool:
    return transport.supports-reconnect

/**
An MQTT session.

Keeps track of data that must survive disconnects (assuming the user didn't use
  the 'clean_session' flag).
*/
class Session_:
  next-packet-id_ /int? := 1

  /**
  The persistence store used by this session.

  All messages with qos=1 are stored in this store immediately after they have
    been sent. Once the client receives an 'ack' they are removed from it.

  If the client closes, but the store isn't empty, then some messages might not
    have reached the broker.

  Use $PersistenceStore.do to iterate over the stored messages.
  */
  persistence-store_ /PersistenceStore

  options /SessionOptions

  static NOT-ACKED_ ::= 0
  static ACKED_ ::= 1

  /**
  Keeps track of ids that haven't been given to the persistence store yet
    but are in the process of being sent to the broker.
  Once the sending is done without an exception, the id is removed from this
    set and given to the persistence store.
  Depending on whether it was $ACKED_ or $NOT-ACKED_, the packet is then
    immediately removed from the store again.
  */
  ack-ids-to-hold_/Map ::= {:}

  constructor .options .persistence-store_:

  /**
  Marks the given packet $id as being in the process of being sent to the broker.

  The session holds the acks and doesn't immediately give it to the
    persistence store.
  */
  hold-ack-for id/int -> none:
    ack-ids-to-hold_[id] = NOT-ACKED_

  /**
  Restores normal ack handling for the given packet $id.
  */
  restore-ack-handling-for id/int -> none:
    ack-ids-to-hold_.remove id

  set-pending-ack topic/string payload/ByteArray --persistence-token/any --packet-id/int --retain/bool:
    persistent := PersistedPacket topic payload
        --retain=retain
        --packet-id=packet-id
        --token=persistence-token
    persistence-store_.add persistent
    ack-ids-to-hold_.get packet-id --if-present=: | current-value |
      if current-value == ACKED_:
        // This packet was already acked.
        // Inform the persistence store.
        remove-pending packet-id
      // Either way, stop treating acks for this id specially.
      restore-ack-handling-for packet-id

  handle-ack id/int [--if-absent] -> none:
    if ack-ids-to-hold_.contains id:
      ack-ids-to-hold_[id] = ACKED_
      return

    if not persistence-store_.remove-persisted-with-id id:
      if-absent.call id

  remove-pending id/int -> none:
    persistence-store_.remove-persisted-with-id id

  next-packet-id -> int:
    while true:
      candidate := next-packet-id_
      next-packet-id_ = (candidate + 1) & ((1 << PublishPacket.ID-BIT-SIZE) - 1)

      // Message ID 0 is reserved as invalid.
      if candidate == 0: continue
      // If we are waiting for an ACK of the candidate id, pick a different one.
      if persistence-store_.get candidate: continue
      return candidate

  /**
  Runs over the pending packets.

  It is not allowed to change the pending map while calling this method.

  Calls the $block with the packet_id and persisted_id of each pending packet.
  */
  do --pending/bool [block] -> none:
    persistence-store_.do block

  pending-count -> int:
    return persistence-store_.size

/**
A persistence strategy for the MQTT client.

Note that the client does *not* automatically resend old messages when it starts up.
This is also true if the client tries to connect reusing an existing session, but
  the session has expired. In these cases, the user must manually resend the old
  messages.
*/
interface PersistenceStore:
  /**
  Stores the publish-packet information in the persistent store.
  */
  add packet/PersistedPacket -> none

  /**
  Finds the persistent packet with $packet-id and returns it.

  If no such packet exists, returns null.

  # Advanced
  The MQTT protocol does not explicitly allow to drop packets. However,
    most brokers should do fine with it.
  */
  get packet-id/int -> PersistedPacket?

  /**
  Removes the data for the packet with $packet-id.
  Does nothing if there is no data associated to $packet-id.

  Returns whether the store contained the id.
  */
  remove-persisted-with-id packet-id/int -> bool

  /**
  Calls the given block for each stored packet.

  The order of the packets is in insertion order.

  The block is called with a $PersistedPacket.

  # Inheritance
  The MQTT protocol does not allow to drop messages that are not acknowledged.
    However, most brokers can probably deal with it.
  */
  do [block] -> none

  /**
  The amount of stored packets.
  */
  size -> int

class PersistedPacket:
  token /any
  packet-id /int
  topic /string
  payload /ByteArray
  retain /bool

  constructor .topic .payload --.token --.packet-id --.retain:

/**
A persistence store that stores the packets in memory.
*/
class MemoryPersistenceStore implements PersistenceStore:
  /**
  A map from packet id to $PersistedPacket.
  */
  storage_ /Map := {:}

  add packet/PersistedPacket -> none:
    id := packet.packet-id
    storage_[id] = packet

  get packet-id/int -> PersistedPacket?:
    return storage_.get packet-id

  remove-persisted-with-id packet-id/int -> bool:
    storage_.remove packet-id --if-absent=: return false
    return true

  /**
  Calls the given block for each stored packet in insertion order.
  See $PersistenceStore.do.
  */
  do [block] -> none:
    storage_.do --values block

  /** Whether no packet is persisted. */
  is-empty -> bool:
    return storage_.is-empty

  /**
  The count of persisted packets.
  */
  size -> int:
    return storage_.size

/**
An MQTT client.

The client is responsible for maintaining the connection to the server.
If necessary it reconnects to the server.

When the connection to the broker is established with the clean-session bit, the client
  resubscribes all its subscriptions. However, due to the bit, there might be some
  messages that are lost.
*/
class FullClient:
  /** The client has been created. Handle has not been called yet. */
  static STATE-CREATED_ ::= 0
  /**
  The client is in the process of connecting.
  */
  static STATE-CONNECTING_ ::= 1
  /**
  The client is (or was) connected.
  This is a necesarry precondition for calling $handle.
  */
  static STATE-CONNECTED_ ::= 2
  /** The client is handling incoming packets. */
  static STATE-HANDLING_ ::= 3
  /**
  The client has disconnected.
  The packet might not be sent yet (if other packets are queued in front), but no
    calls to $publish, ... should be done.
  */
  static STATE-DISCONNECTED_ ::= 4
  /**
  The client is disconnected and in the process of shutting down.
  This only happens once the current message has been handled. That is, once
    the $handle method's block has returned.
  Externally, the client class considers this to be equivalent to being closed. The
    $is-closed method returns true when the $state_ is set to $STATE-CLOSING_.
  */
  static STATE-CLOSING_ ::= 5
  /** The client is closed. */
  static STATE-CLOSED_ ::= 6

  state_ /int := STATE-CREATED_

  transport_ /ActivityMonitoringTransport_ := ?
  logger_ /log.Logger

  session_ /Session_? := null
  reconnection-strategy_ /ReconnectionStrategy? := null

  connection_ /Connection_? := null
  connecting_ /monitor.Mutex := monitor.Mutex

  /**
  Latch that is set when the $handle method is run. This indicates that the
    client is running.

  Note that we allow to read the latch multiple times.
  */
  handling-latch_ /monitor.Latch := monitor.Latch

  /**
  A mutex to queue the senders.

  Relies on the fact that Toit mutexes are fair and execute them in the order
    in which they reached the mutex.
  */
  sending_ /monitor.Mutex := monitor.Mutex

  /**
  Keeps track of the last acked packet.

  If the packet was not acked during the callback of $handle we will do so automatically
    when the callback returns.
  */
  unacked-packet_ /Packet? := null

  /**
  A signal that is triggered whenever a packet acknowledgement is received.
  */
  ack-received-signal_ /monitor.Signal := monitor.Signal

  /**
  Constructs an MQTT client.

  The client starts disconnected. Call $connect, followed by $handle to initiate
    the connection.
  */
  constructor
      --transport /Transport
      --logger /log.Logger = log.default:
    transport_ = ActivityMonitoringTransport_.private_ transport
    logger_ = logger

  /**
  Checks whether a user is allowed to send a message.

  The client must have connected, and run the $handle loop.
  */
  check-allowed-to-send_:
    if is-closed: throw CLIENT-CLOSED-EXCEPTION
    // The client is only active once $handle has been called.
    if not is-running: throw "INVALID_STATE"

  /**
  Connects the client to the broker.

  Once the client is connected, the user should call $handle to handle incoming
    packets. Only, when the handler is active, can messages be sent.
  */
  connect -> none
      --options /SessionOptions
      --persistence-store /PersistenceStore = MemoryPersistenceStore
      --reconnection-strategy /ReconnectionStrategy? = null:
    if state_ != STATE-CREATED_: throw "INVALID_STATE"
    assert: not connection_

    if reconnection-strategy:
      reconnection-strategy_ = reconnection-strategy
    else if options.clean-session:
      reconnection-strategy_ = NoReconnectionStrategy
          --logger=logger_.with-name "reconnect"
    else:
      reconnection-strategy_ = RetryReconnectionStrategy
          --logger=logger_.with-name "reconnect"

    session_ = Session_ options persistence-store

    state_ = STATE-CONNECTING_
    reconnect_ --is-initial-connection
    state_ = STATE-CONNECTED_

  /**
  Runs the receiving end of the client.

  The client is not considered started until this method has been called.
  This method is blocking.

  The given $on-packet block is called for each received packet with a $Packet.
    The block should $ack the packet as soon as possible.

  The $when-running method executes a given block when this method is running.
  */
  handle [on-packet] -> none:
    if state_ != STATE-CONNECTED_: throw "INVALID_STATE"
    state_ = STATE-HANDLING_

    try:
      handling-latch_.set true
      while not Task.current.is-canceled:
        packet /Packet? := null
        catch --unwind=(: not state_ == STATE-DISCONNECTED_ and not state_ == STATE-CLOSING_):
          do-connected_ --allow-disconnected: packet = connection_.read

        if not packet:
          // Normally the broker should only close the connection when we have
          // sent a disconnect. However, we are also ok with being in a closed state.
          // In theory this could hide unexpected disconnects from the server, but it's
          // much more likely that we first disconnected, and then called $close for
          // another reason (like timeout).
          if not is-closed:
            logger_.info "disconnect from server"
            // Reset the connection and try again.
            // The loop will call `do_connected_` which will ensure that we reconnect if
            // that's possible.
            connection_.close
            continue

          // The user called 'close'. Doesn't mean it was always completely graceful, but
          // good enough.
          break

        if packet is PublishPacket or packet is SubAckPacket or packet is UnsubAckPacket:
          unacked-packet_ = packet
          try:
            on-packet.call packet
            packet.ensure-drained_
            if is-running and unacked-packet_: ack unacked-packet_
          finally:
            unacked-packet_ = null
        else if packet is ConnAckPacket:
          logger_.info "spurious conn-ack packet"
        else if packet is PingRespPacket:
          // Ignore.
        else if packet is PubAckPacket:
          ack := packet as PubAckPacket
          id := ack.packet-id
          // The persistence store is allowed to toss out packets it doesn't want to
          // send again. If we receive an unmatched packet id, it might be from an
          // earlier attempt to send it.
          session_.handle-ack id
              --if-absent=: logger_.info "unmatched packet id: $id"
          ack-received-signal_.raise
        else:
          logger_.info "unexpected packet of type $packet.type"
    finally:
      tear-down_

  /**
  Calls the given $block when the client is running.
  If the client was not able to connect, then the block is not called.
  */
  when-running [block] -> none:
    if is-closed: return
    is-running := handling-latch_.get
    if not is-running: return
    block.call

  /**
  Closes the client.

  Unless $force is true, just sends a disconnect packet to the broker. The client then
    shuts down gracefully once the broker has closed the connection.

  If $force is true, shuts down the client by severing the transport.

  # Disconnect
  In the case of a graceful (non-$force close), the following applies:

  If other tasks are currently sending packets (with $publish or (un)$subscribe), then
    waits for these tasks to finish, before taking any action.

  If the client has already sent a disconnect packet, or is closed, does nothing.

  Never tries to reconnect. If the connection is not alive when it's time to send the
    disconnect packet, closes the client as if called with $force.

  # Forced shutdown
  In the case of a forced shutdown, calls $Transport.close on the current transport.

  No further packet is sent or received at this point.

  If the client is handling a callback, it still waits for it to finish before
    returning from $handle, at which point the client is considered fully closed.
  */
  close --force/bool=false -> none:
    if state_ == STATE-CLOSED_ or state_ == STATE-CLOSING_: return

    // If the $handle method hasn't been called, just tear down the client.
    if state_ == STATE-CREATED_ or state_ == STATE-CONNECTING_ or state_ == STATE-CONNECTED_:
      tear-down_
      return

    if not force: disconnect_
    else: close-force_

  disconnect_ -> none:
    if state_ == STATE-DISCONNECTED_: return

    state_ = STATE-DISCONNECTED_

    // We might be calling close from within the reconnection-strategy's callback.
    // If the connection is currently not alive simply force-close.
    if not connection_.is-alive:
      close-force_
      return

    reconnection-strategy_.close

    sending_.do:
      if state_ == STATE-CLOSING_ or state_ == STATE-CLOSED_ : return

      // Make sure we take the same lock as the reconnecting function.
      // Otherwise we might not send a disconnect, even though we just reconnected (or are
      // reconnecting).
      connecting_.do:
        if connection_.is-alive:
          connection_.write DisconnectPacket
        else:
          close-force_

  /**
  Forcefully closes the client.
  */
  close-force_ -> none:
    assert: state_ == STATE-HANDLING_ or state_ == STATE-DISCONNECTED_
    // Since we are in a handling/disconnected state, there must have been a $connect call, and as such
    // a reconnection_strategy
    assert: session_

    state_ = STATE-CLOSING_

    reconnection-strategy_.close

    // Shut down the connection, which will lead to the $handle method returning
    //   which will then tear down the client.
    connection_.close

  /**
  Whether the client is disconnected, closing or closed.

  After a call to disconnect, or a call to $close, the client starts to shut down.
  However, it is only considered to be fully closed when the $handle method returns.
  */
  is-closed -> bool:
    return state_ == STATE-DISCONNECTED_ or state_ == STATE-CLOSING_ or state_ == STATE-CLOSED_

  /**
  Whether the client is connected and running.

  If true, users are allowed to send messages or change subscriptions.
  */
  is-running -> bool:
    return state_ == STATE-HANDLING_

  /**
  Publishes an MQTT message on $topic.

  The $qos parameter must be either:
  - 0: at most once, aka "fire and forget". In this configuration the message is sent, but the delivery
        is not guaranteed.
  - 1: at least once. The MQTT client ensures that the message is received by the MQTT broker.

  QOS = 2 (exactly once) is not implemented by this client.

  The $retain parameter lets the MQTT broker know whether it should retain this message. A new (later)
    subscription to this $topic would receive the retained message, instead of needing to wait for
    a new message on that topic.
  Not all MQTT brokers support $retain.

  The $persistence-token parameter is used when a packet is sent with $qos equal to 1. In this case
    the $PersistedPacket that is given to the $PersistenceStore contains this token. The persistence
    store can use this information to avoid keeping the data in memory, or to clear data from
    the flash.
  */
  publish topic/string payload/io.Data --qos/int=1 --retain/bool=false --persistence-token/any=null -> none:
    // We already convert to a ByteArray now, as the `PublishPacket` wants ByteArrays only.
    // We don't want to change that, since the same packet is also used on the receiving end
    // where the payload is always a ByteArray.
    byte-array-payload := payload is ByteArray
        ? payload
        : ByteArray.from payload
    if topic == "" or topic.contains "+" or topic.contains "#": throw "INVALID_ARGUMENT"
    if qos == 0:
      send-publish_ topic byte-array-payload --packet-id=null --qos=qos --retain=retain
    else if  qos == 1:
      ack-received-signal_.wait: session_.pending-count < session_.options.max-inflight
      packet-id := session_.next-packet-id
      // We need to tell the session_ to keep acks for this packet id before we
      // call 'set_pending_ack'. Otherwise we have a race condition where the
      // broker can send the ack before the session is ready for it.
      session_.hold-ack-for packet-id
      try:
        send-publish_ topic byte-array-payload --packet-id=packet-id --qos=qos --retain=retain
        session_.set-pending-ack topic byte-array-payload
            --packet-id=packet-id
            --retain=retain
            --persistence-token=persistence-token
      finally:
        // Either 'set_pending_ack' was called, or we had an exception.
        // Either way we don't need to do special ack-handling for this packet id anymore.
        session_.restore-ack-handling-for packet-id

  /**
  Sends a $PublishPacket with the given $topic, $payload, $qos and $retain.

  If $qos is 1, then allocates a packet id and returns it.
  */
  send-publish_ topic/string payload/ByteArray --packet-id --qos/int --retain/bool -> none:
    if qos != 0 and qos != 1: throw "INVALID_ARGUMENT"

    packet := PublishPacket
        topic
        payload
        --qos=qos
        --retain=retain
        --packet-id=packet-id

    send_ packet

  /**
  Subscribes to the given $topic with a max qos of $max-qos.

  The $topic might have wildcards and is thus more of a filter than a single topic.

  Returns the packet id of the subscribe packet.
  */
  subscribe topic/string --max-qos/int=1 -> int:
    return subscribe-all [ TopicQos topic --max-qos=max-qos ]

  /**
  Subscribes to the given list $topics of type $TopicQos.

  Returns the packet id of the subscribe packet or -1 if the $topics is empty.
  */
  subscribe-all topics/List -> int:
    if topics.is-empty: return -1

    packet-id := session_.next-packet-id
    packet := SubscribePacket topics --packet-id=packet-id
    send_ packet
    return packet-id

  /**
  Unsubscribes from a single $topic.

  If the broker has a subscription (see $subscribe) of the given $topic, it will be removed.

  Returns the packet id of the unsubscribe packet.
  */
  unsubscribe topic/string -> int:
    return unsubscribe-all [topic]

  /**
  Unsubscribes from the list of $topics (of type $string).

  For each topic in the list of $topics, the broker checks if it has a subscription
    (see $subscribe) and removes it, if it exists.

  Returns the packet id of the unsubscribe packet or -1 if the $topics is empty.
  */
  unsubscribe-all topics/List -> int:
    if topics.is-empty: return -1

    packet-id := session_.next-packet-id
    packet := UnsubscribePacket topics --packet-id=packet-id
    send_ packet
    return packet-id

  /**
  Acknowledges the hand-over of the packet.

  If the packet has qos=1, sends an ack packet to the broker.

  If the client is closed, does nothing.
  */
  ack packet/Packet:
    if unacked-packet_ == packet: unacked-packet_ = null

    if is-closed: return
    if state_ != STATE-HANDLING_: throw "INVALID_STATE"

    if packet is PublishPacket:
      id := (packet as PublishPacket).packet-id
      if id:
        check-allowed-to-send_
        ack := PubAckPacket --packet-id=id
        // Skip the 'sending_' queue and write directly to the connection.
        // This way ack-packets are transmitted faster.
        do-connected_: connection_.write ack

  /**
  Sends the given $packet.

  Takes a lock and hands the packet to the connection.
  */
  send_ packet/Packet -> none:
    if packet is ConnectPacket: throw "INVALID_PACKET"
    check-allowed-to-send_
    sending_.do:
      // While waiting in the queue the client could have been closed.
      if is-closed: throw CLIENT-CLOSED-EXCEPTION
      do-connected_: connection_.write packet

  /**
  Runs the given $block in a connected state.

  If necessary reconnects the client.

  If $allow-disconnected is true, then the client may also be in a disconnected (but
    not closed) state.
  */
  do-connected_ --allow-disconnected/bool=false [block]:
    if is-closed and not (allow-disconnected and state_ == STATE-DISCONNECTED_): throw CLIENT-CLOSED-EXCEPTION
    while not Task.current.is-canceled:
      // If the connection is still alive, or if the manager doesn't want us to reconnect, let the
      // exception go through.
      should-abandon := :
        connection_.is-alive or reconnection-strategy_.is-closed or
          not reconnection-strategy_.should-try-reconnect transport_

      exception := catch --unwind=should-abandon:
        block.call
        return
      assert: exception != null
      if is-closed: throw CLIENT-CLOSED-EXCEPTION
      reconnect_ --is-initial-connection=false
      // While trying to reconnect there might have been a 'close' call.
      // Since we managed to connect, the 'close' call was too late to intervene with
      // the connection attempt. As such we consider the reconnection a success (as if
      // it happened before the 'close' call), and we continue.
      // There will be a 'disconnect' packet that will close the connection to the broker.

  /**
  Connects (or reconnects) to the broker.

  Uses the reconnection_strategy to do the actual connecting.
  */
  reconnect_ --is-initial-connection/bool -> none:
    assert: not connection_ or not connection_.is-alive
    old-connection := connection_

    connecting_.do:
      // Check that nobody else reconnected while we waited to take the lock.
      if connection_ != old-connection: return

      if not connection_:
        assert: is-initial-connection
        connection_ = Connection_ transport_
            --keep-alive=session_.options.keep-alive
            --logger=logger_

      should-force-close := true
      try:
        had-session := reconnection-strategy_.connect transport_
            --is-initial-connection = is-initial-connection
            --reconnect-transport = :
              transport_.reconnect
              connection_ = Connection_ transport_
                  --keep-alive=session_.options.keep-alive
                  --logger=logger_
            --disconnect-transport = :
              transport_.disconnect
            --send-connect = :
              packet := ConnectPacket session_.options.client-id
                  --clean-session=session_.options.clean-session
                  --username=session_.options.username
                  --password=session_.options.password
                  --keep-alive=session_.options.keep-alive
                  --last-will=session_.options.last-will
              connection_.write packet
            --receive-connect-ack = :
              response := connection_.read
              if not response: throw "CONNECTION_CLOSED"
              ack := (response as ConnAckPacket)
              return-code := ack.return-code
              if return-code != 0:
                refused-reason := refused-reason-for-return-code_ return-code
                connection_.close --reason=refused-reason
                // The broker refused the connection. This means that the
                // problem isn't due to a bad connection but almost certainly due to
                // some bad arguments (like a bad client-id). As such don't try to reconnect
                // again and just give up.
                close --force
                throw refused-reason
              ack.session-present
            --disconnect = :
              connection_.write DisconnectPacket

        // If we are here, then the reconnection succeeded.
        // If we throw now, then we should go through the standard way of dealing with a disconnect.
        should-force-close = false

        // Make sure the connection sends pings so the broker doesn't drop us.
        connection_.keep-alive --background

        // Resend the pending messages.
        // If we didn't have a session, then we just send the data again. We might send the data too
        // often, but there isn't much we can do.
        session_.do --pending: | persisted/PersistedPacket |
          packet-id := persisted.packet-id
          topic := persisted.topic
          payload := persisted.payload
          retain := persisted.retain
          packet := PublishPacket topic payload --packet-id=packet-id --qos=1 --retain=retain --duplicate=had-session
          connection_.write packet

      finally: | is-exception exception |
        if is-exception and should-force-close:
          close --force
  /** Tears down the client. */
  tear-down_:
    critical-do:
      state_ = STATE-CLOSED_
      if reconnection-strategy_: reconnection-strategy_.close
      connection_.close
      if not handling-latch_.has-value: handling-latch_.set false
