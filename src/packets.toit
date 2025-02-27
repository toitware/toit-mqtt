// Copyright (C) 2021 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import io
import reader as old-reader

import .last-will
import .topic-qos

interface AckPacket:
  packet-id -> int

abstract class Packet:
  type/int
  flags/int

  constructor .type --.flags=0:

  static deserialize reader/io.Reader -> Packet?:
    if not reader.try-ensure-buffered 1: return null
    byte1 := reader.read-byte
    kind := byte1 >> 4
    flags := byte1 & 0xf
    size := 0
    for i := 0; i < 4; i++:
      byte := reader.read-byte
      size |= (byte & 0x7f) << (i * 7)
      if byte & 0x80 == 0: break
    if kind == ConnectPacket.TYPE:
      return ConnectPacket.deserialize_ reader
    if kind == ConnAckPacket.TYPE:
      return ConnAckPacket.deserialize_ reader
    if kind == PublishPacket.TYPE:
      return PublishPacket.deserialize_ reader size flags
    if kind == PubAckPacket.TYPE:
      return PubAckPacket.deserialize_ reader
    if kind == SubscribePacket.TYPE:
      return SubscribePacket.deserialize_ reader size
    if kind == SubAckPacket.TYPE:
      return SubAckPacket.deserialize_ reader size
    if kind == UnsubscribePacket.TYPE:
      return UnsubscribePacket.deserialize_ reader size
    if kind == UnsubAckPacket.TYPE:
      return UnsubAckPacket.deserialize_ reader
    if kind == PingRespPacket.TYPE:
      return PingRespPacket.deserialize_ reader
    if kind == PingReqPacket.TYPE:
      return PingReqPacket.deserialize_ reader
    if kind == DisconnectPacket.TYPE:
      return DisconnectPacket.deserialize_ reader

    throw "invalid packet kind: $kind"

  abstract variable-header -> ByteArray

  abstract payload -> ByteArray

  ensure-drained_ -> none:
    // Most packets drain the reader they deserialize from eagerly, but
    // we allow streaming the payload for a few types. For those, we
    // override this method and make sure the whole payload has been
    // drained when we're done processing a packet.

  serialize -> ByteArray:
    buffer := io.Buffer
    buffer.write-byte type << 4 | flags

    header := variable-header
    payload := payload

    encode-length buffer header.size + payload.size

    buffer.write header
    buffer.write payload

    return buffer.bytes

  static encode-length buffer/io.Buffer length/int:
    if length == 0:
      buffer.write-byte 0
      return

    while length > 0:
      byte := length & 0x7f
      length >>= 7
      if length > 0: byte |= 0x80
      buffer.write-byte byte

  static encode-string buffer/io.Buffer str/string:
    buffer.big-endian.write-uint16 str.size
    buffer.write str

  static encode-uint16 value/int -> ByteArray:
    result := ByteArray 2
    io.BIG-ENDIAN.put-uint16 result 0 value
    return result

  static encode-uint16 buffer/io.Buffer value/int:
    buffer.big-endian.write-uint16 value

  static decode-string reader/io.Reader -> string:
    length := decode-uint16 reader
    return reader.read-string length

  static decode-uint16 reader/io.Reader -> int:
    length-bytes := reader.read-bytes 2
    return io.BIG-ENDIAN.uint16 length-bytes 0

  static debug-string_ packet/Packet -> string:
    if packet is ConnectPacket:
      connect := packet as ConnectPacket
      return ("Connect: $connect.client-id"
              + " $(connect.clean-session ? "clean": "reuse")"
              + " $(connect.last-will ? "last-will-for-$connect.last-will.topic": "no-last-will")"
              + " $(connect.username ? "with-username-$connect.username": "no-username")"
              + " $(connect.password ? "with-password-$connect.password": "no-password")"
              + " $(connect.keep-alive)")
    else if packet is PingReqPacket:
      return "Ping request"
    else if packet is PingRespPacket:
      return "Ping response"
    else if packet is ConnAckPacket:
      connack := packet as ConnAckPacket
      return "ConnAck: $connack.return-code $connack.session-present"
    else if packet is PublishPacket:
      publish := packet as PublishPacket
      result := "Publish$(publish.packet-id ? "($publish.packet-id)" : "")"
          + " topic=$publish.topic"
          + " qos=$publish.qos"
          + " $(publish.duplicate ? "dup": "no-dup")"
          + " $(publish.retain ? "retain": "no-retain")"
          + " $(publish.payload.size) bytes"
      if publish.payload.size < 15 and (publish.payload.every: it < 128):
        result += " \"$(publish.payload.to-string-non-throwing)\""
      return result
    else if packet is PubAckPacket:
      puback := packet as PubAckPacket
      return "PubAck($puback.packet-id)"
    else if packet is SubscribePacket:
      subscribe := packet as SubscribePacket
      result := "Subscribe($subscribe.packet-id)"
      subscribe.topics.do: | topic-qos/TopicQos |
        result += " $topic-qos.topic-$topic-qos.max-qos"
      return result
    else if packet is SubAckPacket:
      suback := packet as SubAckPacket
      return "SubAck($suback.packet-id)"
    else if packet is UnsubscribePacket:
      unsubscribe := packet as UnsubscribePacket
      result := "Unsubscribe($unsubscribe.packet-id)"
      unsubscribe.topics.do:
        result += " $it"
      return result
    else if packet is UnsubAckPacket:
      unsuback := packet as UnsubAckPacket
      return "UnsubAck($unsuback.packet-id)"
    else if packet is DisconnectPacket:
      disconnect := packet as DisconnectPacket
      return "Disconnect"
    else:
      return "Packet of type $packet.type"

class ConnectPacket extends Packet:
  static TYPE ::= 1

  client-id /string
  clean-session /bool
  username /string?
  password /string?
  keep-alive /Duration?
  last-will /LastWill?

  constructor .client-id --.clean-session --.username --.password --.keep-alive --.last-will:
    super TYPE

  constructor.deserialize_ reader/io.Reader:
    protocol := Packet.decode-string reader
    if protocol != "MQTT": throw "UNEQUAL PROTOCOL $protocol"
    level := reader.read-byte
    if level != 4: throw "UNEXPECTED PROTOCOL LEVEL: $level"
    connect-flags := reader.read-byte
    clean-session = connect-flags & 0b0000_0010 != 0
    has-username := connect-flags & 0b1000_0000 != 0
    has-password := connect-flags & 0b0100_0000 != 0
    has-last-will := connect-flags & 0b0000_0100 != 0
    last-will-remain := connect-flags & 0b0010_0000 != 0
    last-will-qos := (connect-flags >> 3) & 0b11

    keep-alive = Duration --s=(Packet.decode-uint16 reader)

    client-id = Packet.decode-string reader
    if has-last-will:
      last-will-topic := Packet.decode-string reader
      last-will-payload-size := Packet.decode-uint16 reader
      last-will-payload := reader.read-bytes last-will-payload-size
      last-will = LastWill last-will-topic last-will-payload --qos=last-will-qos --retain=last-will-remain
    else:
      last-will = null

    username = has-username ? Packet.decode-string reader : null
    password = has-password ? Packet.decode-string reader : null

    super TYPE

  variable-header -> ByteArray:
    connect-flags := 0
    if clean-session: connect-flags |= 0b0000_0010
    if username:      connect-flags |= 0b1000_0000
    if password:      connect-flags |= 0b0100_0000
    if last-will:
      connect-flags                 |= 0b0000_0100
      connect-flags                 |= last-will.qos << 3
      if last-will.retain:
        connect-flags               |= 0b0010_0000


    data := #[0, 4, 'M', 'Q', 'T', 'T', 4, connect-flags, 0, 0]
    io.BIG-ENDIAN.put-uint16 data 8 keep-alive.in-s
    return data

  payload -> ByteArray:
    buffer := io.Buffer
    Packet.encode-string buffer client-id
    if last-will:
      Packet.encode-string buffer last-will.topic
      Packet.encode-uint16 buffer last-will.payload.size
      buffer.write last-will.payload

    if username: Packet.encode-string buffer username
    if password: Packet.encode-string buffer password
    return buffer.bytes

class ConnAckPacket extends Packet:
  static TYPE ::= 2

  static UNACCEPTABLE-PROTOCOL-VERSION ::= 0x01
  static IDENTIFIER-REJECTED ::= 0x02
  static SERVER-UNAVAILABLE ::= 0x03
  static BAD-USERNAME-OR-PASSWORD ::= 0x04
  static NOT-AUTHORIZED ::= 0x05

  return-code /int
  session-present /bool

  constructor --.return-code=0 --.session-present=false:
    super TYPE

  constructor.deserialize_ reader/io.Reader:
    data := reader.read-bytes 2
    session-present = data[0] & 0x01 != 0
    return-code = data[1]
    super TYPE

  variable-header -> ByteArray:
    return #[ session-present ? 1 : 0, return-code]

  payload -> ByteArray: return #[]

class PublishPacket extends Packet:
  static ID-BIT-SIZE ::= 16
  static TYPE ::= 3

  topic /string
  packet-id /int?

  reader_ /PublishPacketReader_? := ?
  payload_ /ByteArray? := ?

  constructor.deserialize_ reader/io.Reader size/int flags/int:
    retain := flags & 0b0001 != 0
    qos := (flags & 0b0110) >> 1
    duplicate := flags & 0b1000 != 0
    topic = Packet.decode-string reader
    size -= 2 + topic.size
    if qos > 0:
      packet-id = Packet.decode-uint16 reader
      size -= 2
    else:
      packet-id = null
    reader_ = PublishPacketReader_ reader size
    payload_ = null
    super TYPE --flags=(duplicate ? 0b1000 : 0) | (qos << 1) | (retain ? 1 : 0)

  constructor .topic payload/ByteArray --qos/int --retain/bool --.packet-id --duplicate=false:
    reader_ = null
    payload_ = payload
    super TYPE
        --flags=(duplicate ? 0b1000 : 0) | (qos << 1) | (retain ? 1 : 0)

  payload -> ByteArray:
    payload := payload_
    if payload: return payload
    payload = reader_.read-payload_
    if not payload: throw "Already streaming payload"
    payload_ = payload
    reader_ = null
    return payload

  payload-stream -> io.Reader:
    if payload_: throw "Already read payload"
    return reader_.stream_

  ensure-drained_ -> none:
    if not reader_: return
    reader_.drain_
    reader_ = null

  variable-header -> ByteArray:
    buffer := io.Buffer
    Packet.encode-string buffer topic
    if packet-id: Packet.encode-uint16 buffer packet-id
    return buffer.bytes

  retain -> bool: return flags & 0b1 != 0
  qos -> int: return (flags >> 1) & 0b11
  duplicate -> bool: return flags & 0b1000 != 0

  /**
  Creates a new packet with the provided parameters replacing the current values.

  Optional arguments don't work if one wants to pass in null. As such, the
    $packet-id must be given as integer. If the packet-id should be null, then '-1' should
    be used instead.
  */
  with -> PublishPacket
      --topic/string?=null
      --payload/ByteArray?=null
      --qos/int?=null
      --retain/bool?=null
      --packet-id/int=-2
      --duplicate/bool?=null:
    new-packet-id /int? := ?
    if packet-id == -2: new-packet-id = this.packet-id
    else if packet-id == -1: new-packet-id = null
    else: new-packet-id = packet-id
    return PublishPacket
        topic or this.topic
        payload or this.payload
        --qos = qos or this.qos
        --retain = retain != null ? retain : this.retain
        --packet-id = new-packet-id
        --duplicate = duplicate != null ? duplicate : this.duplicate

class PublishPacketReader_ extends io.Reader implements old-reader.SizedReader:
  reader_ /io.Reader
  content-size /int
  remaining_ /int? := null

  constructor .reader_ .content-size:

  /** Deprecated. Use $content-size instead. */
  size -> int:
    return content-size

  read_ -> ByteArray?:
    remaining := remaining_
    assert: remaining != null  // Should have called $stream_ already.
    if remaining == 0: return null
    bytes := reader_.read --max-size=remaining
    if bytes: remaining_ = remaining - bytes.size
    return bytes

  stream_ -> io.Reader:
    if remaining_ == null: remaining_ = content-size
    return this

  drain_ -> none:
    remaining := remaining_
    if remaining == 0: return
    reader_.skip (remaining or content-size)
    remaining_ = 0

  read-payload_ -> ByteArray?:
    // If we already started streaming from the underlying reader,
    // we cannot produce a payload.
    if remaining_ != null: return null
    payload := reader_.read-bytes content-size
    remaining_ = 0
    return payload

class PubAckPacket extends Packet implements AckPacket:
  static TYPE ::= 4

  packet-id/int

  constructor --.packet-id:
    super TYPE

  constructor.deserialize_ reader/io.Reader:
    packet-id = Packet.decode-uint16 reader
    super TYPE

  variable-header -> ByteArray:
    return Packet.encode-uint16 packet-id

  payload -> ByteArray: return #[]

class SubscribePacket extends Packet:
  static TYPE ::= 8

  topics/List/*<TopicFilter>*/
  packet-id/int

  constructor .topics --.packet-id:
    super TYPE --flags=0b0010

  constructor.deserialize_ reader/io.Reader size/int:
    consumed := 0
    packet-id = Packet.decode-uint16 reader
    consumed += 2
    topics = []
    while consumed < size:
      topic := Packet.decode-string reader
      consumed += 2 + topic.size
      max-qos := reader.read-byte
      consumed++
      topic-qos := TopicQos topic --max-qos=max-qos
      topics.add topic-qos
    super TYPE --flags=0b0010

  variable-header -> ByteArray:
    return Packet.encode-uint16 packet-id

  payload -> ByteArray:
    buffer := io.Buffer
    topics.do: | topic-qos/TopicQos |
      Packet.encode-string buffer topic-qos.topic
      buffer.write-byte topic-qos.max-qos
    return buffer.bytes

class SubAckPacket extends Packet implements AckPacket:
  static TYPE ::= 9

  /** The qos value for a failed subscription. */
  static FAILED-SUBSCRIPTION-QOS ::= 0x80

  packet-id /int
  qos /List  // The list of qos matches the list of topics from the SubPacket.

  constructor --.packet-id --.qos:
    super TYPE

  constructor.deserialize_ reader/io.Reader size/int:
    packet-id = Packet.decode-uint16 reader
    size -= 2
    qos = List size: reader.read-byte
    super TYPE

  variable-header -> ByteArray:
    return Packet.encode-uint16 packet-id

  payload -> ByteArray: return ByteArray qos.size: qos[it]

class UnsubscribePacket extends Packet:
  static TYPE ::= 10

  topics/List/*<string>*/
  packet-id/int

  constructor.deserialize_ reader/io.Reader size/int:
    consumed := 0
    packet-id = Packet.decode-uint16 reader
    consumed += 2
    topics = []
    while consumed < size:
      topic := Packet.decode-string reader
      consumed += 2 + topic.size
      topics.add topic
    super TYPE --flags=0b0010

  constructor .topics --.packet-id:
    super TYPE --flags=0b0010

  variable-header -> ByteArray:
    return Packet.encode-uint16 packet-id

  payload -> ByteArray:
    buffer := io.Buffer
    topics.do: | topic-qos/string |
      Packet.encode-string buffer topic-qos
    return buffer.bytes

class UnsubAckPacket extends Packet implements AckPacket:
  static TYPE ::= 11

  packet-id /int

  constructor --.packet-id:
    super TYPE

  constructor.deserialize_ reader/io.Reader:
    packet-id = Packet.decode-uint16 reader
    super TYPE

  variable-header -> ByteArray:
    return Packet.encode-uint16 packet-id

  payload -> ByteArray: return #[]

class PingReqPacket extends Packet:
  static TYPE ::= 12

  constructor:
    super TYPE

  constructor.deserialize_ reader/io.Reader:
    super TYPE

  variable-header -> ByteArray:
    return #[]

  payload -> ByteArray:
    return #[]

class PingRespPacket extends Packet:
  static TYPE ::= 13

  constructor:
    super TYPE

  constructor.deserialize_ reader/io.Reader:
    super TYPE

  variable-header -> ByteArray:
    return #[]

  payload -> ByteArray:
    return #[]

class DisconnectPacket extends Packet:
  static TYPE ::= 14

  constructor:
    super TYPE

  constructor.deserialize_ reader/io.Reader:
    super TYPE

  variable-header -> ByteArray:
    return #[]

  payload -> ByteArray:
    return #[]
