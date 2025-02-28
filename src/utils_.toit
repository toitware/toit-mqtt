// Copyright (C) 2024 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import io
import .packets

io-data-to-byte-array_ data/io.Data from/int=0 to/int=data.byte-size -> ByteArray:
  if data is ByteArray:
    byte-array := data as ByteArray
    if from == 0 and to == byte-array.size: return byte-array
    return byte-array[from..to]

  byte-array := ByteArray (to - from)
  data.write-to-byte-array byte-array --at=0 from to
  return byte-array

refused-reason-for-return-code_ return-code/int -> string:
  refused-reason := "CONNECTION_REFUSED"
  if return-code == ConnAckPacket.UNACCEPTABLE-PROTOCOL-VERSION:
    refused-reason = "UNACCEPTABLE_PROTOCOL_VERSION"
  else if return-code == ConnAckPacket.IDENTIFIER-REJECTED:
    refused-reason = "IDENTIFIER_REJECTED"
  else if return-code == ConnAckPacket.SERVER-UNAVAILABLE:
    refused-reason = "SERVER_UNAVAILABLE"
  else if return-code == ConnAckPacket.BAD-USERNAME-OR-PASSWORD:
    refused-reason = "BAD_USERNAME_OR_PASSWORD"
  else if return-code == ConnAckPacket.NOT-AUTHORIZED:
    refused-reason = "NOT_AUTHORIZED"
  return refused-reason

