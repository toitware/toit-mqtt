// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

/**
Demonstrates the use of Adafruit's MQTT server.

Adafruit is free, but has some limitations. For example, one can only publish
  at most 30 messages per minute.

Before running this program:
- create an account at https://io.adafruit.com/
- create a feed: https://io.adafruit.com/$ADAFRUIT-IO-USERNAME/feeds (replace the $ADAFRUIT-IO-USERNAME with your username)
- update the $ADAFRUIT-IO-USERNAME, $ADAFRUIT-IO-KEY, and $ADAFRUIT-IO-FEEDNAME constants.
*/

import mqtt
import certificate-roots

ADAFRUIT-IO-USERNAME ::= "<YOUR_USERNAME>"

// From io.adafruit.com/$ADAFRUIT_IO_USERNAME/dashboards -> click "My Key"
ADAFRUIT-IO-KEY ::= "<YOUR_KEY>"

ADAFRUIT-IO-FEEDNAME ::= "<YOUR_FEEDNAME>"

HOST ::= "io.adafruit.com"

main:
  certificate-roots.install-common-trusted-roots
  client := mqtt.SimpleClient.tls --host=HOST
  /**
  // Alternatively, you can also connect without TLS, by using the
  // following client:
  client := mqtt.Client --host=HOST
  // In that case you can remove the `certificate_roots` import and the
  // `certificate-roots.install-common-trusted-roots` call.
  */

  options := mqtt.SessionOptions
    --client-id = "toit-example-client"
    --username = ADAFRUIT-IO-USERNAME
    --password = ADAFRUIT-IO-KEY

  client.start --options=options

  print "Connected to broker"

  topic := "$ADAFRUIT-IO-USERNAME/feeds/$ADAFRUIT-IO-FEEDNAME"

  // Simulates a temperature sensor.
  temperature := 25.0
  10.repeat:
    temperature += ((random 100) - 50) / 100.0
    client.publish topic "$temperature"
    // Don't publish too often to avoid rate limiting.
    sleep --ms=2_500

  client.close
