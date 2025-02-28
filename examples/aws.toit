// Copyright (C) 2022 Toitware ApS.
// Use of this source code is governed by a Zero-Clause BSD license that can
// be found in the EXAMPLES_LICENSE file.

/**
Demonstrates the use of AWS IoT Core.
*/

import certificate-roots
import mqtt
import mqtt.transport as mqtt
import net
import net.x509
import tls

// The Endpoint. Can be found in the 'Settings' section of the AWS IoT
// Core console. Also available with
//   `aws iot describe-endpoint --endpoint-type iot:Jobs`
// Of the form <account-specific-prefix>.jobs.iot.<aws-region>.amazonaws.com.
HOST ::= "<- insert endpoint URL here ->"
PORT ::= 8883

CLIENT-CERTIFICATE ::= """
-----BEGIN CERTIFICATE-----

<- insert cert here ->

-----END CERTIFICATE-----
"""

CLIENT-KEY ::= """
-----BEGIN RSA PRIVATE KEY-----

<- insert cert here ->

-----END RSA PRIVATE KEY-----
"""

// The client ID.
// Note that the active policy might require the ID to be of a
// specific form.
// See the 'iot:Connect" Policy action. If it ends with ':client/foo/*' then
// clients must be prefixed with "foo/".
CLIENT-ID ::= "<- insert client ID here ->"

// The topic to publish to.
// See the Policy action 'iot:Publish' to see which topics are allowed.
// If it ends with ':topic/foo/*' then only topics that are prefixed with
// "foo/" are allowed.
MY-TOPIC ::= "<- insert topic here ->"

create-transport -> mqtt.Transport:
  client-certificate := tls.Certificate (x509.Certificate.parse CLIENT-CERTIFICATE) CLIENT-KEY
  return mqtt.TcpTransport.tls --host=HOST --port=PORT
      --server-name=HOST
      --certificate=client-certificate

main:
  certificate-roots.install-common-trusted-roots

  transport := create-transport

  client := mqtt.SimpleClient --transport=transport

  options := mqtt.SessionOptions
      --client-id=CLIENT-ID
      --clean-session=true

  client.start --options=options

  topic := MY-TOPIC

  // Simulate a temperature sensor.
  temperature := 25.0
  10.repeat:
    temperature += ((random 100) - 50) / 100.0
    print "publishing"
    client.publish topic "$temperature"
    sleep --ms=2_500

  client.close
