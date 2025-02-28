// Copyright (C) 2021 Toitware ApS. All rights reserved.
// Use of this source code is governed by an MIT-style license that can be
// found in the LICENSE file.

import .client
import .full-client
import .session-options
import .simple-client
import .tcp
import .last-will
import .topic-qos

export *

/**
An MQTT package.

This library provides 3 different clients:
- $SimpleClient
- $Client
- $FullClient

In most cases, the $SimpleClient is sufficient. It is conceptually simpler, but
  misses features like automatic reconnect, resending of unacknowledged messages,
  topic routing, etc.

The $Client is a routing client. It allows you to register callbacks for specific
  topics. It will automatically subscribe to these topics and call the registered
  callbacks when a message is received. It is a wrapper around the $FullClient.

The $FullClient is the most powerful client. It is, however, also the most complex.
  If you want to control how the client reconnects, or how unacknowledged messages
  are stored across restarts, then the $FullClient is the right choice.
*/
