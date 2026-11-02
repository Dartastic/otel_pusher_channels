// Licensed under the Apache License, Version 2.0
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:otel_pusher_channels/otel_pusher_channels.dart';

/// Traces Pusher Channels calls with OpenTelemetry.
///
/// `otel_pusher_channels` depends on the OpenTelemetry API only, so the application
/// initializes an SDK once at startup. Wrap each Pusher Channels call in
/// [tracedPusherChannelsPublish]; the `invoke` callback makes the real call and reports what
/// came back, which is recorded on the span.
Future<void> main() async {
  await OTel.initialize(serviceName: 'pusher-channels-example');

  // Wraps a Pusher Channels publish (server-side trigger / client emit) in a PRODUCER span using OTel messaging semconv.
  final pusherChannelsPublishId = await tracedPusherChannelsPublish<String>(
    channel: 'orders',
    event: 'order.created',
    payloadSize: 512,
    socketId: 'socket_8f3a2c',
    invoke: () async {
      // Make the Pusher Channels call here, then report what it returned.
      const id = 'pusher_channels_8f3a2c';
      return const PusherChannelsCallResult(
          value: id, messageId: 'message_8f3a2c');
    },
  );
  OTelLog.info(
      'tracedPusherChannelsPublish messaging.destination.name returned $pusherChannelsPublishId');

  // Wraps a Pusher Channels subscribe (channel subscription handshake) in a CLIENT span.
  final pusherChannelsSubscribeId = await tracedPusherChannelsSubscribe<String>(
    channel: 'orders',
    presence: true,
    invoke: () async {
      // Make the Pusher Channels call here, then report what it returned.
      return 'pusher_channels_8f3a2c';
    },
  );
  OTelLog.info(
      'tracedPusherChannelsSubscribe returned $pusherChannelsSubscribeId');

  // Calls that should not be traced, such as health checks, run inside
  // runWithoutPusherChannelsInstrumentationAsync.
  await runWithoutPusherChannelsInstrumentationAsync(
    () => tracedPusherChannelsPublish<String>(
      channel: 'orders',
      event: 'order.created',
      payloadSize: 512,
      socketId: 'socket_8f3a2c',
      invoke: () async {
        const id = 'pusher_channels_8f3a2c';
        return const PusherChannelsCallResult(
            value: id, messageId: 'message_8f3a2c');
      },
    ),
  );

  await OTel.shutdown();
}
