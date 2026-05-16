// Licensed under the Apache License, Version 2.0
// Copyright 2025, Mindful Software LLC, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';

import 'otel_pusher_channels_suppression.dart';

const _tracerName = 'dartastic_otel_libs_pusher_channels';
const _systemValue = 'pusher_channels';

Tracer _tracer() => OTel.tracerProvider().getTracer(_tracerName);

/// Pair returned from a `tracedPusherChannels*` invoke callback.
class PusherChannelsCallResult<T> {
  const PusherChannelsCallResult({required this.value, this.messageId});
  final T value;
  final String? messageId;
}

/// Wraps a Pusher Channels publish (server-side trigger / client emit)
/// in a PRODUCER span using OTel messaging semconv. Channel name is
/// mapped to `messaging.destination.name`; event name is recorded as
/// `pusher.event.name`.
Future<T> tracedPusherChannelsPublish<T>({
  required String channel,
  required String event,
  required Future<PusherChannelsCallResult<T>> Function() invoke,
  int? payloadSize,
  String? socketId,
}) async {
  if (pusherChannelsInstrumentationSuppressed()) {
    final r = await invoke();
    return r.value;
  }

  final m = <String, Object>{
    'messaging.system': _systemValue,
    'messaging.operation': 'publish',
    'messaging.destination.name': channel,
    'pusher.event.name': event,
  };
  if (payloadSize != null) m['messaging.message.body.size'] = payloadSize;
  if (socketId != null) m['pusher.socket.id'] = socketId;

  final span = _tracer().startSpan(
    'publish $channel',
    kind: SpanKind.producer,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    final r = await invoke();
    if (r.messageId != null) {
      span.addAttributes(OTel.attributes([
        OTel.attributeString('messaging.message.id', r.messageId!),
      ]));
    }
    return r.value;
  } catch (e, st) {
    span.addAttributes(
      OTel.attributes([
        OTel.attributeString(
          ErrorResource.errorType.key,
          e.runtimeType.toString(),
        ),
      ]),
    );
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}

/// Wraps a Pusher Channels subscribe (channel subscription handshake)
/// in a CLIENT span.
Future<T> tracedPusherChannelsSubscribe<T>({
  required String channel,
  required Future<T> Function() invoke,
  bool? presence,
}) async {
  if (pusherChannelsInstrumentationSuppressed()) {
    return invoke();
  }

  final m = <String, Object>{
    'messaging.system': _systemValue,
    'messaging.operation': 'subscribe',
    'messaging.destination.name': channel,
    if (presence != null) 'pusher.channel.presence': presence,
  };

  final span = _tracer().startSpan(
    'subscribe $channel',
    kind: SpanKind.client,
    attributes: OTel.attributesFromMap(m),
  );
  try {
    return await invoke();
  } catch (e, st) {
    span.addAttributes(
      OTel.attributes([
        OTel.attributeString(
          ErrorResource.errorType.key,
          e.runtimeType.toString(),
        ),
      ]),
    );
    span.recordException(e, stackTrace: st);
    span.setStatus(SpanStatusCode.Error, e.toString());
    rethrow;
  } finally {
    span.end();
  }
}
