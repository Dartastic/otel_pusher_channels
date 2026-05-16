// Licensed under the Apache License, Version 2.0
// Copyright 2025, Mindful Software LLC, All rights reserved.

import 'package:dartastic_opentelemetry/dartastic_opentelemetry.dart';
import 'package:dartastic_opentelemetry/testing.dart';
import 'package:otel_pusher_channels/otel_pusher_channels.dart';
import 'package:test/test.dart';

void main() {
  late TestHarness harness;
  late InMemorySpanExporter spans;

  setUpAll(() async {
    harness = await maybeInitializeOtelForTest(
      serviceName: 'otel_pusher_channels-test',
    );
    spans = harness.spans;
  });

  setUp(() => harness.clear());

  Map<String, Object> attrsOf(Span s) =>
      {for (final a in s.attributes.toList()) a.key: a.value};

  test('publish records PRODUCER span with messaging.* attrs', () async {
    await tracedPusherChannelsPublish<String>(
      channel: 'private-order-updates',
      event: 'order.created',
      payloadSize: 256,
      socketId: 'sock-1',
      invoke: () async => const PusherChannelsCallResult(
        value: 'ok',
        messageId: 'evt-9',
      ),
    );
    final span = spans.findSpanByName('publish private-order-updates');
    expect(span, isNotNull);
    expect(span!.kind, SpanKind.producer);
    final a = attrsOf(span);
    expect(a['messaging.system'], 'pusher_channels');
    expect(a['messaging.operation'], 'publish');
    expect(a['messaging.destination.name'], 'private-order-updates');
    expect(a['pusher.event.name'], 'order.created');
    expect(a['messaging.message.body.size'], 256);
    expect(a['pusher.socket.id'], 'sock-1');
    expect(a['messaging.message.id'], 'evt-9');
  });

  test('subscribe records CLIENT span', () async {
    await tracedPusherChannelsSubscribe<String>(
      channel: 'presence-room-1',
      presence: true,
      invoke: () async => 'subscribed',
    );
    final span = spans.findSpanByName('subscribe presence-room-1');
    expect(span, isNotNull);
    expect(span!.kind, SpanKind.client);
    final a = attrsOf(span);
    expect(a['messaging.system'], 'pusher_channels');
    expect(a['messaging.operation'], 'subscribe');
    expect(a['messaging.destination.name'], 'presence-room-1');
    expect(a['pusher.channel.presence'], true);
  });

  test('records exception on throw', () async {
    await expectLater(
      tracedPusherChannelsPublish<String>(
        channel: 'c',
        event: 'e',
        invoke: () async => throw StateError('boom'),
      ),
      throwsA(isA<StateError>()),
    );
    expect(
      spans.findSpanByName('publish c')!.status,
      SpanStatusCode.Error,
    );
  });

  test('suppression skips spans', () async {
    await runWithoutPusherChannelsInstrumentationAsync(() async {
      await tracedPusherChannelsPublish<String>(
        channel: 'c',
        event: 'e',
        invoke: () async => const PusherChannelsCallResult(value: 'v'),
      );
      await tracedPusherChannelsSubscribe<String>(
        channel: 'c',
        invoke: () async => 'ok',
      );
    });
    expect(spans.findSpansStartingWith('publish '), isEmpty);
    expect(spans.findSpansStartingWith('subscribe '), isEmpty);
  });
}
