// Licensed under the Dartastic Pro Software License Agreement.
// Copyright 2025-2026 Mindful Software LLC d/b/a Dartastic.io, All rights reserved.

import 'dart:async';

const Symbol _suppressKey = #dartastic_otel_libs_pusher_channels_suppress;

bool pusherChannelsInstrumentationSuppressed() {
  return Zone.current[_suppressKey] == true;
}

T runWithoutPusherChannelsInstrumentation<T>(T Function() body) {
  return runZoned(body, zoneValues: {_suppressKey: true});
}

Future<T> runWithoutPusherChannelsInstrumentationAsync<T>(
  Future<T> Function() body,
) {
  return runZoned(body, zoneValues: {_suppressKey: true});
}
