#!/bin/sh
set -eu

# Chromium is only a short-lived helper that lets the official provider page
# generate its authenticated bootstrap requests. Keep autoplay/media work off,
# but prevent headless/background scheduling from stretching the provider's own
# bootstrap timers on a single-core host.
exec /usr/bin/chromium \
  --window-size=1280,720 \
  --disable-blink-features=AutomationControlled \
  --disable-background-timer-throttling \
  --disable-backgrounding-occluded-windows \
  --disable-renderer-backgrounding \
  "$@"
