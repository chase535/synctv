#!/bin/sh
set -eu

# Chromium is only a short-lived helper that lets the official provider page
# generate its authenticated bootstrap requests. Keep the launcher intentionally
# small: desktop layout plus normal WebDriver identity, then let the Rust CDP
# bootstrap hook observe bounded same-provider XHR/fetch responses.
exec /usr/bin/chromium \
  --window-size=1280,720 \
  --disable-blink-features=AutomationControlled \
  "$@"
