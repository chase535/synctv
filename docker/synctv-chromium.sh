#!/bin/sh
set -eu

EXTENSION_DIR=/usr/local/share/synctv-iqiyi-bootstrap

filter_and_exec() {
  for original do
    shift
    if [ "$original" = "--disable-extensions" ]; then
      continue
    fi
    set -- "$@" "$original"
  done

  exec /usr/bin/chromium \
    --autoplay-policy=no-user-gesture-required \
    --window-size=1280,720 \
    --disable-background-timer-throttling \
    --disable-backgrounding-occluded-windows \
    --disable-renderer-backgrounding \
    --disable-blink-features=AutomationControlled \
    --disable-component-extensions-with-background-pages \
    --disable-extensions-except="$EXTENSION_DIR" \
    --load-extension="$EXTENSION_DIR" \
    "$@"
}

filter_and_exec "$@"
