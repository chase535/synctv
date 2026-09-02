(() => {
  if (window.__synctvIqiyiBootstrapHookInstalled === true) return;
  window.__synctvIqiyiBootstrapHookInstalled = true;

  const MAX_RESPONSES = 8;
  const MAX_BODY_CHARS = 262144;
  const MAX_MEDIA_URLS = 12;
  const MEDIA_URL_RE = /(?:https?:)?\/\/[^\s\"'<>\\]+?\.(?:m3u8|mpd|mp4)(?:\?[^\s\"'<>\\]*)?/gi;
  const MEDIA_KEY_RE = /(?:m3u8|mpd|play.?url|stream.?url|video.?url|media.?url)/i;
  const IMPORTANT_PATH_RE = /(?:\/dash(?:[/?#]|$)|\/jp\/|\/videos?\/|play|stream|manifest)/i;
  const seen = new Set();
  let inspectedResponses = 0;

  const normalizeText = (value) => String(value || '')
    .slice(0, MAX_BODY_CHARS)
    .replace(/\\u003a/gi, ':')
    .replace(/\\u002f/gi, '/')
    .replace(/\\u0026/gi, '&')
    .replace(/\\u003d/gi, '=')
    .replace(/\\x3a/gi, ':')
    .replace(/\\x2f/gi, '/')
    .replace(/\\x26/gi, '&')
    .replace(/\\x3d/gi, '=')
    .replace(/\\\//g, '/')
    .replace(/&amp;/gi, '&');

  const parseUrl = (raw) => {
    try {
      return new URL(raw, location.href);
    } catch (_) {
      return null;
    }
  };

  const isProviderUrl = (raw) => {
    const url = parseUrl(raw);
    if (!url) return false;
    const host = url.hostname.toLowerCase();
    return host === 'iqiyi.com' || host.endsWith('.iqiyi.com') || host === 'qiyi.com' || host.endsWith('.qiyi.com');
  };

  const isInterestingResponse = (raw) => {
    const url = parseUrl(raw);
    if (!url || !isProviderUrl(url.href)) return false;
    const host = url.hostname.toLowerCase();
    return host.includes('video.') || host.startsWith('video.') || host.startsWith('cache.') || IMPORTANT_PATH_RE.test(url.pathname);
  };

  const attachSource = (href) => {
    if (seen.size >= MAX_MEDIA_URLS || seen.has(href)) return;
    seen.add(href);
    const source = document.createElement('source');
    source.setAttribute('data-synctv-iqiyi-bootstrap', '1');
    source.setAttribute('src', href);
    source.hidden = true;

    const attach = () => {
      const parent = document.documentElement || document.head || document.body;
      if (parent) {
        parent.appendChild(source);
      } else {
        setTimeout(attach, 0);
      }
    };
    attach();
  };

  const publishCandidate = (raw) => {
    if (!raw || seen.size >= MAX_MEDIA_URLS) return;
    const url = parseUrl(String(raw).trim());
    if (!url || (url.protocol !== 'http:' && url.protocol !== 'https:')) return;
    attachSource(url.href);
  };

  const scanJson = (value, keyHint, depth, budget) => {
    if (budget.count <= 0 || depth > 8 || seen.size >= MAX_MEDIA_URLS) return;
    budget.count -= 1;
    if (typeof value === 'string') {
      if (MEDIA_KEY_RE.test(keyHint || '') && /^(?:https?:)?\/\//i.test(value.trim())) {
        publishCandidate(value);
      }
      scanText(value, false);
      return;
    }
    if (!value || typeof value !== 'object') return;
    if (Array.isArray(value)) {
      for (const item of value) {
        scanJson(item, keyHint, depth + 1, budget);
        if (budget.count <= 0 || seen.size >= MAX_MEDIA_URLS) break;
      }
      return;
    }
    for (const [key, child] of Object.entries(value)) {
      scanJson(child, key, depth + 1, budget);
      if (budget.count <= 0 || seen.size >= MAX_MEDIA_URLS) break;
    }
  };

  const scanText = (rawText, tryJson = true) => {
    if (!rawText || seen.size >= MAX_MEDIA_URLS) return;
    const text = normalizeText(rawText);
    const matches = text.match(MEDIA_URL_RE) || [];
    for (const match of matches) {
      publishCandidate(match);
      if (seen.size >= MAX_MEDIA_URLS) return;
    }
    if (!tryJson) return;
    const trimmed = text.trimStart();
    if (!(trimmed.startsWith('{') || trimmed.startsWith('['))) return;
    try {
      scanJson(JSON.parse(text), '', 0, { count: 2000 });
    } catch (_) {}
  };

  const inspectText = (url, text) => {
    if (inspectedResponses >= MAX_RESPONSES || !isInterestingResponse(url)) return;
    inspectedResponses += 1;
    scanText(text, true);
  };

  const nativeFetch = window.fetch;
  if (typeof nativeFetch === 'function') {
    window.fetch = function (...args) {
      const result = nativeFetch.apply(this, args);
      return Promise.resolve(result).then((response) => {
        try {
          const url = response && response.url ? response.url : String(args[0] || '');
          if (inspectedResponses < MAX_RESPONSES && isInterestingResponse(url)) {
            const length = Number(response.headers && response.headers.get('content-length')) || 0;
            if (length <= MAX_BODY_CHARS) {
              response.clone().text().then((text) => inspectText(url, text)).catch(() => {});
            }
          }
        } catch (_) {}
        return response;
      });
    };
  }

  const nativeOpen = XMLHttpRequest.prototype.open;
  const nativeSend = XMLHttpRequest.prototype.send;
  const xhrUrl = Symbol('synctvIqiyiXhrUrl');

  XMLHttpRequest.prototype.open = function (method, url, ...rest) {
    try {
      this[xhrUrl] = new URL(String(url || ''), location.href).href;
    } catch (_) {
      this[xhrUrl] = String(url || '');
    }
    return nativeOpen.call(this, method, url, ...rest);
  };

  XMLHttpRequest.prototype.send = function (...args) {
    const url = this[xhrUrl] || '';
    if (inspectedResponses < MAX_RESPONSES && isInterestingResponse(url)) {
      this.addEventListener('loadend', () => {
        try {
          if (inspectedResponses >= MAX_RESPONSES) return;
          if (this.responseType === '' || this.responseType === 'text') {
            inspectText(url, this.responseText || '');
          } else if (this.responseType === 'json' && this.response != null) {
            inspectText(url, JSON.stringify(this.response));
          }
        } catch (_) {}
      }, { once: true });
    }
    return nativeSend.apply(this, args);
  };
})();
