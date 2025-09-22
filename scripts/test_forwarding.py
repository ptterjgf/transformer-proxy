#!/usr/bin/env python3
"""Utility script to verify SiliconFlow proxy forwarding.

Run this locally or inside your deployment environment after the service is
started. The script performs three checks:

1. A public health probe (`GET /health`).
2. An authenticated key health check (`GET /admin/keys/health-check`).
3. A sample chat completion request (`POST /v1/chat/completions`).

Configuration happens entirely through environment variables so you do not
have to modify the source code or expose real secrets in the repository.

Required environment variables
------------------------------
PROXY_AUTH_KEY
    Auth token that the proxy accepts in the `Authorization` header.

Optional environment variables
------------------------------
PROXY_BASE_URL        Base URL of the deployed proxy. Defaults to
                      ``http://127.0.0.1:10000``.
PROXY_ADMIN_KEY       Admin token if it differs from ``PROXY_AUTH_KEY``.
PROXY_TIMEOUT_SECONDS Request timeout in seconds. Defaults to ``45``.
PROXY_TEST_MODEL      Model name for the chat completion request.
                      Defaults to ``Qwen/Qwen2.5-7B-Instruct``.
PROXY_TEST_PROMPT     User prompt for the chat completion request.
PROXY_TEST_SYSTEM_PROMPT  System prompt for the chat completion request.
PROXY_TEST_MAX_TOKENS Maximum tokens to request. Defaults to ``64``.
PROXY_TEST_TEMPERATURE Sampling temperature. Defaults to ``0``.
PROXY_TEST_EXTRA_JSON Additional JSON fields merged into the chat payload.
PROXY_VERIFY_TLS      Set to ``false``/``0`` to disable TLS verification when
                      targeting HTTPS endpoints with self-signed certs.
PROXY_CA_BUNDLE       Path to a CA bundle when custom verification is needed.

Usage example
-------------
```bash
export PROXY_BASE_URL="https://your-proxy.example.com"
export PROXY_AUTH_KEY="sk-auth-..."
export PROXY_ADMIN_KEY="admin-..."   # optional if different
python scripts/test_forwarding.py
```

The script exits with a non-zero status code if any of the checks fail.
"""

from __future__ import annotations

import asyncio
import json
import os
import ssl
import sys
import time
from typing import Any, Dict

import aiohttp


def _bool_from_env(value: str | None, *, default: bool = True) -> bool:
    if value is None:
        return default
    return value.strip().lower() not in {"0", "false", "no", "off"}


def _build_chat_payload() -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "model": os.getenv("PROXY_TEST_MODEL", "Qwen/Qwen2.5-7B-Instruct"),
        "messages": [
            {
                "role": "system",
                "content": os.getenv(
                    "PROXY_TEST_SYSTEM_PROMPT",
                    "You are a helpful assistant that verifies proxy connectivity.",
                ),
            },
            {
                "role": "user",
                "content": os.getenv(
                    "PROXY_TEST_PROMPT",
                    "Please confirm that the SiliconFlow proxy can reach the upstream service.",
                ),
            },
        ],
        "max_tokens": int(os.getenv("PROXY_TEST_MAX_TOKENS", "64")),
        "temperature": float(os.getenv("PROXY_TEST_TEMPERATURE", "0")),
    }

    extra = os.getenv("PROXY_TEST_EXTRA_JSON")
    if extra:
        try:
            payload.update(json.loads(extra))
        except json.JSONDecodeError as exc:
            print(f"[WARN] Failed to parse PROXY_TEST_EXTRA_JSON: {exc}", file=sys.stderr)
    return payload


async def _check_health(session: aiohttp.ClientSession, base_url: str, ssl_ctx: Any) -> None:
    url = f"{base_url}/health"
    print(f"[INFO] Checking public health endpoint: {url}")
    async with session.get(url, ssl=ssl_ctx) as response:
        response.raise_for_status()
        data = await response.json()
    status = data.get("status")
    print(f"[OK] /health returned status={status!r}, active_keys={data.get('active_keys')}.")


async def _check_key_health(
    session: aiohttp.ClientSession, base_url: str, admin_key: str, ssl_ctx: Any
) -> None:
    url = f"{base_url}/admin/keys/health-check"
    print(f"[INFO] Checking key health endpoint: {url}")
    headers = {"Authorization": f"Bearer {admin_key}"}
    async with session.get(url, headers=headers, ssl=ssl_ctx) as response:
        response.raise_for_status()
        data = await response.json()
    print(
        "[OK] /admin/keys/health-check => "
        f"total={data.get('total_keys')} active={data.get('active_keys')}"
    )


async def _check_chat_completion(
    session: aiohttp.ClientSession, base_url: str, auth_key: str, ssl_ctx: Any
) -> None:
    url = f"{base_url}/v1/chat/completions"
    payload = _build_chat_payload()
    print(
        "[INFO] Sending chat completion request to "
        f"model={payload.get('model')!r} via {url}"
    )
    headers = {"Authorization": f"Bearer {auth_key}"}
    started = time.perf_counter()
    async with session.post(url, headers=headers, json=payload, ssl=ssl_ctx) as response:
        response.raise_for_status()
        data = await response.json()
    elapsed = time.perf_counter() - started
    choices = data.get("choices")
    usage = data.get("usage", {})
    print(
        "[OK] Chat completion succeeded in "
        f"{elapsed:.2f}s, choices={len(choices) if choices else 0}, "
        f"tokens={usage.get('total_tokens')}"
    )


async def main() -> None:
    base_url = os.getenv("PROXY_BASE_URL", "http://127.0.0.1:10000").rstrip("/")
    auth_key = os.getenv("PROXY_AUTH_KEY")
    if not auth_key:
        print("[ERROR] Missing required environment variable PROXY_AUTH_KEY.", file=sys.stderr)
        sys.exit(1)

    admin_key = os.getenv("PROXY_ADMIN_KEY", auth_key)
    timeout = float(os.getenv("PROXY_TIMEOUT_SECONDS", "45"))

    verify_tls = _bool_from_env(
        os.getenv("PROXY_VERIFY_TLS"), default=base_url.startswith("https://")
    )
    ssl_ctx: Any = None
    if base_url.startswith("https://"):
        if not verify_tls:
            ssl_ctx = False  # disable TLS verification
        else:
            ca_bundle = os.getenv("PROXY_CA_BUNDLE")
            if ca_bundle:
                ssl_ctx = ssl.create_default_context(cafile=ca_bundle)

    timeout_cfg = aiohttp.ClientTimeout(total=timeout)
    async with aiohttp.ClientSession(timeout=timeout_cfg) as session:
        await _check_health(session, base_url, ssl_ctx)
        await _check_key_health(session, base_url, admin_key, ssl_ctx)
        await _check_chat_completion(session, base_url, auth_key, ssl_ctx)

    print("[DONE] All checks completed successfully.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except aiohttp.ClientResponseError as exc:
        print(
            f"[ERROR] Request failed with status {exc.status}: {exc.message}",
            file=sys.stderr,
        )
        sys.exit(2)
    except aiohttp.ClientError as exc:
        print(f"[ERROR] HTTP request error: {exc}", file=sys.stderr)
        sys.exit(3)
    except KeyboardInterrupt:
        print("[WARN] Aborted by user.", file=sys.stderr)
        sys.exit(130)
    except Exception as exc:  # pragma: no cover - unexpected errors
        print(f"[ERROR] Unexpected error: {exc}", file=sys.stderr)
        sys.exit(99)
