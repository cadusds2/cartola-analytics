"""HTTP client utilities for interacting with Cartola FC endpoints."""

from __future__ import annotations

import hashlib
import json
import logging
import time
from pathlib import Path
from typing import Any

import httpx

from .config import CartolaSettings, load_settings
from .endpoints import Endpoint


class CartolaClient:
    """Thin wrapper around httpx with retry and cache support."""

    def __init__(
        self,
        *,
        timeout: float | None = None,
        max_retries: int | None = None,
        backoff_factor: float | None = None,
        cache_ttl: int | None = None,
        cache_dir: Path | None = None,
        headers: dict[str, str] | None = None,
        settings: CartolaSettings | None = None,
    ) -> None:
        self.settings = settings or load_settings()
        self.logger = logging.getLogger(f"{__name__}.{self.__class__.__name__}")
        self.logger.setLevel(
            getattr(logging, self.settings.log_level.upper(), logging.INFO)
        )

        self.timeout = timeout if timeout is not None else self.settings.timeout
        retries_candidate = (
            max_retries if max_retries is not None else self.settings.max_retries
        )
        self.max_retries = max(retries_candidate, 0)
        backoff_candidate = (
            backoff_factor
            if backoff_factor is not None
            else self.settings.backoff_factor
        )
        self.backoff_factor = max(backoff_candidate, 0.0)
        ttl_candidate = cache_ttl if cache_ttl is not None else self.settings.cache_ttl
        self.cache_ttl = max(ttl_candidate, 0)
        self.cache_dir = cache_dir if cache_dir is not None else self.settings.cache_dir

        if self.cache_dir is not None:
            self.cache_dir.mkdir(parents=True, exist_ok=True)

        resolved_headers = headers or default_headers(self.settings)
        self._client = httpx.Client(
            timeout=httpx.Timeout(self.timeout),
            headers=resolved_headers,
            follow_redirects=True,
        )

        self.logger.debug(
            "client_initialized",
            extra={
                "event": "client_initialized",
                "timeout": self.timeout,
                "max_retries": self.max_retries,
                "backoff_factor": self.backoff_factor,
                "cache_ttl": self.cache_ttl,
                "cache_dir": str(self.cache_dir) if self.cache_dir else None,
            },
        )

    @classmethod
    def from_env(cls, **kwargs: Any) -> CartolaClient:
        """Instantiate client pulling configuration from .env/env vars."""
        return cls(settings=load_settings(), **kwargs)

    def close(self) -> None:
        """Close the underlying HTTP client."""
        self._client.close()

    def __enter__(self) -> CartolaClient:  # pragma: no cover - convenience
        return self

    def __exit__(self, *_args: Any) -> None:  # pragma: no cover - convenience
        self.close()

    def fetch(
        self,
        endpoint: Endpoint,
        *,
        rodada: int | None = None,
        use_cache: bool = True,
    ) -> Any:
        """Fetch JSON payload for a given endpoint."""
        url = endpoint.resolve(rodada)
        self.logger.info(
            "fetch_start",
            extra={
                "event": "fetch_start",
                "endpoint": endpoint.name,
                "url": url,
                "rodada": rodada,
                "use_cache": use_cache,
            },
        )
        cache_key = self._cache_key(url)
        if use_cache:
            cached = self._read_cache(cache_key)
            if cached is not None:
                self.logger.info(
                    "cache_hit",
                    extra={
                        "event": "cache_hit",
                        "endpoint": endpoint.name,
                        "url": url,
                    },
                )
                return cached

        response_json = self._request_with_retries(url, endpoint.name)
        if use_cache:
            self._write_cache(cache_key, response_json)
        self.logger.info(
            "fetch_success",
            extra={
                "event": "fetch_success",
                "endpoint": endpoint.name,
                "url": url,
            },
        )
        return response_json

    def _request_with_retries(self, url: str, endpoint_name: str) -> Any:
        attempt = 0
        while True:
            try:
                self.logger.debug(
                    "http_request",
                    extra={
                        "event": "http_request",
                        "url": url,
                        "endpoint": endpoint_name,
                        "attempt": attempt + 1,
                    },
                )
                response = self._client.get(url)
                if response.status_code in {429} or response.status_code >= 500:
                    raise httpx.HTTPStatusError(
                        "Retryable response",
                        request=response.request,
                        response=response,
                    )
                response.raise_for_status()
                self.logger.info(
                    "http_success",
                    extra={
                        "event": "http_success",
                        "url": url,
                        "endpoint": endpoint_name,
                        "status": response.status_code,
                        "attempt": attempt + 1,
                    },
                )
                if not response.content:
                    self.logger.warning(
                        "http_empty",
                        extra={
                            "event": "http_empty",
                            "url": url,
                            "endpoint": endpoint_name,
                        },
                    )
                    return {}
                try:
                    return response.json()
                except json.JSONDecodeError as err:
                    self.logger.error(
                        "http_invalid_json",
                        extra={
                            "event": "http_invalid_json",
                            "url": url,
                            "endpoint": endpoint_name,
                            "error": str(err),
                        },
                    )
                    raise ValueError(
                        "Resposta JSON invalida da API Cartola"
                    ) from err
            except (
                httpx.TimeoutException,
                httpx.TransportError,
                httpx.HTTPStatusError,
            ) as err:
                if attempt >= self.max_retries:
                    self.logger.error(
                        "http_failure",
                        extra={
                            "event": "http_failure",
                            "url": url,
                            "endpoint": endpoint_name,
                            "attempt": attempt + 1,
                            "error": str(err),
                        },
                    )
                    raise
                self.logger.warning(
                    "http_retry",
                    extra={
                        "event": "http_retry",
                        "url": url,
                        "endpoint": endpoint_name,
                        "attempt": attempt + 1,
                        "error": str(err),
                    },
                )
                sleep_for = self.backoff_factor * (2**attempt)
                time.sleep(sleep_for)
                attempt += 1

    def _cache_key(self, url: str) -> str:
        digest = hashlib.sha1(url.encode("utf-8"), usedforsecurity=False)
        return digest.hexdigest()

    def _cache_path(self, key: str) -> Path:
        assert self.cache_dir is not None  # sanity
        return self.cache_dir / f"{key}.json"

    def _read_cache(self, key: str) -> Any | None:
        if self.cache_dir is None or self.cache_ttl <= 0:
            return None
        path = self._cache_path(key)
        if not path.exists():
            return None
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            path.unlink(missing_ok=True)
            self.logger.warning(
                "cache_corrupted",
                extra={"event": "cache_corrupted", "path": str(path)},
            )
            return None
        timestamp = raw.get("timestamp")
        payload = raw.get("payload")
        if timestamp is None or time.time() - float(timestamp) > self.cache_ttl:
            path.unlink(missing_ok=True)
            self.logger.info(
                "cache_expired",
                extra={"event": "cache_expired", "path": str(path)},
            )
            return None
        return payload

    def _write_cache(self, key: str, payload: Any) -> None:
        if self.cache_dir is None or self.cache_ttl <= 0:
            return
        path = self._cache_path(key)
        data = {"timestamp": time.time(), "payload": payload}
        path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        self.logger.debug(
            "cache_store",
            extra={
                "event": "cache_store",
                "path": str(path),
                "cache_ttl": self.cache_ttl,
            },
        )


def default_headers(settings: CartolaSettings | None = None) -> dict[str, str]:
    """Default headers for Cartola API calls."""
    active_settings = settings or load_settings()
    return {
        "User-Agent": active_settings.user_agent,
        "Accept": active_settings.accept,
    }
