"""Generic REST page connector; credentials are read from a named environment variable."""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen

from ..model import IngestionBatch, TableMetadata
from .file import watermark_lower_bound


class RestApiConnector:
    def extract(self, metadata: TableMetadata, watermark: str | None) -> IngestionBatch:
        options = metadata.source_options
        base_url = options["url"]
        token_env = options.get("token_env")
        token = os.environ.get(token_env) if token_env else None
        if token_env and not token:
            raise RuntimeError(f"Required API credential environment variable {token_env!r} is not set")
        params = dict(options.get("params", {}))
        if metadata.watermark_column and watermark:
            params["updated_after"] = watermark_lower_bound(
                watermark, int(options.get("watermark_lookback_seconds", 0)))
        records, url = [], base_url
        page_param = options.get("page_param", "page")
        page = int(options.get("start_page", 1))
        started = time.monotonic()
        max_pages = int(options.get("max_pages", 1000))
        for _ in range(max_pages):
            page_params = {**params, page_param: page}
            request = Request(url + ("&" if "?" in url else "?") + urlencode(page_params),
                              headers={"Accept": "application/json", **({"Authorization": f"Bearer {token}"} if token else {})})
            payload = _request_json(request, int(options.get("timeout_seconds", 30)), int(options.get("retries", metadata.retries)))
            records.extend(payload.get(options.get("records_key", "records"), []))
            next_url = payload.get(options.get("next_url_key", "next"))
            if next_url:
                url, page = urljoin(url, next_url), page + 1
            elif payload.get("has_more", False):
                page += 1
            else:
                break
            if time.monotonic() - started > int(options.get("max_duration_seconds", 300)):
                raise TimeoutError("REST extraction exceeded max_duration_seconds")
        else:
            raise RuntimeError(f"REST extraction exceeded max_pages={max_pages}")
        next_watermark = max((str(r[metadata.watermark_column]) for r in records if metadata.watermark_column and r.get(metadata.watermark_column)), default=None)
        return IngestionBatch(records, len(records), datetime.now(timezone.utc), next_watermark)


def _request_json(request: Request, timeout: int, retries: int) -> dict:
    for attempt in range(retries + 1):
        try:
            with urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            if exc.code not in {429, 500, 502, 503, 504} or attempt == retries:
                raise
            delay = min(2 ** attempt, 30)
            retry_after = exc.headers.get("Retry-After")
            time.sleep(min(float(retry_after), 60) if retry_after and retry_after.isdigit() else delay)
        except (URLError, TimeoutError):
            if attempt == retries:
                raise
            time.sleep(min(2 ** attempt, 30))
    raise AssertionError("unreachable")
