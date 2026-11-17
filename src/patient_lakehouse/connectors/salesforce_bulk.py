"""Salesforce Bulk API v2 query connector with bounded polling and locator paging."""
from __future__ import annotations

import csv
import io
import json
import os
import re
import time
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from ..model import IngestionBatch, TableMetadata
from .file import watermark_lower_bound


class SalesforceBulkApiConnector:
    def extract(self, metadata: TableMetadata, watermark: str | None) -> IngestionBatch:
        opts = metadata.source_options
        token_env = opts.get("token_env", "SALESFORCE_ACCESS_TOKEN")
        token = os.environ.get(token_env)
        if not token:
            raise RuntimeError(f"Required Salesforce credential environment variable {token_env!r} is not set")
        base_url = opts["instance_url"].rstrip("/")
        version = str(opts.get("api_version", "v60.0"))
        root = f"{base_url}/services/data/{version}/jobs/query"
        fields = opts["fields"]
        object_name = opts["object"]
        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", object_name) or not fields:
            raise ValueError("Salesforce object and fields must be API names from reviewed metadata")
        for field in fields:
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", field):
                raise ValueError(f"Invalid Salesforce field API name {field!r}")
        soql = f"SELECT {', '.join(fields)} FROM {object_name}"
        if metadata.watermark_column and watermark:
            lower_bound = watermark_lower_bound(watermark, int(opts.get("watermark_lookback_seconds", 0)))
            if not re.fullmatch(r"[0-9TtZz:+.\-]+", lower_bound):
                raise ValueError("Invalid watermark value for Salesforce SOQL")
            soql += f" WHERE {metadata.watermark_column} > '{lower_bound}'"
        headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
        job = self._json_request("POST", root, headers, {"operation": "query", "query": soql}, opts)
        job_id = job["id"]
        deadline = time.monotonic() + int(opts.get("max_job_seconds", 900))
        delay = float(opts.get("poll_interval_seconds", 2))
        while True:
            if time.monotonic() >= deadline:
                raise TimeoutError(f"Salesforce Bulk API job {job_id} exceeded its time budget")
            state = self._json_request("GET", f"{root}/{job_id}", headers, None, opts)
            status = state.get("state")
            if status == "JobComplete":
                break
            if status in {"Failed", "Aborted"}:
                raise RuntimeError(f"Salesforce Bulk API job {job_id} {status}: {state.get('errorMessage', '')}")
            time.sleep(delay)
            delay = min(delay * 1.5, float(opts.get("max_poll_interval_seconds", 15)))
        records, locator = [], None
        page_size = int(opts.get("page_size", 10000))
        while True:
            query = urlencode({"maxRecords": page_size, **({"locator": locator} if locator else {})})
            response, next_locator = self._csv_page(f"{root}/{job_id}/results?{query}", headers, opts)
            records.extend(response)
            if not next_locator or next_locator == "null":
                break
            locator = next_locator
        next_watermark = max((str(row[metadata.watermark_column]) for row in records
                              if metadata.watermark_column and row.get(metadata.watermark_column)), default=None)
        for row in records:
            _coerce_record(row, opts.get("data_types", {}))
        return IngestionBatch(records, len(records), datetime.now(timezone.utc), next_watermark, source_offset=job_id)

    def _json_request(self, method: str, url: str, headers: dict, payload: dict | None, options: dict) -> dict:
        body = json.dumps(payload).encode() if payload is not None else None
        request_headers = {**headers, **({"Content-Type": "application/json"} if body else {})}
        response = self._request(Request(url, data=body, headers=request_headers, method=method), options)
        return json.loads(response.decode()) if response else {}

    def _csv_page(self, url: str, headers: dict, options: dict) -> tuple[list[dict], str | None]:
        request = Request(url, headers={**headers, "Accept": "text/csv"})
        response = self._request(request, options, include_headers=True)
        data, response_headers = response
        rows = list(csv.DictReader(io.StringIO(data.decode("utf-8"))))
        return rows, response_headers.get("Sforce-Locator")

    def _request(self, request: Request, options: dict, include_headers: bool = False):
        retries = int(options.get("retries", 4))
        timeout = int(options.get("timeout_seconds", 60))
        for attempt in range(retries + 1):
            try:
                with urlopen(request, timeout=timeout) as response:
                    content = response.read()
                    return (content, response.headers) if include_headers else content
            except HTTPError as exc:
                if exc.code not in {429, 500, 502, 503, 504} or attempt == retries:
                    raise
                retry_after = exc.headers.get("Retry-After")
                time.sleep(min(float(retry_after), 60) if retry_after and retry_after.isdigit() else min(2 ** attempt, 30))
            except (URLError, TimeoutError):
                if attempt == retries:
                    raise
                time.sleep(min(2 ** attempt, 30))
        raise AssertionError("unreachable")


def _coerce_record(row: dict, data_types: dict) -> None:
    for column, data_type in data_types.items():
        value = row.get(column)
        if value in (None, ""):
            row[column] = None
        elif data_type == "integer":
            row[column] = int(value)
        elif data_type == "number":
            row[column] = float(value)
        elif data_type == "boolean":
            normalized = str(value).strip().lower()
            if normalized not in {"true", "false"}:
                raise ValueError(f"Invalid Salesforce boolean value for {column!r}")
            row[column] = normalized == "true"
