"""Raising `creditsUsed` from Kiro's daily per-user activity report.

The report is the authoritative record of consumption, and it is a day late. Kiro
writes one CSV per client type (IDE, CLI, Plugin) each day at 02:00 UTC, under

    <prefix>/AWSLogs/<account>/KiroLogs/user_report/<region>/<year>/<month>/<day>/00/
        <clientType>_<account>_user_report_<timestamp>.csv

with `Credits_Used` as the credits a user consumed from their plan **during that
day**. This function sums those days into a month-to-date figure for each
subscription's current window and raises `creditsUsed` to it when it is higher.

Both this and the workers' `/usage` readings are cumulative estimates of the same
server-side figure for the same window, so the merge is the larger of the two. A
`/usage` reading taken after the report's last day is normally already higher, and
the write is a no-op. The report raises the value only when usage happened that no
fleet reading saw, such as the owner's IDE on a day the fleet did not run. If the
two sources count slightly differently, taking the larger one stops a subscription
slightly early rather than late.

Subscriptions are matched to report rows by `reportUserId`, which an operator sets
once. Subscriptions without it, or without a known window, are skipped.

Not verified against a real report: the `Date` format, that `Date` is a UTC day, and
that the month folder is the activity month. The parsing below accepts the likely
date formats and counts any row it cannot read.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import re
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Final

import boto3

from leaseclient import schema
from leaseclient.credits import window_from_environment, window_start
from leaseclient.lambda_logging import configure_logging
from leaseclient.metrics import CreditSource, Metrics
from leaseclient.registry import Registry, SubscriptionRecord

logger = logging.getLogger(__name__)

REPORT_FILE: Final = re.compile(r"_user_report_[^/]*\.csv$")
"""The per-client report. The legacy `by_user_analytic` report has no credits column."""

_DATE_FOLDERS: Final = re.compile(r"^(\d{4})/(\d{1,2})/(\d{1,2})/")

DATE_FORMATS: Final = ("%Y-%m-%d", "%m-%d-%Y", "%Y/%m/%d")

COLUMN_DATE: Final = "Date"
COLUMN_USER: Final = "UserId"
COLUMN_CLIENT: Final = "Client_Type"
COLUMN_CREDITS: Final = "Credits_Used"
# `Overage_Credits_Used` is deliberately not read: `/usage` reports credits "covered
# in plan", and the budget is compared with that figure.


@dataclass
class IngestResult:
    subscriptions_raised: int = 0
    unchanged: int = 0
    skipped_no_user_id: int = 0
    skipped_no_window: int = 0
    files_read: int = 0
    rows_used: int = 0
    rows_skipped: int = 0

    def as_dict(self) -> dict[str, int]:
        return {
            "subscriptionsRaised": self.subscriptions_raised,
            "unchanged": self.unchanged,
            "skippedNoUserId": self.skipped_no_user_id,
            "skippedNoWindow": self.skipped_no_window,
            "filesRead": self.files_read,
            "rowsUsed": self.rows_used,
            "rowsSkipped": self.rows_skipped,
        }


@dataclass(frozen=True)
class _ReportObject:
    key: str
    last_modified: datetime


@dataclass(frozen=True)
class _Day:
    credits: Decimal
    last_modified: datetime


class ReportIngester:
    """One pass: read the report files for every open window, raise where higher.

    Each pass recomputes whole windows from every file, so a file that arrives late
    is counted the next day and a rewritten day is not counted twice.
    """

    def __init__(
        self,
        s3,
        registry: Registry,
        metrics: Metrics,
        bucket: str,
        prefix: str,
        window: timedelta | None = None,
        now: datetime | None = None,
    ) -> None:
        self._s3 = s3
        self._registry = registry
        self._metrics = metrics
        self._bucket = bucket
        self._prefix = prefix if prefix.endswith("/") else f"{prefix}/"
        self._window = window
        self._now = now

    def run(self) -> IngestResult:
        result = IngestResult()
        now = self._now or datetime.now(UTC)

        candidates: list[tuple[SubscriptionRecord, date, date]] = []
        for record in self._registry.describe_all():
            if not record.report_user_id:
                result.skipped_no_user_id += 1
                continue
            if record.window_resets_at is None or record.window_resets_at_raw is None:
                # No window is known yet. The first claim's reading sets one.
                result.skipped_no_window += 1
                continue
            resets = record.window_resets_at.astimezone(UTC)
            start = window_start(resets, self._window)
            candidates.append((record, start.date(), resets.date()))

        if not candidates:
            self._metrics.report_files_read(0)
            return result

        earliest = min(start for _, start, _ in candidates)
        users = {record.report_user_id for record, _, _ in candidates}
        days = self._read_days(earliest, now.date(), users, result)
        self._metrics.report_files_read(result.files_read)

        for record, start, end in candidates:
            month_to_date = Decimal(0)
            for (day, user, _client), entry in days.items():
                if user == record.report_user_id and start <= day < end:
                    month_to_date += entry.credits
                    result.rows_used += 1

            if self._registry.raise_credits_from_report(record, month_to_date):
                result.subscriptions_raised += 1
                after = month_to_date
                logger.info(
                    "raised %s from %s to %s credits from the activity report",
                    record.subscription_id,
                    record.credits_used,
                    month_to_date,
                )
            else:
                result.unchanged += 1
                after = record.credits_used
            self._metrics.subscription_credits_used(
                record.subscription_id, after, CreditSource.REPORT
            )
        return result

    # --- reading -------------------------------------------------------------

    def _read_days(
        self, first: date, last: date, users: set[str | None], result: IngestResult
    ) -> dict[tuple[date, str, str], _Day]:
        days: dict[tuple[date, str, str], _Day] = {}
        for report in self._objects(first, last):
            body = self._s3.get_object(Bucket=self._bucket, Key=report.key)["Body"].read()
            result.files_read += 1
            text = body.decode("utf-8-sig", errors="replace")
            for row in csv.DictReader(io.StringIO(text)):
                user = (row.get(COLUMN_USER) or "").strip()
                if user not in users:
                    continue
                day = _parse_date(row.get(COLUMN_DATE))
                credits = _parse_credits(row.get(COLUMN_CREDITS))
                if day is None or credits is None:
                    result.rows_skipped += 1
                    continue
                key = (day, user, (row.get(COLUMN_CLIENT) or "").strip())
                held = days.get(key)
                # A day written twice is counted once, from the newest file.
                if held is None or report.last_modified > held.last_modified:
                    days[key] = _Day(credits, report.last_modified)
        return days

    def _objects(self, first: date, last: date) -> list[_ReportObject]:
        """Report files in date folders from `first`'s month to `last`'s month.

        Listed by year and filtered by the folder's month, so zero-padded and
        unpadded month folders both match.
        """
        wanted = (first.year, first.month), (last.year, last.month)
        found: list[_ReportObject] = []
        paginator = self._s3.get_paginator("list_objects_v2")
        for year in range(first.year, last.year + 1):
            pages = paginator.paginate(Bucket=self._bucket, Prefix=f"{self._prefix}{year}/")
            for page in pages:
                for entry in page.get("Contents", []):
                    key = entry["Key"]
                    if not REPORT_FILE.search(key):
                        continue
                    folders = _DATE_FOLDERS.match(key[len(self._prefix):])
                    if folders is None:
                        continue
                    month = (int(folders.group(1)), int(folders.group(2)))
                    if wanted[0] <= month <= wanted[1]:
                        found.append(_ReportObject(key, entry["LastModified"]))
        return found


def _parse_date(raw: str | None) -> date | None:
    value = (raw or "").strip()
    for layout in DATE_FORMATS:
        try:
            return datetime.strptime(value, layout).date()
        except ValueError:
            continue
    return None


def _parse_credits(raw: str | None) -> Decimal | None:
    value = (raw or "").strip()
    if not value:
        return None
    try:
        credits = Decimal(value)
    except InvalidOperation:
        return None
    if not credits.is_finite() or credits < 0:
        return None
    return credits


def handler(event: dict[str, Any], context: Any = None) -> dict[str, Any]:
    configure_logging()
    result = _from_environment().run()
    logger.info("activity report result: %s", result.as_dict())
    return result.as_dict()


def _from_environment() -> ReportIngester:
    region = os.environ.get("REPORT_REGION") or None
    return ReportIngester(
        s3=boto3.client("s3", region_name=region),
        registry=Registry(boto3.client("dynamodb"), schema.subscriptions_table_name()),
        metrics=Metrics(),
        bucket=os.environ["REPORT_BUCKET"],
        prefix=os.environ["REPORT_PREFIX"],
        window=window_from_environment(),
    )
