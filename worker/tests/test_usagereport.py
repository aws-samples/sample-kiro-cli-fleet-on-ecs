"""Raising `creditsUsed` from the daily activity report.

The CSVs here are synthetic. Their columns and object keys follow the documented
layout (Kiro docs, "Viewing per-user activity"); no real report has been read by
these tests or by anything else in this repository. The `Date` format in particular
is an assumption, which is why more than one format is accepted and an unreadable
row is counted rather than fatal.
"""

from __future__ import annotations

import io
import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest

from leaseclient.metrics import Metrics
from leaseclient.registry import Registry
from usagereport.handler import ReportIngester

ACCOUNT = "123456789012"
PREFIX = f"activity/AWSLogs/{ACCOUNT}/KiroLogs/user_report/us-east-1/"
BUCKET = "example-report-bucket"

WINDOW_END = datetime(2026, 11, 1, tzinfo=UTC)
NOW = datetime(2026, 10, 20, 3, 30, tzinfo=UTC)

COLUMNS = (
    "Date,UserId,Client_Type,Chat_Conversations,Credits_Used,Overage_Cap,"
    "Overage_Credits_Used,Overage_Enabled,ProfileId,Subscription_Tier,Total_Messages,"
    "New_User,User_Email,auto_messages"
)


def row(
    day: str,
    user: str,
    client: str,
    credits: str,
    overage: str = "0",
) -> str:
    return (
        f"{day},{user},{client},3,{credits},10000,{overage},false,"
        f"profile-example,Power,40,false,{user}@example.com,40"
    )


def csv_of(*rows: str) -> bytes:
    return ("\ufeff" + "\n".join((COLUMNS, *rows)) + "\n").encode()


def key_for(day: str, client: str = "KIRO_CLI", folder_month: str | None = None) -> str:
    year, month, dom = day.split("-")
    month = folder_month or month
    return (
        f"{PREFIX}{year}/{month}/{dom}/00/"
        f"{client}_{ACCOUNT}_user_report_{year}{month}{dom}T020000Z.csv"
    )


class FakeS3:
    """ListObjectsV2 by prefix and GetObject, which is all the ingester calls."""

    def __init__(self) -> None:
        self.objects: dict[str, tuple[bytes, datetime]] = {}
        self.listed_prefixes: list[str] = []

    def put(self, key: str, body: bytes, modified: datetime | None = None) -> None:
        self.objects[key] = (body, modified or datetime(2026, 10, 2, 2, tzinfo=UTC))

    def get_object(self, Bucket: str, Key: str) -> dict[str, Any]:  # noqa: N803
        assert Bucket == BUCKET
        return {"Body": io.BytesIO(self.objects[Key][0])}

    def get_paginator(self, name: str) -> Any:
        assert name == "list_objects_v2"
        objects = self.objects
        listed = self.listed_prefixes

        class Paginator:
            def paginate(self, Bucket: str, Prefix: str) -> list[dict[str, Any]]:  # noqa: N803
                listed.append(Prefix)
                contents = [
                    {"Key": k, "LastModified": m}
                    for k, (_, m) in sorted(objects.items())
                    if k.startswith(Prefix)
                ]
                # Two pages, so pagination is exercised.
                half = len(contents) // 2
                return [{"Contents": contents[:half]}, {"Contents": contents[half:]}]

        return Paginator()


@pytest.fixture
def s3() -> FakeS3:
    return FakeS3()


@pytest.fixture
def sink() -> io.StringIO:
    return io.StringIO()


@pytest.fixture
def registry(pool, dynamodb) -> Registry:
    return Registry(dynamodb, pool.subscriptions_table)


def ingest(s3: FakeS3, registry: Registry, sink: io.StringIO):
    return ReportIngester(
        s3=s3,
        registry=registry,
        metrics=Metrics(stream=sink),
        bucket=BUCKET,
        prefix=PREFIX,
        now=NOW,
    ).run()


def a_mapped_subscription(pool, user: str = "user-a", used: str = "0", **kwargs):
    return pool.seed_subscription(
        credits_used=used,
        window_resets_raw=WINDOW_END.isoformat(),
        report_user_id=user,
        **kwargs,
    )


class TestMonthToDate:
    def test_days_and_client_types_are_summed(self, pool, registry, s3, sink) -> None:
        """`Credits_Used` is per day and per client; `/usage` is the user's total."""
        seeded = a_mapped_subscription(pool)
        s3.put(key_for("2026-10-01"), csv_of(row("2026-10-01", "user-a", "KIRO_CLI", "1.25")))
        s3.put(
            key_for("2026-10-01", "KIRO_IDE"),
            csv_of(row("2026-10-01", "user-a", "KIRO_IDE", "2.00")),
        )
        s3.put(key_for("2026-10-02"), csv_of(row("2026-10-02", "user-a", "KIRO_CLI", "0.50")))

        result = ingest(s3, registry, sink)

        assert pool.credits_used(seeded.subscription_id) == Decimal("3.75")
        assert result.subscriptions_raised == 1
        assert result.files_read == 3
        assert result.rows_used == 3

    def test_rows_outside_the_window_are_ignored(self, pool, registry, s3, sink) -> None:
        seeded = a_mapped_subscription(pool)
        s3.put(
            key_for("2026-10-05"),
            csv_of(
                row("2026-09-30", "user-a", "KIRO_CLI", "500"),
                row("2026-10-05", "user-a", "KIRO_CLI", "4"),
                row("2026-11-01", "user-a", "KIRO_CLI", "700"),
            ),
        )

        ingest(s3, registry, sink)

        assert pool.credits_used(seeded.subscription_id) == Decimal("4")

    def test_a_rewritten_day_is_counted_once_from_the_newest_file(
        self, pool, registry, s3, sink
    ) -> None:
        seeded = a_mapped_subscription(pool)
        s3.put(
            key_for("2026-10-03"),
            csv_of(row("2026-10-03", "user-a", "KIRO_CLI", "1")),
            modified=datetime(2026, 10, 4, 2, tzinfo=UTC),
        )
        s3.put(
            key_for("2026-10-03").replace("T020000Z", "T090000Z"),
            csv_of(row("2026-10-03", "user-a", "KIRO_CLI", "6")),
            modified=datetime(2026, 10, 4, 9, tzinfo=UTC),
        )

        ingest(s3, registry, sink)

        assert pool.credits_used(seeded.subscription_id) == Decimal("6")

    def test_overage_is_not_counted(self, pool, registry, s3, sink) -> None:
        """`/usage` reports credits covered in plan, and the budget compares with that."""
        seeded = a_mapped_subscription(pool)
        s3.put(
            key_for("2026-10-07"),
            csv_of(row("2026-10-07", "user-a", "KIRO_CLI", "2", overage="50")),
        )

        ingest(s3, registry, sink)

        assert pool.credits_used(seeded.subscription_id) == Decimal("2")

    def test_another_users_rows_are_not_counted(self, pool, registry, s3, sink) -> None:
        seeded = a_mapped_subscription(pool)
        s3.put(
            key_for("2026-10-07"),
            csv_of(
                row("2026-10-07", "user-a", "KIRO_CLI", "2"),
                row("2026-10-07", "user-b", "KIRO_CLI", "90"),
            ),
        )

        ingest(s3, registry, sink)

        assert pool.credits_used(seeded.subscription_id) == Decimal("2")

    def test_an_unpadded_month_folder_is_found(self, pool, registry, s3, sink) -> None:
        # The docs show `year/month/day` without saying whether the month is padded.
        seeded = a_mapped_subscription(pool)
        s3.put(
            key_for("2026-10-08", folder_month="10").replace("/10/08/", "/10/8/"),
            csv_of(row("2026-10-08", "user-a", "KIRO_CLI", "3")),
        )

        ingest(s3, registry, sink)

        assert pool.credits_used(seeded.subscription_id) == Decimal("3")

    def test_the_legacy_report_without_credits_is_not_read(
        self, pool, registry, s3, sink
    ) -> None:
        a_mapped_subscription(pool)
        s3.put(
            f"{PREFIX}2026/10/09/00/{ACCOUNT}_by_user_analytic_20261009T020000Z.csv",
            b"UserId,Chat_MessagesSent\nuser-a,5\n",
        )

        assert ingest(s3, registry, sink).files_read == 0


class TestTheMergeWithReadings:
    def test_a_fresher_usage_figure_is_not_regressed(self, pool, registry, s3, sink) -> None:
        """A `/usage` reading after the report's last day is already higher."""
        seeded = a_mapped_subscription(pool, used="25.40")
        s3.put(key_for("2026-10-10"), csv_of(row("2026-10-10", "user-a", "KIRO_CLI", "20")))

        result = ingest(s3, registry, sink)

        assert pool.credits_used(seeded.subscription_id) == Decimal("25.40")
        assert result.unchanged == 1
        assert result.subscriptions_raised == 0

    def test_usage_no_reading_saw_raises_the_figure(self, pool, registry, s3, sink) -> None:
        """The owner's IDE on days the fleet did not run."""
        seeded = a_mapped_subscription(pool, used="5")
        s3.put(
            key_for("2026-10-11", "KIRO_IDE"),
            csv_of(row("2026-10-11", "user-a", "KIRO_IDE", "300")),
        )

        ingest(s3, registry, sink)

        assert pool.credits_used(seeded.subscription_id) == Decimal("300")


class TestWhatIsSkipped:
    def test_unmapped_and_windowless_subscriptions_are_skipped(
        self, pool, registry, s3, sink
    ) -> None:
        unmapped = pool.seed_subscription(credits_used="1", window_resets_raw=WINDOW_END.isoformat())
        windowless = pool.seed_subscription(
            credits_used="1", with_window=False, report_user_id="user-c"
        )
        s3.put(
            key_for("2026-10-12"),
            csv_of(row("2026-10-12", "user-c", "KIRO_CLI", "99")),
        )

        result = ingest(s3, registry, sink)

        assert result.skipped_no_user_id == 1
        assert result.skipped_no_window == 1
        assert pool.credits_used(unmapped.subscription_id) == Decimal("1")
        assert pool.credits_used(windowless.subscription_id) == Decimal("1")

    def test_an_unreadable_row_is_counted_and_the_rest_used(
        self, pool, registry, s3, sink
    ) -> None:
        seeded = a_mapped_subscription(pool)
        s3.put(
            key_for("2026-10-13"),
            csv_of(
                row("13 October", "user-a", "KIRO_CLI", "40"),
                row("2026-10-13", "user-a", "KIRO_CLI", "not-a-number"),
                row("10-13-2026", "user-a", "KIRO_IDE", "1.5"),
            ),
        )

        result = ingest(s3, registry, sink)

        assert result.rows_skipped == 2
        assert pool.credits_used(seeded.subscription_id) == Decimal("1.5")

    def test_nothing_to_do_still_reports_files_read(self, pool, registry, s3, sink) -> None:
        pool.seed_subscription()

        result = ingest(s3, registry, sink)

        assert result.files_read == 0
        records = [json.loads(line) for line in sink.getvalue().splitlines()]
        assert [r["ReportFilesRead"] for r in records if "ReportFilesRead" in r] == [0]


class TestMetrics:
    def test_files_read_and_the_figure_per_subscription_are_published(
        self, pool, registry, s3, sink
    ) -> None:
        seeded = a_mapped_subscription(pool)
        s3.put(key_for("2026-10-14"), csv_of(row("2026-10-14", "user-a", "KIRO_CLI", "7")))

        ingest(s3, registry, sink)

        records = [json.loads(line) for line in sink.getvalue().splitlines()]
        files = [r for r in records if "ReportFilesRead" in r]
        used = [r for r in records if "SubscriptionCreditsUsed" in r]
        assert files[0]["ReportFilesRead"] == 1
        assert used == [
            {
                **used[0],
                "SubscriptionCreditsUsed": 7.0,
                "SubscriptionId": seeded.subscription_id,
                "Source": "Report",
            }
        ]

    def test_listing_is_by_the_window_s_year(self, pool, registry, s3, sink) -> None:
        a_mapped_subscription(pool)

        ingest(s3, registry, sink)

        assert s3.listed_prefixes == [f"{PREFIX}2026/"]
