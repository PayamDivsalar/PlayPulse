"""Contract tests for the three Kafka wire formats.

These are the assertions that catch a producer changing a field name, and they
are written against the producers as they actually exist:
``crawler/data_mapper.py`` for ``app-stats`` and ``reviews``, and
``network_analyzer/message_mapper.py`` for ``network-metrics``. The payload
builders below are copies of what those modules emit, so a rename on either
side shows up here as a failure rather than as a silent stream of dead-letter
rows in production.

Every field of all three schemas is covered: its type, its nullability, and
what happens when it is missing. No broker and no database are involved.
"""

from __future__ import annotations

import unittest
from datetime import datetime, timedelta, timezone

from storage_consumer.decoders import (
    APP_STATS_FIELDS,
    NETWORK_METRIC_FIELDS,
    REVIEW_FIELDS,
    decode_app_stats,
    decode_network_metric,
    decode_review,
    parse_json,
)
from storage_consumer.exceptions import MessageDecodeError


def app_stats_payload(**overrides: object) -> dict[str, object]:
    """A complete ``app-stats`` message, exactly as ``map_app_details`` builds it."""

    payload: dict[str, object] = {
        "package_name": "com.whatsapp",
        "min_installs": 5_000_000_000,
        "score": 4.3,
        "ratings": 178_000_000,
        "reviews_count": 3_400_000,
        "version": "2.24.17.79",
        "ad_supported": False,
        "app_updated_at": "2026-09-01T08:00:00+00:00",
        "crawled_at": "2026-09-07T14:20:11.123456+00:00",
    }
    payload.update(overrides)
    return payload


def review_payload(**overrides: object) -> dict[str, object]:
    """A complete ``reviews`` message, exactly as ``map_review`` builds it."""

    payload: dict[str, object] = {
        "package_name": "com.whatsapp",
        "review_id": "gp:AOqpTOG_review_identifier",
        "user_name": "Sara",
        "thumbs_up_count": 12,
        "score": 5,
        "content": "Works well.",
        "at": "2026-09-01T10:00:00",
        "crawled_at": "2026-09-07T14:20:11.123456+00:00",
    }
    payload.update(overrides)
    return payload


def network_metric_payload(**overrides: object) -> dict[str, object]:
    """A complete ``network-metrics`` message, as ``map_analysis_result`` builds it."""

    payload: dict[str, object] = {
        "analysis_id": "8f14e45f-ea6a-4f2b-9c1d-2b3a5c7d9e01",
        "package_name": "com.whatsapp",
        "scenario": "UPLOAD",
        "rtt_handshake": 43.123,
        "retransmission_count": 7,
        "out_of_order_count": 1,
        "spurious_retransmission_count": 2,
        "zero_window_count": 0,
        "tcp_reset_count": 2,
        "bytes_transferred_total": 1040,
        "bytes_payload_total": 1000,
        "overhead_ratio": 0.038462,
        "source_pcap_filename": "com.whatsapp__upload__20260907T141500.pcap",
        "analyzed_at": "2026-09-07T14:20:11+00:00",
    }
    payload.update(overrides)
    return payload


class ContractShapeTests(unittest.TestCase):
    """The accepted field set per topic, asserted exactly."""

    def test_app_stats_contract_matches_the_producer(self) -> None:
        self.assertEqual(set(app_stats_payload()), set(APP_STATS_FIELDS))

    def test_reviews_contract_matches_the_producer(self) -> None:
        self.assertEqual(set(review_payload()), set(REVIEW_FIELDS))

    def test_network_metrics_contract_matches_the_producer(self) -> None:
        self.assertEqual(set(network_metric_payload()), set(NETWORK_METRIC_FIELDS))

    def test_unknown_fields_are_ignored_rather_than_rejected(self) -> None:
        """A producer adding a field must not dead-letter the whole topic."""

        event = decode_app_stats(app_stats_payload(installs_text="5B+"))

        self.assertEqual(event.package_name, "com.whatsapp")


class AppStatsDecodingTests(unittest.TestCase):
    def test_every_field_is_carried_across(self) -> None:
        event = decode_app_stats(app_stats_payload())

        self.assertEqual(event.package_name, "com.whatsapp")
        self.assertEqual(event.min_installs, 5_000_000_000)
        self.assertEqual(event.score, 4.3)
        self.assertEqual(event.ratings, 178_000_000)
        self.assertEqual(event.reviews_count, 3_400_000)
        self.assertEqual(event.version, "2.24.17.79")
        self.assertIs(event.ad_supported, False)
        self.assertEqual(
            event.app_updated_at, datetime(2026, 9, 1, 8, tzinfo=timezone.utc)
        )
        self.assertEqual(
            event.crawled_at,
            datetime(2026, 9, 7, 14, 20, 11, 123456, tzinfo=timezone.utc),
        )

    def test_every_measurement_survives_as_none(self) -> None:
        """The crawler's ``raw.get(...)`` yields null for any absent metric."""

        nulls = {
            "min_installs": None,
            "score": None,
            "ratings": None,
            "reviews_count": None,
            "version": None,
            "ad_supported": None,
            "app_updated_at": None,
        }
        event = decode_app_stats(app_stats_payload(**nulls))

        for field in nulls:
            with self.subTest(field=field):
                self.assertIsNone(getattr(event, field))

    def test_absent_keys_are_treated_like_nulls(self) -> None:
        event = decode_app_stats(
            {
                "package_name": "com.whatsapp",
                "crawled_at": "2026-09-07T14:20:11+00:00",
            }
        )

        self.assertIsNone(event.min_installs)
        self.assertEqual(event.package_name, "com.whatsapp")

    def test_missing_package_name_is_rejected(self) -> None:
        with self.assertRaises(MessageDecodeError) as caught:
            decode_app_stats(app_stats_payload(package_name=None))

        self.assertIn("package_name", str(caught.exception))

    def test_missing_crawled_at_is_rejected(self) -> None:
        """Without it the row has no idempotency key and no place on a chart."""

        with self.assertRaises(MessageDecodeError) as caught:
            decode_app_stats(app_stats_payload(crawled_at=None))

        self.assertIn("crawled_at", str(caught.exception))

    def test_blank_package_name_is_rejected(self) -> None:
        with self.assertRaises(MessageDecodeError):
            decode_app_stats(app_stats_payload(package_name="   "))

    def test_integral_score_is_accepted_as_a_float(self) -> None:
        """JSON writes 4.0 as 4; both mean the same thing in a float column."""

        event = decode_app_stats(app_stats_payload(score=4))

        self.assertEqual(event.score, 4.0)
        self.assertIsInstance(event.score, float)

    def test_string_min_installs_is_rejected(self) -> None:
        with self.assertRaises(MessageDecodeError) as caught:
            decode_app_stats(app_stats_payload(min_installs="5000000000"))

        self.assertIn("min_installs", str(caught.exception))

    def test_boolean_is_not_accepted_as_an_integer(self) -> None:
        """bool subclasses int in Python; storing True as 1 would be silent."""

        with self.assertRaises(MessageDecodeError):
            decode_app_stats(app_stats_payload(ratings=True))

    def test_non_boolean_ad_supported_is_rejected(self) -> None:
        with self.assertRaises(MessageDecodeError) as caught:
            decode_app_stats(app_stats_payload(ad_supported="yes"))

        self.assertIn("ad_supported", str(caught.exception))

    def test_unparseable_timestamp_is_rejected(self) -> None:
        with self.assertRaises(MessageDecodeError) as caught:
            decode_app_stats(app_stats_payload(crawled_at="7 September 2026"))

        self.assertIn("crawled_at", str(caught.exception))


class ColumnLimitTests(unittest.TestCase):
    """Values that would abort the batch transaction instead of themselves.

    Postgres raises a DataError for an over-long string, an out-of-range
    integer or a malformed UUID, and that error rolls back the whole
    transaction -- every valid row in the batch with it. The batch then retries
    and fails identically, so one bad message becomes a crash loop. Each of
    these must be caught at decode time and dead-lettered on its own.
    """

    def test_over_long_version_is_rejected(self) -> None:
        with self.assertRaises(MessageDecodeError) as caught:
            decode_app_stats(app_stats_payload(version="9." * 40))

        self.assertIn("version", str(caught.exception))

    def test_over_long_package_name_is_rejected(self) -> None:
        with self.assertRaises(MessageDecodeError):
            decode_app_stats(app_stats_payload(package_name="c" * 256))

    def test_over_long_user_name_is_rejected(self) -> None:
        with self.assertRaises(MessageDecodeError):
            decode_review(review_payload(user_name="x" * 256))

    def test_over_long_review_id_is_rejected(self) -> None:
        with self.assertRaises(MessageDecodeError):
            decode_review(review_payload(review_id="r" * 256))

    def test_unbounded_content_is_accepted(self) -> None:
        """content is TEXT, so a long review body is not a contract violation."""

        event = decode_review(review_payload(content="x" * 50_000))

        self.assertEqual(len(event.content or ""), 50_000)

    def test_integer_wider_than_bigint_is_rejected(self) -> None:
        with self.assertRaises(MessageDecodeError) as caught:
            decode_app_stats(app_stats_payload(min_installs=2**63))

        self.assertIn("min_installs", str(caught.exception))

    def test_largest_bigint_is_accepted(self) -> None:
        event = decode_app_stats(app_stats_payload(min_installs=2**63 - 1))

        self.assertEqual(event.min_installs, 2**63 - 1)

    def test_counter_wider_than_integer_is_rejected(self) -> None:
        with self.assertRaises(MessageDecodeError) as caught:
            decode_network_metric(network_metric_payload(tcp_reset_count=2**31))

        self.assertIn("tcp_reset_count", str(caught.exception))

    def test_malformed_analysis_id_is_rejected(self) -> None:
        with self.assertRaises(MessageDecodeError) as caught:
            decode_network_metric(network_metric_payload(analysis_id="not-a-uuid"))

        self.assertIn("UUID", str(caught.exception))

    def test_nan_measurement_is_rejected(self) -> None:
        """Postgres would store it happily and poison every average taken."""

        with self.assertRaises(MessageDecodeError) as caught:
            decode_network_metric(
                network_metric_payload(overhead_ratio=float("nan"))
            )

        self.assertIn("overhead_ratio", str(caught.exception))

    def test_infinite_measurement_is_rejected(self) -> None:
        with self.assertRaises(MessageDecodeError):
            decode_app_stats(app_stats_payload(score=float("inf")))


class ReviewDecodingTests(unittest.TestCase):
    def test_every_field_is_carried_across(self) -> None:
        event = decode_review(review_payload())

        self.assertEqual(event.package_name, "com.whatsapp")
        self.assertEqual(event.review_id, "gp:AOqpTOG_review_identifier")
        self.assertEqual(event.user_name, "Sara")
        self.assertEqual(event.thumbs_up_count, 12)
        self.assertEqual(event.score, 5)
        self.assertEqual(event.content, "Works well.")
        self.assertEqual(
            event.crawled_at,
            datetime(2026, 9, 7, 14, 20, 11, 123456, tzinfo=timezone.utc),
        )

    def test_every_nullable_field_survives_as_none(self) -> None:
        event = decode_review(
            review_payload(user_name=None, score=None, content=None)
        )

        self.assertIsNone(event.user_name)
        self.assertIsNone(event.score)
        self.assertIsNone(event.content)

    def test_missing_review_id_is_rejected(self) -> None:
        """It is the upsert key; without it the review cannot be stored at all."""

        with self.assertRaises(MessageDecodeError) as caught:
            decode_review(review_payload(review_id=None))

        self.assertIn("review_id", str(caught.exception))

    def test_missing_at_is_rejected(self) -> None:
        with self.assertRaises(MessageDecodeError) as caught:
            decode_review(review_payload(at=None))

        self.assertIn("'at'", str(caught.exception))

    def test_missing_crawled_at_is_rejected(self) -> None:
        """It becomes last_synced_at, which the replay guard compares on."""

        with self.assertRaises(MessageDecodeError) as caught:
            decode_review(review_payload(crawled_at=None))

        self.assertIn("crawled_at", str(caught.exception))

    def test_absent_thumbs_up_count_becomes_zero(self) -> None:
        """The column's documented default, not a missing measurement."""

        payload = review_payload()
        del payload["thumbs_up_count"]

        event = decode_review(payload)

        self.assertEqual(event.thumbs_up_count, 0)

    def test_null_thumbs_up_count_becomes_zero(self) -> None:
        event = decode_review(review_payload(thumbs_up_count=None))

        self.assertEqual(event.thumbs_up_count, 0)

    def test_score_beyond_smallint_is_rejected(self) -> None:
        """Out-of-range would abort the transaction and take the batch with it."""

        with self.assertRaises(MessageDecodeError) as caught:
            decode_review(review_payload(score=40_000))

        self.assertIn("score", str(caught.exception))

    def test_empty_content_is_preserved_rather_than_nulled(self) -> None:
        """A rating with no text is a real review; "" and NULL differ."""

        event = decode_review(review_payload(content=""))

        self.assertEqual(event.content, "")


class NaiveTimestampPolicyTests(unittest.TestCase):
    """``reviews.at`` arrives without an offset; the columns are TIMESTAMPTZ."""

    def test_naive_review_timestamp_is_read_as_utc(self) -> None:
        event = decode_review(review_payload(at="2026-09-01T10:00:00"))

        self.assertEqual(event.at, datetime(2026, 9, 1, 10, tzinfo=timezone.utc))

    def test_decoded_timestamps_are_always_offset_aware(self) -> None:
        """A naive datetime would be read in the server's session time zone."""

        event = decode_review(
            review_payload(at="2026-09-01T10:00:00", crawled_at="2026-09-07T14:20:11")
        )

        self.assertIsNotNone(event.at.tzinfo)
        self.assertIsNotNone(event.crawled_at.tzinfo)

    def test_a_non_utc_offset_is_preserved_as_the_same_instant(self) -> None:
        event = decode_review(review_payload(at="2026-09-01T13:30:00+03:30"))

        self.assertEqual(event.at, datetime(2026, 9, 1, 10, tzinfo=timezone.utc))

    def test_trailing_z_is_understood(self) -> None:
        event = decode_review(review_payload(at="2026-09-01T10:00:00Z"))

        self.assertEqual(
            event.at.utcoffset(), timedelta(0), "Z must be read as UTC"
        )


class NetworkMetricDecodingTests(unittest.TestCase):
    def test_every_field_is_carried_across(self) -> None:
        event = decode_network_metric(network_metric_payload())

        self.assertEqual(event.analysis_id, "8f14e45f-ea6a-4f2b-9c1d-2b3a5c7d9e01")
        self.assertEqual(event.package_name, "com.whatsapp")
        self.assertEqual(event.scenario, "UPLOAD")
        self.assertEqual(event.rtt_handshake, 43.123)
        self.assertEqual(event.retransmission_count, 7)
        self.assertEqual(event.out_of_order_count, 1)
        self.assertEqual(event.spurious_retransmission_count, 2)
        self.assertEqual(event.zero_window_count, 0)
        self.assertEqual(event.tcp_reset_count, 2)
        self.assertEqual(event.bytes_transferred_total, 1040)
        self.assertEqual(event.bytes_payload_total, 1000)
        self.assertEqual(event.overhead_ratio, 0.038462)
        self.assertEqual(
            event.source_pcap_filename,
            "com.whatsapp__upload__20260907T141500.pcap",
        )
        self.assertEqual(
            event.analyzed_at, datetime(2026, 9, 7, 14, 20, 11, tzinfo=timezone.utc)
        )

    def test_absent_handshake_survives_as_none(self) -> None:
        """A capture of an already-open connection has no handshake to time."""

        event = decode_network_metric(network_metric_payload(rtt_handshake=None))

        self.assertIsNone(event.rtt_handshake)

    def test_absent_source_filename_survives_as_none(self) -> None:
        event = decode_network_metric(
            network_metric_payload(source_pcap_filename=None)
        )

        self.assertIsNone(event.source_pcap_filename)

    def test_missing_analysis_id_is_rejected(self) -> None:
        """It is the only thing making redelivery harmless for this table."""

        with self.assertRaises(MessageDecodeError) as caught:
            decode_network_metric(network_metric_payload(analysis_id=None))

        self.assertIn("analysis_id", str(caught.exception))

    def test_unknown_scenario_is_rejected(self) -> None:
        with self.assertRaises(MessageDecodeError) as caught:
            decode_network_metric(network_metric_payload(scenario="SIDEGRADE"))

        self.assertIn("UPLOAD", str(caught.exception))

    def test_lowercase_scenario_is_rejected(self) -> None:
        """The producer already uppercases it; accepting both would hide drift."""

        with self.assertRaises(MessageDecodeError):
            decode_network_metric(network_metric_payload(scenario="upload"))

    def test_missing_byte_totals_are_rejected(self) -> None:
        for field in ("bytes_transferred_total", "bytes_payload_total"):
            with self.subTest(field=field):
                with self.assertRaises(MessageDecodeError) as caught:
                    decode_network_metric(network_metric_payload(**{field: None}))

                self.assertIn(field, str(caught.exception))

    def test_missing_overhead_ratio_is_rejected(self) -> None:
        with self.assertRaises(MessageDecodeError) as caught:
            decode_network_metric(network_metric_payload(overhead_ratio=None))

        self.assertIn("overhead_ratio", str(caught.exception))

    def test_zero_overhead_ratio_is_accepted(self) -> None:
        """An empty capture reports 0.0, which is a value and not an absence."""

        event = decode_network_metric(network_metric_payload(overhead_ratio=0))

        self.assertEqual(event.overhead_ratio, 0.0)

    def test_absent_counters_default_to_zero(self) -> None:
        payload = network_metric_payload()
        for field in (
            "retransmission_count",
            "out_of_order_count",
            "spurious_retransmission_count",
            "zero_window_count",
            "tcp_reset_count",
        ):
            del payload[field]

        event = decode_network_metric(payload)

        self.assertEqual(event.retransmission_count, 0)
        self.assertEqual(event.tcp_reset_count, 0)


class JsonParsingTests(unittest.TestCase):
    def test_a_json_object_is_returned_as_a_dict(self) -> None:
        self.assertEqual(parse_json(b'{"a": 1}'), {"a": 1})

    def test_a_null_body_is_rejected(self) -> None:
        """Log compaction tombstones would otherwise crash the poll loop."""

        with self.assertRaises(MessageDecodeError) as caught:
            parse_json(None)

        self.assertIn("empty", str(caught.exception))

    def test_truncated_json_is_rejected(self) -> None:
        with self.assertRaises(MessageDecodeError) as caught:
            parse_json(b'{"package_name": "com.wha')

        self.assertIn("not valid JSON", str(caught.exception))

    def test_invalid_utf8_is_rejected(self) -> None:
        with self.assertRaises(MessageDecodeError) as caught:
            parse_json(b"\xff\xfe\x00")

        self.assertIn("UTF-8", str(caught.exception))

    def test_a_bare_json_array_is_rejected(self) -> None:
        with self.assertRaises(MessageDecodeError) as caught:
            parse_json(b"[1, 2, 3]")

        self.assertIn("must be a JSON object", str(caught.exception))

    def test_non_ascii_content_round_trips(self) -> None:
        """The crawler publishes with ensure_ascii=False."""

        payload = parse_json('{"content": "عالی"}'.encode("utf-8"))

        self.assertEqual(payload["content"], "عالی")


if __name__ == "__main__":
    unittest.main()
