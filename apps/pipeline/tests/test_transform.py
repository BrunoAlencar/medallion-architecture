"""Unit tests for the Pipeline's rules. Plain Python: no Spark or Java needed."""
import json

from apps.pipeline import transform as t
from apps.pipeline.transform import ChangeEvent


def ev(uid="u1", op="u", after="{}", ts=1000, ord_=1, part=0, off=10):
    return ChangeEvent(user_id=uid, op=op, after=after, ts_ms=ts, ord=ord_, kafka_partition=part, kafka_offset=off)


# ---- Event Position winner ------------------------------------------------------------------
def test_highest_event_position_wins():
    events = [ev(ts=1000, off=1, after='{"v": 1}'), ev(ts=3000, off=2, after='{"v": 3}'), ev(ts=2000, off=3, after='{"v": 2}')]
    assert t.pick_winners(events)["u1"].after == '{"v": 3}'


def test_ord_then_offset_break_ties():
    a = ev(ts=1000, ord_=1, off=50, after="a")
    b = ev(ts=1000, ord_=2, off=10, after="b")
    assert t.pick_winners([a, b])["u1"].after == "b"
    c = ev(ts=1000, ord_=2, off=11, after="c")
    assert t.pick_winners([a, b, c])["u1"].after == "c"


def test_delete_is_final_within_a_batch():
    events = [ev(op="c", ts=1000), ev(op="d", ts=2000, after=None), ev(op="u", ts=3000, after='{"late": true}')]
    assert t.pick_winners(events)["u1"].op == "d"


def test_delete_in_same_batch_as_create_keeps_the_document():
    events = [ev(op="c", ts=1000, after='{"created_at": "x"}'), ev(op="d", ts=2000, after=None)]
    winner = t.pick_winners(events)["u1"]
    assert winner.op == "d" and winner.after == '{"created_at": "x"}'


def test_update_without_document_is_dropped():
    events = [ev(op="c", ts=1000, after='{"a": 1}'), ev(op="u", ts=2000, after=None)]
    assert t.pick_winners(events)["u1"].after == '{"a": 1}'


def test_snapshot_read_is_treated_like_create():
    assert t.pick_winners([ev(op="r", after='{"x": 1}')])["u1"].op == "r"


# ---- MERGE guard (replays) ------------------------------------------------------------------
def test_replayed_older_event_is_ignored():
    stored = (5000, 1, 100)
    replay = ev(ts=4000, ord_=1, off=900)  # higher Kafka offset, older Event Position
    assert not t.should_apply(stored, False, replay)


def test_newer_event_applies_and_equal_does_not():
    stored = (5000, 1, 100)
    assert t.should_apply(stored, False, ev(ts=5001, off=1))
    assert not t.should_apply(stored, False, ev(ts=5000, ord_=1, off=100))


def test_nothing_changes_a_deleted_user():
    assert not t.should_apply((5000, 1, 100), True, ev(ts=9999))
    assert not t.should_apply((5000, 1, 100), True, ev(op="d", ts=9999, after=None))


def test_delete_applies_to_live_user_even_if_older():
    assert t.should_apply((5000, 1, 100), False, ev(op="d", ts=1, after=None))


# ---- flattening and New Attributes -----------------------------------------------------------
def test_flatten_nested_and_extended_json():
    after = json.dumps(
        {
            "_id": {"$oid": "65f0c0ffee0000000000abcd"},
            "name": "Ada",
            "created_at": {"$date": 1700000000000},
            "updated_at": {"$date": "2024-01-02T03:04:05.000Z"},
            "address": {"city": "Recife", "geo": {"lat": 1.5}},
            "tags": ["a", "b"],
            "vip": True,
            "score": {"$numberLong": "42"},
            "empty": {},
            "nothing": None,
        }
    )
    flat = t.flatten_document(after)
    assert flat == {
        "name": "Ada",
        "created_at": "2023-11-14T22:13:20.000Z",
        "updated_at": "2024-01-02T03:04:05.000Z",
        "address_city": "Recife",
        "address_geo_lat": "1.5",
        "tags": '["a", "b"]',
        "vip": "true",
        "score": "42",
        "nothing": None,
    }


def test_canonical_date_form():
    flat = t.flatten_document('{"created_at": {"$date": {"$numberLong": "0"}}}')
    assert flat == {"created_at": "1970-01-01T00:00:00.000Z"}


def test_new_attribute_appears_without_code_change():
    flat = t.flatten_document('{"_id": "x", "phone": "+55 81 0000", "preferences": {"newsletter": false}}')
    assert flat["phone"] == "+55 81 0000"
    assert flat["preferences_newsletter"] == "false"


def test_document_field_clashing_with_system_column_is_renamed():
    flat = t.flatten_document('{"is_deleted": "maybe", "Run ID": 3}')
    assert flat == {"attr_is_deleted": "maybe", "attr_run_id": "3"}


def test_flatten_none():
    assert t.flatten_document(None) is None


def test_sanitize_column():
    assert t.sanitize_column("Phone Number") == "phone_number"
    assert t.sanitize_column("2fa") == "c_2fa"
    assert t.sanitize_column("$$") == "unnamed"


# ---- user id ---------------------------------------------------------------------------------
def test_normalize_user_id_forms():
    assert t.normalize_user_id('{"$oid": "65f0c0ffee0000000000abcd"}') == "65f0c0ffee0000000000abcd"
    assert t.normalize_user_id('"user-1"') == "user-1"
    assert t.normalize_user_id('{"id": "{\\"$oid\\": \\"abc\\"}"}') == "abc"
    assert t.normalize_user_id("plain") == "plain"
    assert t.normalize_user_id(None) is None
    assert t.normalize_user_id("   ") is None


# ---- High-Water Mark and file selection ------------------------------------------------------
def p(part, start):
    return f"s3a://bronze/topics/cdc.app.users/dt=2026-11-05/hr=10/cdc.app.users-{part:010d}-{start:020d}.parquet"


def test_parse_bronze_file():
    assert t.parse_bronze_file(p(2, 1234)) == (2, 1234)
    assert t.parse_bronze_file("s3a://bronze/_SUCCESS") is None


def test_first_run_reads_everything():
    files = [p(0, 0), p(0, 10), p(1, 0)]
    assert sorted(t.select_bronze_files(files, {})) == sorted(files)


def test_skips_fully_consumed_files():
    files = [p(0, 0), p(0, 10), p(0, 20)]
    assert t.select_bronze_files(files, {0: 19}) == [p(0, 20)]


def test_keeps_file_that_may_straddle_the_mark():
    # sink restarted and wrote a file starting at 15 that can run past the mark 19
    files = [p(0, 0), p(0, 15)]
    assert t.select_bronze_files(files, {0: 19}) == [p(0, 15)]


def test_nothing_new():
    assert t.select_bronze_files([p(0, 0), p(0, 10)], {0: 19}) == [p(0, 10)]


def test_partition_without_mark_is_read_fully():
    files = [p(0, 0), p(1, 0), p(1, 5)]
    assert t.select_bronze_files(files, {0: 100}) == [p(0, 0), p(1, 0), p(1, 5)]


def test_mark_only_moves_forward():
    assert t.advance_high_water_mark({0: 10, 1: 5}, {0: 7, 2: 3}) == {0: 10, 1: 5, 2: 3}


# ---- checks ----------------------------------------------------------------------------------
def test_status_rule():
    assert t.status_is_accepted("active", False)
    assert not t.status_is_accepted("banned", False)
    assert not t.status_is_accepted(None, False)
    assert t.status_is_accepted(None, True)
