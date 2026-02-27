"""
Unit tests for dtpr.base.event.EventRecord.

Pure awkward-Array construction — no ROOT file needed.
Run with:  pytest tests/test_event.py -v
"""

import re
import pytest
import awkward as ak
from dtpr.base.event import EventRecord, behavior


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def events():
    """Two-event mock array with one scalar, two collections (one empty)."""
    return ak.with_name(
        ak.Array([
            {
                "num":      42,
                "digis":    [{"wh": 1, "time": 100}, {"wh": 2, "time": 200}],
                "genmuons": [{"pt": 25.0, "pdgId": 13}],
            },
            {
                "num":      43,
                "digis":    [{"wh": 3, "time": 300}],
                "genmuons": [],                     # empty collection
            },
        ]),
        name="Event",
        behavior=behavior,
    )


@pytest.fixture(scope="module")
def events_no_id():
    """Event array with no id-like field — forces layout.at fallback."""
    return ak.with_name(
        ak.Array([{"wh": 1}, {"wh": 2}]),
        name="Event",
        behavior=behavior,
    )


def _strip_ansi(s: str) -> str:
    return re.sub(r"\033\[[^m]*m", "", s)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestEventRecordDispatch:

    def test_dispatch_to_event_record(self, events):
        assert isinstance(events[0], EventRecord)
        assert isinstance(events[1], EventRecord)

    def test_behavior_keys_present(self):
        """behavior dict must expose Event, Particle and *Particle so both levels dispatch."""
        assert "Event"     in behavior
        assert "Particle"  in behavior
        assert "*Particle" in behavior

    def test_fields(self, events):
        assert set(events[0].fields) == {"num", "digis", "genmuons"}


class TestFindIdField:

    def test_matches_num(self):
        assert EventRecord._find_id_field(["num", "digis"]) == "num"

    def test_matches_compound_name(self):
        assert EventRecord._find_id_field(["event_number", "digis"]) == "event_number"

    def test_no_match_returns_none(self):
        assert EventRecord._find_id_field(["wh", "time"]) is None

    def test_case_insensitive(self):
        """_ID_PATTERN uses re.IGNORECASE — uppercase variants must match."""
        assert EventRecord._find_id_field(["INDEX", "digis"])  == "INDEX"
        assert EventRecord._find_id_field(["Event_ID"])        == "Event_ID"
        assert EventRecord._find_id_field(["NUMBER"])          == "NUMBER"
        assert EventRecord._find_id_field(["Idx"])             == "Idx"
        assert EventRecord._find_id_field(["ev"])              == "ev"
        assert EventRecord._find_id_field(["evnum"])           == "evnum"

    def test_first_match_wins(self):
        """When multiple candidates exist, the first field in order is returned."""
        assert EventRecord._find_id_field(["num", "event_id"]) == "num"


class TestEventLabel:

    def test_label_uses_id_field_value(self, events):
        assert events[0]._event_label == "42"
        assert events[1]._event_label == "43"

    def test_label_fallback_to_positional_index(self, events_no_id):
        assert events_no_id[0]._event_label == "0"
        assert events_no_id[1]._event_label == "1"

    def test_label_is_cached(self, events):
        """cached_property must return the same object on repeated access."""
        ev = events[0]
        assert ev._event_label is ev._event_label


class TestRepresentation:

    def test_repr_contains_label(self, events):
        assert repr(events[0]) == "<Event 42>"
        assert repr(events[1]) == "<Event 43>"

    def test_repr_fallback_index(self, events_no_id):
        assert repr(events_no_id[1]) == "<Event 1>"

    def test_str_contains_label(self, events):
        plain = _strip_ansi(str(events[0]))
        assert "42" in plain

    def test_str_contains_all_field_names(self, events):
        plain = _strip_ansi(str(events[0]))
        for field in events[0].fields:
            assert field in plain

    def test_str_empty_collection_shown(self, events):
        """Empty genmuons must still appear as a collection line, not be silently dropped."""
        plain = _strip_ansi(str(events[1]))
        assert "genmuons" in plain
        assert "0 items"  in plain


class TestCollectionDetection:

    def test_scalar_not_detected_as_collection(self, events):
        val = events[0]["num"]
        assert not (hasattr(val, "fields") and len(val.fields) > 0)

    def test_non_empty_collection_detected(self, events):
        val = events[0]["digis"]
        assert hasattr(val, "fields") and len(val.fields) > 0

    def test_empty_collection_still_detected(self, events):
        """An empty collection still carries .fields — must not fall through to scalar path."""
        val = events[1]["genmuons"]
        assert hasattr(val, "fields") and len(val.fields) > 0
