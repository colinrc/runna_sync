import pathlib
from runna_sync import parse_ics_events

FIXTURES = pathlib.Path(__file__).parent / "fixtures"

def load(name: str) -> str:
    return (FIXTURES / name).read_text()

def test_parse_single_event():
    events = parse_ics_events(load("simple_single_event.ics"))
    assert len(events) == 1
    ev = events[0]
    assert ev.uid == "test-1"
    assert ev.dtstart_date.isoformat() == "2026-01-21"

def test_parse_multiple_fixtures():
    events = parse_ics_events(load("rolling_400s_with_separators.ics"))
    assert len(events) == 1
    assert events[0].summary == "Rolling 400s"