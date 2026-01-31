import pathlib
from runna_sync import translate_workout_to_intervals_text

SNAPSHOTS = pathlib.Path(__file__).parent / "snapshots"

def assert_snapshot(name: str, actual: str):
    expected = (SNAPSHOTS / name).read_text().strip()
    assert actual.strip() == expected

def test_rolling_400s_snapshot():
    desc = """
Rolling 400s

3.5km warm up

Repeat the following 6x:
----------
400m at 4:55/km
400m at 5:40/km
----------

90s walking rest

2.5km cool down
"""
    out, partial = translate_workout_to_intervals_text("Rolling 400s", desc)
    assert partial is False
    assert_snapshot("rolling_400s.txt", out)

def test_hills_snapshot():
    desc = """
Hills • 8km

11 reps of:
60s running hard uphill, 30s walking rest. Easy jog back down.
"""
    out, _ = translate_workout_to_intervals_text("Hills • 8km", desc)
    assert_snapshot("hills_repeat.txt", out)