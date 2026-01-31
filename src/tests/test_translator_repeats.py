from runna_sync import translate_workout_to_intervals_text

def test_repeat_with_separators_keeps_only_inner_steps():
    desc = """
Rolling 400s

Repeat the following 6x:
----------
400m at 4:55/km
400m at 5:40/km
----------
"""
    out, partial = translate_workout_to_intervals_text("Rolling 400s", desc)

    assert partial is False
    assert "Main Set 6x" in out
    assert out.count("400m") == 2
    assert "walking rest" not in out.lower()

def test_repeat_without_separators():
    desc = """
Hills • 8km

11 reps of:
60s running hard uphill, 30s walking rest. Easy jog back down.
"""
    out, partial = translate_workout_to_intervals_text("Hills • 8km", desc)

    assert "Main Set 11x" in out
    assert "HR" in out
    assert "Jog Downhill" in out