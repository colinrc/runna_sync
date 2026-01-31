import unittest

from runna_sync import translate_workout_to_intervals_text


class TranslatorTests(unittest.TestCase):
    def test_easy_run_single_line_with_disclaimer(self):
        name = "Easy Run • 12km • 1h10m - 1h20m"
        desc = (
            "Easy Run • 12km • 1h10m - 1h20m\n\n"
            "12km easy run at a conversational pace (no faster than 6:00/km). "
            "This is a limit, not a target - run at whatever pace feels truly easy!\n"
        )
        out, partial = translate_workout_to_intervals_text(name, desc)
        self.assertFalse(partial)
        self.assertIn("Main Set", out)
        self.assertIn("- 12km Z2-Z3 Pace", out)

    def test_noise_view_in_app_is_ignored(self):
        name = "Easy Run • 10km • 55m - 1h10m"
        desc = (
            "Easy Run • 10km • 55m - 1h10m\n\n"
            "10km easy run at a conversational pace (no faster than 5:55/km). "
            "This is a limit, not a target - run at whatever pace feels truly easy!\n\n"
            "📲 View in the Runna app: https://club.runna.com/example\n"
        )
        out, partial = translate_workout_to_intervals_text(name, desc)
        self.assertFalse(partial)
        self.assertIn("- 10km Z2-Z3 Pace", out)

    def test_pace_step_ends_with_pace(self):
        name = "Long Run • 11km"
        desc = (
            "Long Run • 11km • 55m - 1h10m\n\n"
            "3km at 5:30/km\n"
        )
        out, _ = translate_workout_to_intervals_text(name, desc)
        self.assertTrue(out.strip().endswith("Pace"))

    def test_hills_hard_uphill_to_hr(self):
        name = "Hills • 8km"
        desc = (
            "Hills • 8km • 50m - 55m\n\n"
            "11 reps of:\n"
            "• 60s running hard uphill, 30s walking rest. Easy jog back down.\n"
        )
        out, _ = translate_workout_to_intervals_text(name, desc)
        self.assertIn("HR", out)


if __name__ == "__main__":
    unittest.main(verbosity=2)