import os
import unittest
from unittest.mock import patch

from ev_weather.app import get_weather


class WeatherTests(unittest.TestCase):
    def test_manual_temperature_is_used(self):
        self.assertEqual(get_weather("Madrid", fixed_temp=-2.5), -2.5)

    def test_no_key_uses_explicit_simulation(self):
        with patch.dict(os.environ, {"OPENWEATHER_API_KEY": ""}), patch(
            "ev_weather.app.random.uniform", return_value=12.0
        ):
            self.assertEqual(get_weather("Madrid"), 12.0)

    def test_provider_error_is_not_reported_as_real_weather(self):
        class Response:
            status_code = 401

        with patch.dict(os.environ, {"OPENWEATHER_API_KEY": "invalid-placeholder"}), patch(
            "ev_weather.app.requests.get", return_value=Response()
        ):
            self.assertIsNone(get_weather("Madrid"))


if __name__ == "__main__":
    unittest.main()
