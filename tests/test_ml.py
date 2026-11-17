import unittest
from unittest.mock import patch
from src.ml.inference import predict_disaster_type, predict_severity


class TestMLInference(unittest.TestCase):
    def setUp(self):
        self.state = patch.multiple(
            "src.ml.inference", _models_loaded=True, _text_model=None, _sev_model=None
        )
        self.state.start()
        self.addCleanup(self.state.stop)

    def test_flood_text_prediction(self):
        text = "The river has burst its banks and rising floodwaters are submerging houses."
        pred_type, conf = predict_disaster_type(text)
        self.assertEqual(pred_type, "flood")
        self.assertEqual(conf, 0.0)

    def test_earthquake_text_prediction(self):
        text = "Strong seismic tremors and shaking reported, magnitude 6.2 on the Richter scale."
        pred_type, conf = predict_disaster_type(text)
        self.assertEqual(pred_type, "earthquake")
        self.assertEqual(conf, 0.0)

    def test_critical_severity_prediction(self):
        sev, conf = predict_severity(rainfall_mm=250.0, water_level_m=6.5)
        self.assertEqual(sev, "critical")
        self.assertEqual(conf, 0.0)

    def test_minor_severity_prediction(self):
        sev, conf = predict_severity(rainfall_mm=10.0, water_level_m=0.2)
        self.assertEqual(sev, "minor")
        self.assertEqual(conf, 0.0)


if __name__ == "__main__":
    unittest.main()
