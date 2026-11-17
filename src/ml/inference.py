import os
import logging
from typing import Tuple, Optional

logger = logging.getLogger("cataclysm.ml")

MODELS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "models"
)
TEXT_MODEL_PATH = os.path.join(MODELS_DIR, "disaster_classifier.joblib")
SEV_MODEL_PATH = os.path.join(MODELS_DIR, "severity_predictor.joblib")

_text_model = None
_sev_model = None
_models_loaded = False


def load_models():
    global _text_model, _sev_model, _models_loaded
    if _models_loaded:
        return
    try:
        import joblib

        if os.path.exists(TEXT_MODEL_PATH):
            _text_model = joblib.load(TEXT_MODEL_PATH)
            logger.info("Loaded text disaster classifier from disk.")
        if os.path.exists(SEV_MODEL_PATH):
            _sev_model = joblib.load(SEV_MODEL_PATH)
            logger.info("Loaded severity predictor from disk.")
        _models_loaded = True
    except Exception as e:
        logger.warning(
            f"Could not load ML models from disk: {e}. Using resilient heuristic fallback."
        )
        _models_loaded = True


def predict_disaster_type(text: str) -> Tuple[str, float]:
    """Predicts disaster category from text with fallback heuristic."""
    load_models()
    if _text_model is not None:
        try:
            pred = _text_model.predict([text])[0]
            probs = _text_model.predict_proba([text])[0]
            confidence = float(max(probs))
            return pred, confidence
        except Exception as e:
            logger.warning(f"Model prediction failed: {e}")

    # Resilient Heuristic Fallback
    t = text.lower()
    if any(w in t for w in ["flood", "water", "river", "rain", "submerged", "inundat"]):
        return "flood", 0.0
    elif any(
        w in t for w in ["quake", "tremor", "shake", "richter", "seismic", "crack"]
    ):
        return "earthquake", 0.0
    elif any(
        w in t for w in ["cyclone", "storm", "gale", "hurricane", "tornado", "surge"]
    ):
        return "cyclone", 0.0
    elif any(
        w in t for w in ["fire", "blaze", "wildfire", "smoke", "flame", "inferno"]
    ):
        return "fire", 0.0
    elif any(
        w in t
        for w in [
            "bridge",
            "collapse",
            "pipeline",
            "blackout",
            "track",
            "tunnel",
            "structural",
        ]
    ):
        return "infrastructure", 0.0
    return "unknown", 0.0


def predict_severity(
    rainfall_mm: Optional[float] = None,
    water_level_m: Optional[float] = None,
    wind_speed_kmh: Optional[float] = None,
    temperature_c: Optional[float] = None,
) -> Tuple[str, float]:
    """Predicts severity rating from physical telemetry with fallback heuristic."""
    load_models()
    if all(
        value is None
        for value in (rainfall_mm, water_level_m, wind_speed_kmh, temperature_c)
    ):
        return "unknown", 0.0
    r = rainfall_mm if rainfall_mm is not None else 0.0
    w = water_level_m if water_level_m is not None else 0.0
    wind = wind_speed_kmh if wind_speed_kmh is not None else 0.0
    temp = temperature_c if temperature_c is not None else 25.0

    if _sev_model is not None:
        try:
            features = [[r, w, wind, temp]]
            pred = _sev_model.predict(features)[0]
            probs = _sev_model.predict_proba(features)[0]
            confidence = float(max(probs))
            return pred, confidence
        except Exception as e:
            logger.warning(f"Severity model prediction failed: {e}")

    # Resilient Heuristic Fallback
    if r > 150 or w > 4.5 or wind > 110 or temp > 45:
        return "critical", 0.0
    elif r > 70 or w > 2.0 or wind > 65:
        return "severe", 0.0
    elif r > 25 or w > 0.8 or wind > 30:
        return "moderate", 0.0
    return "minor", 0.0


def model_status() -> dict:
    """Expose provenance; zero confidence means uncalibrated heuristic output."""
    load_models()
    return {
        "classifier": "trained" if _text_model is not None else "heuristic",
        "severity": "trained" if _sev_model is not None else "heuristic",
        "confidence_note": "Model probabilities are uncalibrated; heuristic confidence is zero.",
        "training_note": "Bundled training uses a small curated text corpus and synthetic telemetry.",
    }
