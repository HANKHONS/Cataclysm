"""Correct legacy zero-score provenance without changing observation/model labels."""

import json

from src.common.db import get_pool


def main() -> None:
    """Unknown severity is unavailable; zero-score keyword output is heuristic."""
    with get_pool().connection() as conn:
        unavailable = conn.execute(
            "UPDATE predictions SET provenance='unavailable', confidence=NULL "
            "WHERE model_name='severity' AND predicted_label='unknown' "
            "AND provenance='trained' AND COALESCE(confidence,0)=0"
        ).rowcount
        heuristic = conn.execute(
            "UPDATE predictions SET provenance='heuristic', confidence=NULL "
            "WHERE provenance='trained' AND confidence=0 "
            "AND predicted_label <> 'unknown'"
        ).rowcount
    print(
        json.dumps(
            {
                "legacy_unavailable_corrected": unavailable,
                "legacy_heuristic_corrected": heuristic,
            }
        )
    )


if __name__ == "__main__":
    main()
