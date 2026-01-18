from typing import List


def probe_truth_spine(header: str, goal: str, witnesses: List[str]) -> bool:
    """
    v0 probe:
    Super simple checks that catch obvious drift.

    Later we will add:
    - entity consistency checks
    - timeline sanity checks
    - tool-call correctness checks
    """
    if not header.strip():
        return False
    if not goal.strip():
        return False

    # For our toy logs, header and goal should start like this:
    if not header.startswith("Project:"):
        return False
    if not goal.startswith("Goal:"):
        return False

    # Witness lines must still look like witnesses
    for w in witnesses:
        if not (w.startswith("Decision:") or w.startswith("Note:")):
            return False

    return True


def confidence_score(tier: int, probe_ok: bool) -> float:
    """
    v0 confidence model:
    - tier 3 (lossless) is always higher confidence than tier 0.
    - probe must pass.
    """
    if not probe_ok:
        return 0.0

    if tier == 3:
        return 0.95
    if tier == 0:
        return 0.70

    return 0.50
