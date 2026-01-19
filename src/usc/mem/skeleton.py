from dataclasses import dataclass
from typing import List


@dataclass
class Skeleton:
    """
    Minimal truth spine for USC memory.
    Must always produce non-empty header+goal so probes don't fail on chunks.
    """
    header: str
    goal: str


def _nonempty_lines(text: str) -> List[str]:
    lines = []
    for ln in text.splitlines():
        s = ln.strip()
        if s:
            lines.append(ln.rstrip("\n"))
    return lines


def extract_skeleton(text: str) -> Skeleton:
    """
    v0.8 skeleton extractor

    Rules:
    1) If we find explicit 'Project:' and 'Goal:' lines, use them.
    2) Otherwise: fallback to the first 2 non-empty lines of the chunk.
       This keeps chunking stable and keeps Tier3 lossless.
    """
    lines = _nonempty_lines(text)

    # Default fallback (chunk-friendly)
    fallback_header = lines[0] if len(lines) >= 1 else "Chunk:"
    fallback_goal = lines[1] if len(lines) >= 2 else fallback_header

    header = fallback_header
    goal = fallback_goal

    # Prefer explicit markers if present
    for ln in lines:
        if ln.startswith("Project:"):
            header = ln
            break

    for ln in lines:
        if ln.startswith("Goal:"):
            goal = ln
            break

    return Skeleton(header=header, goal=goal)


def render_skeleton(sk: Skeleton) -> str:
    """
    Render skeleton into text that can be removed as a prefix for residual coding.
    """
    return f"{sk.header}\n{sk.goal}\n"
