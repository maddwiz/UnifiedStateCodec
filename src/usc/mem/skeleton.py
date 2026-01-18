from dataclasses import dataclass


@dataclass
class Skeleton:
    header: str
    goal: str


def extract_skeleton(text: str) -> Skeleton:
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
    header = lines[0] if len(lines) > 0 else ""
    goal = lines[1] if len(lines) > 1 else ""
    return Skeleton(header=header, goal=goal)


def render_skeleton(sk: Skeleton) -> str:
    return f"{sk.header}\n{sk.goal}\n"
