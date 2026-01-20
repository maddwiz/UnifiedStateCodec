from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple, Optional

from drain3 import TemplateMiner
from drain3.template_miner_config import TemplateMinerConfig


@dataclass
class Drain3Mined:
    template: str          # e.g. "User <*> logged in from <*>"
    params: List[str]      # e.g. ["Bob", "10.0.0.1"]


def make_miner() -> TemplateMiner:
    cfg = TemplateMinerConfig()
    cfg.profiling_enabled = False
    # defaults are fine for now
    return TemplateMiner(config=cfg)


def _extract_params_from_template(message: str, template_mined: str) -> List[str]:
    """
    Drain3 template uses "<*>" as wildcard.
    This extracts the actual wildcard strings from the original message by
    matching the literal parts in order.

    Works best for typical logs.
    """
    parts = template_mined.split("<*>")
    if len(parts) == 1:
        return []

    params: List[str] = []
    pos = 0

    # parts look like: [prefix0, literal1, literal2, ..., suffix]
    for i in range(len(parts) - 1):
        lit_a = parts[i]
        lit_b = parts[i + 1]

        # message must contain lit_a starting at pos (best effort)
        if lit_a:
            idx_a = message.find(lit_a, pos)
            if idx_a == -1:
                # give up: no reliable extraction
                return []
            pos = idx_a + len(lit_a)

        # now find next literal lit_b
        if lit_b:
            idx_b = message.find(lit_b, pos)
            if idx_b == -1:
                # last wildcard might go to end
                params.append(message[pos:])
                pos = len(message)
            else:
                params.append(message[pos:idx_b])
                pos = idx_b
        else:
            # lit_b empty => wildcard runs to end
            params.append(message[pos:])
            pos = len(message)

    return params


def mine_message(miner: TemplateMiner, message: str) -> Drain3Mined:
    """
    Mines a single message into a Drain3 template + extracted params.
    """
    res = miner.add_log_message(message)
    tmpl = res["template_mined"]
    params = _extract_params_from_template(message, tmpl)
    return Drain3Mined(template=tmpl, params=params)


def mine_chunk_lines(chunks: List[str]) -> Tuple[List[str], List[List[str]]]:
    """
    Treat each *line* as its own log message for Drain3.
    Then rebuild per-chunk templates by joining mined line templates with "\\n".

    Returns:
      templates_per_chunk: list[str]
      params_per_chunk: list[list[str]]  (flattened params across lines)
    """
    miner = make_miner()

    chunk_templates: List[str] = []
    chunk_params: List[List[str]] = []

    for ch in chunks:
        lines = ch.splitlines(keepends=False)

        line_templates: List[str] = []
        all_params: List[str] = []

        for line in lines:
            mined = mine_message(miner, line)
            line_templates.append(mined.template)
            all_params.extend(mined.params)

        # Join templates with newlines to keep chunk structure stable
        chunk_templates.append("\n".join(line_templates))
        chunk_params.append(all_params)

    return chunk_templates, chunk_params
