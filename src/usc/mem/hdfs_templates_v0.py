from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Tuple


# Supports both LogHub wildcard styles:
#   - Drain / LogHub style: "<*>"
#   - Older style: "[*]"
_WILDCARD_RE = re.compile(r"(\<\*\>|\[\*\])")

# Timestamp patterns commonly seen in LogHub:
_TS_FULL = re.compile(r"\b\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2},\d{3}\b")
_TS_DATE = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")


def _normalize_template(tpl: str) -> str:
    """
    Make templates robust:
    Many miners accidentally "freeze" timestamps (e.g., 2015-07-29).
    We replace explicit timestamps/dates with a wildcard so matching is not date-locked.
    """
    s = tpl.strip()
    s = _TS_FULL.sub("<*>", s)
    s = _TS_DATE.sub("<*>", s)
    return s


def _compile_template_regex(template: str) -> Tuple[re.Pattern, int]:
    """
    Compile a template string into a regex that captures wildcards.
    Wildcards: "<*>" or "[*]" → each becomes a captured group (.*?)
    """
    t = _normalize_template(template)

    parts = _WILDCARD_RE.split(t)
    out = ["^"]
    wildcards = 0

    for p in parts:
        if not p:
            continue
        if p in ("<*>", "[*]"):
            out.append("(.*?)")
            wildcards += 1
        else:
            out.append(re.escape(p))

    out.append("$")
    rx = re.compile("".join(out))
    return rx, wildcards


@dataclass
class HDFSTemplateBank:
    """
    Template bank built from LogHub-style CSV:
      EventId,EventTemplate
    """
    compiled: List[Tuple[int, re.Pattern, int, str]]  # (event_id, regex, wildcard_count, raw_template)

    @classmethod
    def from_csv(cls, csv_path: str | Path) -> "HDFSTemplateBank":
        """
        ✅ Keep this name because your CLI calls .from_csv(...)
        """
        path = Path(csv_path)
        compiled: List[Tuple[int, re.Pattern, int, str]] = []

        with path.open("r", encoding="utf-8", errors="ignore", newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                eid_s = (row.get("EventId") or "").strip()
                tpl = (row.get("EventTemplate") or "").strip()
                if not eid_s or not tpl:
                    continue

                # EventId looks like "E000123" → int 123
                try:
                    eid = int(eid_s[1:])
                except Exception:
                    continue

                rx, wc = _compile_template_regex(tpl)
                compiled.append((eid, rx, wc, tpl))

        # Prefer more specific templates first (fewer wildcards)
        compiled.sort(key=lambda x: x[2])
        return cls(compiled=compiled)


def parse_hdfs_lines(
    lines: List[str],
    bank: HDFSTemplateBank,
) -> Tuple[List[Tuple[int, List[str]]], List[str]]:
    """
    Parse log lines into:
      events: [(event_id_int, params_list)]
      unknown_lines: [raw_line]

    Params are extracted from wildcard capture groups.
    """
    events: List[Tuple[int, List[str]]] = []
    unknown_lines: List[str] = []

    for ln in lines:
        s = ln.rstrip("\n")
        matched = False

        for eid, rx, _wc, _tpl in bank.compiled:
            m = rx.match(s)
            if m:
                params = list(m.groups()) if m.groups() else []
                events.append((eid, params))
                matched = True
                break

        if not matched:
            unknown_lines.append(s)

    return events, unknown_lines
