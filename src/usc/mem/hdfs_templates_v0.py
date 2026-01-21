import csv
import re
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple


@dataclass
class HDFSTemplateBank:
    """
    Minimal template bank for LogHub HDFS templates.
    Each template is compiled into a regex where [*] becomes a captured wildcard group.
    """
    templates: Dict[int, str]
    compiled: List[Tuple[int, re.Pattern, int]]  # (event_id, regex, wildcard_count)

    @classmethod
    def from_csv(cls, csv_path: str) -> "HDFSTemplateBank":
        templates: Dict[int, str] = {}
        with open(csv_path, "r", errors="replace") as f:
            reader = csv.DictReader(f)
            for row in reader:
                eid_raw = (row.get("EventId") or "").strip()
                tpl = (row.get("EventTemplate") or "").strip()
                if not eid_raw or not tpl:
                    continue
                eid = _event_id_to_int(eid_raw)
                templates[eid] = tpl

        compiled: List[Tuple[int, re.Pattern, int]] = []
        for eid, tpl in templates.items():
            rx, wc = _compile_template_regex(tpl)
            compiled.append((eid, rx, wc))

        # Optional: try more specific templates first (fewer wildcards)
        compiled.sort(key=lambda x: x[2])
        return cls(templates=templates, compiled=compiled)


@dataclass
class HDFSEvent:
    event_id: int
    params: List[str]
    raw: str


def _event_id_to_int(s: str) -> int:
    # "E1" -> 1
    s = s.strip()
    if s.startswith("E"):
        return int(s[1:])
    return int(s)


def _compile_template_regex(template: str) -> Tuple[re.Pattern, int]:
    """
    LogHub templates use [*] as wildcard.
    We compile into a regex that captures each wildcard as a group.

    Example:
      "[*]Served block[*]to[*]"
    becomes:
      r"^(.*)Served\\ block(.*)to(.*)$"
    """
    esc = re.escape(template)
    # Replace "\[\*\]" with "(.*)" capture group
    esc = esc.replace(r"\[\*\]", r"(.*)")
    pat = r"^" + esc + r"$"
    wildcard_count = template.count("[*]")
    return re.compile(pat), wildcard_count


def load_hdfs_template_bank(csv_path: str) -> HDFSTemplateBank:
    """
    Convenience wrapper (bench07 expects this name).
    """
    return HDFSTemplateBank.from_csv(csv_path)


def parse_hdfs_lines(lines: List[str], bank: HDFSTemplateBank) -> Tuple[List[Tuple[int, List[str]]], List[str]]:
    """
    Returns:
      events: list of (event_id_int, params)
      unknown_lines: list[str]
    """
    events: List[Tuple[int, List[str]]] = []
    unknown: List[str] = []

    compiled = bank.compiled

    for ln in lines:
        s = ln.rstrip("\n")
        matched = False
        for eid, rx, _wc in compiled:
            m = rx.match(s)
            if m:
                params = [g.strip() for g in m.groups()]
                events.append((eid, params))
                matched = True
                break
        if not matched:
            unknown.append(s)

    return events, unknown
