#!python
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import requests

# ============================================================
# Constants / TZ
# ============================================================
BASE_URL = "https://intervals.icu"
DEFAULT_FOLDER_NAME = "Runna"
RUNNA_TAG = "runna"

AUS_TZ = dt.timezone(dt.timedelta(hours=10))
LOG_LEVELS = {"DEBUG": 10, "INFO": 20, "WARN": 30, "ERROR": 40}


# ============================================================
# Logging
# ============================================================
def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def now_aus_date() -> dt.date:
    return dt.datetime.now(AUS_TZ).date()


def normalize_log_level(level: str) -> str:
    lvl = (level or "INFO").strip().upper()
    return lvl if lvl in LOG_LEVELS else "INFO"


def is_debug(level: str) -> bool:
    return LOG_LEVELS[normalize_log_level(level)] <= LOG_LEVELS["DEBUG"]


def log(level: str, msg: str, **fields: Any) -> None:
    payload = {"ts": now_utc().isoformat(), "level": level, "msg": msg, **fields}
    print(json.dumps(payload, ensure_ascii=False), file=sys.stderr)


# ============================================================
# Auth
# ============================================================
def basic_auth(api_key: str) -> requests.auth.HTTPBasicAuth:
    return requests.auth.HTTPBasicAuth("API_KEY", api_key)


# ============================================================
# ICS parsing
# ============================================================
@dataclass
class IcsEvent:
    uid: str
    summary: str
    description: str
    dtstart_date: dt.date


def unfold_ics_lines(text: str) -> List[str]:
    out: List[str] = []
    for ln in text.splitlines():
        if out and ln.startswith((" ", "\t")):
            out[-1] += ln[1:]
        else:
            out.append(ln)
    return out


def parse_ics_events(text: str) -> List[IcsEvent]:
    events: List[IcsEvent] = []
    cur: Dict[str, str] = {}
    in_event = False

    for line in unfold_ics_lines(text):
        if line == "BEGIN:VEVENT":
            cur = {}
            in_event = True
            continue
        if line == "END:VEVENT":
            if in_event and "UID" in cur and "DTSTART" in cur:
                d = cur["DTSTART"][:8]
                events.append(
                    IcsEvent(
                        uid=cur["UID"],
                        summary=cur.get("SUMMARY", "").strip(),
                        description=cur.get("DESCRIPTION", "").replace("\\n", "\n"),
                        dtstart_date=dt.date(int(d[:4]), int(d[4:6]), int(d[6:8])),
                    )
                )
            in_event = False
            continue
        if in_event and ":" in line:
            k, v = line.split(":", 1)
            cur[k.split(";", 1)[0]] = v
    return events


# ============================================================
# Translation helpers
# ============================================================
PACE_RE = re.compile(r"(\d+:\d{2})/km")
KM_RE = re.compile(r"(\d+(?:\.\d+)?)\s*km", re.I)
RANGE_RE = re.compile(r"(\d+:\d{2})-(\d+:\d{2})/km")
REPS_RE = re.compile(r"(\d+)\s*reps of", re.I)
DASH_RE = re.compile(r"^-{3,}$")


def clean_line(s: str) -> str:
    return s.strip().lstrip("•").strip()


def conversational_zone(name: str, context: str) -> str:
    if context in ("warmup", "cooldown"):
        return "Z1-Z3 Pace"
    if "long run" in name.lower():
        return "Z1-Z2 Pace"
    return "Z1-Z3 Pace"


# ============================================================
# Translation core
# ============================================================
def translate_workout_to_intervals_text(
    name: str, description: str
) -> str:
    lines = [clean_line(l) for l in description.splitlines() if clean_line(l)]

    hills = any(
        k in description.lower()
        for k in ("hill", "uphill", "downhill", "jog back down")
    )

    if not lines:
        return "Main Set\n- 60m Z1-Z2 Pace"

    header = lines[0]
    steps = lines[1:]

    # -------- single-line fallback --------
    if not steps:
        m = KM_RE.search(header)
        if m:
            km = m.group(1)
            if "easy" in header.lower() or "conversational" in header.lower():
                return f"Main Set\n- {km}km Z2-Z3 Pace"
        return "Main Set\n- 60m Z1-Z2 Pace"

    groups: List[str] = []
    current: List[str] = []
    title = "Main Set"

    def flush():
        nonlocal current
        if current:
            groups.append(title)
            groups.extend(current)
            current = []

    i = 0
    while i < len(steps):
        ln = steps[i]

        if DASH_RE.match(ln):
            i += 1
            continue

        mrep = REPS_RE.search(ln)
        if mrep:
            reps = mrep.group(1)
            flush()
            title = f"Main Set {reps}x"
            i += 1
            first = True
            while i < len(steps) and not DASH_RE.match(steps[i]):
                part = steps[i]
                km = KM_RE.search(part)
                rng = RANGE_RE.search(part)
                pace = PACE_RE.search(part)

                if km and rng:
                    prefix = "run uphill " if hills and first else ""
                    current.append(f"- {prefix}{km.group(1)}km {rng.group(1)}-{rng.group(2)}/km Pace")
                elif km and pace:
                    prefix = "run uphill " if hills and first else ""
                    current.append(f"- {prefix}{km.group(1)}km {pace.group(1)}/km Pace")
                first = False
                i += 1
            flush()
            title = "Main Set"
            continue

        km = KM_RE.search(ln)
        pace = PACE_RE.search(ln)
        if km and pace:
            current.append(f"- {km.group(1)}km {pace.group(1)}/km Pace")
        elif km and "conversational" in ln.lower():
            current.append(f"- {km.group(1)}km {conversational_zone(name,'main')}")

        i += 1

    flush()
    return "\n".join(groups)


# ============================================================
# Main
# ============================================================
def main() -> None:
    api_key = os.environ["INTERVALS_API_KEY"]
    runna_ics = os.environ["RUNNA_ICS_URL"]

    r = requests.get(runna_ics)
    r.raise_for_status()

    events = parse_ics_events(r.text)

    for ev in events:
        text = translate_workout_to_intervals_text(ev.summary, ev.description)
        print(f"\n{ev.dtstart_date} — {ev.summary}\n{text}")


if __name__ == "__main__":
    main()