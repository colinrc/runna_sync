#!python
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import sys
import unittest
from dataclasses import dataclass
from enum import Enum, auto
from typing import Any, Dict, List, Optional, Tuple

import requests

# ============================================================
# Cloudflare Workers guarded import
# ============================================================
try:
    from workers import WorkerEntrypoint, Response  # type: ignore
    CF_AVAILABLE = True
except Exception:
    WorkerEntrypoint = object  # type: ignore
    Response = None  # type: ignore
    CF_AVAILABLE = False

# ============================================================
# Constants / TZ
# ============================================================
BASE_URL = "https://intervals.icu"
DEFAULT_FOLDER_NAME = "Runna"
RUNNA_TAG = "runna"

# Australia/Brisbane fixed offset (no DST)
AUS_TZ = dt.timezone(dt.timedelta(hours=10))

LOG_LEVELS = {"DEBUG": 10, "INFO": 20, "WARN": 30, "ERROR": 40}


# ============================================================
# Logging (structured JSON)
# ============================================================
def normalize_log_level(level: str) -> str:
    lvl = (level or "INFO").strip().upper()
    return lvl if lvl in LOG_LEVELS else "INFO"


def is_debug(level: str) -> bool:
    return LOG_LEVELS[normalize_log_level(level)] <= LOG_LEVELS["DEBUG"]


def now_utc() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


def aus_today() -> dt.date:
    return dt.datetime.now(AUS_TZ).date()


def log(level: str, msg: str, **fields: Any) -> None:
    payload = {"ts": now_utc().isoformat(), "level": level, "msg": msg, **fields}

    # Cloudflare/Wrangler marks stderr output as "[ERROR]" regardless of JSON.
    # Route INFO/DEBUG to stdout; WARN/ERROR to stderr.
    lvl = (level or "INFO").upper()
    stream = sys.stderr if lvl in ("WARN", "ERROR") else sys.stdout

    print(json.dumps(payload, ensure_ascii=False), file=stream)


# ============================================================
# Auth (Basic Auth, user="API_KEY", password=<value>)
# ============================================================
def basic_auth(api_key_value: str) -> requests.auth.HTTPBasicAuth:
    return requests.auth.HTTPBasicAuth("API_KEY", api_key_value)


# ============================================================
# ICS parsing (minimal, robust for Runna feed)
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
            out.append(ln.rstrip("\r\n"))
    return out


def ics_unescape(s: str) -> str:
    return (
        s.replace("\\n", "\n")
        .replace("\\,", ",")
        .replace("\\;", ";")
        .replace("\\:", ":")
        .replace("\\\\", "\\")
    )


def parse_dtstart_date(raw_value: str) -> Optional[dt.date]:
    v = (raw_value or "").strip()
    # DTSTART;VALUE=DATE:YYYYMMDD (or DTSTART:YYYYMMDD)
    if len(v) >= 8 and v[:8].isdigit():
        y, m, d = int(v[:4]), int(v[4:6]), int(v[6:8])
        return dt.date(y, m, d)
    return None


def parse_ics_events(text: str) -> List[IcsEvent]:
    lines = unfold_ics_lines(text)
    events: List[IcsEvent] = []
    cur: Dict[str, str] = {}
    in_event = False

    for line in lines:
        if line == "BEGIN:VEVENT":
            cur = {}
            in_event = True
            continue

        if line == "END:VEVENT":
            if in_event:
                uid = cur.get("UID", "").strip()
                summary = ics_unescape(cur.get("SUMMARY", "")).strip()
                desc = ics_unescape(cur.get("DESCRIPTION", "")).strip()
                dtstart = parse_dtstart_date(cur.get("DTSTART", ""))
                if uid and dtstart:
                    events.append(
                        IcsEvent(uid=uid, summary=summary, description=desc, dtstart_date=dtstart)
                    )
            in_event = False
            continue

        if not in_event or ":" not in line:
            continue

        k, v = line.split(":", 1)
        key = k.split(";", 1)[0].strip()
        cur[key] = v

    return events


# ============================================================
# Translator (STATE MACHINE)
# ============================================================
PACE_RE = re.compile(r"(\d+:\d{2})/km")
RANGE_RE = re.compile(r"(\d+:\d{2})-(\d+:\d{2})/km")
KM_RE = re.compile(r"(\d+(?:\.\d+)?)\s*km\b", re.IGNORECASE)
M_DIST_RE = re.compile(r"(\d+)\s*m\b", re.IGNORECASE)  # meters -> km
SEC_RE = re.compile(r"(\d+)\s*s\b", re.IGNORECASE)
MIN_RE = re.compile(r"(\d+)\s*m\b", re.IGNORECASE)

REPS_RE = re.compile(r"^\s*(\d+)\s*reps of\s*:?\s*$", re.IGNORECASE)
REPEAT_FOLLOWING_RE = re.compile(r"^\s*repeat the following\s+(\d+)x\s*:?\s*$", re.IGNORECASE)
SEP_LINE_RE = re.compile(r"^\s*-{3,}\s*$")  # ----------

FAST_BURSTS_HINT = "add 3x 15s fast bursts"

# Only true boilerplate is noise
NOISE_SUBSTRINGS = [
    "view in the runna app",
    "📲",
]


def clean_line(s: str) -> str:
    s = (s or "").strip()
    if s.startswith("•"):
        s = s[1:].strip()

    # remove pace cap parenthetical, keep the line
    s = re.sub(r"\(no faster than[^)]*\)", "", s, flags=re.IGNORECASE).strip()

    # strip Runna disclaimers BUT keep actionable content
    s = re.sub(r"\bthis is a limit\b.*$", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\bnot a target\b.*$", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\brun at whatever pace feels truly easy!?$", "", s, flags=re.IGNORECASE).strip()

    # clean trailing punctuation
    s = re.sub(r"[.\s]+$", "", s).strip()
    return s


def split_on_commas(line: str) -> List[str]:
    parts = [p.strip() for p in (line or "").split(",") if p.strip()]
    return parts if parts else [line.strip()]


def meters_to_km_str(meters: int) -> str:
    km = meters / 1000.0
    s = f"{km:.3f}".rstrip("0").rstrip(".")
    return f"{s}km"


def parse_distance(line: str) -> Optional[str]:
    km = KM_RE.search(line)
    if km:
        return f"{km.group(1)}km"
    m = M_DIST_RE.search(line)
    if m:
        return meters_to_km_str(int(m.group(1)))
    return None


def parse_duration(line: str) -> Optional[str]:
    m = SEC_RE.search(line)
    if m:
        return f"{int(m.group(1))}s"
    m = MIN_RE.search(line)
    if m:
        return f"{int(m.group(1))}m"
    return None


def is_noise(line: str) -> bool:
    ll = (line or "").lower()
    return any(x in ll for x in NOISE_SUBSTRINGS)


def is_hills_by_text(full_description: str) -> bool:
    t = (full_description or "").lower()
    return any(
        k in t
        for k in (
            "hill",
            "uphill",
            "downhill",
            "jog back down",
            "jog downhill",
            "base of hill",
        )
    )


def conversational_zone(workout_name: str, context: str) -> str:
    wn = (workout_name or "").lower()
    if context in ("warmup", "cooldown"):
        return "Z1-Z3 Pace"
    if "long run" in wn:
        return "Z1-Z2 Pace"
    return "Z1-Z3 Pace"


def single_line_easy_zone() -> str:
    return "Z2-Z3 Pace"


class TState(Enum):
    NORMAL = auto()
    REPEAT_OPEN = auto()           # repeat without separators
    REPEAT_SEP_WAIT_OPEN = auto()  # after repeat header waiting for first separator
    REPEAT_SEP_CAPTURE = auto()    # capturing ONLY between separators


@dataclass
class Group:
    title: str
    steps: List[str]


class RunnaTranslatorStateMachine:
    def __init__(self, workout_name: str, full_description: str):
        self.workout_name = workout_name or ""
        self.full_description = full_description or ""
        self.hills_mode = is_hills_by_text(full_description)

        self.groups: List[Group] = []
        self.current = Group("Main Set", [])
        self.partial = False

        self.state = TState.NORMAL
        self.repeat_group: Optional[Group] = None
        self.repeat_first_step_pending = False

        # hills-only fallback injection guard
        self.hills_fallback_added = False
        self.cooldown_steps: set[int] = set()

    def _flush_current(self) -> None:
        if self.current.steps:
            self.groups.append(self.current)
        self.current = Group("Main Set", [])

    def _start_group(self, title: str) -> None:
        self._flush_current()
        self.current = Group(title, [])

    def _start_repeat_group(self, reps: int) -> None:
        self._flush_current()
        self.repeat_group = Group(f"Main Set {reps}x", [])
        self.repeat_first_step_pending = True

    def _close_repeat_group(self) -> None:
        if self.repeat_group and self.repeat_group.steps:
            self.groups.append(self.repeat_group)
        self.repeat_group = None
        self.repeat_first_step_pending = False

    def _add_hills_fallback_if_needed(self) -> None:
        # Only include "2m Z1-Z2 Pace" fallback in context of a hill
        if self.hills_mode and not self.hills_fallback_added:
            self.current.steps.append("- 2m Z1-Z2 Pace")
            self.hills_fallback_added = True

    def _add_step(self, step: str, into_repeat: bool) -> None:
        if into_repeat and self.repeat_group is not None:
            if self.hills_mode and self.repeat_first_step_pending:
                if step.startswith("- "):
                    step = "- run uphill " + step[2:]
                else:
                    step = "run uphill " + step
                self.repeat_first_step_pending = False
            self.repeat_group.steps.append(step)
        else:
            self.current.steps.append(step)

    def _process_fragment(self, frag: str, allow_group_change: bool, into_repeat: bool) -> None:
        line = clean_line(frag)
        if not line or is_noise(line) or SEP_LINE_RE.match(line):
            return

        ll = line.lower()

        # Ignore fast bursts instruction entirely
        if FAST_BURSTS_HINT in ll:
            return

        # Warmup / cooldown group changes only allowed outside repeat capture
        if allow_group_change and "warm up" in ll:
            self._start_group("Warmup")
            dist = parse_distance(line) or "2m"
            self._add_step(f"- {dist} ramp {conversational_zone(self.workout_name, 'warmup')}", into_repeat=False)
            return

        if allow_group_change and "cool down" in ll:
            self._start_group("Cooldown")
            dist = parse_distance(line) or "2m"
            step = f"- {dist} ramp Z3-Z1 Pace"
            self._add_step(step, into_repeat=False)
            self.cooldown_steps.add(id(step))
            return

        # Walking rest -> Z1 Pace (Walk replaced by Pace)
        if "walking rest" in ll:
            dur = parse_duration(line) or "90s"
            self._add_step(f"- {dur} Z1 Pace", into_repeat=into_repeat)
            return

        # Hills rules (HR only for hills)
        if self.hills_mode:
            if "running hard uphill" in ll or ("hard" in ll and "uphill" in ll):
                dur = parse_duration(line) or "2m"
                self._add_step(f"- {dur} Z3-Z5 HR", into_repeat=into_repeat)
                return

            if (
                "easy jog back down" in ll
                or "jog back down" in ll
                or "easy jog back" in ll
                or "jog downhill" in ll
                or "downhill" in ll
            ):
                jog_dur = "120s"
                prev_steps = self.repeat_group.steps if (into_repeat and self.repeat_group) else self.current.steps
                if prev_steps:
                    prev = prev_steps[-1]
                    ms = re.search(r"\b(\d+)s\b", prev)
                    mm = re.search(r"\b(\d+)m\b", prev)
                    if ms:
                        jog_dur = f"{int(ms.group(1)) * 2}s"
                    elif mm:
                        jog_dur = f"{int(mm.group(1)) * 2}m"
                self._add_step(f"- Press lap, Jog Downhill {jog_dur} Z1-Z2 Pace", into_repeat=into_repeat)
                return

        # "easy jog" / "easy run" phrases => Z1-Z2 Pace
        if ("easy jog" in ll or "easy run" in ll) and (parse_distance(line) or parse_duration(line)):
            dist = parse_distance(line)
            dur = parse_duration(line)
            if dist:
                self._add_step(f"- {dist} Z1-Z2 Pace", into_repeat=into_repeat)
            else:
                self._add_step(f"- {dur} Z1-Z2 Pace", into_repeat=into_repeat)  # type: ignore[arg-type]
            return

        # Distance + pace parsing
        dist = parse_distance(line)
        rng = RANGE_RE.search(line)
        pace = PACE_RE.search(line)

        if dist and rng:
            target = rng.group(1)
            lo, hi = rng.group(1), rng.group(2)
            self._add_step(f"- {dist} {target}/km PACE ({lo}-{hi}/km) Pace", into_repeat=into_repeat)
            return

        if dist and pace:
            self._add_step(f"- {dist} {pace.group(1)}/km Pace", into_repeat=into_repeat)
            return

        # Conversational pace handling
        if dist and "conversational" in ll:
            self._add_step(f"- {dist} {conversational_zone(self.workout_name, 'main')}", into_repeat=into_repeat)
            return

        # Single-line easy run style
        if dist and ("easy run" in ll or (self.workout_name.lower().startswith("easy run") and "easy" in ll)):
            self._add_step(f"- {dist} {single_line_easy_zone()}", into_repeat=into_repeat)
            return

        # Distance-only
        if dist:
            self._add_step(f"- {dist} Z1-Z2 Pace", into_repeat=into_repeat)
            return

        # Duration-only
        dur = parse_duration(line)
        if dur:
            self._add_step(f"- {dur} Z1-Z2 Pace", into_repeat=into_repeat)
            return
        

        # Unknown fragment
        self.partial = True
        self._add_hills_fallback_if_needed()

    def translate(self) -> Tuple[str, bool]:
        raw_lines = [(l or "") for l in (self.full_description or "").splitlines()]
        if not raw_lines:
            return ("Main Set\n- 60m Z1-Z2 Pace", True)

        # First line is not converted into steps (title line)
        first_line = raw_lines[0]
        body_lines = raw_lines[1:]

        # If no other step lines exist, translate the title line (single-line workouts)
        if not body_lines:
            dist = parse_distance(first_line)
            ll = first_line.lower()
            if dist and ("easy" in ll or "conversational" in ll):
                return (f"Main Set\n- {dist} {single_line_easy_zone()}", False)
            if self.workout_name.lower().startswith("easy run") and dist:
                return (f"Main Set\n- {dist} {single_line_easy_zone()}", False)
            return ("Main Set\n- 60m Z1-Z2 Pace", True)

        for ln in body_lines:
            raw = ln
            ln = clean_line(ln)

            # NEW RULE: blank line ends repeat block
            if raw.strip() == "" and self.state in (
                TState.REPEAT_OPEN,
                TState.REPEAT_SEP_CAPTURE,
                TState.REPEAT_SEP_WAIT_OPEN,
            ):
                self._close_repeat_group()
                self.state = TState.NORMAL
                continue

            # ------------------------------------------------------------
            # BLANK LINE TERMINATES REPEAT BLOCK
            # ------------------------------------------------------------
            if not ln and self.state in (TState.REPEAT_OPEN, TState.REPEAT_SEP_CAPTURE):
                self._close_repeat_group()
                self.state = TState.NORMAL
                continue

            if not ln or is_noise(ln):
                continue
            # separators govern repeat capture
            if SEP_LINE_RE.match(ln):
                if self.state == TState.REPEAT_SEP_WAIT_OPEN:
                    self.state = TState.REPEAT_SEP_CAPTURE
                elif self.state == TState.REPEAT_SEP_CAPTURE:
                    self._close_repeat_group()
                    self.state = TState.NORMAL
                continue

            # repeat header detection (NORMAL only)
            if self.state == TState.NORMAL:
                m1 = REPS_RE.match(ln)
                m2 = REPEAT_FOLLOWING_RE.match(ln)
                if m1 or m2:
                    reps = int((m1 or m2).group(1))  # type: ignore
                    self._start_repeat_group(reps)
                    self.state = TState.REPEAT_SEP_WAIT_OPEN
                    continue

            # if waiting for separator but got content => repeat without separators
            if self.state == TState.REPEAT_SEP_WAIT_OPEN:
                self.state = TState.REPEAT_OPEN

            if self.state in (TState.REPEAT_OPEN, TState.REPEAT_SEP_CAPTURE):
                for frag in split_on_commas(ln):
                    self._process_fragment(frag, allow_group_change=False, into_repeat=True)
                continue

            # NORMAL
            for frag in split_on_commas(ln):
                self._process_fragment(frag, allow_group_change=True, into_repeat=False)

        # Close any open repeat at EOF
        if self.state in (TState.REPEAT_OPEN, TState.REPEAT_SEP_CAPTURE, TState.REPEAT_SEP_WAIT_OPEN):
            self._close_repeat_group()

        self._flush_current()
        self._postprocess_trailing_cooldown()

        # Format: blank line between groups, no blank line between title and steps
        out: List[str] = []
        for gi, g in enumerate(self.groups):
            if gi > 0:
                out.append("")
            out.append(g.title)
            out.extend(g.steps)

        if not out:
            return ("Main Set\n- 60m Z1-Z2 Pace", True)

        return ("\n".join(out).rstrip(), self.partial)
    
    def _postprocess_trailing_conversational_to_cooldown(self) -> None:
        """
        If the final step is conversational pace and not part of a repeat,
        move it into a dedicated Cooldown group.
        """
        if not self.groups:
            return

        last_group = self.groups[-1]

        # Only move from Main Set
        if last_group.title != "Main Set":
            return

        if not last_group.steps:
            return

        last_step = last_group.steps[-1].lower()

        if "conversational" not in last_step:
            return

        # Remove from Main Set
        step = last_group.steps.pop()

        # Drop empty Main Set if needed
        if not last_group.steps:
            self.groups.pop()

        # Append new Cooldown group
        self.groups.append(Group("Cooldown", [step]))


    def _postprocess_trailing_cooldown(self) -> None:
        if not self.groups:
            return

        last_group = self.groups[-1]
        if last_group.title != "Main Set":
            return

        if not last_group.steps:
            return

        step = last_group.steps[-1]

        if id(step) not in self.cooldown_steps:
            return

        # Move to Cooldown group
        last_group.steps.pop()
        if not last_group.steps:
            self.groups.pop()

        self.groups.append(Group("Cooldown", [step]))

def translate_workout_to_intervals_text(workout_name: str, description: str) -> Tuple[str, bool]:
    sm = RunnaTranslatorStateMachine(workout_name, description)
    return sm.translate()


# ============================================================
# Intervals API helpers
# ============================================================
def intervals_http(
    log_level: str,
    method: str,
    url: str,
    api_key: str,
    json_body: Any = None,
    timeout: int = 60,
) -> requests.Response:
    lvl = normalize_log_level(log_level)
    log("DEBUG" if is_debug(lvl) else "INFO", "intervals_http", method=method, url=url)
    r = requests.request(
        method=method,
        url=url,
        auth=basic_auth(api_key),
        json=json_body,
        timeout=timeout,
    )
    log("DEBUG" if is_debug(lvl) else "INFO", "intervals_http_response", status=r.status_code, url=url)
    r.raise_for_status()
    return r


def auth_test(api_key: str, athlete_id: str, log_level: str) -> None:
    url = f"{BASE_URL}/api/v1/athlete/{athlete_id}/folders"
    log("INFO", "auth_test_start", url=url)
    r = intervals_http(log_level, "GET", url, api_key)
    log("INFO", "auth_test_ok", status=r.status_code)


def ensure_folder(api_key: str, athlete_id: str, folder_name: str, log_level: str) -> int:
    url = f"{BASE_URL}/api/v1/athlete/{athlete_id}/folders"
    r = intervals_http(log_level, "GET", url, api_key)
    data = r.json()

    for obj in data:
        if obj.get("type") == "FOLDER" and obj.get("name") == folder_name:
            log("INFO", "folder_found", name=folder_name, folder_id=obj.get("id"))
            return int(obj["id"])

    r2 = intervals_http(log_level, "POST", url, api_key, json_body={"type": "FOLDER", "name": folder_name})
    created = r2.json()
    fid = int(created["id"])
    log("INFO", "folder_created", name=folder_name, folder_id=fid)
    return fid


def bulk_upload_events(api_key: str, events: List[Dict[str, Any]], log_level: str, dry_run: bool) -> Dict[str, Any]:
    lvl = normalize_log_level(log_level)

    if dry_run:
        log("INFO", "dry_run_upload_skipped", count=len(events))
        return {"dry_run": True, "count": len(events)}

    url = f"{BASE_URL}/api/v1/athlete/0/events/bulk?upsert=true"

    if is_debug(lvl):
        log("DEBUG", "upload_payload", payload=events)

    r = intervals_http(lvl, "POST", url, api_key, json_body=events, timeout=120)

    out: Dict[str, Any] = {"dry_run": False, "count": len(events), "status": r.status_code}
    try:
        out["json"] = r.json()
    except Exception:
        out["json"] = None
    return out


def bulk_delete_events(
    api_key: str,
    athlete_id: str,
    refs: List[Dict[str, Any]],
    log_level: str,
    dry_run: bool,
) -> Dict[str, Any]:
    lvl = normalize_log_level(log_level)

    if dry_run:
        log("INFO", "dry_run_delete_skipped", count=len(refs))
        return {"dry_run": True, "count": len(refs)}

    url = f"{BASE_URL}/api/v1/athlete/{athlete_id}/events/bulk-delete"

    if is_debug(lvl):
        log("DEBUG", "delete_payload", payload=refs)

    r = intervals_http(lvl, "PUT", url, api_key, json_body=refs, timeout=120)

    out: Dict[str, Any] = {"dry_run": False, "count": len(refs), "status": r.status_code}
    try:
        out["json"] = r.json()
    except Exception:
        out["json"] = None
    return out


# ============================================================
# Selection logic
# ============================================================
def start_date_local(ev: IcsEvent) -> str:
    return f"{ev.dtstart_date.isoformat()}T07:00:00"


def select_events(
    events: List[IcsEvent],
    include_today: bool,
    all_workouts: bool,
    next_week: bool,
    log_level: str,
) -> List[IcsEvent]:
    today = aus_today()

    if all_workouts:
        selected = events[:]
    else:
        if include_today:
            selected = [e for e in events if e.dtstart_date >= today]
        else:
            selected = [e for e in events if e.dtstart_date > today]

    if next_week:
        start = today if include_today else (today + dt.timedelta(days=1))
        end = start + dt.timedelta(days=7)
        selected = [e for e in selected if start <= e.dtstart_date < end]

    if selected:
        dmin = min(e.dtstart_date for e in selected)
        dmax = max(e.dtstart_date for e in selected)
        log("INFO", "ics_selected_dates", start=str(dmin), end=str(dmax))
    else:
        log("INFO", "ics_selected_dates", start=None, end=None)

    log(
        "INFO",
        "ics_selected",
        selected=len(selected),
        include_today=include_today,
        all_workouts=all_workouts,
        next_week=next_week,
    )
    return selected


def hard_guard_drop_past(selected: List[IcsEvent], include_today: bool, log_level: str) -> List[IcsEvent]:
    lvl = normalize_log_level(log_level)
    today = aus_today()

    if include_today:
        kept = [e for e in selected if e.dtstart_date >= today]
        dropped = [e for e in selected if e.dtstart_date < today]
    else:
        kept = [e for e in selected if e.dtstart_date > today]
        dropped = [e for e in selected if e.dtstart_date <= today]

    if dropped:
        log("WARN", "dropped_past_events", dropped=len(dropped), kept=len(kept), today=str(today))
        if is_debug(lvl):
            log(
                "DEBUG",
                "dropped_past_event_examples",
                examples=[{"date": e.dtstart_date.isoformat(), "uid": e.uid, "summary": e.summary} for e in dropped[:10]],
            )

    return kept


# ============================================================
# Build Intervals payload
# ============================================================
def build_intervals_event(ev: IcsEvent, folder_id: int, log_level: str) -> Tuple[Dict[str, Any], bool]:
    name = (ev.summary or "").strip() or "Runna Workout"
    original_text = (ev.description or "").strip()

    translated, partial = translate_workout_to_intervals_text(name, original_text)

    # Divider between original and translated must be "-" (NOT "---")
    combined_desc = original_text.rstrip() + "\n\n-\n\n" + translated.rstrip()

    payload: Dict[str, Any] = {
        "start_date_local": start_date_local(ev),
        "name": name,
        "category": "WORKOUT",
        "type": "Run",
        "folder_id": folder_id,
        "external_id": ev.uid,
        "tags": [RUNNA_TAG],
        "description": combined_desc,
    }
    return payload, partial


def make_validation_report(results: List[Tuple[IcsEvent, bool, Dict[str, Any]]], log_level: str) -> Dict[str, Any]:
    lvl = normalize_log_level(log_level)
    total = len(results)
    partials = [r for r in results if r[1]]
    ok = total - len(partials)

    rep: Dict[str, Any] = {"total": total, "ok": ok, "partial": len(partials)}
    if is_debug(lvl):
        rep["partial_examples"] = [
            {"start_date_local": start_date_local(ev), "uid": ev.uid, "name": payload.get("name")}
            for (ev, _is_partial, payload) in partials[:10]
        ]
    return rep


# ============================================================
# Sync runner
# ============================================================
def run_sync(
    *,
    api_key: str,
    athlete_id: str,
    runna_ics_url: str,
    folder_name: str,
    dry_run: bool,
    include_today: bool,
    all_workouts: bool,
    next_week: bool,
    delete_all: bool,
    clean_legacy: bool,
    log_level: str,
) -> Dict[str, Any]:
    lvl = normalize_log_level(log_level)

    log(
        "INFO",
        "run_start",
        dry_run=dry_run,
        include_today=include_today,
        all_workouts=all_workouts,
        next_week=next_week,
        delete_all=delete_all,
        clean_legacy=clean_legacy,
        log_level=lvl,
    )

    auth_test(api_key, athlete_id, lvl)

    folder_name = folder_name or DEFAULT_FOLDER_NAME
    folder_id = ensure_folder(api_key, athlete_id, folder_name, lvl)

    log("INFO", "fetching_ics", url=runna_ics_url)
    ics_resp = requests.get(runna_ics_url, timeout=60)
    ics_resp.raise_for_status()

    events = parse_ics_events(ics_resp.text)
    log("INFO", "ics_parsed", events=len(events))

    selected = select_events(events, include_today, all_workouts, next_week, lvl)
    selected = hard_guard_drop_past(selected, include_today, lvl)

    built: List[Tuple[IcsEvent, bool, Dict[str, Any]]] = []
    for ev in selected:
        payload, partial = build_intervals_event(ev, folder_id, lvl)
        built.append((ev, partial, payload))

    report = make_validation_report(built, lvl)
    log("INFO", "validation_report", total=report["total"], ok=report["ok"], partial=report["partial"])
    if is_debug(lvl) and "partial_examples" in report:
        log("DEBUG", "validation_partial_examples", examples=report["partial_examples"])

    if clean_legacy:
        log("INFO", "clean_legacy_noop", note="Flag is present; legacy cleanup is not implemented in this sync script.")

    if delete_all:
        refs = [{"external_id": ev.uid} for (ev, _partial, _payload) in built]
        log("INFO", "delete_all_start", count=len(refs))
        delete_result = bulk_delete_events(api_key, athlete_id, refs, lvl, dry_run)

        if is_debug(lvl):
            log("DEBUG", "delete_all_complete", result=delete_result)
        else:
            slim = dict(delete_result)
            slim.pop("json", None)
            log("INFO", "delete_all_complete", **slim)

        log("INFO", "upload_skipped", reason="--delete-all set")
        return {
            "identified": len(built),
            "validation_report": report,
            "delete_result": (delete_result if is_debug(lvl) else {k: v for k, v in dict(delete_result).items() if k != "json"}),
            "upload_result": {"skipped": True, "reason": "--delete-all set"},
            "dry_run": dry_run,
        }

    upload_payload = [payload for (_ev, _partial, payload) in built]
    upload_result = bulk_upload_events(api_key, upload_payload, lvl, dry_run)

    # Upload-complete: full response JSON only in DEBUG
    if is_debug(lvl):
        log("DEBUG", "upload_complete", result=upload_result)
    else:
        slim = dict(upload_result)
        slim.pop("json", None)
        log("INFO", "upload_complete", **slim)

    return {
        "identified": len(built),
        "validation_report": report,
        "upload_result": (upload_result if is_debug(lvl) else {k: v for k, v in dict(upload_result).items() if k != "json"}),
        "dry_run": dry_run,
    }


# ============================================================
# CLI
# ============================================================
def parse_args(argv: List[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true", help="Do not delete or upload, just log.")
    p.add_argument("--include-today", action="store_true", help="Include workouts on today's date.")
    p.add_argument("--all-workouts", action="store_true", help="Include all workouts regardless of date.")
    p.add_argument("--next-week", action="store_true", help="Include only the next 7 days.")
    p.add_argument("--delete-all", action="store_true", help="Bulk-delete identified workouts and exit (no upload).")
    p.add_argument("--clean-legacy", action="store_true", help="Legacy cleanup hook (currently logs only).")
    p.add_argument("--folder-name", default=DEFAULT_FOLDER_NAME, help='Folder name to create/use (default "Runna").')
    p.add_argument("--log-level", default="INFO", help="DEBUG/INFO/WARN/ERROR (case-insensitive). Default INFO.")
    p.add_argument("--run-tests", action="store_true", help="Run unit tests and exit.")
    return p.parse_args(argv)


# ============================================================
# Unit Tests
# ============================================================


def run_tests() -> None:
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(TranslatorTests)
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    raise SystemExit(0 if result.wasSuccessful() else 1)


# ============================================================
# main
# ============================================================
def main(argv: Optional[List[str]] = None) -> None:
    args = parse_args(argv or sys.argv[1:])
    if args.run_tests:
        run_tests()

    api_key = os.environ.get("INTERVALS_API_KEY")
    athlete_id = os.environ.get("INTERVALS_ATHLETE_ID")
    runna_ics_url = os.environ.get("RUNNA_ICS_URL")

    if not api_key:
        raise RuntimeError("INTERVALS_API_KEY is not set")
    if not athlete_id:
        raise RuntimeError("INTERVALS_ATHLETE_ID is not set")
    if not runna_ics_url:
        raise RuntimeError("RUNNA_ICS_URL is not set")

    lvl = normalize_log_level(args.log_level)

    res = run_sync(
        api_key=api_key,
        athlete_id=athlete_id,
        runna_ics_url=runna_ics_url,
        folder_name=args.folder_name or DEFAULT_FOLDER_NAME,
        dry_run=args.dry_run,
        include_today=args.include_today,
        all_workouts=args.all_workouts,
        next_week=args.next_week,
        delete_all=args.delete_all,
        clean_legacy=args.clean_legacy,
        log_level=lvl,
    )

    if is_debug(lvl):
        log("DEBUG", "run_complete", result=res)
    else:
        log(
            "INFO",
            "run_complete",
            identified=res.get("identified"),
            ok=res.get("validation_report", {}).get("ok"),
            partial=res.get("validation_report", {}).get("partial"),
            dry_run=res.get("dry_run"),
            upload_status=res.get("upload_result", {}).get("status"),
        )


# ============================================================
# Cloudflare Workers entrypoint (scheduled + fetch)
# ============================================================
class Default(WorkerEntrypoint):  # type: ignore
    async def scheduled(self, controller, env, ctx):
        log("INFO", "cron_processed")

        # Use self.env (per your requirement)
        api_key = getattr(self.env, "INTERVALS_API_KEY", None)
        athlete_id = getattr(self.env, "INTERVALS_ATHLETE_ID", None)
        runna_ics_url = getattr(self.env, "RUNNA_ICS_URL", None)
        folder_name = getattr(self.env, "FOLDER_NAME", DEFAULT_FOLDER_NAME) or DEFAULT_FOLDER_NAME

        if not api_key or not athlete_id or not runna_ics_url:
            log(
                "ERROR",
                "worker_missing_env",
                have_api_key=bool(api_key),
                have_athlete_id=bool(athlete_id),
                have_ics_url=bool(runna_ics_url),
            )
            return

        run_sync(
            api_key=api_key,
            athlete_id=athlete_id,
            runna_ics_url=runna_ics_url,
            folder_name=folder_name,
            dry_run=False,
            include_today=False,
            all_workouts=False,
            next_week=False,
            delete_all=False,
            clean_legacy=False,
            log_level="INFO",
        )

    async def fetch(self, request, env, ctx):
        from urllib.parse import parse_qs, urlparse

        # Use self.env (per your requirement)
        api_key = getattr(self.env, "INTERVALS_API_KEY", None)
        athlete_id = getattr(self.env, "INTERVALS_ATHLETE_ID", None)
        runna_ics_url = getattr(self.env, "RUNNA_ICS_URL", None)

        if not api_key or not athlete_id or not runna_ics_url:
            return Response("Missing env vars", status=500)  # type: ignore

        q = parse_qs(urlparse(request.url).query)

        def qbool(name: str) -> bool:
            v = (q.get(name, ["false"])[0] or "").lower()
            return v in ("1", "true", "yes", "y", "on")

        dry_run = qbool("dry_run")
        include_today = qbool("include_today")
        all_workouts = qbool("all_workouts")
        next_week = qbool("next_week")
        delete_all = qbool("delete_all")
        clean_legacy = qbool("clean_legacy")

        log_level = normalize_log_level(q.get("log_level", ["INFO"])[0])
        folder_name = (q.get("folder_name", [DEFAULT_FOLDER_NAME])[0] or DEFAULT_FOLDER_NAME)

        res = run_sync(
            api_key=api_key,
            athlete_id=athlete_id,
            runna_ics_url=runna_ics_url,
            folder_name=folder_name,
            dry_run=dry_run,
            include_today=include_today,
            all_workouts=all_workouts,
            next_week=next_week,
            delete_all=delete_all,
            clean_legacy=clean_legacy,
            log_level=log_level,
        )

        return Response(json.dumps(res), headers={"Content-Type": "application/json"})  # type: ignore


if __name__ == "__main__":
    main()