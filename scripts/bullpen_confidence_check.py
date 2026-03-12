#!/usr/bin/env python3
import argparse
import json
import random
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Tuple

ARTIFACT_DIR = Path("tmp_external_check")
ROWS_PATH = ARTIFACT_DIR / "bullpen_rows_conf.json"
SUMMARY_PATH = ARTIFACT_DIR / "external_conf_summary.json"

SCHEDULE_URL = "https://statsapi.mlb.com/api/v1/schedule"
BOXSCORE_URL_TMPL = "https://statsapi.mlb.com/api/v1/game/{gamePk}/boxscore"

HIGH_CONF_MIN_RELIEVERS = 2
HIGH_CONF_MIN_TEAM_PITCHES = 25


@dataclass
class FetchResult:
    payload: Dict
    error: str


def fetch_json(url: str, timeout_s: int = 20) -> FetchResult:
    req = urllib.request.Request(url, headers={"User-Agent": "mlb-edge-board-bullpen-check/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read().decode("utf-8")
            return FetchResult(payload=json.loads(raw), error="")
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError, json.JSONDecodeError) as exc:
        return FetchResult(payload={}, error=str(exc))


def ymd(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d")


def get_schedule_games(lookback_days: int) -> Tuple[List[Dict], str, str, str]:
    now = datetime.now(timezone.utc)
    end_date = ymd(now)
    start_date = ymd(now - timedelta(days=lookback_days))
    q = urllib.parse.urlencode({"sportId": 1, "startDate": start_date, "endDate": end_date})
    url = f"{SCHEDULE_URL}?{q}"
    res = fetch_json(url)
    if res.error:
        raise RuntimeError(f"schedule fetch failed: {res.error}")

    games: List[Dict] = []
    for date_block in res.payload.get("dates", []):
        for g in date_block.get("games", []):
            games.append(g)
    return games, start_date, end_date, url


def calc_days_ago(game_date_iso: str, now: datetime) -> int:
    try:
        game_ts = datetime.fromisoformat(game_date_iso.replace("Z", "+00:00"))
    except ValueError:
        return 0
    delta = now - game_ts
    return max(0, int(delta.total_seconds() // 86400))


def accumulate_team_relief(team: Dict, side: str, game_pk: str, game_date: str, days_ago: int) -> Dict:
    team_name = str(((team or {}).get("team") or {}).get("name") or "").strip()
    pitchers = (team or {}).get("pitchers") or []
    players = (team or {}).get("players") or {}

    starter_id = str(pitchers[0]) if pitchers else ""
    relievers = []
    team_pitch_total = 0

    for idx, pid_raw in enumerate(pitchers):
        pid = str(pid_raw)
        if not pid or pid == starter_id:
            continue
        p = players.get(f"ID{pid}", {})
        stats = (p.get("stats") or {}).get("pitching") or {}
        pitches = stats.get("numberOfPitches", 0)
        try:
            pitches = int(pitches)
        except (TypeError, ValueError):
            pitches = 0
        if pitches < 0:
            pitches = 0

        relievers.append({
            "pitcherId": pid,
            "pitcherName": str((p.get("person") or {}).get("fullName") or "").strip(),
            "orderInPitchersList": idx,
            "pitches": pitches,
            "outs": stats.get("outs"),
            "battersFaced": stats.get("battersFaced"),
        })
        team_pitch_total += pitches

    confidence = "HIGH" if (len(relievers) >= HIGH_CONF_MIN_RELIEVERS and team_pitch_total >= HIGH_CONF_MIN_TEAM_PITCHES) else "LOW"
    reason = "ok"
    if not relievers:
        reason = "no_relievers_recorded"
    elif len(relievers) < HIGH_CONF_MIN_RELIEVERS:
        reason = "low_reliever_count"
    elif team_pitch_total < HIGH_CONF_MIN_TEAM_PITCHES:
        reason = "low_reliever_pitch_total"

    return {
        "gamePk": game_pk,
        "gameDate": game_date,
        "daysAgo": days_ago,
        "side": side,
        "team": team_name,
        "relieverCount": len(relievers),
        "relieverPitchTotal": team_pitch_total,
        "relievers": relievers,
        "bullpenConfidence": confidence,
        "bullpenReason": reason,
        "boxscoreFetched": True,
        "boxscoreFetchError": "",
    }


def extract_rows(games: List[Dict], max_retry: int) -> Tuple[List[Dict], List[Dict], int]:
    now = datetime.now(timezone.utc)
    rows: List[Dict] = []
    failed_games: List[Dict] = []

    for g in games:
        game_pk = str(g.get("gamePk") or "").strip()
        if not game_pk:
            continue
        game_date = str(g.get("gameDate") or "")
        days_ago = calc_days_ago(game_date, now)

        url = BOXSCORE_URL_TMPL.format(gamePk=urllib.parse.quote(game_pk))
        res = fetch_json(url)
        if res.error:
            failed_games.append({"gamePk": game_pk, "error": res.error})
            for side in ("away", "home"):
                team_name = str((((g.get("teams") or {}).get(side) or {}).get("team") or {}).get("name") or "").strip()
                rows.append({
                    "gamePk": game_pk,
                    "gameDate": game_date,
                    "daysAgo": days_ago,
                    "side": side,
                    "team": team_name,
                    "relieverCount": None,
                    "relieverPitchTotal": None,
                    "relievers": [],
                    "bullpenConfidence": "LOW",
                    "bullpenReason": "boxscore_fetch_error",
                    "boxscoreFetched": False,
                    "boxscoreFetchError": res.error,
                })
            continue

        box = res.payload
        for side in ("away", "home"):
            team_box = ((box.get("teams") or {}).get(side)) or {}
            rows.append(accumulate_team_relief(team_box, side, game_pk, game_date, days_ago))

    retries_used = 0
    if failed_games:
        retries_used = retry_failed_games(rows, failed_games, max_retry)

    return rows, failed_games, retries_used


def retry_failed_games(rows: List[Dict], failed_games: List[Dict], max_retry: int) -> int:
    retries_used = 0
    remaining = list({f["gamePk"]: f for f in failed_games}.values())

    for _ in range(max_retry):
        if not remaining:
            break
        retries_used += 1
        next_remaining = []
        for fg in remaining:
            game_pk = fg["gamePk"]
            url = BOXSCORE_URL_TMPL.format(gamePk=urllib.parse.quote(game_pk))
            res = fetch_json(url)
            if res.error:
                next_remaining.append({"gamePk": game_pk, "error": res.error})
                continue

            box = res.payload
            for row in rows:
                if str(row.get("gamePk")) != game_pk:
                    continue
                side = row.get("side")
                team_box = ((box.get("teams") or {}).get(side)) or {}
                refreshed = accumulate_team_relief(
                    team_box,
                    side,
                    game_pk,
                    str(row.get("gameDate") or ""),
                    int(row.get("daysAgo") or 0),
                )
                row.update(refreshed)

        remaining = next_remaining

    return retries_used


def summarize(rows: List[Dict], lookback_days: int, start_date: str, end_date: str, schedule_url: str, sample_size: int) -> Dict:
    total = len(rows)
    nonnull_team = sum(1 for r in rows if str(r.get("team") or "").strip())
    high_conf = sum(1 for r in rows if str(r.get("bullpenConfidence") or "").upper() == "HIGH")
    fetch_errors = sum(1 for r in rows if not r.get("boxscoreFetched"))

    team_nonnull_pct = (nonnull_team / total * 100.0) if total else 0.0
    high_conf_pct = (high_conf / total * 100.0) if total else 0.0

    low_conf_rows = [
        {
            "gamePk": r.get("gamePk"),
            "team": r.get("team"),
            "side": r.get("side"),
            "reason": r.get("bullpenReason"),
            "fetchError": r.get("boxscoreFetchError"),
        }
        for r in rows
        if str(r.get("bullpenConfidence") or "").upper() != "HIGH" or not r.get("boxscoreFetched")
    ]

    rng = random.Random(42)
    sampled = rng.sample(rows, k=min(sample_size, len(rows))) if rows else []
    sampled_checks = []
    for r in sampled:
        reliever_count = r.get("relieverCount")
        pitch_total = r.get("relieverPitchTotal")
        plausible = isinstance(reliever_count, int) and isinstance(pitch_total, int) and 0 <= reliever_count <= 12 and 0 <= pitch_total <= 220
        sampled_checks.append({
            "gamePk": r.get("gamePk"),
            "team": r.get("team"),
            "side": r.get("side"),
            "relieverCount": reliever_count,
            "relieverPitchTotal": pitch_total,
            "plausible": plausible,
            "bullpenConfidence": r.get("bullpenConfidence"),
            "bullpenReason": r.get("bullpenReason"),
        })

    return {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": "mlb_statsapi",
        "lookback_days": lookback_days,
        "start_date": start_date,
        "end_date": end_date,
        "schedule_url": schedule_url,
        "bullpen_total_rows": total,
        "bullpen_team_nonnull_rows": nonnull_team,
        "bullpen_team_nonnull_pct": round(team_nonnull_pct, 3),
        "bullpen_high_conf_rows": high_conf,
        "bullpen_high_conf_pct": round(high_conf_pct, 3),
        "bullpen_boxscore_fetch_errors": fetch_errors,
        "thresholds": {
            "bullpen_team_nonnull_pct_gte_99": team_nonnull_pct >= 99.0,
            "bullpen_high_conf_pct_gte_99": high_conf_pct >= 99.0,
            "bullpen_boxscore_fetch_errors_eq_0": fetch_errors == 0,
        },
        "low_conf_or_error_rows": low_conf_rows,
        "spot_check_random_rows": sampled_checks,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Bullpen extraction confidence checker")
    parser.add_argument("--lookback-days", type=int, default=4)
    parser.add_argument("--sample-size", type=int, default=5)
    parser.add_argument("--max-retry", type=int, default=2)
    args = parser.parse_args()

    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)

    games = []
    start_date = ""
    end_date = ""
    schedule_url = ""
    schedule_error = ""
    try:
        games, start_date, end_date, schedule_url = get_schedule_games(args.lookback_days)
    except Exception as exc:  # keep producing artifacts for monitoring
        schedule_error = str(exc)

    rows, failed_games, retries_used = extract_rows(games, args.max_retry) if not schedule_error else ([], [], 0)
    summary = summarize(rows, args.lookback_days, start_date, end_date, schedule_url, args.sample_size)
    summary["schedule_games"] = len(games)
    summary["schedule_fetch_error"] = schedule_error
    summary["initial_failed_boxscore_games"] = failed_games
    summary["retries_used"] = retries_used

    ROWS_PATH.write_text(json.dumps(rows, indent=2) + "\n")
    SUMMARY_PATH.write_text(json.dumps(summary, indent=2) + "\n")

    print(json.dumps({
        "bullpen_rows_artifact": str(ROWS_PATH),
        "summary_artifact": str(SUMMARY_PATH),
        "bullpen_total_rows": summary["bullpen_total_rows"],
        "bullpen_team_nonnull_pct": summary["bullpen_team_nonnull_pct"],
        "bullpen_high_conf_pct": summary["bullpen_high_conf_pct"],
        "bullpen_boxscore_fetch_errors": summary["bullpen_boxscore_fetch_errors"],
        "thresholds": summary["thresholds"],
        "retries_used": retries_used,
    }, indent=2))

    threshold_ok = all(summary["thresholds"].values()) and not schedule_error
    return 0 if threshold_ok else 2


if __name__ == "__main__":
    sys.exit(main())
