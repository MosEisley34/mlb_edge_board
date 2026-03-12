#!/usr/bin/env python3
import json
from pathlib import Path

ARTIFACT_DIR = Path('tmp_external_check')
SOURCE = ARTIFACT_DIR / 'weather_joined_conf.json'
OUT_ALL = ARTIFACT_DIR / 'weather_joined_conf_overridden.json'
OUT_ACCEPTED = ARTIFACT_DIR / 'weather_joined_conf_high_only.json'
OUT_REJECTS = ARTIFACT_DIR / 'weather_rejects.json'

PRESEASON_PARK_OVERRIDES = {
    'Camelback Ranch': {'geoVenueName': 'Camelback Ranch', 'lat': 33.5130, 'lon': -112.2950},
    'CACTI Park of the Palm Beaches': {'geoVenueName': 'CACTI Park of the Palm Beaches', 'lat': 26.7845, 'lon': -80.1002},
    'Publix Field at Joker Marchant Stadium': {'geoVenueName': 'Publix Field at Joker Marchant Stadium', 'lat': 28.0623, 'lon': -81.9496},
    'Lee Health Sports Complex': {'geoVenueName': 'Lee Health Sports Complex', 'lat': 26.5365, 'lon': -81.7892},
}


def load_rows(path: Path):
    data = json.loads(path.read_text())
    if not isinstance(data, list):
        raise ValueError(f'Expected list JSON at {path}, got {type(data).__name__}')
    return data


def apply_preseason_overrides(rows):
    for row in rows:
        venue = str(row.get('venueName', '')).strip()
        if venue in PRESEASON_PARK_OVERRIDES and str(row.get('geoConfidence', '')).upper() != 'HIGH':
            override = PRESEASON_PARK_OVERRIDES[venue]
            row['geoVenueName'] = override['geoVenueName']
            row['lat'] = override['lat']
            row['lon'] = override['lon']
            row['geoReason'] = 'static_override_preseason_park'
            row['geoConfidence'] = 'HIGH'
            row['geoOverrideApplied'] = True
    return rows


def split_rows(rows):
    accepted, rejected = [], []
    for row in rows:
        if str(row.get('geoConfidence', '')).upper() == 'HIGH':
            accepted.append(row)
        else:
            rejected.append(row)
    return accepted, rejected


def main():
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)
    rows = load_rows(SOURCE)
    rows = apply_preseason_overrides(rows)
    accepted, rejected = split_rows(rows)

    OUT_ALL.write_text(json.dumps(rows, indent=2) + '\n')
    OUT_ACCEPTED.write_text(json.dumps(accepted, indent=2) + '\n')
    OUT_REJECTS.write_text(json.dumps(rejected, indent=2) + '\n')

    pct = (len(accepted) / len(rows) * 100.0) if rows else 0.0
    print(json.dumps({
        'weather_total_rows': len(rows),
        'weather_accepted_rows': len(accepted),
        'weather_rejected_rows': len(rejected),
        'weather_accepted_pct': round(pct, 2),
        'accepted_artifact': str(OUT_ACCEPTED),
        'rejected_artifact': str(OUT_REJECTS),
    }, indent=2))


if __name__ == '__main__':
    main()
