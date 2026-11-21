#!/usr/bin/env python3
"""
Shopping Sessions: slow, literal reference implementation of rules.md.

It exists only as an oracle for differential testing of the fast Rust
pipeline. Plain loops, dicts and sorts; polars is used only to write Parquet.

Usage: reference.py [DATA_DIR OUTPUT_DIR]   (or env DATA_DIR / OUTPUT_DIR)
"""
import bisect
import json
import os
import sys
from pathlib import Path

DAY_MS = 86_400_000
GAP_MS = 1_800_000
REACH_MS = 7 * DAY_MS
LATE_MS = 24 * 3_600_000


# ---------------------------------------------------------------- CSV dialect

def csv_rows(path, ncols):
    """Yield the field lists (bytes) of every well-formed data line of a file.

    Only b'\\n' ends a line; the first line is the header. A field may be
    wrapped in quotes as a whole; any other quote makes the line malformed. A
    line with the wrong field count (an empty line included) is malformed.
    Empty fields (quoted or not) come back as b'' (null)."""
    data = Path(path).read_bytes()
    lines = data.split(b"\n")
    if data.endswith(b"\n"):
        lines.pop()  # the text after the final newline is not a line
    for line in lines[1:]:
        fields = line.split(b",")
        if len(fields) != ncols:
            continue
        out = []
        ok = True
        for f in fields:
            if b'"' in f:
                if len(f) >= 2 and f[0:1] == b'"' and f[-1:] == b'"' and b'"' not in f[1:-1]:
                    out.append(f[1:-1])
                else:
                    ok = False
                    break
            else:
                out.append(f)
        if ok:
            yield out


def csv_files(folder):
    folder = Path(folder)
    if not folder.is_dir():
        return []
    return sorted(p for p in folder.iterdir() if p.name.endswith(".csv") and p.is_file())


# ----------------------------------------------------------------- timestamps

def is_digits(b):
    return len(b) > 0 and all(48 <= c <= 57 for c in b)


def days_from_civil(y, m, d):
    y = y - 1 if m <= 2 else y
    era = y // 400
    yoe = y - era * 400
    doy = (153 * ((m + 9) % 12) + 2) // 5 + d - 1
    doe = yoe * 365 + yoe // 4 - yoe // 100 + doy
    return era * 146_097 + doe - 719_468


def is_leap(y):
    return y % 4 == 0 and (y % 100 != 0 or y % 400 == 0)


def days_in_month(y, m):
    if m == 2:
        return 29 if is_leap(y) else 28
    return 30 if m in (4, 6, 9, 11) else 31


def parse_civil(b):
    """`YYYY-MM-DDTHH:MM:SS` (19 bytes) -> ms since epoch, or None."""
    if len(b) != 19:
        return None
    if b[4:5] != b"-" or b[7:8] != b"-" or b[10:11] != b"T" or b[13:14] != b":" or b[16:17] != b":":
        return None
    parts = (b[0:4], b[5:7], b[8:10], b[11:13], b[14:16], b[17:19])
    if not all(is_digits(p) for p in parts):
        return None
    y, mo, d, h, mi, s = (int(p) for p in parts)
    if not (1 <= mo <= 12):
        return None
    if not (1 <= d <= days_in_month(y, mo)):
        return None
    if h > 23 or mi > 59 or s > 59:
        return None
    return (days_from_civil(y, mo, d) * 86_400 + h * 3600 + mi * 60 + s) * 1000


def parse_ts(b):
    """F1 / F2 / F3 -> integer ms since the epoch (any size), or None if bad."""
    if len(b) == 0:
        return None
    if is_digits(b):  # F3
        return int(b)
    if len(b) == 20 and b[19:20] == b"Z":  # F1
        return parse_civil(b[:19])
    if len(b) == 25 and b[19:20] in (b"+", b"-") and b[22:23] == b":":  # F2
        base = parse_civil(b[:19])
        oh, om = b[20:22], b[23:25]
        if base is None or not is_digits(oh) or not is_digits(om):
            return None
        oh, om = int(oh), int(om)
        if oh > 23 or om > 59:
            return None
        off = (oh * 60 + om) * 60_000
        return base - off if b[19:20] == b"+" else base + off
    return None


def utc_date(ms):
    days = ms // DAY_MS
    # civil_from_days
    z = days + 719_468
    era = z // 146_097
    doe = z - era * 146_097
    yoe = (doe - doe // 1460 + doe // 36524 - doe // 146096) // 365
    doy = doe - (365 * yoe + yoe // 4 - yoe // 100)
    mp = (5 * doy + 2) // 153
    d = doy - (153 * mp + 2) // 5 + 1
    m = mp + 3 if mp < 10 else mp - 9
    y = yoe + era * 400 + (1 if m <= 2 else 0)
    import datetime
    return datetime.date(y, m, d)


# -------------------------------------------------------------------- pipeline

def main():
    if len(sys.argv) >= 3:
        data_dir, out_dir = Path(sys.argv[-2]), Path(sys.argv[-1])
    else:
        data_dir = Path(os.environ.get("DATA_DIR", "/data"))
        out_dir = Path(os.environ.get("OUTPUT_DIR", "/output"))

    manifest = json.loads((data_dir / "manifest.json").read_text())
    ws = parse_ts(manifest["window_start"].encode())
    we = parse_ts(manifest["window_end"].encode())
    assert ws is not None and we is not None

    # Lookups (clean files, parsed with the same dialect anyway).
    category = {}
    for f in csv_rows(data_dir / "products.csv", 2):
        category[f[0]] = f[1]
    channel = {}
    for f in csv_rows(data_dir / "campaigns.csv", 2):
        channel[f[0]] = f[1]

    # ---- Linked IDs: union-find, person named by the byte-wise smallest ID.
    parent = {}

    def find(x):
        root = x
        while parent.get(root, root) != root:
            root = parent[root]
        while x != root:
            nxt = parent[x]
            parent[x] = root
            x = nxt
        return root

    for path in csv_files(data_dir / "links"):
        for a, b in csv_rows(path, 2):
            if a == b"" or b == b"":
                continue
            parent.setdefault(a, a)
            parent.setdefault(b, b)
            ra, rb = find(a), find(b)
            if ra != rb:
                parent[ra] = rb
    smallest = {}
    for x in parent:
        r = find(x)
        if r not in smallest or x < smallest[r]:
            smallest[r] = x

    def person(uid):
        if uid in parent:
            return smallest[find(uid)]
        return uid

    # ---- Events: parse, dedup on event_id (smallest ingest_ts), then validate.
    kept = {}  # event_id -> (ingest_ms, fields)
    for path in csv_files(data_dir / "events"):
        for f in csv_rows(path, 7):
            eid = f[0]
            if eid == b"":
                continue
            ingest = parse_ts(f[3])
            if ingest is None:  # never happens: ingest_ts is always valid F1
                continue
            cur = kept.get(eid)
            if cur is None or ingest < cur[0]:
                kept[eid] = (ingest, f)

    by_person = {}  # person -> list of (ts, event_type, product_id, price)
    for eid, (ingest, f) in kept.items():
        _, uid, ts_raw, _, etype, pid, price_raw = f
        if uid == b"":
            continue
        ts = parse_ts(ts_raw)
        if ts is None or not (ws <= ts < we):
            continue
        if etype not in (b"view", b"cart", b"purchase"):
            continue
        if pid == b"":
            continue
        price = 0
        if etype == b"purchase":
            if not is_digits(price_raw):
                continue
            price = int(price_raw)
            if not (1 <= price <= 10_000_000):
                continue
        if ingest - ts > LATE_MS:
            continue
        by_person.setdefault(person(uid), []).append((ts, etype, pid, price))

    # ---- Clicks: parse, dedup on click_id, validate.
    seen_clicks = {}
    for path in csv_files(data_dir / "clicks"):
        for f in csv_rows(path, 4):
            if f[0] == b"":
                continue
            if f[0] not in seen_clicks:
                seen_clicks[f[0]] = f
    clicks_by_person = {}  # person -> list of (ts, click_id, campaign)
    for cid, f in seen_clicks.items():
        _, uid, camp, ts_raw = f
        if uid == b"":
            continue
        ts = parse_ts(ts_raw)
        if ts is None or not (ws - REACH_MS <= ts < we):
            continue
        if camp not in channel:
            continue
        clicks_by_person.setdefault(person(uid), []).append((ts, cid, camp))
    for lst in clicks_by_person.values():
        lst.sort()

    # ---- Sessionize, credit, assist, aggregate.
    user_daily = {}  # (person, date) -> [sessions, events, revenue, {category: cents}]
    camp_daily = {}  # (campaign, date) -> [sessions, purchasing, revenue, assisted]

    for p, evs in by_person.items():
        evs.sort(key=lambda e: e[0])
        sessions = []
        prev = None
        for e in evs:
            if prev is None or e[0] - prev > GAP_MS:
                sessions.append([e])
            else:
                sessions[-1].append(e)
            prev = e[0]
        pclicks = clicks_by_person.get(p, [])
        cts = [c[0] for c in pclicks]
        for s in sessions:
            start = s[0][0]
            day = utc_date(start)
            revenue = sum(e[3] for e in s if e[1] == b"purchase")
            purchased = any(e[1] == b"purchase" for e in s)
            ud = user_daily.setdefault((p, day), [0, 0, 0, {}])
            ud[0] += 1
            ud[1] += len(s)
            ud[2] += revenue
            for e in s:
                if e[1] == b"purchase":
                    cat = category.get(e[2], b"unknown")
                    ud[3][cat] = ud[3].get(cat, 0) + e[3]
            # clicks with start - 7d <= ts <= start
            lo = bisect.bisect_left(cts, start - REACH_MS)
            hi = bisect.bisect_right(cts, start)
            best = None
            assists = set()
            for c in pclicks[lo:hi]:
                assists.add(c[2])
                if best is None or c[0] > best[0] or (c[0] == best[0] and c[1] < best[1]):
                    best = c
            credited = best[2] if best is not None else b"organic"
            cd = camp_daily.setdefault((credited, day), [0, 0, 0, 0])
            cd[0] += 1
            cd[1] += 1 if purchased else 0
            cd[2] += revenue
            for a in assists:
                cd = camp_daily.setdefault((a, day), [0, 0, 0, 0])
                cd[3] += 1

    import polars as pl

    u_rows = {"user_id": [], "day": [], "sessions": [], "events": [], "revenue_cents": [], "top_category": []}
    for (p, day), (ns, ne, rev, cats) in user_daily.items():
        top = None
        if cats:
            best = max(cats.values())
            top = min(c for c, v in cats.items() if v == best).decode()
        u_rows["user_id"].append(p.decode())
        u_rows["day"].append(day)
        u_rows["sessions"].append(ns)
        u_rows["events"].append(ne)
        u_rows["revenue_cents"].append(rev)
        u_rows["top_category"].append(top)
    c_rows = {"campaign_id": [], "channel": [], "day": [], "sessions": [], "purchasing_sessions": [],
              "revenue_cents": [], "assisted_sessions": []}
    for (c, day), (ns, npur, rev, nas) in camp_daily.items():
        c_rows["campaign_id"].append(c.decode())
        c_rows["channel"].append("organic" if c == b"organic" else channel[c].decode())
        c_rows["day"].append(day)
        c_rows["sessions"].append(ns)
        c_rows["purchasing_sessions"].append(npur)
        c_rows["revenue_cents"].append(rev)
        c_rows["assisted_sessions"].append(nas)

    u_schema = {"user_id": pl.String, "day": pl.Date, "sessions": pl.Int64, "events": pl.Int64,
                "revenue_cents": pl.Int64, "top_category": pl.String}
    c_schema = {"campaign_id": pl.String, "channel": pl.String, "day": pl.Date, "sessions": pl.Int64,
                "purchasing_sessions": pl.Int64, "revenue_cents": pl.Int64, "assisted_sessions": pl.Int64}
    for name, rows, schema in (("user_daily", u_rows, u_schema), ("campaign_daily", c_rows, c_schema)):
        folder = out_dir / name
        folder.mkdir(parents=True, exist_ok=True)
        pl.DataFrame(rows, schema=schema).write_parquet(folder / "part-0.parquet")


if __name__ == "__main__":
    main()
