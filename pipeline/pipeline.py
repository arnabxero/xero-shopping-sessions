#!/usr/bin/env python3
"""
Shopping Sessions: DuckDB implementation of rules.md.

This is the readable reference pipeline. The submitted image runs the native
pipeline in native/ (same rules, much faster); this one is kept as an
independent cross-check: scripts/crosscheck.sh diffs the two on generated data.

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)
"""
import json
import os
import sys

import duckdb

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))

MINUTE = 60_000
DAY = 24 * 60 * MINUTE


def log(*args):
    print(*args, file=sys.stderr, flush=True)


con = duckdb.connect()
con.execute(f"SET threads = {NCPU}")
con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.8))}GB'")
con.execute(f"SET temp_directory = '{os.path.join(SCRATCH, 'duckdb_tmp')}'")
con.execute("SET preserve_insertion_order = false")

# ---------------------------------------------------------------- CSV dialect
# DuckDB's CSV reader does not follow rules.md (quotes, \r, malformed lines), so
# every file is read as text and split by hand: only \n ends a line, the first
# line is the header, every comma separates fields, and a field is either
# quote-free or wrapped in one pair of quotes. Anything else is malformed.
con.execute("""
CREATE MACRO fields(line) AS list_transform(
    string_split(line, ','),
    x -> CASE WHEN len(x) >= 2 AND x[1] = '"' AND x[len(x)] = '"'
              THEN substr(x, 2, len(x) - 2) ELSE x END)
""")


def parsed(folder_or_file, n):
    """SQL for the well-formed data lines of the CSV files, as a list `p` of n fields."""
    path = os.path.join(DATA, folder_or_file)
    if not path.endswith(".csv"):
        path = os.path.join(path, "*.csv")
    return f"""
        SELECT p FROM (
            SELECT fields(line) AS p FROM (
                SELECT unnest(string_split(content, chr(10))[2:]) AS line
                FROM read_text('{path}')
            ) WHERE line <> ''
        ) WHERE len(p) = {n} AND NOT contains(array_to_string(p, ''), '"')
    """


# ----------------------------------------------------------------- timestamps
# F1 / F2 dates are checked by DuckDB's TIMESTAMP cast; the time-of-day and offset
# ranges are checked here. F3 is digits only; more than 15 significant digits is
# far outside any window, so it is null like any other bad timestamp.
D2 = "[0-9]{2}"
CIVIL = f"[0-9]{{4}}-{D2}-{D2}T{D2}:{D2}:{D2}"
con.execute(f"""
CREATE MACRO civil_ms(s) AS CASE
    -- DuckDB's cast accepts hour 24 as the next midnight; the rules do not.
    WHEN substr(s, 12, 2) <= '23' AND substr(s, 15, 2) <= '59' AND substr(s, 18, 2) <= '59'
        THEN epoch_ms(TRY_CAST(replace(substr(s, 1, 19), 'T', ' ') AS TIMESTAMP)) END;
CREATE MACRO parse_ts(s) AS CASE
    WHEN regexp_full_match(s, '{CIVIL}Z') THEN civil_ms(s)
    WHEN regexp_full_match(s, '{CIVIL}[+-]{D2}:{D2}')
         AND substr(s, 21, 2) <= '23' AND substr(s, 24, 2) <= '59'
        THEN civil_ms(s) - (CASE WHEN substr(s, 20, 1) = '-' THEN -1 ELSE 1 END)
             * (CAST(substr(s, 21, 2) AS BIGINT) * 60 + CAST(substr(s, 24, 2) AS BIGINT)) * {MINUTE}
    WHEN regexp_full_match(s, '[0-9]+') AND len(ltrim(s, '0')) <= 15
        THEN CAST('0' || ltrim(s, '0') AS BIGINT)
    END;
CREATE MACRO parse_price(s) AS CASE
    WHEN regexp_full_match(s, '[0-9]+') AND len(ltrim(s, '0')) <= 8
        THEN CAST('0' || ltrim(s, '0') AS BIGINT) END;
""")

with open(os.path.join(DATA, "manifest.json")) as f:
    manifest = json.load(f)
W0, W1 = (con.execute("SELECT parse_ts(?)", [manifest[k]]).fetchone()[0]
          for k in ("window_start", "window_end"))
log("window:", manifest["window_start"], "to", manifest["window_end"])

con.execute(f"CREATE TABLE products AS SELECT p[1] AS product_id, p[2] AS category FROM ({parsed('products.csv', 2)})")
con.execute(f"CREATE TABLE campaigns AS SELECT p[1] AS campaign_id, p[2] AS channel FROM ({parsed('campaigns.csv', 2)})")

# ------------------------------------------------------------------ linked IDs
# Union-find over the link lines (SQL has no good transitive closure for chains
# hundreds of links long), then each person is named by their smallest ID.
con.execute(f"""
CREATE TABLE links AS SELECT p[1] AS a, p[2] AS b FROM ({parsed('links', 2)})
WHERE p[1] <> '' AND p[2] <> ''
""")
con.execute("""
CREATE TABLE link_ids AS SELECT id, CAST(row_number() OVER () - 1 AS BIGINT) AS n
FROM (SELECT a AS id FROM links UNION SELECT b FROM links)
""")
pairs = con.execute("""
SELECT DISTINCT x.n, y.n FROM links JOIN link_ids x ON x.id = a JOIN link_ids y ON y.id = b
WHERE a <> b
""").fetchall()
parent = list(range(con.execute("SELECT count(*) FROM link_ids").fetchone()[0]))


def find(x):
    root = x
    while parent[root] != root:
        root = parent[root]
    while parent[x] != root:
        parent[x], x = root, parent[x]
    return root


for a, b in pairs:
    ra, rb = find(a), find(b)
    if ra != rb:
        parent[ra] = rb
con.execute("CREATE TABLE roots (n BIGINT, root BIGINT)")
if parent:
    import numpy as np
    roots_np = {"n": np.arange(len(parent), dtype=np.int64),
                "root": np.fromiter((find(i) for i in range(len(parent))), dtype=np.int64, count=len(parent))}
    con.execute("INSERT INTO roots SELECT n, root FROM roots_np")
# VARCHAR comparison is byte-wise (no collation), which is what the rules ask for.
con.execute("""
CREATE TABLE person AS
SELECT id, min(id) OVER (PARTITION BY root) AS person FROM link_ids JOIN roots USING (n)
""")
log("links:", len(pairs), "distinct pairs,", len(parent), "linked ids")

# ---------------------------------------------------------------------- events
con.execute(f"""
CREATE TABLE events AS
WITH parsed AS (
    SELECT p[1] AS event_id, p[2] AS user_id, p[3] AS ts, p[4] AS ingest_ts,
           p[5] AS event_type, p[6] AS product_id, p[7] AS price
    FROM ({parsed('events', 7)}) WHERE p[1] <> ''
), kept AS (  -- dedup before validation: the earliest-ingested copy, valid or not
    SELECT arg_min(parsed, ingest_ts) AS r FROM parsed GROUP BY event_id
), typed AS (
    SELECT r.user_id AS user_id, parse_ts(r.ts) AS ts, parse_ts(r.ingest_ts) AS ingest,
           r.event_type AS event_type, r.product_id AS product_id,
           CASE WHEN r.event_type = 'purchase' THEN parse_price(r.price) END AS price
    FROM kept
)
SELECT coalesce(person.person, user_id) AS person, ts, product_id, price
FROM typed LEFT JOIN person ON person.id = user_id
WHERE user_id <> '' AND ts >= {W0} AND ts < {W1}
  AND event_type IN ('view', 'cart', 'purchase') AND product_id <> ''
  AND (event_type <> 'purchase' OR price BETWEEN 1 AND 10000000)
  AND ingest - ts <= {DAY}
""")

# -------------------------------------------------------------------- sessions
# The default window frame is RANGE, so events sharing a ts are peers: they all
# get the same running count whichever of them carried the "new session" flag.
con.execute(f"""
CREATE TABLE session_events AS
SELECT person, ts, product_id, price,
       sum(new_session) OVER (PARTITION BY person ORDER BY ts) AS sid
FROM (
    SELECT *, CASE WHEN ts - lag(ts) OVER (PARTITION BY person ORDER BY ts) <= {30 * MINUTE}
                   THEN 0 ELSE 1 END AS new_session
    FROM events
)
""")
con.execute(f"""
CREATE TABLE sessions AS
SELECT person, sid, min(ts) AS start,
       CAST(to_timestamp(min(ts) / 1000) AT TIME ZONE 'UTC' AS DATE) AS day,
       count(*) AS events, coalesce(sum(price), 0) AS revenue, count(price) > 0 AS purchased
FROM session_events GROUP BY person, sid
""")

# ---------------------------------------------------------------------- clicks
con.execute(f"""
CREATE TABLE clicks AS
WITH parsed AS (
    SELECT DISTINCT p[1] AS click_id, p[2] AS user_id, p[3] AS campaign_id, parse_ts(p[4]) AS ts
    FROM ({parsed('clicks', 4)}) WHERE p[1] <> ''
)
SELECT click_id, coalesce(person.person, user_id) AS person, campaign_id, ts
FROM parsed LEFT JOIN person ON person.id = user_id
WHERE user_id <> '' AND ts >= {W0 - 7 * DAY} AND ts < {W1}
  AND campaign_id IN (SELECT campaign_id FROM campaigns)
""")
# Credit: the latest click at or before the start (ties: smallest click_id), if
# it is within 7 days. Assists: every campaign with a click in those 7 days.
con.execute(f"""
CREATE TABLE credited AS
WITH best AS (  -- one candidate per person and instant
    SELECT person, ts, arg_min(campaign_id, click_id) AS campaign_id FROM clicks GROUP BY person, ts
)
SELECT s.person, s.sid, s.day, s.revenue, s.purchased,
       CASE WHEN s.start - c.ts <= {7 * DAY} THEN c.campaign_id ELSE 'organic' END AS campaign_id
FROM sessions s ASOF LEFT JOIN best c ON s.person = c.person AND s.start >= c.ts
""")
con.execute(f"""
CREATE TABLE assists AS
SELECT campaign_id, day, count(*) AS assisted_sessions FROM (
    SELECT DISTINCT s.person, s.sid, s.day, c.campaign_id
    FROM sessions s JOIN clicks c
      ON s.person = c.person AND c.ts <= s.start AND c.ts >= s.start - {7 * DAY}
) GROUP BY campaign_id, day
""")

# --------------------------------------------------------------------- outputs
os.makedirs(os.path.join(OUT, "user_daily"), exist_ok=True)
os.makedirs(os.path.join(OUT, "campaign_daily"), exist_ok=True)
con.execute(f"""
COPY (
    WITH top AS (
        SELECT person, day, category FROM (
            SELECT s.person, s.day, coalesce(p.category, 'unknown') AS category, sum(e.price) AS revenue
            FROM session_events e JOIN sessions s USING (person, sid)
            LEFT JOIN products p USING (product_id)
            WHERE e.price IS NOT NULL
            GROUP BY ALL
        ) QUALIFY row_number() OVER (PARTITION BY person, day ORDER BY revenue DESC, category) = 1
    ), daily AS (
        SELECT person, day, count(*) AS sessions, sum(events) AS events, sum(revenue) AS revenue_cents
        FROM sessions GROUP BY person, day
    )
    SELECT person AS user_id, day, sessions::BIGINT AS sessions, events::BIGINT AS events,
           revenue_cents::BIGINT AS revenue_cents, top.category AS top_category
    FROM daily LEFT JOIN top USING (person, day)
) TO '{os.path.join(OUT, "user_daily", "part-0.parquet")}' (FORMAT parquet)
""")
con.execute(f"""
COPY (
    WITH c AS (
        SELECT campaign_id, day, count(*) AS sessions, count(*) FILTER (purchased) AS purchasing_sessions,
               sum(revenue) AS revenue_cents
        FROM credited GROUP BY campaign_id, day
    )
    SELECT campaign_id, coalesce(k.channel, 'organic') AS channel, day,
           coalesce(c.sessions, 0)::BIGINT AS sessions,
           coalesce(c.purchasing_sessions, 0)::BIGINT AS purchasing_sessions,
           coalesce(c.revenue_cents, 0)::BIGINT AS revenue_cents,
           coalesce(a.assisted_sessions, 0)::BIGINT AS assisted_sessions
    FROM c FULL JOIN assists a USING (campaign_id, day) LEFT JOIN campaigns k USING (campaign_id)
) TO '{os.path.join(OUT, "campaign_daily", "part-0.parquet")}' (FORMAT parquet)
""")
log("wrote user_daily and campaign_daily")
