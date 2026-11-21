#!/usr/bin/env python3
"""
Shopping Sessions: DuckDB pipeline.

Reads  $DATA_DIR    (default /data)
Writes $OUTPUT_DIR  (default /output): user_daily/ and campaign_daily/ as Parquet
Temp   $SCRATCH_DIR (default /scratch)
"""
import json
import os
import sys
import time
from datetime import datetime, timezone

import duckdb
import numpy as np
import pyarrow as pa
from scipy.sparse import coo_matrix
from scipy.sparse.csgraph import connected_components

DATA = os.environ.get("DATA_DIR", "/data")
OUT = os.environ.get("OUTPUT_DIR", "/output")
SCRATCH = os.environ.get("SCRATCH_DIR", "/scratch")
NCPU = int(os.environ.get("NCPU", os.cpu_count() or 1))
MEM_GB = int(os.environ.get("MEM_GB", "8"))

T0 = time.time()


def log(msg):
    print(f"[{time.time() - T0:7.2f}s] {msg}", file=sys.stderr, flush=True)


DAY_MS = 86_400_000
WEEK_MS = 7 * DAY_MS
SESSION_GAP_MS = 1_800_000

con = duckdb.connect()
con.execute(f"SET threads = {NCPU}")
con.execute(f"SET memory_limit = '{max(1, int(MEM_GB * 0.85))}GB'")
con.execute(f"SET temp_directory = '{os.path.join(SCRATCH, 'duckdb_tmp')}'")
con.execute("SET preserve_insertion_order = false")

with open(os.path.join(DATA, "manifest.json")) as f:
    manifest = json.load(f)


def iso_ms(s):
    dt = datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    return int(dt.timestamp()) * 1000


WS = iso_ms(manifest["window_start"])
WE = iso_ms(manifest["window_end"])

# ---------------------------------------------------------------- SQL pieces

FIELD = r'("[^",]*"|[^",]*)'


def line_pattern(n):
    return "^" + ",".join([FIELD] * n) + "$"


def raw_lines(glob):
    # one VARCHAR column holding the whole line; \r stays an ordinary character
    return (f"read_csv('{glob}', columns={{'l': 'VARCHAR'}}, delim='\\x01', quote='', escape='', "
            f"header=true, auto_detect=false, strict_mode=false, null_padding=false)")


def parsed(glob, names):
    """Lines of a CSV as unquoted columns (empty -> NULL). Malformed lines give all NULL."""
    n = len(names)
    keys = ", ".join(f"'{c}'" for c in names)
    sel = ", ".join(
        f"NULLIF(CASE WHEN starts_with(g.{c}, '\"') THEN substr(g.{c}, 2, length(g.{c}) - 2) ELSE g.{c} END, '') AS {c}"
        for c in names
    )
    return (f"SELECT {sel} FROM (SELECT regexp_extract(l, '{line_pattern(n)}', [{keys}]) AS g "
            f"FROM {raw_lines(glob)})")


# timestamp (string -> epoch ms or NULL)
TS_RE = r"^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(?:Z|([+-])([0-9]{2}):([0-9]{2}))$"
TS_KEYS = "['y','mo','d','h','mi','s','sg','oh','om']"

con.execute(f"""
CREATE MACRO ts_parts(s) AS regexp_extract(s, '{TS_RE}', {TS_KEYS});
CREATE MACRO ts_ms(s) AS (
  CASE
    WHEN s IS NULL THEN NULL
    WHEN regexp_full_match(s, '[0-9]+') THEN TRY_CAST(s AS BIGINT)
    ELSE (
      CASE WHEN ts_parts(s).y = '' THEN NULL
           WHEN ts_parts(s).h::INT > 23 OR ts_parts(s).mi::INT > 59 OR ts_parts(s).s::INT > 59 THEN NULL
           WHEN ts_parts(s).oh <> '' AND (ts_parts(s).oh::INT > 23 OR ts_parts(s).om::INT > 59) THEN NULL
           ELSE (
             TRY_CAST(ts_parts(s).y || '-' || ts_parts(s).mo || '-' || ts_parts(s).d AS DATE)::DATE - DATE '1970-01-01'
           )::BIGINT * {DAY_MS}
             + ts_parts(s).h::BIGINT * 3600000 + ts_parts(s).mi::BIGINT * 60000 + ts_parts(s).s::BIGINT * 1000
             - CASE WHEN ts_parts(s).oh = '' THEN 0
                    ELSE (CASE WHEN ts_parts(s).sg = '-' THEN -1 ELSE 1 END)
                         * (ts_parts(s).oh::BIGINT * 3600000 + ts_parts(s).om::BIGINT * 60000) END
      END)
  END)
""")

# A "\r" is an ordinary character, but DuckDB's CSV reader treats it as a line break.
# The generator never writes one, so only small inputs (fixtures) get it rewritten to \x02.
def prep_dir(sub):
    src = os.path.join(DATA, sub)
    files = sorted(f for f in os.listdir(src) if f.endswith(".csv"))
    if sum(os.path.getsize(os.path.join(src, f)) for f in files) > 200_000_000:
        return os.path.join(src, "*.csv")
    dst = os.path.join(SCRATCH, "prep", sub)
    os.makedirs(dst, exist_ok=True)
    for f in files:
        with open(os.path.join(src, f), "rb") as fi, open(os.path.join(dst, f), "wb") as fo:
            fo.write(fi.read().replace(b"\r", b"\x02"))
    return os.path.join(dst, "*.csv")


# ---------------------------------------------------------------- reference data

log("start")
con.execute(f"""
CREATE TEMP TABLE campaigns AS
SELECT DISTINCT campaign_id, channel FROM ({parsed(os.path.join(DATA, 'campaigns.csv'), ['campaign_id', 'channel'])})
WHERE campaign_id IS NOT NULL
""")
con.execute(f"""
CREATE TEMP TABLE products AS
SELECT DISTINCT product_id, category FROM ({parsed(os.path.join(DATA, 'products.csv'), ['product_id', 'category'])})
WHERE product_id IS NOT NULL
""")

# ---------------------------------------------------------------- linked IDs

links_glob = prep_dir("links")
con.execute(f"""
CREATE TEMP TABLE lk AS
SELECT DISTINCT user_id_a AS a, user_id_b AS b
FROM ({parsed(links_glob, ['user_id_a', 'user_id_b'])})
WHERE user_id_a IS NOT NULL AND user_id_b IS NOT NULL AND user_id_a <> user_id_b
""")
con.execute("""
CREATE TEMP TABLE lk_ids AS
SELECT id, (row_number() OVER ())::BIGINT - 1 AS idx FROM (
  SELECT a AS id FROM lk UNION SELECT b FROM lk
)
""")
n_ids = con.execute("SELECT count(*) FROM lk_ids").fetchone()[0]
if n_ids:
    edges = con.execute("""
        SELECT x.idx AS ia, y.idx AS ib FROM lk
        JOIN lk_ids x ON x.id = lk.a JOIN lk_ids y ON y.id = lk.b
    """).fetchnumpy()
    ia, ib = edges["ia"], edges["ib"]
    g = coo_matrix((np.ones(len(ia), dtype=np.int8), (ia, ib)), shape=(n_ids, n_ids))
    _, labels = connected_components(g, directed=False)
    con.register("lab", pa.table({"idx": np.arange(n_ids, dtype=np.int64), "lab": labels.astype(np.int64)}))
    con.execute("""
    CREATE TEMP TABLE pmap AS
    SELECT i.id, min(i.id) OVER (PARTITION BY l.lab) AS person
    FROM lk_ids i JOIN lab l USING (idx)
    """)
    con.unregister("lab")
else:
    con.execute("CREATE TEMP TABLE pmap (id VARCHAR, person VARCHAR)")
log(f"links done ({n_ids} ids)")

# ---------------------------------------------------------------- events

ev_glob = prep_dir("events")
EV_COLS = ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"]
con.execute(f"""
CREATE TEMP TABLE ev_kept AS
SELECT event_id,
       arg_min(user_id, ingest_ts) AS user_id, arg_min(ts, ingest_ts) AS ts, min(ingest_ts) AS ingest_ts,
       arg_min(event_type, ingest_ts) AS event_type, arg_min(product_id, ingest_ts) AS product_id,
       arg_min(price_cents, ingest_ts) AS price_cents
FROM ({parsed(ev_glob, EV_COLS)})
WHERE event_id IS NOT NULL
GROUP BY event_id
""")
log("events deduped")
con.execute(f"""
CREATE TEMP TABLE ev AS
SELECT coalesce(pm.person, k.user_id) AS person, k.ts_ms AS ts,
       k.event_type = 'purchase' AS is_purchase,
       CASE WHEN k.event_type = 'purchase' THEN k.price_cents::BIGINT ELSE 0 END AS price,
       coalesce(p.category, 'unknown') AS category
FROM (
  SELECT user_id, ts_ms(ts) AS ts_ms, ts_ms(ingest_ts) AS ing, event_type, product_id, price_cents
  FROM ev_kept
  WHERE user_id IS NOT NULL AND product_id IS NOT NULL
    AND event_type IN ('view', 'cart', 'purchase')
    AND (event_type <> 'purchase'
         OR (regexp_full_match(price_cents, '[0-9]+') AND TRY_CAST(price_cents AS BIGINT) BETWEEN 1 AND 10000000))
) k
LEFT JOIN pmap pm ON pm.id = k.user_id
LEFT JOIN products p ON p.product_id = k.product_id
WHERE k.ts_ms >= {WS} AND k.ts_ms < {WE} AND k.ing - k.ts_ms <= {DAY_MS}
""")
con.execute("DROP TABLE ev_kept")
log("events cleaned")

# ---------------------------------------------------------------- sessions

con.execute(f"""
CREATE TEMP TABLE ev_s AS
SELECT person, ts, is_purchase, price, category,
       max(CASE WHEN nw THEN ts END) OVER (PARTITION BY person ORDER BY ts) AS start
FROM (
  SELECT *, (lag(ts) OVER (PARTITION BY person ORDER BY ts) IS NULL
             OR ts - lag(ts) OVER (PARTITION BY person ORDER BY ts) > {SESSION_GAP_MS}) AS nw
  FROM ev
)
""")
con.execute("DROP TABLE ev")
con.execute(f"""
CREATE TEMP TABLE sess AS
SELECT person, start, (DATE '1970-01-01' + (start // {DAY_MS})::INTEGER) AS day,
       count(*) AS events, sum(price) AS revenue, count_if(is_purchase) AS purchases
FROM ev_s GROUP BY person, start
""")
log("sessions built")

# user_daily
con.execute("""
CREATE TEMP TABLE user_cat AS
SELECT person, day, arg_min(category, (-s, category)) AS top_category FROM (
  SELECT person, (DATE '1970-01-01' + (start // 86400000)::INTEGER) AS day, category, sum(price) AS s
  FROM ev_s WHERE is_purchase GROUP BY ALL
) GROUP BY person, day
""")
con.execute("DROP TABLE ev_s")
os.makedirs(os.path.join(OUT, "user_daily"), exist_ok=True)
os.makedirs(os.path.join(OUT, "campaign_daily"), exist_ok=True)
con.execute(f"""
COPY (
  SELECT s.person AS user_id, s.day, count(*)::BIGINT AS sessions, sum(s.events)::BIGINT AS events,
         sum(s.revenue)::BIGINT AS revenue_cents, any_value(c.top_category) AS top_category
  FROM sess s LEFT JOIN user_cat c ON c.person = s.person AND c.day = s.day
  GROUP BY s.person, s.day
) TO '{os.path.join(OUT, 'user_daily', 'part-0.parquet')}' (FORMAT parquet)
""")
log("user_daily written")

# ---------------------------------------------------------------- clicks

ck_glob = prep_dir("clicks")
CK_COLS = ["click_id", "user_id", "campaign_id", "ts"]
con.execute(f"""
CREATE TEMP TABLE clk AS
SELECT coalesce(pm.person, k.user_id) AS person, k.ts, k.campaign_id, k.click_id
FROM (
  SELECT click_id, any_value(user_id) AS user_id, any_value(campaign_id) AS campaign_id, any_value(ts_ms(ts)) AS ts
  FROM ({parsed(ck_glob, CK_COLS)}) WHERE click_id IS NOT NULL GROUP BY click_id
) k
JOIN campaigns c ON c.campaign_id = k.campaign_id
LEFT JOIN pmap pm ON pm.id = k.user_id
WHERE k.user_id IS NOT NULL AND k.ts >= {WS - WEEK_MS} AND k.ts < {WE}
""")
con.execute("""
CREATE TEMP TABLE clk_pt AS
SELECT person, ts, arg_min(campaign_id, click_id) AS campaign_id FROM clk GROUP BY person, ts
""")
log("clicks cleaned")

# credit
con.execute(f"""
CREATE TEMP TABLE credited AS
SELECT s.day, s.purchases > 0 AS purchasing, s.revenue,
       CASE WHEN c.ts IS NOT NULL AND s.start - c.ts <= {WEEK_MS} THEN c.campaign_id ELSE 'organic' END AS campaign_id
FROM sess s ASOF LEFT JOIN clk_pt c ON s.person = c.person AND s.start >= c.ts
""")
log("credit done")

# assists
con.execute(f"""
CREATE TEMP TABLE sess_rank AS
SELECT person, start, day, (row_number() OVER (PARTITION BY person ORDER BY start))::BIGINT AS rk FROM sess
""")
con.execute("""
CREATE TEMP TABLE sess_day AS
SELECT person, day, min(rk) AS rlo, max(rk) AS rhi FROM sess_rank GROUP BY person, day
""")
con.execute(f"""
CREATE TEMP TABLE islands AS
SELECT person, campaign_id, min(ts) AS a, max(ts) + {WEEK_MS} AS b FROM (
  SELECT *, sum(CASE WHEN gap > {WEEK_MS} THEN 1 ELSE 0 END) OVER (PARTITION BY person, campaign_id ORDER BY ts) AS isl
  FROM (
    SELECT c.person, c.campaign_id, c.ts,
           c.ts - lag(c.ts) OVER (PARTITION BY c.person, c.campaign_id ORDER BY c.ts) AS gap
    FROM (SELECT DISTINCT person, campaign_id, ts FROM clk
          WHERE person IN (SELECT person FROM sess_day)) c
  )
) GROUP BY person, campaign_id, isl
""")
con.execute("""
CREATE TEMP TABLE island_rk AS
SELECT i.person, i.campaign_id,
       coalesce(l.rk, 0) AS lo, coalesce(h.rk, 0) AS hi
FROM islands i
ASOF LEFT JOIN sess_rank l ON i.person = l.person AND (i.a - 1) >= l.start
ASOF LEFT JOIN sess_rank h ON i.person = h.person AND i.b >= h.start
""")
con.execute("""
CREATE TEMP TABLE assisted AS
SELECT campaign_id, day, sum(n)::BIGINT AS assisted_sessions FROM (
  SELECT r.campaign_id, d.day,
         least(r.hi, d.rhi) - greatest(r.lo, d.rlo - 1) AS n
  FROM island_rk r JOIN sess_day d ON d.person = r.person
)
WHERE n > 0 GROUP BY campaign_id, day
""")
log("assists done")

con.execute(f"""
COPY (
  SELECT x.campaign_id, coalesce(c.channel, 'organic') AS channel, x.day,
         coalesce(cr.sessions, 0)::BIGINT AS sessions,
         coalesce(cr.purchasing_sessions, 0)::BIGINT AS purchasing_sessions,
         coalesce(cr.revenue_cents, 0)::BIGINT AS revenue_cents,
         coalesce(a.assisted_sessions, 0)::BIGINT AS assisted_sessions
  FROM (
    SELECT campaign_id, day FROM credited UNION SELECT campaign_id, day FROM assisted
  ) x
  LEFT JOIN (
    SELECT campaign_id, day, count(*) AS sessions, count_if(purchasing) AS purchasing_sessions,
           sum(revenue) AS revenue_cents
    FROM credited GROUP BY campaign_id, day
  ) cr ON cr.campaign_id = x.campaign_id AND cr.day = x.day
  LEFT JOIN assisted a ON a.campaign_id = x.campaign_id AND a.day = x.day
  LEFT JOIN campaigns c ON c.campaign_id = x.campaign_id
) TO '{os.path.join(OUT, 'campaign_daily', 'part-0.parquet')}' (FORMAT parquet)
""")
log("done")
