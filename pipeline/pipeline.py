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

_con = duckdb.connect()


class _Con:
    # PROFILE=1 logs every statement's time to stderr
    def __getattr__(self, name):
        return getattr(_con, name)

    def execute(self, sql, *a):
        t = time.time()
        r = _con.execute(sql, *a)
        if os.environ.get("PROFILE"):
            log(f"  stmt {time.time() - t:6.2f}s  {' '.join(sql.split())[:70]}")
        return r


con = _Con()
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

FIELD_NC = r'(?:"[^",]*"|[^",]*)'


def line_pattern(n):
    return "^" + ",".join([FIELD_NC] * n) + "$"


def raw_lines(glob):
    # one VARCHAR column holding the whole line
    return (f"read_csv('{glob}', columns={{'l': 'VARCHAR'}}, delim='\\x01', quote='', escape='', "
            f"header=true, auto_detect=false, strict_mode=false, null_padding=false)")


def parsed(glob, names):
    """Well-formed lines of a CSV as unquoted columns (empty -> NULL).

    A valid line has exactly n fields, each either free of quotes or wrapped whole in
    one pair; since no value holds a comma, dropping all quotes and splitting on commas
    then gives the values."""
    n = len(names)
    sel = ", ".join(f"NULLIF(f[{i + 1}], '') AS {c}" for i, c in enumerate(names))
    return (f"SELECT {sel} FROM (SELECT string_split(replace(l, '\"', ''), ',') AS f "
            f"FROM {raw_lines(glob)} WHERE regexp_full_match(l, '{line_pattern(n)}'))")


ISO_RE = "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(?:Z|[+-][0-9]{2}:[0-9]{2})"


def ts_sql(c):
    """SQL expression: timestamp string column -> epoch ms, NULL when bad."""
    return f"""(CASE
  WHEN {c} IS NULL THEN NULL
  WHEN regexp_full_match({c}, '[0-9]+') THEN TRY_CAST({c} AS BIGINT)
  WHEN regexp_full_match({c}, '{ISO_RE}') THEN
    CASE WHEN substr({c}, 12, 2)::INT <= 23 AND substr({c}, 15, 2)::INT <= 59 AND substr({c}, 18, 2)::INT <= 59
          AND (length({c}) = 20 OR (substr({c}, 21, 2)::INT <= 23 AND substr({c}, 24, 2)::INT <= 59))
    THEN (TRY_CAST(substr({c}, 1, 10) AS DATE) - DATE '1970-01-01')::BIGINT * {DAY_MS}
         + substr({c}, 12, 2)::BIGINT * 3600000 + substr({c}, 15, 2)::BIGINT * 60000 + substr({c}, 18, 2)::BIGINT * 1000
         - CASE WHEN length({c}) = 20 THEN 0
                ELSE (CASE WHEN substr({c}, 20, 1) = '-' THEN -1 ELSE 1 END)
                     * (substr({c}, 21, 2)::BIGINT * 3600000 + substr({c}, 24, 2)::BIGINT * 60000) END
    END
END)"""


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


def day_date(col):
    return f"(DATE '1970-01-01' + {col}::INTEGER)"


# ---------------------------------------------------------------- reference data

log("start")
con.execute(f"""
CREATE TEMP TABLE campaigns AS
SELECT campaign_id, channel, (row_number() OVER (ORDER BY campaign_id))::INTEGER AS cid FROM (
  SELECT DISTINCT campaign_id, channel FROM ({parsed(os.path.join(DATA, 'campaigns.csv'), ['campaign_id', 'channel'])})
  WHERE campaign_id IS NOT NULL
)
""")
con.execute(f"""
CREATE TEMP TABLE prod_raw AS
SELECT DISTINCT product_id, category FROM ({parsed(os.path.join(DATA, 'products.csv'), ['product_id', 'category'])})
WHERE product_id IS NOT NULL
""")
# category codes sort like the category names (byte-wise); 'unknown' is a category like any other
con.execute("""
CREATE TEMP TABLE cats AS
SELECT category, (row_number() OVER (ORDER BY category))::SMALLINT AS code
FROM (SELECT category FROM prod_raw WHERE category IS NOT NULL UNION SELECT 'unknown')
""")
con.execute("""
CREATE TEMP TABLE products AS
SELECT p.product_id, c.code FROM prod_raw p JOIN cats c ON c.category = p.category
""")
UNKNOWN_CODE = con.execute("SELECT code FROM cats WHERE category = 'unknown'").fetchone()[0]

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
    SELECT id, person, hash(id) AS idh, hash(person) AS ph FROM (
      SELECT i.id, min(i.id) OVER (PARTITION BY l.lab) AS person
      FROM lk_ids i JOIN lab l USING (idx)
    )
    """)
    con.unregister("lab")
else:
    con.execute("CREATE TEMP TABLE pmap (id VARCHAR, person VARCHAR, idh UBIGINT, ph UBIGINT)")
log(f"links done ({n_ids} ids)")

# ---------------------------------------------------------------- events

ev_glob = prep_dir("events")
EV_COLS = ["event_id", "user_id", "ts", "ingest_ts", "event_type", "product_id", "price_cents"]
# One parse of the files. Timestamps become integers right away (a bad one becomes NULL), so
# the table holds short inline strings and integers only. Validation waits until after dedup.
con.execute(f"""
CREATE TEMP TABLE ev_raw AS
SELECT hash(event_id) AS h, event_id, user_id, {ts_sql('ts')} AS ts, {ts_sql('ingest_ts')} AS ing,
       event_type, product_id, price_cents
FROM ({parsed(ev_glob, EV_COLS)}) WHERE event_id IS NOT NULL
""")
log("events parsed")
# Most event ids occur once: find repeated hashes cheaply; only those rows (plus any
# hash collisions) go through the exact group-by on event_id.
con.execute("CREATE TEMP TABLE dup_h AS SELECT h FROM ev_raw GROUP BY h HAVING count(*) > 1")
con.execute(f"""
CREATE TEMP TABLE ev AS
WITH kept AS (
  SELECT user_id, ts, ing, event_type, product_id, price_cents FROM ev_raw ANTI JOIN dup_h USING (h)
  UNION ALL
  SELECT arg_min_null(user_id, ing), arg_min_null(ts, ing), min(ing),
         arg_min_null(event_type, ing), arg_min_null(product_id, ing), arg_min_null(price_cents, ing)
  FROM ev_raw SEMI JOIN dup_h USING (h) GROUP BY event_id
),
k AS (
  SELECT * FROM kept
  WHERE user_id IS NOT NULL AND product_id IS NOT NULL
    AND ts >= {WS} AND ts < {WE} AND ing - ts <= {DAY_MS}
    AND event_type IN ('view', 'cart', 'purchase')
    AND (event_type <> 'purchase'
         OR (regexp_full_match(price_cents, '[0-9]+') AND TRY_CAST(price_cents AS BIGINT) BETWEEN 1 AND 10000000))
)
SELECT k.user_id AS uid, coalesce(pm.ph, hash(k.user_id)) AS ph, k.ts, k.event_type = 'purchase' AS is_purchase,
       CASE WHEN k.event_type = 'purchase' THEN k.price_cents::BIGINT ELSE 0 END AS price,
       coalesce(p.code, {UNKNOWN_CODE}::SMALLINT) AS cat
FROM k
LEFT JOIN pmap pm ON pm.idh = hash(k.user_id)
LEFT JOIN products p ON p.product_id = k.product_id
""")
con.execute("DROP TABLE ev_raw")
con.execute("CREATE TEMP TABLE names AS SELECT ph, any_value(uid) AS uid FROM ev GROUP BY ph")
log("events cleaned")

# ---------------------------------------------------------------- sessions

con.execute(f"""
CREATE TEMP TABLE ev_s AS
SELECT ph, is_purchase, price, cat,
       (max(CASE WHEN nw THEN ts END) OVER (PARTITION BY ph ORDER BY ts)) AS start
FROM (
  SELECT ph, ts, is_purchase, price, cat,
         (lag(ts) OVER (PARTITION BY ph ORDER BY ts) IS NULL
          OR ts - lag(ts) OVER (PARTITION BY ph ORDER BY ts) > {SESSION_GAP_MS}) AS nw
  FROM ev
)
""")
con.execute("DROP TABLE ev")
con.execute(f"""
CREATE TEMP TABLE sess AS
SELECT ph, start, start // {DAY_MS} AS day,
       count(*) AS events, sum(price) AS revenue, count_if(is_purchase) AS purchases
FROM ev_s GROUP BY ph, start
""")
log("sessions built")

con.execute(f"""
CREATE TEMP TABLE user_cat AS
SELECT ph, day, arg_min(cat, (-s, cat)) AS top_cat FROM (
  SELECT ph, start // {DAY_MS} AS day, cat, sum(price) AS s
  FROM ev_s WHERE is_purchase GROUP BY ALL
) GROUP BY ph, day
""")
con.execute("DROP TABLE ev_s")
os.makedirs(os.path.join(OUT, "user_daily"), exist_ok=True)
os.makedirs(os.path.join(OUT, "campaign_daily"), exist_ok=True)
con.execute(f"""
COPY (
  SELECT coalesce(pm.person, n.uid) AS user_id, {day_date('s.day')} AS day, s.sessions, s.events, s.revenue_cents,
         cc.category AS top_category
  FROM (
    SELECT ph, day, count(*)::BIGINT AS sessions, sum(events)::BIGINT AS events, sum(revenue)::BIGINT AS revenue_cents
    FROM sess GROUP BY ph, day
  ) s
  JOIN names n ON n.ph = s.ph
  LEFT JOIN pmap pm ON pm.id = n.uid
  LEFT JOIN user_cat c ON c.ph = s.ph AND c.day = s.day
  LEFT JOIN cats cc ON cc.code = c.top_cat
) TO '{os.path.join(OUT, 'user_daily', 'part-0.parquet')}' (FORMAT parquet)
""")
log("user_daily written")

# ---------------------------------------------------------------- clicks
# Duplicate clicks are identical, so they change nothing below and are not removed.

ck_glob = prep_dir("clicks")
CK_COLS = ["click_id", "user_id", "campaign_id", "ts"]
con.execute(f"""
CREATE TEMP TABLE clk AS
SELECT coalesce(pm.ph, hash(k.user_id)) AS ph, k.ts, c.cid, k.click_id
FROM (
  SELECT click_id, user_id, campaign_id, {ts_sql('ts')} AS ts
  FROM ({parsed(ck_glob, CK_COLS)})
  WHERE click_id IS NOT NULL AND user_id IS NOT NULL
) k
JOIN campaigns c ON c.campaign_id = k.campaign_id
LEFT JOIN pmap pm ON pm.idh = hash(k.user_id)
WHERE k.ts >= {WS - WEEK_MS} AND k.ts < {WE}
""")
con.execute("""
CREATE TEMP TABLE clk_pt AS
SELECT ph, ts, arg_min(cid, click_id) AS cid FROM clk GROUP BY ph, ts
""")
log("clicks cleaned")

con.execute(f"""
CREATE TEMP TABLE credited AS
SELECT s.day, s.purchases > 0 AS purchasing, s.revenue,
       CASE WHEN c.ts IS NOT NULL AND s.start - c.ts <= {WEEK_MS} THEN c.cid ELSE 0 END AS cid
FROM sess s ASOF LEFT JOIN clk_pt c ON s.ph = c.ph AND s.start >= c.ts
""")
log("credit done")

# assists: per (person, campaign) the clicks' 7-day windows merge into islands; count the
# person's sessions inside each island per day using session ranks.
con.execute("""
CREATE TEMP TABLE sess_rank AS
SELECT ph, start, day, (row_number() OVER (PARTITION BY ph ORDER BY start))::BIGINT AS rk FROM sess
""")
con.execute("""
CREATE TEMP TABLE sess_day AS
SELECT ph, day, min(rk) AS rlo, max(rk) AS rhi FROM sess_rank GROUP BY ph, day
""")
con.execute(f"""
CREATE TEMP TABLE islands AS
SELECT ph, cid, min(ts) AS a, max(ts) + {WEEK_MS} AS b FROM (
  SELECT *, sum(CASE WHEN gap > {WEEK_MS} THEN 1 ELSE 0 END) OVER (PARTITION BY ph, cid ORDER BY ts) AS isl
  FROM (
    SELECT c.ph, c.cid, c.ts, c.ts - lag(c.ts) OVER (PARTITION BY c.ph, c.cid ORDER BY c.ts) AS gap
    FROM (SELECT DISTINCT ph, cid, ts FROM clk SEMI JOIN sess_day USING (ph)) c
  )
) GROUP BY ph, cid, isl
""")
con.execute("""
CREATE TEMP TABLE island_rk AS
SELECT i.ph, i.cid, coalesce(l.rk, 0) AS lo, coalesce(h.rk, 0) AS hi
FROM islands i
ASOF LEFT JOIN sess_rank l ON i.ph = l.ph AND (i.a - 1) >= l.start
ASOF LEFT JOIN sess_rank h ON i.ph = h.ph AND i.b >= h.start
""")
con.execute("""
CREATE TEMP TABLE assisted AS
SELECT cid, day, sum(n)::BIGINT AS assisted_sessions FROM (
  SELECT r.cid, d.day, least(r.hi, d.rhi) - greatest(r.lo, d.rlo - 1) AS n
  FROM island_rk r JOIN sess_day d ON d.ph = r.ph
)
WHERE n > 0 GROUP BY cid, day
""")
log("assists done")

con.execute(f"""
COPY (
  SELECT coalesce(c.campaign_id, 'organic') AS campaign_id, coalesce(c.channel, 'organic') AS channel,
         {day_date('x.day')} AS day,
         coalesce(cr.sessions, 0)::BIGINT AS sessions,
         coalesce(cr.purchasing_sessions, 0)::BIGINT AS purchasing_sessions,
         coalesce(cr.revenue_cents, 0)::BIGINT AS revenue_cents,
         coalesce(a.assisted_sessions, 0)::BIGINT AS assisted_sessions
  FROM (
    SELECT cid, day FROM credited UNION SELECT cid, day FROM assisted
  ) x
  LEFT JOIN (
    SELECT cid, day, count(*) AS sessions, count_if(purchasing) AS purchasing_sessions,
           sum(revenue) AS revenue_cents
    FROM credited GROUP BY cid, day
  ) cr ON cr.cid = x.cid AND cr.day = x.day
  LEFT JOIN assisted a ON a.cid = x.cid AND a.day = x.day
  LEFT JOIN campaigns c ON c.cid = x.cid
) TO '{os.path.join(OUT, 'campaign_daily', 'part-0.parquet')}' (FORMAT parquet)
""")
log("done")
