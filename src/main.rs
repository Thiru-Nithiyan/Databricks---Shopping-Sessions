//! Shopping Sessions: single-pass parallel pipeline.
//!
//! Reads $DATA_DIR, writes $OUTPUT_DIR/{user_daily,campaign_daily}/*.parquet.
//! Phases: links -> union-find; events parsed in parallel chunks and sharded by event id for
//! dedup; surviving events are re-sharded by person, sorted, sessionized, credited and
//! assisted per person shard; each shard writes its own Parquet part.

use std::fs::{self, File};
use std::hash::{BuildHasherDefault, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use hashbrown::HashMap;
use memchr::{memchr, memchr_iter};
use memmap2::Mmap;
use parquet::basic::Compression;
use parquet::data_type::{ByteArray, ByteArrayType, Int32Type, Int64Type};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::file::writer::SerializedFileWriter;
use parquet::schema::parser::parse_message_type;
use rayon::prelude::*;
use xxhash_rust::xxh3::{xxh3_128, xxh3_64};

const DAY_MS: i64 = 86_400_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const GAP_MS: i64 = 1_800_000;
const NSHARD: usize = 256; // event-id shards (dedup)
const NPART: usize = 256; // person shards
const CHUNK: usize = 8 << 20;
const NO_CAT: u8 = 255;

type Bytes = &'static [u8];

// ------------------------------------------------------------------ utilities

#[derive(Default)]
struct IdHasher(u64);
impl Hasher for IdHasher {
    fn finish(&self) -> u64 {
        self.0
    }
    fn write(&mut self, _: &[u8]) {
        unreachable!()
    }
    fn write_u64(&mut self, v: u64) {
        self.0 = v;
    }
}
type IdMap<V> = HashMap<u64, V, BuildHasherDefault<IdHasher>>;

fn log(t: &Instant, msg: &str) {
    eprintln!("[{:7.2}s] {}", t.elapsed().as_secs_f64(), msg);
}

/// Map a file for the life of the process.
fn load(path: &Path) -> Bytes {
    let f = File::open(path).unwrap_or_else(|e| panic!("open {}: {e}", path.display()));
    if f.metadata().map(|m| m.len()).unwrap_or(0) == 0 {
        return &[];
    }
    let m = unsafe { Mmap::map(&f) }.unwrap_or_else(|e| panic!("mmap {}: {e}", path.display()));
    let m: &'static Mmap = Box::leak(Box::new(m));
    &m[..]
}

fn csv_files(dir: &Path) -> Vec<PathBuf> {
    let mut v: Vec<PathBuf> = match fs::read_dir(dir) {
        Ok(rd) => rd
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.is_file() && p.extension().map_or(false, |x| x == "csv"))
            .collect(),
        Err(_) => vec![],
    };
    v.sort();
    v
}

/// Split every file (header skipped) into pieces that end on a line boundary.
fn chunks(files: &[PathBuf]) -> Vec<Bytes> {
    let mut out = vec![];
    for p in files {
        let data = load(p);
        let mut start = match memchr(b'\n', data) {
            Some(i) => i + 1,
            None => continue,
        };
        while start < data.len() {
            let mut end = (start + CHUNK).min(data.len());
            if end < data.len() {
                end = match memchr(b'\n', &data[end..]) {
                    Some(i) => end + i + 1,
                    None => data.len(),
                };
            }
            out.push(&data[start..end]);
            start = end;
        }
    }
    out
}

/// Split a line into exactly N comma-separated fields, removing the quotes that wrap a
/// whole field. Returns None for a malformed line.
#[inline]
fn fields<const N: usize>(line: Bytes) -> Option<[Bytes; N]> {
    let mut out: [Bytes; N] = [&line[0..0]; N];
    let mut n = 0;
    let mut s = 0;
    for i in memchr_iter(b',', line) {
        if n >= N - 1 {
            return None;
        }
        out[n] = &line[s..i];
        n += 1;
        s = i + 1;
    }
    if n != N - 1 {
        return None;
    }
    out[n] = &line[s..];
    for f in out.iter_mut() {
        if memchr(b'"', f).is_some() {
            let l = f.len();
            if l < 2 || f[0] != b'"' || f[l - 1] != b'"' {
                return None;
            }
            let inner = &f[1..l - 1];
            if memchr(b'"', inner).is_some() {
                return None;
            }
            *f = inner;
        }
    }
    Some(out)
}

#[inline]
fn pack(s: Bytes) -> u64 {
    (s.as_ptr() as u64) | ((s.len() as u64) << 48)
}
#[inline]
fn unpack(v: u64) -> Bytes {
    unsafe { std::slice::from_raw_parts((v & ((1u64 << 48) - 1)) as *const u8, (v >> 48) as usize) }
}

fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

#[inline]
fn dig2(s: &[u8], i: usize) -> Option<i64> {
    let a = s[i].wrapping_sub(b'0');
    let b = s[i + 1].wrapping_sub(b'0');
    if a < 10 && b < 10 {
        Some((a * 10 + b) as i64)
    } else {
        None
    }
}

/// Timestamp field -> epoch ms, None when it is a bad timestamp.
fn parse_ts(s: &[u8]) -> Option<i64> {
    if s.is_empty() {
        return None;
    }
    if s.iter().all(|b| b.is_ascii_digit()) {
        let mut v: i64 = 0;
        for &b in s {
            v = v.checked_mul(10)?.checked_add((b - b'0') as i64)?;
        }
        return Some(v);
    }
    let n = s.len();
    if n != 20 && n != 25 {
        return None;
    }
    if s[4] != b'-' || s[7] != b'-' || s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let y = dig2(s, 0)? * 100 + dig2(s, 2)?;
    let mo = dig2(s, 5)?;
    let d = dig2(s, 8)?;
    let h = dig2(s, 11)?;
    let mi = dig2(s, 14)?;
    let se = dig2(s, 17)?;
    if !(1..=12).contains(&mo) || d < 1 || h > 23 || mi > 59 || se > 59 {
        return None;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let dim = match mo {
        2 => 28 + leap as i64,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if d > dim {
        return None;
    }
    let off = if n == 20 {
        if s[19] != b'Z' {
            return None;
        }
        0
    } else {
        let sign = match s[19] {
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        if s[22] != b':' {
            return None;
        }
        let oh = dig2(s, 20)?;
        let om = dig2(s, 23)?;
        if oh > 23 || om > 59 {
            return None;
        }
        sign * (oh * 3_600_000 + om * 60_000)
    };
    Some(days_from_civil(y, mo, d) * DAY_MS + h * 3_600_000 + mi * 60_000 + se * 1000 - off)
}

fn parse_price(s: &[u8]) -> Option<u32> {
    if s.is_empty() {
        return None;
    }
    let mut v: u64 = 0;
    for &b in s {
        if !b.is_ascii_digit() {
            return None;
        }
        v = v * 10 + (b - b'0') as u64;
        if v > 10_000_000 {
            return None;
        }
    }
    if v >= 1 {
        Some(v as u32)
    } else {
        None
    }
}

// ------------------------------------------------------------------ context

struct Ctx {
    ws: i64,
    we: i64,
    linked: IdMap<(u64, u64)>, // id hash -> (person key, person name)
    products: HashMap<Bytes, u8>,
    unknown: u8,
    campaigns: HashMap<Bytes, u32>,
}

impl Ctx {
    #[inline]
    fn person(&self, uid: Bytes) -> (u64, u64) {
        let h = xxh3_64(uid);
        match self.linked.get(&h) {
            Some(&v) => v,
            None => (h, pack(uid)),
        }
    }
}

fn read_manifest(dir: &Path) -> (i64, i64) {
    let text = fs::read_to_string(dir.join("manifest.json")).expect("manifest.json");
    let get = |key: &str| -> i64 {
        let i = text.find(key).unwrap_or_else(|| panic!("manifest key {key}"));
        let rest = &text[i + key.len()..];
        let q1 = rest.find('"').unwrap();
        let rest = &rest[q1 + 1..];
        let q2 = rest.find('"').unwrap();
        let rest = &rest[q2 + 1..];
        let q3 = rest.find('"').unwrap();
        parse_ts(rest[..q3].as_bytes()).expect("manifest ts")
    };
    (get("window_start"), get("window_end"))
}

// ------------------------------------------------------------------ records

#[derive(Clone, Copy)]
struct Rec {
    id: [u64; 2],
    ing: i64,
    ts: i64,
    person: u64,
    name: u64,
    price: u32,
    cat: u8,
    ok: bool,
}

#[derive(Clone, Copy)]
struct Ev {
    person: u64,
    name: u64,
    ts: i64,
    price: u32,
    cat: u8,
    purch: bool,
}

#[derive(Clone, Copy)]
struct Click {
    person: u64,
    ts: i64,
    cid: u32,
    id: Bytes,
}

fn parse_events(chunk: Bytes, ctx: &Ctx) -> Vec<Vec<Rec>> {
    let mut out: Vec<Vec<Rec>> = (0..NSHARD).map(|_| Vec::new()).collect();
    let mut pos = 0;
    while pos < chunk.len() {
        let end = memchr(b'\n', &chunk[pos..]).map_or(chunk.len(), |i| pos + i);
        let line = &chunk[pos..end];
        pos = end + 1;
        let Some(f) = fields::<7>(line) else { continue };
        if f[0].is_empty() {
            continue;
        }
        let id = xxh3_128(f[0]);
        let ts = parse_ts(f[2]);
        let ing = parse_ts(f[3]);
        let mut rec = Rec {
            id: [id as u64, (id >> 64) as u64],
            ing: ing.unwrap_or(i64::MAX),
            ts: ts.unwrap_or(0),
            person: 0,
            name: 0,
            price: 0,
            cat: 0,
            ok: false,
        };
        let et = match f[4] {
            b"view" | b"cart" => 1,
            b"purchase" => 2,
            _ => 0,
        };
        let mut ok = et != 0 && !f[1].is_empty() && !f[5].is_empty();
        if ok && et == 2 {
            match parse_price(f[6]) {
                Some(p) => rec.price = p,
                None => ok = false,
            }
        }
        if ok {
            ok = match (ts, ing) {
                (Some(t), Some(i)) => t >= ctx.ws && t < ctx.we && i - t <= DAY_MS,
                _ => false,
            };
        }
        if ok {
            let (key, name) = ctx.person(f[1]);
            rec.person = key;
            rec.name = name;
            rec.cat = *ctx.products.get(f[5]).unwrap_or(&ctx.unknown);
            if et != 2 {
                rec.price = u32::MAX; // marks a non-purchase
            }
            rec.ok = true;
        }
        out[(rec.id[0] % NSHARD as u64) as usize].push(rec);
    }
    out
}

fn parse_clicks(chunk: Bytes, ctx: &Ctx) -> Vec<Vec<Click>> {
    let mut out: Vec<Vec<Click>> = (0..NPART).map(|_| Vec::new()).collect();
    let mut pos = 0;
    while pos < chunk.len() {
        let end = memchr(b'\n', &chunk[pos..]).map_or(chunk.len(), |i| pos + i);
        let line = &chunk[pos..end];
        pos = end + 1;
        let Some(f) = fields::<4>(line) else { continue };
        if f[0].is_empty() || f[1].is_empty() {
            continue;
        }
        let Some(&cid) = ctx.campaigns.get(f[2]) else { continue };
        let Some(ts) = parse_ts(f[3]) else { continue };
        if ts < ctx.ws - WEEK_MS || ts >= ctx.we {
            continue;
        }
        let (key, _) = ctx.person(f[1]);
        out[(key % NPART as u64) as usize].push(Click { person: key, ts, cid, id: f[0] });
    }
    out
}

// ------------------------------------------------------------------ links

fn build_links(files: &[PathBuf]) -> IdMap<(u64, u64)> {
    let ch = chunks(files);
    let pairs: Vec<Vec<(Bytes, Bytes)>> = ch
        .par_iter()
        .map(|c| {
            let mut v = vec![];
            let mut pos = 0;
            while pos < c.len() {
                let end = memchr(b'\n', &c[pos..]).map_or(c.len(), |i| pos + i);
                let line = &c[pos..end];
                pos = end + 1;
                if let Some(f) = fields::<2>(line) {
                    if !f[0].is_empty() && !f[1].is_empty() && f[0] != f[1] {
                        v.push((f[0], f[1]));
                    }
                }
            }
            v
        })
        .collect();
    let total: usize = pairs.iter().map(|v| v.len()).sum();
    let mut index: IdMap<u32> = IdMap::with_capacity_and_hasher(total + 16, Default::default());
    let mut names: Vec<Bytes> = Vec::with_capacity(total + 16);
    let mut hashes: Vec<u64> = Vec::with_capacity(total + 16);
    let mut parent: Vec<u32> = Vec::with_capacity(total + 16);
    let mut min_name: Vec<Bytes> = Vec::with_capacity(total + 16);

    fn find(parent: &mut [u32], mut x: u32) -> u32 {
        while parent[x as usize] != x {
            let p = parent[x as usize];
            parent[x as usize] = parent[p as usize];
            x = parent[x as usize];
        }
        x
    }

    for v in &pairs {
        for &(a, b) in v {
            let mut ids = [0u32; 2];
            for (k, s) in [a, b].into_iter().enumerate() {
                let h = xxh3_64(s);
                let next = names.len() as u32;
                let id = *index.entry(h).or_insert(next);
                if id == next {
                    names.push(s);
                    hashes.push(h);
                    parent.push(next);
                    min_name.push(s);
                }
                ids[k] = id;
            }
            let ra = find(&mut parent, ids[0]);
            let rb = find(&mut parent, ids[1]);
            if ra != rb {
                parent[rb as usize] = ra;
                if min_name[rb as usize] < min_name[ra as usize] {
                    min_name[ra as usize] = min_name[rb as usize];
                }
            }
        }
    }
    let mut linked: IdMap<(u64, u64)> = IdMap::with_capacity_and_hasher(names.len(), Default::default());
    // person key per root, computed once
    let mut root_key: Vec<u64> = vec![0; names.len()];
    for i in 0..names.len() {
        let r = find(&mut parent, i as u32) as usize;
        if root_key[r] == 0 {
            root_key[r] = xxh3_64(min_name[r]);
        }
        linked.insert(hashes[i], (root_key[r], pack(min_name[r])));
    }
    linked
}

// ------------------------------------------------------------------ per-person processing

struct Sess {
    start: i64,
    di: usize,
    rev: u64,
    purch: u32,
}

struct UserRow {
    name: u64,
    day: i32,
    sessions: u32,
    events: u64,
    rev: u64,
    top: u8,
}

struct PartOut {
    users: Vec<UserRow>,
    credit: Vec<u64>, // [cid][day] x (sessions, purchasing, revenue)
    assist: Vec<u64>, // [cid][day]
}

struct Dims {
    min_day: i64,
    ndays: usize,
    ncid: usize,
    ncat: usize,
}

fn process_part(mut evs: Vec<Ev>, mut clicks: Vec<Click>, d: &Dims) -> PartOut {
    evs.sort_unstable_by_key(|e| (e.person, e.ts));
    clicks.sort_unstable_by(|a, b| (a.person, a.ts).cmp(&(b.person, b.ts)).then_with(|| a.id.cmp(b.id)));
    let mut out = PartOut {
        users: vec![],
        credit: vec![0; d.ncid * d.ndays * 3],
        assist: vec![0; d.ncid * d.ndays],
    };
    let mut sess: Vec<Sess> = vec![];
    let mut catsum = vec![0u64; d.ncat];
    let mut touched: Vec<u8> = vec![];
    let mut cts: Vec<(u32, i64)> = vec![];
    let mut ranges: Vec<(usize, usize, usize)> = vec![];
    let mut ci = 0usize;
    let mut i = 0usize;
    while i < evs.len() {
        let person = evs[i].person;
        let name = evs[i].name;
        let mut j = i;
        while j < evs.len() && evs[j].person == person {
            j += 1;
        }
        sess.clear();
        let mut cur_day = i64::MIN;
        let (mut d_sess, mut d_events, mut d_rev) = (0u32, 0u64, 0u64);
        let mut prev_ts = 0i64;

        macro_rules! flush {
            () => {
                if d_sess > 0 {
                    let mut top = NO_CAT;
                    let mut best = 0u64;
                    for &c in &touched {
                        let s = catsum[c as usize];
                        if s > best || (s == best && c < top) {
                            best = s;
                            top = c;
                        }
                        catsum[c as usize] = 0;
                    }
                    touched.clear();
                    out.users.push(UserRow {
                        name,
                        day: cur_day as i32,
                        sessions: d_sess,
                        events: d_events,
                        rev: d_rev,
                        top,
                    });
                    d_sess = 0;
                    d_events = 0;
                    d_rev = 0;
                }
            };
        }

        for e in &evs[i..j] {
            if sess.is_empty() || e.ts - prev_ts > GAP_MS {
                let day = e.ts.div_euclid(DAY_MS);
                if day != cur_day {
                    flush!();
                    cur_day = day;
                }
                d_sess += 1;
                sess.push(Sess { start: e.ts, di: (day - d.min_day) as usize, rev: 0, purch: 0 });
            }
            prev_ts = e.ts;
            d_events += 1;
            if e.purch {
                let p = e.price as u64;
                d_rev += p;
                let s = sess.last_mut().unwrap();
                s.rev += p;
                s.purch += 1;
                if catsum[e.cat as usize] == 0 {
                    touched.push(e.cat);
                }
                catsum[e.cat as usize] += p;
            }
        }
        flush!();

        // clicks of this person
        while ci < clicks.len() && clicks[ci].person < person {
            ci += 1;
        }
        let mut cj = ci;
        while cj < clicks.len() && clicks[cj].person == person {
            cj += 1;
        }
        let cl = &clicks[ci..cj];
        ci = cj;

        // credit
        let mut q = 0usize;
        for s in &sess {
            while q < cl.len() && cl[q].ts <= s.start {
                q += 1;
            }
            let mut cid = 0usize;
            if q > 0 {
                let t = cl[q - 1].ts;
                if s.start - t <= WEEK_MS {
                    let mut g = q - 1;
                    while g > 0 && cl[g - 1].ts == t {
                        g -= 1;
                    }
                    cid = cl[g].cid as usize;
                }
            }
            let k = (cid * d.ndays + s.di) * 3;
            out.credit[k] += 1;
            out.credit[k + 1] += (s.purch > 0) as u64;
            out.credit[k + 2] += s.rev;
        }

        // assists
        if !cl.is_empty() {
            ranges.clear();
            let mut a = 0usize;
            while a < sess.len() {
                let mut b = a;
                while b < sess.len() && sess[b].di == sess[a].di {
                    b += 1;
                }
                ranges.push((sess[a].di, a, b));
                a = b;
            }
            cts.clear();
            cts.extend(cl.iter().map(|c| (c.cid, c.ts)));
            cts.sort_unstable();
            let mut k = 0;
            while k < cts.len() {
                let cid = cts[k].0;
                let mut isl_start = cts[k].1;
                let mut last = isl_start;
                k += 1;
                loop {
                    let more = k < cts.len() && cts[k].0 == cid;
                    if more && cts[k].1 - last <= WEEK_MS {
                        last = cts[k].1;
                        k += 1;
                        continue;
                    }
                    // close island [isl_start, last + WEEK]
                    let hi_t = last + WEEK_MS;
                    let lo = sess.partition_point(|s| s.start < isl_start);
                    let hi = sess.partition_point(|s| s.start <= hi_t);
                    if hi > lo {
                        for &(di, s0, s1) in &ranges {
                            let n = hi.min(s1).saturating_sub(lo.max(s0));
                            if n > 0 {
                                out.assist[cid as usize * d.ndays + di] += n as u64;
                            }
                        }
                    }
                    if more {
                        isl_start = cts[k].1;
                        last = isl_start;
                        k += 1;
                        continue;
                    }
                    break;
                }
            }
        }
        i = j;
    }
    out
}

// ------------------------------------------------------------------ parquet output

fn props() -> Arc<WriterProperties> {
    Arc::new(
        WriterProperties::builder()
            .set_compression(Compression::UNCOMPRESSED)
            .set_dictionary_enabled(false)
            .set_statistics_enabled(EnabledStatistics::None)
            .build(),
    )
}

fn ba(s: &[u8]) -> ByteArray {
    ByteArray::from(s.to_vec())
}

fn write_users(path: &Path, rows: &[UserRow], cats: &[Bytes]) {
    let schema = Arc::new(
        parse_message_type(
            "message m { REQUIRED BYTE_ARRAY user_id (UTF8); REQUIRED INT32 day (DATE); \
             REQUIRED INT64 sessions; REQUIRED INT64 events; REQUIRED INT64 revenue_cents; \
             OPTIONAL BYTE_ARRAY top_category (UTF8); }",
        )
        .unwrap(),
    );
    let mut w = SerializedFileWriter::new(File::create(path).unwrap(), schema, props()).unwrap();
    for rg in rows.chunks(1 << 20) {
        let mut g = w.next_row_group().unwrap();
        let v: Vec<ByteArray> = rg.iter().map(|r| ba(unpack(r.name))).collect();
        let mut c = g.next_column().unwrap().unwrap();
        c.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
        c.close().unwrap();
        let v: Vec<i32> = rg.iter().map(|r| r.day).collect();
        let mut c = g.next_column().unwrap().unwrap();
        c.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
        c.close().unwrap();
        for f in 0..3 {
            let v: Vec<i64> = rg
                .iter()
                .map(|r| match f {
                    0 => r.sessions as i64,
                    1 => r.events as i64,
                    _ => r.rev as i64,
                })
                .collect();
            let mut c = g.next_column().unwrap().unwrap();
            c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
            c.close().unwrap();
        }
        let defs: Vec<i16> = rg.iter().map(|r| (r.top != NO_CAT) as i16).collect();
        let v: Vec<ByteArray> = rg.iter().filter(|r| r.top != NO_CAT).map(|r| ba(cats[r.top as usize])).collect();
        let mut c = g.next_column().unwrap().unwrap();
        c.typed::<ByteArrayType>().write_batch(&v, Some(&defs), None).unwrap();
        c.close().unwrap();
        g.close().unwrap();
    }
    w.close().unwrap();
}

struct CampRow {
    id: Vec<u8>,
    channel: Vec<u8>,
    day: i32,
    vals: [i64; 4],
}

fn write_campaigns(path: &Path, rows: &[CampRow]) {
    let schema = Arc::new(
        parse_message_type(
            "message m { REQUIRED BYTE_ARRAY campaign_id (UTF8); REQUIRED BYTE_ARRAY channel (UTF8); \
             REQUIRED INT32 day (DATE); REQUIRED INT64 sessions; REQUIRED INT64 purchasing_sessions; \
             REQUIRED INT64 revenue_cents; REQUIRED INT64 assisted_sessions; }",
        )
        .unwrap(),
    );
    let mut w = SerializedFileWriter::new(File::create(path).unwrap(), schema, props()).unwrap();
    for rg in rows.chunks(1 << 20) {
        let mut g = w.next_row_group().unwrap();
        for f in 0..2 {
            let v: Vec<ByteArray> = rg.iter().map(|r| ba(if f == 0 { &r.id } else { &r.channel })).collect();
            let mut c = g.next_column().unwrap().unwrap();
            c.typed::<ByteArrayType>().write_batch(&v, None, None).unwrap();
            c.close().unwrap();
        }
        let v: Vec<i32> = rg.iter().map(|r| r.day).collect();
        let mut c = g.next_column().unwrap().unwrap();
        c.typed::<Int32Type>().write_batch(&v, None, None).unwrap();
        c.close().unwrap();
        for f in 0..4 {
            let v: Vec<i64> = rg.iter().map(|r| r.vals[f]).collect();
            let mut c = g.next_column().unwrap().unwrap();
            c.typed::<Int64Type>().write_batch(&v, None, None).unwrap();
            c.close().unwrap();
        }
        g.close().unwrap();
    }
    w.close().unwrap();
}

// ------------------------------------------------------------------ main

fn main() {
    let t = Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let ncpu: usize = std::env::var("NCPU").ok().and_then(|v| v.parse().ok()).unwrap_or(0);
    rayon::ThreadPoolBuilder::new().num_threads(ncpu).build_global().unwrap();

    let (ws, we) = read_manifest(&data);

    // reference data
    let mut cat_names: Vec<Bytes> = vec![];
    let prod_rows: Vec<[Bytes; 2]> = {
        let d = load(&data.join("products.csv"));
        let mut v = vec![];
        let mut pos = d.iter().position(|&b| b == b'\n').map_or(d.len(), |i| i + 1);
        while pos < d.len() {
            let end = memchr(b'\n', &d[pos..]).map_or(d.len(), |i| pos + i);
            if let Some(f) = fields::<2>(&d[pos..end]) {
                if !f[0].is_empty() {
                    v.push(f);
                }
            }
            pos = end + 1;
        }
        v
    };
    for r in &prod_rows {
        if !r[1].is_empty() {
            cat_names.push(r[1]);
        }
    }
    cat_names.push(b"unknown");
    cat_names.sort_unstable();
    cat_names.dedup();
    let mut products: HashMap<Bytes, u8> = HashMap::new();
    for r in &prod_rows {
        if let Ok(c) = cat_names.binary_search(&r[1]) {
            products.insert(r[0], c as u8);
        }
    }
    let unknown = cat_names.binary_search(&(b"unknown" as Bytes)).unwrap() as u8;

    let mut camp_names: Vec<[Bytes; 2]> = vec![[b"organic", b"organic"]]; // cid 0
    let mut campaigns: HashMap<Bytes, u32> = HashMap::new();
    {
        let d = load(&data.join("campaigns.csv"));
        let mut pos = d.iter().position(|&b| b == b'\n').map_or(d.len(), |i| i + 1);
        while pos < d.len() {
            let end = memchr(b'\n', &d[pos..]).map_or(d.len(), |i| pos + i);
            if let Some(f) = fields::<2>(&d[pos..end]) {
                if !f[0].is_empty() && !campaigns.contains_key(f[0]) {
                    campaigns.insert(f[0], camp_names.len() as u32);
                    camp_names.push(f);
                }
            }
            pos = end + 1;
        }
    }
    log(&t, "reference data");

    let linked = build_links(&csv_files(&data.join("links")));
    log(&t, &format!("links: {} linked ids", linked.len()));
    let ctx = Ctx { ws, we, linked, products, unknown, campaigns };

    // parse events
    let ev_chunks = chunks(&csv_files(&data.join("events")));
    let parsed: Vec<Vec<Vec<Rec>>> = ev_chunks.par_iter().map(|c| parse_events(c, &ctx)).collect();
    log(&t, "events parsed");

    // dedup per shard, re-shard by person
    let deduped: Vec<Vec<Vec<Ev>>> = (0..NSHARD)
        .into_par_iter()
        .map(|s| {
            let n: usize = parsed.iter().map(|c| c[s].len()).sum();
            let mut all: Vec<Rec> = Vec::with_capacity(n);
            for c in &parsed {
                all.extend_from_slice(&c[s]);
            }
            all.sort_unstable_by_key(|r| (r.id, r.ing));
            let mut parts: Vec<Vec<Ev>> = (0..NPART).map(|_| Vec::new()).collect();
            let mut prev: Option<[u64; 2]> = None;
            for r in &all {
                if prev == Some(r.id) {
                    continue;
                }
                prev = Some(r.id);
                if r.ok {
                    parts[(r.person % NPART as u64) as usize].push(Ev {
                        person: r.person,
                        name: r.name,
                        ts: r.ts,
                        price: if r.price == u32::MAX { 0 } else { r.price },
                        cat: r.cat,
                        purch: r.price != u32::MAX,
                    });
                }
            }
            parts
        })
        .collect();
    std::mem::forget(parsed); // freeing thousands of small vectors costs more than it saves
    log(&t, "events deduped");

    // parse clicks
    let ck_chunks = chunks(&csv_files(&data.join("clicks")));
    let click_parts: Vec<Vec<Vec<Click>>> = ck_chunks.par_iter().map(|c| parse_clicks(c, &ctx)).collect();
    log(&t, "clicks parsed");

    // per person shard
    let min_day = ws.div_euclid(DAY_MS);
    let dims = Dims {
        min_day,
        ndays: ((we - 1).div_euclid(DAY_MS) - min_day + 1) as usize,
        ncid: camp_names.len(),
        ncat: cat_names.len(),
    };
    let parts: Vec<PartOut> = (0..NPART)
        .into_par_iter()
        .map(|p| {
            let n: usize = deduped.iter().map(|s| s[p].len()).sum();
            let mut evs = Vec::with_capacity(n);
            for s in &deduped {
                evs.extend_from_slice(&s[p]);
            }
            let m: usize = click_parts.iter().map(|s| s[p].len()).sum();
            let mut cks = Vec::with_capacity(m);
            for s in &click_parts {
                cks.extend_from_slice(&s[p]);
            }
            process_part(evs, cks, &dims)
        })
        .collect();
    std::mem::forget(deduped);
    std::mem::forget(click_parts);
    log(&t, "sessions, credit and assists done");

    // outputs
    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    fs::create_dir_all(&ud).unwrap();
    fs::create_dir_all(&cd).unwrap();
    parts.par_iter().enumerate().for_each(|(p, o)| {
        if !o.users.is_empty() || p == 0 {
            write_users(&ud.join(format!("part-{p}.parquet")), &o.users, &cat_names);
        }
    });
    let mut rows: Vec<CampRow> = vec![];
    let nd = dims.ndays;
    for cid in 0..dims.ncid {
        for di in 0..nd {
            let mut v = [0u64; 4];
            for o in &parts {
                let k = (cid * nd + di) * 3;
                v[0] += o.credit[k];
                v[1] += o.credit[k + 1];
                v[2] += o.credit[k + 2];
                v[3] += o.assist[cid * nd + di];
            }
            if v[0] > 0 || v[3] > 0 {
                rows.push(CampRow {
                    id: camp_names[cid][0].to_vec(),
                    channel: camp_names[cid][1].to_vec(),
                    day: (min_day + di as i64) as i32,
                    vals: [v[0] as i64, v[1] as i64, v[2] as i64, v[3] as i64],
                });
            }
        }
    }
    write_campaigns(&cd.join("part-0.parquet"), &rows);
    log(&t, "done");
}
