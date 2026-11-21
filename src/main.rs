//! Shopping Sessions: single-pass parallel pipeline.
//!
//! Reads $DATA_DIR, writes $OUTPUT_DIR/{user_daily,campaign_daily}/*.parquet.
//! Phases: links -> union-find; events parsed in parallel chunks and sharded by event id for
//! dedup; surviving events are re-sharded by person, sorted, sessionized, credited and
//! assisted per person shard; each shard writes its own Parquet part.

use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering::{AcqRel, Acquire, Relaxed};
use std::sync::atomic::{AtomicU32, AtomicU64};
use std::sync::Mutex;
use std::time::Instant;

use hashbrown::HashMap;
use memchr::memchr;
use memmap2::Mmap;
use rayon::prelude::*;
use xxhash_rust::xxh3::{xxh3_128, xxh3_64};

const DAY_MS: i64 = 86_400_000;
const WEEK_MS: i64 = 7 * DAY_MS;
const GAP_MS: i64 = 1_800_000;
const NSHARD: usize = 256; // event-id shards (dedup)
const NPART: usize = 256; // person shards
const CHUNK: usize = 8 << 20; // events
const SMALL_CHUNK: usize = 1 << 20; // clicks and links: fewer, unevenly sized files
const NO_CAT: u8 = 255;

type Bytes = &'static [u8];

// ------------------------------------------------------------------ utilities

fn log(t: &Instant, msg: &str) {
    let anon = fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|st| st.lines().find(|l| l.starts_with("RssAnon")).map(|l| l.split_whitespace().nth(1).unwrap_or("?").to_string()))
        .unwrap_or_default();
    eprintln!("[{:7.2}s] {} (anon {} MB)", t.elapsed().as_secs_f64(), msg, anon.parse::<u64>().unwrap_or(0) / 1024);
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
fn chunks(files: &[PathBuf], size: usize) -> Vec<Bytes> {
    let mut out = vec![];
    for p in files {
        let data = load(p);
        let mut start = match memchr(b'\n', data) {
            Some(i) => i + 1,
            None => continue,
        };
        while start < data.len() {
            let mut end = (start + size).min(data.len());
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
    let mut quoted = false;
    for (i, &b) in line.iter().enumerate() {
        if b == b',' {
            if n >= N - 1 {
                return None;
            }
            out[n] = &line[s..i];
            n += 1;
            s = i + 1;
        } else if b == b'"' {
            quoted = true;
        }
    }
    if n != N - 1 {
        return None;
    }
    out[n] = &line[s..];
    if !quoted {
        return Some(out);
    }
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

/// Positions of every comma, newline and double quote in `buf`, in order.
fn index_delims(buf: &[u8], out: &mut Vec<u32>) {
    out.clear();
    let n = buf.len();
    let mut i = 0;
    #[cfg(target_arch = "x86_64")]
    unsafe {
        use std::arch::x86_64::*;
        let comma = _mm_set1_epi8(b',' as i8);
        let nl = _mm_set1_epi8(b'\n' as i8);
        let quote = _mm_set1_epi8(b'"' as i8);
        while i + 64 <= n {
            let mut mask = 0u64;
            for k in 0..4 {
                let v = _mm_loadu_si128(buf.as_ptr().add(i + 16 * k) as *const __m128i);
                let m = _mm_or_si128(
                    _mm_or_si128(_mm_cmpeq_epi8(v, comma), _mm_cmpeq_epi8(v, nl)),
                    _mm_cmpeq_epi8(v, quote),
                );
                mask |= (_mm_movemask_epi8(m) as u32 as u64) << (16 * k);
            }
            while mask != 0 {
                out.push((i + mask.trailing_zeros() as usize) as u32);
                mask &= mask - 1;
            }
            i += 64;
        }
    }
    while i < n {
        let b = buf[i];
        if b == b',' || b == b'\n' || b == b'"' {
            out.push(i as u32);
        }
        i += 1;
    }
}

/// A field with `qc` quotes: unchanged without quotes, unwrapped when one pair wraps it,
/// otherwise malformed.
#[inline(always)]
fn unquote(sub: Bytes, s: usize, e: usize, qc: u32) -> Option<Bytes> {
    if qc == 0 {
        Some(&sub[s..e])
    } else if qc == 2 && e - s >= 2 && sub[s] == b'"' && sub[e - 1] == b'"' {
        Some(&sub[s + 1..e - 1])
    } else {
        None
    }
}

/// Calls `f` with the N fields of every well-formed line of `chunk` (a piece that ends on a
/// line boundary), split from a vectorized index of the delimiters; malformed lines are skipped.
fn for_each_line<const N: usize>(chunk: Bytes, mut f: impl FnMut([Bytes; N])) {
    const SUB: usize = 64 << 10;
    let mut idx: Vec<u32> = Vec::with_capacity(SUB / 8);
    let mut a = 0;
    while a < chunk.len() {
        let mut b = (a + SUB).min(chunk.len());
        if b < chunk.len() {
            b = memchr(b'\n', &chunk[b..]).map_or(chunk.len(), |i| b + i + 1);
        }
        let sub = &chunk[a..b];
        a = b;
        index_delims(sub, &mut idx);
        let mut ls = 0usize;
        let mut fs = 0usize;
        let mut nf = 0usize;
        let mut qc = 0u32;
        let mut bad = false;
        let mut arr: [Bytes; N] = [&sub[0..0]; N];
        for &p in &idx {
            let p = p as usize;
            match sub[p] {
                b'"' => qc += 1,
                b',' => {
                    if nf < N - 1 {
                        match unquote(sub, fs, p, qc) {
                            Some(v) => arr[nf] = v,
                            None => bad = true,
                        }
                    }
                    nf += 1;
                    fs = p + 1;
                    qc = 0;
                }
                _ => {
                    if nf == N - 1 && !bad {
                        if let Some(v) = unquote(sub, fs, p, qc) {
                            arr[nf] = v;
                            f(arr);
                        }
                    }
                    ls = p + 1;
                    fs = ls;
                    nf = 0;
                    qc = 0;
                    bad = false;
                }
            }
        }
        if ls < sub.len() {
            if let Some(v) = fields::<N>(&sub[ls..]) {
                f(v);
            }
        }
    }
}

#[inline]
fn pack(s: Bytes) -> u64 {
    (s.as_ptr() as u64) | ((s.len() as u64) << 48)
}
#[inline]
fn unpack(v: u64) -> Bytes {
    unsafe { std::slice::from_raw_parts((v & ((1u64 << 48) - 1)) as *const u8, (v >> 48) as usize) }
}

const fn days_from_civil(y: i64, m: i64, d: i64) -> i64 {
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y - era * 400;
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    era * 146_097 + doe - 719_468
}

const TABLE_Y0: i64 = 1970;
const TABLE_YEARS: usize = 131;

/// Day number of the first of every month from 1970 to 2100.
static MONTH_START: [i64; TABLE_YEARS * 12] = {
    let mut t = [0i64; TABLE_YEARS * 12];
    let mut i = 0;
    while i < TABLE_YEARS * 12 {
        t[i] = days_from_civil(TABLE_Y0 + (i / 12) as i64, (i % 12) as i64 + 1, 1);
        i += 1;
    }
    t
};

#[inline]
fn days_of(y: i64, m: i64, d: i64) -> i64 {
    let k = (y - TABLE_Y0) * 12 + m - 1;
    if (0..(TABLE_YEARS * 12) as i64).contains(&k) {
        MONTH_START[k as usize] + d - 1
    } else {
        days_from_civil(y, m, d)
    }
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
#[inline]
fn parse_ts(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if (n == 20 || n == 25) && s[4] == b'-' {
        return parse_iso(s);
    }
    if n == 0 {
        return None;
    }
    if n <= 18 {
        // cannot overflow
        let mut v: i64 = 0;
        for &b in s {
            let d = b.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            v = v * 10 + d as i64;
        }
        return Some(v);
    }
    if !s.iter().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let mut v: i64 = 0;
    for &b in s {
        v = v.checked_mul(10)?.checked_add((b - b'0') as i64)?;
    }
    Some(v)
}

/// `YYYY-MM-DDTHH:MM:SSZ` or `YYYY-MM-DDTHH:MM:SS+HH:MM` (length 20 or 25, `-` at 4).
fn parse_iso(s: &[u8]) -> Option<i64> {
    Some(iso_date(s)? * DAY_MS + iso_time(s)?)
}

/// Day number of the `YYYY-MM-DD` part (bytes 0..10), None when it is not a real date.
fn iso_date(s: &[u8]) -> Option<i64> {
    if s[7] != b'-' {
        return None;
    }
    let mut d = [0i64; 8];
    let mut bad = false;
    for (k, &p) in [0, 1, 2, 3, 5, 6, 8, 9].iter().enumerate() {
        let x = s[p].wrapping_sub(b'0');
        bad |= x > 9;
        d[k] = x as i64;
    }
    if bad {
        return None;
    }
    let y = d[0] * 1000 + d[1] * 100 + d[2] * 10 + d[3];
    let mo = d[4] * 10 + d[5];
    let dd = d[6] * 10 + d[7];
    if !(1..=12).contains(&mo) || dd < 1 {
        return None;
    }
    let leap = (y % 4 == 0 && y % 100 != 0) || y % 400 == 0;
    let dim = match mo {
        2 => 28 + leap as i64,
        4 | 6 | 9 | 11 => 30,
        _ => 31,
    };
    if dd > dim {
        return None;
    }
    Some(days_of(y, mo, dd))
}

/// Ms since midnight UTC of the `THH:MM:SS` part plus its `Z` or `+HH:MM` offset.
#[inline]
fn iso_time(s: &[u8]) -> Option<i64> {
    let n = s.len();
    if s[10] != b'T' || s[13] != b':' || s[16] != b':' {
        return None;
    }
    let h = dig2(s, 11)?;
    let mi = dig2(s, 14)?;
    let se = dig2(s, 17)?;
    if h > 23 || mi > 59 || se > 59 {
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
    Some(h * 3_600_000 + mi * 60_000 + se * 1000 - off)
}

/// `parse_ts` with a small exact cache of ISO dates: a dataset spans only a few weeks.
struct TsParser {
    k1: [u64; 64],
    k2: [u16; 64],
    day: [i64; 64], // i64::MIN: not a date
}

impl TsParser {
    fn new() -> TsParser {
        TsParser { k1: [0; 64], k2: [0; 64], day: [i64::MIN; 64] }
    }

    #[inline]
    fn parse(&mut self, s: &[u8]) -> Option<i64> {
        let n = s.len();
        if !((n == 20 || n == 25) && s[4] == b'-') {
            return parse_ts(s);
        }
        let a = u64::from_le_bytes(s[0..8].try_into().unwrap());
        let b = u16::from_le_bytes(s[8..10].try_into().unwrap());
        let i = ((a ^ (b as u64) << 48).wrapping_mul(0x9e37_79b9_7f4a_7c15) >> 58) as usize;
        let day = if self.k1[i] == a && self.k2[i] == b {
            self.day[i]
        } else {
            let d = iso_date(s).unwrap_or(i64::MIN);
            (self.k1[i], self.k2[i], self.day[i]) = (a, b, d);
            d
        };
        if day == i64::MIN {
            return None;
        }
        Some(day * DAY_MS + iso_time(s)?)
    }
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

/// Product id hash -> category code. Open addressing with the 64-bit hash as the whole key:
/// small enough to stay in cache, and no string compare on a lookup.
struct StrTable<V> {
    mask: usize,
    keys: Vec<u64>,
    cats: Vec<V>,
}

/// Product id -> category code. Ids of the form <prefix letter><number without leading zeros>
/// (`p1`, `p50000`) index a dense array that stays in cache; any other id uses the hash table.
/// A canonical lookup id can only equal a canonical product id, so the array alone answers it.
struct ProdTable {
    prefix: u8,
    dense: Vec<u8>, // DENSE_NONE: not in the array
    table: StrTable<u8>,
    canonical_in_table: bool,
}

const DENSE_NONE: u8 = 255;
const DENSE_CAP: usize = 1 << 22;

impl ProdTable {
    fn new(n: usize, prefix: u8) -> ProdTable {
        ProdTable { prefix, dense: vec![], table: StrTable::new(n), canonical_in_table: false }
    }

    /// The number of a canonical id, if `id` is one.
    #[inline]
    fn number(&self, id: &[u8]) -> Option<usize> {
        if id.len() < 2 || id.len() > 8 || id[0] != self.prefix || id[1] == b'0' {
            return None;
        }
        let mut v = 0usize;
        for &b in &id[1..] {
            let d = b.wrapping_sub(b'0');
            if d > 9 {
                return None;
            }
            v = v * 10 + d as usize;
        }
        Some(v)
    }

    /// Insert or overwrite (a later row for the same product wins).
    fn insert(&mut self, id: &[u8], cat: u8) {
        match self.number(id) {
            Some(v) if v < DENSE_CAP && cat != DENSE_NONE => {
                if v >= self.dense.len() {
                    self.dense.resize(v + 1, DENSE_NONE);
                }
                self.dense[v] = cat;
            }
            Some(_) => {
                self.canonical_in_table = true;
                self.table.insert(id, cat);
            }
            None => self.table.insert(id, cat),
        }
    }

    #[inline]
    fn get(&self, id: &[u8]) -> Option<u8> {
        if let Some(v) = self.number(id) {
            let c = self.dense.get(v).copied().unwrap_or(DENSE_NONE);
            if c != DENSE_NONE || !self.canonical_in_table {
                return (c != DENSE_NONE).then_some(c);
            }
        }
        self.table.get(id)
    }
}

impl<V: Copy + Default> StrTable<V> {
    fn new(n: usize) -> StrTable<V> {
        let cap = (n * 2).next_power_of_two().max(16);
        StrTable { mask: cap - 1, keys: vec![0; cap], cats: vec![V::default(); cap] }
    }

    #[inline]
    fn key(id: &[u8]) -> u64 {
        xxh3_64(id) | 1 // 0 marks an empty slot
    }

    /// Insert or overwrite (a later row for the same product wins).
    fn insert(&mut self, id: &[u8], cat: V) {
        let h = Self::key(id);
        let mut i = h as usize & self.mask;
        while self.keys[i] != 0 && self.keys[i] != h {
            i = (i + 1) & self.mask;
        }
        self.keys[i] = h;
        self.cats[i] = cat;
    }

    #[inline]
    fn get(&self, id: &[u8]) -> Option<V> {
        let h = Self::key(id);
        let mut i = h as usize & self.mask;
        loop {
            let k = self.keys[i];
            if k == h {
                return Some(self.cats[i]);
            }
            if k == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }
}

struct Ctx {
    ws: i64,
    we: i64,
    base: i64,
    products: ProdTable,
    unknown: u8,
    campaigns: StrTable<u32>,
}

/// Linked id hash -> (person key, person name). Open addressing over a power-of-two array of
/// 3-word slots (hash, person key, packed name), so one lookup touches one cache line and can be
/// prefetched; hash 0 marks an empty slot and is never stored. A cache-sized bit filter answers
/// most lookups of unlinked ids without touching the table.
struct LinkTable {
    mask: usize,
    shift: u32,
    slots: Vec<u64>,
    filter: Vec<u64>,
}

const FILTER_BITS: u32 = 24; // 2 MB

impl LinkTable {
    fn with_capacity(n: usize) -> LinkTable {
        let cap = (n + n / 2).next_power_of_two().max(16);
        LinkTable {
            mask: cap - 1,
            shift: 64 - cap.trailing_zeros(),
            slots: vec![0; 3 * cap],
            filter: vec![0; 1 << (FILTER_BITS - 6)],
        }
    }

    #[inline]
    fn filter_bit(h: u64) -> (usize, u64) {
        let b = (h & ((1 << FILTER_BITS) - 1)) as usize; // the table slot uses the high bits
        (b >> 6, 1u64 << (b & 63))
    }

    /// Home slot: the high bits, so inserting in hash order walks the table in order.
    #[inline]
    fn slot(&self, h: u64) -> usize {
        (h >> self.shift) as usize
    }

    #[inline]
    fn maybe(&self, h: u64) -> bool {
        let (w, m) = Self::filter_bit(h);
        unsafe { *self.filter.get_unchecked(w) & m != 0 }
    }

    fn len(&self) -> usize {
        self.slots.iter().step_by(3).filter(|&&k| k != 0).count()
    }

    #[inline]
    fn get(&self, h: u64) -> Option<(u64, u64)> {
        if h == 0 || !self.maybe(h) {
            return None;
        }
        let mut i = self.slot(h);
        loop {
            // SAFETY: i <= mask, and slots holds 3 * (mask + 1) words.
            let k = unsafe { *self.slots.get_unchecked(3 * i) };
            if k == h {
                return Some(unsafe { (*self.slots.get_unchecked(3 * i + 1), *self.slots.get_unchecked(3 * i + 2)) });
            }
            if k == 0 {
                return None;
            }
            i = (i + 1) & self.mask;
        }
    }

    /// First stage of a lookup ahead of time: bring in the filter word.
    #[inline]
    fn prefetch_filter(&self, h: u64) {
        #[cfg(target_arch = "x86_64")]
        unsafe {
            use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
            let (w, _) = Self::filter_bit(h);
            _mm_prefetch(self.filter.as_ptr().add(w) as *const i8, _MM_HINT_T0);
        }
    }

    /// Second stage (the filter word is cached by now): bring in the table slot when needed.
    #[inline]
    fn prefetch(&self, h: u64) {
        #[cfg(target_arch = "x86_64")]
        if self.maybe(h) {
            unsafe {
                use std::arch::x86_64::{_mm_prefetch, _MM_HINT_T0};
                let p = self.slots.as_ptr().add(3 * self.slot(h));
                _mm_prefetch(p as *const i8, _MM_HINT_T0);
                _mm_prefetch(p.add(2) as *const i8, _MM_HINT_T0); // a slot may cross a line
            }
        }
    }
}

/// Person key and name for a user id hash; `fallback` is the packed name of an unlinked id.
#[inline]
fn person(linked: &LinkTable, h: u64, fallback: u64) -> (u64, u64) {
    linked.get(h).unwrap_or((h, fallback))
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

/// Dedup key of one event copy. A valid copy lives at `outs[thread].evs[part][idx]`; `idx` is
/// u32::MAX for an invalid copy (it can still be the kept copy, and then the event is dropped).
#[derive(Clone, Copy, Default)]
struct Key {
    id0: u64,
    ing: i64,
    idx: u32,
    id1: u16,
    part: u8,
    thread: u8,
}

/// A valid event. Times are ms offsets from `Ctx::base` (window_start - 7 days), so event and
/// click times compare directly; `pc` is price_cents << 8 | category (price 0: not a purchase).
#[derive(Clone, Copy, Default)]
struct Ev {
    person: u64,
    name: u64,
    ts: u32,
    pc: u32,
}

impl Ev {
    #[inline]
    fn price(&self) -> u64 {
        (self.pc >> 8) as u64
    }
    #[inline]
    fn cat(&self) -> u8 {
        self.pc as u8
    }
    #[inline]
    fn purch(&self) -> bool {
        self.pc >= 256
    }
}

/// A valid click; `id` is the packed click id, `ts` an offset from `Ctx::base`.
#[derive(Clone, Copy, Default)]
struct Click {
    person: u64,
    id: u64,
    ts: u32,
    cid: u32,
}

/// Everything one worker thread parsed: valid events by person shard and dedup keys of all
/// copies by event-id shard.
struct ThreadOut {
    evs: Vec<Vec<Ev>>,
    keys: Vec<Vec<Key>>,
}

impl ThreadOut {
    /// `per_thread`: expected events parsed by one thread.
    fn new(per_thread: usize) -> ThreadOut {
        ThreadOut {
            evs: (0..NPART).map(|_| Vec::with_capacity(per_thread / NPART + 64)).collect(),
            keys: (0..NSHARD).map(|_| Vec::with_capacity(per_thread * 9 / 8 / NSHARD + 64)).collect(),
        }
    }
}

/// Parsed events wait here while their link-table lookups are prefetched; the oldest is then
/// resolved and written straight to its person shard.
struct Pipe {
    ring: [(Ev, Key); Pipe::N],
    head: usize,
    tail: usize,
}

impl Pipe {
    const N: usize = 32;
    const AHEAD: usize = 16;

    fn new() -> Pipe {
        Pipe { ring: [(Ev::default(), Key::default()); Pipe::N], head: 0, tail: 0 }
    }

    #[inline]
    fn push(&mut self, ev: Ev, key: Key, linked: &LinkTable, out: &mut ThreadOut, tid: u8) {
        linked.prefetch_filter(ev.person);
        if self.head - self.tail == Self::N {
            self.pop(linked, out, tid);
        }
        self.ring[self.head % Self::N] = (ev, key);
        self.head += 1;
        if self.head - self.tail > Self::AHEAD {
            linked.prefetch(self.ring[(self.head - Self::AHEAD) % Self::N].0.person);
        }
    }

    #[inline]
    fn pop(&mut self, linked: &LinkTable, out: &mut ThreadOut, tid: u8) {
        let (mut ev, mut key) = self.ring[self.tail % Self::N];
        self.tail += 1;
        (ev.person, ev.name) = person(linked, ev.person, ev.name);
        let p = (ev.person % NPART as u64) as usize;
        let v = &mut out.evs[p];
        key.idx = v.len() as u32;
        key.part = p as u8;
        key.thread = tid;
        v.push(ev);
        out.keys[(key.id0 % NSHARD as u64) as usize].push(key);
    }

    fn drain(&mut self, linked: &LinkTable, out: &mut ThreadOut, tid: u8) {
        while self.tail < self.head {
            self.pop(linked, out, tid);
        }
    }
}

#[inline(never)]
fn parse_events(chunk: Bytes, ctx: &Ctx, linked: &LinkTable, out: &mut ThreadOut, tid: u8) {
    let mut pipe = Pipe::new();
    let mut tsp = TsParser::new();
    for_each_line::<7>(chunk, |f| {
        if f[0].is_empty() {
            return;
        }
        let id = xxh3_128(f[0]);
        let ts = tsp.parse(f[2]);
        let ing = tsp.parse(f[3]);
        let key = Key {
            id0: id as u64,
            ing: ing.unwrap_or(i64::MAX),
            idx: u32::MAX,
            id1: (id >> 64) as u16,
            part: 0,
            thread: tid,
        };
        let et = match f[4] {
            b"view" | b"cart" => 1,
            b"purchase" => 2,
            _ => 0,
        };
        let mut ok = et != 0 && !f[1].is_empty() && !f[5].is_empty();
        let mut price = 0;
        if ok && et == 2 {
            match parse_price(f[6]) {
                Some(p) => price = p,
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
            let cat = ctx.products.get(f[5]).unwrap_or(ctx.unknown);
            let ev = Ev {
                person: xxh3_64(f[1]), // the user id hash until the pipe resolves the person
                name: pack(f[1]),
                ts: (ts.unwrap() - ctx.base) as u32,
                pc: price << 8 | cat as u32,
            };
            pipe.push(ev, key, linked, out, tid);
        } else {
            out.keys[(key.id0 % NSHARD as u64) as usize].push(key);
        }
    });
    pipe.drain(linked, out, tid);
}

/// Clicks of one chunk grouped by person shard: shard `p` is `recs[off[p]..off[p + 1]]`.
struct PClicks {
    recs: Vec<Click>,
    off: [u32; NPART + 1],
}

impl PClicks {
    fn part(&self, p: usize) -> &[Click] {
        &self.recs[self.off[p] as usize..self.off[p + 1] as usize]
    }
}

fn parse_clicks(chunk: Bytes, ctx: &Ctx, linked: &LinkTable, flat: &mut Vec<Click>) -> PClicks {
    flat.clear();
    flat.reserve(chunk.len() / 48 + 16);
    let mut tsp = TsParser::new();
    for_each_line::<4>(chunk, |f| {
        if f[0].is_empty() || f[1].is_empty() {
            return;
        }
        let Some(cid) = ctx.campaigns.get(f[2]) else { return };
        let Some(ts) = tsp.parse(f[3]) else { return };
        if ts < ctx.ws - WEEK_MS || ts >= ctx.we {
            return;
        }
        flat.push(Click { person: xxh3_64(f[1]), id: pack(f[0]), ts: (ts - ctx.base) as u32, cid }); // user id hash for now
    });
    let n = flat.len();
    for i in 0..n {
        if i + 32 < n {
            linked.prefetch_filter(flat[i + 32].person);
        }
        if i + 16 < n {
            linked.prefetch(flat[i + 16].person);
        }
        flat[i].person = person(linked, flat[i].person, 0).0;
    }
    let mut off = [0u32; NPART + 1];
    for c in flat.iter() {
        off[(c.person % NPART as u64) as usize + 1] += 1;
    }
    for i in 0..NPART {
        off[i + 1] += off[i];
    }
    let mut next = off;
    let mut recs: Vec<Click> = Vec::with_capacity(flat.len());
    // SAFETY: every slot in 0..flat.len() is written exactly once by the counting sort below.
    unsafe { recs.set_len(flat.len()) };
    for c in flat.iter() {
        let sh = (c.person % NPART as u64) as usize;
        recs[next[sh] as usize] = *c;
        next[sh] += 1;
    }
    PClicks { recs, off }
}

// ------------------------------------------------------------------ links

/// A raw pointer that parallel workers write through at disjoint indices.
struct SyncPtr<T>(*mut T);
unsafe impl<T> Sync for SyncPtr<T> {}
unsafe impl<T> Send for SyncPtr<T> {}
impl<T> SyncPtr<T> {
    /// SAFETY: callers write each index from one worker only, inside the buffer.
    #[inline]
    unsafe fn write(&self, i: usize, v: T) {
        self.0.add(i).write(v);
    }
}

fn atomics(v: &mut [u64]) -> &[AtomicU64] {
    // SAFETY: AtomicU64 has the size and alignment of u64 on the supported targets.
    unsafe { &*(v as *mut [u64] as *const [AtomicU64]) }
}

/// Root of `x` in a union-find forest shared between threads (path halving by CAS).
#[inline]
fn uf_find(parent: &[AtomicU32], mut x: u32) -> u32 {
    loop {
        let p = parent[x as usize].load(Acquire);
        if p == x {
            return x;
        }
        let g = parent[p as usize].load(Acquire);
        if g != p {
            let _ = parent[x as usize].compare_exchange_weak(p, g, AcqRel, Relaxed);
        }
        x = p;
    }
}

/// Join the sets of `a` and `b`; the larger root index is hung under the smaller.
fn uf_union(parent: &[AtomicU32], a: u32, b: u32) {
    let (mut a, mut b) = (a, b);
    loop {
        a = uf_find(parent, a);
        b = uf_find(parent, b);
        if a == b {
            return;
        }
        if a < b {
            std::mem::swap(&mut a, &mut b);
        }
        if parent[a as usize].compare_exchange(a, b, AcqRel, Relaxed).is_ok() {
            return;
        }
    }
}

fn build_links(files: &[PathBuf]) -> LinkTable {
    let ch = chunks(files, SMALL_CHUNK);
    // (hash, packed name) of both ends of every link, per chunk
    let pairs: Vec<Vec<[(u64, u64); 2]>> = ch
        .par_iter()
        .map(|c| {
            let mut v = Vec::with_capacity(c.len() / 40 + 4);
            for_each_line::<2>(c, |f| {
                if !f[0].is_empty() && !f[1].is_empty() && f[0] != f[1] {
                    v.push([(xxh3_64(f[0]), pack(f[0])), (xxh3_64(f[1]), pack(f[1]))]);
                }
            });
            v
        })
        .collect();
    // global link number of each chunk's first link; endpoint slot = 2 * link + side
    let mut first: Vec<usize> = Vec::with_capacity(pairs.len());
    let mut acc = 0;
    for v in &pairs {
        first.push(acc);
        acc += v.len();
    }
    let name_of = |slot: u32| {
        let e = slot as usize / 2;
        let c = first.partition_point(|&o| o <= e) - 1;
        pairs[c][e - first[c]][slot as usize % 2].1
    };

    // dense ids: sort the (hash, endpoint) pairs; each distinct hash is one id
    let mut keys: Vec<(u64, u32)> = pairs
        .par_iter()
        .enumerate()
        .flat_map_iter(|(c, v)| {
            let f = first[c];
            v.iter().enumerate().flat_map(move |(i, e)| {
                let s = 2 * (f + i) as u32;
                [(e[0].0, s), (e[1].0, s + 1)]
            })
        })
        .collect();
    keys.par_sort_unstable_by_key(|k| k.0);
    let m = keys.len();
    const NB: usize = 256;
    let bs = m.div_ceil(NB).max(1);
    let is_new = |j: usize| j == 0 || keys[j].0 != keys[j - 1].0;
    let mut base: Vec<u32> = (0..NB)
        .into_par_iter()
        .map(|b| (b * bs..((b + 1) * bs).min(m)).filter(|&j| is_new(j)).count() as u32)
        .collect();
    let mut acc = 0;
    for x in base.iter_mut() {
        let c = *x;
        *x = acc;
        acc += c;
    }
    let n = acc as usize;
    let mut hashes: Vec<u64> = vec![0; n];
    let mut names: Vec<u64> = vec![0; n]; // packed
    let mut end_id: Vec<u32> = vec![0; m];
    {
        let (hp, np, ep_) = (SyncPtr(hashes.as_mut_ptr()), SyncPtr(names.as_mut_ptr()), SyncPtr(end_id.as_mut_ptr()));
        (0..NB).into_par_iter().for_each(|b| {
            let mut id = base[b] as i64 - 1;
            for j in b * bs..((b + 1) * bs).min(m) {
                let (h, slot) = keys[j];
                if is_new(j) {
                    id += 1;
                    // SAFETY: each id and each endpoint slot is written by exactly one block.
                    unsafe {
                        hp.write(id as usize, h);
                        np.write(id as usize, name_of(slot));
                    }
                }
                unsafe { ep_.write(slot as usize, id as u32) };
            }
        });
    }
    drop(keys); // freed memory is reused by the event parse

    // connected components, then the byte-wise smallest name of each
    let parent: Vec<AtomicU32> = (0..n as u32).into_par_iter().map(AtomicU32::new).collect();
    end_id.par_chunks_exact(2).for_each(|e| uf_union(&parent, e[0], e[1]));
    let root: Vec<u32> = (0..n as u32).into_par_iter().map(|i| uf_find(&parent, i)).collect();
    let best: Vec<AtomicU32> = (0..n).into_par_iter().map(|_| AtomicU32::new(u32::MAX)).collect();
    (0..n).into_par_iter().for_each(|i| {
        let r = &best[root[i] as usize];
        let mut cur = r.load(Relaxed);
        while cur == u32::MAX || unpack(names[i]) < unpack(names[cur as usize]) {
            match r.compare_exchange_weak(cur, i as u32, Relaxed, Relaxed) {
                Ok(_) => break,
                Err(c) => cur = c,
            }
        }
    });

    // the table, filled in parallel
    let mut linked = LinkTable::with_capacity(n);
    {
        let (mask, shift) = (linked.mask, linked.shift);
        let filter_a = atomics(&mut linked.filter);
        let vals = SyncPtr(linked.slots.as_mut_ptr());
        let keys_a = atomics(&mut linked.slots);
        (0..n).into_par_iter().for_each(|i| {
            let h = hashes[i];
            if h == 0 {
                return;
            }
            let (w, bit) = LinkTable::filter_bit(h);
            filter_a[w].fetch_or(bit, Relaxed);
            let nm = names[best[root[i] as usize].load(Relaxed) as usize];
            let mut j = (h >> shift) as usize;
            loop {
                match keys_a[3 * j].compare_exchange(0, h, Relaxed, Relaxed) {
                    Ok(_) => {
                        // SAFETY: the slot was claimed by this worker alone.
                        unsafe {
                            vals.write(3 * j + 1, xxh3_64(unpack(nm)));
                            vals.write(3 * j + 2, nm);
                        }
                        break;
                    }
                    Err(_) => j = (j + 1) & mask,
                }
            }
        });
    }
    drop(pairs);
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

/// Campaign totals of one worker thread, summed over the shards it processed.
#[derive(Default)]
struct CampAcc {
    credit: Vec<u64>, // [cid][day] x (sessions, purchasing, revenue)
    assist: Vec<u64>, // [cid][day]
}

struct Dims {
    base: i64,
    min_day: i64,
    ndays: usize,
    ncid: usize,
    ncat: usize,
}

/// Order of persons inside a person shard: 12 bits of the key (above the 8 that pick the shard)
/// first, so the shard can be bucketed in one pass and only small buckets sorted.
const NBUCKET: usize = 4096;

#[inline]
fn bucket_of(person: u64) -> usize {
    ((person >> 8) as usize) & (NBUCKET - 1)
}

#[inline]
fn person_order(person: u64) -> (usize, u64) {
    (bucket_of(person), person)
}

/// Copy the slices of `srcs` into `out` grouped by `bucket` (a counting sort); returns the
/// bucket offsets.
fn gather_bucketed<'a, T: Copy + 'a>(
    srcs: impl Iterator<Item = &'a [T]> + Clone,
    out: &mut Vec<T>,
    bucket: impl Fn(&T) -> usize,
) -> Vec<u32> {
    let mut off = vec![0u32; NBUCKET + 1];
    for sl in srcs.clone() {
        for e in sl {
            off[bucket(e) + 1] += 1;
        }
    }
    for i in 0..NBUCKET {
        off[i + 1] += off[i];
    }
    let n = off[NBUCKET] as usize;
    out.clear();
    out.reserve(n);
    // SAFETY: capacity is reserved and the counting pass writes every slot of 0..n once.
    unsafe { out.set_len(n) };
    let mut next = off.clone();
    for sl in srcs {
        for e in sl {
            let b = bucket(e);
            out[next[b] as usize] = *e;
            next[b] += 1;
        }
    }
    off
}

/// Surviving events of person shard `p`, ordered by (person_order, ts).
#[inline(never)]
fn gather_events(outs: &[ThreadOut], dead: &[Vec<Vec<AtomicU64>>], p: usize, evs: &mut Vec<Ev>) {
    let alive = |d: &[AtomicU64], r: usize| d[r >> 6].load(Relaxed) & (1 << (r & 63)) == 0;
    let mut off = vec![0u32; NBUCKET + 1];
    for (o, d) in outs.iter().zip(dead) {
        for (r, e) in o.evs[p].iter().enumerate() {
            if alive(&d[p], r) {
                off[bucket_of(e.person) + 1] += 1;
            }
        }
    }
    for i in 0..NBUCKET {
        off[i + 1] += off[i];
    }
    let n = off[NBUCKET] as usize;
    evs.clear();
    evs.reserve(n);
    // SAFETY: capacity is reserved and the pass below writes every slot of 0..n once.
    unsafe { evs.set_len(n) };
    let mut next = off.clone();
    for (o, d) in outs.iter().zip(dead) {
        for (r, &e) in o.evs[p].iter().enumerate() {
            if alive(&d[p], r) {
                let b = bucket_of(e.person);
                evs[next[b] as usize] = e;
                next[b] += 1;
            }
        }
    }
    for w in off.windows(2) {
        evs[w[0] as usize..w[1] as usize].sort_unstable_by_key(|e| (e.person, e.ts));
    }
}

/// Clicks of person shard `p`, ordered by (person_order, ts, click id).
#[inline(never)]
fn gather_clicks(click_parts: &[PClicks], p: usize, cks: &mut Vec<Click>) {
    let coff = gather_bucketed(click_parts.iter().map(|s| s.part(p)), cks, |c| bucket_of(c.person));
    for w in coff.windows(2) {
        cks[w[0] as usize..w[1] as usize]
            .sort_unstable_by(|a, b| (a.person, a.ts).cmp(&(b.person, b.ts)).then_with(|| unpack(a.id).cmp(unpack(b.id))));
    }
}

/// Sessions, credit and assists of one person shard. `evs` is ordered by (person_order, ts) and
/// `clicks` by (person_order, ts, click id).
#[inline(never)]
fn process_part(evs: &[Ev], clicks: &[Click], d: &Dims, out: &mut CampAcc) -> Vec<UserRow> {
    if out.credit.is_empty() {
        out.credit = vec![0; d.ncid * d.ndays * 3];
        out.assist = vec![0; d.ncid * d.ndays];
    }
    let mut users: Vec<UserRow> = vec![];
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
                    users.push(UserRow {
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
            let ts = e.ts as i64;
            if sess.is_empty() || ts - prev_ts > GAP_MS {
                let day = (d.base + ts).div_euclid(DAY_MS);
                if day != cur_day {
                    flush!();
                    cur_day = day;
                }
                d_sess += 1;
                sess.push(Sess { start: ts, di: (day - d.min_day) as usize, rev: 0, purch: 0 });
            }
            prev_ts = ts;
            d_events += 1;
            if e.purch() {
                let p = e.price();
                d_rev += p;
                let s = sess.last_mut().unwrap();
                s.rev += p;
                s.purch += 1;
                let c = e.cat();
                if catsum[c as usize] == 0 {
                    touched.push(c);
                }
                catsum[c as usize] += p;
            }
        }
        flush!();

        // clicks of this person
        while ci < clicks.len() && person_order(clicks[ci].person) < person_order(person) {
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
            while q < cl.len() && cl[q].ts as i64 <= s.start {
                q += 1;
            }
            let mut cid = 0usize;
            if q > 0 {
                let t = cl[q - 1].ts as i64;
                if s.start - t <= WEEK_MS {
                    let mut g = q - 1;
                    while g > 0 && cl[g - 1].ts as i64 == t {
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
            cts.extend(cl.iter().map(|c| (c.cid, c.ts as i64)));
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
    users
}

// ------------------------------------------------------------------ parquet output

/// Thrift compact-protocol encoder, just enough for Parquet page headers and the footer.
struct Thrift {
    b: Vec<u8>,
    cur: i16,
    stack: Vec<i16>,
}

impl Thrift {
    const I32: u8 = 5;
    const I64: u8 = 6;
    const BIN: u8 = 8;
    const LIST: u8 = 9;
    const STRUCT: u8 = 12;

    fn new() -> Thrift {
        Thrift { b: Vec::new(), cur: 0, stack: Vec::new() }
    }

    fn varint(&mut self, mut v: u64) {
        while v >= 0x80 {
            self.b.push(v as u8 | 0x80);
            v >>= 7;
        }
        self.b.push(v as u8);
    }

    fn field(&mut self, id: i16, ty: u8) {
        let d = id - self.cur;
        if d > 0 && d <= 15 {
            self.b.push((d as u8) << 4 | ty);
        } else {
            self.b.push(ty);
            self.varint(((id << 1) ^ (id >> 15)) as u16 as u64);
        }
        self.cur = id;
    }

    fn i32(&mut self, id: i16, v: i32) {
        self.field(id, Self::I32);
        self.varint(((v << 1) ^ (v >> 31)) as u32 as u64);
    }

    fn i64(&mut self, id: i16, v: i64) {
        self.field(id, Self::I64);
        self.varint(((v << 1) ^ (v >> 63)) as u64);
    }

    fn bin(&mut self, id: i16, s: &[u8]) {
        self.field(id, Self::BIN);
        self.varint(s.len() as u64);
        self.b.extend_from_slice(s);
    }

    fn list(&mut self, id: i16, elem: u8, n: usize) {
        self.field(id, Self::LIST);
        if n < 15 {
            self.b.push((n as u8) << 4 | elem);
        } else {
            self.b.push(0xf0 | elem);
            self.varint(n as u64);
        }
    }

    /// Start a struct: a field (`Some(id)`) or a list element (`None`).
    fn begin(&mut self, id: Option<i16>) {
        if let Some(id) = id {
            self.field(id, Self::STRUCT);
        }
        self.stack.push(self.cur);
        self.cur = 0;
    }

    fn end(&mut self) {
        self.b.push(0);
        self.cur = self.stack.pop().unwrap_or(0);
    }
}

/// One output column.
enum Col<'a> {
    Str(&'static str, Vec<&'a [u8]>),
    OptStr(&'static str, Vec<Option<&'a [u8]>>),
    Date(&'static str, Vec<i32>),
    I64(&'static str, Vec<i64>),
}

impl Col<'_> {
    fn name(&self) -> &'static str {
        match self {
            Col::Str(n, _) | Col::OptStr(n, _) | Col::Date(n, _) | Col::I64(n, _) => n,
        }
    }

    /// (physical type, converted type, optional)
    fn kind(&self) -> (i32, Option<i32>, bool) {
        match self {
            Col::Str(..) => (6, Some(0), false), // BYTE_ARRAY, UTF8
            Col::OptStr(..) => (6, Some(0), true),
            Col::Date(..) => (1, Some(6), false), // INT32, DATE
            Col::I64(..) => (2, None, false),     // INT64
        }
    }

    /// Plain-encoded page body (definition levels first for the optional column).
    fn page(&self, out: &mut Vec<u8>) {
        match self {
            Col::Str(_, v) => {
                for s in v {
                    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    out.extend_from_slice(s);
                }
            }
            Col::OptStr(_, v) => {
                // definition levels: one bit-packed run of bit width 1, length-prefixed
                let groups = v.len().div_ceil(8);
                let mut lv = Thrift::new();
                lv.varint(((groups as u64) << 1) | 1);
                let mut bits = vec![0u8; groups];
                for (i, s) in v.iter().enumerate() {
                    if s.is_some() {
                        bits[i / 8] |= 1 << (i % 8);
                    }
                }
                lv.b.extend_from_slice(&bits);
                out.extend_from_slice(&(lv.b.len() as u32).to_le_bytes());
                out.extend_from_slice(&lv.b);
                for s in v.iter().flatten() {
                    out.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    out.extend_from_slice(s);
                }
            }
            Col::Date(_, v) => {
                for x in v {
                    out.extend_from_slice(&x.to_le_bytes());
                }
            }
            Col::I64(_, v) => {
                for x in v {
                    out.extend_from_slice(&x.to_le_bytes());
                }
            }
        }
    }
}

/// Write `cols` (each `rows` long) as one Parquet file: one row group, one plain, uncompressed
/// data page per column.
fn write_parquet(path: &Path, rows: usize, cols: &[Col]) {
    let mut f: Vec<u8> = Vec::with_capacity(64 + rows * 64);
    f.extend_from_slice(b"PAR1");
    let mut chunks: Vec<(i64, i64)> = vec![]; // (page offset, chunk size)
    if rows > 0 {
        let mut body = Vec::new();
        for c in cols {
            body.clear();
            c.page(&mut body);
            let mut h = Thrift::new();
            h.i32(1, 0); // DATA_PAGE
            h.i32(2, body.len() as i32);
            h.i32(3, body.len() as i32);
            h.begin(Some(5));
            h.i32(1, rows as i32);
            h.i32(2, 0); // PLAIN
            h.i32(3, 3); // RLE
            h.i32(4, 3); // RLE
            h.end();
            h.b.push(0);
            let off = f.len() as i64;
            f.extend_from_slice(&h.b);
            f.extend_from_slice(&body);
            chunks.push((off, f.len() as i64 - off));
        }
    }
    let mut m = Thrift::new();
    m.i32(1, 1);
    m.list(2, Thrift::STRUCT, cols.len() + 1);
    m.begin(None);
    m.bin(4, b"schema");
    m.i32(5, cols.len() as i32);
    m.end();
    for c in cols {
        let (ty, conv, opt) = c.kind();
        m.begin(None);
        m.i32(1, ty);
        m.i32(3, opt as i32); // REQUIRED 0, OPTIONAL 1
        m.bin(4, c.name().as_bytes());
        if let Some(cv) = conv {
            m.i32(6, cv);
        }
        m.end();
    }
    m.i64(3, rows as i64);
    m.list(4, Thrift::STRUCT, chunks.len().min(1));
    if rows > 0 {
        m.begin(None);
        m.list(1, Thrift::STRUCT, cols.len());
        for (c, &(off, size)) in cols.iter().zip(&chunks) {
            m.begin(None);
            m.i64(2, off);
            m.begin(Some(3));
            m.i32(1, c.kind().0);
            m.list(2, Thrift::I32, 2);
            m.varint(0); // PLAIN (zigzag 0)
            m.varint(6); // RLE 3 (zigzag 6)
            m.list(3, Thrift::BIN, 1);
            m.varint(c.name().len() as u64);
            m.b.extend_from_slice(c.name().as_bytes());
            m.i32(4, 0); // UNCOMPRESSED
            m.i64(5, rows as i64);
            m.i64(6, size);
            m.i64(7, size);
            m.i64(9, off);
            m.end();
            m.end();
        }
        m.i64(2, chunks.iter().map(|c| c.1).sum());
        m.i64(3, rows as i64);
        m.end();
    }
    m.b.push(0);
    f.extend_from_slice(&m.b);
    f.extend_from_slice(&(m.b.len() as u32).to_le_bytes());
    f.extend_from_slice(b"PAR1");
    fs::write(path, &f).unwrap_or_else(|e| panic!("write {}: {e}", path.display()));
}

#[inline(never)]
fn write_users(path: &Path, rows: &[UserRow], cats: &[Bytes]) {
    let cols = [
        Col::Str("user_id", rows.iter().map(|r| unpack(r.name)).collect()),
        Col::Date("day", rows.iter().map(|r| r.day).collect()),
        Col::I64("sessions", rows.iter().map(|r| r.sessions as i64).collect()),
        Col::I64("events", rows.iter().map(|r| r.events as i64).collect()),
        Col::I64("revenue_cents", rows.iter().map(|r| r.rev as i64).collect()),
        Col::OptStr("top_category", rows.iter().map(|r| (r.top != NO_CAT).then(|| cats[r.top as usize])).collect()),
    ];
    write_parquet(path, rows.len(), &cols);
}

struct CampRow {
    id: Vec<u8>,
    channel: Vec<u8>,
    day: i32,
    vals: [i64; 4],
}

fn write_campaigns(path: &Path, rows: &[CampRow]) {
    let cols = [
        Col::Str("campaign_id", rows.iter().map(|r| &r.id[..]).collect()),
        Col::Str("channel", rows.iter().map(|r| &r.channel[..]).collect()),
        Col::Date("day", rows.iter().map(|r| r.day).collect()),
        Col::I64("sessions", rows.iter().map(|r| r.vals[0]).collect()),
        Col::I64("purchasing_sessions", rows.iter().map(|r| r.vals[1]).collect()),
        Col::I64("revenue_cents", rows.iter().map(|r| r.vals[2]).collect()),
        Col::I64("assisted_sessions", rows.iter().map(|r| r.vals[3]).collect()),
    ];
    write_parquet(path, rows.len(), &cols);
}

// ------------------------------------------------------------------ main

#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

/// Dedup one event-id shard over the keys of every thread (see `dedup_shard`).
type GKey = Key;

/// Dedup one event-id shard: for every event id the copy with the smallest ingest_ts is kept
/// (equal ones are identical); every other valid copy is marked dead in its chunk. The table
/// holds the current winner of each id as (32-bit tag of the first id word, index in `g`), so
/// a probe rarely touches `g`.
#[inline(never)]
fn dedup_shard(g: &[GKey], table: &mut Vec<u64>, dead: &[Vec<Vec<AtomicU64>>]) {
    const EMPTY: u64 = u64::MAX;
    let cap = (g.len() * 2).next_power_of_two().max(1024);
    let mask = cap - 1;
    table.clear();
    table.resize(cap, EMPTY);
    let kill = |k: &GKey| {
        if k.idx != u32::MAX {
            let r = k.idx as usize;
            dead[k.thread as usize][k.part as usize][r >> 6].fetch_or(1 << (r & 63), Relaxed);
        }
    };
    for (i, k) in g.iter().enumerate() {
        let tag = k.id0 >> 32;
        let mut j = (k.id0 >> 8) as usize & mask; // the low 8 bits are the shard number
        loop {
            let e = table[j];
            if e == EMPTY {
                table[j] = tag << 32 | i as u64;
                break;
            }
            if e >> 32 == tag {
                let w = (e & 0xffff_ffff) as usize;
                let o = &g[w];
                if o.id0 == k.id0 && o.id1 == k.id1 {
                    if k.ing < o.ing {
                        kill(o);
                        table[j] = tag << 32 | i as u64;
                    } else {
                        kill(k);
                    }
                    break;
                }
            }
            j = (j + 1) & mask;
        }
    }
}

/// Scratch buffers of one worker thread, kept for the whole run: fresh memory is expensive to
/// fault in, and rayon's `map_init` would rebuild its state for every split job.
#[derive(Default)]
struct Bufs {
    tid: u8,
    out: Option<ThreadOut>,
    clicks: Vec<Click>,
    g: Vec<GKey>,
    table: Vec<u64>,
    pevs: Vec<Ev>,
    pcks: Vec<Click>,
    camp: CampAcc,
}

struct Pool(Vec<Mutex<Bufs>>);

impl Pool {
    fn new() -> Pool {
        Pool((0..rayon::current_num_threads() + 1).map(|i| Mutex::new(Bufs { tid: i as u8, ..Default::default() })).collect())
    }

    /// Run `f` with this thread's buffers (no rayon calls may happen inside `f`).
    fn with<R>(&self, f: impl FnOnce(&mut Bufs) -> R) -> R {
        let i = rayon::current_thread_index().unwrap_or(self.0.len() - 1);
        f(&mut self.0[i].lock().unwrap())
    }
}

/// CPUs granted by a cgroup CPU quota (v2 `cpu.max`, or v1 CFS quota), if one is set.
fn cgroup_cpus() -> Option<usize> {
    let ceil = |q: f64, p: f64| (q > 0.0 && p > 0.0).then(|| (q / p).ceil().max(1.0) as usize);
    if let Ok(t) = fs::read_to_string("/sys/fs/cgroup/cpu.max") {
        let mut it = t.split_whitespace();
        let q = it.next()?;
        let p: f64 = it.next()?.parse().ok()?;
        return if q == "max" { None } else { ceil(q.parse().ok()?, p) };
    }
    for dir in ["/sys/fs/cgroup/cpu", "/sys/fs/cgroup/cpu,cpuacct"] {
        let read = |f: &str| fs::read_to_string(format!("{dir}/{f}")).ok().and_then(|v| v.trim().parse::<f64>().ok());
        if let (Some(q), Some(p)) = (read("cpu.cfs_quota_us"), read("cpu.cfs_period_us")) {
            return ceil(q, p);
        }
    }
    None
}

/// Worker threads: $NCPU if set, else the usable CPUs (affinity and cgroup quota), at most 8,
/// the size of the scoring container. More threads than granted CPUs get throttled badly.
fn worker_count() -> usize {
    if let Some(n) = std::env::var("NCPU").ok().and_then(|v| v.parse::<usize>().ok()).filter(|&n| n > 0) {
        return n.min(255);
    }
    let mut n = std::thread::available_parallelism().map(|n| n.get()).unwrap_or(8);
    if let Some(q) = cgroup_cpus() {
        n = n.min(q);
    }
    n.clamp(1, 8)
}

fn main() {
    let t = Instant::now();
    let data = PathBuf::from(std::env::var("DATA_DIR").unwrap_or("/data".into()));
    let out = PathBuf::from(std::env::var("OUTPUT_DIR").unwrap_or("/output".into()));
    let workers = worker_count();
    rayon::ThreadPoolBuilder::new().num_threads(workers).build_global().unwrap();
    log(&t, &format!("{workers} worker threads"));

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
    let mut products = ProdTable::new(prod_rows.len(), prod_rows.first().and_then(|r| r[0].first().copied()).unwrap_or(b'p'));
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

    // all times are kept as u32 ms offsets from the click window start
    let base = ws - WEEK_MS;
    assert!(we - base < u32::MAX as i64, "window too long for 32-bit time offsets");
    let mut camp_table = StrTable::<u32>::new(campaigns.len());
    for (k, &v) in &campaigns {
        camp_table.insert(k, v);
    }
    let ctx = Ctx { ws, we, base, products, unknown, campaigns: camp_table };

    let linked = build_links(&csv_files(&data.join("links")));
    log(&t, &format!("links: {} linked ids", linked.len()));

    let pool = Pool::new();

    // parse events: each thread writes its own person and event-id shards
    let ev_files = csv_files(&data.join("events"));
    let ev_bytes: u64 = ev_files.iter().map(|f| fs::metadata(f).map(|m| m.len()).unwrap_or(0)).sum();
    let per_thread = ev_bytes as usize / 64 / pool.0.len();
    let ev_chunks = chunks(&ev_files, CHUNK);
    ev_chunks.par_iter().for_each(|c| {
        pool.with(|b| {
            let tid = b.tid;
            let out = b.out.get_or_insert_with(|| ThreadOut::new(per_thread));
            parse_events(c, &ctx, &linked, out, tid)
        })
    });
    let mut outs: Vec<ThreadOut> = pool
        .0
        .iter()
        .map(|m| m.lock().unwrap().out.take().unwrap_or_else(|| ThreadOut { evs: (0..NPART).map(|_| vec![]).collect(), keys: (0..NSHARD).map(|_| vec![]).collect() }))
        .collect();
    let dead: Vec<Vec<Vec<AtomicU64>>> = outs
        .par_iter()
        .map(|o| o.evs.iter().map(|v| (0..v.len().div_ceil(64)).map(|_| AtomicU64::new(0)).collect()).collect())
        .collect();
    log(&t, "events parsed");

    // dedup per event-id shard
    (0..NSHARD).into_par_iter().for_each(|s| {
        pool.with(|b| {
            let (g, table) = (&mut b.g, &mut b.table);
            g.clear();
            for o in &outs {
                g.extend_from_slice(&o.keys[s]);
            }
            dedup_shard(g, table, &dead);
        })
    });
    // the keys are done with: hand their memory back to the allocator for the phases below,
    // which saves faulting in fresh pages
    outs.par_iter_mut().for_each(|o| o.keys = Vec::new());
    log(&t, "events deduped");

    // parse clicks
    let ck_chunks = chunks(&csv_files(&data.join("clicks")), SMALL_CHUNK);
    let click_parts: Vec<PClicks> = ck_chunks.par_iter().map(|c| pool.with(|b| parse_clicks(c, &ctx, &linked, &mut b.clicks))).collect();
    log(&t, "clicks parsed");

    let ud = out.join("user_daily");
    let cd = out.join("campaign_daily");
    fs::create_dir_all(&ud).unwrap();
    fs::create_dir_all(&cd).unwrap();

    // per person shard
    let min_day = ws.div_euclid(DAY_MS);
    let dims = Dims {
        base: ctx.base,
        min_day,
        ndays: ((we - 1).div_euclid(DAY_MS) - min_day + 1) as usize,
        ncid: camp_names.len(),
        ncat: cat_names.len(),
    };
    (0..NPART)
        .into_par_iter()
        .for_each(|p| {
            pool.with(|b| {
                let (evs, cks) = (&mut b.pevs, &mut b.pcks);
                gather_events(&outs, &dead, p, evs);
                gather_clicks(&click_parts, p, cks);
                let users = process_part(evs, cks, &dims, &mut b.camp);
                if !users.is_empty() || p == 0 {
                    write_users(&ud.join(format!("part-{p}.parquet")), &users, &cat_names);
                }
            })
        });
    std::mem::forget(outs); // freeing thousands of vectors costs more than it saves
    std::mem::forget(dead);
    std::mem::forget(click_parts);
    log(&t, "sessions, credit and assists done");

    // campaign output (user parts were written per shard)
    let accs: Vec<CampAcc> = pool.0.into_iter().map(|m| m.into_inner().unwrap().camp).collect();
    let mut rows: Vec<CampRow> = vec![];
    let nd = dims.ndays;
    for cid in 0..dims.ncid {
        for di in 0..nd {
            let mut v = [0u64; 4];
            for o in &accs {
                if o.credit.is_empty() {
                    continue;
                }
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
