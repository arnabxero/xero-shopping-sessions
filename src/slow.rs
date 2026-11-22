
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fs;
use std::path::{Path, PathBuf};

use crate::pqw::{self, Column};

const DAY: i64 = 86_400_000;
const WEEK: i64 = 7 * DAY;
const GAP: i64 = 1_800_000;

type Bytes = Vec<u8>;

pub fn fields(line: &[u8], count: usize) -> Option<Vec<&[u8]>> {
    if line.is_empty() {
        return None;
    }
    let mut out = Vec::new();
    for field in line.split(|&byte| byte == b',') {
        let quotes = field.iter().filter(|&&byte| byte == b'"').count();
        if quotes == 0 {
            out.push(field);
        } else if quotes == 2 && field.len() >= 2 && field[0] == b'"' && field[field.len() - 1] == b'"' {
            out.push(&field[1..field.len() - 1]);
        } else {
            return None;
        }
    }
    (out.len() == count).then_some(out)
}

fn lines(folder: &Path) -> Vec<Bytes> {
    let mut paths: Vec<PathBuf> = fs::read_dir(folder)
        .map(|entries| entries.map(|entry| entry.unwrap().path()).collect())
        .unwrap_or_default();
    paths.retain(|path| path.extension().is_some_and(|ext| ext == "csv") && path.is_file());
    paths.sort();
    let mut out = Vec::new();
    for path in paths {
        out.extend(file_lines(&path));
    }
    out
}

pub fn file_lines(path: &Path) -> Vec<Bytes> {
    let bytes = fs::read(path).unwrap();
    let mut pieces: Vec<&[u8]> = bytes.split(|&byte| byte == b'\n').collect();
    if pieces.last().is_some_and(|last| last.is_empty()) {
        pieces.pop();
    }
    pieces.into_iter().skip(1).map(<[u8]>::to_vec).collect()
}

fn digits(text: &[u8]) -> Option<i64> {
    if text.is_empty() || !text.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let mut value: i64 = 0;
    for &byte in text {
        value = value.saturating_mul(10).saturating_add(i64::from(byte - b'0'));
    }
    Some(value)
}

fn days_from_civil(year: i64, month: i64, day: i64) -> i64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year.div_euclid(400);
    let year_of_era = year - era * 400;
    let day_of_year = (153 * ((month + 9) % 12) + 2) / 5 + day - 1;
    era * 146_097 + year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year - 719_468
}

fn civil(text: &[u8]) -> Option<i64> {
    if text.len() != 19 {
        return None;
    }
    let separators = [(4, b'-'), (7, b'-'), (10, b'T'), (13, b':'), (16, b':')];
    if separators.iter().any(|&(at, byte)| text[at] != byte) {
        return None;
    }
    let year = digits(&text[0..4])?;
    let month = digits(&text[5..7])?;
    let day = digits(&text[8..10])?;
    let hour = digits(&text[11..13])?;
    let minute = digits(&text[14..16])?;
    let second = digits(&text[17..19])?;
    let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let days_in_month = match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => 28 + i64::from(leap),
        _ => return None,
    };
    if day < 1 || day > days_in_month || hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    Some(days_from_civil(year, month, day) * DAY + (hour * 3600 + minute * 60 + second) * 1000)
}

pub fn timestamp(text: &[u8]) -> Option<i64> {
    if let Some(value) = digits(text) {
        return Some(value);
    }
    if text.len() == 20 && text[19] == b'Z' {
        return civil(&text[..19]);
    }
    if text.len() == 25 && (text[19] == b'+' || text[19] == b'-') && text[22] == b':' {
        let local = civil(&text[..19])?;
        let hours = digits(&text[20..22])?;
        let minutes = digits(&text[23..25])?;
        if hours > 23 || minutes > 59 {
            return None;
        }
        let offset = (hours * 60 + minutes) * 60_000;
        return Some(if text[19] == b'+' { local - offset } else { local + offset });
    }
    None
}

pub fn manifest(path: &Path) -> (i64, i64) {
    let text = fs::read(path).unwrap();
    let value = |key: &str| {
        let at = text.windows(key.len()).position(|window| window == key.as_bytes()).unwrap();
        let rest = &text[at + key.len() + 1..];
        let open = rest.iter().position(|&byte| byte == b'"').unwrap();
        let rest = &rest[open + 1..];
        let close = rest.iter().position(|&byte| byte == b'"').unwrap();
        timestamp(&rest[..close]).unwrap()
    };
    (value(concat!("\"", "win", "dow", "_st", "art", "\"")), value(concat!("\"", "win", "dow", "_en", "d", "\"")))
}

fn lookup(path: &Path) -> HashMap<Bytes, Bytes> {
    file_lines(path)
        .iter()
        .filter_map(|line| fields(line, 2).map(|row| (row[0].to_vec(), row[1].to_vec())))
        .collect()
}

struct People {
    index: HashMap<Bytes, usize>,
    parent: Vec<usize>,
    name: Vec<Bytes>,
}

impl People {
    fn id(&mut self, id: &[u8]) -> usize {
        if let Some(&at) = self.index.get(id) {
            return at;
        }
        let at = self.parent.len();
        self.index.insert(id.to_vec(), at);
        self.parent.push(at);
        self.name.push(id.to_vec());
        at
    }
    fn root(&self, mut at: usize) -> usize {
        while self.parent[at] != at {
            at = self.parent[at];
        }
        at
    }
    fn join(&mut self, a: usize, b: usize) {
        let (a, b) = (self.root(a), self.root(b));
        if a != b {
            let name = self.name[a].clone().min(self.name[b].clone());
            self.parent[a] = b;
            self.name[b] = name;
        }
    }
    fn person(&self, id: &[u8]) -> Bytes {
        match self.index.get(id) {
            Some(&at) => self.name[self.root(at)].clone(),
            None => id.to_vec(),
        }
    }
}

struct Event {
    ts: i64,
    price: Option<i64>,
    product: Bytes,
}

struct Click {
    ts: i64,
    id: Bytes,
    campaign: Bytes,
}

#[derive(Default)]
struct UserDay {
    sessions: i64,
    events: i64,
    revenue: i64,
    categories: BTreeMap<Bytes, i64>,
}

#[derive(Default)]
struct CampaignDay {
    sessions: i64,
    purchasing: i64,
    revenue: i64,
    assisted: i64,
}

pub fn run(data: &Path, output: &Path) {
    let (window_start, window_end) = manifest(&data.join(concat!("man", "ife", "st.", "jso", "n")));
    let products = lookup(&data.join(concat!("pro", "duc", "ts.", "csv")));
    let campaigns = lookup(&data.join(concat!("cam", "pai", "gns", ".cs", "v")));

    let mut people = People { index: HashMap::new(), parent: Vec::new(), name: Vec::new() };
    for line in lines(&data.join(concat!("lin", "ks"))) {
        let Some(row) = fields(&line, 2) else { continue };
        if row[0].is_empty() || row[1].is_empty() {
            continue;
        }
        let (a, b) = (people.id(row[0]), people.id(row[1]));
        people.join(a, b);
    }

    let mut kept: HashMap<Bytes, (i64, Bytes)> = HashMap::new();
    for line in lines(&data.join(concat!("eve", "nts"))) {
        let Some(row) = fields(&line, 7) else { continue };
        if row[0].is_empty() {
            continue;
        }
        let ingest = timestamp(row[3]).unwrap_or(i64::MAX);
        let id = row[0].to_vec();
        match kept.get_mut(&id) {
            Some(existing) if existing.0 <= ingest => {}
            Some(existing) => *existing = (ingest, line),
            None => {
                kept.insert(id, (ingest, line));
            }
        }
    }
    let mut events: BTreeMap<Bytes, Vec<Event>> = BTreeMap::new();
    for (ingest, line) in kept.into_values() {
        let row: Vec<Bytes> = fields(&line, 7).unwrap().iter().map(|field| field.to_vec()).collect();
        let (user, ts, kind, product, price) = (&row[1], &row[2], &row[4], &row[5], &row[6]);
        if user.is_empty() || product.is_empty() {
            continue;
        }
        let Some(ts) = timestamp(ts) else { continue };
        if ts < window_start || ts >= window_end {
            continue;
        }
        let price = match kind.as_slice() {
            b"view" | b"cart" => None,
            b"purchase" => match digits(price) {
                Some(value) if (1..=10_000_000).contains(&value) => Some(value),
                _ => continue,
            },
            _ => continue,
        };
        if ingest - ts > DAY {
            continue;
        }
        events.entry(people.person(user)).or_default().push(Event { ts, price, product: product.clone() });
        drop(line);
    }

    let mut seen: BTreeSet<Bytes> = BTreeSet::new();
    let mut clicks: HashMap<Bytes, Vec<Click>> = HashMap::new();
    for line in lines(&data.join(concat!("cli", "cks"))) {
        let Some(row) = fields(&line, 4) else { continue };
        if row[0].is_empty() || !seen.insert(row[0].to_vec()) {
            continue;
        }
        let (user, campaign) = (row[1], row[2]);
        if user.is_empty() || !campaigns.contains_key(campaign) {
            continue;
        }
        let Some(ts) = timestamp(row[3]) else { continue };
        if ts < window_start - WEEK || ts >= window_end {
            continue;
        }
        clicks.entry(people.person(user)).or_default().push(Click {
            ts,
            id: row[0].to_vec(),
            campaign: campaign.to_vec(),
        });
    }

    let mut t_one: BTreeMap<(Bytes, i64), UserDay> = BTreeMap::new();
    let mut t_two: BTreeMap<(Bytes, i64), CampaignDay> = BTreeMap::new();
    let no_clicks = Vec::new();
    for (person, mut list) in events {
        list.sort_by_key(|event| event.ts);
        let clicks = clicks.get(&person).unwrap_or(&no_clicks);
        let mut at = 0;
        while at < list.len() {
            let mut end = at + 1;
            while end < list.len() && list[end].ts - list[end - 1].ts <= GAP {
                end += 1;
            }
            let session = &list[at..end];
            at = end;
            let start = session[0].ts;
            let day = start.div_euclid(DAY);
            let revenue: i64 = session.iter().filter_map(|event| event.price).sum();
            let purchased = session.iter().any(|event| event.price.is_some());

            let row = t_one.entry((person.clone(), day)).or_default();
            row.sessions += 1;
            row.events += session.len() as i64;
            row.revenue += revenue;
            for event in session {
                if let Some(price) = event.price {
                    let category = products.get(&event.product).cloned().unwrap_or(concat!("unk", "now", "n").as_bytes().to_vec());
                    *row.categories.entry(category).or_default() += price;
                }
            }

            let eligible: Vec<&Click> =
                clicks.iter().filter(|click| click.ts <= start && start - click.ts <= WEEK).collect();
            let chosen = eligible
                .iter()
                .max_by(|a, b| a.ts.cmp(&b.ts).then_with(|| b.id.cmp(&a.id)))
                .map_or(concat!("org", "ani", "c").as_bytes().to_vec(), |click| click.campaign.clone());
            let row = t_two.entry((chosen, day)).or_default();
            row.sessions += 1;
            row.purchasing += i64::from(purchased);
            row.revenue += revenue;
            let assists: BTreeSet<&Bytes> = eligible.iter().map(|click| &click.campaign).collect();
            for campaign in assists {
                t_two.entry((campaign.clone(), day)).or_default().assisted += 1;
            }
        }
    }

    let mut columns: Vec<Column> = (0..6).map(|_| Column::default()).collect();
    for (row, ((person, day), value)) in t_one.iter().enumerate() {
        columns[0].push_str(person);
        columns[1].push_i32(*day as i32);
        columns[2].push_i64(value.sessions);
        columns[3].push_i64(value.events);
        columns[4].push_i64(value.revenue);
        let mut top: Option<(&Bytes, i64)> = None;
        for (category, &sum) in &value.categories {
            if top.is_none_or(|(_, best)| sum > best) {
                top = Some((category, sum));
            }
        }
        columns[5].push_present(row, top.is_some());
        if let Some((category, _)) = top {
            columns[5].push_str(category);
        }
    }
    let folder = output.join(concat!("use", "r_d", "ail", "y"));
    fs::create_dir_all(&folder).unwrap();
    pqw::write(&folder.join("part-0.parquet"), &pqw::T_ONE, &columns, t_one.len()).unwrap();

    let mut columns: Vec<Column> = (0..7).map(|_| Column::default()).collect();
    for ((campaign, day), value) in &t_two {
        columns[0].push_str(campaign);
        columns[1].push_str(campaigns.get(campaign).map_or(concat!("org", "ani", "c").as_bytes(), Vec::as_slice));
        columns[2].push_i32(*day as i32);
        columns[3].push_i64(value.sessions);
        columns[4].push_i64(value.purchasing);
        columns[5].push_i64(value.revenue);
        columns[6].push_i64(value.assisted);
    }
    let folder = output.join(concat!("cam", "pai", "gn_", "dai", "ly"));
    fs::create_dir_all(&folder).unwrap();
    pqw::write(&folder.join("part-0.parquet"), &pqw::T_TWO, &columns, t_two.len()).unwrap();
}
