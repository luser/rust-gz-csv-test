extern crate csv;
extern crate flate2;
#[macro_use]
extern crate serde_derive;
extern crate chrono;
extern crate rayon;

use std::boxed::Box;
use std::env;
use std::fs::File;
use std::iter::Sum;
use std::str::{self, FromStr};

use chrono::{DateTime, Utc, Duration};
use csv::{ByteRecord, ReaderBuilder};
use flate2::read::GzDecoder;
use rayon::prelude::*;

#[derive(Debug, Deserialize)]
struct Row {
    bucket: String,
    key: String,
    size: usize,
    last_modified_date: DateTime<Utc>,
    etag: String,
}

struct Stats {
    total: usize,
    recent: usize,
}

impl Sum for Stats {
    fn sum<I: Iterator<Item=Self>>(iter: I) -> Self {
        let mut acc = Stats { total: 0, recent: 0 };
        for stat in  iter {
            acc.total += stat.total;
            acc.recent += stat.recent;
        }
        acc
    }
}

fn main() {
    let cutoff = Utc::now() - Duration::days(180);
    let filenames: Vec<String> = env::args().skip(1).collect();

    let stats: Stats = filenames.par_iter()
        .map(|filename| count(&filename, cutoff).expect(&format!("Couldn't read {}", &filename)))
        .sum();

    let percent = 100.0 * stats.recent as f32 / stats.total as f32;
    println!("{} / {} = {:.2}%", stats.recent, stats.total, percent);
}

fn count(path: &str, cutoff: DateTime<Utc>) -> Result<Stats, Box<std::error::Error>> {
    let mut input_file = File::open(&path)?;
    let decoder = GzDecoder::new(&mut input_file)?;
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .from_reader(decoder);
    let mut record = ByteRecord::new();

    let mut total = 0;
    let mut recent = 0;
    while let Ok(true) = reader.read_byte_record(&mut record) {
        total += 1;
        if let Some(bytes) = record.get(3) {
            let s = str::from_utf8(bytes)?;
            let dt = DateTime::<Utc>::from_str(s)?;
            if dt > cutoff {
                recent += 1
            }
        }
    }

    Ok(Stats { total, recent })
}
