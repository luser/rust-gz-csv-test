extern crate csv;
extern crate flate2;
extern crate chrono;
#[macro_use]
extern crate nom;
extern crate rayon;

use std::boxed::Box;
use std::env;
use std::fs::File;
use std::iter::Sum;
use std::str::{self, FromStr};

use chrono::{DateTime, Duration, NaiveDate, NaiveTime, NaiveDateTime, Utc};
use csv::{ByteRecord, ReaderBuilder};
use flate2::read::GzDecoder;
use rayon::prelude::*;

named!(int32_4 <&str, i32>,
    map_res!(take!(4), FromStr::from_str)
);

named!(uint32_2 <&str, u32>,
    map_res!(take!(2), FromStr::from_str)
);

named!(uint32_3 <&str, u32>,
    map_res!(take!(3), FromStr::from_str)
);

named!(rfc3339<&str, DateTime<Utc>>,
  do_parse!(
      year:  int32_4  >>
          tag!("-") >>
          month: uint32_2 >>
          tag!("-") >>
          day:  uint32_2 >>
          tag!("T") >>
          hour: uint32_2 >>
          tag!(":") >>
          minute: uint32_2 >>
          tag!(":") >>
          second: uint32_2 >>
          tag!(".") >>
          milli: uint32_3 >>
          tag!("Z") >>
          (DateTime::<Utc>::from_utc(
              NaiveDateTime::new(NaiveDate::from_ymd(year, month, day),
                                 NaiveTime::from_hms_milli(hour, minute, second, milli)),
              Utc)
          )
  )
);

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

// 2008-09-08T22:47:31Z

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
            rfc3339(s).map(|dt| {
                if dt > cutoff {
                    recent += 1
                }
            });
        }
    }

    Ok(Stats { total, recent })
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn basic() {
        const S: &str = "2015-10-27T17:38:38.000Z";
        let dt = DateTime::<Utc>::from_str(S).unwrap();
        let (_, dt2) = rfc3339(S).unwrap();
        assert_eq!(dt, dt2);
    }
}
