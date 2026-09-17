//! A minimal command-line front end.
//!
//! ```text
//! cargo run --example cli -- <db-dir> put <key> <value>
//! cargo run --example cli -- <db-dir> get <key>
//! cargo run --example cli -- <db-dir> delete <key>
//! cargo run --example cli -- <db-dir> scan [start] [end]     # [start, end)
//! cargo run --example cli -- <db-dir> load <count>           # insert key00000000.. for testing
//! cargo run --example cli -- <db-dir> compact
//! cargo run --example cli -- <db-dir> stats
//! ```

use std::process::ExitCode;

use emberdb::{Db, Options};

fn usage() -> ExitCode {
    eprintln!(
        "usage: cli <db-dir> <command> [args]\n\
         commands:\n  \
           put <key> <value>\n  \
           get <key>\n  \
           delete <key>\n  \
           scan [start] [end]   keys in [start, end)\n  \
           load <count>         insert <count> generated keys\n  \
           compact\n  \
           stats"
    );
    ExitCode::from(2)
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.len() < 2 {
        return usage();
    }
    match run(&args[0], &args[1], &args[2..]) {
        Ok(Some(code)) => code,
        Ok(None) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

fn run(dir: &str, cmd: &str, rest: &[String]) -> emberdb::Result<Option<ExitCode>> {
    // Run maintenance inline so the process can exit as soon as the command is done.
    let opts = Options {
        background_compaction: false,
        ..Options::default()
    };
    let db = Db::open(dir, opts)?;
    match (cmd, rest) {
        ("put", [k, v]) => db.put(k, v)?,
        ("get", [k]) => match db.get(k)? {
            Some(v) => println!("{}", String::from_utf8_lossy(&v)),
            None => {
                eprintln!("(not found)");
                return Ok(Some(ExitCode::from(1)));
            }
        },
        ("delete", [k]) => db.delete(k)?,
        ("scan", bounds) if bounds.len() <= 2 => {
            let iter = match bounds {
                [] => db.iter()?,
                [start] => db.scan(start.as_str()..)?,
                [start, end] => db.scan(start.as_str()..end.as_str())?,
                _ => unreachable!(),
            };
            let mut n = 0usize;
            for kv in iter {
                let (k, v) = kv?;
                println!(
                    "{}\t{}",
                    String::from_utf8_lossy(&k),
                    String::from_utf8_lossy(&v)
                );
                n += 1;
            }
            eprintln!("({n} entries)");
        }
        ("load", [count]) => {
            let Ok(count) = count.parse::<u64>() else {
                return Ok(Some(usage()));
            };
            for i in 0..count {
                db.put(format!("key{i:08}"), format!("value{i}"))?;
            }
            db.flush()?;
            eprintln!("loaded {count} keys");
        }
        ("compact", []) => {
            db.compact()?;
            println!("{}", db.stats());
        }
        ("stats", []) => println!("{}", db.stats()),
        _ => return Ok(Some(usage())),
    }
    Ok(None)
}
