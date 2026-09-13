use std::error::Error;
use std::process::ExitCode;

use voodoo_store_core::{PushOptions, Store};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("voodoo-store: {error}");
            ExitCode::FAILURE
        }
    }
}

fn run() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = std::env::args().collect();
    let Some(command) = args.get(1).map(String::as_str) else {
        print_usage();
        return Ok(());
    };

    match command {
        "put" => {
            require_len(&args, 5)?;
            let mut store = Store::open(&args[2])?;
            store.put(args[3].as_bytes(), args[4].as_bytes())?;
            println!("ok");
        }
        "get" => {
            require_len(&args, 4)?;
            let store = Store::open(&args[2])?;
            match store.get(args[3].as_bytes()) {
                Some(value) => println!("{}", String::from_utf8_lossy(value)),
                None => println!("not-found"),
            }
        }
        "delete" => {
            require_len(&args, 4)?;
            let mut store = Store::open(&args[2])?;
            store.delete(args[3].as_bytes())?;
            println!("ok");
        }
        "verify" => {
            require_len(&args, 3)?;
            let report = Store::verify(&args[2])?;
            println!("store_id={}", hex(&report.header.store_id));
            println!(
                "format={}.{}",
                report.header.format_major, report.header.format_minor
            );
            println!("file_bytes={}", report.file_bytes);
            println!("valid_bytes={}", report.valid_bytes);
            println!("records={}", report.records);
            println!("committed_transactions={}", report.committed_transactions);
            println!("pending_transactions={}", report.pending_transactions);
            println!("keys={}", report.keys);
            println!("torn_tail={}", report.has_torn_tail());
        }
        "backup" => {
            require_len(&args, 4)?;
            let store = Store::open(&args[2])?;
            let bytes = store.backup_to(&args[3])?;
            println!("backup_bytes={bytes}");
        }
        "compact-copy" => {
            require_len(&args, 4)?;
            let store = Store::open(&args[2])?;
            let report = store.compact_copy_to(&args[3])?;
            println!("source_bytes={}", report.source_bytes);
            println!("compacted_bytes={}", report.compacted_bytes);
            println!("bytes_reclaimed={}", report.bytes_reclaimed());
            println!("keys={}", report.keys);
        }
        "queue-push" => {
            if args.len() < 5 || args.len() > 7 {
                return Err("usage: voodoo-store queue-push <store> <queue> <payload> [available_at_ms] [priority]".into());
            }
            let available_at_ms = args
                .get(5)
                .map(|value| value.parse())
                .transpose()?
                .unwrap_or(0);
            let priority = args
                .get(6)
                .map(|value| value.parse())
                .transpose()?
                .unwrap_or(0);
            let mut store = Store::open(&args[2])?;
            let mut queue = store.queue(args[3].as_bytes())?;
            let id = queue.push_with_options(
                args[4].as_bytes(),
                PushOptions {
                    available_at_ms,
                    priority,
                },
            )?;
            println!("id={id}");
        }
        "queue-claim" => {
            require_len(&args, 6)?;
            let now_ms: i64 = args[4].parse()?;
            let lease_ms: u64 = args[5].parse()?;
            let mut store = Store::open(&args[2])?;
            let mut queue = store.queue(args[3].as_bytes())?;
            match queue.claim(now_ms, lease_ms)? {
                Some(message) => {
                    println!("id={}", message.id);
                    println!("lease_generation={}", message.lease_generation);
                    println!("attempts={}", message.attempts);
                    println!("priority={}", message.priority);
                    println!("lease_until_ms={}", message.lease_until_ms);
                    println!("payload={}", String::from_utf8_lossy(&message.payload));
                }
                None => println!("empty"),
            }
        }
        "queue-ack" => {
            require_len(&args, 6)?;
            let id: u64 = args[4].parse()?;
            let lease_generation: u32 = args[5].parse()?;
            let mut store = Store::open(&args[2])?;
            let mut queue = store.queue(args[3].as_bytes())?;
            queue.ack(id, lease_generation)?;
            println!("ok");
        }
        "queue-nack" => {
            require_len(&args, 7)?;
            let id: u64 = args[4].parse()?;
            let lease_generation: u32 = args[5].parse()?;
            let available_at_ms: i64 = args[6].parse()?;
            let mut store = Store::open(&args[2])?;
            let mut queue = store.queue(args[3].as_bytes())?;
            queue.nack(id, lease_generation, available_at_ms)?;
            println!("ok");
        }
        "queue-dead" => {
            require_len(&args, 6)?;
            let id: u64 = args[4].parse()?;
            let lease_generation: u32 = args[5].parse()?;
            let mut store = Store::open(&args[2])?;
            let mut queue = store.queue(args[3].as_bytes())?;
            queue.dead_letter(id, lease_generation)?;
            println!("ok");
        }
        "queue-stats" => {
            require_len(&args, 4)?;
            let mut store = Store::open(&args[2])?;
            let queue = store.queue(args[3].as_bytes())?;
            let stats = queue.stats()?;
            println!("ready={}", stats.ready);
            println!("leased={}", stats.leased);
            println!("dead={}", stats.dead);
            println!("total={}", stats.total);
        }
        "help" | "--help" | "-h" => print_usage(),
        other => return Err(format!("unknown command: {other}").into()),
    }

    Ok(())
}

fn require_len(args: &[String], expected: usize) -> Result<(), Box<dyn Error>> {
    if args.len() == expected {
        Ok(())
    } else {
        Err("invalid arguments; run `voodoo-store help` for usage".into())
    }
}

fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn print_usage() {
    println!(
        "Voodoo Store 0.1\n\
         \n\
         KV / lifecycle:\n\
           voodoo-store put <store> <key> <value>\n\
           voodoo-store get <store> <key>\n\
           voodoo-store delete <store> <key>\n\
           voodoo-store verify <store>\n\
           voodoo-store backup <store> <destination>\n\
           voodoo-store compact-copy <store> <destination>\n\
         \n\
         Queue:\n\
           voodoo-store queue-push <store> <queue> <payload> [available_at_ms] [priority]\n\
           voodoo-store queue-claim <store> <queue> <now_ms> <lease_ms>\n\
           voodoo-store queue-ack <store> <queue> <id> <lease_generation>\n\
           voodoo-store queue-nack <store> <queue> <id> <lease_generation> <available_at_ms>\n\
           voodoo-store queue-dead <store> <queue> <id> <lease_generation>\n\
           voodoo-store queue-stats <store> <queue>"
    );
}
