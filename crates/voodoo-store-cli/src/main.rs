use std::error::Error;
use std::process::ExitCode;

use voodoo_store_core::{
    CollectionDefinition, IndexDefinition, IndexValue, JobSpec, PushOptions, ScheduleMode, Store,
};

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
            print_optional(store.get(args[3].as_bytes()));
        }
        "delete" => {
            require_len(&args, 4)?;
            let mut store = Store::open(&args[2])?;
            store.delete(args[3].as_bytes())?;
            println!("ok");
        }
        "health" => {
            require_len(&args, 3)?;
            let store = Store::open(&args[2])?;
            let report = store.health_report()?;
            println!("store_id={}", hex(&report.store_id));
            println!("format={}.{}", report.format_major, report.format_minor);
            println!("file_bytes={}", report.storage.file_bytes);
            println!("live_keys={}", report.storage.live_keys);
            println!("user_keys={}", report.storage.user_keys);
            println!("internal_keys={}", report.storage.internal_keys);
            println!("live_bytes={}", report.storage.live_bytes());
            println!("amplification={:.3}", report.storage.amplification_ratio());
            for namespace in report.namespaces {
                println!(
                    "namespace.{}.keys={} namespace.{}.value_bytes={}",
                    namespace.name, namespace.keys, namespace.name, namespace.value_bytes
                );
            }
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
            println!("backup_bytes={}", store.backup_to(&args[3])?);
        }
        "restore-copy" => {
            require_len(&args, 4)?;
            let report = Store::restore_copy(&args[2], &args[3])?;
            println!("restore_bytes={}", report.bytes);
            println!("store_id={}", hex(&report.store_id));
            println!("keys={}", report.keys);
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
        "ttl-put" => {
            require_len(&args, 6)?;
            let expires_at_ms: i64 = args[5].parse()?;
            let mut store = Store::open(&args[2])?;
            store.put_with_ttl(args[3].as_bytes(), args[4].as_bytes(), expires_at_ms)?;
            println!("ok");
        }
        "ttl-get-at" => {
            require_len(&args, 5)?;
            let now_ms: i64 = args[4].parse()?;
            let store = Store::open(&args[2])?;
            print_optional(store.get_at(args[3].as_bytes(), now_ms)?);
        }
        "ttl-clear" => {
            require_len(&args, 4)?;
            let mut store = Store::open(&args[2])?;
            println!("cleared={}", store.clear_ttl(args[3].as_bytes())?);
        }
        "ttl-purge" => {
            require_len(&args, 5)?;
            let now_ms: i64 = args[3].parse()?;
            let limit: usize = args[4].parse()?;
            let mut store = Store::open(&args[2])?;
            let report = store.purge_expired(now_ms, limit)?;
            println!("scanned={}", report.scanned);
            println!("expired={}", report.expired);
            println!("removed={}", report.removed);
        }
        "collection-create" => {
            if args.len() < 4 || args.len() > 5 {
                return Err(
                    "usage: voodoo-store collection-create <store> <collection> [codec]".into(),
                );
            }
            let mut store = Store::open(&args[2])?;
            let definition = CollectionDefinition {
                schema_version: 1,
                codec: args
                    .get(4)
                    .map_or(b"bytes".to_vec(), |value| value.as_bytes().to_vec()),
            };
            println!(
                "created={}",
                store.create_collection(args[3].as_bytes(), &definition)?
            );
        }
        "collection-index" => {
            if args.len() < 5 || args.len() > 6 {
                return Err(
                    "usage: voodoo-store collection-index <store> <collection> <index> [unique]"
                        .into(),
                );
            }
            let unique = args.get(5).is_some_and(|value| value == "unique");
            let mut store = Store::open(&args[2])?;
            println!(
                "created={}",
                store.define_index(
                    args[3].as_bytes(),
                    &IndexDefinition {
                        name: args[4].as_bytes().to_vec(),
                        unique,
                    },
                )?
            );
        }
        "collection-put" => {
            if args.len() < 6 {
                return Err("usage: voodoo-store collection-put <store> <collection> <pk> <value> [index=value ...]".into());
            }
            let indexes = args[6..]
                .iter()
                .map(|entry| parse_index_value(entry))
                .collect::<Result<Vec<_>, _>>()?;
            let mut store = Store::open(&args[2])?;
            store.upsert_record(
                args[3].as_bytes(),
                args[4].as_bytes(),
                args[5].as_bytes(),
                &indexes,
            )?;
            println!("ok");
        }
        "collection-get" => {
            require_len(&args, 5)?;
            let store = Store::open(&args[2])?;
            match store.get_record(args[3].as_bytes(), args[4].as_bytes())? {
                Some(record) => {
                    println!(
                        "primary_key={}",
                        String::from_utf8_lossy(&record.primary_key)
                    );
                    println!("value={}", String::from_utf8_lossy(&record.value));
                    for index in record.indexes {
                        println!(
                            "index.{}={}",
                            String::from_utf8_lossy(&index.index),
                            String::from_utf8_lossy(&index.value)
                        );
                    }
                }
                None => println!("not-found"),
            }
        }
        "collection-query" => {
            require_len(&args, 6)?;
            let store = Store::open(&args[2])?;
            for record in store.query_index_exact(
                args[3].as_bytes(),
                args[4].as_bytes(),
                args[5].as_bytes(),
            )? {
                println!(
                    "{}\t{}",
                    String::from_utf8_lossy(&record.primary_key),
                    String::from_utf8_lossy(&record.value)
                );
            }
        }
        "collection-delete" => {
            require_len(&args, 5)?;
            let mut store = Store::open(&args[2])?;
            println!(
                "deleted={}",
                store.delete_record(args[3].as_bytes(), args[4].as_bytes())?
            );
        }
        "queue-push" => queue_push(&args)?,
        "queue-claim" => queue_claim(&args)?,
        "queue-ack" => queue_ack(&args)?,
        "queue-nack" => queue_nack(&args)?,
        "queue-dead" => queue_dead(&args)?,
        "queue-stats" => queue_stats(&args)?,
        "stream-append" => {
            require_len(&args, 5)?;
            let mut store = Store::open(&args[2])?;
            let mut stream = store.stream(args[3].as_bytes())?;
            println!("offset={}", stream.append(args[4].as_bytes())?);
        }
        "stream-read" => {
            require_len(&args, 6)?;
            let offset: u64 = args[4].parse()?;
            let limit: usize = args[5].parse()?;
            let mut store = Store::open(&args[2])?;
            let stream = store.stream(args[3].as_bytes())?;
            for entry in stream.read_from(offset, limit)? {
                println!(
                    "{}\t{}",
                    entry.offset,
                    String::from_utf8_lossy(&entry.payload)
                );
            }
        }
        "topic-publish" => {
            require_len(&args, 5)?;
            let mut store = Store::open(&args[2])?;
            let mut topic = store.topic(args[3].as_bytes())?;
            println!("offset={}", topic.publish(args[4].as_bytes())?);
        }
        "topic-poll" => {
            require_len(&args, 6)?;
            let limit: usize = args[5].parse()?;
            let mut store = Store::open(&args[2])?;
            let topic = store.topic(args[3].as_bytes())?;
            for entry in topic.poll(args[4].as_bytes(), limit)? {
                println!(
                    "{}\t{}",
                    entry.offset,
                    String::from_utf8_lossy(&entry.payload)
                );
            }
        }
        "topic-ack" => {
            require_len(&args, 6)?;
            let offset: u64 = args[5].parse()?;
            let mut store = Store::open(&args[2])?;
            let mut topic = store.topic(args[3].as_bytes())?;
            let state = topic.acknowledge_through(args[4].as_bytes(), offset)?;
            println!("next_offset={}", state.next_offset);
        }
        "topic-reset" => {
            require_len(&args, 6)?;
            let offset: u64 = args[5].parse()?;
            let mut store = Store::open(&args[2])?;
            let mut topic = store.topic(args[3].as_bytes())?;
            topic.reset_subscription(args[4].as_bytes(), offset)?;
            println!("ok");
        }
        "object-put" => {
            require_len(&args, 4)?;
            let mut store = Store::open(&args[2])?;
            println!("id={}", hex(&store.put_object(args[3].as_bytes())?));
        }
        "object-get" => {
            require_len(&args, 4)?;
            let id = parse_hex::<32>(&args[3])?;
            let store = Store::open(&args[2])?;
            print_optional(store.get_object(&id)?);
        }
        "object-verify" => {
            require_len(&args, 4)?;
            let id = parse_hex::<32>(&args[3])?;
            let store = Store::open(&args[2])?;
            println!("valid={}", store.verify_object(&id)?);
        }
        "object-link" => {
            require_len(&args, 6)?;
            let id = parse_hex::<32>(&args[5])?;
            let mut store = Store::open(&args[2])?;
            store.link_object(args[3].as_bytes(), args[4].as_bytes(), &id)?;
            println!("ok");
        }
        "object-unlink" => {
            require_len(&args, 5)?;
            let mut store = Store::open(&args[2])?;
            println!(
                "unlinked={}",
                store.unlink_object(args[3].as_bytes(), args[4].as_bytes())?
            );
        }
        "object-gc" => {
            require_len(&args, 4)?;
            let limit: usize = args[3].parse()?;
            let mut store = Store::open(&args[2])?;
            let report = store.gc_orphan_objects(limit)?;
            println!("scanned={}", report.scanned);
            println!("referenced={}", report.referenced);
            println!("removed={}", report.removed);
        }
        "job-submit" => {
            require_len(&args, 6)?;
            let now_ms: i64 = args[5].parse()?;
            let mut store = Store::open(&args[2])?;
            let id = store.submit_job(
                JobSpec::new(args[3].as_bytes().to_vec(), args[4].as_bytes().to_vec()),
                now_ms,
            )?;
            println!("id={}", hex(&id));
        }
        "job-claim" => {
            require_len(&args, 5)?;
            let now_ms: i64 = args[3].parse()?;
            let lease_ms: u64 = args[4].parse()?;
            let mut store = Store::open(&args[2])?;
            match store.claim_job(now_ms, lease_ms)? {
                Some(job) => {
                    println!("id={}", hex(&job.id));
                    println!("handler={}", String::from_utf8_lossy(&job.handler));
                    println!("payload={}", String::from_utf8_lossy(&job.payload));
                    println!("attempts={}", job.attempts);
                    println!("lease_generation={}", job.lease_generation);
                }
                None => println!("empty"),
            }
        }
        "job-complete" => {
            require_len(&args, 6)?;
            let id = parse_hex::<16>(&args[3])?;
            let generation: u32 = args[4].parse()?;
            let now_ms: i64 = args[5].parse()?;
            let mut store = Store::open(&args[2])?;
            store.complete_job(&id, generation, now_ms)?;
            println!("ok");
        }
        "job-fail" => {
            require_len(&args, 7)?;
            let id = parse_hex::<16>(&args[3])?;
            let generation: u32 = args[4].parse()?;
            let now_ms: i64 = args[5].parse()?;
            let mut store = Store::open(&args[2])?;
            println!(
                "state={:?}",
                store.fail_job(&id, generation, now_ms, args[6].as_bytes())?
            );
        }
        "job-history" => {
            require_len(&args, 4)?;
            let id = parse_hex::<16>(&args[3])?;
            let store = Store::open(&args[2])?;
            for entry in store.job_history(&id)? {
                println!(
                    "{}\t{}\t{:?}\t{}",
                    entry.sequence,
                    entry.at_ms,
                    entry.kind,
                    String::from_utf8_lossy(&entry.detail)
                );
            }
        }
        "schedule-once" => {
            require_len(&args, 6)?;
            let first_run_ms: i64 = args[5].parse()?;
            let mut store = Store::open(&args[2])?;
            let id = store.create_schedule(
                JobSpec::new(args[3].as_bytes().to_vec(), args[4].as_bytes().to_vec()),
                ScheduleMode::Once,
                first_run_ms,
            )?;
            println!("id={}", hex(&id));
        }
        "schedule-interval" => {
            require_len(&args, 7)?;
            let first_run_ms: i64 = args[5].parse()?;
            let every_ms: u64 = args[6].parse()?;
            let mut store = Store::open(&args[2])?;
            let id = store.create_schedule(
                JobSpec::new(args[3].as_bytes().to_vec(), args[4].as_bytes().to_vec()),
                ScheduleMode::Interval { every_ms },
                first_run_ms,
            )?;
            println!("id={}", hex(&id));
        }
        "schedule-tick" => {
            require_len(&args, 5)?;
            let now_ms: i64 = args[3].parse()?;
            let limit: usize = args[4].parse()?;
            let mut store = Store::open(&args[2])?;
            let report = store.tick_schedules(now_ms, limit)?;
            println!("scanned={}", report.scanned);
            println!("fired={}", report.fired);
        }
        "workflow-create" => {
            require_len(&args, 7)?;
            let now_ms: i64 = args[6].parse()?;
            let mut store = Store::open(&args[2])?;
            let id = store.create_workflow(
                args[3].as_bytes(),
                args[4].as_bytes(),
                args[5].as_bytes(),
                None,
                now_ms,
            )?;
            println!("id={}", hex(&id));
        }
        "workflow-get" => {
            require_len(&args, 4)?;
            let id = parse_hex::<16>(&args[3])?;
            let store = Store::open(&args[2])?;
            match store.get_workflow(&id)? {
                Some(workflow) => {
                    println!("status={:?}", workflow.status);
                    println!("type={}", String::from_utf8_lossy(&workflow.workflow_type));
                    println!("step={}", String::from_utf8_lossy(&workflow.current_step));
                    println!("state={}", String::from_utf8_lossy(&workflow.state));
                    println!("wait={:?}", workflow.wait);
                }
                None => println!("not-found"),
            }
        }
        "workflow-wait-signal" => {
            require_len(&args, 6)?;
            let id = parse_hex::<16>(&args[3])?;
            let now_ms: i64 = args[5].parse()?;
            let mut store = Store::open(&args[2])?;
            store.wait_for_signal(&id, args[4].as_bytes(), now_ms)?;
            println!("ok");
        }
        "workflow-signal" => {
            require_len(&args, 7)?;
            let id = parse_hex::<16>(&args[3])?;
            let now_ms: i64 = args[6].parse()?;
            let mut store = Store::open(&args[2])?;
            println!(
                "accepted={}",
                store.signal_workflow(&id, args[4].as_bytes(), args[5].as_bytes(), now_ms)?
            );
        }
        "workflow-wait-until" => {
            require_len(&args, 6)?;
            let id = parse_hex::<16>(&args[3])?;
            let resume_at_ms: i64 = args[4].parse()?;
            let now_ms: i64 = args[5].parse()?;
            let mut store = Store::open(&args[2])?;
            store.wait_until(&id, resume_at_ms, now_ms)?;
            println!("ok");
        }
        "workflow-tick" => {
            require_len(&args, 5)?;
            let now_ms: i64 = args[3].parse()?;
            let limit: usize = args[4].parse()?;
            let mut store = Store::open(&args[2])?;
            let report = store.resume_due_workflows(now_ms, limit)?;
            println!("scanned={}", report.scanned);
            println!("resumed={}", report.resumed);
        }
        "workflow-complete" => {
            require_len(&args, 6)?;
            let id = parse_hex::<16>(&args[3])?;
            let now_ms: i64 = args[5].parse()?;
            let mut store = Store::open(&args[2])?;
            store.complete_workflow(&id, args[4].as_bytes(), now_ms)?;
            println!("ok");
        }
        "workflow-history" => {
            require_len(&args, 4)?;
            let id = parse_hex::<16>(&args[3])?;
            let store = Store::open(&args[2])?;
            for entry in store.workflow_history(&id)? {
                println!(
                    "{}\t{}\t{:?}\t{}",
                    entry.sequence,
                    entry.at_ms,
                    entry.kind,
                    String::from_utf8_lossy(&entry.detail)
                );
            }
        }
        "help" | "--help" | "-h" => print_usage(),
        other => return Err(format!("unknown command: {other}").into()),
    }

    Ok(())
}

fn queue_push(args: &[String]) -> Result<(), Box<dyn Error>> {
    if args.len() < 5 || args.len() > 7 {
        return Err(
            "usage: voodoo-store queue-push <store> <queue> <payload> [available_at_ms] [priority]"
                .into(),
        );
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
    Ok(())
}

fn queue_claim(args: &[String]) -> Result<(), Box<dyn Error>> {
    require_len(args, 6)?;
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
    Ok(())
}

fn queue_ack(args: &[String]) -> Result<(), Box<dyn Error>> {
    require_len(args, 6)?;
    let id: u64 = args[4].parse()?;
    let lease_generation: u32 = args[5].parse()?;
    let mut store = Store::open(&args[2])?;
    store.queue(args[3].as_bytes())?.ack(id, lease_generation)?;
    println!("ok");
    Ok(())
}

fn queue_nack(args: &[String]) -> Result<(), Box<dyn Error>> {
    require_len(args, 7)?;
    let id: u64 = args[4].parse()?;
    let lease_generation: u32 = args[5].parse()?;
    let available_at_ms: i64 = args[6].parse()?;
    let mut store = Store::open(&args[2])?;
    store
        .queue(args[3].as_bytes())?
        .nack(id, lease_generation, available_at_ms)?;
    println!("ok");
    Ok(())
}

fn queue_dead(args: &[String]) -> Result<(), Box<dyn Error>> {
    require_len(args, 6)?;
    let id: u64 = args[4].parse()?;
    let lease_generation: u32 = args[5].parse()?;
    let mut store = Store::open(&args[2])?;
    store
        .queue(args[3].as_bytes())?
        .dead_letter(id, lease_generation)?;
    println!("ok");
    Ok(())
}

fn queue_stats(args: &[String]) -> Result<(), Box<dyn Error>> {
    require_len(args, 4)?;
    let mut store = Store::open(&args[2])?;
    let stats = store.queue(args[3].as_bytes())?.stats()?;
    println!("ready={}", stats.ready);
    println!("leased={}", stats.leased);
    println!("dead={}", stats.dead);
    println!("total={}", stats.total);
    Ok(())
}

fn parse_index_value(value: &str) -> Result<IndexValue, Box<dyn Error>> {
    let (index, value) = value
        .split_once('=')
        .ok_or("index entry must use index=value")?;
    if index.is_empty() {
        return Err("index name cannot be empty".into());
    }
    Ok(IndexValue {
        index: index.as_bytes().to_vec(),
        value: value.as_bytes().to_vec(),
    })
}

fn require_len(args: &[String], expected: usize) -> Result<(), Box<dyn Error>> {
    if args.len() == expected {
        Ok(())
    } else {
        Err("invalid arguments; run `voodoo-store help` for usage".into())
    }
}

fn print_optional(value: Option<&[u8]>) {
    match value {
        Some(value) => println!("{}", String::from_utf8_lossy(value)),
        None => println!("not-found"),
    }
}

fn parse_hex<const N: usize>(value: &str) -> Result<[u8; N], Box<dyn Error>> {
    if value.len() != N * 2 {
        return Err(format!("expected {} hexadecimal characters", N * 2).into());
    }
    let mut output = [0u8; N];
    for (index, chunk) in value.as_bytes().chunks_exact(2).enumerate() {
        let part = std::str::from_utf8(chunk)?;
        output[index] = u8::from_str_radix(part, 16)?;
    }
    Ok(output)
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
         Core: put get delete health verify backup restore-copy compact-copy\n\
         TTL: ttl-put ttl-get-at ttl-clear ttl-purge\n\
         Collections: collection-create collection-index collection-put collection-get collection-query collection-delete\n\
         Queue: queue-push queue-claim queue-ack queue-nack queue-dead queue-stats\n\
         Messaging: stream-append stream-read topic-publish topic-poll topic-ack topic-reset\n\
         Objects: object-put object-get object-verify object-link object-unlink object-gc\n\
         Jobs: job-submit job-claim job-complete job-fail job-history\n\
         Scheduler: schedule-once schedule-interval schedule-tick\n\
         Workflows: workflow-create workflow-get workflow-wait-signal workflow-signal workflow-wait-until workflow-tick workflow-complete workflow-history\n\
         \n\
         Run a command with invalid arguments to see its expected parameters."
    );
}