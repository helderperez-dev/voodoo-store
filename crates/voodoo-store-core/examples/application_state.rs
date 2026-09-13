use std::error::Error;

use voodoo_store_core::{JobSpec, Store};

fn main() -> Result<(), Box<dyn Error>> {
    let path = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "application.vstore".to_owned());

    let mut store = Store::open(&path)?;

    // One durability boundary for application data + background work + event.
    let event_id = {
        let mut tx = store.begin()?;
        tx.put(b"order:42:status", b"paid")?;
        tx.enqueue_job(
            JobSpec::new(b"email.send_receipt", b"order:42"),
            1_000,
        )?;
        let event_id = tx.emit_event(b"order.paid", b"order:42", 1_000)?;
        tx.commit()?;
        event_id
    };

    assert_eq!(store.get(b"order:42:status"), Some(b"paid".as_slice()));

    let jobs = store.list_jobs()?;
    println!("jobs={}", jobs.len());

    let events = store.outbox_events_after(None, 100)?;
    println!("outbox_events={}", events.len());

    // A Runtime/dispatcher would publish the event externally and ACK only
    // after the side effect succeeds.
    if store.ack_outbox_event(event_id)? {
        println!("acked_event_tx={}", event_id.tx_id);
    }

    let health = store.health_report()?;
    println!(
        "store_id={:02x?} file_bytes={} live_keys={}",
        health.store_id, health.storage.file_bytes, health.storage.live_keys
    );

    Ok(())
}
