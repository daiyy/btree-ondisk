#![no_main]
//! Fuzz `BMap::lookup_batch` — drive a small randomized BMap through a
//! population phase, then issue a single `lookup_batch` over an
//! arbitrary set of keys and cross-check every per-key result both
//! against `std::collections::BTreeMap` (oracle of truth) and
//! against `BMap::lookup` (oracle of internal consistency).
//!
//! Encoding:
//!     header byte: how many populate ops to apply (mod 64)
//!     populate phase: each op is 3 bytes (1 byte opcode, 2 bytes LE key)
//!         opcode bit 0: 0 = insert, 1 = delete
//!     remaining bytes: (2 byte LE key) probes for the batch.

use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;
use std::io::ErrorKind;

use btree_ondisk::bmap::BMap;
use btree_ondisk::{NullBlockLoader, NullNodeCache};

fn key_from(data: &[u8]) -> Option<u64> {
    if data.len() < 2 {
        return None;
    }
    // small key space -> frequent collisions and many splits/merges
    Some(1 + u16::from_le_bytes([data[0], data[1]]) as u64 % 511)
}

fuzz_target!(|data: &[u8]| {
    if data.is_empty() {
        return;
    }
    let rt = match tokio::runtime::Builder::new_current_thread().build() {
        Ok(rt) => rt,
        Err(_) => return,
    };
    rt.block_on(async move {
        let mut m: BMap<u64, u64, u64, NullBlockLoader, NullNodeCache> =
            match BMap::new(56, 256, NullBlockLoader, NullNodeCache) {
                Ok(m) => m,
                Err(_) => return,
            };
        let mut oracle: BTreeMap<u64, u64> = BTreeMap::new();

        // populate phase
        let pop_ops = (data[0] as usize) % 64;
        let mut i = 1;
        for _ in 0..pop_ops {
            if i + 3 > data.len() {
                break;
            }
            let opcode = data[i];
            let Some(k) = key_from(&data[i + 1..]) else { break };
            i += 3;
            if opcode & 1 == 0 {
                let _ = m.insert(k, k).await;
                oracle.insert(k, k);
            } else {
                let bmap_res = m.delete(&k).await;
                let oracle_res = oracle.remove(&k);
                match (bmap_res, oracle_res) {
                    (Ok(()), Some(_)) => {}
                    (Err(e), None) if e.kind() == ErrorKind::NotFound => {}
                    _ => panic!("populate-phase delete oracle mismatch on {k}"),
                }
            }
        }

        // batch phase: parse remaining bytes as a key list (capped)
        let mut keys: Vec<u64> = Vec::new();
        while i + 2 <= data.len() && keys.len() < 256 {
            if let Some(k) = key_from(&data[i..]) {
                keys.push(k);
            }
            i += 2;
        }

        // 1) Compare each batch entry against the std::BTreeMap oracle.
        let batch = m.lookup_batch(&keys).await;
        assert_eq!(batch.len(), keys.len());
        for (idx, k) in keys.iter().enumerate() {
            let oracle_res = oracle.get(k).copied();
            match (&batch[idx], oracle_res) {
                (Ok(v), Some(ov)) if *v == ov => {}
                (Err(e), None) if e.kind() == ErrorKind::NotFound => {}
                (a, b) => panic!(
                    "lookup_batch oracle mismatch at i={idx} key={k}: batch={a:?}, oracle={b:?}"
                ),
            }
        }

        // 2) Cross-check against BMap::lookup() — internal consistency.
        // Doing per-key lookup AFTER the batch is fine: lookup_batch
        // does not mutate map state any more than lookup does.
        for (idx, k) in keys.iter().enumerate() {
            let single = m.lookup(k).await;
            match (&batch[idx], single) {
                (Ok(a), Ok(b)) if *a == b => {}
                (Err(ea), Err(eb)) if ea.kind() == eb.kind() => {}
                (a, b) => panic!(
                    "batch/single divergence at i={idx} key={k}: batch={a:?}, single={b:?}"
                ),
            }
        }
    });
});
