#![no_main]
//! Drive BMap through a sequence of insert/delete/lookup/truncate/seek ops
//! parsed from the fuzzer's byte input, and cross-check every observable
//! result against std::collections::BTreeMap as an oracle.
//!
//! Op encoding: 1 byte opcode + (up to) 2 bytes LE key; values derive from
//! the key to keep them non-invalid (!= 0).

use libfuzzer_sys::fuzz_target;
use std::collections::BTreeMap;
use std::io::ErrorKind;

use btree_ondisk::bmap::BMap;
use btree_ondisk::{NullBlockLoader, NullNodeCache};

fn key_from(data: &[u8]) -> Option<u64> {
    if data.len() < 2 { return None; }
    // keys in [1, 512) — small to exercise split/merge, nonzero so value=key is not invalid
    Some(1 + u16::from_le_bytes([data[0], data[1]]) as u64 % 511)
}

fuzz_target!(|data: &[u8]| {
    // tokio current-thread runtime is cheap enough to build per invocation
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

        let mut i = 0;
        while i + 3 <= data.len() {
            let op = data[i] & 0x07;
            let Some(k) = key_from(&data[i + 1..]) else { break };
            i += 3;
            match op {
                0 | 1 => {
                    // insert/update
                    let _ = m.insert(k, k).await;
                    oracle.insert(k, k);
                }
                2 => {
                    // delete
                    let bmap_res = m.delete(&k).await;
                    let oracle_res = oracle.remove(&k);
                    match (bmap_res, oracle_res) {
                        (Ok(()), Some(_)) => {}
                        (Err(e), None) if e.kind() == ErrorKind::NotFound => {}
                        _ => panic!("delete oracle mismatch on {k}"),
                    }
                }
                3 => {
                    // lookup
                    let bmap_res = m.lookup(&k).await;
                    let oracle_res = oracle.get(&k).copied();
                    match (bmap_res, oracle_res) {
                        (Ok(v), Some(ov)) if v == ov => {}
                        (Err(e), None) if e.kind() == ErrorKind::NotFound => {}
                        _ => panic!("lookup oracle mismatch on {k}"),
                    }
                }
                4 => {
                    // seek_key
                    let bmap_res = m.seek_key(&k).await;
                    let oracle_res = oracle.range(k..).next().map(|(&k, _)| k);
                    match (bmap_res, oracle_res) {
                        (Ok(v), Some(ov)) if v == ov => {}
                        (Err(e), None) if e.kind() == ErrorKind::NotFound => {}
                        _ => panic!("seek oracle mismatch on {k}"),
                    }
                }
                5 => {
                    // truncate: drop every key >= k
                    let _ = m.truncate(&k).await;
                    oracle.retain(|&ok, _| ok < k);
                }
                6 => {
                    // last_key
                    let bmap_res = m.last_key().await;
                    let oracle_res = oracle.keys().last().copied();
                    match (bmap_res, oracle_res) {
                        (Ok(v), Some(ov)) if v == ov => {}
                        (Err(e), None) if e.kind() == ErrorKind::NotFound => {}
                        _ => panic!("last_key oracle mismatch"),
                    }
                }
                _ => {
                    // try_insert (must reject duplicates)
                    let bmap_res = m.try_insert(k, k).await;
                    let existed = oracle.contains_key(&k);
                    if !existed {
                        assert!(bmap_res.is_ok());
                        oracle.insert(k, k);
                    } else {
                        assert_eq!(
                            bmap_res.err().map(|e| e.kind()),
                            Some(ErrorKind::AlreadyExists),
                        );
                    }
                }
            }
        }
    });
});
